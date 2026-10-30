"""异步审批存储（FR-3 人工确认异步化）。

PgApprovalRequestStore: 生产实现（PG，SKIP LOCKED 原子领取）
InMemoryApprovalRequestStore: 单测 / 无 PG 环境实现

状态机：pending → approved | rejected | expired → resumed
流转：approve 节点 submit 后 interrupt 挂起 → 人工 decide → Resumer
take_decided 原子领取 → Command(resume=decision) 恢复执行。
"""

import os
import threading
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from core.config import pg_dsn
from core.logger import get_logger
from core.models import ActionPlan, Alert, ApprovalRecord, EvaluationResult
from interfaces.base import IApprovalRequestStore

logger = get_logger("approval_store")

# 已决策未恢复状态集合（take_decided 领取范围）
DECIDED_STATUSES = ("approved", "rejected", "expired")


class InMemoryApprovalRequestStore(IApprovalRequestStore):
    """进程内实现（单测/无 PG 环境）。线程安全。"""

    def __init__(self):
        self._records = {}  # approval_id -> ApprovalRecord
        self._lock = threading.Lock()

    def submit(self, pipeline_run_id: str, alert: Alert, plan: ActionPlan,
               evaluation: EvaluationResult, timeout_seconds: int) -> str:
        now = datetime.now()
        with self._lock:
            # 幂等护栏：按 pipeline_run_id 查 pending，已存在则返回既有 id
            for record in self._records.values():
                if record.pipeline_run_id == pipeline_run_id and record.status == "pending":
                    return record.approval_id
            approval_id = str(uuid.uuid4())
            self._records[approval_id] = ApprovalRecord(
                approval_id=approval_id,
                pipeline_run_id=pipeline_run_id,
                alert_id=alert.alert_id,
                proposal=plan.model_dump(),
                eval_summary={
                    "is_passed": evaluation.is_passed,
                    "reason": evaluation.reason,
                    "risk_level": evaluation.risk_level,
                },
                status="pending",
                created_at=now,
                timeout_at=now + timedelta(seconds=timeout_seconds),
            )
        return approval_id

    def find_by_run(self, pipeline_run_id: str) -> Optional[ApprovalRecord]:
        with self._lock:
            pending = [
                r for r in self._records.values()
                if r.pipeline_run_id == pipeline_run_id and r.status == "pending"
            ]
            if pending:
                return pending[0]
            records = [r for r in self._records.values() if r.pipeline_run_id == pipeline_run_id]
            if not records:
                return None
            return max(records, key=lambda r: r.created_at)

    def decide(self, approval_id: str, decision: str, decider: str = "", reason: str = "") -> bool:
        if decision not in ("approved", "rejected"):
            return False
        with self._lock:
            record = self._records.get(approval_id)
            if not record or record.status != "pending":
                return False
            record.status = decision
            record.decider = decider
            record.decide_reason = reason
            record.decided_at = datetime.now()
        return True

    def expire_overdue(self) -> List[ApprovalRecord]:
        now = datetime.now()
        expired = []
        with self._lock:
            for record in self._records.values():
                if record.status == "pending" and record.timeout_at and record.timeout_at < now:
                    record.status = "expired"
                    record.decider = "system"
                    record.decide_reason = "审批超时，自动拒绝"
                    record.decided_at = now
                    expired.append(record)
        return expired

    def take_decided(self, limit: int = 10, instance_id: str = "") -> List[ApprovalRecord]:
        now = datetime.now()
        taken = []
        with self._lock:
            candidates = sorted(
                [r for r in self._records.values()
                 if r.status in DECIDED_STATUSES and r.resumed_at is None],
                key=lambda r: r.decided_at or now,
            )
            for record in candidates[:max(1, limit)]:
                record.resumed_at = now
                record.resumed_by = instance_id
                taken.append(record)
        return taken

    def mark_resumed(self, approval_id: str, instance_id: str, success: bool) -> None:
        with self._lock:
            record = self._records.get(approval_id)
            if not record:
                return
            if success:
                record.status = "resumed"
            else:
                # 恢复失败：清 resumed_at 待下轮重试
                record.resumed_at = None
                record.resumed_by = None

    def list_by_status(self, status: str, limit: int = 50) -> List[ApprovalRecord]:
        with self._lock:
            records = [r for r in self._records.values() if r.status == status]
            records.sort(key=lambda r: r.created_at, reverse=True)
            return records[:limit]

    def get(self, approval_id: str) -> Optional[ApprovalRecord]:
        with self._lock:
            return self._records.get(approval_id)


class PgApprovalRequestStore(IApprovalRequestStore):
    """PostgreSQL 实现（生产）。DDL 见 sql/init_approval.sql。

    - submit: 部分唯一索引 (pipeline_run_id) WHERE status='pending' + ON CONFLICT
      DO NOTHING —— 多实例/interrupt 重跑幂等
    - take_decided: SELECT ... FOR UPDATE SKIP LOCKED —— 多实例原子领取不重复捡起
    """

    def __init__(self, dsn: Optional[str] = None, table: str = "approval_requests"):
        self._dsn = dsn or pg_dsn()
        self._table = table
        self._pool = None

    def _get_pool(self):
        if self._pool is None:
            import psycopg_pool
            self._pool = psycopg_pool.ConnectionPool(
                self._dsn, min_size=1, max_size=5, open=True,
            )
        return self._pool

    def submit(self, pipeline_run_id: str, alert: Alert, plan: ActionPlan,
               evaluation: EvaluationResult, timeout_seconds: int) -> str:
        pool = self._get_pool()
        eval_summary = {
            "is_passed": evaluation.is_passed,
            "reason": evaluation.reason,
            "risk_level": evaluation.risk_level,
        }
        with pool.connection() as conn:
            # 幂等：已有 pending 记录则不重复插入（部分唯一索引 + ON CONFLICT DO NOTHING）
            conn.execute(
                f"""INSERT INTO {self._table}
                    (approval_id, pipeline_run_id, alert_id, proposal, eval_summary, timeout_at)
                    VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s))
                    ON CONFLICT (pipeline_run_id) WHERE status = 'pending' DO NOTHING""",
                (
                    str(uuid.uuid4()), pipeline_run_id, alert.alert_id,
                    plan.model_dump(), eval_summary, timeout_seconds,
                ),
            )
            row = conn.execute(
                f"SELECT approval_id FROM {self._table} "
                f"WHERE pipeline_run_id = %s AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
                (pipeline_run_id,),
            ).fetchone()
        return row[0] if row else ""

    def _row_to_record(self, row) -> ApprovalRecord:
        return ApprovalRecord(
            approval_id=str(row[0]),
            pipeline_run_id=row[1],
            alert_id=row[2],
            proposal=row[3] if isinstance(row[3], dict) else dict(row[3] or {}),
            eval_summary=row[4] if isinstance(row[4], dict) else dict(row[4] or {}),
            status=row[5],
            decider=row[6],
            decide_reason=row[7],
            created_at=row[8],
            timeout_at=row[9],
            decided_at=row[10],
            resumed_at=row[11],
            resumed_by=row[12],
        )

    _SELECT_COLS = ("approval_id, pipeline_run_id, alert_id, proposal, eval_summary, "
                    "status, decider, decide_reason, created_at, timeout_at, "
                    "decided_at, resumed_at, resumed_by")

    def find_by_run(self, pipeline_run_id: str) -> Optional[ApprovalRecord]:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                f"SELECT {self._SELECT_COLS} FROM {self._table} "
                f"WHERE pipeline_run_id = %s AND status = 'pending' "
                f"ORDER BY created_at DESC LIMIT 1",
                (pipeline_run_id,),
            ).fetchone()
            if not row:
                row = conn.execute(
                    f"SELECT {self._SELECT_COLS} FROM {self._table} "
                    f"WHERE pipeline_run_id = %s ORDER BY created_at DESC LIMIT 1",
                    (pipeline_run_id,),
                ).fetchone()
        return self._row_to_record(row) if row else None

    def decide(self, approval_id: str, decision: str, decider: str = "", reason: str = "") -> bool:
        if decision not in ("approved", "rejected"):
            return False
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                f"UPDATE {self._table} SET status = %s, decider = %s, decide_reason = %s, "
                f"decided_at = now() WHERE approval_id = %s::uuid AND status = 'pending' "
                f"RETURNING approval_id",
                (decision, decider, reason, approval_id),
            ).fetchone()
        return row is not None

    def expire_overdue(self) -> List[ApprovalRecord]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                f"UPDATE {self._table} SET status = 'expired', decider = 'system', "
                f"decide_reason = '审批超时，自动拒绝', decided_at = now() "
                f"WHERE status = 'pending' AND timeout_at < now() RETURNING {self._SELECT_COLS}",
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def take_decided(self, limit: int = 10, instance_id: str = "") -> List[ApprovalRecord]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                f"""WITH candidates AS (
                        SELECT approval_id FROM {self._table}
                        WHERE status = ANY(%s) AND resumed_at IS NULL
                        ORDER BY decided_at ASC LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self._table} a
                    SET resumed_at = now(), resumed_by = %s
                    FROM candidates c
                    WHERE a.approval_id = c.approval_id
                    RETURNING {', '.join('a.' + c.strip() for c in self._SELECT_COLS.split(','))}""",
                (list(DECIDED_STATUSES), max(1, limit), instance_id),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def mark_resumed(self, approval_id: str, instance_id: str, success: bool) -> None:
        pool = self._get_pool()
        with pool.connection() as conn:
            if success:
                conn.execute(
                    f"UPDATE {self._table} SET status = 'resumed' WHERE approval_id = %s::uuid",
                    (approval_id,),
                )
            else:
                conn.execute(
                    f"UPDATE {self._table} SET resumed_at = NULL, resumed_by = NULL "
                    f"WHERE approval_id = %s::uuid",
                    (approval_id,),
                )

    def list_by_status(self, status: str, limit: int = 50) -> List[ApprovalRecord]:
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                f"SELECT {self._SELECT_COLS} FROM {self._table} "
                f"WHERE status = %s ORDER BY created_at DESC LIMIT %s",
                (status, limit),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def get(self, approval_id: str) -> Optional[ApprovalRecord]:
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                f"SELECT {self._SELECT_COLS} FROM {self._table} WHERE approval_id = %s::uuid",
                (approval_id,),
            ).fetchone()
        return self._row_to_record(row) if row else None

    def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None
