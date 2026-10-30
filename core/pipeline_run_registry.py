"""pipeline_runs 登记与断点恢复查询（FR-4）。

每个 pipeline_run 启动时登记（alert_id → run_id/status），崩溃重启后
run_pipeline 据此找到未终结的 run 复用 checkpoint 续跑，不重复执行已完成
的节点（如 execute）。

状态机：running → completed | failed | suspended
- running:   执行中；进程崩溃后保持 running（僵死），重启后可被断点恢复
- suspended: 人工审批挂起（FR-3，interrupt）；仅由审批恢复入口唤醒，
             新告警到达不会复用 suspended run
- completed/failed: 终态

双实现：PgPipelineRunRegistry（生产）/ InMemoryRunRegistry（单测）
"""

import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from core.logger import get_logger

logger = get_logger("run_registry")

# 未终结状态：断点恢复只针对 running；suspended 由审批恢复入口独占唤醒
RESUMABLE_STATUS = "running"


@dataclass
class PipelineRunRecord:
    """一次 pipeline 执行的登记记录。"""
    run_id: str
    alert_id: str
    status: str  # running | suspended | completed | failed
    started_at: datetime
    updated_at: datetime


class IPipelineRunRegistry(ABC):
    """pipeline_run 登记接口。"""

    @abstractmethod
    def find_open_run(self, alert_id: str, include_suspended: bool = False) -> Optional[PipelineRunRecord]:
        """查找该告警未终结的最近一次 run（默认仅 running）。

        include_suspended=False 时不含 suspended（suspended 只由审批恢复入口唤醒）。
        返回 None 表示无未终结 run，应启动新 run。
        """
        pass

    @abstractmethod
    def register(self, run_id: str, alert_id: str, status: str = "running") -> None:
        """登记（或重登记）一次 run，幂等 upsert。"""
        pass

    @abstractmethod
    def update_status(self, run_id: str, status: str) -> None:
        """更新 run 状态。"""
        pass

    @abstractmethod
    def find_stale_runs(self, stale_after_seconds: int, limit: int = 50) -> List[PipelineRunRecord]:
        """running 但 updated_at 超过阈值的 run（僵死巡检，供 resume_stale_runs 脚本）。"""
        pass


class InMemoryRunRegistry(IPipelineRunRegistry):
    """进程内实现（单测/无 PG 环境）。线程安全。"""

    def __init__(self):
        self._runs = {}  # run_id -> PipelineRunRecord
        self._lock = threading.Lock()

    def find_open_run(self, alert_id: str, include_suspended: bool = False) -> Optional[PipelineRunRecord]:
        statuses = ("running", "suspended") if include_suspended else (RESUMABLE_STATUS,)
        with self._lock:
            candidates = [
                r for r in self._runs.values()
                if r.alert_id == alert_id and r.status in statuses
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda r: r.updated_at)

    def register(self, run_id: str, alert_id: str, status: str = "running") -> None:
        now = datetime.now()
        with self._lock:
            existing = self._runs.get(run_id)
            if existing:
                existing.status = status
                existing.updated_at = now
            else:
                self._runs[run_id] = PipelineRunRecord(
                    run_id=run_id, alert_id=alert_id, status=status,
                    started_at=now, updated_at=now,
                )

    def update_status(self, run_id: str, status: str) -> None:
        with self._lock:
            record = self._runs.get(run_id)
            if record:
                record.status = status
                record.updated_at = datetime.now()

    def find_stale_runs(self, stale_after_seconds: int, limit: int = 50) -> List[PipelineRunRecord]:
        cutoff = datetime.now().timestamp() - stale_after_seconds
        with self._lock:
            stale = [
                r for r in self._runs.values()
                if r.status == "running" and r.updated_at.timestamp() < cutoff
            ]
            stale.sort(key=lambda r: r.updated_at)
            return stale[:limit]

    # ── 测试辅助 ──
    def all_runs(self) -> List[PipelineRunRecord]:
        with self._lock:
            return list(self._runs.values())


class PgPipelineRunRegistry(IPipelineRunRegistry):
    """PostgreSQL 实现（生产）。DDL 见 sql/init_pipeline_runs.sql。"""

    def __init__(self, dsn: Optional[str] = None, table: str = "pipeline_runs"):
        # 修复：DSN 收口到 pg_dsn()（AIOPS__PG__DSN > AIOPS_PG_DSN > kb.pg.dsn）
        from core.config import pg_dsn
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

    def find_open_run(self, alert_id: str, include_suspended: bool = False) -> Optional[PipelineRunRecord]:
        statuses = ("running", "suspended") if include_suspended else (RESUMABLE_STATUS,)
        sql = (
            f"SELECT run_id, alert_id, status, started_at, updated_at "
            f"FROM {self._table} WHERE alert_id = %s AND status = ANY(%s) "
            f"ORDER BY updated_at DESC LIMIT 1"
        )
        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(sql, (alert_id, list(statuses))).fetchone()
        if not row:
            return None
        return PipelineRunRecord(run_id=row[0], alert_id=row[1], status=row[2],
                                 started_at=row[3], updated_at=row[4])

    def register(self, run_id: str, alert_id: str, status: str = "running") -> None:
        sql = (
            f"INSERT INTO {self._table} (run_id, alert_id, status) VALUES (%s, %s, %s) "
            f"ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, updated_at = now()"
        )
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(sql, (run_id, alert_id, status))

    def update_status(self, run_id: str, status: str) -> None:
        sql = f"UPDATE {self._table} SET status = %s, updated_at = now() WHERE run_id = %s"
        pool = self._get_pool()
        with pool.connection() as conn:
            conn.execute(sql, (status, run_id))

    def find_stale_runs(self, stale_after_seconds: int, limit: int = 50) -> List[PipelineRunRecord]:
        sql = (
            f"SELECT run_id, alert_id, status, started_at, updated_at "
            f"FROM {self._table} WHERE status = 'running' "
            f"AND updated_at < now() - make_interval(secs => %s) "
            f"ORDER BY updated_at ASC LIMIT %s"
        )
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(sql, (stale_after_seconds, limit)).fetchall()
        return [
            PipelineRunRecord(run_id=r[0], alert_id=r[1], status=r[2],
                              started_at=r[3], updated_at=r[4])
            for r in rows
        ]

    def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None
