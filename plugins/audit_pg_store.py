"""PostgreSQL 审计存储。

审计事件存入 PostgreSQL audit_events 表，支持：
- 复杂条件查询（时间范围、事件类型、告警 ID）
- hash chain 完整性校验
- 自动分区（按月）
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.config import pg_dsn
from core.models import AuditEvent
from interfaces.base import IAuditStore


class PgAuditStore(IAuditStore):
    """PostgreSQL 审计事件存储。

    FR-2.3: supports_chain_state=True —— emit 在单个事务内 FOR UPDATE 行锁
    读链头 → 填 prev_hash → 计算 event_hash → 插入事件 → 推进链头。
    多实例并发写入链不分叉（行锁串行化），重启后链头接续。
    """

    supports_chain_state = True

    def __init__(self, dsn: Optional[str] = None, table: str = "audit_events"):
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

    def emit(self, event: AuditEvent) -> Optional[str]:
        """事务内推进链并落库；返回本事件 hash（链头）。

        事务步骤（FOR UPDATE 行锁串行化多实例写入）:
        1. SELECT last_hash FROM audit_chain_state WHERE id=1 FOR UPDATE
        2. event.prev_hash = 链头；计算 event_hash
        3. INSERT 事件 + UPSERT 链头（同一事务，原子）
        """
        import hashlib

        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT last_hash FROM audit_chain_state WHERE id = 1 FOR UPDATE"
            ).fetchone()
            prev_hash = row[0] if row else None
            event.prev_hash = prev_hash

            content = event.model_dump_json(exclude={"prev_hash"})
            event_hash = hashlib.sha256(
                (content + (prev_hash or "")).encode()
            ).hexdigest()

            conn.execute(
                f"""INSERT INTO {self._table}
                    (event_id, timestamp, event_type, actor, alert_id,
                     pipeline_run_id, category, severity, payload, outcome,
                     event_hash, prev_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    event.event_id,
                    event.timestamp,
                    event.event_type,
                    event.actor,
                    event.alert_id,
                    event.pipeline_run_id,
                    event.category,
                    event.severity,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.outcome,
                    event_hash,
                    event.prev_hash,
                ),
            )
            conn.execute(
                """INSERT INTO audit_chain_state (id, last_hash, last_event_id)
                   VALUES (1, %s, %s)
                   ON CONFLICT (id) DO UPDATE
                   SET last_hash = EXCLUDED.last_hash,
                       last_event_id = EXCLUDED.last_event_id,
                       updated_at = now()""",
                (event_hash, event.event_id),
            )
        return event_hash

    def query(
        self,
        event_type: Optional[str] = None,
        alert_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        conditions = []
        params: list = []

        if event_type:
            conditions.append("event_type LIKE %s")
            params.append(f"{event_type}%")
        if alert_id:
            conditions.append("alert_id = %s")
            params.append(alert_id)
        if start_time:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= %s")
            params.append(end_time)

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = (
            f"SELECT event_id, timestamp, event_type, actor, alert_id, "
            f"pipeline_run_id, category, severity, payload, outcome, prev_hash "
            f"FROM {self._table} WHERE {where} "
            f"ORDER BY timestamp ASC LIMIT %s"
        )
        params.append(limit)

        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()

        results = []
        for row in rows:
            results.append(AuditEvent(
                event_id=row[0],
                timestamp=row[1],
                event_type=row[2],
                actor=row[3],
                alert_id=row[4],
                pipeline_run_id=row[5],
                category=row[6],
                severity=row[7],
                payload=row[8] if isinstance(row[8], dict) else json.loads(row[8]),
                outcome=row[9],
                prev_hash=row[10],
            ))
        return results

    def verify_chain(self, from_id: str, to_id: str) -> bool:
        import hashlib

        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                f"""SELECT event_id, timestamp, event_type, actor, alert_id,
                           pipeline_run_id, category, severity, payload, outcome,
                           event_hash, prev_hash
                    FROM {self._table}
                    WHERE event_id = %s OR event_id = %s
                       OR (timestamp >= (SELECT timestamp FROM {self._table} WHERE event_id = %s)
                          AND timestamp <= (SELECT timestamp FROM {self._table} WHERE event_id = %s))
                    ORDER BY timestamp ASC""",
                (from_id, to_id, from_id, to_id),
            ).fetchall()

        if not rows:
            return False

        prev_hash = rows[0][11]
        for row in rows:
            stored_hash = row[10]
            stored_prev = row[11]

            if stored_prev != prev_hash:
                return False

            event_data = {
                "event_id": row[0], "timestamp": str(row[1]),
                "event_type": row[2], "actor": row[3],
                "alert_id": row[4], "pipeline_run_id": row[5],
                "category": row[6], "severity": row[7],
                "payload": row[8], "outcome": row[9],
            }
            content = json.dumps(event_data, ensure_ascii=False, default=str)
            computed = hashlib.sha256(
                (content + (prev_hash or "")).encode()
            ).hexdigest()

            if computed != stored_hash:
                return False
            prev_hash = computed

        return True

    def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None
