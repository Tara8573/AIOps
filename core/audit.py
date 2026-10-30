"""安全审计模块。

提供 AuditEmitter（审计事件发射器 + hash chain 防篡改）
和 audit_node 装饰器（为 LangGraph 节点透明埋点）。
"""

import hashlib
import os
import uuid
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional

from core.logger import get_logger
from core.models import AuditEvent

logger = get_logger("audit")

_AUDIT_ENABLED: Optional[bool] = None


def is_audit_enabled() -> bool:
    global _AUDIT_ENABLED
    if _AUDIT_ENABLED is None:
        _AUDIT_ENABLED = os.getenv("AIOPS_AUDIT_ENABLED", "1") == "1"
    return _AUDIT_ENABLED


_audit_store_instance = None


def get_audit_store():
    """按 audit.store 配置返回共享审计存储（pg / file / None=loguru）。

    - pg：AIOPS__PG__DSN（经 core.config.pg_dsn）已配置时创建 PgAuditStore
    - file：FileAuditStore（现状行为）
    - 未配置/创建失败 → None（AuditEmitter 回落 loguru，不阻断主流程）
    模块级单例：多个 emitter（pipeline/降级/熔断/web api）共享同一 store。
    """
    global _audit_store_instance
    if _audit_store_instance is not None:
        return _audit_store_instance

    mode = ""
    try:
        from core.config import get_settings
        mode = str(get_settings().get("audit.store", "") or "").strip().lower()
    except Exception:
        mode = ""
    if not mode:
        mode = (os.getenv("AIOPS_AUDIT_STORE") or "").strip().lower()

    store = None
    if mode == "pg":
        try:
            from plugins.audit_pg_store import PgAuditStore
            store = PgAuditStore()
        except Exception as exc:
            logger.warning("审计 PG store 创建失败，回落 loguru | error={}", exc)
            store = None
    elif mode == "file":
        try:
            from plugins.audit_file_store import FileAuditStore
            log_dir = "./logs/audit"
            try:
                from core.config import get_settings
                log_dir = str(get_settings().get("audit.dir", "") or "./logs/audit")
            except Exception:
                pass
            store = FileAuditStore(log_dir=log_dir)
        except Exception as exc:
            logger.warning("审计 file store 创建失败，回落 loguru | error={}", exc)
            store = None

    _audit_store_instance = store
    return store


class AuditEmitter:
    """审计事件发射器。

    维护一条 SHA256 hash chain：每条事件的 prev_hash 指向前一条事件
    的 hash，任何篡改都会导致 verify_chain 校验失败。
    """

    def __init__(self, store=None, enabled: Optional[bool] = None):
        self._store = store
        self._last_hash: Optional[str] = None
        self._enabled = enabled if enabled is not None else is_audit_enabled()
        self._events: list = []

    def emit(
        self,
        event_type: str,
        *,
        actor: str = "system",
        alert_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        category: str = "operational",
        severity: str = "info",
        payload: Optional[Dict[str, Any]] = None,
        outcome: str = "success",
    ) -> Optional[AuditEvent]:
        if not self._enabled:
            return None

        chain_enabled = os.getenv("AIOPS_AUDIT_HASH_CHAIN_ENABLED", "1") == "1"
        store_handles_chain = (
            chain_enabled
            and self._store is not None
            and getattr(self._store, "supports_chain_state", False)
        )

        if store_handles_chain:
            # FR-2.3: 链头托管给 store —— PG 事务内 FOR UPDATE 行锁推进链，
            # 多实例写入链不分叉、重启后接续（store 负责填 prev_hash + event_hash 并落库）
            event = AuditEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(),
                event_type=event_type,
                actor=actor,
                alert_id=alert_id,
                pipeline_run_id=pipeline_run_id,
                category=category,
                severity=severity,
                payload=payload or {},
                outcome=outcome,
                prev_hash=None,
            )
            try:
                new_hash = self._store.emit(event)
                if new_hash:
                    self._last_hash = new_hash
            except Exception as exc:
                logger.error_occurred("audit_store_error", str(exc))
            self._events.append(event)
            return event

        # 本地链（现状行为：file/memory store / 无 store）
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            event_type=event_type,
            actor=actor,
            alert_id=alert_id,
            pipeline_run_id=pipeline_run_id,
            category=category,
            severity=severity,
            payload=payload or {},
            outcome=outcome,
            prev_hash=self._last_hash,
        )

        if chain_enabled:
            content = event.model_dump_json(exclude={"prev_hash"})
            self._last_hash = hashlib.sha256(
                (content + (self._last_hash or "")).encode()
            ).hexdigest()

        self._events.append(event)

        if self._store:
            try:
                self._store.emit(event)
            except Exception as exc:
                logger.error_occurred("audit_store_error", str(exc))
        else:
            logger.audit_event(event.event_type, event.model_dump())

        return event

    @property
    def last_hash(self) -> Optional[str]:
        return self._last_hash

    @property
    def events(self) -> list:
        return list(self._events)

    def reset(self):
        self._last_hash = None
        self._events.clear()


def audit_node(event_type: str):
    """装饰器工厂：为 LangGraph 节点函数自动埋点审计事件。

    使用方式：
        @audit_node("receive")
        def receive(state):
            ...
    """
    def decorator(node_func: Callable) -> Callable:
        @wraps(node_func)
        def wrapper(state: dict) -> dict:
            emitter = state.get("_audit_emitter")
            alert = state.get("alert")
            alert_id = alert.alert_id if alert else None
            run_id = state.get("pipeline_run_id")

            if emitter:
                emitter.emit(
                    f"{event_type}.start",
                    alert_id=alert_id,
                    pipeline_run_id=run_id,
                )

            try:
                result = node_func(state)

                if emitter:
                    emitter.emit(
                        f"{event_type}.complete",
                        alert_id=alert_id,
                        pipeline_run_id=run_id,
                        outcome="success",
                    )
                return result

            except Exception as exc:
                if emitter:
                    emitter.emit(
                        f"{event_type}.error",
                        alert_id=alert_id,
                        pipeline_run_id=run_id,
                        outcome="failure",
                        severity="critical",
                        payload={"error": str(exc), "type": type(exc).__name__},
                    )
                raise

        wrapper._audit_event_type = event_type
        return wrapper
    return decorator
