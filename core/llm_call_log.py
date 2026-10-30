"""LLM 调用事件日志（LLMO-2，规格：docs/LLM调用可观测性与质量评估开发规格说明书.md）。

单一事实源：每次 LLM HTTP 调用 = 一行 append-only JSONL 事件，指标/评测/审计均为其派生或独立维度。

- LLMO-2.1: LLMCallLogEmitter —— append-only 落盘 + 并发锁 + 轮转 + 内存环形摘要
- LLMO-2.2: content_hash —— prompt/response 指纹（评测用例/调用关联键）
- LLMO-3.1: emit_parse_failure —— 引擎解析失败补记（failure_type=parse_failure）
- LLMO-6.1: 明文写入前必经 redact_text 脱敏；canonical 留本地

观测失败不影响主流程（规格 2.3 原则 6：异常吞噬 + metrics 计数）。
"""

import hashlib
import json
import os
import threading
import uuid
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, Optional

from core.logger import get_logger, redact_text
from core.models import LLMCallEvent, LLMUsage
from core.observability import metrics

logger = get_logger("llm_call_log")

# 明文截断上限（规格 NFR 4.1：内存/磁盘体积控制）
_PROMPT_TRUNCATE = int(os.getenv("AIOPS_LLM_CALL_PROMPT_TRUNCATE", "2000"))
_RESPONSE_TRUNCATE = int(os.getenv("AIOPS_LLM_CALL_RESPONSE_TRUNCATE", "4000"))

_TRUTHY = {"1", "true", "yes", "on"}


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in _TRUTHY


def _config_bool(flat_env_key: str, yaml_path: str, default: bool) -> bool:
    v = os.getenv(flat_env_key)
    if v is None:
        v = _yaml_value(yaml_path)
    if v is None:
        return default
    return _as_bool(v)


def _config_scalar(flat_env_key: str, yaml_path: str, default, cast):
    v = os.getenv(flat_env_key)
    if v is None:
        v = _yaml_value(yaml_path)
    if v is None:
        return default
    try:
        return cast(v)
    except (TypeError, ValueError):
        return default


def _yaml_value(yaml_path: str):
    """读取 yaml 覆盖（AIOPS__LLM__CALL_LOG_* 对应 llm.call_log.*）。"""
    try:
        from core.config import get_settings
        return get_settings().get(yaml_path)
    except Exception:
        return None


def content_hash(text: str) -> str:
    """文本 SHA256 前 16 位指纹（LLMO-2.2：评测用例/调用关联键，对 raw 输入稳定）。"""
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


class LLMCallLogEmitter:
    """LLM 调用事件发射器（LLMO-2.1，与 AuditEmitter 同构：emit + 异常吞噬 + 可选 hash chain）。"""

    def __init__(
        self,
        path: Optional[str] = None,
        enabled: Optional[bool] = None,
        capture_content: Optional[bool] = None,
        hash_chain: Optional[bool] = None,
        max_memory: Optional[int] = None,
        rotate_bytes: Optional[int] = None,
    ):
        self._path = path if path is not None else _config_scalar(
            "AIOPS_LLM_CALL_LOG_PATH", "llm.call_log.path", "logs/llm_calls.jsonl", str)
        self._enabled = enabled if enabled is not None else _config_bool(
            "AIOPS_LLM_CALL_LOG_ENABLED", "llm.call_log.enabled", True)
        self._capture_content = capture_content if capture_content is not None else _config_bool(
            "AIOPS_LLM_CALL_LOG_CAPTURE_CONTENT", "llm.call_log.capture_content", True)
        self._hash_chain = hash_chain if hash_chain is not None else _config_bool(
            "AIOPS_LLM_CALL_LOG_HASH_CHAIN", "llm.call_log.hash_chain", False)
        self._max_memory = max_memory if max_memory is not None else _config_scalar(
            "AIOPS_LLM_CALL_LOG_MAX_MEMORY", "llm.call_log.max_memory", 100, int)
        self._rotate_bytes = rotate_bytes if rotate_bytes is not None else _config_scalar(
            "AIOPS_LLM_CALL_LOG_ROTATE_BYTES", "llm.call_log.rotate_bytes", 50 * 1024 * 1024, int)

        self._lock = threading.Lock()
        self._memory: Deque[Dict[str, Any]] = deque(maxlen=max(1, self._max_memory))
        self._last_hash: Optional[str] = None
        # 线程级最近调用元数据（LLMO-3.1 parse failure 补记关联用）
        self._tls = threading.local()

    # ── 发射 ──

    def emit_call(
        self,
        *,
        provider: str,
        model: str,
        stage: Optional[str] = None,
        role: Optional[str] = None,
        prompt: str = "",
        response: Optional[str] = None,
        usage: Optional[Dict[str, Any]] = None,
        latency_ms: float = 0.0,
        outcome: str = "success",
        failure_type: Optional[str] = None,
        error_summary: Optional[str] = None,
        alert_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> Optional[LLMCallEvent]:
        """组装并发射一条调用事件。返回事件；未启用/观测失败返回 None（不影响主流程）。"""
        if not self._enabled:
            return None
        try:
            prompt_text = redact_text(prompt or "") if self._capture_content else None
            response_text = (
                redact_text(response) if (self._capture_content and response) else None
            )
            event = LLMCallEvent(
                call_id=str(uuid.uuid4()),
                timestamp=datetime.now().isoformat(timespec="milliseconds"),
                stage=stage,
                role=role,
                provider=provider or "unknown",
                model=model or "unknown",
                prompt_chars=len(prompt or ""),
                prompt_hash=content_hash(prompt or ""),
                prompt=prompt_text[:_PROMPT_TRUNCATE] if prompt_text is not None else None,
                response_chars=len(response or ""),
                response_hash=content_hash(response) if response else "",
                response=response_text[:_RESPONSE_TRUNCATE] if response_text is not None else None,
                usage=LLMUsage(**usage) if usage else None,
                latency_ms=round(float(latency_ms), 2),
                outcome=outcome,
                failure_type=failure_type,
                error_summary=(error_summary or "")[:500] or None,
                alert_id=alert_id,
                pipeline_run_id=pipeline_run_id,
            )
            self._write(event)
            return event
        except Exception as exc:
            metrics.incr("llm.observe.error")
            try:
                logger.logger.warning("LLM 调用事件发射失败 | error={}", exc)
            except Exception:
                pass
            return None

    def emit_parse_failure(
        self,
        *,
        stage: Optional[str] = None,
        role: Optional[str] = None,
        alert_id: Optional[str] = None,
        error: str = "",
        raw_response: Optional[str] = None,
    ) -> Optional[LLMCallEvent]:
        """LLMO-3.1: 引擎层解析失败补记（HTTP 调用本身成功，解析失败是质量事实）。

        关联线程内最近一次调用（copy provider/model，error_summary 引用其 call_id）。
        """
        last = getattr(self._tls, "last_call", None) or {}
        return self.emit_call(
            provider=last.get("provider", "unknown"),
            model=last.get("model", "unknown"),
            stage=stage or last.get("stage"),
            role=role or last.get("role"),
            prompt="",
            response=raw_response,
            usage=None,
            latency_ms=0.0,
            outcome="error",
            failure_type="parse_failure",
            error_summary=f"after call {last.get('call_id', 'unknown')}: {error}",
            alert_id=alert_id,
        )

    # ── 落盘 ──

    def _write(self, event: LLMCallEvent) -> None:
        """LLMO-2.1: append-only 单行 JSONL + 并发锁 + 轮转 + 可选 hash chain + 内存摘要。"""
        with self._lock:
            if self._hash_chain:
                event.prev_hash = self._last_hash
                content = event.model_dump_json(exclude={"prev_hash", "event_hash"})
                event.event_hash = hashlib.sha256(
                    (content + (self._last_hash or "")).encode()
                ).hexdigest()
                self._last_hash = event.event_hash

            self._rotate_if_needed()
            try:
                with open(self._path, "a", encoding="utf-8") as f:
                    f.write(event.model_dump_json() + "\n")
            except OSError as exc:
                metrics.incr("llm.call_log.write_failure")
                logger.logger.warning("LLM 调用日志落盘失败 | path={} | error={}", self._path, exc)

            # 内存环形摘要（LLMO-2.1：不含明文，调试查询用）
            summary = event.model_dump()
            summary.pop("prompt", None)
            summary.pop("response", None)
            self._memory.append(summary)

            # 记录线程级最近调用元数据（parse failure 补记关联）
            self._tls.last_call = {
                "call_id": event.call_id,
                "provider": event.provider,
                "model": event.model,
                "stage": event.stage,
                "role": event.role,
                "prompt_hash": event.prompt_hash,
            }

    def _rotate_if_needed(self) -> None:
        """单文件超过阈值时轮转：当前 → .1（os.replace 原子覆盖，最多保留 1 代）。"""
        try:
            if not os.path.exists(self._path):
                return
            if os.path.getsize(self._path) < self._rotate_bytes:
                return
            os.replace(self._path, self._path + ".1")
        except OSError as exc:
            logger.logger.warning("LLM 调用日志轮转失败 | path={} | error={}", self._path, exc)

    # ── 查询 ──

    def snapshot(self) -> Dict[str, Any]:
        """内存环形摘要（不含明文，调试查询用）。"""
        with self._lock:
            return {
                "path": self._path,
                "enabled": self._enabled,
                "capture_content": self._capture_content,
                "recent": list(self._memory),
            }


# 全局单例（装配于 core/llm_observed.ObservedLLMClient；测试可构造独立实例）
llm_call_log = LLMCallLogEmitter()
