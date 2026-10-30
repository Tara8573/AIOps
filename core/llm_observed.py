"""LLM 调用观测包装层（LLMO-1/3/5/6，规格：docs/LLM调用可观测性与质量评估开发规格说明书.md）。

设计原则（规格 2.3）：
- 观测在中间层：ObservedLLMClient 实现 ILLMClient，零侵入 provider 适配器
- 装配顺序：熔断/配额链 → ObservedLLMClient → provider client（观测层最内层，
  计时 = 模型调用耗时不含配额等待；get_last_usage 直达 provider client）
- disjoint usage：input_tokens 不含 cached_read_tokens
- 观测失败不影响主流程：emit/打点异常吞掉并计数，调用结果（含异常）原样透传
"""

import json
import threading
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Optional

import httpx

from core.llm_call_log import llm_call_log
from core.models import LLMUsage
from core.observability import metrics
from interfaces.base import ILLMClient


# ---------------------------------------------------------------------------
# 失败分类（LLMO-3.1）
# ---------------------------------------------------------------------------

class LLMFailureType(str, Enum):
    """LLM 调用失败类型枚举（规格 5.2 failure_type 取值）"""
    HTTP_ERROR = "http_error"
    RATE_LIMITED = "rate_limited"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    PROVIDER_EXCEPTION = "provider_exception"
    PARSE_FAILURE = "parse_failure"
    UNKNOWN = "unknown"


def classify_llm_failure(exc: BaseException) -> LLMFailureType:
    """异常 → 失败类型归类。

    映射（规格 LLMO-3.1）：
    - httpx.HTTPStatusError → HTTP_ERROR（429 → RATE_LIMITED）
    - httpx.TimeoutException → TIMEOUT
    - httpx.RequestError（其余）→ NETWORK_ERROR
    - json.JSONDecodeError → PARSE_FAILURE（引擎层解析失败）
    - 其他异常 → PROVIDER_EXCEPTION
    禁止字符串匹配异常 message 归类（除 HTTP status code）。
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = getattr(exc.response, "status_code", None)
        return LLMFailureType.RATE_LIMITED if status == 429 else LLMFailureType.HTTP_ERROR
    if isinstance(exc, httpx.TimeoutException):
        return LLMFailureType.TIMEOUT
    if isinstance(exc, httpx.RequestError):
        return LLMFailureType.NETWORK_ERROR
    if isinstance(exc, json.JSONDecodeError):
        return LLMFailureType.PARSE_FAILURE
    if isinstance(exc, Exception):
        return LLMFailureType.PROVIDER_EXCEPTION
    return LLMFailureType.UNKNOWN


# ---------------------------------------------------------------------------
# usage 解析（LLMO-2.2，disjoint 约定）
# ---------------------------------------------------------------------------

def parse_openai_usage(raw: Optional[Dict[str, Any]]) -> Optional[LLMUsage]:
    """OpenAI usage dict → disjoint LLMUsage。

    约定：prompt_tokens 含缓存命中部分，input_tokens = prompt_tokens - cached_read_tokens。
    各字段缺失默认 0；usage 整体缺失（兼容网关不返回）返回 None。
    """
    if not raw or not isinstance(raw, dict):
        return None
    prompt_tokens = int(raw.get("prompt_tokens") or 0)
    completion_tokens = int(raw.get("completion_tokens") or 0)
    prompt_details = raw.get("prompt_tokens_details")
    cached = int(prompt_details.get("cached_tokens") or 0) if isinstance(prompt_details, dict) else 0
    completion_details = raw.get("completion_tokens_details")
    reasoning = int(completion_details.get("reasoning_tokens") or 0) if isinstance(completion_details, dict) else 0
    return LLMUsage(
        input_tokens=max(prompt_tokens - cached, 0),
        output_tokens=completion_tokens,
        cached_read_tokens=cached,
        reasoning_tokens=reasoning,
    )


# ---------------------------------------------------------------------------
# 调用上下文（LLMO-5.2）
# ---------------------------------------------------------------------------

class LLMCallContext:
    """调用上下文（threading.local），调用方在 generate_proposal 外层设置。

    并发池 worker 线程各自隔离；LLM 调用为同步 httpx（同线程），包装层读取时序安全。
    若未来 LLM 调用改异步（asyncio），需换 contextvars.ContextVar（规格 8 风险项）。
    """

    _local = threading.local()

    @classmethod
    def set(cls, **fields: Any) -> None:
        cls._local.fields = fields

    @classmethod
    def clear(cls) -> None:
        cls._local.fields = None

    @classmethod
    def get(cls) -> Dict[str, Any]:
        return getattr(cls._local, "fields", None) or {}


@contextmanager
def llm_call_context(
    *,
    alert_id: Optional[str] = None,
    stage: Optional[str] = None,
    role: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
):
    """LLMO-5.2: 调用上下文管理器。调用方用法（不新增方法参数，不改既有签名）：

        with llm_call_context(alert_id=..., stage="analyze", role="diagnosis"):
            text = llm_client.generate_proposal(prompt)
    """
    LLMCallContext.set(
        alert_id=alert_id, stage=stage, role=role, pipeline_run_id=pipeline_run_id
    )
    try:
        yield
    finally:
        LLMCallContext.clear()


# ---------------------------------------------------------------------------
# 观测包装器（LLMO-1.1）
# ---------------------------------------------------------------------------

class ObservedLLMClient(ILLMClient):
    """观测包装器（LLMO-1.1，最内层紧贴 provider client，对上层透明）。

    - 计时 = 模型调用耗时（不含配额等待，LLMO-1.2）
    - 每次调用一条事件（LLMO-2.1）
    - 失败枚举分类，异常原样上抛（LLMO-3.1）
    - usage disjoint 拆分 + GenAI semconv 打点（LLMO-2.2/5.1）
    - stage/role/alert_id 从 LLMCallContext 合并（LLMO-5.2）
    """

    def __init__(self, base: ILLMClient):
        self.base = base

    @property
    def provider(self) -> str:
        return getattr(self.base, "provider_name", "unknown") or "unknown"

    @property
    def model(self) -> str:
        return getattr(self.base, "model", None) or "unknown"

    def get_last_usage(self) -> Optional[dict]:
        """透传 provider client 的只读 usage 能力（LLMO-2.2，duck-typing）。"""
        fn = getattr(self.base, "get_last_usage", None)
        return fn() if callable(fn) else None

    def generate_proposal(self, prompt: str) -> str:
        start = time.perf_counter()
        response_text: Optional[str] = None
        failure_type: Optional[str] = None
        error_summary: Optional[str] = None
        outcome = "success"
        try:
            response_text = self.base.generate_proposal(prompt)
            return response_text
        except Exception as exc:
            outcome = "error"
            failure_type = classify_llm_failure(exc).value
            error_summary = f"{type(exc).__name__}: {exc}"[:500]
            raise
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            try:
                ctx = LLMCallContext.get()
                usage = parse_openai_usage(self.get_last_usage()) if outcome == "success" else None
                llm_call_log.emit_call(
                    provider=self.provider,
                    model=self.model,
                    stage=ctx.get("stage"),
                    role=ctx.get("role"),
                    prompt=prompt,
                    response=response_text,
                    usage=usage.model_dump() if usage else None,
                    latency_ms=latency_ms,
                    outcome=outcome,
                    failure_type=failure_type,
                    error_summary=error_summary,
                    alert_id=ctx.get("alert_id"),
                    pipeline_run_id=ctx.get("pipeline_run_id"),
                )
                # LLMO-5.1: GenAI semconv 指标（MetricsCollector 无 label，维度编码进指标名）
                metrics.observe("genai.client.operation.duration.ms", latency_ms)
                if usage:
                    metrics.incr("genai.client.token.usage.input", usage.input_tokens)
                    metrics.incr("genai.client.token.usage.output", usage.output_tokens)
                    metrics.incr("genai.client.token.usage.cache_read", usage.cached_read_tokens)
                    metrics.incr("genai.client.token.usage.reasoning", usage.reasoning_tokens)
            except Exception:
                # 观测失败不影响主流程（规格 2.3 原则 6）
                metrics.incr("llm.observe.error")
