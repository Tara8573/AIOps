"""LLM 调用观测包装层测试（LLMO-1/2.2/3.1/5.2）。

覆盖：
- ObservedLLMClient 透传（返回值/异常，LLMO-1.1）
- 成功/失败调用事件发射（LLMO-1.1/2.1）
- 失败分类（LLMO-3.1：429→rate_limited、超时、网络、provider 异常）
- usage disjoint 解析 + 缺失降级（LLMO-2.2）
- 观测层故障注入不影响主调用（LLMO-1.1 验收）
- LLMCallContext 上下文合并（LLMO-5.2）
- 并发 usage 不串线（LLMO-2.2 验收）
- CognitiveEngine 解析失败补记（LLMO-3.1）
"""

import json
import threading

import httpx
import pytest

import core.llm_engine as llm_engine_module
import core.llm_observed as llm_observed_module
from core.llm_call_log import LLMCallLogEmitter, content_hash
from core.llm_engine import CognitiveEngine
from core.llm_observed import (
    LLMCallContext,
    LLMFailureType,
    ObservedLLMClient,
    classify_llm_failure,
    llm_call_context,
    parse_openai_usage,
)
from core.models import Alert
from core.observability import metrics
from interfaces.base import ILLMClient


class FakeLLMClient(ILLMClient):
    """可编程 fake：返回值/异常/usage 可控。"""

    def __init__(self, result="ok", exc=None, usage=None, provider_name="openai", model="fake-model"):
        self.result = result
        self.exc = exc
        self._usage = usage
        self.provider_name = provider_name
        self.model = model
        self.calls = []

    def generate_proposal(self, prompt: str) -> str:
        self.calls.append(prompt)
        self._tls_usage = self._usage
        if self.exc is not None:
            raise self.exc
        return self.result

    def get_last_usage(self):
        return getattr(self, "_tls_usage", None)


@pytest.fixture()
def captured_log(tmp_path, monkeypatch):
    """替换全局 llm_call_log 为独立实例（含 engine 模块的绑定），便于断言。"""
    emitter = LLMCallLogEmitter(path=str(tmp_path / "calls.jsonl"))
    monkeypatch.setattr(llm_observed_module, "llm_call_log", emitter)
    monkeypatch.setattr(llm_engine_module, "llm_call_log", emitter)
    yield emitter


def _events(emitter):
    return list(emitter.snapshot()["recent"])


class TestObservedLLMClientPassthrough:

    def test_return_value_passthrough(self, captured_log):
        client = ObservedLLMClient(FakeLLMClient(result='{"a": 1}'))
        assert client.generate_proposal("prompt") == '{"a": 1}'

    def test_exception_passthrough_same_type(self, captured_log):
        exc = httpx.TimeoutException("timed out")
        client = ObservedLLMClient(FakeLLMClient(exc=exc))
        with pytest.raises(httpx.TimeoutException):
            client.generate_proposal("prompt")


class TestCallEvents:

    def test_success_event_fields(self, captured_log):
        client = ObservedLLMClient(FakeLLMClient(
            result="resp",
            usage={"prompt_tokens": 100, "completion_tokens": 40,
                   "prompt_tokens_details": {"cached_tokens": 40}},
        ))
        with llm_call_context(alert_id="ALT-9", stage="analyze", role="diagnosis"):
            client.generate_proposal("my prompt")
        events = _events(captured_log)
        assert len(events) == 1
        ev = events[0]
        assert ev["provider"] == "openai"
        assert ev["model"] == "fake-model"
        assert ev["stage"] == "analyze"
        assert ev["role"] == "diagnosis"
        assert ev["outcome"] == "success"
        assert ev["failure_type"] is None
        assert ev["alert_id"] == "ALT-9"
        assert ev["prompt_hash"] == content_hash("my prompt")
        assert ev["usage"]["input_tokens"] == 60  # disjoint: 100 - cached(40)
        assert ev["usage"]["cached_read_tokens"] == 40
        assert ev["usage"]["output_tokens"] == 40
        assert ev["latency_ms"] >= 0

    def test_no_context_fields_null(self, captured_log):
        client = ObservedLLMClient(FakeLLMClient(result="resp"))
        client.generate_proposal("p")
        ev = _events(captured_log)[0]
        assert ev["stage"] is None
        assert ev["role"] is None
        assert ev["alert_id"] is None

    def test_error_event_failure_type(self, captured_log):
        client = ObservedLLMClient(FakeLLMClient(exc=httpx.ConnectError("refused")))
        with pytest.raises(httpx.ConnectError):
            client.generate_proposal("p")
        ev = _events(captured_log)[0]
        assert ev["outcome"] == "error"
        assert ev["failure_type"] == "timeout" or ev["failure_type"] == "network_error"
        # ConnectError 是 RequestError 子类且非 Timeout → network_error
        assert ev["failure_type"] == "network_error"


class TestFailureClassification:

    def test_429_rate_limited(self):
        req = httpx.Request("POST", "http://x")
        resp = httpx.Response(429, request=req)
        assert classify_llm_failure(httpx.HTTPStatusError("429", request=req, response=resp)) == LLMFailureType.RATE_LIMITED

    def test_500_http_error(self):
        req = httpx.Request("POST", "http://x")
        resp = httpx.Response(500, request=req)
        assert classify_llm_failure(httpx.HTTPStatusError("500", request=req, response=resp)) == LLMFailureType.HTTP_ERROR

    def test_timeout(self):
        assert classify_llm_failure(httpx.TimeoutException("t")) == LLMFailureType.TIMEOUT

    def test_network_error(self):
        assert classify_llm_failure(httpx.ConnectError("c")) == LLMFailureType.NETWORK_ERROR

    def test_parse_failure(self):
        assert classify_llm_failure(json.JSONDecodeError("e", "doc", 0)) == LLMFailureType.PARSE_FAILURE

    def test_provider_exception(self):
        assert classify_llm_failure(ValueError("v")) == LLMFailureType.PROVIDER_EXCEPTION


class TestUsageParsing:

    def test_disjoint_split(self):
        usage = parse_openai_usage({
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "prompt_tokens_details": {"cached_tokens": 30},
            "completion_tokens_details": {"reasoning_tokens": 10},
        })
        assert usage.input_tokens == 70  # 100 - 30（disjoint）
        assert usage.cached_read_tokens == 30
        assert usage.output_tokens == 50
        assert usage.reasoning_tokens == 10

    def test_missing_fields_default_zero(self):
        usage = parse_openai_usage({"prompt_tokens": 10, "completion_tokens": 5})
        assert usage.input_tokens == 10
        assert usage.cached_read_tokens == 0
        assert usage.reasoning_tokens == 0

    def test_usage_missing_returns_none(self):
        assert parse_openai_usage(None) is None
        assert parse_openai_usage({}) is None


class TestUsageIsolation:

    def test_concurrent_usage_not_crossed(self, captured_log):
        # 每线程独立 usage，4 线程 × 3 次调用，事件 usage 必须与本线程一致
        def worker(token_base):
            client = ObservedLLMClient(FakeLLMClient(
                result="resp", usage={"prompt_tokens": token_base, "completion_tokens": 1},
            ))
            for _ in range(3):
                with llm_call_context(stage="analyze", role="diagnosis"):
                    client.generate_proposal("p")

        threads = [threading.Thread(target=worker, args=(100 * (i + 1),)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        events = _events(captured_log)
        assert len(events) == 12
        bases = {ev["usage"]["input_tokens"] for ev in events}
        assert bases == {100, 200, 300, 400}  # 每线程 usage 隔离


class TestObserveFailureNotFatal:

    def test_emit_failure_call_still_succeeds(self, tmp_path, monkeypatch):
        # 发射器注入故障：写盘路径为目录 → 落盘失败，但 emit 内部吞掉
        emitter = LLMCallLogEmitter(path=str(tmp_path))
        monkeypatch.setattr(llm_observed_module, "llm_call_log", emitter)
        before = metrics.snapshot()["counters"].get("llm.observe.error", 0)
        client = ObservedLLMClient(FakeLLMClient(result="ok"))
        assert client.generate_proposal("p") == "ok"  # 主调用不受影响
        after = metrics.snapshot()["counters"].get("llm.observe.error", 0)
        assert after >= before

    def test_metrics_semconv_on_success(self, captured_log):
        before = metrics.snapshot()["counters"].get("genai.client.token.usage.input", 0)
        client = ObservedLLMClient(FakeLLMClient(
            result="r", usage={"prompt_tokens": 50, "completion_tokens": 20},
        ))
        client.generate_proposal("p")
        after = metrics.snapshot()["counters"].get("genai.client.token.usage.input", 0)
        assert after == before + 50
        latencies = metrics.snapshot()["latencies"].get("genai.client.operation.duration.ms")
        assert latencies is not None and latencies["count"] >= 1


class TestEngineParseFailureSupplement:

    def test_engine_parse_failure_supplement(self, tmp_path, captured_log):
        class BadJSONLLM(ILLMClient):
            def generate_proposal(self, prompt):
                return "not-json{{"

        engine = CognitiveEngine(BadJSONLLM())
        alert = Alert(alert_id="ALT-PF", title="Disk full", level="Critical", content="usage 99%")
        proposal = engine.analyze_alert(alert, history_sops=[], intent_category="disk")
        # 引擎返回 fallback plan
        assert "解析LLM响应失败" in proposal.plan.root_cause_analysis
        # 解析失败补记事件（stage=analyze, role=diagnosis, failure_type=parse_failure）
        events = _events(captured_log)
        assert len(events) >= 1
        parse_failures = [e for e in events if e.get("failure_type") == "parse_failure"]
        assert len(parse_failures) == 1
        pf = parse_failures[0]
        assert pf["stage"] == "analyze"
        assert pf["role"] == "diagnosis"
        assert pf["alert_id"] == "ALT-PF"
        assert pf["outcome"] == "error"
        # 明文在文件中（快照摘要不含明文），读文件验证 raw response 被补记
        with open(captured_log._path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        pf_file = [e for e in lines if e["failure_type"] == "parse_failure"][0]
        assert "not-json" in pf_file["response"]
        assert pf_file["response_chars"] == len("not-json{{")
