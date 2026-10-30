"""LLM 调用事件日志测试（LLMO-2/3/6）。

覆盖：
- LLMCallEvent schema 与 emit_call 基本发射（LLMO-2.1）
- append-only JSONL + 并发写安全（LLMO-2.1）
- 轮转（LLMO-2.1）
- 明文采集开关（LLMO-6.2）
- redaction 脱敏（LLMO-6.1）
- hash chain（LLMO-2.1 可选）
- 落盘失败不影响主流程（LLMO-2.1）
- emit_parse_failure 补记（LLMO-3.1）
"""

import json
import threading

import pytest

from core.llm_call_log import LLMCallLogEmitter, content_hash
from core.observability import metrics


def _read_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _make_emitter(tmp_path, **kwargs):
    return LLMCallLogEmitter(path=str(tmp_path / "llm_calls.jsonl"), **kwargs)


class TestEmitCall:

    def test_emit_basic_schema(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        event = emitter.emit_call(
            provider="openai", model="gpt-4", stage="analyze", role="diagnosis",
            prompt="分析这个告警", response='{"root_cause": "disk"}',
            usage={"input_tokens": 100, "output_tokens": 50},
            latency_ms=123.45, alert_id="ALT-001",
        )
        assert event is not None
        lines = _read_lines(emitter._path)
        assert len(lines) == 1
        rec = lines[0]
        assert rec["provider"] == "openai"
        assert rec["model"] == "gpt-4"
        assert rec["stage"] == "analyze"
        assert rec["role"] == "diagnosis"
        assert rec["outcome"] == "success"
        assert rec["failure_type"] is None
        assert rec["prompt_hash"] == content_hash("分析这个告警")
        assert rec["prompt_chars"] == 6
        assert rec["usage"]["input_tokens"] == 100
        assert rec["usage"]["output_tokens"] == 50
        assert rec["alert_id"] == "ALT-001"
        assert rec["latency_ms"] == 123.45

    def test_emit_disabled_no_file(self, tmp_path):
        emitter = _make_emitter(tmp_path, enabled=False)
        event = emitter.emit_call(provider="openai", model="gpt-4", prompt="x", response="y")
        assert event is None
        assert not (tmp_path / "llm_calls.jsonl").exists()

    def test_capture_content_off_no_plaintext(self, tmp_path):
        emitter = _make_emitter(tmp_path, capture_content=False)
        emitter.emit_call(provider="openai", model="gpt-4", prompt="secret prompt", response="secret response")
        rec = _read_lines(emitter._path)[0]
        assert rec["prompt"] is None
        assert rec["response"] is None
        assert rec["prompt_hash"] == content_hash("secret prompt")
        assert rec["prompt_chars"] == 13
        # hash/长度仍记录
        assert rec["response_hash"] != ""

    def test_redaction_on_plaintext(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        prompt = '配置: api_key="sk-abc123def456"，请分析'
        emitter.emit_call(provider="openai", model="gpt-4", prompt=prompt, response="ok")
        rec = _read_lines(emitter._path)[0]
        assert "sk-abc123def456" not in rec["prompt"]
        assert 'api_key="***"' in rec["prompt"]

    def test_truncation_keeps_original_chars(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        long_prompt = "x" * 5000
        emitter.emit_call(provider="openai", model="gpt-4", prompt=long_prompt, response="y" * 9000)
        rec = _read_lines(emitter._path)[0]
        assert rec["prompt_chars"] == 5000
        assert len(rec["prompt"]) <= 2000
        assert rec["response_chars"] == 9000
        assert len(rec["response"]) <= 4000

    def test_error_event_with_failure_type(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        emitter.emit_call(
            provider="openai", model="gpt-4", prompt="p",
            outcome="error", failure_type="timeout", error_summary="TimeoutError: x",
        )
        rec = _read_lines(emitter._path)[0]
        assert rec["outcome"] == "error"
        assert rec["failure_type"] == "timeout"
        assert rec["error_summary"] == "TimeoutError: x"

    def test_hash_chain(self, tmp_path):
        emitter = _make_emitter(tmp_path, hash_chain=True)
        e1 = emitter.emit_call(provider="openai", model="gpt-4", prompt="a", response="b")
        e2 = emitter.emit_call(provider="openai", model="gpt-4", prompt="c", response="d")
        assert e1.prev_hash is None
        assert e1.event_hash is not None
        assert e2.prev_hash == e1.event_hash
        recs = _read_lines(emitter._path)
        assert recs[1]["prev_hash"] == recs[0]["event_hash"]

    def test_rotation(self, tmp_path):
        emitter = _make_emitter(tmp_path, rotate_bytes=10)
        emitter.emit_call(provider="openai", model="gpt-4", prompt="a", response="b")
        emitter.emit_call(provider="openai", model="gpt-4", prompt="c", response="d")
        assert (tmp_path / "llm_calls.jsonl.1").exists()
        lines = _read_lines(emitter._path)
        assert len(lines) == 1  # 新文件只有轮转后的事件
        assert len(_read_lines(str(emitter._path) + ".1")) == 1

    def test_concurrent_writes_valid_jsonl(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        def worker(i):
            for j in range(5):
                emitter.emit_call(provider="openai", model="gpt-4", prompt=f"p-{i}-{j}", response="r")
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        lines = _read_lines(emitter._path)
        assert len(lines) == 20

    def test_write_failure_not_fatal(self, tmp_path):
        emitter = LLMCallLogEmitter(path=str(tmp_path / "no_such_dir" / "llm_calls.jsonl"))
        before = metrics.snapshot()["counters"].get("llm.call_log.write_failure", 0)
        event = emitter.emit_call(provider="openai", model="gpt-4", prompt="p", response="r")
        after = metrics.snapshot()["counters"].get("llm.call_log.write_failure", 0)
        # 落盘失败不抛异常，主流程不受影响（事件仍返回，write_failure 计数递增）
        assert event is not None
        assert after == before + 1

    def test_memory_snapshot_no_plaintext(self, tmp_path):
        emitter = _make_emitter(tmp_path, max_memory=5)
        emitter.emit_call(provider="openai", model="gpt-4", prompt="secret-prompt-text", response="secret-response")
        snap = emitter.snapshot()
        assert len(snap["recent"]) == 1
        assert snap["recent"][0].get("prompt") is None  # 摘要不含明文
        assert snap["recent"][0].get("response") is None
        assert snap["recent"][0]["prompt_hash"] != ""
        assert snap["recent"][0]["prompt_chars"] > 0


class TestEmitParseFailure:

    def test_parse_failure_linked_to_last_call(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        # 先模拟一次成功调用（线程内）
        emitter.emit_call(provider="openai", model="gpt-4", prompt="原始 prompt", response="raw")
        last_call_id = emitter._tls.last_call["call_id"]
        # 解析失败补记
        event = emitter.emit_parse_failure(
            stage="analyze", role="diagnosis", alert_id="ALT-1",
            error="JSONDecodeError: Expecting value",
            raw_response="not-json",
        )
        assert event.failure_type == "parse_failure"
        assert event.outcome == "error"
        assert event.provider == "openai"
        assert event.model == "gpt-4"
        assert f"after call {last_call_id}" in event.error_summary
        lines = _read_lines(emitter._path)
        assert len(lines) == 2

    def test_parse_failure_without_last_call(self, tmp_path):
        emitter = _make_emitter(tmp_path)
        event = emitter.emit_parse_failure(stage="analyze", role="diagnosis", error="x")
        assert event.failure_type == "parse_failure"
        assert "after call unknown" in event.error_summary


class TestContentHash:

    def test_stable_and_short(self):
        assert content_hash("abc") == content_hash("abc")
        assert content_hash("abc") != content_hash("abd")
        assert len(content_hash("abc")) == 16

    def test_empty_text(self):
        assert content_hash("") == content_hash("")
