"""离线重放评测测试（LLMO-4）。

覆盖：
- report 路径重放：同 raw response 重放分数与在线评分一致（LLMO-4.1 验收）
- calls 路径重放：prompt_hash 匹配 + 不匹配计入 skipped（LLMO-4.1 验收）
- 零 LLM/零网络：重放不依赖任何 LLM 客户端（结构上无 client 参数）
- CaseResult raw_response 落盘（LLMO-4.2）
"""

import json

import pytest

from core.eval_models import CaseResult, DimensionScore, ExpectedOutcome
from core.eval_runner import DEFAULT_TEMPLATE, EvalRunner, PromptTemplateManager
from core.llm_call_log import content_hash
from core.models import Alert
from evals.replay_eval import (
    expected_prompt_hashes,
    load_cases,
    replay_from_calls,
    replay_from_report,
    score_raw_response,
)

FIXTURE = {
    "version": "1.0",
    "cases": [
        {
            "case_id": "disk-001",
            "alert": {
                "alert_id": "EVAL-DISK-001",
                "title": "Disk Space Critical",
                "level": "Critical",
                "content": "disk usage > 98% on /var",
            },
            "expected": {
                "root_cause_keywords": ["磁盘", "disk"],
                "min_troubleshooting_steps": 1,
                "expect_script": False,
                "min_confidence": 0.5,
                "max_confidence": 1.0,
            },
        },
    ],
}


def _write_fixtures(tmp_path):
    p = tmp_path / "fixtures.json"
    p.write_text(json.dumps(FIXTURE, ensure_ascii=False), encoding="utf-8")
    return str(p)


def _good_response():
    return json.dumps({
        "root_cause_analysis": "磁盘 disk 空间占满导致告警",
        "troubleshooting_steps": ["df -h 检查磁盘"],
        "script_content": None,
        "confidence_score": 0.8,
    }, ensure_ascii=False)


class TestReplayFromReport:

    def test_scores_consistent_with_online(self, tmp_path):
        """同 raw response 重放分数与在线评分一致（LLMO-4.1 验收）。"""
        template = DEFAULT_TEMPLATE
        case = FIXTURE["cases"][0]
        raw = _good_response()

        # 在线评分（模拟 EvalRunner 输出），并按 LLMO-4.2 落盘 raw_response
        online = score_raw_response(raw, case, pass_threshold=0.6)
        online_dump = online.model_dump()
        online_dump["raw_response"] = raw
        online_dump["raw_prompt_hash"] = content_hash(raw)

        # 构造 eval 报告（raw_response 落盘，LLMO-4.2）
        report = {
            "run_id": "test",
            "prompt_template": template,
            "train_results": [],
            "test_results": [online_dump],
            "aggregate": {"total_cases": 1, "passed_cases": 1, "failed_cases": 0,
                          "pass_rate": 1.0, "avg_overall_score": online.overall_score,
                          "avg_latency_ms": 0.0},
        }
        report_path = tmp_path / "eval_loop_result.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")

        fixtures = _write_fixtures(tmp_path)
        cases = load_cases(fixtures)
        result = replay_from_report(str(report_path), cases, pass_threshold=0.6)

        assert result["scored_cases"] == 1
        assert result["consistency_check"][0]["consistent"] is True
        assert result["consistency_check"][0]["replay_score"] == online.overall_score

    def test_missing_raw_response_skipped(self, tmp_path):
        report = {
            "prompt_template": "x",
            "train_results": [],
            "test_results": [{"case_id": "disk-001", "alert_title": "t", "alert_level": "Critical",
                              "raw_response": None, "overall_score": 0.0}],
        }
        report_path = tmp_path / "eval_loop_result.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        cases = load_cases(_write_fixtures(tmp_path))
        result = replay_from_report(str(report_path), cases, pass_threshold=0.6)
        assert result["scored_cases"] == 0
        assert result["per_case"][0]["error"] is not None


class TestReplayFromCalls:

    def test_prompt_hash_matching_and_skipped(self, tmp_path):
        """prompt_hash 匹配的计入评分，不匹配的计入 skipped（LLMO-4.1 验收）。"""
        template = DEFAULT_TEMPLATE
        cases = load_cases(_write_fixtures(tmp_path))
        expected = expected_prompt_hashes(template, cases)
        assert len(expected) == 1
        matched_hash = list(expected.keys())[0]

        calls_path = tmp_path / "llm_calls.jsonl"
        events = [
            # 匹配的成功调用
            {"outcome": "success", "prompt_hash": matched_hash, "response": _good_response()},
            # prompt_hash 不匹配 → skipped
            {"outcome": "success", "prompt_hash": "deadbeef" * 2, "response": _good_response()},
            # 失败调用 → skipped
            {"outcome": "error", "prompt_hash": matched_hash, "response": None},
        ]
        with open(calls_path, "w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")

        result = replay_from_calls(str(calls_path), cases, template, pass_threshold=0.6)
        assert result["total_events"] == 3
        assert result["scored_cases"] == 1
        assert result["skipped"] == 2
        assert result["aggregate"]["pass_rate"] == 1.0


class TestZeroNetwork:

    def test_replay_needs_no_llm_client(self):
        """重放函数签名无 llm_client 参数（结构上零 LLM 调用）。"""
        import inspect
        assert "llm_client" not in inspect.signature(replay_from_report).parameters
        assert "llm_client" not in inspect.signature(replay_from_calls).parameters
        assert "llm_client" not in inspect.signature(score_raw_response).parameters


class TestRawResponseCapture:

    def test_eval_runner_captures_raw_response(self, tmp_path, monkeypatch):
        """EvalRunner 落盘 raw_response（LLMO-4.2）。"""
        template = DEFAULT_TEMPLATE
        raw = _good_response()

        class FakeLLM:
            def generate_proposal(self, prompt):
                return raw

        from core.eval_models import EvalCase
        eval_case = EvalCase(**FIXTURE["cases"][0])
        runner = EvalRunner(
            llm_client=FakeLLM(),
            prompt_template=template,
            train_cases=[],
            test_cases=[eval_case],
        )
        report = runner.run()
        test_result = report.test_results[0]
        assert test_result.raw_response == raw
        assert test_result.raw_prompt_hash == content_hash(
            PromptTemplateManager(template).render(
                Alert(**FIXTURE["cases"][0]["alert"]), [], "无可用 skill 输出")
        )
