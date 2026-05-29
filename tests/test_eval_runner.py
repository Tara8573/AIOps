"""eval_runner 模块的单元测试"""

import json
import pytest

from core.eval_models import (
    CaseResult,
    DimensionScore,
    EvalCase,
    ExpectedOutcome,
)
from core.eval_runner import (
    DEFAULT_TEMPLATE,
    EvalImproveLoop,
    EvalRunner,
    EvalScorer,
    PromptImprover,
    PromptTemplateManager,
)
from core.models import ActionPlan, Alert
from interfaces.base import ILLMClient


# ---------------------------------------------------------------------------
# 辅助 Mock
# ---------------------------------------------------------------------------


class MockLLM(ILLMClient):
    """返回固定 JSON 的 Mock LLM"""

    def __init__(self, response: dict):
        self._response = response

    def generate_proposal(self, prompt: str) -> str:
        return json.dumps(self._response, ensure_ascii=False)


class RecordingLLM(ILLMClient):
    """记录调用的 Mock LLM"""

    def __init__(self, response: str = '{"root_cause_analysis":"test","troubleshooting_steps":["step1"],"script_content":null,"confidence_score":0.8}'):
        self.calls = []
        self._response = response

    def generate_proposal(self, prompt: str) -> str:
        self.calls.append(prompt)
        return self._response


# ---------------------------------------------------------------------------
# 测试 PromptTemplateManager
# ---------------------------------------------------------------------------


class TestPromptTemplateManager:
    def test_default_template_renders_correctly(self):
        alert = Alert(
            alert_id="t1", title="Test Alert", level="Critical", content="disk full"
        )
        mgr = PromptTemplateManager()
        rendered = mgr.render(alert, ["sop1"], "no skills")
        assert "Test Alert" in rendered
        assert "Critical" in rendered
        assert "disk full" in rendered
        assert "sop1" in rendered

    def test_custom_template_rendered(self):
        alert = Alert(alert_id="t1", title="X", level="Info", content="Y")
        mgr = PromptTemplateManager("title={alert_title} level={alert_level} content={alert_content} sops={history_sops} skills={skill_context}")
        rendered = mgr.render(alert, [], "")
        assert "title=X" in rendered
        assert "level=Info" in rendered
        assert "content=Y" in rendered

    def test_validate_placeholders_valid(self):
        assert PromptTemplateManager.validate_placeholders(
            "{alert_title} {alert_level} {alert_content} {history_sops} {skill_context}"
        )

    def test_validate_placeholders_missing(self):
        assert not PromptTemplateManager.validate_placeholders(
            "{alert_title} {alert_level}"
        )


# ---------------------------------------------------------------------------
# 测试 EvalScorer
# ---------------------------------------------------------------------------


class TestEvalScorer:
    def test_root_cause_keyword_hit(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="磁盘空间不足导致服务异常",
            troubleshooting_steps=["检查磁盘"],
            confidence_score=0.8,
        )
        expected = ExpectedOutcome(root_cause_keywords=["磁盘", "disk", "空间"])
        dims = scorer.score(plan, expected)
        rc = next(d for d in dims if d.name == "root_cause_relevance")
        assert rc.score > 0.3
        assert rc.passed

    def test_root_cause_no_keywords(self):
        scorer = EvalScorer()
        plan = ActionPlan(root_cause_analysis="anything", confidence_score=0.8)
        expected = ExpectedOutcome()
        dims = scorer.score(plan, expected)
        rc = next(d for d in dims if d.name == "root_cause_relevance")
        assert rc.score == 0.5

    def test_step_quality_enough_steps(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test",
            troubleshooting_steps=["df -h 检查磁盘", "du -sh /var/log 查找大文件"],
            confidence_score=0.8,
        )
        expected = ExpectedOutcome(
            min_troubleshooting_steps=2, step_keywords=["df", "du"]
        )
        dims = scorer.score(plan, expected)
        step = next(d for d in dims if d.name == "step_quality")
        assert step.score > 0.5

    def test_step_quality_too_few_steps(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test",
            troubleshooting_steps=["check"],
            confidence_score=0.8,
        )
        expected = ExpectedOutcome(min_troubleshooting_steps=3)
        dims = scorer.score(plan, expected)
        step = next(d for d in dims if d.name == "step_quality")
        assert step.score < 0.5

    def test_confidence_in_range(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test", confidence_score=0.7
        )
        expected = ExpectedOutcome(min_confidence=0.5, max_confidence=0.9)
        dims = scorer.score(plan, expected)
        conf = next(d for d in dims if d.name == "confidence_calibration")
        assert conf.score == 1.0

    def test_confidence_out_of_range(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test", confidence_score=0.95
        )
        expected = ExpectedOutcome(min_confidence=0.4, max_confidence=0.8)
        dims = scorer.score(plan, expected)
        conf = next(d for d in dims if d.name == "confidence_calibration")
        assert conf.score < 1.0

    def test_script_expected_but_missing(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test",
            troubleshooting_steps=["step"],
            script_content=None,
            confidence_score=0.8,
        )
        expected = ExpectedOutcome(expect_script=True, script_keywords=["df"])
        dims = scorer.score(plan, expected)
        script = next(d for d in dims if d.name == "script_relevance")
        assert script.score == 0.0
        assert not script.passed

    def test_script_not_expected(self):
        scorer = EvalScorer()
        plan = ActionPlan(
            root_cause_analysis="test",
            troubleshooting_steps=["step"],
            script_content=None,
            confidence_score=0.8,
        )
        expected = ExpectedOutcome(expect_script=False)
        dims = scorer.score(plan, expected)
        script = next(d for d in dims if d.name == "script_relevance")
        assert script.score == 1.0
        assert script.passed

    def test_overall_score_weighted(self):
        scorer = EvalScorer()
        dims = [
            DimensionScore(name="root_cause_relevance", score=1.0, passed=True),
            DimensionScore(name="step_quality", score=1.0, passed=True),
            DimensionScore(name="confidence_calibration", score=1.0, passed=True),
            DimensionScore(name="script_relevance", score=1.0, passed=True),
        ]
        assert scorer.overall_score(dims) == 1.0

    def test_overall_score_partial(self):
        scorer = EvalScorer()
        dims = [
            DimensionScore(name="root_cause_relevance", score=1.0, passed=True),
            DimensionScore(name="step_quality", score=0.0, passed=False),
            DimensionScore(name="confidence_calibration", score=1.0, passed=True),
            DimensionScore(name="script_relevance", score=0.0, passed=False),
        ]
        # 0.35*1 + 0.25*0 + 0.15*1 + 0.25*0 = 0.50
        assert scorer.overall_score(dims) == 0.5


# ---------------------------------------------------------------------------
# 测试 EvalRunner
# ---------------------------------------------------------------------------


class TestEvalRunner:
    def _make_case(self, case_id="t1") -> EvalCase:
        return EvalCase(
            case_id=case_id,
            alert={
                "alert_id": "A1",
                "title": "Disk Full",
                "level": "Critical",
                "content": "/var full",
            },
            expected=ExpectedOutcome(
                root_cause_keywords=["磁盘", "disk"],
                min_troubleshooting_steps=1,
                expect_script=True,
                script_keywords=["find", "delete"],
            ),
        )

    def test_runner_returns_report(self):
        mock_llm = MockLLM(
            {
                "root_cause_analysis": "磁盘空间满",
                "troubleshooting_steps": ["df -h", "清理日志"],
                "script_content": "find /var/log -name '*.gz' -delete",
                "confidence_score": 0.85,
            }
        )
        case = self._make_case()
        runner = EvalRunner(
            llm_client=mock_llm,
            prompt_template=DEFAULT_TEMPLATE,
            train_cases=[case],
            test_cases=[case],
        )
        report = runner.run()
        assert report.run_id
        assert len(report.train_results) == 1
        assert len(report.test_results) == 1
        assert report.aggregate.total_cases == 1

    def test_runner_handles_llm_error(self):
        class ErrorLLM(ILLMClient):
            def generate_proposal(self, prompt: str) -> str:
                raise RuntimeError("LLM unavailable")

        case = self._make_case()
        runner = EvalRunner(
            llm_client=ErrorLLM(),
            prompt_template=DEFAULT_TEMPLATE,
            train_cases=[case],
            test_cases=[],
        )
        report = runner.run()
        assert len(report.train_results) == 1
        assert report.train_results[0].error is not None
        assert report.train_results[0].overall_score == 0.0

    def test_runner_handles_bad_json(self):
        class BadJsonLLM(ILLMClient):
            def generate_proposal(self, prompt: str) -> str:
                return "this is not json"

        case = self._make_case()
        runner = EvalRunner(
            llm_client=BadJsonLLM(),
            prompt_template=DEFAULT_TEMPLATE,
            train_cases=[case],
            test_cases=[],
        )
        report = runner.run()
        assert report.train_results[0].error is not None


# ---------------------------------------------------------------------------
# 测试 PromptImprover
# ---------------------------------------------------------------------------


class TestPromptImprover:
    def test_improver_calls_llm_with_failure_context(self):
        recorder = RecordingLLM(response="改进后的模板: {alert_title} {alert_level} {alert_content} {history_sops} {skill_context}")
        improver = PromptImprover(recorder)
        failed = [
            CaseResult(
                case_id="f1",
                alert_title="Disk Full",
                alert_level="Critical",
                dimensions=[
                    DimensionScore(name="root_cause_relevance", score=0.1, passed=False, detail="低分"),
                ],
                overall_score=0.2,
                passed=False,
                generated_plan={"root_cause_analysis": "磁盘空间满"},
            )
        ]
        result = improver.improve("old template {alert_title} {alert_level} {alert_content} {history_sops} {skill_context}", failed)
        assert len(recorder.calls) == 1
        assert "Disk Full" in recorder.calls[0]
        assert "{alert_title}" in result

    def test_improver_fallback_on_missing_placeholders(self):
        # LLM 返回的模板缺少占位符
        recorder = RecordingLLM(response="改进后没有占位符的模板")
        improver = PromptImprover(recorder)
        original = "original {alert_title} {alert_level} {alert_content} {history_sops} {skill_context}"
        result = improver.improve(original, [])
        assert result == original  # 回退到原模板

    def test_improver_strips_markdown(self):
        recorder = RecordingLLM(response="```\n{alert_title} {alert_level} {alert_content} {history_sops} {skill_context}\n```")
        improver = PromptImprover(recorder)
        result = improver.improve("old {alert_title} {alert_level} {alert_content} {history_sops} {skill_context}", [])
        assert not result.startswith("```")


# ---------------------------------------------------------------------------
# 测试 EvalImproveLoop
# ---------------------------------------------------------------------------


class TestEvalImproveLoop:
    def _make_cases(self, n=4) -> list:
        return [
            EvalCase(
                case_id=f"c{i}",
                alert={
                    "alert_id": f"A{i}",
                    "title": f"Alert {i}",
                    "level": "Critical",
                    "content": f"content {i}",
                },
                expected=ExpectedOutcome(
                    root_cause_keywords=["test"],
                    min_troubleshooting_steps=1,
                ),
            )
            for i in range(n)
        ]

    def test_loop_runs_multiple_iterations(self):
        iteration_count = [0]

        class CountingLLM(ILLMClient):
            def generate_proposal(self, prompt: str) -> str:
                iteration_count[0] += 1
                return json.dumps(
                    {
                        "root_cause_analysis": "test root cause",
                        "troubleshooting_steps": ["step1"],
                        "script_content": None,
                        "confidence_score": 0.8,
                    },
                    ensure_ascii=False,
                )

        cases = self._make_cases(4)
        loop = EvalImproveLoop(
            llm_client=CountingLLM(),
            eval_cases=cases,
            max_iterations=2,
            train_ratio=0.5,
        )
        result = loop.run()
        assert result.iterations >= 1
        assert result.best_prompt
        # 每轮每个 case 调一次 LLM + 改进时调一次
        assert iteration_count[0] > 0

    def test_loop_early_stop_on_all_pass(self):
        cases = self._make_cases(4)
        # 一个总是返回高分结果的 LLM
        mock = MockLLM(
            {
                "root_cause_analysis": "test root cause analysis",
                "troubleshooting_steps": ["step1"],
                "script_content": None,
                "confidence_score": 0.8,
            }
        )
        loop = EvalImproveLoop(
            llm_client=mock,
            eval_cases=cases,
            max_iterations=5,
            train_ratio=0.5,
            pass_threshold=0.0,  # 全部通过
        )
        result = loop.run()
        # 全部通过时应只跑 1 轮
        assert result.iterations == 1

    def test_loop_uses_test_score_for_best(self):
        cases = self._make_cases(6)
        mock = MockLLM(
            {
                "root_cause_analysis": "test",
                "troubleshooting_steps": ["step1"],
                "script_content": None,
                "confidence_score": 0.8,
            }
        )
        loop = EvalImproveLoop(
            llm_client=mock,
            eval_cases=cases,
            max_iterations=2,
            train_ratio=0.5,
            early_stop_delta=0.0,  # 禁用提前停止
        )
        result = loop.run()
        assert result.best_iteration >= 1
        assert result.best_test_avg_score >= 0
