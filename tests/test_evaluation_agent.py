"""EvaluationAgent 单元测试。"""

import json
from typing import List, Optional

from agents.evaluation_agent import EvaluationAgent
from core.models import ActionPlan, Alert, EvaluationResult, LLMProposal


def _alert():
    return Alert(
        alert_id="ALT-EVAL-001",
        title="Disk Space Critical",
        level="Critical",
        content="/var disk usage > 98%",
    )


def _proposal(confidence: float = 0.9, script: str = "echo fix") -> LLMProposal:
    return LLMProposal(
        alert_id="ALT-EVAL-001",
        plan=ActionPlan(
            root_cause_analysis="/var 磁盘空间被日志占满导致 usage > 98%",
            troubleshooting_steps=["df -h 检查", "du -sh /var/log", "清理过期日志"],
            script_content=script,
            confidence_score=confidence,
        ),
    )


class TestEvaluationAgent:

    def test_evaluate_pass_high_confidence(self):
        agent = EvaluationAgent(min_confidence=0.7)
        result = agent.evaluate(_alert(), _proposal(confidence=0.9))
        assert result.is_passed is True
        assert result.risk_level == "Low"

    def test_evaluate_block_low_confidence(self):
        agent = EvaluationAgent(min_confidence=0.7)
        result = agent.evaluate(_alert(), _proposal(confidence=0.3))
        assert result.is_passed is False
        assert "confidence" in result.reason

    def test_evaluate_block_dangerous_script(self):
        agent = EvaluationAgent(min_confidence=0.7)
        result = agent.evaluate(_alert(), _proposal(script="rm -rf /"))
        assert result.is_passed is False
        assert result.risk_level == "High"

    def test_evaluate_with_retry_passes_on_second_attempt(self):
        agent = EvaluationAgent(min_confidence=0.7)
        call_count = 0

        def diagnosis_fn(retry_feedback=None, prev_plan=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"proposal": _proposal(confidence=0.6)}
            return {"proposal": _proposal(confidence=0.9)}

        result = agent.evaluate_with_retry(
            alert=_alert(),
            diagnosis_fn=diagnosis_fn,
            max_retries=1,
            min_retry_confidence=0.5,
        )
        assert result["eval_result"].is_passed is True
        assert call_count == 2

    def test_evaluate_with_retry_blocks_when_max_retries_exceeded(self):
        agent = EvaluationAgent(min_confidence=0.7)

        def diagnosis_fn(retry_feedback=None, prev_plan=None):
            return {"proposal": _proposal(confidence=0.6)}

        result = agent.evaluate_with_retry(
            alert=_alert(),
            diagnosis_fn=diagnosis_fn,
            max_retries=1,
            min_retry_confidence=0.5,
        )
        assert result["eval_result"].is_passed is False

    def test_evaluate_with_retry_no_retry_below_min_confidence(self):
        agent = EvaluationAgent(min_confidence=0.7)
        call_count = 0

        def diagnosis_fn(retry_feedback=None, prev_plan=None):
            nonlocal call_count
            call_count += 1
            return {"proposal": _proposal(confidence=0.2)}

        result = agent.evaluate_with_retry(
            alert=_alert(),
            diagnosis_fn=diagnosis_fn,
            max_retries=2,
            min_retry_confidence=0.5,
        )
        assert result["eval_result"].is_passed is False
        assert call_count == 1
