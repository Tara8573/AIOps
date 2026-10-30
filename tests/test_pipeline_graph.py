"""core/pipeline_graph.py 的端到端测试。

验证 LangGraph 主图的各条路径：
- PASS + 有脚本 + 审批通过 + 执行成功 → 无工单
- PASS + 有脚本 + 审批通过 + 执行失败 → 创建工单
- BLOCK → 创建工单
- 规则引擎快路径 → 无工单
"""

import json
from typing import List, Optional

from core.models import ActionPlan, Alert, EvaluationResult, Feedback
from core.pipeline_graph import run_pipeline
from interfaces.base import IApprovalGate, IExecutor, IKnowledgeBase, ILLMClient, ITicketing
from pipeline import AIOpsPipeline


class MockKB(IKnowledgeBase):
    def search_experience(self, alert_feature: str) -> List[str]:
        return []

    def learn_new_experience(self, feedback: Feedback) -> bool:
        return True


class AlwaysApproveGate(IApprovalGate):
    def confirm_execution(self, alert, proposal, evaluation) -> bool:
        return True


class RejectGate(IApprovalGate):
    def confirm_execution(self, alert, proposal, evaluation) -> bool:
        return False


class StaticLLM(ILLMClient):
    def __init__(self, confidence: float = 0.95, script: Optional[str] = "echo fix"):
        self.confidence = confidence
        self.script = script

    def generate_proposal(self, prompt: str) -> str:
        return json.dumps({
            "root_cause_analysis": "/var disk disk 空间被日志占满导致 usage > 98%",
            "troubleshooting_steps": [
                "df -h 检查 /var disk disk 占用",
                "du -sh /var/log 确认日志目录大小",
                "清理 /var/log 过期日志",
            ],
            "script_content": self.script,
            "confidence_score": self.confidence,
        }, ensure_ascii=False)


class SuccessExecutor(IExecutor):
    def execute_script(self, script_content: str, context: dict) -> bool:
        return True


class FailingExecutor(IExecutor):
    def execute_script(self, script_content: str, context: dict) -> bool:
        return False


class MockTicketing(ITicketing):
    def __init__(self):
        self.tickets = {}
        self.reasons = []

    def create_ticket(self, alert, proposal, eval_reason: str) -> str:
        tid = f"T-{len(self.tickets) + 1}"
        self.tickets[tid] = eval_reason
        self.reasons.append(eval_reason)
        return tid

    def get_resolution(self, ticket_id: str) -> Optional[Feedback]:
        return None


def _build_pipeline(llm=None, executor=None, gate=None, ticketing=None, rule_engine=None):
    pipeline = AIOpsPipeline(
        kb=MockKB(),
        llm=llm or StaticLLM(),
        executor=executor or SuccessExecutor(),
        approval_gate=gate or AlwaysApproveGate(),
        ticketing=ticketing or MockTicketing(),
        rule_engine=rule_engine,
    )
    return pipeline


class _NoRuleEngine:
    """哨兵对象：显式禁用规则引擎。"""
    pass


NO_RULE_ENGINE = _NoRuleEngine()


def _alert(alert_id: str = "ALT-001") -> Alert:
    return Alert(
        alert_id=alert_id,
        title="Disk Space Critical",
        level="Critical",
        content="/var disk usage > 98%",
    )


class TestPipelineGraphPASS:
    def test_pass_with_script_success_no_ticket(self):
        """PASS + 有脚本 + 审批通过 + 执行成功 → 无工单。"""
        pipeline = _build_pipeline()
        state = run_pipeline(alert=_alert(), pipeline=pipeline)
        assert state.get("ticket_id") is None or state.get("ticket_id") == ""
        assert state.get("execution_success") is True

    def test_pass_with_script_execution_fails_creates_ticket(self):
        """PASS + 有脚本 + 执行失败 → 创建工单。"""
        ticketing = MockTicketing()
        pipeline = _build_pipeline(
            executor=FailingExecutor(),
            ticketing=ticketing,
        )
        state = run_pipeline(alert=_alert("ALT-002"), pipeline=pipeline)
        assert state.get("ticket_id") == "T-1"
        assert "自动执行失败" in ticketing.reasons[0]


class TestPipelineGraphBLOCK:
    def test_low_confidence_blocks_creates_ticket(self):
        """置信度低于阈值 → BLOCK → 创建工单。"""
        ticketing = MockTicketing()
        pipeline = _build_pipeline(
            llm=StaticLLM(confidence=0.3),
            ticketing=ticketing,
            rule_engine=None,  # 禁用规则引擎，强制走 AIOps 路径
        )
        state = run_pipeline(alert=_alert("ALT-003"), pipeline=pipeline)
        assert state.get("ticket_id") == "T-1"
        assert "confidence" in ticketing.reasons[0].lower()

    def test_approval_rejected_creates_ticket(self):
        """审批拒绝 → 创建工单。"""
        ticketing = MockTicketing()
        pipeline = _build_pipeline(
            gate=RejectGate(),
            ticketing=ticketing,
            rule_engine=None,  # 禁用规则引擎
        )
        state = run_pipeline(alert=_alert("ALT-004"), pipeline=pipeline)
        assert state.get("ticket_id") == "T-1"
        assert "人工确认未通过" in ticketing.reasons[0]


class TestPipelineGraphNoScript:
    def test_pass_without_script_no_ticket(self):
        """PASS + 无脚本 → 流程止步，无工单。"""
        ticketing = MockTicketing()
        pipeline = _build_pipeline(
            llm=StaticLLM(confidence=0.95, script=None),
            ticketing=ticketing,
        )
        state = run_pipeline(alert=_alert("ALT-005"), pipeline=pipeline)
        assert state.get("ticket_id") is None or state.get("ticket_id") == ""
        assert len(ticketing.reasons) == 0
