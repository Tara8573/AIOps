import json
from typing import Optional

from core.models import ActionPlan, Alert, EvaluationResult, Feedback
from interfaces.base import IApprovalGate, IExecutor, IKnowledgeBase, ILLMClient, ITicketing
from pipeline import AIOpsPipeline


class RecordingKnowledgeBase(IKnowledgeBase):
    def __init__(self):
        self.feedbacks = []

    def search_experience(self, alert_feature: str):
        return []

    def learn_new_experience(self, feedback: Feedback) -> bool:
        self.feedbacks.append(feedback)
        return True


class AlwaysApproveGate(IApprovalGate):
    def confirm_execution(
        self, alert: Alert, proposal: ActionPlan, evaluation: EvaluationResult
    ) -> bool:
        return True


class StaticLLM(ILLMClient):
    def generate_proposal(self, prompt: str) -> str:
        return json.dumps(
            {
                "root_cause_analysis": "日志目录占用过高",
                "troubleshooting_steps": ["检查磁盘占用", "清理过期日志"],
                "script_content": "find /var/log -name '*.old' -delete",
                "confidence_score": 0.95,
            },
            ensure_ascii=False,
        )


class FailingExecutor(IExecutor):
    def __init__(self, exc: Optional[Exception] = None):
        self.exc = exc

    def execute_script(self, script_content: str, context: dict) -> bool:
        if self.exc:
            raise self.exc
        return False


class RecordingTicketing(ITicketing):
    def __init__(self):
        self.tickets = {}
        self.create_reasons = []

    def create_ticket(
        self, alert: Alert, proposal: ActionPlan, eval_reason: str
    ) -> str:
        ticket_id = f"TICKET-{len(self.tickets) + 1}"
        self.create_reasons.append(eval_reason)
        self.tickets[ticket_id] = {
            "alert": alert,
            "proposal": proposal,
            "eval_reason": eval_reason,
        }
        return ticket_id

    def get_resolution(self, ticket_id: str) -> Optional[Feedback]:
        ticket = self.tickets.get(ticket_id)
        if not ticket:
            return None
        return Feedback(
            alert_id=ticket["alert"].alert_id,
            ticket_id=ticket_id,
            actual_root_cause="人工确认自动修复未生效，日志仍占满磁盘",
            resolution_steps="人工清理指定日志并扩容磁盘",
            is_successful=True,
        )


def _build_pipeline(executor: IExecutor):
    kb = RecordingKnowledgeBase()
    ticketing = RecordingTicketing()
    pipeline = AIOpsPipeline(
        kb=kb,
        llm=StaticLLM(),
        executor=executor,
        approval_gate=AlwaysApproveGate(),
        ticketing=ticketing,
    )
    return pipeline, kb, ticketing


def _alert(alert_id: str = "ALT-EXEC-FAIL") -> Alert:
    return Alert(
        alert_id=alert_id,
        title="Disk Space Critical",
        level="Critical",
        content="/var disk usage > 98%",
    )


def test_execution_false_creates_ticket_and_feedback_can_be_learned():
    pipeline, kb, ticketing = _build_pipeline(FailingExecutor())

    ticket_id = pipeline.process_alert(_alert())
    pipeline.sync_feedbacks([ticket_id])

    assert ticket_id == "TICKET-1"
    assert ticketing.create_reasons == ["自动执行失败: 脚本执行返回失败"]
    assert len(kb.feedbacks) == 1
    assert kb.feedbacks[0].ticket_id == ticket_id


def test_execution_exception_creates_ticket_and_feedback_can_be_learned():
    pipeline, kb, ticketing = _build_pipeline(FailingExecutor(RuntimeError("ssh timeout")))

    ticket_id = pipeline.process_alert(_alert("ALT-EXEC-EXC"))
    pipeline.sync_feedbacks([ticket_id])

    assert ticket_id == "TICKET-1"
    assert ticketing.create_reasons == ["自动执行失败: 脚本执行异常: ssh timeout"]
    assert len(kb.feedbacks) == 1
    assert kb.feedbacks[0].ticket_id == ticket_id
