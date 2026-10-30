"""FeedbackAgent 单元测试。"""

from typing import List, Optional

from agents.feedback_agent import FeedbackAgent
from core.models import ActionPlan, Alert, Feedback
from interfaces.base import IKnowledgeBase, ITicketing


class MockTicketing(ITicketing):
    def __init__(self, resolutions=None):
        self._resolutions = resolutions or {}

    def create_ticket(self, alert, proposal, eval_reason: str) -> str:
        return "T-001"

    def get_resolution(self, ticket_id: str) -> Optional[Feedback]:
        return self._resolutions.get(ticket_id)


class MockKB(IKnowledgeBase):
    def __init__(self):
        self.learned = []

    def search_experience(self, alert_feature: str) -> List[str]:
        return []

    def learn_new_experience(self, feedback: Feedback) -> bool:
        self.learned.append(feedback)
        return True


def _feedback(ticket_id, is_successful=True):
    return Feedback(
        alert_id="ALT-FB-001",
        ticket_id=ticket_id,
        actual_root_cause="磁盘满",
        resolution_steps="清理日志",
        is_successful=is_successful,
    )


class TestFeedbackAgent:

    def test_learn_positive_feedback(self):
        kb = MockKB()
        ticketing = MockTicketing({"T-001": _feedback("T-001", True)})
        agent = FeedbackAgent(ticketing=ticketing, kb=kb)

        result = agent.learn(["T-001"])

        assert result["learned_count"] == 1
        assert len(result["positive_feedbacks"]) == 1
        assert len(result["negative_feedbacks"]) == 0
        assert len(kb.learned) == 1

    def test_learn_negative_feedback(self):
        kb = MockKB()
        ticketing = MockTicketing({"T-002": _feedback("T-002", False)})
        agent = FeedbackAgent(ticketing=ticketing, kb=kb)

        result = agent.learn(["T-002"])

        assert result["learned_count"] == 1
        assert len(result["positive_feedbacks"]) == 0
        assert len(result["negative_feedbacks"]) == 1

    def test_learn_mixed_feedback(self):
        kb = MockKB()
        ticketing = MockTicketing({
            "T-001": _feedback("T-001", True),
            "T-002": _feedback("T-002", False),
            "T-003": _feedback("T-003", True),
        })
        agent = FeedbackAgent(ticketing=ticketing, kb=kb)

        result = agent.learn(["T-001", "T-002", "T-003"])

        assert result["learned_count"] == 3
        assert len(result["positive_feedbacks"]) == 2
        assert len(result["negative_feedbacks"]) == 1

    def test_learn_empty_ticket_ids(self):
        agent = FeedbackAgent(ticketing=MockTicketing(), kb=MockKB())
        result = agent.learn([])
        assert result["learned_count"] == 0

    def test_learn_no_resolution(self):
        agent = FeedbackAgent(ticketing=MockTicketing(), kb=MockKB())
        result = agent.learn(["T-NONEXIST"])
        assert result["learned_count"] == 0
