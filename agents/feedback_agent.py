"""反馈学习 Agent — 负责从工单结果中学习经验。

从 feedback_graph.py 提取逻辑，封装为独立 Agent。
节点从 state 中直接获取 ticketing 和 kb 依赖，不再依赖 state["pipeline"]。

内部图（3 节点）:
  fetch_resolutions → classify → learn
"""

import functools
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from agents.base import BaseAgent
from core.models import Feedback
from core.observability import metrics
from interfaces.base import IKnowledgeBase, ILLMClient, ITicketing


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class FeedbackAgentState(TypedDict):
    ticketing: ITicketing
    kb: IKnowledgeBase
    llm_client: Optional[ILLMClient]
    ticket_ids: List[str]
    resolutions: List[Dict[str, Any]]
    positive_feedbacks: List[Feedback]
    negative_feedbacks: List[Feedback]
    learned_count: int


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def fetch_resolutions(state: FeedbackAgentState) -> dict:
    """拉取所有工单的解决信息。"""
    ticketing = state["ticketing"]
    ticket_ids = state["ticket_ids"]

    resolutions = []
    for tid in ticket_ids:
        feedback = ticketing.get_resolution(tid)
        if feedback:
            resolutions.append({
                "ticket_id": tid,
                "feedback": feedback,
            })

    print(f"[FeedbackAgent] 拉取到 {len(resolutions)} 条工单解决信息")
    return {"resolutions": resolutions}


def classify(state: FeedbackAgentState) -> dict:
    """将反馈分类为正样本和负样本。"""
    resolutions = state["resolutions"]

    positive_feedbacks = []
    negative_feedbacks = []

    for res in resolutions:
        feedback = res["feedback"]
        tid = res["ticket_id"]

        if feedback.is_successful:
            print(f"[FeedbackAgent] 成功猎取票据 {tid} 的最终人工处理手段")
            feedback.feedback_type = "positive"
            positive_feedbacks.append(feedback)
        else:
            print(f"[FeedbackAgent] 票据 {tid} 处置失败，记录为负样本")
            feedback.feedback_type = "negative"
            negative_feedbacks.append(feedback)

    print(
        f"[FeedbackAgent] 分类完成: "
        f"正样本 {len(positive_feedbacks)} 条, "
        f"负样本 {len(negative_feedbacks)} 条"
    )

    return {
        "positive_feedbacks": positive_feedbacks,
        "negative_feedbacks": negative_feedbacks,
    }


def learn(state: FeedbackAgentState) -> dict:
    """将反馈学习进知识库。"""
    kb = state["kb"]
    positive_feedbacks = state["positive_feedbacks"]
    negative_feedbacks = state["negative_feedbacks"]

    learned_count = 0

    for feedback in positive_feedbacks:
        try:
            kb.learn_new_experience(feedback)
            metrics.incr("pipeline.feedback.learned")
            learned_count += 1
            print(f"[FeedbackAgent] 学习正样本: alert_id={feedback.alert_id}")
        except Exception as e:
            print(f"[FeedbackAgent] 学习正样本失败: {e}")

    for feedback in negative_feedbacks:
        try:
            kb.learn_new_experience(feedback)
            metrics.incr("pipeline.feedback.learned_negative")
            learned_count += 1
            print(f"[FeedbackAgent] 学习负样本: alert_id={feedback.alert_id}")
        except Exception as e:
            print(f"[FeedbackAgent] 学习负样本失败: {e}")

    print(f"[FeedbackAgent] 学习完成: 成功 {learned_count} 条")
    return {"learned_count": learned_count}


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _build_feedback_graph():
    builder = StateGraph(FeedbackAgentState)

    builder.add_node("fetch_resolutions", fetch_resolutions)
    builder.add_node("classify", classify)
    builder.add_node("learn", learn)

    builder.add_edge(START, "fetch_resolutions")
    builder.add_edge("fetch_resolutions", "classify")
    builder.add_edge("classify", "learn")
    builder.add_edge("learn", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Agent class
# ---------------------------------------------------------------------------

class FeedbackAgent(BaseAgent):
    """反馈学习 Agent：工单反馈拉取 → 分类 → 知识库学习。"""

    def __init__(
        self,
        ticketing: ITicketing,
        kb: IKnowledgeBase,
        llm_client: Optional[ILLMClient] = None,
    ):
        self.ticketing = ticketing
        self.kb = kb
        self.llm_client = llm_client

    def name(self) -> str:
        return "feedback"

    def build_graph(self):
        return _build_feedback_graph()

    def learn(self, ticket_ids: List[str]) -> Dict[str, Any]:
        """运行反馈学习循环。

        Returns:
            dict with keys: learned_count, positive_feedbacks, negative_feedbacks
        """
        if not ticket_ids:
            return {
                "learned_count": 0,
                "positive_feedbacks": [],
                "negative_feedbacks": [],
            }

        initial_state = {
            "ticketing": self.ticketing,
            "kb": self.kb,
            "llm_client": self.llm_client,
            "ticket_ids": ticket_ids,
            "resolutions": [],
            "positive_feedbacks": [],
            "negative_feedbacks": [],
            "learned_count": 0,
        }

        try:
            return self._invoke(initial_state)
        except Exception as exc:
            print(f"[FeedbackAgent] [ERROR] 反馈学习异常: {exc}")
            return {
                "learned_count": 0,
                "positive_feedbacks": [],
                "negative_feedbacks": [],
            }
