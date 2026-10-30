"""诊断 Agent — 负责知识检索、Skill 执行和 LLM 推理。

从 pipeline_graph 的 knowledge/skills 节点和 eval_graph 的 reason 节点提取逻辑，
封装为独立 Agent，拥有自己的 LLM 实例和内部 LangGraph 子图。

内部图（3 节点）:
  retrieve_knowledge → run_skills → reason
"""

import functools
from typing import Any, Dict, List, NotRequired, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from agents.base import BaseAgent
from core.llm_engine import CognitiveEngine
from core.models import (
    ActionPlan,
    Alert,
    LLMProposal,
    SkillExecutionResult,
)
from core.observability import metrics, pipeline_states
from core.skills import SkillRegistry
from interfaces.base import IKnowledgeBase, ILLMClient


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class DiagnosisState(TypedDict):
    alert: Alert
    engine: CognitiveEngine
    kb: IKnowledgeBase
    skill_registry: SkillRegistry
    intent_category: NotRequired[str]
    intent_confidence: NotRequired[float]
    intent_reason: NotRequired[str]
    intent_tags: NotRequired[List[str]]
    jira_knowledge_retriever: NotRequired[Any]
    log_querier: NotRequired[Any]
    metrics_querier: NotRequired[Any]
    retry_feedback: NotRequired[Optional[str]]
    prev_plan: NotRequired[Optional[ActionPlan]]
    # Outputs
    sops: NotRequired[List[str]]
    skill_results: NotRequired[List[SkillExecutionResult]]
    proposal: NotRequired[LLMProposal]


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def _retrieve_knowledge(state: DiagnosisState) -> dict:
    """知识检索：KB + Jira 历史案例。"""
    alert = state["alert"]
    kb = state["kb"]
    intent_category = state.get("intent_category", "general")

    pipeline_states.update(alert.alert_id, "knowledge_search")
    with metrics.timer("pipeline.knowledge_search.ms",
                       success_counter="pipeline.knowledge_search.success",
                       failure_counter="pipeline.knowledge_search.failure"):
        search_query = (
            f"category={intent_category} "
            f"{alert.title} {alert.content}"
        )
        sops = kb.search_experience(search_query)

        jira_retriever = state.get("jira_knowledge_retriever")
        if jira_retriever:
            try:
                cases = jira_retriever.search_for_alert(alert)
                formatted = jira_retriever.format_cases(cases)
                if formatted:
                    print(f"[DiagnosisAgent] 命中 Jira 历史案例 {len(cases)} 条")
                    sops = list(sops) + formatted
            except Exception as exc:
                print(f"[DiagnosisAgent] [WARN] Jira 历史案例检索失败，已降级: {exc}")

    return {"sops": sops}


def _run_skills(state: DiagnosisState) -> dict:
    """Skill 调度执行。"""
    alert = state["alert"]
    skill_registry = state["skill_registry"]

    pipeline_states.update(alert.alert_id, "skills_running")
    skill_results = skill_registry.run(alert, {
        "history_sops": state.get("sops") or [],
        "alert": alert.model_dump(),
        "intent": {
            "category": state.get("intent_category", "general"),
            "confidence": state.get("intent_confidence", 0.0),
            "reason": state.get("intent_reason", ""),
            "tags": state.get("intent_tags") or [],
        },
        "log_querier": state.get("log_querier"),
        "metrics_querier": state.get("metrics_querier"),
    })
    return {"skill_results": skill_results}


def _reason(state: DiagnosisState) -> dict:
    """调用认知引擎生成 ActionPlan。"""
    engine = state["engine"]
    alert = state["alert"]
    retry_feedback = state.get("retry_feedback")
    prev_plan = state.get("prev_plan")

    proposal = engine.analyze_alert(
        alert,
        state.get("sops") or [],
        state.get("skill_results"),
        intent_category=state.get("intent_category", "general"),
        intent_confidence=state.get("intent_confidence", 0.0),
        intent_reason=state.get("intent_reason", ""),
        retry_feedback=retry_feedback,
        prev_plan=prev_plan,
    )
    return {"proposal": proposal}


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _build_diagnosis_graph():
    builder = StateGraph(DiagnosisState)

    builder.add_node("retrieve_knowledge", _retrieve_knowledge)
    builder.add_node("run_skills", _run_skills)
    builder.add_node("reason", _reason)

    builder.add_edge(START, "retrieve_knowledge")
    builder.add_edge("retrieve_knowledge", "run_skills")
    builder.add_edge("run_skills", "reason")
    builder.add_edge("reason", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Agent class
# ---------------------------------------------------------------------------

class DiagnosisAgent(BaseAgent):
    """诊断 Agent：知识检索 + Skill 执行 + LLM 推理。"""

    def __init__(
        self,
        llm_client: ILLMClient,
        kb: IKnowledgeBase,
        skill_registry: SkillRegistry,
        jira_knowledge_retriever: Optional[Any] = None,
        log_querier: Optional[Any] = None,
        metrics_querier: Optional[Any] = None,
    ):
        self.llm_client = llm_client
        self.kb = kb
        self.skill_registry = skill_registry
        self.jira_knowledge_retriever = jira_knowledge_retriever
        self.log_querier = log_querier
        self.metrics_querier = metrics_querier
        self.engine = CognitiveEngine(llm_client)

    def name(self) -> str:
        return "diagnosis"

    def build_graph(self):
        return _build_diagnosis_graph()

    def retrieve_knowledge(self, alert: Alert, intent_category: str = "general") -> List[str]:
        """仅知识检索（不执行 Skill 和 LLM 推理）。"""
        search_query = f"category={intent_category} {alert.title} {alert.content}"
        sops = self.kb.search_experience(search_query)

        if self.jira_knowledge_retriever:
            try:
                cases = self.jira_knowledge_retriever.search_for_alert(alert)
                formatted = self.jira_knowledge_retriever.format_cases(cases)
                if formatted:
                    sops = list(sops) + formatted
            except Exception:
                pass

        return sops

    def run_skills(self, alert: Alert, context: dict) -> List[SkillExecutionResult]:
        """仅 Skill 执行。"""
        return self.skill_registry.run(alert, context)

    def diagnose(
        self,
        alert: Alert,
        intent_category: str = "general",
        intent_confidence: float = 0.0,
        intent_reason: str = "",
        intent_tags: Optional[List[str]] = None,
        correlated_alerts: Optional[List[Dict]] = None,
        retry_feedback: Optional[str] = None,
        prev_plan: Optional[ActionPlan] = None,
    ) -> Dict[str, Any]:
        """完整诊断流程：知识检索 → Skill 执行 → LLM 推理。

        Returns:
            dict with keys: sops, skill_results, proposal
        """
        initial_state = {
            "alert": alert,
            "engine": self.engine,
            "kb": self.kb,
            "skill_registry": self.skill_registry,
            "intent_category": intent_category,
            "intent_confidence": intent_confidence,
            "intent_reason": intent_reason,
            "intent_tags": intent_tags or [],
            "jira_knowledge_retriever": self.jira_knowledge_retriever,
            "log_querier": self.log_querier,
            "metrics_querier": self.metrics_querier,
            "retry_feedback": retry_feedback,
            "prev_plan": prev_plan,
        }

        return self._invoke(initial_state)
