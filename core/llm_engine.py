import json
import os
from typing import List, Optional

from core.cache import TTLCache, stable_cache_key
from core.models import Alert, ActionPlan, LLMProposal, SkillExecutionResult
from core.observability import metrics
from interfaces.base import ILLMClient


class CognitiveEngine:
    """认知引擎：封装与大模型对话、Prompt工程及结构化解析的过程"""

    def __init__(self, llm_client: ILLMClient):
        self.llm_client = llm_client
        self._proposal_cache = TTLCache(
            ttl_seconds=int(os.getenv("AIOPS_LLM_CACHE_TTL_SECONDS", "300")),
            max_size=int(os.getenv("AIOPS_LLM_CACHE_MAX_SIZE", "256")),
        )

    def analyze_alert(
        self,
        alert: Alert,
        history_sops: List[str],
        skill_results: Optional[List[SkillExecutionResult]] = None,
    ) -> LLMProposal:
        """接收告警和历史经验，提示 LLM 生成排查动作"""
        cache_key = stable_cache_key(
            alert.title,
            alert.level,
            alert.content,
            history_sops,
            [result.model_dump() for result in (skill_results or [])],
        )
        cached_plan = self._proposal_cache.get(cache_key)
        if cached_plan is not None:
            metrics.incr("llm.cache.hit")
            return LLMProposal(alert_id=alert.alert_id, plan=ActionPlan(**cached_plan))

        metrics.incr("llm.cache.miss")
        sops_str = "\n".join(history_sops) if history_sops else "无历史参考"
        skill_results = skill_results or []
        skill_context = self._format_skill_context(skill_results)
        prompt = f"""
        你是一个生产系统 AIOps 专家。请分析以下告警：
        【告警名称】: {alert.title}
        【告警级别】: {alert.level}
        【告警详情】: {alert.content}
        【历史经验推荐】:
        {sops_str}
        【可用 Skills 执行结果】:
        {skill_context}
        
        请严格以 JSON 格式输出，包含以下字段：
        - root_cause_analysis (str): 根因推测
        - troubleshooting_steps (list[str]): 排查步骤
        - script_content (str | null): 如果有明确修复手段，提供可执行脚本代码，否则为 null
        - confidence_score (float): 你对该结论的置信度，0.0 到 1.0 的小数。
        """

        response_text = self.llm_client.generate_proposal(prompt)

        try:
            # 清理可能的 markdown 代码块标记
            cleaned_text = response_text.strip()
            if cleaned_text.startswith("```json"):
                cleaned_text = cleaned_text[7:]
            if cleaned_text.startswith("```"):
                cleaned_text = cleaned_text[3:]
            if cleaned_text.endswith("```"):
                cleaned_text = cleaned_text[:-3]

            data = json.loads(cleaned_text.strip())
            action_plan = ActionPlan(**data)
            self._proposal_cache.set(cache_key, action_plan.model_dump())
            return LLMProposal(alert_id=alert.alert_id, plan=action_plan)
        except Exception as e:
            # 兜底：如果 LLM 没有正确返回结构化数据
            metrics.incr("llm.parse.failure")
            fallback_plan = ActionPlan(
                root_cause_analysis=f"解析LLM响应失败。Exception: {str(e)}\nRaw Response: {response_text[:100]}",
                script_content=None,
                confidence_score=0.0,
            )
            return LLMProposal(alert_id=alert.alert_id, plan=fallback_plan)

    @staticmethod
    def _format_skill_context(skill_results: List[SkillExecutionResult]) -> str:
        if not skill_results:
            return "无可用 skill 输出"

        lines = []
        for result in skill_results:
            lines.append(
                (
                    f"- Skill={result.skill_name}; success={result.is_successful}; "
                    f"confidence={result.confidence_score}; summary={result.summary}; "
                    f"recommended_actions={result.recommended_actions}; details={json.dumps(result.details, ensure_ascii=False)}"
                )
            )
        return "\n".join(lines)
