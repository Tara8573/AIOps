import json
from core.models import Alert, ActionPlan, LLMProposal
from interfaces.base import ILLMClient

class CognitiveEngine:
    """认知引擎：封装与大模型对话、Prompt工程及结构化解析的过程"""
    def __init__(self, llm_client: ILLMClient):
        self.llm_client = llm_client
        
    def analyze_alert(self, alert: Alert, history_sops: list[str]) -> LLMProposal:
        """接收告警和历史经验，提示 LLM 生成排查动作"""
        sops_str = "\n".join(history_sops) if history_sops else "无历史参考"
        prompt = f"""
        你是一个生产系统 AIOps 专家。请分析以下告警：
        【告警名称】: {alert.title}
        【告警级别】: {alert.level}
        【告警详情】: {alert.content}
        【历史经验推荐】:
        {sops_str}
        
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
            return LLMProposal(alert_id=alert.alert_id, plan=action_plan)
        except Exception as e:
            # 兜底：如果 LLM 没有正确返回结构化数据
            fallback_plan = ActionPlan(
                root_cause_analysis=f"解析LLM响应失败。Exception: {str(e)}\nRaw Response: {response_text[:100]}",
                confidence_score=0.0
            )
            return LLMProposal(alert_id=alert.alert_id, plan=fallback_plan)
