from abc import ABC, abstractmethod
from typing import Optional, List
from core.models import Alert, ActionPlan, Feedback, EvaluationResult

class IKnowledgeBase(ABC):
    @abstractmethod
    def search_experience(self, alert_feature: str) -> List[str]:
        """根据告警特征检索相关的历史排查经验和 SOP"""
        pass

    @abstractmethod
    def learn_new_experience(self, feedback: Feedback) -> bool:
        """沉淀新经验到知识库中"""
        pass

class IExecutor(ABC):
    @abstractmethod
    def execute_script(self, script_content: str, context: dict) -> bool:
        """在目标环境中执行修复脚本"""
        pass

class IApprovalGate(ABC):
    @abstractmethod
    def confirm_execution(self, alert: Alert, proposal: ActionPlan, evaluation: EvaluationResult) -> bool:
        """在脚本执行前执行人工确认，返回是否允许继续执行"""
        pass

class ITicketing(ABC):
    @abstractmethod
    def create_ticket(self, alert: Alert, proposal: ActionPlan, eval_reason: str) -> str:
        """向工单系统（如 Jira）创建人工干预票据，返回 ticket_id"""
        pass

    @abstractmethod
    def get_resolution(self, ticket_id: str) -> Optional[Feedback]:
        """获取已完结的工单处理结果"""
        pass

class ILLMClient(ABC):
    @abstractmethod
    def generate_proposal(self, prompt: str) -> str:
        """与底层大模型交互返回字符串。
        供后续 CognitiveEngine 封装强结构化 JSON。
        """
        pass
