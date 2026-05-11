from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Generator, Iterable
from core.models import (
    Alert,
    ActionPlan,
    Feedback,
    EvaluationResult,
    SkillExecutionResult,
)


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
    def confirm_execution(
        self, alert: Alert, proposal: ActionPlan, evaluation: EvaluationResult
    ) -> bool:
        """在脚本执行前执行人工确认，返回是否允许继续执行"""
        pass


class ITicketing(ABC):
    @abstractmethod
    def create_ticket(
        self, alert: Alert, proposal: ActionPlan, eval_reason: str
    ) -> str:
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


class ISkill(ABC):
    @abstractmethod
    def name(self) -> str:
        """skill 唯一名称"""
        pass

    @abstractmethod
    def can_handle(self, alert: Alert) -> bool:
        """判断当前告警是否适合由该 skill 处理"""
        pass

    @abstractmethod
    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        """执行 skill，并返回结构化结果"""
        pass


class IAlertSource(ABC):
    """告警数据源接口，抽象 Kafka / Prometheus / Webhook 等外部告警接入"""

    @abstractmethod
    def consume(self) -> Generator[Dict[str, Any], None, None]:
        """持续消费原始告警消息，yield 原始字典"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭数据源连接、释放资源"""
        pass


class IAlertCleaner(ABC):
    """告警清洗接口：将原始消息映射为标准 Alert 对象"""

    @abstractmethod
    def clean(self, raw: Dict[str, Any]) -> Optional[Alert]:
        """清洗并转换一条原始消息；返回 None 表示无法解析"""
        pass


class IAlertFilter(ABC):
    """告警过滤接口：根据规则决定是否放行"""

    @abstractmethod
    def should_process(self, alert: Alert) -> bool:
        """返回 True 表示放行该告警进入后续流水线"""
        pass
