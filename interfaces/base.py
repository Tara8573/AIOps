from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Generator, Iterable
from core.models import (
    Alert,
    ActionPlan,
    AuditEvent,
    DecisionResult,
    DimensionVerdict,
    Feedback,
    EvaluationResult,
    JudgeResult,
    NoulResult,
    Principal,
    SandboxResult,
    ScoreResult,
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


class IApprovalRequestStore(ABC):
    """异步审批存储接口（FR-3 人工确认异步化，控制面/数据面分离）。

    流转：approve 节点 submit 后挂起（LangGraph interrupt，worker 释放）
    → 人工经回调 API/IM 决策写入（decide）
    → ApprovalResumer 原子领取已决策未恢复的审批（take_decided，多实例不重复捡起）
    → Command(resume=decision) 恢复执行。

    状态机：pending → approved | rejected | expired → resumed
    """

    @abstractmethod
    def submit(
        self,
        pipeline_run_id: str,
        alert: Alert,
        plan: ActionPlan,
        evaluation: EvaluationResult,
        timeout_seconds: int,
    ) -> str:
        """登记待审批记录，返回 approval_id。

        按 pipeline_run_id 幂等：interrupt 恢复时 approve 节点会重跑，
        已存在则返回既有 id，不重复登记。
        """
        pass

    @abstractmethod
    def find_by_run(self, pipeline_run_id: str) -> Optional["ApprovalRecord"]:
        """按 run 查找审批记录（幂等护栏）。"""
        pass

    @abstractmethod
    def decide(self, approval_id: str, decision: str, decider: str = "", reason: str = "") -> bool:
        """人工决策：decision ∈ approved | rejected。仅 pending 可决策。"""
        pass

    @abstractmethod
    def expire_overdue(self) -> List["ApprovalRecord"]:
        """把 pending 且 timeout_at 已过的标记 expired（超时默认拒绝），返回本次过期记录。"""
        pass

    @abstractmethod
    def take_decided(self, limit: int = 10, instance_id: str = "") -> List["ApprovalRecord"]:
        """原子领取已决策未恢复的审批（approved/rejected/expired）。

        领取即标记 resumed_at/resumed_by —— 多实例不重复捡起。
        恢复失败可用 mark_resumed(success=False) 释放重试。
        """
        pass

    @abstractmethod
    def mark_resumed(self, approval_id: str, instance_id: str, success: bool) -> None:
        """恢复结果回写：success=True 置 resumed；False 清 resumed_at 待重试。"""
        pass

    @abstractmethod
    def list_by_status(self, status: str, limit: int = 50) -> List["ApprovalRecord"]:
        """按状态列出审批（供回调 API/Web/IM 轮询）。"""
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


class ILogQuerier(ABC):
    """日志查询接口：对接 ELK / Loki / Splunk 等日志系统。

    Skill 通过 context["log_querier"] 获取实例，
    在 run() 中主动拉取目标主机/服务的近期日志作为诊断证据。
    """

    @abstractmethod
    def query(
        self,
        query: str,
        host: str = "",
        service: str = "",
        time_range_minutes: int = 30,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """查询日志条目。

        Args:
            query: 搜索关键词或查询语句（如 KQL / LogQL）。
            host: 目标主机标识（IP / hostname）。
            service: 目标服务名称。
            time_range_minutes: 向前查询的时间范围（分钟）。
            limit: 最大返回条数。

        Returns:
            日志条目列表，每条至少包含 timestamp / level / message。
        """
        pass


class IMetricsQuerier(ABC):
    """指标查询接口：对接 Prometheus / Grafana / CloudWatch 等监控系统。

    Skill 通过 context["metrics_querier"] 获取实例，
    在 run() 中主动拉取目标主机/服务的实时指标作为诊断证据。
    """

    @abstractmethod
    def query(
        self,
        metric_name: str,
        host: str = "",
        service: str = "",
        time_range_minutes: int = 30,
        step: str = "60s",
    ) -> List[Dict[str, Any]]:
        """查询时间序列指标数据。

        Args:
            metric_name: 指标名称（如 node_filesystem_avail_bytes）。
            host: 目标主机标识。
            service: 目标服务名称。
            time_range_minutes: 向前查询的时间范围（分钟）。
            step: 数据点间隔。

        Returns:
            指标数据列表，每条至少包含 timestamp / value。
        """
        pass


class IRuleEngine(ABC):
    """规则引擎接口：对接 StackStorm / Rundeck / Ansible Tower 等自动化平台。

    模式 C（双轨并行）中，已知问题走规则引擎快路径（毫秒级确定性修复），
    未知问题走 AIOps 智能诊断。规则引擎匹配失败时返回 None，由 AIOps 接管。
    """

    @abstractmethod
    def match(self, alert: Alert) -> Optional[Dict[str, Any]]:
        """尝试匹配已知规则。

        Returns:
            匹配到的规则描述 dict，至少包含 rule_id / action_description。
            返回 None 表示未命中任何规则，应由 AIOps 接管。
        """
        pass

    @abstractmethod
    def execute(self, alert: Alert, rule: Dict[str, Any]) -> bool:
        """执行匹配到的规则，返回是否成功。"""
        pass


# ============================================================
# Security Interfaces (审计 / 沙箱 / 权限)
# ============================================================

class IAuditStore(ABC):
    """审计事件存储接口"""

    @abstractmethod
    def emit(self, event: AuditEvent) -> None:
        """写入一条审计事件"""
        pass

    @abstractmethod
    def query(
        self,
        event_type: Optional[str] = None,
        alert_id: Optional[str] = None,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """查询审计事件"""
        pass

    @abstractmethod
    def verify_chain(self, from_id: str, to_id: str) -> bool:
        """验证一段审计链的完整性（hash chain 校验）"""
        pass


class ISandbox(ABC):
    """脚本执行沙箱接口"""

    @abstractmethod
    def execute(
        self,
        script: str,
        context: Dict[str, Any],
        timeout_seconds: int = 300,
    ) -> SandboxResult:
        """在隔离环境中执行脚本"""
        pass

    @abstractmethod
    def preflight_check(self, script: str) -> List[str]:
        """静态分析，返回发现的问题列表（空 = 通过）"""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """检查沙箱环境是否就绪"""
        pass


class IAuthProvider(ABC):
    """认证提供者"""

    @abstractmethod
    def authenticate(self, credentials: Dict[str, Any]) -> Optional[Principal]:
        """验证凭据，返回 Principal 或 None"""
        pass


class IPermissionChecker(ABC):
    """权限检查接口"""

    @abstractmethod
    def can_read(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        """检查是否可读"""
        pass

    @abstractmethod
    def can_write(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        """检查是否可写"""
        pass

    @abstractmethod
    def can_execute(self, principal: Principal, action: str) -> bool:
        """检查是否可执行特定操作"""
        pass


# ============================================================
# Message Queue
# ============================================================

# ============================================================
# Decision Engine (判别式决策接口 — 与 ILLMClient 生成式互补)
# ============================================================

class IDecisionClient(ABC):
    """判别式决策接口。

    定位：答案集合可在调用前写死的分类/打分/判定场景。
    区别于 ILLMClient（生成文字/开放性推理）。

    当前内置实现：
    - RuleDecisionClient: 确定性规则（关键词/正则），零成本、零延迟
    - JevDecisionClient:  概率语义模型（HTTP/SDK），语义泛化

    设计原则：
    - 管线只依赖本接口，不直接绑定 Jev 或任何具体模型
    - 未来可随时替换为本地模型、ensemble、或任意第三方服务
    """

    @abstractmethod
    def classify(self, prompt: str, options: List[str]) -> DecisionResult:
        """多选一分类。

        Args:
            prompt: 分类依据的上下文（告警文本、脚本内容等）
            options: 候选分类标签列表

        Returns:
            DecisionResult: 最佳选项 + 校准置信度 + 备选排名
        """
        pass

    @abstractmethod
    def judge(
        self, prompt: str, dimensions: List[Dict[str, Any]]
    ) -> JudgeResult:
        """多维度评估。

        Args:
            prompt: 评估对象描述（方案、脚本、模板等）
            dimensions: 维度定义列表，每项含 name / question / weight

        Returns:
            JudgeResult: 各维度判定 + 综合评分
        """
        pass

    @abstractmethod
    def noul(self, prompt: str, question: str) -> NoulResult:
        """二元判定 (Yes/No)。

        Args:
            prompt: 判定上下文
            question: 需要回答的是/否问题

        Returns:
            NoulResult: 判定结果 + 校准置信度
        """
        pass

    @abstractmethod
    def score(
        self, prompt: str, min_val: float, max_val: float, instructions: str = ""
    ) -> ScoreResult:
        """数值打分。

        Args:
            prompt: 评估对象描述
            min_val: 分值下限
            max_val: 分值上限
            instructions: 打分指引

        Returns:
            ScoreResult: 分值 + 校准置信度
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """检查决策服务是否可用"""
        pass


class IMessageProducer(ABC):
    """消息生产者接口 — 将事件发布到消息队列。"""

    @abstractmethod
    def publish(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """发布一条消息到指定 topic。"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭生产者，刷出缓冲消息。"""
        pass

