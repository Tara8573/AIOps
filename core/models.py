from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class Alert(BaseModel):
    """告警实体"""
    alert_id: str
    title: str
    level: str # e.g., Critical, Warning, Info
    content: str
    timestamp: datetime = Field(default_factory=datetime.now)
    source: str = "prometheus"
    raw_data: Optional[Dict[str, Any]] = None
    correlated_alerts: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="时间窗口内来自同一主机/服务的关联告警",
    )

class Hypothesis(BaseModel):
    """单条根因假设"""
    description: str = Field(description="假设描述")
    likelihood: str = Field("medium", description="可能性: high / medium / low")


class ActionPlan(BaseModel):
    """LLM 给出的排查建议和解决方案，可能包含脚本"""
    possible_hypotheses: List[Hypothesis] = Field(
        default_factory=list, description="可能的根因假设（按可能性排序）"
    )
    root_cause_analysis: str = Field(description="详细的根因分析")
    troubleshooting_steps: List[str] = Field(description="分步排查建议", default_factory=list)
    script_content: Optional[str] = Field(None, description="可执行的修复脚本代码（如shell/python）")
    confidence_score: float = Field(0.0, description="LLM 对该解决方案的置信度 (0.0-1.0)")

class LLMProposal(BaseModel):
    """LLM 整体输出提案"""
    alert_id: str
    plan: ActionPlan
    
class EvaluationResult(BaseModel):
    """方案评估结果"""
    is_passed: bool = Field(False, description="是否允许自动执行")
    reason: str = Field("", description="通过或拦截的具体原因")
    risk_level: str = Field("Unknown", description="识别出的动作风险等级 (Low, Medium, High)")


class SkillExecutionResult(BaseModel):
    """单个 skill 的执行结果"""
    skill_name: str
    summary: str = Field("", description="skill 返回的摘要信息")
    details: Dict[str, Any] = Field(default_factory=dict, description="结构化明细")
    recommended_actions: List[str] = Field(
        default_factory=list, description="建议优先执行的动作"
    )
    confidence_score: float = Field(
        0.0, description="skill 对自身结果的置信度 (0.0-1.0)"
    )
    is_successful: bool = Field(True, description="skill 是否执行成功")

class Feedback(BaseModel):
    """来自工单系统的最终人工处理经验"""
    alert_id: str
    ticket_id: str
    actual_root_cause: str
    resolution_steps: str
    is_successful: bool
    feedback_type: str = Field(
        "positive",
        description="positive=成功处置经验, negative=失败/误判案例",
    )


# ============================================================
# Security Models (审计 / 沙箱 / 权限)
# ============================================================

class SandboxResult(BaseModel):
    """沙箱执行结果"""
    success: bool
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    duration_ms: float = 0.0
    resource_usage: Dict[str, Any] = Field(default_factory=dict)
    filesystem_changes: List[str] = Field(default_factory=list)
    network_connections: List[str] = Field(default_factory=list)
    sandbox_violations: List[str] = Field(default_factory=list)


class Principal(BaseModel):
    """认证主体（用户或服务账号）"""
    principal_id: str
    roles: List[str] = Field(default_factory=lambda: ["viewer"])
    name: str = ""
    authenticated_at: Optional[datetime] = None


class AuditEvent(BaseModel):
    """审计事件"""
    event_id: str
    timestamp: datetime = Field(default_factory=datetime.now)
    event_type: str
    actor: str = "system"
    alert_id: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    category: str = "operational"
    severity: str = "info"
    payload: Dict[str, Any] = Field(default_factory=dict)
    outcome: str = "success"
    prev_hash: Optional[str] = None


class ApprovalRecord(BaseModel):
    """待审批记录（FR-3 异步审批）"""
    approval_id: str
    pipeline_run_id: str
    alert_id: str
    proposal: Dict[str, Any] = Field(default_factory=dict, description="ActionPlan.model_dump()")
    eval_summary: Dict[str, Any] = Field(default_factory=dict, description="EvaluationResult + risk_level")
    status: str = "pending"  # pending | approved | rejected | expired | resumed
    decider: Optional[str] = None
    decide_reason: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    timeout_at: Optional[datetime] = None
    decided_at: Optional[datetime] = None
    resumed_at: Optional[datetime] = None
    resumed_by: Optional[str] = None


# ============================================================
# Decision Engine Models (判别式决策结果)
# ============================================================

class DecisionResult(BaseModel):
    """多选一分类结果"""
    choice: str = Field(description="选中的分类标签")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="校准置信度")
    reason: str = Field("", description="判定依据摘要")
    alternatives: List[Dict[str, Any]] = Field(
        default_factory=list, description="其他候选及其分数"
    )


class DimensionVerdict(BaseModel):
    """单个评估维度的判定"""
    name: str = Field(description="维度名称")
    passed: bool = Field(description="是否通过")
    score: float = Field(0.0, description="维度得分")
    reason: str = Field("", description="判定原因")


class JudgeResult(BaseModel):
    """多维度评估结果"""
    dimensions: List[DimensionVerdict] = Field(default_factory=list)
    overall_passed: bool = Field(False, description="是否所有维度均通过")
    overall_score: float = Field(0.0, ge=0.0, le=1.0, description="综合得分")
    summary: str = Field("", description="评估总结")


class NoulResult(BaseModel):
    """二元判定结果 (Noul = Yes/No 判别)"""
    answer: bool = Field(description="判定结果: True=是, False=否")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="校准置信度")
    reason: str = Field("", description="判定依据摘要")


class ScoreResult(BaseModel):
    """数值打分结果"""
    value: float = Field(description="分值")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="校准置信度")
    reason: str = Field("", description="打分依据摘要")


# ============================================================
# LLM 调用观测（LLMO-2，规格：docs/LLM调用可观测性与质量评估开发规格说明书.md）
# ============================================================

class LLMUsage(BaseModel):
    """LLM 调用 usage（LLMO-2.2 disjoint 约定：input_tokens 不含 cached_read_tokens）"""
    input_tokens: int = Field(0, description="输入 token（不含缓存命中部分）")
    output_tokens: int = Field(0, description="输出 token")
    cached_read_tokens: int = Field(0, description="缓存命中 token（与 input_tokens 不相交）")
    reasoning_tokens: int = Field(0, description="推理 token（如返回）")


class LLMCallEvent(BaseModel):
    """单次 LLM HTTP 调用事件（LLMO-2.1，一次调用 = 一行，append-only）。

    stage/role 来自 LLMCallContext（LLMO-5.2），上下文缺失（Mock/直接调用）时为 None。
    prompt/response 明文受 AIOPS_LLM_CALL_LOG_CAPTURE_CONTENT 控制，
    写入前必经 redact_text 脱敏（LLMO-6.1）。
    """
    call_id: str = Field(description="调用唯一 ID (uuid4)")
    timestamp: str = Field(description="调用完成时刻 (ISO8601)")
    stage: Optional[str] = Field(None, description="analyze / retry / judge / improve / feedback")
    role: Optional[str] = Field(None, description="main / diagnosis / evaluation / feedback / eval_loop")
    provider: str = Field(description="openai / ollama / unknown")
    model: str = Field(description="模型名")
    prompt_chars: int = Field(0, description="prompt 字符数（截断前原始长度）")
    prompt_hash: str = Field(description="prompt SHA256 前 16 位（关联评测用例键）")
    prompt: Optional[str] = Field(None, description="prompt 明文（经脱敏，截断 2000 字符；明文开关控制）")
    response_chars: int = Field(0, description="response 字符数（截断前原始长度）")
    response_hash: str = Field("", description="response SHA256 前 16 位")
    response: Optional[str] = Field(None, description="response 明文（经脱敏，截断 4000 字符；明文开关控制）")
    usage: Optional[LLMUsage] = Field(None, description="usage（disjoint 拆分；网关未返回时为 null）")
    latency_ms: float = Field(0.0, description="模型调用耗时（不含配额等待）")
    outcome: str = Field("success", description="success / error")
    failure_type: Optional[str] = Field(
        None,
        description="LLMO-3.1 枚举：http_error / rate_limited / network_error / timeout / "
                    "provider_exception / parse_failure（success 时为 null）",
    )
    error_summary: Optional[str] = Field(None, description="失败摘要（截断 500 字符）")
    alert_id: Optional[str] = Field(None, description="调用上下文关联键（LLMO-5.2）")
    pipeline_run_id: Optional[str] = Field(None, description="流水线运行 ID")
    prev_hash: Optional[str] = Field(None, description="hash chain 前驱（AIOPS_LLM_CALL_LOG_HASH_CHAIN=1 时）")
    event_hash: Optional[str] = Field(None, description="hash chain 本事件哈希")
