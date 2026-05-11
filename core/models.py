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

class ActionPlan(BaseModel):
    """LLM 给出的排查建议和解决方案，可能包含脚本"""
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
