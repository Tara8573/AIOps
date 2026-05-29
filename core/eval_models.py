"""评估系统专用 Pydantic 模型，与生产模型 core/models.py 解耦"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class ExpectedOutcome(BaseModel):
    """评估用例的期望结果（ground truth）"""

    root_cause_keywords: List[str] = Field(
        default_factory=list, description="根因分析中应包含的关键词"
    )
    root_cause_category: str = Field(
        default="", description="期望的根因分类，如 disk, memory, network"
    )
    min_troubleshooting_steps: int = Field(
        default=1, description="最少排查步骤数"
    )
    step_keywords: List[str] = Field(
        default_factory=list, description="排查步骤中应出现的关键词（至少命中一个）"
    )
    expect_script: bool = Field(
        default=False, description="是否期望方案包含修复脚本"
    )
    script_keywords: List[str] = Field(
        default_factory=list, description="脚本中应出现的关键词"
    )
    min_confidence: float = Field(default=0.5, description="期望的最低置信度")
    max_confidence: float = Field(
        default=1.0, description="期望的最高置信度（用于检测过度自信）"
    )


class EvalCase(BaseModel):
    """单条评估用例：告警输入 + 期望输出"""

    case_id: str
    alert: Dict[str, Any]  # title, level, content -- 运行时转为 Alert
    expected: ExpectedOutcome
    tags: List[str] = Field(default_factory=list, description="分类标签")


class DimensionScore(BaseModel):
    """单个维度的评分"""

    name: str
    score: float = Field(ge=0.0, le=1.0)
    passed: bool
    detail: str = ""


class CaseResult(BaseModel):
    """单条用例的评估结果"""

    case_id: str
    alert_title: str
    alert_level: str
    dimensions: List[DimensionScore] = Field(default_factory=list)
    overall_score: float = 0.0
    passed: bool = False
    generated_plan: Dict[str, Any] = Field(default_factory=dict)
    latency_ms: float = 0.0
    error: Optional[str] = None


class DimensionStats(BaseModel):
    """单个维度的汇总统计"""

    mean: float
    stddev: float
    min_score: float
    max_score: float
    pass_rate: float


class TagStats(BaseModel):
    """按标签分组的统计"""

    count: int
    pass_rate: float
    avg_score: float


class AggregateStats(BaseModel):
    """汇总统计"""

    total_cases: int
    passed_cases: int
    failed_cases: int
    pass_rate: float
    avg_overall_score: float
    avg_latency_ms: float
    per_dimension: Dict[str, DimensionStats] = Field(default_factory=dict)
    per_tag: Dict[str, TagStats] = Field(default_factory=dict)


class RunReport(BaseModel):
    """单轮 eval 的汇总报告"""

    run_id: str
    prompt_template: str
    timestamp: datetime = Field(default_factory=datetime.now)
    train_results: List[CaseResult] = Field(default_factory=list)
    test_results: List[CaseResult] = Field(default_factory=list)
    aggregate: AggregateStats


class LoopResult(BaseModel):
    """整个 eval-improve 循环的最终结果"""

    iterations: int
    best_iteration: int
    best_prompt: str
    best_test_pass_rate: float
    best_test_avg_score: float
    all_reports: List[RunReport] = Field(default_factory=list)
    improvement_delta: float = Field(
        default=0.0, description="最佳轮 vs 第一轮的 test avg_score 差值"
    )
