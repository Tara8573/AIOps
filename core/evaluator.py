import os
import re
from typing import List, Optional

from core.models import Alert, EvaluationResult, LLMProposal
from core.observability import metrics


class ScriptSafetyScanner:
    """Performs conservative script safety checks before execution."""

    DEFAULT_PATTERNS = [
        r"rm\s+-rf\s+/",
        r"mkfs",
        r"dd\s+if=",
        r">\s*/dev/sd[a-z]",
        r"reboot\b",
        r"shutdown\b",
        r"curl\s+.*\|\s*(sh|bash)",
        r"wget\s+.*\|\s*(sh|bash)",
        r"chmod\s+777",
        r":\(\)\s*\{\s*:\|\:&\s*\};:",  # fork bomb
    ]

    def __init__(self, patterns: Optional[List[str]] = None):
        pattern_text = os.getenv("AIOPS_DANGEROUS_SCRIPT_PATTERNS", "").strip()
        configured_patterns = [p.strip() for p in pattern_text.split(",") if p.strip()]
        self.patterns = patterns or configured_patterns or self.DEFAULT_PATTERNS

    def find_risks(self, script: str) -> List[str]:
        findings = []
        for pattern in self.patterns:
            if re.search(pattern, script, re.IGNORECASE):
                findings.append(pattern)
        return findings


class SolutionEvaluator:
    """评估器：负责审批单次大模型的解决方案是否正确且安全"""

    def __init__(self, min_confidence: Optional[float] = None):
        self.min_confidence = min_confidence or float(
            os.getenv("AIOPS_MIN_CONFIDENCE", "0.7")
        )
        self.script_scanner = ScriptSafetyScanner()

    def evaluate(self, alert: Alert, proposal: LLMProposal) -> EvaluationResult:
        plan = proposal.plan

        # 1. 置信度检查
        if plan.confidence_score < self.min_confidence:
            metrics.incr("pipeline.evaluation.blocked.low_confidence")
            return EvaluationResult(
                is_passed=False,
                reason=(
                    f"拦截: 方案置信度过低 "
                    f"({plan.confidence_score} < {self.min_confidence})"
                ),
                risk_level="Unknown",
            )

        # 2. 脚本安全性检查
        script = plan.script_content
        if script:
            findings = self.script_scanner.find_risks(script)
            if findings:
                metrics.incr("pipeline.evaluation.blocked.script_risk")
                return EvaluationResult(
                    is_passed=False,
                    reason=f"拦截: 检测到高危修复命令 {findings}",
                    risk_level="High",
                )

        # 3. 完整性检查：既然置信度高，必须有对应的排查步骤支撑
        if not plan.troubleshooting_steps:
            metrics.incr("pipeline.evaluation.blocked.missing_steps")
            return EvaluationResult(
                is_passed=False,
                reason="拦截: 方案缺乏明确的排查步骤支持",
                risk_level="Medium",
            )

        # 4. （可选）在此处可扩展对接另一个 LLM 进行逻辑二次“自省反问”打分
        metrics.incr("pipeline.evaluation.passed")
        return EvaluationResult(
            is_passed=True,
            reason="评估通过：方案置信度合格且合法安全",
            risk_level="Low",
        )
