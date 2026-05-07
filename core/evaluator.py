from core.models import Alert, LLMProposal, EvaluationResult
import re

class SolutionEvaluator:
    """评估器：负责审批单次大模型的解决方案是否正确且安全"""
    
    def __init__(self):
        # 针对演示配置的无脑高危脚本黑名单
        self.dangerous_commands = [
            r"rm\s+-rf\s+/",
            r"mkfs",
            r"dd\s+if=",
            r">.+/dev/sda",
            r"reboot",
            r"shutdown"
        ]

    def evaluate(self, alert: Alert, proposal: LLMProposal) -> EvaluationResult:
        plan = proposal.plan
        
        # 1. 置信度检查
        if plan.confidence_score < 0.7:
            return EvaluationResult(
                is_passed=False,
                reason=f"拦截: 方案置信度过低 ({plan.confidence_score} < 0.7)",
                risk_level="Unknown"
            )
            
        # 2. 脚本安全性检查
        script = plan.script_content
        if script:
            for pattern in self.dangerous_commands:
                if re.search(pattern, script):
                    return EvaluationResult(
                        is_passed=False,
                        reason=f"拦截: 检测到高危修复命令 '{pattern}'",
                        risk_level="High"
                    )
                    
        # 3. 完整性检查：既然置信度高，必须有对应的排查步骤支撑
        if not plan.troubleshooting_steps:
            return EvaluationResult(
                is_passed=False,
                reason="拦截: 方案缺乏明确的排查步骤支持",
                risk_level="Medium"
            )

        # 4. （可选）在此处可扩展对接另一个 LLM 进行逻辑二次“自省反问”打分
        
        return EvaluationResult(
            is_passed=True,
            reason="评估通过：方案置信度合格且合法安全",
            risk_level="Low"
        )
