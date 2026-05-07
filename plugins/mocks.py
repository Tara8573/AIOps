import uuid
import json
from typing import List, Optional
from core.models import Alert, ActionPlan, Feedback, EvaluationResult
from interfaces.base import IKnowledgeBase, IExecutor, IApprovalGate, ITicketing, ILLMClient

class MockKnowledgeBase(IKnowledgeBase):
    def search_experience(self, alert_feature: str) -> List[str]:
        print(f"[MockKB] 检索相关经验: {alert_feature}")
        return ["历史SOP: 发现磁盘空间满可清理 /var/log"]

    def learn_new_experience(self, feedback: Feedback) -> bool:
        print(f"[MockKB] 记录新经验: 告警={feedback.alert_id}, 处理手法={feedback.resolution_steps}")
        return True

class MockExecutor(IExecutor):
    def execute_script(self, script_content: str, context: dict) -> bool:
        print(f"[MockExecutor] 正在物理机执行脚本...\n---SCRIPT---\n{script_content}\n---END---")
        return True

class MockApprovalGate(IApprovalGate):
    def confirm_execution(self, alert: Alert, proposal: ActionPlan, evaluation: EvaluationResult) -> bool:
        print("[Approval] 检测到待执行脚本，等待人工确认")
        print(f"[Approval] Alert={alert.alert_id} Level={alert.level} Risk={evaluation.risk_level}")
        print(f"[Approval] RootCause={proposal.root_cause_analysis}")
        print("[Approval] TroubleshootingSteps:")
        for idx, step in enumerate(proposal.troubleshooting_steps, start=1):
            print(f"  {idx}. {step}")
        print(f"[Approval] Script:\n{proposal.script_content}")
        decision = input("[Approval] 输入 yes 执行脚本，其他任意内容转人工工单: ").strip().lower()
        approved = decision in {"y", "yes"}
        print(f"[Approval] 人工确认结果: {'通过' if approved else '拒绝'}")
        return approved

class MockJiraTicketing(ITicketing):
    def __init__(self):
        self.tickets = {}

    def create_ticket(self, alert: Alert, proposal: ActionPlan, eval_reason: str) -> str:
        ticket_id = f"JIRA-{uuid.uuid4().hex[:6].upper()}"
        print(f"[MockJira] 提单成功, TicketID={ticket_id}, 拦截原因={eval_reason}")
        self.tickets[ticket_id] = {
            "alert": alert,
            "proposal": proposal,
            "status": "OPEN"
        }
        return ticket_id

    def get_resolution(self, ticket_id: str) -> Optional[Feedback]:
        """模拟一个已完结的工单供反馈循环测试"""
        ticket = self.tickets.get(ticket_id)
        if not ticket: 
            return None
        return Feedback(
            alert_id=ticket["alert"].alert_id,
            ticket_id=ticket_id,
            actual_root_cause=ticket["proposal"].root_cause_analysis,
            resolution_steps="[人工干预] 确认并手工执行了清理命令",
            is_successful=True
        )

class MockLLMClient(ILLMClient):
    def __init__(self, override_script: str = "rm -rf /var/log/old_logs/*", confidence: float = 0.85):
        self.override_script = override_script
        self.confidence = confidence

    def generate_proposal(self, prompt: str) -> str:
        print("[MockLLM] 假装在调用大模型生成分析...")
        # 强制返回一个符合要求的 JSON 文本
        res = {
            "root_cause_analysis": "可能是系统磁盘空间占满导致服务异常",
            "troubleshooting_steps": ["检查磁盘占用 df -h", "清理无用日志"],
            "script_content": self.override_script,
            "confidence_score": self.confidence
        }
        return json.dumps(res, ensure_ascii=False)
