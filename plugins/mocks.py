import uuid
import json
from typing import List, Optional
from core.models import (
    Alert, ActionPlan, AuditEvent, Feedback, EvaluationResult,
    Principal, SandboxResult,
)
from interfaces.base import (
    IKnowledgeBase, IExecutor, IApprovalGate, ITicketing, ILLMClient,
    ILogQuerier, IMetricsQuerier, IAuditStore, ISandbox,
    IAuthProvider, IPermissionChecker,
)
from typing import Any, Dict

class MockKnowledgeBase(IKnowledgeBase):
    def search_experience(self, alert_feature: str) -> List[str]:
        print(f"[MockKB] 检索相关经验: {alert_feature}")
        return ["历史SOP: 发现磁盘空间满可清理 /var/log"]

    def learn_new_experience(self, feedback: Feedback) -> bool:
        feedback_type = getattr(feedback, "feedback_type", "positive")
        label = "负样本" if feedback_type == "negative" else "经验"
        print(f"[MockKB] 记录新{label}: 告警={feedback.alert_id}, 处理手法={feedback.resolution_steps}")
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


class MockLogQuerier(ILogQuerier):
    """日志查询 Mock：返回模拟日志条目，供本地开发和测试使用。"""

    def query(
        self,
        query: str,
        host: str = "",
        service: str = "",
        time_range_minutes: int = 30,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        scope = host or service or "unknown"
        print(f"[MockLogQuerier] 查询日志: query={query!r}, scope={scope}, range={time_range_minutes}min")
        return [
            {
                "timestamp": "2026-06-17T10:00:00Z",
                "level": "ERROR",
                "message": f"[Mock] {scope} 近 {time_range_minutes} 分钟内的模拟错误日志",
                "source": "mock",
            }
        ]


class MockMetricsQuerier(IMetricsQuerier):
    """指标查询 Mock：返回模拟指标数据，供本地开发和测试使用。"""

    def query(
        self,
        metric_name: str,
        host: str = "",
        service: str = "",
        time_range_minutes: int = 30,
        step: str = "60s",
    ) -> List[Dict[str, Any]]:
        scope = host or service or "unknown"
        print(f"[MockMetricsQuerier] 查询指标: metric={metric_name}, scope={scope}, range={time_range_minutes}min")
        return [
            {
                "metric": metric_name,
                "timestamp": "2026-06-17T10:00:00Z",
                "value": 0.95,
                "labels": {"instance": scope},
                "source": "mock",
            }
        ]


class MockSandbox(ISandbox):
    """Mock 沙箱：模拟隔离执行，用于本地开发和测试。"""

    def __init__(self, available: bool = True, preflight_issues: Optional[List[str]] = None):
        self._available = available
        self._preflight_issues = preflight_issues or []

    def execute(self, script: str, context: Dict[str, Any], timeout_seconds: int = 300) -> SandboxResult:
        print(f"[MockSandbox] 在模拟沙箱中执行脚本...\n---SCRIPT---\n{script}\n---END---")
        return SandboxResult(
            success=True,
            exit_code=0,
            stdout="[MockSandbox] 脚本执行成功\n",
            stderr="",
            duration_ms=42.0,
            resource_usage={"cpu_peak": 0.3, "mem_peak": "128m"},
        )

    def preflight_check(self, script: str) -> List[str]:
        return list(self._preflight_issues)

    def is_available(self) -> bool:
        return self._available


class MockAuditStore(IAuditStore):
    """Mock 审计存储：事件保存在内存中，用于测试。"""

    def __init__(self):
        self.events: List[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)

    def query(
        self,
        event_type: Optional[str] = None,
        alert_id: Optional[str] = None,
        start_time=None,
        end_time=None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        results = self.events
        if event_type:
            results = [e for e in results if e.event_type.startswith(event_type)]
        if alert_id:
            results = [e for e in results if e.alert_id == alert_id]
        return results[:limit]

    def verify_chain(self, from_id: str, to_id: str) -> bool:
        return True


class MockAuthProvider(IAuthProvider):
    """Mock 认证提供者：用于测试，硬编码返回指定 Principal。"""

    def __init__(self, principal: Optional[Principal] = None):
        self._principal = principal or Principal(
            principal_id="test-user",
            roles=["operator"],
            name="测试用户",
        )

    def authenticate(self, credentials: Dict[str, Any]) -> Optional[Principal]:
        api_key = credentials.get("api_key")
        if not api_key or api_key == "invalid":
            return None
        return self._principal


class MockPermissionChecker(IPermissionChecker):
    """Mock 权限检查器：全部放行或全部拒绝。"""

    def __init__(self, allow_all: bool = True):
        self._allow_all = allow_all

    def can_read(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        return self._allow_all

    def can_write(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        return self._allow_all

    def can_execute(self, principal: Principal, action: str) -> bool:
        return self._allow_all

