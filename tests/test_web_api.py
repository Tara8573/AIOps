"""Web BFF API 测试（前端展示与交互架构设计 P0）。

覆盖：
- 鉴权（401/有效 key）
- /me 角色探测 + 明文分级
- /overview 聚合（metrics/states/审批挂起）
- /alerts 列表 + 状态筛选
- /alerts/{id}/trace 单告警串联（llm_calls + approvals + 审计）
- /llm-calls 尾读分页 + 筛选 + 明文分级
- /llm-calls/{id} 404/明文
- /approvals 列表 + 决策（RBAC approval.decide）
"""

import json
import threading
import time
from datetime import datetime

import pytest

from core.llm_call_log import LLMCallLogEmitter, content_hash
from core.models import ApprovalRecord, AuditEvent, Principal
from core.observability import metrics, pipeline_states


class FakeAuth:
    """按 key 映射 Principal 的 fake 认证。"""

    def __init__(self, keys=None):
        self._keys = keys or {}

    def authenticate(self, credentials):
        return self._keys.get(credentials.get("api_key"))


class FakeRbac:
    """可开关的 RBAC（对齐 RBACPermissionChecker 语义）。"""

    def __init__(self, enabled=False):
        self._enabled = enabled

    def can_read_access_level(self, principal, access_level):
        if not self._enabled:
            return True
        levels = {"public": 0, "internal": 1, "secret": 2}
        max_by_role = {"viewer": "public", "operator": "internal", "auditor": "internal", "admin": "secret"}
        max_level = "public"
        for role in principal.roles:
            if role == "admin" or max_by_role.get(role, "public") == "secret":
                return True
            max_level = max(max_level, max_by_role.get(role, "public"), key=lambda x: levels[x])
        return levels[max_level] >= levels[access_level]

    def can_execute(self, principal, action):
        if not self._enabled:
            return True
        if "admin" in principal.roles:
            return True
        if action == "approval.decide":
            return "operator" in principal.roles
        return False


class FakeApprovalStore:
    """审批 store fake（decide/list_by_status 语义对齐 pg_approval_store）。"""

    def __init__(self):
        self._records = {}
        self._lock = threading.Lock()

    def submit(self, alert_id, proposal=None, eval_summary=None):
        rec = ApprovalRecord(
            approval_id=f"APR-{len(self._records) + 1}",
            pipeline_run_id="run-1",
            alert_id=alert_id,
            proposal=proposal or {},
            eval_summary=eval_summary or {},
        )
        with self._lock:
            self._records[rec.approval_id] = rec
        return rec

    def decide(self, approval_id, decision, decider="", reason=""):
        with self._lock:
            rec = self._records.get(approval_id)
            if rec is None or rec.status != "pending":
                return False
            rec.status = decision
            rec.decider = decider
            rec.decide_reason = reason
            return True

    def list_by_status(self, status, limit=50):
        with self._lock:
            items = [r for r in self._records.values() if r.status == status]
        return items[:limit]


class FakeAuditStore:
    def __init__(self):
        self._events = []

    def add(self, event_type, alert_id):
        self._events.append(AuditEvent(
            event_id=f"EV-{len(self._events) + 1}",
            timestamp=datetime.now(),
            event_type=event_type,
            alert_id=alert_id,
            severity="info",
            outcome="success",
        ))

    def query(self, event_type=None, alert_id=None, start_time=None, end_time=None, limit=100):
        rows = self._events
        if alert_id:
            rows = [e for e in rows if e.alert_id == alert_id]
        if event_type:
            rows = [e for e in rows if e.event_type == event_type]
        return rows[:limit]

    def verify_chain(self, from_id="", to_id=""):
        return True


class FakeRegistry:
    def find_open_run(self, alert_id, include_suspended=False):
        return None

    def find_stale_runs(self, stale_after_seconds, limit=50):
        return []


@pytest.fixture()
def call_log_file(tmp_path):
    emitter = LLMCallLogEmitter(path=str(tmp_path / "calls.jsonl"))
    emitter.emit_call(
        provider="openai", model="gpt-4", stage="analyze", role="diagnosis",
        prompt="分析告警", response='{"ok": 1}', usage={"prompt_tokens": 10, "completion_tokens": 5},
        outcome="success", alert_id="ALT-1", latency_ms=12.0,
    )
    emitter.emit_call(
        provider="openai", model="gpt-4", stage="retry", role="diagnosis",
        prompt="重试", outcome="error", failure_type="timeout",
        error_summary="TimeoutError", alert_id="ALT-1", latency_ms=50.0,
    )
    emitter.emit_call(
        provider="openai", model="gpt-4", stage="judge", role="evaluation",
        prompt="评审", response='{"verdict": "pass"}', outcome="success", alert_id="ALT-2",
    )
    return emitter


@pytest.fixture()
def client(call_log_file):
    from fastapi.testclient import TestClient
    from interfaces.web_api import create_app

    store = FakeApprovalStore()
    store.submit("ALT-1", proposal={"root_cause_analysis": "磁盘满"}, eval_summary={"risk_level": "Low"})
    audit = FakeAuditStore()
    audit.add("knowledge.start", "ALT-1")
    audit.add("knowledge.complete", "ALT-1")

    app = create_app(
        store=store,
        auth_provider=FakeAuth(keys={
            "viewer-key": Principal(principal_id="u1", roles=["viewer"]),
            "operator-key": Principal(principal_id="u2", roles=["operator"]),
            "admin-key": Principal(principal_id="u3", roles=["admin"]),
        }),
        rbac_checker=FakeRbac(enabled=True),
        llm_log=call_log_file,
        audit_store=audit,
        run_registry=FakeRegistry(),
    )
    return TestClient(app), store


def _auth(key):
    return {"Authorization": f"Bearer {key}"}


class TestAuth:

    def test_missing_token_401(self, client):
        tc, _ = client
        assert tc.get("/api/v1/overview").status_code == 401

    def test_invalid_key_401(self, client):
        tc, _ = client
        assert tc.get("/api/v1/overview", headers=_auth("bad")).status_code == 401

    def test_me_roles_and_plaintext(self, client):
        tc, _ = client
        me = tc.get("/api/v1/me", headers=_auth("operator-key")).json()
        assert me["roles"] == ["operator"]
        assert me["plaintext"] is True
        me_viewer = tc.get("/api/v1/me", headers=_auth("viewer-key")).json()
        assert me_viewer["plaintext"] is False


class TestOverview:

    def test_overview_aggregation(self, client):
        tc, _ = client
        # 全局计数器跨用例累积（流水线测试也打点），用差值断言
        before = metrics.snapshot()["counters"]
        d_correct = before.get("pipeline.diagnosis.correct", 0)
        d_incorrect = before.get("pipeline.diagnosis.incorrect", 0)
        d_req = before.get("llm.provider.openai.request", 0)
        d_ok = before.get("llm.provider.openai.success", 0)
        d_tokens = before.get("genai.client.token.usage.input", 0)
        metrics.incr("pipeline.diagnosis.correct", 8)
        metrics.incr("pipeline.diagnosis.incorrect", 2)
        metrics.incr("llm.provider.openai.request", 10)
        metrics.incr("llm.provider.openai.success", 9)
        metrics.incr("genai.client.token.usage.input", 500)
        ov = tc.get("/api/v1/overview", headers=_auth("operator-key")).json()
        assert ov["diagnosis_accuracy"]["rate"] == round((d_correct + 8) / (d_correct + d_incorrect + 10), 4)
        assert ov["diagnosis_accuracy"]["correct"] == d_correct + 8
        assert ov["llm"]["success_rate"] == round((d_ok + 9) / (d_req + 10), 4)
        assert ov["llm"]["tokens"]["input"] == d_tokens + 500
        assert ov["approval_pending"] == 1  # fake store 初始 pending


class TestAlerts:

    def test_alerts_list_and_filter(self, client):
        tc, _ = client
        pipeline_states.update("ALT-1", "knowledge_search")
        pipeline_states.update("ALT-2", "evaluate")
        rows = tc.get("/api/v1/alerts", headers=_auth("viewer-key")).json()
        assert rows["total"] >= 2
        filtered = tc.get("/api/v1/alerts", params={"state": "evaluate"}, headers=_auth("viewer-key")).json()
        assert all(r["state"] == "evaluate" for r in filtered["items"])

    def test_trace_links_all_surfaces(self, client):
        tc, _ = client
        trace = tc.get("/api/v1/alerts/ALT-1/trace", headers=_auth("operator-key")).json()
        assert trace["plaintext"] is True
        assert len(trace["llm_calls"]) == 2  # analyze + retry
        assert "prompt" in trace["llm_calls"][0]  # operator 可见明文
        assert len(trace["approvals"]) == 1
        assert len(trace["audit_events"]) == 2
        assert len(trace["node_timeline"]) == 2

    def test_trace_plaintext_gated_for_viewer(self, client):
        tc, _ = client
        trace = tc.get("/api/v1/alerts/ALT-1/trace", headers=_auth("viewer-key")).json()
        assert trace["plaintext"] is False
        for call in trace["llm_calls"]:
            assert "prompt" not in call
            assert call["prompt_hash"] != ""
            assert call["prompt_chars"] > 0
        # proposal 也被隐藏
        assert all(a["proposal"] is None for a in trace["approvals"])


class TestLlmCalls:

    def test_pagination_tail_order(self, client):
        tc, _ = client
        page1 = tc.get("/api/v1/llm-calls", params={"limit": 2}, headers=_auth("operator-key")).json()
        assert len(page1["items"]) == 2
        # 最新在前：judge 是最后一次发射
        assert page1["items"][0]["stage"] == "judge"
        assert page1["next_cursor"] == 2
        page2 = tc.get("/api/v1/llm-calls", params={"limit": 2, "cursor": 2}, headers=_auth("operator-key")).json()
        assert len(page2["items"]) == 1
        assert page2["items"][0]["stage"] == "analyze"  # 尾读：最旧一条

    def test_filter_by_alert_and_outcome(self, client):
        tc, _ = client
        rows = tc.get("/api/v1/llm-calls", params={"alert_id": "ALT-1", "outcome": "error"},
                      headers=_auth("operator-key")).json()
        assert len(rows["items"]) == 1
        assert rows["items"][0]["failure_type"] == "timeout"

    def test_plaintext_gated_for_viewer(self, client):
        tc, _ = client
        rows = tc.get("/api/v1/llm-calls", headers=_auth("viewer-key")).json()
        assert rows["plaintext"] is False
        for item in rows["items"]:
            assert "prompt" not in item
            assert item["prompt_hash"] != ""

    def test_call_detail_404(self, client):
        tc, _ = client
        assert tc.get("/api/v1/llm-calls/no-such-id", headers=_auth("operator-key")).status_code == 404

    def test_call_detail_plaintext(self, client):
        tc, store = client
        detail = tc.get("/api/v1/llm-calls", params={"limit": 1}, headers=_auth("admin-key")).json()
        call_id = detail["items"][0]["call_id"]
        one = tc.get(f"/api/v1/llm-calls/{call_id}", headers=_auth("admin-key")).json()
        assert one["call_id"] == call_id


class TestApprovals:

    def test_list_pending(self, client):
        tc, _ = client
        rows = tc.get("/api/v1/approvals", params={"status": "pending"}, headers=_auth("operator-key")).json()
        assert len(rows["items"]) == 1
        assert rows["items"][0]["alert_id"] == "ALT-1"

    def test_decide_approved(self, client):
        tc, store = client
        resp = tc.post(
            "/api/v1/approvals/APR-1/decision",
            json={"decision": "approved", "decider": "ops-1", "reason": "评估通过"},
            headers=_auth("operator-key"),
        )
        assert resp.status_code == 200
        assert store.list_by_status("pending") == []

    def test_decide_rejected_by_viewer_forbidden(self, client):
        tc, _ = client
        resp = tc.post(
            "/api/v1/approvals/APR-1/decision",
            json={"decision": "rejected"},
            headers=_auth("viewer-key"),
        )
        assert resp.status_code == 403

    def test_decide_invalid_value_422(self, client):
        tc, _ = client
        resp = tc.post(
            "/api/v1/approvals/APR-1/decision",
            json={"decision": "maybe"},
            headers=_auth("operator-key"),
        )
        assert resp.status_code == 422

    def test_decide_already_decided_409(self, client):
        """已决策再次决策 → 409（修复：原 web_api 404 与 approval_api 409 不一致，
        统一为 404=不存在 / 409=已决策）"""
        tc, _ = client
        tc.post("/api/v1/approvals/APR-1/decision", json={"decision": "approved"}, headers=_auth("operator-key"))
        resp = tc.post("/api/v1/approvals/APR-1/decision", json={"decision": "rejected"}, headers=_auth("operator-key"))
        assert resp.status_code == 409
