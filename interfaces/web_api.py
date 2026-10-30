"""Web BFF/API（前端展示与交互架构设计 P0，规格：docs/前端展示与交互架构设计.md）。

读模型端点（viewer+）+ 已有命令复用（审批决策）。前端不直连 PG/Prometheus，全部经此层。

- GET  /api/v1/me                          Principal 探测（登录页角色识别）
- GET  /api/v1/overview                    概览聚合（状态分布/准确率/评估/降级/积压/熔断/LLM）
- GET  /api/v1/system                      系统健康（熔断/降级/lag/积压/僵死 run）
- GET  /api/v1/alerts                      告警列表（pipeline_states 快照）
- GET  /api/v1/alerts/{alert_id}/trace     单告警全链路串联（run + llm_calls + approvals + audit）
- GET  /api/v1/llm-calls                   调用事件日志（尾读 cursor 分页 + 筛选下推；明文按 RBAC 分级）
- GET  /api/v1/llm-calls/{call_id}         单事件全量
- GET  /api/v1/approvals?status=           审批列表（复用 approval store）
- POST /api/v1/approvals/{id}/decision     审批决策（复用 approval store，RBAC approval.decide）

鉴权：AIOPS_WEB_API_AUTH=1（默认）走 ApiKeyAuthProvider（Bearer api_key → Principal）。
明文分级：llm_calls 的 prompt/response 明文 = RBAC 知识分级 internal 及以上
（operator/admin/auditor 可看，viewer 仅 hash/长度）——复用 can_read_access_level，不新增权限点。

生产部署（独立进程）:
    uvicorn interfaces.web_api:app --host 0.0.0.0 --port 9093
进程内（可选）:
    start_web_api(...)  # AIOPS_WEB_API_ENABLED=1
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from core.logger import get_logger
from core.observability import metrics, pipeline_states

logger = get_logger("web_api")

_MAX_PAGE = 100
_STALE_RUN_SECONDS = int(os.getenv("AIOPS_WEB_STALE_RUN_SECONDS", "300"))


class DecisionBody(BaseModel):
    """人工决策请求体（同 approval_api）。"""
    decision: str  # approved | rejected
    decider: Optional[str] = None
    reason: Optional[str] = None


def _read_llm_call_events(
    path: str,
    *,
    alert_id: Optional[str] = None,
    stage: Optional[str] = None,
    provider: Optional[str] = None,
    outcome: Optional[str] = None,
    failure_type: Optional[str] = None,
    call_id: Optional[str] = None,
    cursor: int = 0,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """读取 llm_calls.jsonl（append-only，尾读：最新在前），筛选下推 + cursor 分页。

    cursor = 已从末尾消费的行数；next_cursor = cursor + len(返回)。
    """
    if not os.path.exists(path):
        return []
    matched: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue  # 半行（崩溃残留）跳过
            if alert_id and event.get("alert_id") != alert_id:
                continue
            if stage and event.get("stage") != stage:
                continue
            if provider and event.get("provider") != provider:
                continue
            if outcome and event.get("outcome") != outcome:
                continue
            if failure_type and event.get("failure_type") != failure_type:
                continue
            if call_id and event.get("call_id") != call_id:
                continue
            matched.append(event)
    # 尾读：最新在前，跳过 cursor
    end = len(matched) - cursor
    start = max(0, end - limit)
    return list(reversed(matched[start:end])) if end > 0 else []


def create_app(
    store=None,
    auth_provider=None,
    rbac_checker=None,
    llm_log=None,
    run_registry=None,
    audit_store=None,
    pipeline=None,
):
    """构建 Web BFF FastAPI 应用。fastapi 延迟导入（可选依赖）。"""
    try:
        from fastapi import Depends, FastAPI, HTTPException
        from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    except ImportError as exc:
        raise RuntimeError("Web API 需要 fastapi：pip install fastapi uvicorn") from exc

    from plugins.api_key_auth import ApiKeyAuthProvider
    from core.permissions import RBACPermissionChecker

    _auth_provider = auth_provider or ApiKeyAuthProvider()
    _rbac = rbac_checker or RBACPermissionChecker()
    _store = store
    _run_registry = run_registry
    _audit_store = audit_store
    _llm_log = llm_log  # core.llm_call_log.LLMCallLogEmitter（取 snapshot/路径）
    _pipeline = pipeline

    app = FastAPI(title="AIOps Web API", version="1.0")
    bearer = HTTPBearer(auto_error=False)
    auth_required = os.getenv("AIOPS_WEB_API_AUTH", "1") == "1"

    def _principal(
        creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer),
    ):
        """鉴权依赖：Bearer api_key → Principal。"""
        if not auth_required:
            from core.models import Principal
            return Principal(principal_id="anonymous", roles=["admin"], name="anonymous")
        if creds is None:
            raise HTTPException(status_code=401, detail="missing bearer token")
        principal = _auth_provider.authenticate({"api_key": creds.credentials})
        if principal is None:
            raise HTTPException(status_code=401, detail="invalid api key")
        return principal

    def _plaintext_allowed(principal) -> bool:
        """明文分级：RBAC 知识分级 internal 及以上（operator/admin/auditor）。"""
        return _rbac.can_read_access_level(principal, "internal")

    def _strip_plaintext(event: Dict[str, Any]) -> Dict[str, Any]:
        """无明文权限时仅保留 hash/长度（规格 §7 PlainTextGate）。"""
        clean = dict(event)
        clean.pop("prompt", None)
        clean.pop("response", None)
        return clean

    # ── 鉴权探测 ──

    @app.get("/api/v1/me")
    def me(principal=Depends(_principal)):
        return {
            "principal_id": principal.principal_id,
            "name": principal.name,
            "roles": principal.roles,
            "plaintext": _plaintext_allowed(principal),
        }

    # ── 概览 / 系统健康 ──

    def _llm_summary(snap: Dict[str, Any]) -> Dict[str, Any]:
        counters = snap.get("counters", {})
        requested = sum(
            counters.get(f"llm.provider.{p}.request", 0) for p in ("openai", "ollama")
        )
        succeeded = sum(
            counters.get(f"llm.provider.{p}.success", 0) for p in ("openai", "ollama")
        )
        latencies = snap.get("latencies", {}).get("genai.client.operation.duration.ms", {})
        return {
            "requested": requested,
            "success_rate": round(succeeded / requested, 4) if requested else None,
            "parse_failure": counters.get("llm.parse.failure", 0),
            "avg_latency_ms": latencies.get("avg_ms"),
            "tokens": {
                "input": counters.get("genai.client.token.usage.input", 0),
                "output": counters.get("genai.client.token.usage.output", 0),
                "cache_read": counters.get("genai.client.token.usage.cache_read", 0),
                "reasoning": counters.get("genai.client.token.usage.reasoning", 0),
            },
        }

    def _evaluation_summary(snap: Dict[str, Any]) -> Dict[str, Any]:
        counters = snap.get("counters", {})
        blocked = {
            k.replace("pipeline.evaluation.blocked.", ""): v
            for k, v in counters.items()
            if k.startswith("pipeline.evaluation.blocked.")
        }
        return {
            "passed": counters.get("pipeline.evaluation.passed", 0),
            "blocked": blocked,
        }

    def _diagnosis_accuracy(snap: Dict[str, Any]) -> Dict[str, Any]:
        counters = snap.get("counters", {})
        correct = counters.get("pipeline.diagnosis.correct", 0)
        incorrect = counters.get("pipeline.diagnosis.incorrect", 0)
        total = correct + incorrect
        return {
            "correct": correct,
            "incorrect": incorrect,
            "rate": round(correct / total, 4) if total else None,
        }

    def _circuit_info() -> Optional[Dict[str, Any]]:
        """从 pipeline 的 LLM 包装链中找 CircuitBreakerLLM（防御式，找不到返回 None）。"""
        if _pipeline is None:
            return None
        client = None
        for attr in ("llm_client", "_llm_client"):
            client = getattr(_pipeline, attr, None)
            if client is not None:
                break
        node = client
        from core.circuit_breaker import CircuitBreakerLLM
        while node is not None:
            if isinstance(node, CircuitBreakerLLM):
                return {"state": node._state, "timeout_rate": node.timeout_rate()}
            node = getattr(node, "_wrapped", None)
        return None

    def _degradation_level() -> Optional[int]:
        if _pipeline is None:
            return None
        degradation = getattr(_pipeline, "_degradation", None)
        if degradation is None:
            return None
        level = getattr(degradation, "_forced_level", None) or getattr(degradation, "level", "NORMAL")
        return {"NORMAL": 0, "L1": 1, "L2": 2}.get(level, 0)

    def _worker_backlog() -> Optional[int]:
        if _pipeline is None:
            return None
        queue = getattr(_pipeline, "_ingest_queue", None)
        if queue is None:
            return None
        backlog = queue.qsize()
        if hasattr(queue, "storm_parked_size"):
            backlog += queue.storm_parked_size()
        return backlog

    def _kafka_lag() -> Optional[int]:
        if _pipeline is None:
            return None
        source = getattr(_pipeline, "alert_source", None)
        if source is not None and hasattr(source, "lag"):
            try:
                return source.lag()
            except Exception:
                return None
        return None

    @app.get("/api/v1/overview")
    def overview(principal=Depends(_principal)):
        snap = metrics.snapshot()
        states: Dict[str, int] = {}
        for st in pipeline_states.snapshot().values():
            name = st.get("state", "unknown")
            states[name] = states.get(name, 0) + 1
        approval_pending = None
        if _store is not None:
            try:
                approval_pending = len(_store.list_by_status("pending", limit=1000))
            except Exception:
                approval_pending = None
        return {
            "alerts_state": states,
            "diagnosis_accuracy": _diagnosis_accuracy(snap),
            "evaluation": _evaluation_summary(snap),
            "llm": _llm_summary(snap),
            "degradation_level": _degradation_level(),
            "worker_backlog": _worker_backlog(),
            "kafka_lag": _kafka_lag(),
            "approval_pending": approval_pending,
            "circuit": _circuit_info(),
        }

    @app.get("/api/v1/system")
    def system(principal=Depends(_principal)):
        stale_runs = []
        if _run_registry is not None:
            try:
                stale = _run_registry.find_stale_runs(_STALE_RUN_SECONDS)
                stale_runs = [
                    {"run_id": r.run_id, "alert_id": r.alert_id, "status": r.status,
                     "started_at": r.started_at.isoformat(), "updated_at": r.updated_at.isoformat()}
                    for r in stale
                ]
            except Exception:
                stale_runs = []
        return {
            "circuit": _circuit_info(),
            "degradation_level": _degradation_level(),
            "kafka_lag": _kafka_lag(),
            "worker_backlog": _worker_backlog(),
            "stale_runs": stale_runs,
        }

    # ── 告警 ──

    @app.get("/api/v1/alerts")
    def alerts(
        state: Optional[str] = None,
        page: int = 0,
        limit: int = 50,
        principal=Depends(_principal),
    ):
        limit = min(max(1, limit), _MAX_PAGE)
        rows = [
            {"alert_id": alert_id, **payload}
            for alert_id, payload in pipeline_states.snapshot().items()
        ]
        if state:
            rows = [r for r in rows if r.get("state") == state]
        rows.sort(key=lambda r: r.get("updated_at", 0), reverse=True)
        window = rows[page * limit:(page + 1) * limit]
        return {"total": len(rows), "page": page, "limit": limit, "items": window}

    @app.get("/api/v1/alerts/{alert_id}/trace")
    def alert_trace(alert_id: str, principal=Depends(_principal)):
        plaintext = _plaintext_allowed(principal)

        llm_calls: List[Dict[str, Any]] = []
        llm_log_path = getattr(_llm_log, "_path", None) if _llm_log else None
        if llm_log_path:
            llm_calls = _read_llm_call_events(llm_log_path, alert_id=alert_id, limit=_MAX_PAGE)
            if not plaintext:
                llm_calls = [_strip_plaintext(e) for e in llm_calls]

        approvals: List[Dict[str, Any]] = []
        if _store is not None:
            for status in ("pending", "approved", "rejected", "resumed", "expired"):
                try:
                    for rec in _store.list_by_status(status, limit=100):
                        if rec.alert_id == alert_id:
                            approvals.append({
                                "approval_id": rec.approval_id,
                                "pipeline_run_id": rec.pipeline_run_id,
                                "status": rec.status,
                                "decider": rec.decider,
                                "decide_reason": rec.decide_reason,
                                "created_at": rec.created_at.isoformat() if rec.created_at else None,
                                "eval_summary": rec.eval_summary,
                                "proposal": rec.proposal if plaintext else None,
                            })
                except Exception:
                    continue

        audit_events: List[Dict[str, Any]] = []
        if _audit_store is not None:
            try:
                for ev in _audit_store.query(alert_id=alert_id, limit=100):
                    audit_events.append({
                        "event_id": ev.event_id,
                        "event_type": ev.event_type,
                        "timestamp": ev.timestamp.isoformat() if ev.timestamp else None,
                        "severity": ev.severity,
                        "outcome": ev.outcome,
                    })
            except Exception:
                audit_events = []

        run = None
        if _run_registry is not None:
            try:
                run_rec = _run_registry.find_open_run(alert_id, include_suspended=True)
                if run_rec:
                    run = {"run_id": run_rec.run_id, "status": run_rec.status,
                           "started_at": run_rec.started_at.isoformat()}
            except Exception:
                run = None

        # 节点时间线：从审计事件推导（audit_node 装饰器发射 {node}.start/complete）
        node_timeline = [
            {"node": ev["event_type"].rsplit(".", 1)[0], "phase": ev["event_type"].rsplit(".", 1)[1],
             "timestamp": ev["timestamp"], "outcome": ev["outcome"]}
            for ev in audit_events
            if ev["event_type"].endswith((".start", ".complete"))
        ]

        return {
            "alert_id": alert_id,
            "run": run,
            "node_timeline": node_timeline,
            "llm_calls": llm_calls,
            "approvals": approvals,
            "audit_events": audit_events,
            "plaintext": plaintext,
        }

    # ── LLM 调用观测 ──

    @app.get("/api/v1/llm-calls")
    def llm_calls(
        alert_id: Optional[str] = None,
        stage: Optional[str] = None,
        provider: Optional[str] = None,
        outcome: Optional[str] = None,
        failure_type: Optional[str] = None,
        cursor: int = 0,
        limit: int = 50,
        principal=Depends(_principal),
    ):
        limit = min(max(1, limit), _MAX_PAGE)
        llm_log_path = getattr(_llm_log, "_path", None) if _llm_log else None
        if not llm_log_path:
            return {"items": [], "next_cursor": None, "plaintext": _plaintext_allowed(principal)}
        events = _read_llm_call_events(
            llm_log_path, alert_id=alert_id, stage=stage, provider=provider,
            outcome=outcome, failure_type=failure_type, cursor=cursor, limit=limit,
        )
        plaintext = _plaintext_allowed(principal)
        if not plaintext:
            events = [_strip_plaintext(e) for e in events]
        consumed = cursor + len(events) if events else None
        return {"items": events, "next_cursor": consumed, "plaintext": plaintext}

    @app.get("/api/v1/llm-calls/{call_id}")
    def llm_call_detail(call_id: str, principal=Depends(_principal)):
        llm_log_path = getattr(_llm_log, "_path", None) if _llm_log else None
        if not llm_log_path:
            raise HTTPException(status_code=404, detail="llm call log unavailable")
        events = _read_llm_call_events(llm_log_path, call_id=call_id, limit=1)
        if not events:
            raise HTTPException(status_code=404, detail="call not found")
        if not _plaintext_allowed(principal):
            return _strip_plaintext(events[0])
        return events[0]

    # ── 审批（复用 approval store；与 approval_api 同语义）──

    @app.get("/api/v1/approvals")
    def approvals(
        status: str = "pending",
        limit: int = 50,
        principal=Depends(_principal),
    ):
        if _store is None:
            return {"items": [], "plaintext": _plaintext_allowed(principal)}
        records = _store.list_by_status(status, limit=min(limit, _MAX_PAGE))
        plaintext = _plaintext_allowed(principal)
        return {
            "items": [
                {
                    "approval_id": rec.approval_id,
                    "pipeline_run_id": rec.pipeline_run_id,
                    "alert_id": rec.alert_id,
                    "status": rec.status,
                    "decider": rec.decider,
                    "decide_reason": rec.decide_reason,
                    "created_at": rec.created_at.isoformat() if rec.created_at else None,
                    "timeout_at": rec.timeout_at.isoformat() if rec.timeout_at else None,
                    "eval_summary": rec.eval_summary,
                    "proposal": rec.proposal if plaintext else None,
                }
                for rec in records
            ],
            "plaintext": plaintext,
        }

    @app.post("/api/v1/approvals/{approval_id}/decision")
    def approve_decision(
        approval_id: str,
        body: DecisionBody,
        principal=Depends(_principal),
    ):
        if body.decision not in ("approved", "rejected"):
            raise HTTPException(status_code=422, detail="decision 必须是 approved | rejected")
        if not _rbac.can_execute(principal, "approval.decide"):
            raise HTTPException(status_code=403, detail="no approval.decide permission")
        if _store is None:
            raise HTTPException(status_code=503, detail="approval store unavailable")
        # 修复：与 approval_api 错误映射对齐 —— 不存在 404，已决策 409。
        # 仅当 store 支持查询（get）时做精确区分；接口仅含 decide 的实现回退
        # 统一 409（decide 失败即不存在或已决策）
        existing = None
        if hasattr(_store, "get"):
            try:
                existing = _store.get(approval_id)
            except Exception:
                existing = None
            if existing is None:
                raise HTTPException(status_code=404, detail="approval not found")
        ok = _store.decide(
            approval_id, body.decision,
            decider=body.decider or principal.principal_id,
            reason=body.reason or "",
        )
        if not ok:
            if existing is not None:
                raise HTTPException(status_code=409, detail="approval already decided")
            raise HTTPException(status_code=409, detail="approval not found or already decided")
        return {"approval_id": approval_id, "decision": body.decision}

    return app


# 独立启动：uvicorn interfaces.web_api:app --host 127.0.0.1 --port 9093
# （调试/独立部署用；进程内模式用 start_web_api，见 main._maybe_start_web_api）
app = create_app()


def start_web_api(
    store=None,
    auth_provider=None,
    llm_log=None,
    run_registry=None,
    audit_store=None,
    pipeline=None,
    port: Optional[int] = None,
):
    """进程内启动 Web API（AIOPS_WEB_API_ENABLED=1）。"""
    import threading
    import uvicorn

    port = port or int(os.getenv("AIOPS_WEB_API_PORT", "9093"))
    app = create_app(
        store=store, auth_provider=auth_provider, llm_log=llm_log,
        run_registry=run_registry, audit_store=audit_store, pipeline=pipeline,
    )
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning"))
    threading.Thread(target=server.run, name="web-api", daemon=True).start()
    logger.logger.info("Web API 已启动 | port={}", port)
    return server
