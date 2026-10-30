"""审批回调 API（FR-3 人工确认入口契约）。

最小 FastAPI 服务，Web/IM 人工确认的回调入口：
- POST /api/v1/approvals/{approval_id}/decision   人工决策（approved | rejected）
- GET  /api/v1/approvals?status=pending           审批列表（轮询/推送源）

设计：只写库不直接恢复 —— 由 ApprovalResumer 统一捡起（单一恢复路径）。
鉴权：AIOPS_APPROVAL_API_AUTH=1 时走 ApiKeyAuthProvider（RBAC 权限点
approval.decide：operator/admin 可决策，service/viewer/auditor 不可）。

生产部署（独立进程）:
    uvicorn interfaces.approval_api:app --host 0.0.0.0 --port 9092
进程内（可选）:
    start_approval_api()  # AIOPS_APPROVAL_API_ENABLED=1
"""

import os
from typing import List, Optional

from pydantic import BaseModel

from core.logger import get_logger
from core.models import ApprovalRecord

logger = get_logger("approval_api")


class DecisionBody(BaseModel):
    """人工决策请求体。"""
    decision: str  # approved | rejected
    decider: Optional[str] = None
    reason: Optional[str] = None


def create_app(store=None, auth_provider=None, rbac_checker=None):
    """构建 FastAPI 应用。fastapi 延迟导入（可选依赖）。"""
    try:
        from fastapi import Depends, FastAPI, HTTPException
        from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    except ImportError as exc:
        raise RuntimeError("审批回调 API 需要 fastapi：pip install fastapi uvicorn") from exc

    from plugins.pg_approval_store import PgApprovalRequestStore
    from plugins.api_key_auth import ApiKeyAuthProvider
    from core.permissions import RBACPermissionChecker

    _store = store or PgApprovalRequestStore()
    _auth_provider = auth_provider or ApiKeyAuthProvider()
    _rbac = rbac_checker or RBACPermissionChecker()

    app = FastAPI(title="AIOps Approval API", version="1.0")
    bearer = HTTPBearer(auto_error=False)
    auth_required = os.getenv("AIOPS_APPROVAL_API_AUTH", "1") == "1"

    def _principal(
        creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer),
    ):
        """鉴权依赖：Bearer api_key → Principal（RBAC: approval.decide 权限点）。"""
        if not auth_required:
            return None
        if creds is None:
            raise HTTPException(status_code=401, detail="缺少 Authorization Bearer 凭据")
        principal = _auth_provider.authenticate({"api_key": creds.credentials})
        if principal is None:
            raise HTTPException(status_code=401, detail="认证失败")
        if not _rbac.can_execute(principal, "approval.decide"):
            raise HTTPException(status_code=403, detail="无审批决策权限（approval.decide）")
        return principal

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/api/v1/approvals")
    def list_approvals(
        status: str = "pending",
        limit: int = 50,
        principal=Depends(_principal),
    ) -> dict:
        records: List[ApprovalRecord] = _store.list_by_status(status, limit=max(1, limit))
        # 修复：明文分级 gate（与 web_api 对齐）—— 无 internal 分级权限者
        # 不返回 proposal 明文（含 script_content）
        plaintext = bool(_rbac.can_read_access_level(principal, "internal")) if principal else False
        return {
            "items": [
                r.model_dump(mode="json") if plaintext
                else {**r.model_dump(mode="json", exclude={"proposal"}), "proposal": None}
                for r in records
            ],
            "count": len(records),
            "plaintext": plaintext,
        }

    @app.post("/api/v1/approvals/{approval_id}/decision")
    def decide(
        approval_id: str,
        body: DecisionBody,
        principal=Depends(_principal),
    ) -> dict:
        if body.decision not in ("approved", "rejected"):
            raise HTTPException(status_code=422, detail="decision 仅支持 approved | rejected")
        # 修复：错误映射与 web_api 对齐 —— 不存在 404，已决策 409。
        # 仅当 store 支持查询（get）时精确区分；否则回退统一 409
        existing = None
        if hasattr(_store, "get"):
            try:
                existing = _store.get(approval_id)
            except Exception:
                existing = None
            if existing is None:
                raise HTTPException(status_code=404, detail="审批不存在")
        decider = body.decider or (principal.principal_id if principal else "anonymous")
        ok = _store.decide(approval_id, body.decision, decider=decider, reason=body.reason or "")
        if not ok:
            if existing is not None:
                raise HTTPException(status_code=409, detail="审批已决策")
            raise HTTPException(status_code=409, detail="审批不存在或已决策")
        # 只写库不恢复 —— ApprovalResumer 统一捡起（单一恢复路径）
        logger.logger.info(
            "[审批API] 人工决策已写入 | approval_id={} | decision={} | decider={}",
            approval_id, body.decision, decider,
        )
        return {"approval_id": approval_id, "decision": body.decision, "decider": decider}

    return app


def start_approval_api(store=None, auth_provider=None, port: Optional[int] = None):
    """进程内启动审批回调 API（AIOPS_APPROVAL_API_ENABLED=1），返回 uvicorn Server。"""
    import threading
    import uvicorn

    port = port or int(os.getenv("AIOPS_APPROVAL_API_PORT", "9092"))
    app = create_app(store=store, auth_provider=auth_provider)
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning"))
    threading.Thread(target=server.run, name="approval-api", daemon=True).start()
    logger.logger.info("审批回调 API 已启动 | port={}", port)
    return server
