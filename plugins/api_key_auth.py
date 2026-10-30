"""API Key 认证提供者。

基于 SHA256(API key) 与 aiops_users 表比对，返回 Principal。
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

from core.config import pg_dsn
from core.models import Principal
from interfaces.base import IAuthProvider


class ApiKeyAuthProvider(IAuthProvider):
    """API Key 认证。"""

    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or pg_dsn()
        self._pool = None

    def _get_pool(self):
        if self._pool is None:
            import psycopg_pool
            self._pool = psycopg_pool.ConnectionPool(
                self._dsn, min_size=1, max_size=3, open=True,
            )
        return self._pool

    def authenticate(self, credentials: Dict[str, Any]) -> Optional[Principal]:
        api_key = credentials.get("api_key")
        if not api_key:
            return None

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        pool = self._get_pool()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT username, display_name, roles FROM aiops_users "
                "WHERE api_key_hash = %s AND is_active = true",
                (key_hash,),
            ).fetchone()

            if not row:
                return None

            # last_login 更新与 SELECT 同事务同连接（连接归还池前完成，避免
            # 归还后跨线程共用连接 + 事务无显式提交）
            conn.execute(
                "UPDATE aiops_users SET last_login_at = now() WHERE username = %s",
                (row[0],),
            )

        return Principal(
            principal_id=row[0],
            roles=row[2] if isinstance(row[2], list) else list(row[2]),
            name=row[1] or "",
            authenticated_at=datetime.now(),
        )

    def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None
