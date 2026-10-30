"""RBAC 权限检查模块。

基于角色的权限控制，支持 5 类角色（viewer/operator/admin/auditor/service）
和知识分级（public/internal/secret）。
"""

import os
from typing import Dict, List, Optional, Set

from core.models import Principal
from interfaces.base import IPermissionChecker

ROLE_PERMISSIONS: Dict[str, Dict[str, bool]] = {
    "viewer": {
        "kb.read.public": True,
    },
    "operator": {
        "kb.read.public": True,
        "kb.read.internal": True,
        "kb.write": True,
        "approval.execute": True,
        "approval.decide": True,
        "audit.read": True,
    },
    "admin": {
        "*": True,
    },
    "auditor": {
        "kb.read.public": True,
        "kb.read.internal": True,
        "audit.read": True,
        "audit.export": True,
    },
    "service": {
        "kb.read.public": True,
        "kb.read.internal": True,
        "kb.write": True,
        "audit.write": True,
        "approval.execute": False,
        "approval.decide": False,
    },
}

ACCESS_LEVEL_HIERARCHY: Dict[str, int] = {
    "public": 0,
    "internal": 1,
    "secret": 2,
}

ROLE_MAX_ACCESS_LEVEL: Dict[str, str] = {
    "viewer": "public",
    "operator": "internal",
    "admin": "secret",
    "auditor": "internal",
    "service": "internal",
}


class RBACPermissionChecker(IPermissionChecker):
    """基于角色的权限检查器。"""

    def __init__(self, enabled: Optional[bool] = None):
        self._enabled = enabled if enabled is not None else (
            os.getenv("AIOPS_RBAC_ENABLED", "0") == "1"
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _has_permission(self, principal: Principal, permission: str) -> bool:
        if not self._enabled:
            return True
        for role in principal.roles:
            perms = ROLE_PERMISSIONS.get(role, {})
            if perms.get("*") or perms.get(permission):
                return True
        return False

    def can_read(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        return self._has_permission(principal, f"{resource_type}.read.public") or \
               self._has_permission(principal, f"{resource_type}.read.internal") or \
               self._has_permission(principal, f"{resource_type}.read")

    def can_write(self, principal: Principal, resource_type: str, resource_id: str = "") -> bool:
        return self._has_permission(principal, f"{resource_type}.write")

    def can_execute(self, principal: Principal, action: str) -> bool:
        return self._has_permission(principal, action)

    def can_read_access_level(self, principal: Principal, access_level: str) -> bool:
        """检查主体是否有权读取指定分级的知识。"""
        if not self._enabled:
            return True
        level_value = ACCESS_LEVEL_HIERARCHY.get(access_level, 0)
        for role in principal.roles:
            max_level = ROLE_MAX_ACCESS_LEVEL.get(role, "public")
            if ACCESS_LEVEL_HIERARCHY.get(max_level, 0) >= level_value:
                return True
        return False

    def filter_by_access_level(
        self,
        principal: Principal,
        items: List[Dict],
        access_level_field: str = "access_level",
    ) -> List[Dict]:
        """过滤列表，只保留主体有权读取的条目。"""
        if not self._enabled:
            return items
        return [
            item for item in items
            if self.can_read_access_level(
                principal,
                item.get(access_level_field, "public"),
            )
        ]
