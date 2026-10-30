"""权限体系测试。

覆盖：
- RBACPermissionChecker 角色权限矩阵
- 知识分级访问控制
- Principal 模型
- MockAuthProvider 认证
- MockPermissionChecker 全放行/全拒绝
"""

import pytest

from core.models import Principal
from core.permissions import (
    ACCESS_LEVEL_HIERARCHY,
    RBACPermissionChecker,
    ROLE_MAX_ACCESS_LEVEL,
    ROLE_PERMISSIONS,
)
from plugins.mocks import MockAuthProvider, MockPermissionChecker


class TestRBACPermissionChecker:

    def setup_method(self):
        self.checker = RBACPermissionChecker(enabled=True)

    def test_viewer_can_read_public(self):
        viewer = Principal(principal_id="v1", roles=["viewer"])
        assert self.checker.can_read(viewer, "kb", "any") is True

    def test_viewer_cannot_write(self):
        viewer = Principal(principal_id="v1", roles=["viewer"])
        assert self.checker.can_write(viewer, "kb", "any") is False

    def test_viewer_cannot_execute_approval(self):
        viewer = Principal(principal_id="v1", roles=["viewer"])
        assert self.checker.can_execute(viewer, "approval.execute") is False

    def test_operator_can_read(self):
        op = Principal(principal_id="op1", roles=["operator"])
        assert self.checker.can_read(op, "kb", "any") is True

    def test_operator_can_write(self):
        op = Principal(principal_id="op1", roles=["operator"])
        assert self.checker.can_write(op, "kb", "any") is True

    def test_operator_can_approve(self):
        op = Principal(principal_id="op1", roles=["operator"])
        assert self.checker.can_execute(op, "approval.execute") is True

    def test_admin_has_all_permissions(self):
        admin = Principal(principal_id="admin1", roles=["admin"])
        assert self.checker.can_read(admin, "kb", "any") is True
        assert self.checker.can_write(admin, "kb", "any") is True
        assert self.checker.can_execute(admin, "approval.execute") is True

    def test_auditor_can_read_audit(self):
        auditor = Principal(principal_id="aud1", roles=["auditor"])
        assert self.checker.can_execute(auditor, "audit.read") is True

    def test_auditor_cannot_write(self):
        auditor = Principal(principal_id="aud1", roles=["auditor"])
        assert self.checker.can_write(auditor, "kb", "any") is False

    def test_service_can_write_but_not_approve(self):
        svc = Principal(principal_id="svc1", roles=["service"])
        assert self.checker.can_write(svc, "kb", "any") is True
        assert self.checker.can_execute(svc, "approval.execute") is False

    def test_disabled_allows_all(self):
        checker = RBACPermissionChecker(enabled=False)
        viewer = Principal(principal_id="v1", roles=["viewer"])
        assert checker.can_write(viewer, "kb", "any") is True
        assert checker.can_execute(viewer, "approval.execute") is True

    def test_multiple_roles_union(self):
        """多角色取并集权限"""
        user = Principal(principal_id="u1", roles=["viewer", "auditor"])
        assert user.roles == ["viewer", "auditor"]
        assert self.checker.can_execute(user, "audit.read") is True


class TestAccessLevelControl:

    def setup_method(self):
        self.checker = RBACPermissionChecker(enabled=True)

    def test_viewer_public_only(self):
        viewer = Principal(principal_id="v1", roles=["viewer"])
        assert self.checker.can_read_access_level(viewer, "public") is True
        assert self.checker.can_read_access_level(viewer, "internal") is False
        assert self.checker.can_read_access_level(viewer, "secret") is False

    def test_operator_internal(self):
        op = Principal(principal_id="op1", roles=["operator"])
        assert self.checker.can_read_access_level(op, "public") is True
        assert self.checker.can_read_access_level(op, "internal") is True
        assert self.checker.can_read_access_level(op, "secret") is False

    def test_admin_secret(self):
        admin = Principal(principal_id="admin1", roles=["admin"])
        assert self.checker.can_read_access_level(admin, "public") is True
        assert self.checker.can_read_access_level(admin, "internal") is True
        assert self.checker.can_read_access_level(admin, "secret") is True

    def test_filter_by_access_level(self):
        op = Principal(principal_id="op1", roles=["operator"])
        items = [
            {"id": 1, "access_level": "public"},
            {"id": 2, "access_level": "internal"},
            {"id": 3, "access_level": "secret"},
        ]
        filtered = self.checker.filter_by_access_level(op, items)
        assert len(filtered) == 2
        assert all(i["access_level"] != "secret" for i in filtered)

    def test_filter_disabled_returns_all(self):
        checker = RBACPermissionChecker(enabled=False)
        viewer = Principal(principal_id="v1", roles=["viewer"])
        items = [
            {"id": 1, "access_level": "secret"},
            {"id": 2, "access_level": "internal"},
        ]
        filtered = checker.filter_by_access_level(viewer, items)
        assert len(filtered) == 2

    def test_access_level_hierarchy_values(self):
        assert ACCESS_LEVEL_HIERARCHY["public"] < ACCESS_LEVEL_HIERARCHY["internal"]
        assert ACCESS_LEVEL_HIERARCHY["internal"] < ACCESS_LEVEL_HIERARCHY["secret"]


class TestPrincipalModel:

    def test_defaults(self):
        p = Principal(principal_id="user1")
        assert p.roles == ["viewer"]
        assert p.name == ""

    def test_custom_roles(self):
        p = Principal(principal_id="user1", roles=["operator", "auditor"], name="张三")
        assert len(p.roles) == 2
        assert p.name == "张三"


class TestMockAuthProvider:

    def test_authenticate_success(self):
        provider = MockAuthProvider()
        result = provider.authenticate({"api_key": "valid-key"})
        assert result is not None
        assert result.principal_id == "test-user"

    def test_authenticate_invalid(self):
        provider = MockAuthProvider()
        result = provider.authenticate({"api_key": "invalid"})
        assert result is None

    def test_authenticate_no_key(self):
        provider = MockAuthProvider()
        result = provider.authenticate({})
        assert result is None

    def test_custom_principal(self):
        custom = Principal(principal_id="custom", roles=["admin"], name="管理员")
        provider = MockAuthProvider(principal=custom)
        result = provider.authenticate({"api_key": "any"})
        assert result.principal_id == "custom"


class TestMockPermissionChecker:

    def test_allow_all(self):
        checker = MockPermissionChecker(allow_all=True)
        p = Principal(principal_id="any")
        assert checker.can_read(p, "kb") is True
        assert checker.can_write(p, "kb") is True
        assert checker.can_execute(p, "anything") is True

    def test_deny_all(self):
        checker = MockPermissionChecker(allow_all=False)
        p = Principal(principal_id="any")
        assert checker.can_read(p, "kb") is False
        assert checker.can_write(p, "kb") is False
        assert checker.can_execute(p, "anything") is False


class TestRolePermissionsIntegrity:

    def test_all_roles_defined(self):
        expected = {"viewer", "operator", "admin", "auditor", "service"}
        assert set(ROLE_PERMISSIONS.keys()) == expected

    def test_all_roles_have_max_access_level(self):
        for role in ROLE_PERMISSIONS:
            assert role in ROLE_MAX_ACCESS_LEVEL

    def test_admin_has_wildcard(self):
        assert ROLE_PERMISSIONS["admin"].get("*") is True
