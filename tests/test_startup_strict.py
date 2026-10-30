"""FR-8：安全合规验收测试。

验收标准（改造方案 §3.3 / 规格 FR-8）：
- STRICT=1 且 Docker 不可用 → 进程启动即报错（而非降级本地执行）
- 凭据 secretRef 解析（vault:// / env://）+ 日志脱敏（凭据不明文落盘）
- 沙箱执行面最小权限（seccomp/apparmor 接线）
"""

import os

import pytest

from core.logger import redact_text
from main import StartupViolation, resolve_secret_ref
from plugins.credential_manager import DevEnvCredentialProvider, VaultCredentialProvider


class TestStrictStartupCheck:
    def test_strict_startup_fails_without_docker(self, monkeypatch):
        """STRICT=1 且 Docker 不可用 → 启动即报错（而非降级本地执行）。"""
        from main import _strict_startup_check
        from plugins.docker_sandbox import DockerSandbox

        monkeypatch.setenv("AIOPS_SANDBOX_ENABLED", "1")
        monkeypatch.setenv("AIOPS_SANDBOX_STRICT", "1")
        monkeypatch.setattr(DockerSandbox, "is_available", lambda self: False)

        with pytest.raises(StartupViolation):
            _strict_startup_check()

    def test_strict_passes_with_docker(self, monkeypatch):
        from main import _strict_startup_check
        from plugins.docker_sandbox import DockerSandbox

        monkeypatch.setenv("AIOPS_SANDBOX_ENABLED", "1")
        monkeypatch.setenv("AIOPS_SANDBOX_STRICT", "1")
        monkeypatch.setattr(DockerSandbox, "is_available", lambda self: True)
        _strict_startup_check()  # 不抛异常

    def test_no_check_when_sandbox_disabled(self, monkeypatch):
        """沙箱未启用（mock 执行面）→ 不检查（demo/测试不受影响）。"""
        from main import _strict_startup_check
        from plugins.docker_sandbox import DockerSandbox

        monkeypatch.setenv("AIOPS_SANDBOX_ENABLED", "0")
        monkeypatch.setenv("AIOPS_SANDBOX_STRICT", "1")
        monkeypatch.setattr(DockerSandbox, "is_available", lambda self: False)
        _strict_startup_check()  # 不抛异常


class TestSecretRef:
    def test_env_ref_resolved(self, monkeypatch):
        """env:// 引用解析（凭据不明文出现在配置）。"""
        monkeypatch.setenv("AIOPS_SECRET_LLM_API_KEY", "sk-real-key-123")
        assert resolve_secret_ref("env://AIOPS_SECRET_LLM_API_KEY") == "sk-real-key-123"

    def test_vault_ref_resolved(self, monkeypatch):
        """vault:// 引用经 provider 读取（用时取、不留存）。"""
        monkeypatch.setenv("AIOPS_VAULT_URL", "http://vault:8200")
        monkeypatch.setenv("AIOPS_VAULT_TOKEN", "vault-token")
        monkeypatch.setattr(
            VaultCredentialProvider, "get_secret",
            lambda self, path, key=None: "vault-secret-value",
        )
        assert resolve_secret_ref("vault://aiops/llm#api_key") == "vault-secret-value"

    def test_plain_value_passthrough(self):
        """明文值原样返回（兼容，生产不建议）。"""
        assert resolve_secret_ref("plain-key") == "plain-key"
        assert resolve_secret_ref("") == ""

    def test_dev_provider_reads_env(self, monkeypatch):
        monkeypatch.setenv("AIOPS_SECRET_AIOPS_LLM_API_KEY", "dev-key")
        assert DevEnvCredentialProvider().get_secret("aiops/llm", "api_key") == "dev-key"


class TestLogRedaction:
    def test_redact_api_key(self):
        assert redact_text('LLM 调用 api_key="sk-secret-abc" 完成') == \
            'LLM 调用 api_key="***" 完成'

    def test_redact_dsn_password(self):
        out = redact_text("连接 postgresql://postgres:secretpass@host:5432/aiops")
        assert "secretpass" not in out
        assert "postgresql://postgres:***@host:5432/aiops" in out

    def test_redact_bearer_token(self):
        out = redact_text("Authorization: Bearer abc.def.ghi 已过期")
        assert "abc.def.ghi" not in out

    def test_normal_log_untouched(self):
        msg = "[路由] 告警路由完成 | alert_id=ALT-1 | route=standard"
        assert redact_text(msg) == msg


class TestSandboxMinimalPrivilege:
    def test_security_opt_includes_profiles(self, monkeypatch):
        """seccomp/apparmor profile 接线（FR-8 执行面最小权限）。"""
        from plugins.docker_sandbox import DockerSandbox

        monkeypatch.setenv("AIOPS_SANDBOX_SECCOMP_PROFILE", "custom-seccomp.json")
        monkeypatch.setenv("AIOPS_SANDBOX_APPARMOR_PROFILE", "aiops-sandbox")
        sandbox = DockerSandbox()
        opts = sandbox._security_opt()
        assert "no-new-privileges:true" in opts
        assert "seccomp=custom-seccomp.json" in opts
        assert "apparmor=aiops-sandbox" in opts

    def test_whitelist_network_wired(self, monkeypatch):
        """whitelist 网络模式接线（配置字段 → 容器配置）。"""
        from plugins.docker_sandbox import DockerSandbox

        monkeypatch.setenv("AIOPS_SANDBOX_NETWORK_POLICY", "whitelist")
        monkeypatch.setenv("AIOPS_SANDBOX_NETWORK_ALLOW", "aiops-exec-net")
        sandbox = DockerSandbox()
        assert sandbox._network_policy == "whitelist"
        assert sandbox._network_allow == "aiops-exec-net"
