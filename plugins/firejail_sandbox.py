"""Firejail 轻量沙箱实现。

当 Docker 不可用时，使用 firejail 提供轻量级进程隔离：
- 文件系统白名单
- 网络隔离
- 资源限制（通过 ulimit / cgroup）
- seccomp 系统调用过滤
"""

import os
import shutil
import subprocess
import tempfile
import time
from typing import Any, Dict, List

from core.models import SandboxResult
from interfaces.base import ISandbox

FIREJAIL_ESCAPE_PATTERNS = [
    (r"--noprofile", "绕过 firejail profile"),
    (r"--overlay", "尝试使用 overlay 逃逸"),
    (r"--private-dev", "访问设备文件"),
]


class FirejailSandbox(ISandbox):
    """基于 firejail 的轻量级沙箱。"""

    def __init__(self):
        self._timeout = int(os.getenv("AIOPS_SANDBOX_TIMEOUT_SECONDS", "300"))
        self._memory_limit = os.getenv("AIOPS_SANDBOX_MEMORY_LIMIT", "512m")
        self._network_policy = os.getenv("AIOPS_SANDBOX_NETWORK_POLICY", "deny_all")
        self._strict = os.getenv("AIOPS_SANDBOX_STRICT", "1") == "1"
        self._seccomp = os.getenv("AIOPS_SANDBOX_SECCOMP_PROFILE", "default") != "none"

    def execute(
        self,
        script: str,
        context: Dict[str, Any],
        timeout_seconds: int = 0,
    ) -> SandboxResult:
        timeout = timeout_seconds or self._timeout

        violations = self.preflight_check(script)
        if violations:
            return SandboxResult(
                success=False,
                exit_code=-1,
                stderr=f"Preflight 检查失败: {violations}",
                sandbox_violations=violations,
            )

        if not self.is_available():
            if self._strict:
                return SandboxResult(
                    success=False,
                    exit_code=-2,
                    stderr="firejail 不可用且 STRICT 模式已开启",
                )
            return self._bare_execute(script, timeout)

        return self._firejail_execute(script, timeout)

    def _firejail_execute(self, script: str, timeout: int) -> SandboxResult:
        start_time = time.perf_counter()

        with tempfile.TemporaryDirectory(prefix="aiops_fj_") as workspace:
            script_path = os.path.join(workspace, "fix.sh")
            with open(script_path, "w", encoding="utf-8") as f:
                f.write("#!/bin/bash\nset -euo pipefail\n")
                f.write(script)
            os.chmod(script_path, 0o755)

            cmd = ["firejail"]
            cmd.extend(["--quiet", "--nonewprivs"])
            cmd.append(f"--private={workspace}")

            if self._network_policy == "deny_all":
                cmd.append("--net=none")

            if self._seccomp:
                cmd.append("--seccomp")

            mem_mb = self._parse_memory_mb(self._memory_limit)
            if mem_mb > 0:
                cmd.append(f"--rlimit-as={mem_mb * 1024 * 1024}")

            cmd.append("--caps.drop=all")
            cmd.extend(["/bin/bash", "/workspace/fix.sh"])

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=timeout,
                    text=True,
                    cwd=workspace,
                )
                duration_ms = (time.perf_counter() - start_time) * 1000
                return SandboxResult(
                    success=(result.returncode == 0),
                    exit_code=result.returncode,
                    stdout=result.stdout[:10000],
                    stderr=result.stderr[:10000],
                    duration_ms=round(duration_ms, 2),
                )
            except subprocess.TimeoutExpired:
                duration_ms = (time.perf_counter() - start_time) * 1000
                return SandboxResult(
                    success=False,
                    exit_code=-1,
                    stderr=f"脚本执行超时 ({timeout}s)",
                    duration_ms=round(duration_ms, 2),
                )
            except Exception as exc:
                duration_ms = (time.perf_counter() - start_time) * 1000
                return SandboxResult(
                    success=False,
                    exit_code=-1,
                    stderr=str(exc),
                    duration_ms=round(duration_ms, 2),
                )

    def _bare_execute(self, script: str, timeout: int) -> SandboxResult:
        """无隔离的降级执行。"""
        start_time = time.perf_counter()
        try:
            result = subprocess.run(
                ["/bin/bash", "-c", script],
                capture_output=True,
                timeout=timeout,
                text=True,
            )
            duration_ms = (time.perf_counter() - start_time) * 1000
            return SandboxResult(
                success=(result.returncode == 0),
                exit_code=result.returncode,
                stdout=result.stdout[:10000],
                stderr=result.stderr[:10000],
                duration_ms=round(duration_ms, 2),
                sandbox_violations=["未隔离执行（firejail 不可用，降级模式）"],
            )
        except subprocess.TimeoutExpired:
            duration_ms = (time.perf_counter() - start_time) * 1000
            return SandboxResult(
                success=False, exit_code=-1,
                stderr=f"脚本执行超时 ({timeout}s)",
                duration_ms=round(duration_ms, 2),
            )

    def preflight_check(self, script: str) -> List[str]:
        import re
        issues = []
        for pattern, description in FIREJAIL_ESCAPE_PATTERNS:
            if re.search(pattern, script, re.IGNORECASE):
                issues.append(description)
        return issues

    def is_available(self) -> bool:
        return shutil.which("firejail") is not None

    @staticmethod
    def _parse_memory_mb(mem_str: str) -> int:
        mem_str = mem_str.strip().lower()
        if mem_str.endswith("g"):
            return int(mem_str[:-1]) * 1024
        if mem_str.endswith("m"):
            return int(mem_str[:-1])
        if mem_str.endswith("k"):
            return int(mem_str[:-1]) // 1024
        try:
            return int(mem_str) // (1024 * 1024)
        except ValueError:
            return 512
