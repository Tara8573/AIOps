"""Docker 容器沙箱实现。

基于 Docker 容器的脚本执行隔离，提供：
- 文件系统只读挂载 + tmpfs 工作目录
- 网络隔离（deny_all / whitelist / allow_all）
- CPU / 内存 / PID / 超时限制
- 非 root 用户执行 + 丢弃所有 capabilities
- 容器逃逸检测（preflight）
"""

import os
import re
import tempfile
import time
from typing import Any, Dict, List

from core.models import SandboxResult
from interfaces.base import ISandbox

ESCAPE_PATTERNS = [
    (r"/var/run/docker\.sock", "访问 Docker socket（容器逃逸）"),
    (r"--privileged", "特权模式标志"),
    (r"--cap-add", "添加 Linux capabilities"),
    (r"\bnsenter\b", "nsenter 进入其他 namespace"),
    (r"/proc/1/ns", "访问宿主机 namespace"),
    (r"/proc/self/fd", "通过 fd 访问宿主文件系统"),
    (r"mount\s+-t\s+", "尝试挂载文件系统"),
    (r"unshare\b", "创建新 namespace"),
    (r"pivot_root\b", "更改根文件系统"),
    (r"chroot\b", "更改根目录"),
    (r"LD_PRELOAD", "动态链接劫持"),
]


class DockerSandbox(ISandbox):
    """基于 Docker 容器的脚本执行沙箱。"""

    def __init__(self):
        self._image = os.getenv("AIOPS_SANDBOX_IMAGE", "aiops-sandbox:latest")
        self._cpu_limit = float(os.getenv("AIOPS_SANDBOX_CPU_LIMIT", "1.0"))
        self._memory_limit = os.getenv("AIOPS_SANDBOX_MEMORY_LIMIT", "512m")
        self._pids_limit = int(os.getenv("AIOPS_SANDBOX_PIDS_LIMIT", "64"))
        self._timeout = int(os.getenv("AIOPS_SANDBOX_TIMEOUT_SECONDS", "300"))
        self._network_policy = os.getenv("AIOPS_SANDBOX_NETWORK_POLICY", "deny_all")
        self._network_allow = os.getenv("AIOPS_SANDBOX_NETWORK_ALLOW", "")
        # FR-8: seccomp/apparmor（配置已有字段，此处接线）
        self._seccomp_profile = os.getenv("AIOPS_SANDBOX_SECCOMP_PROFILE", "default")
        self._apparmor_profile = os.getenv("AIOPS_SANDBOX_APPARMOR_PROFILE", "docker-default")
        self._readonly_rootfs = os.getenv("AIOPS_SANDBOX_READONLY_ROOTFS", "1") == "1"
        self._user = os.getenv("AIOPS_SANDBOX_USER", "nobody")
        self._strict = os.getenv("AIOPS_SANDBOX_STRICT", "1") == "1"
        self._client = None

    def _get_client(self):
        if self._client is None:
            import docker
            self._client = docker.from_env()
        return self._client

    def _security_opt(self):
        """security_opt 列表：no-new-privileges + seccomp/apparmor profile（配置接线）。"""
        opts = ["no-new-privileges:true"]
        if self._seccomp_profile and self._seccomp_profile != "default":
            opts.append(f"seccomp={self._seccomp_profile}")
        if self._apparmor_profile and self._apparmor_profile != "none":
            opts.append(f"apparmor={self._apparmor_profile}")
        return opts

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

        try:
            client = self._get_client()
            client.ping()
        except Exception as exc:
            if self._strict:
                return SandboxResult(
                    success=False,
                    exit_code=-2,
                    stderr=f"Docker 不可用且 STRICT 模式已开启: {exc}",
                )
            return self._fallback_execute(script, timeout)

        return self._docker_execute(client, script, timeout)

    def _docker_execute(self, client, script: str, timeout: int) -> SandboxResult:
        start_time = time.perf_counter()

        with tempfile.TemporaryDirectory(prefix="aiops_sandbox_") as workspace:
            script_path = os.path.join(workspace, "fix.sh")
            with open(script_path, "w", encoding="utf-8") as f:
                f.write("#!/bin/bash\nset -euo pipefail\n")
                f.write(script)
            os.chmod(script_path, 0o755)

            container_config = {
                "image": self._image,
                "command": ["/bin/bash", "/workspace/fix.sh"],
                "volumes": {
                    workspace: {"bind": "/workspace", "mode": "rw"},
                },
                "cpu_period": 100000,
                "cpu_quota": int(self._cpu_limit * 100000),
                "mem_limit": self._memory_limit,
                "pids_limit": self._pids_limit,
                "read_only": self._readonly_rootfs,
                "tmpfs": {"/tmp": "size=512m,noexec,nosuid"},
                "user": self._user,
                "cap_drop": ["ALL"],
                # FR-8: no-new-privileges + seccomp/apparmor profile（最小权限执行面）
                "security_opt": self._security_opt(),
                "detach": True,
            }

            if self._network_policy == "deny_all":
                container_config["network_mode"] = "none"
            elif self._network_policy == "whitelist" and self._network_allow:
                # FR-8: whitelist 模式 —— 容器接入预建网络（运维预建，仅可达白名单目标）
                container_config["network"] = self._network_allow

            container = None
            try:
                container = client.containers.run(**container_config)
                result = container.wait(timeout=timeout)
                stdout = container.logs(stdout=True, stderr=False).decode("utf-8", errors="replace")
                stderr = container.logs(stdout=False, stderr=True).decode("utf-8", errors="replace")
                exit_code = result.get("StatusCode", -1)
                duration_ms = (time.perf_counter() - start_time) * 1000

                return SandboxResult(
                    success=(exit_code == 0),
                    exit_code=exit_code,
                    stdout=stdout[:10000],
                    stderr=stderr[:10000],
                    duration_ms=round(duration_ms, 2),
                )

            except Exception as exc:
                duration_ms = (time.perf_counter() - start_time) * 1000
                return SandboxResult(
                    success=False,
                    exit_code=-1,
                    stderr=f"沙箱执行异常: {exc}",
                    duration_ms=round(duration_ms, 2),
                )
            finally:
                if container:
                    try:
                        container.remove(force=True)
                    except Exception:
                        pass

    def _fallback_execute(self, script: str, timeout: int) -> SandboxResult:
        """无 Docker 时的受限执行（非真正隔离，仅超时保护）。"""
        import subprocess

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
                sandbox_violations=["未隔离执行（Docker 不可用，降级模式）"],
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

    def preflight_check(self, script: str) -> List[str]:
        issues = []
        for pattern, description in ESCAPE_PATTERNS:
            if re.search(pattern, script, re.IGNORECASE):
                issues.append(description)
        return issues

    def is_available(self) -> bool:
        try:
            client = self._get_client()
            client.ping()
            return True
        except Exception:
            return False
