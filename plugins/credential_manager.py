"""凭据集中管理（FR-8 安全合规）。

ICredentialProvider: 凭据统一读取口 —— executor 用时取、不留存（规格 FR-8）。
- VaultCredentialProvider:  生产实现（Vault K/V v2，hvac；URL/TOKEN 走 env 或 K8s SA 注入）
- DevEnvCredentialProvider: 开发实现（读 env）。⚠ 仅开发，生产禁用；STRICT=1 时启动告警

配置文件中的凭据字段改为 secretRef 形式（如 vault://aiops/llm#api_key），
加载时解析为实际值仅驻内存 —— 凭据不出现在配置文件/日志/环境变量明文。
"""

import json
import os
from abc import ABC, abstractmethod
from typing import Optional

from core.logger import get_logger

logger = get_logger("credential_manager")


class ICredentialProvider(ABC):
    """凭据提供者接口。"""

    @abstractmethod
    def get_secret(self, path: str, key: Optional[str] = None) -> str:
        """读取凭据；path 如 "aiops/llm"，key 如 "api_key"。

        用时取、不留存（不缓存明文）。
        """
        pass


class VaultCredentialProvider(ICredentialProvider):
    """HashiCorp Vault K/V v2 实现。"""

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        mount_point: str = "secret",
    ):
        self._url = url or os.getenv("AIOPS_VAULT_URL", "")
        self._token = token or os.getenv("AIOPS_VAULT_TOKEN", "")
        self._mount = mount_point or os.getenv("AIOPS_VAULT_MOUNT", "secret")
        self._client = None

    def _get_client(self):
        if self._client is None:
            import hvac
            if not self._url:
                raise RuntimeError("AIOPS_VAULT_URL 未配置")
            self._client = hvac.Client(url=self._url, token=self._token)
        return self._client

    def get_secret(self, path: str, key: Optional[str] = None) -> str:
        client = self._get_client()
        secret = client.secrets.kv.v2.read_secret_version(
            path=path.strip("/"), mount_point=self._mount,
        )
        data = (secret or {}).get("data", {}).get("data", {})
        if key is not None:
            return str(data.get(key, ""))
        return json.dumps(data, ensure_ascii=False)


class DevEnvCredentialProvider(ICredentialProvider):
    """开发环境实现（读 env）。⚠ 仅开发，生产禁用（STRICT=1 时启动告警）。

    env 键约定: AIOPS_SECRET_<PATH 大写下划线>[_<KEY>]
    例: path="aiops/llm" key="api_key" → AIOPS_SECRET_AIOPS_LLM_API_KEY
    """

    def get_secret(self, path: str, key: Optional[str] = None) -> str:
        base = "AIOPS_SECRET_" + path.strip("/").upper().replace("/", "_").replace("-", "_")
        env_key = f"{base}_{key.upper()}" if key else base
        return os.getenv(env_key, "")


def credential_provider_factory(strict: bool = False) -> ICredentialProvider:
    """凭据提供者工厂。

    - AIOPS_VAULT_URL 配置 → Vault（生产）
    - 否则 DevEnv（读 env）；strict=True 时告警（STRICT=1 下开发实现不宜用于生产）
    """
    if os.getenv("AIOPS_VAULT_URL"):
        return VaultCredentialProvider()
    if strict:
        logger.logger.warning(
            "[凭据] STRICT=1 但未配置 Vault，回退开发实现（读 env）——生产环境应配置 AIOPS_VAULT_URL",
        )
    return DevEnvCredentialProvider()
