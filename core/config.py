"""统一配置管理：YAML 主配置 + 环境变量覆盖。

加载优先级：环境变量 > YAML 文件 > 内置默认值

环境变量覆盖约定：
  AIOPS__<SECTION>__<KEY> = value
  双下划线分隔层级，自动类型转换。
  例: AIOPS__LLM__API_KEY=sk-xxx → settings.get("llm.api_key")
      AIOPS__AGENTS__DIAGNOSIS__LLM__PROVIDER=openai → settings.get("agents.diagnosis.llm.provider")
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


# ---------------------------------------------------------------------------
# 类型转换
# ---------------------------------------------------------------------------

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off", ""}


def _coerce(value: str, target: Any) -> Any:
    """将字符串环境变量转换为目标类型。"""
    if target is None:
        return value
    if isinstance(target, bool):
        low = value.strip().lower()
        if low in _TRUTHY:
            return True
        if low in _FALSY:
            return False
        return bool(value)
    if isinstance(target, int):
        return int(value)
    if isinstance(target, float):
        return float(value)
    if isinstance(target, list):
        return [v.strip() for v in value.split(",") if v.strip()]
    return value


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class Settings:
    """层级配置访问器，支持点分隔路径和环境变量覆盖。"""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def get(self, path: str, default: Any = None) -> Any:
        """按点分隔路径获取配置值。

        优先检查环境变量 AIOPS__<PATH 用双下划线替换点并大写>。
        """
        env_key = "AIOPS__" + path.replace(".", "__").upper()
        env_val = os.getenv(env_key)

        yaml_val = self._resolve(path)

        if env_val is not None:
            target = yaml_val if yaml_val is not None else default
            return _coerce(env_val, target)

        return yaml_val if yaml_val is not None else default

    def get_section(self, path: str) -> Dict[str, Any]:
        """获取整个子段（不含环境变量覆盖）。"""
        return self._resolve(path) or {}

    def _resolve(self, path: str) -> Any:
        keys = path.split(".")
        node = self._data
        for key in keys:
            if not isinstance(node, dict):
                return None
            node = node.get(key)
            if node is None:
                return None
        return node

    def __repr__(self):
        return f"Settings({list(self._data.keys())})"


# ---------------------------------------------------------------------------
# 加载器
# ---------------------------------------------------------------------------

_default_config_path = Path(__file__).resolve().parent.parent / "config" / "default.yaml"
_settings_instance: Optional[Settings] = None


def load_settings(config_path: Optional[Union[str, Path]] = None) -> Settings:
    """加载配置，返回 Settings 单例。

    优先使用指定路径，否则使用 config/default.yaml。
    重复调用返回缓存的单例（除非显式传 config_path）。
    """
    global _settings_instance

    if config_path is None and _settings_instance is not None:
        return _settings_instance

    path = Path(config_path) if config_path else _default_config_path

    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    settings = Settings(data)

    if config_path is None:
        _settings_instance = settings

    return settings


def get_settings() -> Settings:
    """获取全局 Settings 单例（未加载则自动加载）。"""
    return load_settings()


def pg_dsn() -> str:
    """统一 PG DSN 读取口（审计/审批/checkpoint/run registry/认证共用）。

    优先级：AIOPS__PG__DSN（新约定，经 Settings）> AIOPS_PG_DSN（旧 env，兼容）
    > kb.pg.dsn（仅 KB 配置了 DSN 时的兼容回落）。
    """
    s = get_settings()
    return (
        s.get("pg.dsn", "")
        or os.getenv("AIOPS_PG_DSN", "")
        or s.get("kb.pg.dsn", "")
        or ""
    )


def reset_settings():
    """重置全局单例（测试用）。"""
    global _settings_instance
    _settings_instance = None
