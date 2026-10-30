"""Redis 客户端单例工厂（FR-2.1 状态外置）。

get_redis(): 懒加载单例；AIOPS_REDIS_URL 未配置 → None（调用方回落本地实现）；
ping 失败 → None + 短退避，健康每 5s 复检，恢复后自动切回。

未启用 Redis 时所有外置状态走本地实现（进程内），系统降级运行不崩溃（规格 FR-2.1）。
"""

import os
import threading
import time
from typing import Optional

from core.logger import get_logger

logger = get_logger("redis_client")

_client = None
_available = False
_last_check = 0.0
_lock = threading.Lock()

_CHECK_INTERVAL = 5.0


def get_redis():
    """获取共享 Redis 客户端（单例）；未配置或不可用返回 None。"""
    global _client, _available, _last_check
    url = os.getenv("AIOPS_REDIS_URL", "")
    if not url:
        return None

    now = time.time()
    with _lock:
        if _client is not None and (now - _last_check) < _CHECK_INTERVAL:
            return _client if _available else None
        try:
            if _client is None:
                import redis as redis_lib
                _client = redis_lib.Redis.from_url(
                    url,
                    socket_timeout=1.0,
                    socket_connect_timeout=1.0,
                    decode_responses=True,
                )
            _client.ping()
            if not _available:
                logger.logger.info("Redis 已连接（状态外置启用）")
            _available = True
        except Exception as exc:
            if _available:
                logger.logger.warning("Redis 不可用，降级本地实现 | error={}", exc)
            _available = False
        _last_check = now
        return _client if _available else None


def is_configured() -> bool:
    """Redis 是否配置（不影响可用性探测）。"""
    return bool(os.getenv("AIOPS_REDIS_URL", ""))
