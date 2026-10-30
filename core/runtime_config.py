"""运行时热配置（FR-7 配置热更新）。

RuntimeConfigProvider: 运行时只读参数的统一读取口。
- 热参数存 PG 表 runtime_config + 后台线程定时刷新（默认 5s）+ 版本号缓存
- 读取无 IO（内存快照），热点路径可用
- 表缺失/查询失败 → 回落默认值（不崩溃）
- 启动期参数（连接串/端口等）仍走 Settings（env/yaml，语义不变）

热参数清单（迁移到 runtime_config 表）:
  kafka.min_level / kafka.include_patterns / kafka.exclude_patterns /
  degradation.forced_level / aggregation.window_seconds / aggregation.max_group /
  evaluation.min_confidence / routing.iterative_max_retries / llm.global_concurrent
"""

import os
import threading
import time
from typing import Any, Dict, Optional

from core.config import pg_dsn
from core.logger import get_logger
from core.observability import metrics

logger = get_logger("runtime_config")


class RuntimeConfigProvider:
    """运行时热参数提供者。线程安全。"""

    def __init__(
        self,
        dsn: Optional[str] = None,
        refresh_seconds: Optional[float] = None,
        table: str = "runtime_config",
    ):
        self._dsn = dsn or pg_dsn()
        self._refresh_seconds = refresh_seconds if refresh_seconds is not None else float(
            os.getenv("AIOPS_CONFIG_REFRESH_SECONDS", "5"))
        self._table = table
        self._snapshot: Dict[str, str] = {}
        self._version: int = 0
        self._pool = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._first_refresh()

    # ── 读取（内存快照，无 IO）──

    def version(self) -> int:
        with self._lock:
            return self._version

    def get(self, key: str, default: Any = None) -> Any:
        """读热参数（字符串值；default 在键缺失时返回）。"""
        with self._lock:
            return self._snapshot.get(key, default)

    # ── 刷新 ──

    def _first_refresh(self):
        try:
            self.refresh()
        except Exception as exc:
            metrics.incr("runtime_config.refresh_failed")
            logger.logger.warning("runtime_config 首次刷新失败（表缺失时回落默认值）| error={}", exc)

    def refresh(self) -> int:
        """拉取全表（表小），version 变化时原子替换快照。返回当前 version。"""
        pool = self._get_pool()
        with pool.connection() as conn:
            rows = conn.execute(
                f"SELECT config_key, config_value, version FROM {self._table}"
            ).fetchall()
        new_snapshot = {r[0]: r[1] for r in rows}
        max_version = max((r[2] for r in rows), default=0)

        with self._lock:
            changed = new_snapshot != self._snapshot
            self._snapshot = new_snapshot
            if changed:
                self._version = max_version or (self._version + 1)
                logger.logger.info(
                    "[热配置] runtime_config 已刷新 | keys={} | version={}",
                    len(new_snapshot), self._version,
                )
            return self._version

    def _get_pool(self):
        if self._pool is None:
            import psycopg_pool
            self._pool = psycopg_pool.ConnectionPool(
                self._dsn, min_size=1, max_size=2, open=True,
            )
        return self._pool

    # ── 后台刷新线程 ──

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="runtime-config", daemon=True)
        self._thread.start()
        logger.logger.info("RuntimeConfigProvider 刷新线程启动 | interval={}s", self._refresh_seconds)

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def _run(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(self._refresh_seconds)
            try:
                self.refresh()
            except Exception as exc:
                metrics.incr("runtime_config.refresh_failed")
                logger.logger.warning("runtime_config 刷新失败 | error={}", exc)
