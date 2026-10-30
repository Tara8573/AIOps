"""LangGraph checkpoint 持久化构建（FR-4）。

AIOPS_CHECKPOINT_ENABLED=1 时启用 PostgresSaver（断点续跑 / 人工审批异步挂起的前置）；
否则返回 None，保持现状行为（不持久化）。

- PostgresSaver 基于 psycopg3 连接池，复用 AIOPS_PG_DSN（与知识库/审计同库）
- setup() 自动创建 checkpoints / checkpoint_blobs / checkpoint_writes / checkpoint_migrations 表
- 线程池必须 autocommit=True 且 prepare_threshold=0（PostgresSaver 官方要求）
"""

import os
from typing import Any, Optional

from core.config import pg_dsn
from core.logger import get_logger

logger = get_logger("checkpointing")


def is_checkpoint_enabled() -> bool:
    """checkpoint 是否启用（env 开关，默认关闭保持现状）。"""
    return os.getenv("AIOPS_CHECKPOINT_ENABLED", "0") == "1"


def build_checkpointer(dsn: Optional[str] = None) -> Optional[Any]:
    """构建 LangGraph PostgresSaver。

    Returns:
        PostgresSaver（长连接池），未启用时返回 None。

    Raises:
        RuntimeError: 启用了 checkpoint 但未配置 PG DSN。
    """
    if not is_checkpoint_enabled():
        return None

    dsn = dsn or pg_dsn()
    if not dsn:
        raise RuntimeError(
            "AIOPS_CHECKPOINT_ENABLED=1 但未配置 PG DSN（AIOPS__PG__DSN / AIOPS_PG_DSN），"
            "无法持久化 pipeline 状态"
        )

    from psycopg_pool import ConnectionPool
    from langgraph.checkpoint.postgres import PostgresSaver

    pool = ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=5,
        open=True,
        kwargs={"autocommit": True, "prepare_threshold": 0},
    )
    saver = PostgresSaver(pool)
    saver.setup()  # 自动建 checkpoints 等表（幂等）

    logger.logger.info("LangGraph PostgresSaver 已启用 | dsn={}", _mask_dsn(dsn))
    return saver


def _mask_dsn(dsn: str) -> str:
    """日志中隐藏连接串密码（FR-8 凭据脱敏约定）。"""
    if "@" not in dsn or ":" not in dsn.split("@")[0]:
        return dsn
    head, tail = dsn.split("@", 1)
    if ":" in head:
        head = head.rsplit(":", 1)[0] + ":***"
    return f"{head}@{tail}"
