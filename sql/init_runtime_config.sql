-- ============================================================
-- runtime_config：运行时热配置表（FR-7）
-- 热参数清单：kafka.min_level / kafka.include_patterns / kafka.exclude_patterns /
--   degradation.forced_level / aggregation.window_seconds / aggregation.max_group /
--   evaluation.min_confidence / routing.iterative_max_retries / llm.global_concurrent
-- 修改即生效（5s 内），无需重启进程
-- ============================================================

CREATE TABLE IF NOT EXISTS runtime_config (
    config_key   TEXT PRIMARY KEY,          -- 点分隔键，如 kafka.min_level
    config_value TEXT NOT NULL,             -- 字符串值（读取方自行转换类型）
    version      INT NOT NULL DEFAULT 1,    -- 每次修改 +1（下游按 version 重编译/生效）
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by   TEXT
);

-- 变更审计索引
CREATE INDEX IF NOT EXISTS idx_runtime_config_updated
    ON runtime_config (updated_at DESC);
