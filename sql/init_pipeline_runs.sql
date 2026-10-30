-- ============================================================
-- pipeline_runs：pipeline 执行登记表（FR-4 断点续跑）
-- run_id 同时作为 LangGraph checkpoint 的 thread_id
-- checkpoints/checkpoint_blobs/checkpoint_writes 由
-- langgraph-checkpoint-postgres 的 setup() 自动创建，不在此建
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    alert_id     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running',
                 -- running | suspended | completed | failed
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 断点恢复：按告警找未终结 run
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_alert_open
    ON pipeline_runs (alert_id, updated_at DESC)
    WHERE status IN ('running', 'suspended');

-- 僵死巡检：resume_stale_runs 脚本
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_stale
    ON pipeline_runs (updated_at)
    WHERE status = 'running';
