-- ============================================================
-- approval_requests：异步审批表（FR-3 人工确认异步化）
-- 流转：pending → approved | rejected | expired → resumed
-- ============================================================

CREATE TABLE IF NOT EXISTS approval_requests (
    approval_id      UUID PRIMARY KEY,
    pipeline_run_id  TEXT NOT NULL,          -- = LangGraph checkpoint thread_id
    alert_id         TEXT NOT NULL,
    proposal         JSONB NOT NULL,         -- ActionPlan.model_dump()
    eval_summary     JSONB NOT NULL,         -- {is_passed, reason, risk_level}
    status           TEXT NOT NULL DEFAULT 'pending',
                     -- pending | approved | rejected | expired | resumed
    decider          TEXT,
    decide_reason    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    timeout_at       TIMESTAMPTZ NOT NULL,   -- created_at + AIOPS_APPROVAL_TIMEOUT
    decided_at       TIMESTAMPTZ,
    resumed_at       TIMESTAMPTZ,
    resumed_by       TEXT                    -- 恢复实例 id
);

-- 幂等：同 run 只允许一条 pending（submit ON CONFLICT DO NOTHING 依赖此索引）
CREATE UNIQUE INDEX IF NOT EXISTS uq_approval_pending_per_run
    ON approval_requests (pipeline_run_id)
    WHERE status = 'pending';

-- 待审批列表（回调 API / Web/IM 轮询）
CREATE INDEX IF NOT EXISTS idx_approval_pending
    ON approval_requests (status, timeout_at)
    WHERE status = 'pending';

-- 已决策未恢复（Resumer 领取）
CREATE INDEX IF NOT EXISTS idx_approval_decided
    ON approval_requests (status, decided_at)
    WHERE status IN ('approved', 'rejected', 'expired') AND resumed_at IS NULL;
