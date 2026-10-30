-- ============================================================
-- audit_chain_state：审计 hash chain 链头状态表（FR-2.3）
-- 单行表；emit 事务内 FOR UPDATE 行锁推进链 —— 多实例不分叉、重启接续
-- 注：首条事件 prev_hash=NULL（空链），链头行由 emit 首次 UPSERT 创建，
--     与本地链（emitter 内存链，首条 prev_hash=None）语义一致
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_chain_state (
    id            INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),   -- 单行
    last_hash     CHAR(64) NOT NULL,
    last_event_id UUID,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
