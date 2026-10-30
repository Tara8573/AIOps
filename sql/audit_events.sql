-- ============================================================
-- 审计事件表
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_events (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type VARCHAR(64) NOT NULL,
    actor VARCHAR(128) NOT NULL DEFAULT 'system',
    alert_id TEXT,
    pipeline_run_id TEXT,
    category VARCHAR(32) NOT NULL DEFAULT 'operational',
    severity VARCHAR(16) NOT NULL DEFAULT 'info',
    payload JSONB NOT NULL DEFAULT '{}',
    outcome VARCHAR(16) NOT NULL DEFAULT 'success',
    event_hash CHAR(64) NOT NULL,
    prev_hash CHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp
    ON audit_events (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_event_type
    ON audit_events (event_type);

CREATE INDEX IF NOT EXISTS idx_audit_events_alert_id
    ON audit_events (alert_id)
    WHERE alert_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_events_pipeline_run_id
    ON audit_events (pipeline_run_id)
    WHERE pipeline_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_events_severity
    ON audit_events (severity)
    WHERE severity IN ('warning', 'critical');
