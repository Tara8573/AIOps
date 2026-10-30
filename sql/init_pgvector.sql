CREATE EXTENSION IF NOT EXISTS vector;

DROP TABLE IF EXISTS kb_experiences;

CREATE TABLE IF NOT EXISTS kb_fault_patterns (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL DEFAULT 'manual',
    canonical_root_cause TEXT NOT NULL,
    summary_content TEXT NOT NULL,
    dedupe_key CHAR(32) NOT NULL,
    embedding vector(1024) NOT NULL,
    embedding_provider VARCHAR(32) NOT NULL DEFAULT 'local',
    support_count INTEGER NOT NULL DEFAULT 0,
    access_level VARCHAR(16) NOT NULL DEFAULT 'public',
    created_by VARCHAR(128) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kb_incident_cases (
    id BIGSERIAL PRIMARY KEY,
    fault_pattern_id BIGINT NOT NULL
        REFERENCES kb_fault_patterns(id) ON DELETE CASCADE,
    source VARCHAR(64) NOT NULL DEFAULT 'manual',
    alert_id TEXT,
    ticket_id TEXT,
    actual_root_cause TEXT NOT NULL,
    resolution_steps TEXT NOT NULL,
    case_key CHAR(32) NOT NULL,
    access_level VARCHAR(16) NOT NULL DEFAULT 'public',
    created_by VARCHAR(128) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_fault_patterns_dedupe_key
ON kb_fault_patterns(dedupe_key);

CREATE INDEX IF NOT EXISTS idx_kb_fault_patterns_updated_at
ON kb_fault_patterns(updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_incident_cases_case_key
ON kb_incident_cases(case_key);

CREATE INDEX IF NOT EXISTS idx_kb_incident_cases_pattern_id
ON kb_incident_cases(fault_pattern_id);

-- 迁移：已有库补 embedding_provider 列
ALTER TABLE kb_fault_patterns
    ADD COLUMN IF NOT EXISTS embedding_provider VARCHAR(32) NOT NULL DEFAULT 'local';

CREATE INDEX IF NOT EXISTS idx_kb_fault_patterns_provider
ON kb_fault_patterns(embedding_provider);
