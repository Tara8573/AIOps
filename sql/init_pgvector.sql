CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS kb_experiences (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(64) NOT NULL DEFAULT 'manual',
    alert_feature TEXT NOT NULL,
    content TEXT NOT NULL,
    dedupe_key CHAR(32) NOT NULL,
    embedding vector(1024) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_kb_experiences_created_at
ON kb_experiences(created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_experiences_dedupe_key
ON kb_experiences(dedupe_key);
