-- ============================================================
-- RBAC：用户表 + 知识分级 + Row-Level Security
-- ============================================================

-- 1. 用户与角色表
CREATE TABLE IF NOT EXISTS aiops_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(128) UNIQUE NOT NULL,
    display_name VARCHAR(256),
    api_key_hash CHAR(64) NOT NULL,
    roles TEXT[] NOT NULL DEFAULT '{viewer}',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_aiops_users_api_key_hash
    ON aiops_users (api_key_hash);

-- 2. 知识库增加分级和操作者字段
ALTER TABLE kb_fault_patterns
    ADD COLUMN IF NOT EXISTS access_level VARCHAR(16) NOT NULL DEFAULT 'public',
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128) NOT NULL DEFAULT 'system';

ALTER TABLE kb_incident_cases
    ADD COLUMN IF NOT EXISTS access_level VARCHAR(16) NOT NULL DEFAULT 'public',
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128) NOT NULL DEFAULT 'system';

-- 修复：FORCE RLS —— 表 owner 也受 RLS 约束（否则 owner 默认绕过，策略形同虚设；
-- 注意：超级用户/BYPASSRLS 角色始终绕过 RLS，应用应使用非超管账号连接）
ALTER TABLE kb_fault_patterns FORCE ROW LEVEL SECURITY;
ALTER TABLE kb_incident_cases FORCE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_kb_fault_patterns_access_level
    ON kb_fault_patterns (access_level);

CREATE INDEX IF NOT EXISTS idx_kb_incident_cases_access_level
    ON kb_incident_cases (access_level);

-- 3. Row-Level Security
ALTER TABLE kb_fault_patterns ENABLE ROW LEVEL SECURITY;
ALTER TABLE kb_incident_cases ENABLE ROW LEVEL SECURITY;

-- public 级别：所有已认证角色可读
CREATE POLICY kb_read_public ON kb_fault_patterns
    FOR SELECT
    USING (access_level = 'public');

-- internal 级别：operator / admin / auditor / service 可读
CREATE POLICY kb_read_internal ON kb_fault_patterns
    FOR SELECT
    USING (
        access_level IN ('public', 'internal')
        AND current_setting('aiops.role', true) IN ('operator', 'admin', 'auditor', 'service')
    );

-- secret 级别：仅 admin 可读
CREATE POLICY kb_read_secret ON kb_fault_patterns
    FOR SELECT
    USING (
        access_level = 'secret'
        AND current_setting('aiops.role', true) = 'admin'
    );

-- service 账号可写入（pipeline 知识沉淀），但不能写 secret 级别
CREATE POLICY kb_service_write ON kb_fault_patterns
    FOR INSERT
    WITH CHECK (
        current_setting('aiops.role', true) IN ('service', 'operator', 'admin')
        AND access_level != 'secret'
    );

-- incident_cases 同理
CREATE POLICY kb_cases_read_public ON kb_incident_cases
    FOR SELECT
    USING (access_level = 'public');

CREATE POLICY kb_cases_read_internal ON kb_incident_cases
    FOR SELECT
    USING (
        access_level IN ('public', 'internal')
        AND current_setting('aiops.role', true) IN ('operator', 'admin', 'auditor', 'service')
    );

CREATE POLICY kb_cases_read_secret ON kb_incident_cases
    FOR SELECT
    USING (
        access_level = 'secret'
        AND current_setting('aiops.role', true) = 'admin'
    );

CREATE POLICY kb_cases_service_write ON kb_incident_cases
    FOR INSERT
    WITH CHECK (
        current_setting('aiops.role', true) IN ('service', 'operator', 'admin')
        AND access_level != 'secret'
    );
