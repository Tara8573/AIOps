"""core/config.py 单元测试。"""

import os
from pathlib import Path

import pytest

from core.config import Settings, load_settings, reset_settings


class TestSettingsGet:

    def test_get_top_level(self):
        s = Settings({"runtime": {"mode": "demo", "log_level": "INFO"}})
        assert s.get("runtime.mode") == "demo"
        assert s.get("runtime.log_level") == "INFO"

    def test_get_nested(self):
        s = Settings({"llm": {"cache": {"ttl_seconds": 300}}})
        assert s.get("llm.cache.ttl_seconds") == 300

    def test_get_missing_returns_default(self):
        s = Settings({"runtime": {}})
        assert s.get("runtime.nonexistent") is None
        assert s.get("runtime.nonexistent", "fallback") == "fallback"

    def test_env_var_override(self, monkeypatch):
        s = Settings({"llm": {"api_key": "", "temperature": 0.2}})
        monkeypatch.setenv("AIOPS__LLM__API_KEY", "sk-test-123")
        monkeypatch.setenv("AIOPS__LLM__TEMPERATURE", "0.5")
        assert s.get("llm.api_key") == "sk-test-123"
        assert s.get("llm.temperature") == 0.5

    def test_env_var_bool_coercion(self, monkeypatch):
        s = Settings({"routing": {"llm_judge_enabled": False}})
        monkeypatch.setenv("AIOPS__ROUTING__LLM_JUDGE_ENABLED", "1")
        assert s.get("routing.llm_judge_enabled") is True

    def test_env_var_int_coercion(self, monkeypatch):
        s = Settings({"llm": {"timeout": 180}})
        monkeypatch.setenv("AIOPS__LLM__TIMEOUT", "300")
        assert s.get("llm.timeout") == 300

    def test_env_var_list_coercion(self, monkeypatch):
        s = Settings({"jira": {"project_keys": ["OPS"]}})
        monkeypatch.setenv("AIOPS__JIRA__PROJECT_KEYS", "OPS,SRE,INFRA")
        assert s.get("jira.project_keys") == ["OPS", "SRE", "INFRA"]

    def test_env_var_nested_path(self, monkeypatch):
        s = Settings({"agents": {"diagnosis": {"llm": {"provider": "default"}}}})
        monkeypatch.setenv("AIOPS__AGENTS__DIAGNOSIS__LLM__PROVIDER", "openai")
        assert s.get("agents.diagnosis.llm.provider") == "openai"


class TestSettingsSection:

    def test_get_section(self):
        s = Settings({"kafka": {"topic": "alerts", "group_id": "g1"}})
        section = s.get_section("kafka")
        assert section["topic"] == "alerts"
        assert section["group_id"] == "g1"

    def test_get_section_missing(self):
        s = Settings({})
        assert s.get_section("nonexistent") == {}


class TestLoadSettings:

    def test_load_default_yaml(self):
        reset_settings()
        s = load_settings()
        assert s.get("runtime.mode") == "demo"
        assert s.get("llm.provider") == "openai"
        assert s.get("agents.diagnosis.llm.provider") == "default"

    def test_singleton(self):
        reset_settings()
        s1 = load_settings()
        s2 = load_settings()
        assert s1 is s2

    def test_load_missing_file(self):
        s = load_settings(config_path="/nonexistent/path.yaml")
        assert s.get("runtime.mode", "demo") == "demo"

    def test_reset(self):
        reset_settings()
        s1 = load_settings()
        reset_settings()
        s2 = load_settings()
        assert s1 is not s2
