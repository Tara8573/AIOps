"""core/rule_engine.py 的单元测试。"""

from core.models import Alert
from core.rule_engine import LocalRuleEngine, Rule


def _make_alert(title: str, content: str = "") -> Alert:
    return Alert(
        alert_id="test-001",
        title=title,
        level="Critical",
        content=content,
    )


class TestLocalRuleEngine:
    def test_match_disk_rule(self):
        """磁盘告警匹配 RULE-001。"""
        engine = LocalRuleEngine()
        alert = _make_alert("Disk usage critical", "/var disk usage > 98%")
        result = engine.match(alert)
        assert result is not None
        assert result["rule_id"] == "RULE-001"

    def test_match_oom_rule(self):
        """OOM 告警匹配 RULE-002。"""
        engine = LocalRuleEngine()
        alert = _make_alert("OOM Killer triggered", "Out of memory: killed process nginx")
        result = engine.match(alert)
        assert result is not None
        assert result["rule_id"] == "RULE-002"

    def test_no_match_unknown_alert(self):
        """未知告警不匹配任何规则。"""
        engine = LocalRuleEngine()
        alert = _make_alert("Database connection timeout", "MySQL connection pool exhausted")
        result = engine.match(alert)
        assert result is None

    def test_custom_rules(self):
        """自定义规则可正常匹配。"""
        custom = [
            Rule(
                rule_id="CUSTOM-001",
                name="自定义规则",
                patterns=[r"custom.*alert"],
                action_script="echo custom",
                action_description="自定义修复",
            )
        ]
        engine = LocalRuleEngine(rules=custom)
        alert = _make_alert("Custom alert detected", "something")
        result = engine.match(alert)
        assert result is not None
        assert result["rule_id"] == "CUSTOM-001"

    def test_all_patterns_must_match(self):
        """规则的所有 pattern 必须同时匹配才算命中。"""
        engine = LocalRuleEngine()
        # 有 disk 但没有 /var 或 /log
        alert = _make_alert("Disk usage high", "/data partition full")
        result = engine.match(alert)
        # RULE-001 需要同时匹配 disk 和 /var|/log，这里不满足
        assert result is None or result["rule_id"] != "RULE-001"
