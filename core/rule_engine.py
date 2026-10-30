"""本地规则引擎实现：基于正则匹配的快路径执行。

已知问题（如磁盘满清理日志、OOM 重启服务）走规则引擎，
毫秒级确定性修复；未匹配的告警交由 AIOps 智能诊断。
"""

import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from core.models import Alert
from core.observability import metrics
from interfaces.base import IRuleEngine


@dataclass
class Rule:
    rule_id: str
    name: str
    patterns: List[str]  # 正则表达式列表，匹配 alert.title + alert.content
    action_script: str   # 修复脚本
    action_description: str
    risk_level: str = "Low"


class LocalRuleEngine(IRuleEngine):
    """基于正则匹配的本地规则引擎。

    规则可通过环境变量 AIOPS_RULES_FILE 加载 JSON 文件，
    也可在构造时直接传入 rules 列表。
    """

    def __init__(self, rules: Optional[List[Rule]] = None):
        self.rules = rules or self._load_default_rules()
        self._compiled = [
            (rule, [re.compile(p, re.IGNORECASE) for p in rule.patterns])
            for rule in self.rules
        ]

    def match(self, alert: Alert) -> Optional[Dict[str, any]]:
        haystack = f"{alert.title} {alert.content}"
        for rule, compiled_patterns in self._compiled:
            if all(p.search(haystack) for p in compiled_patterns):
                metrics.incr("pipeline.rule_engine.matched")
                return {
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "action_description": rule.action_description,
                    "action_script": rule.action_script,
                    "risk_level": rule.risk_level,
                }
        metrics.incr("pipeline.rule_engine.no_match")
        return None

    def execute(self, alert: Alert, rule: Dict[str, any]) -> bool:
        """执行规则脚本（通过 IExecutor 注入）。

        注意：实际执行由 pipeline 调用 self.executor.execute_script()，
        这里只记录日志和指标。
        """
        metrics.incr("pipeline.rule_engine.executed")
        print(f"[RuleEngine] 执行规则 {rule['rule_id']}: {rule['action_description']}")
        return True

    @staticmethod
    def _load_default_rules() -> List[Rule]:
        """加载内置的默认规则集。"""
        return [
            Rule(
                rule_id="RULE-001",
                name="磁盘日志清理",
                patterns=[r"disk.*(?:usage|full|space)", r"/var|/log"],
                action_script="find /var/log -name '*.log' -mtime +30 -delete && journalctl --vacuum-time=7d",
                action_description="清理 30 天前的日志文件并压缩 journal",
                risk_level="Low",
            ),
            Rule(
                rule_id="RULE-002",
                name="OOM 进程重启",
                patterns=[r"oom|out of memory|killed process"],
                action_script=r"systemctl restart $(journalctl -k | grep 'Killed process' | tail -1 | grep -oP '(?<=\()[\w-]+')",
                action_description="重启被 OOM Killer 终止的服务",
                risk_level="Medium",
            ),
            Rule(
                rule_id="RULE-003",
                name="临时文件清理",
                patterns=[r"disk.*(?:usage|full)", r"/tmp|tmp"],
                action_script="find /tmp -type f -mtime +7 -delete",
                action_description="清理 7 天前的临时文件",
                risk_level="Low",
            ),
        ]
