import os
import re
from dataclasses import dataclass, field
from typing import List, Pattern, Tuple

from core.models import Alert


@dataclass
class AlertRouteDecision:
    route: str
    priority: int
    reason: str
    tags: List[str] = field(default_factory=list)


class AlertRouter:
    """Routes alerts to different handling lanes with conservative defaults."""

    LEVEL_PRIORITY = {"Critical": 300, "Warning": 200, "Info": 100}
    KEYWORD_PRIORITY = {
        "disk": 25,
        "cpu": 20,
        "memory": 20,
        "network": 20,
        "config": 30,
        "latency": 15,
        "timeout": 15,
        "error": 10,
    }

    def __init__(self):
        self.manual_patterns = self._compile_env_patterns(
            "AIOPS_ROUTER_MANUAL_PATTERNS"
        )
        self.fast_track_patterns = self._compile_env_patterns(
            "AIOPS_ROUTER_FAST_TRACK_PATTERNS"
        )

    def decide(self, alert: Alert) -> AlertRouteDecision:
        haystack = f"{alert.title} {alert.content}"
        priority = self.LEVEL_PRIORITY.get(alert.level, 100)
        tags: List[str] = []

        for keyword, bonus in self.KEYWORD_PRIORITY.items():
            if keyword in haystack.lower():
                priority += bonus
                tags.append(keyword)

        matched_manual = self._find_match(self.manual_patterns, haystack)
        if matched_manual:
            return AlertRouteDecision(
                route="manual_first",
                priority=priority + 100,
                reason=f"命中人工优先路由规则: {matched_manual}",
                tags=tags + ["manual_first"],
            )

        matched_fast_track = self._find_match(self.fast_track_patterns, haystack)
        if matched_fast_track:
            return AlertRouteDecision(
                route="fast_track",
                priority=priority + 50,
                reason=f"命中快速通道路由规则: {matched_fast_track}",
                tags=tags + ["fast_track"],
            )

        return AlertRouteDecision(
            route="standard",
            priority=priority,
            reason="默认标准处理链路",
            tags=tags,
        )

    @staticmethod
    def _compile_env_patterns(env_name: str) -> List[Pattern[str]]:
        raw = os.getenv(env_name, "").strip()
        if not raw:
            return []
        patterns = []
        for item in raw.split(","):
            item = item.strip()
            if item:
                patterns.append(re.compile(item, re.IGNORECASE))
        return patterns

    @staticmethod
    def _find_match(patterns: List[Pattern[str]], text: str) -> str:
        for pattern in patterns:
            if pattern.search(text):
                return pattern.pattern
        return ""


def sort_alerts_by_priority(
    routed_alerts: List[Tuple[Alert, AlertRouteDecision]]
) -> List[Tuple[Alert, AlertRouteDecision]]:
    return sorted(
        routed_alerts,
        key=lambda item: (-item[1].priority, item[0].timestamp),
    )
