from dataclasses import dataclass, field
from typing import Dict, List

from core.models import Alert


@dataclass
class AlertIntentDecision:
    category: str
    confidence: float
    reason: str
    tags: List[str] = field(default_factory=list)


class AlertIntentClassifier:
    """轻量告警意图识别器，用于给后续检索、技能和诊断做路由增强。"""

    CATEGORY_KEYWORDS: Dict[str, List[str]] = {
        "disk": ["disk", "inode", "iops", "/var", "/data", "磁盘", "空间", "挂载"],
        "cpu": ["cpu", "load", "负载", "飙高"],
        "memory": ["memory", "oom", "swap", "内存"],
        "network": ["network", "timeout", "latency", "packet", "port", "网络", "超时"],
        "config": ["config", "drift", "version", "配置", "漂移", "不一致"],
        "service": ["service", "restart", "process", "pod", "container", "服务", "进程"],
        "database": ["mysql", "postgres", "redis", "database", "db", "数据库"],
        "security": ["permission", "denied", "auth", "token", "证书", "权限"],
    }

    def classify(self, alert: Alert) -> AlertIntentDecision:
        haystack = f"{alert.title} {alert.content}".lower()
        matched_tags: List[str] = []
        best_category = "general"
        best_score = 0

        for category, keywords in self.CATEGORY_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword.lower() in haystack)
            if score <= 0:
                continue
            if score > best_score:
                best_category = category
                best_score = score
                matched_tags = [keyword for keyword in keywords if keyword.lower() in haystack]

        if best_score == 0:
            return AlertIntentDecision(
                category="general",
                confidence=0.35,
                reason="未命中明确分类关键词，按通用告警处理",
                tags=[],
            )

        confidence = min(0.55 + best_score * 0.15, 0.95)
        return AlertIntentDecision(
            category=best_category,
            confidence=round(confidence, 2),
            reason=f"命中 {best_category} 类关键词: {', '.join(matched_tags[:4])}",
            tags=matched_tags,
        )
