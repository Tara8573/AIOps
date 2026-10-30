from dataclasses import dataclass, field
from typing import Dict, List

from core.models import Alert


@dataclass
class AlertIntentDecision:
    category: str
    confidence: float
    reason: str
    tags: List[str] = field(default_factory=list)
    secondary_categories: List[str] = field(default_factory=list)


class AlertIntentClassifier:
    """轻量告警意图识别器，用于给后续检索、技能和诊断做路由增强。"""

    CATEGORY_KEYWORDS: Dict[str, List[str]] = {
        "disk": [
            "disk", "inode", "iops", "/var", "/data", "磁盘", "空间", "挂载",
            "storage", "nas", "nfs", "iowait", "await", "磁盘满", "空间不足",
            "logrotate", "df", "du", "tmp",
        ],
        "cpu": [
            "cpu", "load", "负载", "飙高", "thread", "线程", "死循环",
            "process", "jstack", "top", "core dump",
        ],
        "memory": [
            "memory", "oom", "swap", "内存", "heap", "gc", "leak", "泄漏",
            "oomkilled", "rss", "resident", "内存不足", "内存泄漏",
        ],
        "network": [
            "network", "timeout", "latency", "packet", "port", "网络", "超时",
            "connection refused", "dns", "tcp", "丢包", "packet loss",
            "bandwidth", "带宽", "firewall", "防火墙", "connect timed out",
        ],
        "config": [
            "config", "drift", "version", "配置", "漂移", "不一致",
            "certificate", "证书", "tls", "ssl", "expire", "过期",
            "env", "环境变量",
        ],
        "service": [
            "service", "restart", "process", "pod", "container", "服务", "进程",
            "crashloopbackoff", "crash", "oomkilled", "restart", "部署",
            "deploy", "rollback", "回滚", "5xx", "503", "502", "504",
            "health check", "liveness", "readiness",
        ],
        "database": [
            "mysql", "postgres", "redis", "database", "db", "数据库",
            "replication", "复制", "lag", "延迟", "deadlock", "死锁",
            "connection pool", "连接池", "slow query", "慢查询",
            "binlog", "relay log",
        ],
        "security": [
            "permission", "denied", "auth", "token", "证书", "权限",
            "brute force", "暴力", "ssh", "login", "登录", "attack",
            "攻击", "intrusion", "入侵", "suspicious", "可疑",
        ],
    }

    WEIGHTED_KEYWORDS: Dict[str, Dict[str, float]] = {
        "disk": {"disk": 3.0, "磁盘": 3.0, "inode": 2.5, "iops": 2.0, "空间": 2.0},
        "cpu": {"cpu": 3.0, "load": 2.0, "负载": 2.0, "thread": 2.0, "线程": 2.0},
        "memory": {"memory": 3.0, "内存": 3.0, "oom": 3.0, "swap": 2.0, "heap": 2.0},
        "network": {"network": 3.0, "网络": 3.0, "timeout": 2.0, "超时": 2.0, "packet": 2.5},
        "database": {"mysql": 3.0, "postgres": 3.0, "redis": 3.0, "replication": 2.5},
        "service": {"pod": 2.5, "container": 2.5, "crashloop": 3.0, "service": 2.0},
    }

    def classify(self, alert: Alert) -> AlertIntentDecision:
        haystack = f"{alert.title} {alert.content}".lower()
        matched_tags: List[str] = []
        best_category = "general"
        best_score = 0.0

        category_scores: Dict[str, float] = {}
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            weights = self.WEIGHTED_KEYWORDS.get(category, {})
            score = sum(weights.get(kw, 1.0) for kw in keywords if kw.lower() in haystack)
            if score > 0:
                category_scores[category] = score

        if not category_scores:
            return AlertIntentDecision(
                category="general",
                confidence=0.35,
                reason="未命中明确分类关键词，按通用告警处理",
                tags=[],
            )

        sorted_cats = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
        best_category, best_score = sorted_cats[0]
        matched_tags = [
            kw for kw in self.CATEGORY_KEYWORDS[best_category]
            if kw.lower() in haystack
        ]

        secondary = [cat for cat, _ in sorted_cats[1:3] if _ >= best_score * 0.5]

        weights = self.WEIGHTED_KEYWORDS.get(best_category, {})
        total_possible = sum(
            weights.get(kw, 1.0) for kw in self.CATEGORY_KEYWORDS[best_category]
        )
        confidence = min(0.5 + (best_score / max(total_possible, 1)) * 0.5, 0.95)
        confidence = max(confidence, 0.55)

        return AlertIntentDecision(
            category=best_category,
            confidence=round(confidence, 2),
            reason=f"命中 {best_category} 类关键词: {', '.join(matched_tags[:4])}",
            tags=matched_tags,
            secondary_categories=secondary,
        )
