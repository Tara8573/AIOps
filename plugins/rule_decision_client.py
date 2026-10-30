"""RuleDecisionClient — 确定性规则实现的 IDecisionClient。

将现有的 AlertIntentClassifier (关键词意图分类)、ScriptSafetyScanner (正则安全扫描)
等确定性逻辑封装到统一的 IDecisionClient 接口后面。

特点：
- 零网络调用、零模型成本、零延迟
- 结果完全确定（相同输入 → 相同输出）
- 作为 Jev/LLM 的本地 fallback；当远程决策服务不可用时降级到此实现
"""

import re
from typing import Dict, List, Any

from core.models import (
    DecisionResult,
    DimensionVerdict,
    JudgeResult,
    NoulResult,
    ScoreResult,
)
from interfaces.base import IDecisionClient


class RuleDecisionClient(IDecisionClient):
    """基于确定性规则的 IDecisionClient 实现。

    可以用作基线、fallback、或快速本地决策。
    覆盖：意图分类、安全判定、风险评分。
    """

    # ── 意图分类规则（复用 AlertIntentClassifier 的关键词表） ──

    CATEGORY_KEYWORDS: Dict[str, List[str]] = {
        "disk": [
            "disk", "inode", "iops", "/var", "/data", "磁盘", "空间", "挂载",
            "storage", "nas", "nfs", "iowait", "await", "磁盘满", "空间不足",
            "logrotate", "df", "du",
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
            "crashloopbackoff", "crash", "deploy", "rollback", "回滚",
            "5xx", "503", "502", "504", "health check", "liveness", "readiness",
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
        "disk": {"disk": 3.0, "磁盘": 3.0, "inode": 2.5},
        "cpu": {"cpu": 3.0, "load": 2.0, "负载": 2.0, "thread": 2.0},
        "memory": {"memory": 3.0, "内存": 3.0, "oom": 3.0, "swap": 2.0},
        "network": {"network": 3.0, "网络": 3.0, "timeout": 2.0, "packet": 2.5},
        "database": {"mysql": 3.0, "postgres": 3.0, "redis": 3.0, "replication": 2.5},
        "service": {"pod": 2.5, "container": 2.5, "crashloop": 3.0},
    }

    # ── 安全危险命令正则 ──

    DANGEROUS_PATTERNS: List[str] = [
        r"rm\s+-rf\s+/",
        r"mkfs",
        r"dd\s+if=",
        r">\s*/dev/sd[a-z]",
        r"reboot\b",
        r"shutdown\b",
        r"curl\s+.*\|\s*(sh|bash)",
        r"wget\s+.*\|\s*(sh|bash)",
        r"chmod\s+777",
        r":\(\)\s*\{\s*:\|\:&\s*\};:",
    ]

    # ── IDecisionClient 接口实现 ──

    def classify(self, prompt: str, options: List[str]) -> DecisionResult:
        """基于关键词匹配的多选一分类。"""
        haystack = prompt.lower()
        category_scores: Dict[str, float] = {}

        for category, keywords in self.CATEGORY_KEYWORDS.items():
            # 只对 options 中的分类进行计算
            if category not in options:
                continue
            weights = self.WEIGHTED_KEYWORDS.get(category, {})
            score = sum(weights.get(kw, 1.0) for kw in keywords if kw.lower() in haystack)
            if score > 0:
                category_scores[category] = score

        if not category_scores:
            return DecisionResult(
                choice="general",
                confidence=0.35,
                reason="未命中任何明确分类关键词，按通用告警处理",
                alternatives=[],
            )

        sorted_cats = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
        best, best_score = sorted_cats[0]

        # 归一化置信度
        total = sum(s for _, s in sorted_cats)
        confidence = round(min(best_score / max(total, 1), 0.95), 2)
        confidence = max(confidence, 0.55)

        alternatives = [
            {"choice": cat, "score": round(s / max(total, 1), 2)}
            for cat, s in sorted_cats[1:4]
        ]

        matched = [
            kw for kw in self.CATEGORY_KEYWORDS.get(best, [])
            if kw.lower() in haystack
        ]

        return DecisionResult(
            choice=best,
            confidence=confidence,
            reason=f"命中 {best} 类关键词: {', '.join(matched[:4])}",
            alternatives=alternatives,
        )

    def judge(
        self, prompt: str, dimensions: List[Dict[str, Any]]
    ) -> JudgeResult:
        """基于规则的轻量多维度评估。每个维度通过 question 中的关键词做规则判定。"""
        dim_results: List[DimensionVerdict] = []

        for dim in dimensions:
            name = dim.get("name", "unknown")
            question = dim.get("question", "")

            # 简单规则：检测 prompt 中是否包含维度相关的正面关键词
            verdict = self._rule_dimension_verdict(prompt, name, question)
            dim_results.append(verdict)

        if not dim_results:
            return JudgeResult(overall_passed=True, overall_score=1.0, summary="无评估维度")

        overall_passed = all(d.passed for d in dim_results)
        weights = [dim.get("weight", 1.0) for dim in dimensions]
        total_weight = sum(weights) or 1.0
        overall_score = round(
            sum(d.score * w for d, w in zip(dim_results, weights)) / total_weight, 4
        )

        return JudgeResult(
            dimensions=dim_results,
            overall_passed=overall_passed,
            overall_score=overall_score,
            summary=f"{sum(1 for d in dim_results if d.passed)}/{len(dim_results)} 维度通过",
        )

    def noul(self, prompt: str, question: str) -> NoulResult:
        """基于规则关键词的二元判定。"""
        q_lower = question.lower()
        p_lower = prompt.lower()

        # 危险判定
        if "危险" in q_lower or "danger" in q_lower or "破坏" in q_lower:
            for pattern in self.DANGEROUS_PATTERNS:
                if re.search(pattern, p_lower, re.IGNORECASE):
                    return NoulResult(
                        answer=True,
                        confidence=0.95,
                        reason=f"匹配危险模式: {pattern}",
                    )
            return NoulResult(
                answer=False,
                confidence=0.60,
                reason="规则扫描未发现已知危险模式（不保证覆盖所有变体）",
            )

        # 默认保守策略：规则不确定 → 返回低置信度
        return NoulResult(
            answer=False,
            confidence=0.50,
            reason=f"规则引擎无法可靠判定: {question[:80]}",
        )

    def score(
        self, prompt: str, min_val: float, max_val: float, instructions: str = ""
    ) -> ScoreResult:
        """基于规则关键词的数值评分。"""
        instr_lower = instructions.lower()
        p_lower = prompt.lower()

        # 风险评分：检测危险关键词
        if "风险" in instr_lower or "risk" in instr_lower:
            danger_count = sum(
                1 for pattern in self.DANGEROUS_PATTERNS
                if re.search(pattern, p_lower, re.IGNORECASE)
            )
            if danger_count > 0:
                score = min_val + (max_val - min_val) * min(danger_count / 3, 1.0)
                return ScoreResult(
                    value=round(score, 1),
                    confidence=0.85,
                    reason=f"检测到 {danger_count} 个危险模式",
                )
            return ScoreResult(
                value=min_val,
                confidence=0.50,
                reason="规则扫描未发现已知危险模式",
            )

        # 默认中值
        mid = (min_val + max_val) / 2
        return ScoreResult(
            value=round(mid, 1),
            confidence=0.40,
            reason="规则引擎无法可靠评分，返回中值",
        )

    def health_check(self) -> bool:
        """本地规则引擎始终可用。"""
        return True

    # ── 内部辅助 ──

    @staticmethod
    def _rule_dimension_verdict(
        prompt: str, name: str, question: str
    ) -> DimensionVerdict:
        """对单个维度做规则判定。"""

        # 安全维度
        if "safe" in name.lower() or "安全" in name:
            danger_indicators = ["rm -rf", "DROP ", "mkfs", "dd if=", "shutdown"]
            hits = [kw for kw in danger_indicators if kw.lower() in prompt.lower()]
            if hits:
                return DimensionVerdict(
                    name=name, passed=False, score=0.1,
                    reason=f"检测到危险操作: {', '.join(hits)}",
                )
            return DimensionVerdict(name=name, passed=True, score=0.9, reason="未检测到已知危险操作")

        # 一致性维度
        if "consist" in name.lower() or "一致" in name:
            return DimensionVerdict(
                name=name, passed=True, score=0.65,
                reason="规则引擎仅做浅层一致性检查",
            )

        # 置信度维度
        if "confidence" in name.lower() or "置信" in name:
            return DimensionVerdict(
                name=name, passed=True, score=0.70,
                reason="规则引擎不校准置信度",
            )

        # 默认
        return DimensionVerdict(
            name=name, passed=True, score=0.60,
            reason=f"规则引擎对 '{name}' 维度不做深度判定",
        )