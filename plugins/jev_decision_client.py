"""JevDecisionClient — Jev 概率语义模型的 IDecisionClient 实现。

包含两个类：
- JevDecisionClientStub:  本地桩，返回保守的模拟结果（waitlist 期 / 开发环境）
- JevDecisionClient:      HTTP 调用真实的 Jev API（POST /v1/systemone）

通过 IDecisionClient 接口隔离，业务代码无需感知当前是 stub 还是真实 Jev。

典型用法：
    from plugins.jev_decision_client import create_jev_client

    jev = create_jev_client()                  # 自动检测环境 → stub
    jev = create_jev_client(api_key="...")     # 有 key → 真实客户端
"""

import json
import os
import random
from typing import Dict, List, Any, Optional

import httpx

from core.models import (
    DecisionResult,
    DimensionVerdict,
    JudgeResult,
    NoulResult,
    ScoreResult,
)
from interfaces.base import IDecisionClient


# ═══════════════════════════════════════════════════════════════
# Stub / 桩实现
# ═══════════════════════════════════════════════════════════════


class JevDecisionClientStub(IDecisionClient):
    """Jev 本地桩 — waitlist 期 / 离线开发使用。

    不做任何模型调用，返回保守的模拟结果。
    所有判定都带低置信度标记，确保不会在无真实模型时做出高风险决策。
    """

    STUB_MARKER = "[STUB] 未接入 Jev 真实模型，返回保守结果"

    def classify(self, prompt: str, options: List[str]) -> DecisionResult:
        # 对齐 RuleDecisionClient 的行为：从 prompt 中匹配关键词
        # 但与 RuleDecisionClient 不同，stub 返回低置信度以表明这是 stub 结果
        if "general" in options:
            choice = "general"
        elif options:
            choice = options[0]
        else:
            choice = "unknown"

        return DecisionResult(
            choice=choice,
            confidence=0.40,
            reason=f"{self.STUB_MARKER}: 默认选择 '{choice}'",
            alternatives=[],
        )

    def judge(
        self, prompt: str, dimensions: List[Dict[str, Any]]
    ) -> JudgeResult:
        dim_results = []
        for dim in dimensions:
            dim_results.append(DimensionVerdict(
                name=dim.get("name", "unknown"),
                passed=True,
                score=0.50,
                reason=f"{self.STUB_MARKER}: 默认通过",
            ))

        return JudgeResult(
            dimensions=dim_results,
            overall_passed=True,
            overall_score=0.50,
            summary=f"{self.STUB_MARKER}: 所有维度默认通过（保守策略）",
        )

    def noul(self, prompt: str, question: str) -> NoulResult:
        q_lower = question.lower()

        # 安全相关：保守策略 → 倾向判危险
        if any(kw in q_lower for kw in ["危险", "danger", "破坏", "destruct"]):
            return NoulResult(
                answer=True,
                confidence=0.40,
                reason=f"{self.STUB_MARKER}: 安全类问题保守判 Yes",
            )

        # 默认判 No
        return NoulResult(
            answer=False,
            confidence=0.35,
            reason=f"{self.STUB_MARKER}: 默认判 No",
        )

    def score(
        self, prompt: str, min_val: float, max_val: float, instructions: str = ""
    ) -> ScoreResult:
        mid = (min_val + max_val) / 2
        return ScoreResult(
            value=round(mid, 1),
            confidence=0.30,
            reason=f"{self.STUB_MARKER}: 返回中值",
        )

    def health_check(self) -> bool:
        return False  # stub 明确标记"不可用"


# ═══════════════════════════════════════════════════════════════
# 真实 Jev HTTP 客户端
# ═══════════════════════════════════════════════════════════════


class JevDecisionClient(IDecisionClient):
    """Jev HTTP 客户端 — 调用真实 Jev API。

    API: POST /v1/systemone
    Body: { questions: [ { type, instructions, options?, min?, max? } ], context: str }
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: int = 30,
    ):
        self.api_key = api_key or os.getenv("JEV_API_KEY", "")
        self.base_url = base_url or os.getenv(
            "JEV_BASE_URL", "https://api.jev.ai"
        )
        self.timeout = timeout

        self._client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )

    # ── IDecisionClient 实现 ──

    def classify(self, prompt: str, options: List[str]) -> DecisionResult:
        questions = [{
            "type": "choice",
            "instructions": "根据上下文选择最合适的分类",
            "options": options,
        }]

        try:
            response = self._call(prompt, questions)
            answers = response.get("answers", [{}])
            answer = answers[0] if answers else {}
            return DecisionResult(
                choice=answer.get("choice", options[0] if options else "unknown"),
                confidence=answer.get("confidence", 0.0),
                reason=answer.get("reason", ""),
                alternatives=answer.get("alternatives", []),
            )
        except Exception as e:
            # 降级：返回低置信度的默认结果
            return DecisionResult(
                choice=options[0] if options else "unknown",
                confidence=0.30,
                reason=f"Jev API 调用失败，降级默认: {e}",
                alternatives=[],
            )

    def judge(
        self, prompt: str, dimensions: List[Dict[str, Any]]
    ) -> JudgeResult:
        questions = []
        for dim in dimensions:
            q_type = dim.get("type", "noul")
            if q_type == "noul":
                questions.append({
                    "type": "noul",
                    "instructions": dim.get("question", dim.get("name", "")),
                })
            elif q_type == "score":
                questions.append({
                    "type": "score",
                    "instructions": dim.get("question", dim.get("name", "")),
                    "min": dim.get("min", 0),
                    "max": dim.get("max", 10),
                })
            else:  # choice
                questions.append({
                    "type": "choice",
                    "instructions": dim.get("question", dim.get("name", "")),
                    "options": dim.get("options", ["pass", "fail"]),
                })

        try:
            response = self._call(prompt, questions)
            answers = response.get("answers", [])

            dim_results = []
            for i, dim in enumerate(dimensions):
                ans = answers[i] if i < len(answers) else {}
                name = dim.get("name", f"dim_{i}")

                if dim.get("type") == "noul" or "answer" in ans:
                    dim_results.append(DimensionVerdict(
                        name=name,
                        passed=ans.get("answer", ans.get("choice") == "pass"),
                        score=ans.get("confidence", 0.5),
                        reason=ans.get("reason", ""),
                    ))
                elif dim.get("type") == "score" or "value" in ans:
                    val = ans.get("value", 5)
                    max_val = dim.get("max", 10)
                    normalized = val / max_val if max_val > 0 else 0.5
                    dim_results.append(DimensionVerdict(
                        name=name,
                        passed=normalized >= dim.get("pass_threshold", 0.5),
                        score=normalized,
                        reason=ans.get("reason", ""),
                    ))
                else:
                    dim_results.append(DimensionVerdict(
                        name=name,
                        passed=ans.get("choice") == "pass",
                        score=ans.get("confidence", 0.5),
                        reason=ans.get("reason", ""),
                    ))

            weights = [dim.get("weight", 1.0) for dim in dimensions]
            total_weight = sum(weights) or 1.0
            overall_score = round(
                sum(d.score * w for d, w in zip(dim_results, weights)) / total_weight, 4
            )
            overall_passed = all(d.passed for d in dim_results)

            return JudgeResult(
                dimensions=dim_results,
                overall_passed=overall_passed,
                overall_score=overall_score,
                summary=response.get("summary", ""),
            )

        except Exception as e:
            # 降级
            dim_results = [
                DimensionVerdict(
                    name=dim.get("name", f"dim_{i}"),
                    passed=True,  # 保守策略
                    score=0.50,
                    reason=f"Jev API 调用失败，默认通过: {e}",
                )
                for i, dim in enumerate(dimensions)
            ]
            return JudgeResult(
                dimensions=dim_results,
                overall_passed=True,
                overall_score=0.50,
                summary=f"Jev API 调用失败，降级: {e}",
            )

    def noul(self, prompt: str, question: str) -> NoulResult:
        questions = [{
            "type": "noul",
            "instructions": question,
        }]

        try:
            response = self._call(prompt, questions)
            answers = response.get("answers", [{}])
            answer = answers[0] if answers else {}
            return NoulResult(
                answer=answer.get("answer", False),
                confidence=answer.get("confidence", 0.0),
                reason=answer.get("reason", ""),
            )
        except Exception as e:
            return NoulResult(
                answer=False,
                confidence=0.30,
                reason=f"Jev API 调用失败，降级默认 False: {e}",
            )

    def score(
        self, prompt: str, min_val: float, max_val: float, instructions: str = ""
    ) -> ScoreResult:
        questions = [{
            "type": "score",
            "instructions": instructions or "根据上下文打分",
            "min": min_val,
            "max": max_val,
        }]

        try:
            response = self._call(prompt, questions)
            answers = response.get("answers", [{}])
            answer = answers[0] if answers else {}
            return ScoreResult(
                value=answer.get("value", (min_val + max_val) / 2),
                confidence=answer.get("confidence", 0.0),
                reason=answer.get("reason", ""),
            )
        except Exception as e:
            return ScoreResult(
                value=(min_val + max_val) / 2,
                confidence=0.30,
                reason=f"Jev API 调用失败，降级中值: {e}",
            )

    def health_check(self) -> bool:
        try:
            self._client.get("/health", timeout=5)
            return True
        except Exception:
            return False

    # ── 内部 ──

    def _call(self, context: str, questions: List[dict]) -> dict:
        """调用 Jev SystemOne API。"""
        payload = {
            "context": context,
            "questions": questions,
        }
        resp = self._client.post("/v1/systemone", json=payload)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._client.close()


# ═══════════════════════════════════════════════════════════════
# 工厂
# ═══════════════════════════════════════════════════════════════


def create_jev_client(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> "JevDecisionClient | JevDecisionClientStub":
    """创建 Jev 决策客户端。

    自动检测：
    - 有 api_key 或 JEV_API_KEY 环境变量 → 真实 JevDecisionClient
    - 无 → JevDecisionClientStub

    也可显式传入 api_key 强制使用真实客户端。
    """
    key = api_key or os.getenv("JEV_API_KEY", "").strip()
    if key:
        return JevDecisionClient(api_key=key, base_url=base_url)
    return JevDecisionClientStub()