"""DeerFlow 风格的 Eval-Improve 循环引擎。

核心类：
- PromptTemplateManager: 管理可替换的 prompt 模板
- EvalScorer: 对 ActionPlan 进行四维度评分
- EvalRunner: 单轮评估运行器
- PromptImprover: 基于失败用例改进 prompt 模板
- EvalImproveLoop: 循环主控
"""

import json
import random
import time
import uuid
from collections import defaultdict
from statistics import mean, stdev
from typing import List, Optional, Tuple

from core.eval_models import (
    AggregateStats,
    CaseResult,
    DimensionScore,
    DimensionStats,
    EvalCase,
    ExpectedOutcome,
    LoopResult,
    RunReport,
    TagStats,
)
from core.llm_engine import parse_action_plan_json
from core.models import ActionPlan, Alert
from interfaces.base import ILLMClient


# ---------------------------------------------------------------------------
# Prompt 模板管理
# ---------------------------------------------------------------------------

DEFAULT_TEMPLATE = """你是一个生产系统 AIOps 专家。请分析以下告警：
【告警名称】: {alert_title}
【告警级别】: {alert_level}
【告警详情】: {alert_content}
【历史经验推荐】:
{history_sops}
【可用 Skills 执行结果】:
{skill_context}

请严格以 JSON 格式输出，包含以下字段：
- root_cause_analysis (str): 根因推测
- troubleshooting_steps (list[str]): 排查步骤
- script_content (str | null): 如果有明确修复手段，提供可执行脚本代码，否则为 null
- confidence_score (float): 你对该结论的置信度，0.0 到 1.0 的小数。
"""

REQUIRED_PLACEHOLDERS = [
    "{alert_title}",
    "{alert_level}",
    "{alert_content}",
    "{history_sops}",
    "{skill_context}",
]


class PromptTemplateManager:
    """管理认知引擎的 Prompt 模板，支持模板替换和渲染"""

    def __init__(self, template: Optional[str] = None):
        self.template = template or DEFAULT_TEMPLATE

    def render(
        self, alert: Alert, history_sops: List[str], skill_context: str
    ) -> str:
        """将模板渲染为完整 prompt"""
        sops_str = "\n".join(history_sops) if history_sops else "无历史参考"
        return self.template.format(
            alert_title=alert.title,
            alert_level=alert.level,
            alert_content=alert.content,
            history_sops=sops_str,
            skill_context=skill_context,
        )

    @staticmethod
    def validate_placeholders(template: str) -> bool:
        """检查模板是否包含所有必需的占位符"""
        return all(ph in template for ph in REQUIRED_PLACEHOLDERS)


# ---------------------------------------------------------------------------
# 评分器
# ---------------------------------------------------------------------------


class EvalScorer:
    """评估 ActionPlan 质量的四维度打分器"""

    WEIGHTS = {
        "root_cause_relevance": 0.35,
        "step_quality": 0.25,
        "confidence_calibration": 0.15,
        "script_relevance": 0.25,
    }

    def __init__(self, pass_threshold: float = 0.6):
        self.pass_threshold = pass_threshold

    def score(
        self, plan: ActionPlan, expected: ExpectedOutcome
    ) -> List[DimensionScore]:
        return [
            self._score_root_cause(plan, expected),
            self._score_steps(plan, expected),
            self._score_confidence(plan, expected),
            self._score_script(plan, expected),
        ]

    def overall_score(self, dimensions: List[DimensionScore]) -> float:
        total = sum(
            dim.score * self.WEIGHTS.get(dim.name, 0.0) for dim in dimensions
        )
        return round(total, 4)

    # ---- 各维度评分 ----

    def _score_root_cause(
        self, plan: ActionPlan, expected: ExpectedOutcome
    ) -> DimensionScore:
        text = plan.root_cause_analysis.lower()
        keywords = expected.root_cause_keywords
        if not keywords:
            return DimensionScore(
                name="root_cause_relevance", score=0.5, passed=True, detail="无期望关键词"
            )
        matched = sum(1 for kw in keywords if kw.lower() in text)
        ratio = matched / len(keywords)
        score = min(ratio * 1.2, 1.0)  # 轻微放大，1 个命中即可及格
        return DimensionScore(
            name="root_cause_relevance",
            score=round(score, 4),
            passed=score >= 0.3,
            detail=f"命中 {matched}/{len(keywords)} 个关键词",
        )

    def _score_steps(
        self, plan: ActionPlan, expected: ExpectedOutcome
    ) -> DimensionScore:
        steps = plan.troubleshooting_steps
        min_steps = expected.min_troubleshooting_steps

        # 步骤数达标
        count_ok = len(steps) >= min_steps if min_steps > 0 else True
        count_score = 1.0 if count_ok else len(steps) / max(min_steps, 1)

        # 关键词命中
        step_text = " ".join(steps).lower()
        keywords = expected.step_keywords
        if keywords:
            kw_hit = any(kw.lower() in step_text for kw in keywords)
            kw_score = 1.0 if kw_hit else 0.0
        else:
            kw_score = 0.5  # 无期望关键词，给中性分

        # 步骤具体性：平均长度 > 5 字符
        if steps:
            avg_len = mean(len(s) for s in steps)
            specificity = min(avg_len / 20.0, 1.0)
        else:
            specificity = 0.0

        score = 0.4 * count_score + 0.4 * kw_score + 0.2 * specificity
        return DimensionScore(
            name="step_quality",
            score=round(score, 4),
            passed=score >= 0.4,
            detail=f"步骤数 {len(steps)}/{min_steps}, 关键词命中={'是' if kw_score > 0.5 else '否'}",
        )

    def _score_confidence(
        self, plan: ActionPlan, expected: ExpectedOutcome
    ) -> DimensionScore:
        c = plan.confidence_score
        lo, hi = expected.min_confidence, expected.max_confidence
        if lo <= c <= hi:
            score = 1.0
        else:
            dist = lo - c if c < lo else c - hi
            score = max(1.0 - dist / 0.3, 0.0)
        return DimensionScore(
            name="confidence_calibration",
            score=round(score, 4),
            passed=score >= 0.5,
            detail=f"置信度 {c:.2f}, 期望 [{lo:.2f}, {hi:.2f}]",
        )

    def _score_script(
        self, plan: ActionPlan, expected: ExpectedOutcome
    ) -> DimensionScore:
        if expected.expect_script:
            if not plan.script_content:
                return DimensionScore(
                    name="script_relevance",
                    score=0.0,
                    passed=False,
                    detail="期望有脚本但未提供",
                )
            script_lower = plan.script_content.lower()
            keywords = expected.script_keywords
            if keywords:
                matched = sum(1 for kw in keywords if kw.lower() in script_lower)
                score = min(matched / len(keywords) * 1.5, 1.0)
            else:
                score = 0.7  # 有脚本但无关键词要求
            return DimensionScore(
                name="script_relevance",
                score=round(score, 4),
                passed=score >= 0.3,
                detail=f"脚本关键词命中 {matched}/{len(keywords) if keywords else 0}",
            )
        else:
            # 不期望脚本时，有脚本不扣分
            score = 1.0 if not plan.script_content else 0.8
            return DimensionScore(
                name="script_relevance",
                score=round(score, 4),
                passed=True,
                detail="不期望脚本" + (", 已忽略" if plan.script_content else ""),
            )


# ---------------------------------------------------------------------------
# 单轮评估运行器
# ---------------------------------------------------------------------------


class EvalRunner:
    """单轮评估：遍历 train + test 用例，调 LLM 生成方案并评分"""

    def __init__(
        self,
        llm_client: ILLMClient,
        prompt_template: str,
        train_cases: List[EvalCase],
        test_cases: List[EvalCase],
        scorer: Optional[EvalScorer] = None,
    ):
        self.llm_client = llm_client
        self.template_mgr = PromptTemplateManager(prompt_template)
        self.train_cases = train_cases
        self.test_cases = test_cases
        self.scorer = scorer or EvalScorer()

    def run(self) -> RunReport:
        train_results = self._evaluate_cases(self.train_cases)
        test_results = self._evaluate_cases(self.test_cases)
        aggregate = self._compute_aggregate(test_results)
        return RunReport(
            run_id=uuid.uuid4().hex[:8],
            prompt_template=self.template_mgr.template,
            train_results=train_results,
            test_results=test_results,
            aggregate=aggregate,
        )

    def _evaluate_cases(self, cases: List[EvalCase]) -> List[CaseResult]:
        return [self._evaluate_single(case) for case in cases]

    def _evaluate_single(self, case: EvalCase) -> CaseResult:
        alert = Alert(**case.alert)
        prompt = self.template_mgr.render(alert, [], "无可用 skill 输出")
        start = time.perf_counter()
        try:
            response_text = self.llm_client.generate_proposal(prompt)
            plan = parse_action_plan_json(response_text)
            latency_ms = (time.perf_counter() - start) * 1000
            dimensions = self.scorer.score(plan, case.expected)
            overall = self.scorer.overall_score(dimensions)
            return CaseResult(
                case_id=case.case_id,
                alert_title=alert.title,
                alert_level=alert.level,
                dimensions=dimensions,
                overall_score=overall,
                passed=overall >= self.scorer.pass_threshold,
                generated_plan=plan.model_dump(),
                latency_ms=round(latency_ms, 2),
            )
        except Exception as e:
            latency_ms = (time.perf_counter() - start) * 1000
            return CaseResult(
                case_id=case.case_id,
                alert_title=alert.title,
                alert_level=alert.level,
                dimensions=[],
                overall_score=0.0,
                passed=False,
                generated_plan={},
                latency_ms=round(latency_ms, 2),
                error=str(e),
            )

    def _compute_aggregate(self, results: List[CaseResult]) -> AggregateStats:
        if not results:
            return AggregateStats(
                total_cases=0,
                passed_cases=0,
                failed_cases=0,
                pass_rate=0.0,
                avg_overall_score=0.0,
                avg_latency_ms=0.0,
            )

        passed = [r for r in results if r.passed]

        # 按维度统计
        dim_buckets: dict = defaultdict(list)
        for r in results:
            for d in r.dimensions:
                dim_buckets[d.name].append(d.score)

        per_dimension = {}
        for name, scores in dim_buckets.items():
            per_dimension[name] = DimensionStats(
                mean=round(mean(scores), 4),
                stddev=round(stdev(scores), 4) if len(scores) > 1 else 0.0,
                min_score=round(min(scores), 4),
                max_score=round(max(scores), 4),
                pass_rate=round(
                    sum(1 for s in scores if s >= 0.5) / len(scores), 4
                ),
            )

        return AggregateStats(
            total_cases=len(results),
            passed_cases=len(passed),
            failed_cases=len(results) - len(passed),
            pass_rate=round(len(passed) / len(results), 4),
            avg_overall_score=round(
                mean(r.overall_score for r in results), 4
            ),
            avg_latency_ms=round(
                mean(r.latency_ms for r in results), 2
            ),
            per_dimension=per_dimension,
        )


# ---------------------------------------------------------------------------
# Prompt 改进器
# ---------------------------------------------------------------------------

IMPROVE_PROMPT_TEMPLATE = """你是一个 Prompt 工程专家。当前的 AIOps 告警分析 prompt 模板如下：

--- 当前 Prompt 模板 ---
{current_template}
--- 结束 ---

以下是一些评估失败的用例（LLM 生成的方案质量不达标）：

{failure_summary}

请分析失败原因，并生成一个改进后的 prompt 模板。

要求：
1. 必须保留所有占位符：{{alert_title}}, {{alert_level}}, {{alert_content}}, {{history_sops}}, {{skill_context}}
2. 可以添加更明确的指导语来引导 LLM 输出更好的根因分析和排查步骤
3. 可以添加输出格式约束、质量要求等
4. 保持简洁，不要过度膨胀 prompt 长度
5. 只输出改进后的 prompt 模板文本，不要包含其他解释

请输出改进后的 prompt 模板："""


class PromptImprover:
    """基于评估失败用例，使用 LLM 改进 prompt 模板"""

    def __init__(self, llm_client: ILLMClient):
        self.llm_client = llm_client

    def improve(
        self,
        current_template: str,
        failed_cases: List[CaseResult],
        max_failed_cases: int = 5,
    ) -> str:
        failed = sorted(failed_cases, key=lambda c: c.overall_score)
        selected = failed[:max_failed_cases]

        failure_summary = self._format_failure_summary(selected)
        prompt = IMPROVE_PROMPT_TEMPLATE.format(
            current_template=current_template,
            failure_summary=failure_summary,
        )

        response = self.llm_client.generate_proposal(prompt)
        improved = self._extract_template(response)

        # 验证占位符完整性
        if not PromptTemplateManager.validate_placeholders(improved):
            print("[PromptImprover] 改进后的模板缺少占位符，回退到当前模板")
            return current_template

        # 长度限制
        if len(improved) > 3000:
            print("[PromptImprover] 改进后的模板过长，回退到当前模板")
            return current_template

        return improved

    @staticmethod
    def _format_failure_summary(cases: List[CaseResult]) -> str:
        lines = []
        for i, case in enumerate(cases, 1):
            lines.append(f"--- 失败用例 {i} ---")
            lines.append(
                f"告警: {case.alert_title} (Level: {case.alert_level})"
            )
            lines.append(f"总分: {case.overall_score:.4f}")
            for dim in case.dimensions:
                status = "通过" if dim.passed else "未通过"
                lines.append(
                    f"  维度 [{dim.name}]: {dim.score:.4f} ({status}) - {dim.detail}"
                )
            rca = case.generated_plan.get("root_cause_analysis", "")
            if rca:
                lines.append(f"  生成的根因: {rca[:200]}")
            if case.error:
                lines.append(f"  错误: {case.error}")
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _extract_template(response: str) -> str:
        cleaned = response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines)
        return cleaned.strip()


# ---------------------------------------------------------------------------
# 循环主控
# ---------------------------------------------------------------------------


class EvalImproveLoop:
    """DeerFlow 风格的 Eval-Improve 循环主控"""

    def __init__(
        self,
        llm_client: ILLMClient,
        eval_cases: List[EvalCase],
        max_iterations: int = 5,
        pass_threshold: float = 0.6,
        train_ratio: float = 0.6,
        early_stop_delta: float = 0.01,
        seed: int = 42,
    ):
        self.llm_client = llm_client
        self.eval_cases = eval_cases
        self.max_iterations = max_iterations
        self.pass_threshold = pass_threshold
        self.train_ratio = train_ratio
        self.early_stop_delta = early_stop_delta
        self.seed = seed

        self.train_cases, self.test_cases = self._split_cases()
        self.improver = PromptImprover(llm_client)
        self.scorer = EvalScorer(pass_threshold=pass_threshold)
        self.reports: List[RunReport] = []

    def run(self, initial_template: Optional[str] = None) -> LoopResult:
        template = initial_template or DEFAULT_TEMPLATE
        best_test_score = -1.0
        best_template = template
        best_iteration = 0

        for iteration in range(1, self.max_iterations + 1):
            print(f"\n{'=' * 60}")
            print(f"[EvalLoop] 第 {iteration}/{self.max_iterations} 轮评估")
            print(f"{'=' * 60}")

            # 1. 运行评估
            runner = EvalRunner(
                llm_client=self.llm_client,
                prompt_template=template,
                train_cases=self.train_cases,
                test_cases=self.test_cases,
                scorer=self.scorer,
            )
            report = runner.run()
            self.reports.append(report)

            test_avg = self._test_avg_score(report)
            train_avg = self._train_avg_score(report)

            print(f"[EvalLoop] 训练集 avg_score: {train_avg:.4f}")
            print(f"[EvalLoop] 测试集 avg_score: {test_avg:.4f}")
            print(
                f"[EvalLoop] 测试集 pass_rate: {report.aggregate.pass_rate:.2%}"
            )

            # 2. 记录最佳（以 test 集为标准，防过拟合）
            if test_avg > best_test_score:
                best_test_score = test_avg
                best_template = template
                best_iteration = iteration
                print(
                    f"[EvalLoop] >>> 新的最佳模板！iteration={iteration}"
                )

            # 3. 提前停止检查
            if iteration > 1:
                prev_score = self._test_avg_score(self.reports[-2])
                delta = test_avg - prev_score
                if delta < self.early_stop_delta:
                    print(
                        f"[EvalLoop] 改进幅度 {delta:.4f} < {self.early_stop_delta}，提前停止"
                    )
                    break

            # 4. 最后一轮不再改进
            if iteration >= self.max_iterations:
                break

            # 5. 用训练集失败用例改进 prompt
            train_failed = [r for r in report.train_results if not r.passed]
            if not train_failed:
                print("[EvalLoop] 训练集全部通过，无需改进")
                break

            print(
                f"[EvalLoop] 训练集 {len(train_failed)} 条失败，启动 prompt 改进..."
            )
            template = self.improver.improve(template, train_failed)
            print(f"[EvalLoop] 改进后模板长度: {len(template)} 字符")

        # 构建最终结果
        first_score = (
            self._test_avg_score(self.reports[0]) if self.reports else 0.0
        )
        best_pass_rate = (
            self.reports[best_iteration - 1].aggregate.pass_rate
            if best_iteration > 0 and best_iteration <= len(self.reports)
            else 0.0
        )
        return LoopResult(
            iterations=len(self.reports),
            best_iteration=best_iteration,
            best_prompt=best_template,
            best_test_pass_rate=best_pass_rate,
            best_test_avg_score=best_test_score,
            all_reports=self.reports,
            improvement_delta=round(best_test_score - first_score, 4),
        )

    def _split_cases(self) -> Tuple[List[EvalCase], List[EvalCase]]:
        rng = random.Random(self.seed)
        shuffled = list(self.eval_cases)
        rng.shuffle(shuffled)
        split_idx = int(len(shuffled) * self.train_ratio)
        return shuffled[:split_idx], shuffled[split_idx:]

    @staticmethod
    def _test_avg_score(report: RunReport) -> float:
        if not report.test_results:
            return 0.0
        return mean(r.overall_score for r in report.test_results)

    @staticmethod
    def _train_avg_score(report: RunReport) -> float:
        if not report.train_results:
            return 0.0
        return mean(r.overall_score for r in report.train_results)
