"""评估 Agent — 负责方案评估与重试循环。

从 eval_graph.py 提取评估逻辑（check_confidence → check_safety → check_consistency
→ check_llm_judge），封装为独立 Agent。重试循环在 Python 层实现。

内部图（5-6 节点，不含 reason）:
  check_confidence → check_safety → check_consistency → [check_llm_judge] → pass_eval / block_eval
"""

import functools
from typing import Any, Callable, Dict, List, NotRequired, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from agents.base import BaseAgent
from core.evaluator import SolutionEvaluator
from core.logger import get_logger
from core.models import (
    Alert,
    EvaluationResult,
    LLMProposal,
)
from core.observability import metrics
from interfaces.base import ILLMClient

_log = get_logger("evaluation_agent")


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class EvalAgentState(TypedDict):
    """FR-4 纯数据化：evaluator 实例经图构建参数（partial）绑定，不进 state ——
    嵌套 check 图会继承父 pipeline 的 checkpointer，state 内嵌实例会导致
    checkpoint 序列化失败（SolutionEvaluator 不可 msgpack 序列化）。"""
    alert: Alert
    proposal: LLMProposal
    eval_result: NotRequired[EvaluationResult]
    _block: NotRequired[bool]


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def check_confidence(evaluator: SolutionEvaluator, state: EvalAgentState) -> dict:
    """置信度检查。evaluator 经图构建参数绑定（FR-4 纯数据化）。"""
    plan = state["proposal"].plan
    alert_id = state["alert"].alert_id

    _log.logger.info(
        "[评估] 置信度检查 | alert_id={} | confidence={:.2f} | threshold={:.2f}",
        alert_id, plan.confidence_score, evaluator.min_confidence,
    )

    if plan.confidence_score >= evaluator.min_confidence:
        _log.logger.info(
            "[评估] 置信度通过 ✓ | alert_id={} | {:.2f} >= {:.2f}",
            alert_id, plan.confidence_score, evaluator.min_confidence,
        )
        return {"_block": False, "eval_result": None}

    metrics.incr("pipeline.evaluation.blocked.low_confidence")
    _log.logger.warning(
        "[评估] 置信度不足 ✗ | alert_id={} | {:.2f} < {:.2f}",
        alert_id, plan.confidence_score, evaluator.min_confidence,
    )
    return {
        "eval_result": EvaluationResult(
            is_passed=False,
            reason=f"blocked: confidence too low ({plan.confidence_score} < {evaluator.min_confidence})",
            risk_level="Unknown",
        ),
        "_block": True,
    }


def check_safety(evaluator: SolutionEvaluator, state: EvalAgentState) -> dict:
    """脚本安全扫描 + 可逆性分析。evaluator 经图构建参数绑定。"""
    proposal = state["proposal"]
    scanner = evaluator.script_scanner
    alert_id = state["alert"].alert_id

    script = proposal.plan.script_content
    if not script:
        _log.logger.info("[评估] 安全扫描跳过（无脚本）| alert_id={}", alert_id)
        return {"_block": False}

    _log.logger.info("[评估] 安全扫描开始 | alert_id={}", alert_id)

    findings = scanner.find_risks(script)
    if findings:
        metrics.incr("pipeline.evaluation.blocked.script_risk")
        _log.logger.warning(
            "[评估] 安全扫描拦截 ✗ | alert_id={} | 危险命令={}", alert_id, findings,
        )
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=f"blocked: dangerous commands detected {findings}",
                risk_level="High",
            ),
            "_block": True,
        }

    from core.evaluator import ScriptReversibilityAnalyzer, Reversibility
    reversibility = ScriptReversibilityAnalyzer.analyze(script)
    _log.logger.info(
        "[评估] 可逆性分析 | alert_id={} | reversibility={}", alert_id, reversibility.value,
    )

    if reversibility == Reversibility.IRREVERSIBLE:
        metrics.incr("pipeline.evaluation.blocked.irreversible")
        _log.logger.warning("[评估] 不可逆操作拦截 ✗ | alert_id={}", alert_id)
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=(
                    "blocked: script contains irreversible operations "
                    "with no recovery path, requires manual approval"
                ),
                risk_level="High",
            ),
            "_block": True,
        }
    if reversibility == Reversibility.NEEDS_ROLLBACK:
        metrics.incr("pipeline.evaluation.blocked.needs_rollback")
        _log.logger.warning("[评估] 需回滚保护拦截 ✗ | alert_id={}", alert_id)
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=(
                    "blocked: script contains destructive operations "
                    "without backup/rollback protection. "
                    "Add backup steps (e.g., cp/tar before rm) "
                    "or include a rollback script."
                ),
                risk_level="Medium",
            ),
            "_block": True,
        }

    # 修复：与 SolutionEvaluator.evaluate 对齐 —— 补 preflight 阻断级检查
    # （逃逸/动态执行阻断，混淆类告警不拦截）
    preflight_issues = scanner.preflight_blocking(script)
    if preflight_issues:
        metrics.incr("pipeline.evaluation.blocked.preflight")
        _log.logger.warning(
            "[评估] preflight 拦截 ✗ | alert_id={} | issues={}", alert_id, preflight_issues,
        )
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=f"blocked: preflight check failed - {preflight_issues}",
                risk_level="High",
            ),
            "_block": True,
        }
    preflight_warns = scanner.preflight_warnings(script)
    if preflight_warns:
        metrics.incr("pipeline.evaluation.warn.preflight_obfuscation")
        _log.logger.info(
            "[评估] preflight 告警（不拦截）| alert_id={} | warns={}", alert_id, preflight_warns,
        )

    _log.logger.info("[评估] 安全扫描通过 ✓ | alert_id={}", alert_id)
    return {"_block": False}


def check_consistency(evaluator: SolutionEvaluator, state: EvalAgentState) -> dict:
    """根因分析与排查步骤之间的一致性检查。evaluator 经图构建参数绑定。"""
    alert_id = state["alert"].alert_id

    if not evaluator.consistency_check_enabled:
        _log.logger.info("[评估] 一致性检查跳过（已禁用）| alert_id={}", alert_id)
        return {"_block": False}

    from core.evaluator import _ConsistencyChecker

    issue = _ConsistencyChecker.check(state["alert"], state["proposal"])
    if issue:
        metrics.incr("pipeline.evaluation.blocked.inconsistency")
        _log.logger.warning(
            "[评估] 一致性检查拦截 ✗ | alert_id={} | issue={}", alert_id, issue,
        )
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=f"blocked: consistency check failed - {issue}",
                risk_level="Medium",
            ),
            "_block": True,
        }

    _log.logger.info("[评估] 一致性检查通过 ✓ | alert_id={}", alert_id)
    return {"_block": False}


def check_llm_judge(evaluator: SolutionEvaluator, state: EvalAgentState) -> dict:
    """可选的 LLM Judge 二次评审。evaluator 经图构建参数绑定。"""
    alert_id = state["alert"].alert_id

    if not (evaluator.llm_judge_enabled and evaluator.llm_judge):
        return {"_block": False}

    _log.logger.info("[评估] LLM Judge 评审开始 | alert_id={}", alert_id)
    metrics.incr("pipeline.evaluation.llm_judge_invoked")
    issue = evaluator.llm_judge.judge(state["alert"], state["proposal"])
    if issue:
        metrics.incr("pipeline.evaluation.blocked.llm_judge")
        _log.logger.warning(
            "[评估] LLM Judge 拦截 ✗ | alert_id={} | issue={}", alert_id, issue,
        )
        return {
            "eval_result": EvaluationResult(
                is_passed=False,
                reason=f"blocked: LLM Judge rejected - {issue}",
                risk_level="Medium",
            ),
            "_block": True,
        }

    _log.logger.info("[评估] LLM Judge 通过 ✓ | alert_id={}", alert_id)
    return {"_block": False}


def pass_eval(state: EvalAgentState) -> dict:
    """所有检查通过。"""
    alert_id = state["alert"].alert_id
    confidence = state["proposal"].plan.confidence_score
    metrics.incr("pipeline.evaluation.passed")
    _log.logger.info(
        "[评估] 全部检查通过 ✓ | alert_id={} | confidence={:.2f} | risk=Low",
        alert_id, confidence,
    )
    return {
        "eval_result": EvaluationResult(
            is_passed=True,
            reason="passed: confidence OK, consistent, and safe",
            risk_level="Low",
        )
    }


def block_eval(state: EvalAgentState) -> dict:
    """拦截节点：eval_result 已由上游节点设置。"""
    alert_id = state["alert"].alert_id
    eval_result = state.get("eval_result")
    _log.logger.warning(
        "[评估] 方案被拦截 ✗ | alert_id={} | reason={} | risk={}",
        alert_id,
        eval_result.reason if eval_result else "unknown",
        eval_result.risk_level if eval_result else "Unknown",
    )
    return {}


# ---------------------------------------------------------------------------
# Conditional edge router
# ---------------------------------------------------------------------------

def _route_check(state: EvalAgentState) -> str:
    return "block_eval" if state.get("_block") else "next"


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=4)
def _build_eval_check_graph(evaluator: SolutionEvaluator, llm_judge_enabled: bool = False):
    """构建评估检查子图（不含 reason 节点）。

    FR-4 纯数据化：evaluator 经 functools.partial 绑定到节点，不进 state。
    """
    builder = StateGraph(EvalAgentState)

    builder.add_node("check_confidence", functools.partial(check_confidence, evaluator))
    builder.add_node("check_safety", functools.partial(check_safety, evaluator))
    builder.add_node("check_consistency", functools.partial(check_consistency, evaluator))
    builder.add_node("pass_eval", pass_eval)
    builder.add_node("block_eval", block_eval)

    if llm_judge_enabled:
        builder.add_node("check_llm_judge", functools.partial(check_llm_judge, evaluator))

    builder.add_edge(START, "check_confidence")

    builder.add_conditional_edges(
        "check_confidence", _route_check,
        {"next": "check_safety", "block_eval": "block_eval"},
    )
    builder.add_conditional_edges(
        "check_safety", _route_check,
        {"next": "check_consistency", "block_eval": "block_eval"},
    )

    if llm_judge_enabled:
        builder.add_conditional_edges(
            "check_consistency", _route_check,
            {"next": "check_llm_judge", "block_eval": "block_eval"},
        )
        builder.add_conditional_edges(
            "check_llm_judge", _route_check,
            {"next": "pass_eval", "block_eval": "block_eval"},
        )
    else:
        builder.add_conditional_edges(
            "check_consistency", _route_check,
            {"next": "pass_eval", "block_eval": "block_eval"},
        )

    builder.add_edge("pass_eval", END)
    builder.add_edge("block_eval", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Agent class
# ---------------------------------------------------------------------------

class EvaluationAgent(BaseAgent):
    """评估 Agent：方案评估 + 重试循环。"""

    def __init__(
        self,
        llm_client: Optional[ILLMClient] = None,
        min_confidence: Optional[float] = None,
        llm_judge_enabled: bool = False,
        consistency_check_enabled: bool = True,
    ):
        self.llm_client = llm_client
        self.evaluator = SolutionEvaluator(
            min_confidence=min_confidence,
            llm_client=llm_client if llm_judge_enabled else None,
        )
        self.evaluator.llm_judge_enabled = llm_judge_enabled
        self.evaluator.consistency_check_enabled = consistency_check_enabled

    def name(self) -> str:
        return "evaluation"

    def build_graph(self, llm_judge_enabled: bool = False):
        return _build_eval_check_graph(evaluator=self.evaluator, llm_judge_enabled=llm_judge_enabled)

    def _run_check_graph(self, alert: Alert, proposal: LLMProposal) -> EvalAgentState:
        """运行评估检查子图。

        FR-4 纯数据化：state 只含纯数据（alert/proposal）—— 嵌套 check 图
        会继承父 pipeline 的 checkpointer，evaluator 实例进 state 会导致
        checkpoint 序列化失败。
        """
        llm_judge_enabled = (
            self.evaluator.llm_judge_enabled and self.evaluator.llm_judge is not None
        )
        graph = self.build_graph(llm_judge_enabled=llm_judge_enabled)

        initial_state = {
            "alert": alert,
            "proposal": proposal,
        }
        return graph.invoke(initial_state)

    def evaluate(self, alert: Alert, proposal: LLMProposal) -> EvaluationResult:
        """单次评估一个 proposal。"""
        result = self._run_check_graph(alert, proposal)
        return result.get("eval_result", EvaluationResult(
            is_passed=False, reason="evaluation returned no result", risk_level="Unknown",
        ))

    def evaluate_with_retry(
        self,
        alert: Alert,
        diagnosis_fn: Callable,
        max_retries: int = 1,
        min_retry_confidence: float = 0.5,
        **diagnosis_kwargs,
    ) -> Dict[str, Any]:
        """评估 + 迭代重试循环。

        diagnosis_fn: 调用诊断 Agent 获取新 proposal 的函数。
                      签名: diagnosis_fn(retry_feedback=None, prev_plan=None) -> dict
                      返回值须包含 "proposal" 键（LLMProposal 对象）。

        Returns:
            dict with keys: proposal, eval_result
        """
        _log.logger.info(
            "[评估] 开始评估循环 | alert_id={} | max_retries={} | min_retry_confidence={:.2f}",
            alert.alert_id, max_retries, min_retry_confidence,
        )

        retry_feedback = None
        prev_plan = None
        proposal = None
        eval_result = None

        for attempt in range(max_retries + 1):
            _log.logger.info(
                "[评估] 第 {}/{} 轮诊断 | alert_id={}",
                attempt + 1, max_retries + 1, alert.alert_id,
            )

            diag_kwargs = dict(diagnosis_kwargs)
            if attempt > 0 and retry_feedback:
                diag_kwargs["retry_feedback"] = retry_feedback
                diag_kwargs["prev_plan"] = prev_plan

            diag_result = diagnosis_fn(**diag_kwargs)
            proposal = diag_result.get("proposal")

            if not proposal:
                _log.logger.warning(
                    "[评估] 诊断未返回方案 | alert_id={} | attempt={}",
                    alert.alert_id, attempt + 1,
                )
                eval_result = EvaluationResult(
                    is_passed=False,
                    reason="diagnosis returned no proposal",
                    risk_level="Unknown",
                )
                break

            _log.logger.info(
                "[评估] 诊断完成 | alert_id={} | confidence={:.2f} | has_script={} | hypotheses={}",
                alert.alert_id,
                proposal.plan.confidence_score,
                bool(proposal.plan.script_content),
                len(proposal.plan.possible_hypotheses),
            )

            check_result = self._run_check_graph(alert, proposal)
            eval_result = check_result.get("eval_result")

            if eval_result and eval_result.is_passed:
                _log.logger.info(
                    "[评估] 评估通过 | alert_id={} | attempt={} | confidence={:.2f}",
                    alert.alert_id, attempt + 1, proposal.plan.confidence_score,
                )
                break

            if (
                eval_result
                and attempt < max_retries
                and proposal.plan.confidence_score >= min_retry_confidence
            ):
                retry_feedback = eval_result.reason
                prev_plan = proposal.plan
                metrics.incr("pipeline.llm.iterative_retry")
                _log.logger.info(
                    "[评估] 触发重试 | alert_id={} | attempt={} | confidence={:.2f} | reason={}",
                    alert.alert_id, attempt + 1,
                    proposal.plan.confidence_score, eval_result.reason,
                )
                continue

            _log.logger.warning(
                "[评估] 评估终止 | alert_id={} | attempt={} | passed={} | reason={}",
                alert.alert_id, attempt + 1,
                eval_result.is_passed if eval_result else False,
                eval_result.reason if eval_result else "no result",
            )
            break

        if eval_result is None:
            eval_result = EvaluationResult(
                is_passed=False,
                reason="evaluation completed without result",
                risk_level="Unknown",
            )

        _log.logger.info(
            "[评估] 最终结果 | alert_id={} | passed={} | confidence={:.2f} | risk={}",
            alert.alert_id,
            eval_result.is_passed,
            proposal.plan.confidence_score if proposal else 0.0,
            eval_result.risk_level,
        )

        return {"proposal": proposal, "eval_result": eval_result}
