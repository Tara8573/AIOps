"""LangGraph 完整 Pipeline 主图。

将 pipeline.py 中的 process_alert() 改造为 LangGraph StateGraph，
每个步骤是独立节点，条件边驱动分支与循环。

节点流:
  receive → route_intent → manual_check → rule_engine → knowledge → skills
       → evaluate → eval_route → approve → execute → verify → finish
                        ↓           ↓ rejected  ↓ failed  ↓ 验证失败
                     create_ticket ←──────────────────────────┘

evaluate 节点委托 DiagnosisAgent + EvaluationAgent 完成诊断与评估循环。
verify 节点在执行成功后验证告警是否真正消除。

FR-4 改造（生产级分布式改造方案 §1.1）:
  - PipelineState 纯数据化：pipeline 实例与审计 emitter 不再进 state，
    依赖在图构建时以闭包绑定 → state 可完整序列化
  - 接入 LangGraph checkpointer（PostgresSaver），thread_id = pipeline_run_id
  - run_registry（pipeline_runs 表）登记每次 run；崩溃重启后按 alert_id
    找到未终结 run 从断点续跑，不重复执行已完成的节点（如 execute）
"""

import functools
import json
import os
import threading
import time
import uuid as _uuid
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, NotRequired, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from core.models import (
    ActionPlan,
    Alert,
    EvaluationResult,
    LLMProposal,
    SkillExecutionResult,
)
from core.observability import metrics, pipeline_states
from core.routing import AlertRouteDecision
from core.logger import get_logger

# Audit integration
from core.audit import AuditEmitter, get_audit_store, is_audit_enabled

_log = get_logger("pipeline")


class PipelineCancelled(Exception):
    """超时取消：在途节点结束后下一节点边界抛出，阻止后续步骤/副作用继续执行。"""


# ---------------------------------------------------------------------------
# State schema（FR-4：纯数据，可完整序列化）
# ---------------------------------------------------------------------------

class PipelineState(TypedDict):
    # 必填
    alert: Alert  # Pydantic 模型，可序列化

    # 可选 — 各节点逐步填充
    started_at: NotRequired[float]
    correlated_alerts: NotRequired[List[Dict[str, Any]]]
    route_decision: NotRequired[AlertRouteDecision]
    intent_category: NotRequired[str]
    intent_confidence: NotRequired[float]
    intent_reason: NotRequired[str]
    intent_tags: NotRequired[List[str]]
    # Jev 辅助意图分类结果（Phase 1 启用）
    intent_jev_choice: NotRequired[str]
    intent_jev_confidence: NotRequired[float]
    sops: NotRequired[List[str]]
    skill_results: NotRequired[List[SkillExecutionResult]]
    proposal: NotRequired[LLMProposal]
    eval_result: NotRequired[EvaluationResult]
    matched_rule: NotRequired[Optional[Dict[str, Any]]]
    ticket_id: NotRequired[str]
    ticket_reason: NotRequired[str]
    ticket_route: NotRequired[str]
    approved: NotRequired[bool]
    execution_success: NotRequired[bool]
    execution_failure_reason: NotRequired[str]
    verified: NotRequired[bool]
    verify_reason: NotRequired[str]
    pipeline_run_id: NotRequired[str]


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def build_pipeline_graph(
    pipeline: Any,
    audit_emitter: Optional[AuditEmitter] = None,
    checkpointer: Optional[Any] = None,
    cancel_event: Optional[threading.Event] = None,
):
    """构建完整 Pipeline 主图。

    FR-4：依赖（pipeline 实例 / 审计 emitter / checkpointer）经参数注入，
    以闭包绑定到节点 —— PipelineState 只存纯数据，可被 PostgresSaver 持久化。
    注意：依赖是实例级的，不可缓存图单例。
    cancel_event：超时取消标志（可选）；置位后每个节点入口检查并在下一节点边界
    抛 PipelineCancelled，阻止后续步骤/副作用继续（在途节点本身无法中断）。
    """
    # -- 审计 emitter（None 时节点内跳过审计；audit.store=pg 时由调用方传共享 store）--
    emitter = audit_emitter

    def _cancelable(node):
        """节点级协作取消包装：cancel_event 置位后下一节点边界终止。"""
        @functools.wraps(node)
        def wrapper(state):
            if cancel_event is not None and cancel_event.is_set():
                raise PipelineCancelled("pipeline 已取消（超时/取消标志置位）")
            return node(state)
        return wrapper

    # ------------------------------------------------------------------
    # Nodes（闭包绑定依赖，函数签名保持 (state) -> dict）
    # ------------------------------------------------------------------

    def receive(state: PipelineState) -> dict:
        """接收告警：关联分析 + 缓存管理。"""
        p = pipeline
        alert = state["alert"]

        metrics.incr("pipeline.alert.received")
        pipeline_states.update(alert.alert_id, "received", level=alert.level)

        if emitter:
            emitter.emit(
                "alert.received",
                alert_id=alert.alert_id,
                payload={"source": alert.source, "level": alert.level, "title": alert.title},
            )

        p.record_correlation(alert)
        correlated = alert.correlated_alerts
        if not correlated:
            correlated = p._find_correlated_alerts(alert)
            if correlated:
                alert.correlated_alerts = correlated

        return {"correlated_alerts": correlated, "started_at": time.perf_counter()}

    def route_intent(state: PipelineState) -> dict:
        """路由决策 + 意图分类。

        双层分类策略：
        1. 确定性规则（关键词）→ 作为 baseline，始终执行
        2. Jev 语义分类（如果可用）→ 作为增强信号
        管线后续节点可根据 intent_jev_confidence 决定是否采信 Jev 结果。
        """
        p = pipeline
        alert = state["alert"]

        # 第一层：确定性规则（保留，零成本 baseline）
        route_decision = p.router.decide(alert)
        intent_decision = p.intent_classifier.classify(alert)

        metrics.incr(f"pipeline.route.{route_decision.route}")
        metrics.incr(f"pipeline.intent.{intent_decision.category}")
        pipeline_states.update(
            alert.alert_id, "routed",
            route=route_decision.route, priority=route_decision.priority,
            intent=intent_decision.category,
        )

        _log.logger.info(
            "[路由] 告警路由完成 | alert_id={} | route={} | priority={} | reason={}",
            alert.alert_id, route_decision.route, route_decision.priority, route_decision.reason,
        )
        _log.logger.info(
            "[路由] 意图分类完成 | alert_id={} | category={} | confidence={:.2f} | reason={} | tags={}",
            alert.alert_id, intent_decision.category, intent_decision.confidence,
            intent_decision.reason, intent_decision.tags,
        )

        # 第二层：Jev 语义分类（Decision Engine 抽象层，Phase 1 启用）
        jev_choice = ""
        jev_confidence = 0.0
        dc = p.decision_client
        if dc and dc.health_check():
            try:
                from core.models import DecisionResult
                jev_prompt = f"告警标题: {alert.title}\n告警级别: {alert.level}\n告警详情: {alert.content}"
                jev_options = [
                    "disk", "memory", "cpu", "network", "service",
                    "database", "config", "security", "general",
                ]
                jev_result: DecisionResult = dc.classify(jev_prompt, jev_options)
                jev_choice = jev_result.choice
                jev_confidence = jev_result.confidence
                _log.logger.info(
                    "[路由] Jev 语义分类 | alert_id={} | jev_category={} | jev_confidence={:.2f}",
                    alert.alert_id, jev_choice, jev_confidence,
                )
            except Exception as exc:
                _log.logger.warning(
                    "[路由] Jev 语义分类失败，降级到规则结果 | alert_id={} | error={}",
                    alert.alert_id, exc,
                )

        return {
            "route_decision": route_decision,
            "intent_category": intent_decision.category,
            "intent_confidence": intent_decision.confidence,
            "intent_reason": intent_decision.reason,
            "intent_tags": intent_decision.tags,
            "intent_jev_choice": jev_choice,
            "intent_jev_confidence": jev_confidence,
        }

    def manual_check(state: PipelineState) -> dict:
        """manual_first 路由检查：命中则准备创建工单。"""
        route_decision = state["route_decision"]
        alert_id = state["alert"].alert_id

        if route_decision.route != "manual_first":
            _log.logger.info("[路由] 非 manual_first，继续自动处理 | alert_id={}", alert_id)
            return {}

        _log.logger.warning(
            "[路由] 命中 manual_first，转人工处理 | alert_id={} | reason={}",
            alert_id, route_decision.reason,
        )
        # FR-4：不再节点内 import AIOpsPipeline（消除反向依赖），经闭包 pipeline 调用
        plan = pipeline._build_manual_route_plan(route_decision.reason)
        return {
            "ticket_reason": route_decision.reason,
            "ticket_route": "manual_first",
            "proposal": LLMProposal(alert_id=alert_id, plan=plan),
        }

    def rule_engine_node(state: PipelineState) -> dict:
        """规则引擎快路径：匹配已知规则并尝试执行。"""
        p = pipeline
        alert = state["alert"]

        if not p.rule_engine:
            _log.logger.info("[规则引擎] 未配置规则引擎，跳过 | alert_id={}", alert.alert_id)
            return {"matched_rule": None}

        matched_rule = p.rule_engine.match(alert)
        if not matched_rule:
            _log.logger.info(
                "[规则引擎] 未匹配任何规则，进入 LLM 诊断路径 | alert_id={}", alert.alert_id,
            )
            return {"matched_rule": None}

        _log.logger.info(
            "[规则引擎] 匹配规则 | alert_id={} | rule_id={} | name={} | risk={}",
            alert.alert_id, matched_rule["rule_id"], matched_rule["name"], matched_rule.get("risk_level", "Unknown"),
        )

        pipeline_states.update(alert.alert_id, "rule_engine_matched",
                               rule_id=matched_rule["rule_id"])
        try:
            success = p.executor.execute_script(
                matched_rule["action_script"],
                {"alert": alert.model_dump(), "rule": matched_rule},
            )
        except Exception as e:
            success = False
            metrics.incr("pipeline.rule_engine.error")
            _log.logger.error(
                "[规则引擎] 执行异常 | alert_id={} | error={}", alert.alert_id, e,
            )

        if success:
            p.rule_engine.execute(alert, matched_rule)
            metrics.incr("pipeline.rule_engine.success")
            pipeline_states.update(alert.alert_id, "rule_engine_executed")
            _log.logger.info(
                "[规则引擎] 执行成功 ✓ | alert_id={} | rule_id={} | 跳过 LLM 诊断+评估",
                alert.alert_id, matched_rule["rule_id"],
            )
            return {"matched_rule": matched_rule, "execution_success": True}

        metrics.incr("pipeline.rule_engine.failed")
        _log.logger.warning(
            "[规则引擎] 执行失败 ✗ | alert_id={} | rule_id={} | 降级到 LLM 诊断路径",
            alert.alert_id, matched_rule["rule_id"],
        )
        return {"matched_rule": None}

    def knowledge(state: PipelineState) -> dict:
        """知识检索：委托 DiagnosisAgent。"""
        p = pipeline
        alert = state["alert"]
        sops = p.diagnosis_agent.retrieve_knowledge(
            alert, state.get("intent_category", "general"),
        )
        _log.logger.info(
            "[诊断] 知识检索完成 | alert_id={} | SOP数={}", alert.alert_id, len(sops),
        )
        return {"sops": sops}

    def skills(state: PipelineState) -> dict:
        """Skill 调度执行：委托 DiagnosisAgent。"""
        p = pipeline
        alert = state["alert"]

        pipeline_states.update(alert.alert_id, "skills_running")
        skill_results = p.diagnosis_agent.run_skills(alert, {
            "history_sops": state.get("sops") or [],
            "alert": alert.model_dump(),
            "intent": {
                "category": state.get("intent_category", "general"),
                "confidence": state.get("intent_confidence", 0.0),
                "reason": state.get("intent_reason", ""),
                "tags": state.get("intent_tags") or [],
            },
            "log_querier": p.diagnosis_agent.log_querier,
            "metrics_querier": p.diagnosis_agent.metrics_querier,
        })
        _log.logger.info(
            "[诊断] Skill 执行完成 | alert_id={} | 匹配数={} | skills=[{}]",
            alert.alert_id, len(skill_results),
            ", ".join(s.skill_name for s in skill_results),
        )
        return {"skill_results": skill_results}

    def evaluate(state: PipelineState) -> dict:
        """LLM 推理 + 评估循环：委托 DiagnosisAgent + EvaluationAgent。"""
        p = pipeline
        alert = state["alert"]

        pipeline_states.update(alert.alert_id, "llm_analyzing")

        # FR-1.3: L2 档关闭迭代修正（k=1，规格 FR-1.3 L2）
        max_retries = p._iterative_max_retries
        _degradation = getattr(p, "_degradation", None)
        if _degradation is not None and _degradation.is_l2():
            max_retries = 1

        def _diagnose(retry_feedback=None, prev_plan=None):
            proposal = p.diagnosis_agent.engine.analyze_alert(
                alert,
                state.get("sops") or [],
                state.get("skill_results"),
                intent_category=state.get("intent_category", "general"),
                intent_confidence=state.get("intent_confidence", 0.0),
                intent_reason=state.get("intent_reason", ""),
                retry_feedback=retry_feedback,
                prev_plan=prev_plan,
            )
            return {"proposal": proposal}

        with metrics.timer("pipeline.eval_loop.ms"):
            result = p.evaluation_agent.evaluate_with_retry(
                alert=alert,
                diagnosis_fn=_diagnose,
                max_retries=max_retries,
                min_retry_confidence=p._iterative_min_confidence,
            )

        proposal = result.get("proposal")
        eval_result = result.get("eval_result")

        if proposal:
            pipeline_states.update(alert.alert_id, "evaluated")

        if emitter and eval_result:
            emitter.emit(
                "eval.passed" if eval_result.is_passed else "eval.blocked",
                alert_id=alert.alert_id,
                severity="info" if eval_result.is_passed else "warning",
                outcome="success" if eval_result.is_passed else "blocked",
                payload={
                    "reason": eval_result.reason,
                    "risk_level": eval_result.risk_level,
                    "confidence": proposal.plan.confidence_score if proposal else 0.0,
                },
            )

        return {"proposal": proposal, "eval_result": eval_result}

    def block(state: PipelineState) -> dict:
        """评估拦截：设置工单原因和路由。"""
        eval_result = state.get("eval_result")
        reason = eval_result.reason if eval_result else "评估未通过"
        metrics.incr("pipeline.diagnosis.blocked")
        _log.logger.warning(
            "[流程] 评估拦截，转人工工单 | alert_id={} | reason={}",
            state["alert"].alert_id, reason,
        )
        return {"ticket_reason": reason, "ticket_route": "evaluator_block"}

    def approve(state: PipelineState) -> dict:
        """人工审批环节。

        双模式（FR-3）：
        - sync（默认）：同步阻塞调用 approval_gate（demo/未启用异步）
        - async：submit 审批表后 interrupt() 挂起 —— 状态已随本 super-step
          落 checkpoint，worker 立即释放；人工决策后由 ApprovalResumer
          以 Command(resume=decision) 恢复，本节点从头重跑、interrupt 返回决策值。

        幂等约定：interrupt 之前的副作用（submit）必须幂等 —— 已存在同 run
        的 pending 记录则不重复登记。
        """
        p = pipeline
        alert = state["alert"]
        proposal = state["proposal"]
        eval_result = state["eval_result"]

        _log.logger.info(
            "[审批] 等待人工确认 | alert_id={} | confidence={:.2f} | has_script={}",
            alert.alert_id, proposal.plan.confidence_score, bool(proposal.plan.script_content),
        )
        pipeline_states.update(alert.alert_id, "awaiting_approval")

        approval_mode = getattr(p, "approval_mode", "sync")
        approval_store = getattr(p, "approval_store", None)

        # ── FR-3 异步审批：interrupt 挂起，人工决策后恢复 ──
        if approval_mode == "async" and approval_store is not None:
            if checkpointer is None:
                # 无 checkpointer 无法挂起 → 降级同步确认
                _log.logger.warning(
                    "[审批] async 模式但未启用 checkpoint，降级同步确认 | alert_id={}",
                    alert.alert_id,
                )
            else:
                store = approval_store
                # 幂等护栏：interrupt 恢复时本节点从头重跑，已存在则不重复登记
                existing = store.find_by_run(state["pipeline_run_id"])
                if existing:
                    approval_id = existing.approval_id
                else:
                    timeout_seconds = int(os.getenv("AIOPS_APPROVAL_TIMEOUT", "3600"))
                    approval_id = store.submit(
                        pipeline_run_id=state["pipeline_run_id"],
                        alert=alert,
                        plan=proposal.plan,
                        evaluation=eval_result,
                        timeout_seconds=timeout_seconds,
                    )
                    _log.logger.info(
                        "[审批] 已登记待审批 | alert_id={} | approval_id={} | timeout={}s",
                        alert.alert_id, approval_id, timeout_seconds,
                    )
                metrics.incr("pipeline.approval.submitted")

                # ── 挂起：状态已落 checkpoint，worker 立即释放 ──
                # 恢复后从这继续，interrupt 返回 Command(resume=...) 的值：
                #   approved | rejected | timeout（超时 sweeper 置 expired → resume="timeout"）
                decision = interrupt({
                    "approval_id": approval_id,
                    "alert_id": alert.alert_id,
                    "title": alert.title,
                    "risk_level": eval_result.risk_level,
                    "confidence": proposal.plan.confidence_score,
                    "pipeline_run_id": state["pipeline_run_id"],
                })

                if decision == "approved":
                    if emitter:
                        emitter.emit(
                            "approval.granted",
                            alert_id=alert.alert_id,
                            severity="critical",
                            outcome="success",
                            payload={"approval_id": approval_id, "risk_level": eval_result.risk_level},
                        )
                    _log.logger.info("[审批] 人工通过 ✓ | alert_id={}", alert.alert_id)
                    return {"approved": True}

                # rejected / timeout → 转工单
                is_timeout = decision == "timeout"
                route = "approval_timeout" if is_timeout else "manual_rejected"
                reason = "审批超时自动拒绝" if is_timeout else "人工确认未通过"
                metrics.incr("pipeline.approval.timeout" if is_timeout else "pipeline.approval.denied")
                if emitter:
                    emitter.emit(
                        "approval.timeout" if is_timeout else "approval.denied",
                        alert_id=alert.alert_id,
                        severity="critical",
                        outcome="blocked",
                        payload={"approval_id": approval_id, "decision": decision,
                                 "risk_level": eval_result.risk_level},
                    )
                _log.logger.warning(
                    "[审批] {} ✗ | alert_id={} | 转工单",
                    "审批超时" if is_timeout else "人工拒绝", alert.alert_id,
                )
                return {
                    "approved": False,
                    "ticket_reason": reason,
                    "ticket_route": route,
                }

        # ── 同步审批（现状行为，demo/未启用 async）──
        approved = p.approval_gate.confirm_execution(alert, proposal.plan, eval_result)

        if emitter:
            import hashlib
            script_hash = hashlib.sha256(
                (proposal.plan.script_content or "").encode()
            ).hexdigest()[:16]
            emitter.emit(
                "approval.granted" if approved else "approval.denied",
                alert_id=alert.alert_id,
                severity="critical",
                outcome="success" if approved else "blocked",
                payload={"script_hash": script_hash, "risk_level": eval_result.risk_level},
            )

        if not approved:
            _log.logger.warning("[审批] 人工拒绝 ✗ | alert_id={} | 转工单", alert.alert_id)
            return {
                "approved": False,
                "ticket_reason": "人工确认未通过",
                "ticket_route": "manual_rejected",
            }

        _log.logger.info("[审批] 人工通过 ✓ | alert_id={}", alert.alert_id)
        return {"approved": True}

    def execute(state: PipelineState) -> dict:
        """执行修复脚本（支持金丝雀执行）。"""
        p = pipeline
        alert = state["alert"]
        proposal = state["proposal"]

        pipeline_states.update(alert.alert_id, "executing")

        # 检查是否启用金丝雀执行
        canary_enabled = os.getenv("AIOPS_CANARY_ENABLED", "0") == "1"

        if canary_enabled:
            # 使用金丝雀执行
            from core.canary_graph import run_canary_execution

            print(f"[Pipeline] 使用金丝雀执行模式")

            final_state = run_canary_execution(
                alert=alert,
                proposal=proposal,
                script_content=proposal.plan.script_content,
                executor=p.executor,
            )

            execution_success = final_state.get("total_success", False)

            if execution_success:
                metrics.incr("pipeline.execution.success")
                metrics.incr("pipeline.execution.canary.success")
                metrics.incr("pipeline.diagnosis.correct")
                pipeline_states.update(alert.alert_id, "resolved", route="auto_execute_canary")
                return {"execution_success": True}
            else:
                execution_failure_reason = final_state.get("rollback_reason", "金丝雀执行失败")
                metrics.incr("pipeline.execution.failure")
                metrics.incr("pipeline.execution.canary.failure")
                metrics.incr("pipeline.diagnosis.incorrect")
                pipeline_states.update(
                    alert.alert_id, "execution_failed",
                    reason=execution_failure_reason,
                )
                return {
                    "execution_success": False,
                    "execution_failure_reason": execution_failure_reason,
                    "ticket_reason": f"金丝雀执行失败: {execution_failure_reason}",
                    "ticket_route": "canary_failed",
                }

        # 传统直接执行模式
        execution_success = False
        execution_failure_reason = ""

        try:
            with metrics.timer("pipeline.execution.ms"):
                execution_success = bool(
                    p.executor.execute_script(
                        proposal.plan.script_content,
                        {"alert": alert.model_dump()},
                    )
                )
        except Exception as exc:
            execution_failure_reason = f"脚本执行异常: {exc}"
        else:
            if not execution_success:
                execution_failure_reason = "脚本执行返回失败"

        if execution_success:
            metrics.incr("pipeline.execution.success")
            metrics.incr("pipeline.diagnosis.correct")
            pipeline_states.update(alert.alert_id, "resolved", route="auto_execute")

            if emitter:
                emitter.emit(
                    "script.executed",
                    alert_id=alert.alert_id,
                    severity="critical",
                    outcome="success",
                    payload={"route": "auto_execute"},
                )

            return {"execution_success": True}

        metrics.incr("pipeline.execution.failure")
        metrics.incr("pipeline.diagnosis.incorrect")
        pipeline_states.update(alert.alert_id, "execution_failed",
                               reason=execution_failure_reason)

        if emitter:
            emitter.emit(
                "script.failed",
                alert_id=alert.alert_id,
                severity="critical",
                outcome="failure",
                payload={"reason": execution_failure_reason},
            )

        return {
            "execution_success": False,
            "execution_failure_reason": execution_failure_reason,
            "ticket_reason": f"自动执行失败: {execution_failure_reason}",
            "ticket_route": "auto_execute_failed",
        }

    def create_ticket(state: PipelineState) -> dict:
        """创建工单（所有需要人工介入的路径汇聚于此）。"""
        p = pipeline
        alert = state["alert"]
        proposal = state.get("proposal")
        reason = state.get("ticket_reason", "")
        route = state.get("ticket_route", "")

        plan = proposal.plan if proposal else ActionPlan(
            root_cause_analysis="无 LLM 提案",
            troubleshooting_steps=[],
            confidence_score=0.0,
        )
        ticket_id = p.ticketing.create_ticket(alert, plan, eval_reason=reason)
        pipeline_states.update(alert.alert_id, "ticket_created",
                               route=route, reason=reason)
        metrics.incr(f"pipeline.ticket.created.{route}" if route else "pipeline.ticket.created")

        _log.logger.info(
            "[流程] 创建工单 | alert_id={} | ticket_id={} | route={} | reason={}",
            alert.alert_id, ticket_id, route, reason,
        )

        if emitter:
            emitter.emit(
                "ticket.created",
                alert_id=alert.alert_id,
                payload={"ticket_id": ticket_id, "reason": reason, "route": route},
            )

        return {"ticket_id": ticket_id}

    def verify(state: PipelineState) -> dict:
        """执行后验证：确认告警是否真正消除。"""
        alert = state["alert"]

        _log.logger.info("[验证] 开始执行后验证 | alert_id={}", alert.alert_id)

        # TODO: 接入真实监控后替换以下逻辑
        # 1. 调用 metrics_querier 重新查询原始告警指标
        # 2. 调用 log_querier 检查最近 N 分钟是否有同类错误
        # 3. 对比阈值判断是否恢复

        p = pipeline
        verified = True
        verify_reason = ""

        # 尝试通过 metrics_querier 验证（如果有）
        if p.metrics_querier:
            try:
                # TODO: 根据告警类型构造具体的指标查询
                _log.logger.info(
                    "[验证] 查询指标验证中 | alert_id={} | (TODO: 待实现具体查询)",
                    alert.alert_id,
                )
            except Exception as exc:
                _log.logger.warning(
                    "[验证] 指标查询异常 | alert_id={} | error={}", alert.alert_id, exc,
                )

        # 尝试通过 log_querier 验证（如果有）
        if p.log_querier:
            try:
                _log.logger.info(
                    "[验证] 查询日志验证中 | alert_id={} | (TODO: 待实现具体查询)",
                    alert.alert_id,
                )
            except Exception as exc:
                _log.logger.warning(
                    "[验证] 日志查询异常 | alert_id={} | error={}", alert.alert_id, exc,
                )

        if verified:
            metrics.incr("pipeline.verify.passed")
            _log.logger.info(
                "[验证] 验证通过 ✓ | alert_id={} | 告警已消除",
                alert.alert_id,
            )
            return {"verified": True, "verify_reason": ""}
        else:
            metrics.incr("pipeline.verify.failed")
            _log.logger.warning(
                "[验证] 验证失败 ✗ | alert_id={} | reason={} | 降级处理",
                alert.alert_id, verify_reason,
            )
            return {
                "verified": False,
                "verify_reason": verify_reason,
                "ticket_reason": f"执行后验证失败: {verify_reason}",
                "ticket_route": "verify_failed",
            }

    def finish(state: PipelineState) -> dict:
        """结束节点：记录总耗时和最终结果。"""
        alert = state["alert"]
        started_at = state.get("started_at")
        ticket_id = state.get("ticket_id", "")
        execution_success = state.get("execution_success")
        total_ms = 0.0

        if started_at:
            total_ms = round((time.perf_counter() - started_at) * 1000, 2)
            metrics.observe("pipeline.alert.total.ms", total_ms)

        # 判断最终结果
        verified = state.get("verified")
        if execution_success is True and verified is True:
            outcome = "自动执行成功 + 验证通过"
        elif execution_success is True and verified is False:
            outcome = "自动执行成功但验证未通过"
        elif execution_success is False:
            outcome = "自动执行失败"
        elif ticket_id:
            outcome = "转人工工单"
        elif state.get("matched_rule") and state.get("execution_success"):
            outcome = "规则引擎快路径"
        else:
            outcome = "无操作"

        _log.logger.info(
            "[流程] Pipeline 完成 | alert_id={} | 耗时={:.1f}ms | 结果={} | ticket={}",
            alert.alert_id, total_ms, outcome, ticket_id or "无",
        )

        # 发布处理结果到 Kafka
        p = pipeline
        if p.message_producer:
            try:
                proposal = state.get("proposal")
                eval_result = state.get("eval_result")
                result_message = {
                    "alert_id": alert.alert_id,
                    "title": alert.title,
                    "level": alert.level,
                    "outcome": outcome,
                    "ticket_id": ticket_id or None,
                    "execution_success": execution_success,
                    "verified": verified,
                    "confidence": proposal.plan.confidence_score if proposal else None,
                    "eval_passed": eval_result.is_passed if eval_result else None,
                    "risk_level": eval_result.risk_level if eval_result else None,
                    "route": state.get("ticket_route", ""),
                    "duration_ms": total_ms,
                    "timestamp": datetime.now().isoformat(),
                }
                p.message_producer.publish(
                    topic="aiops-results",
                    key=alert.alert_id,
                    value=result_message,
                )
            except Exception as exc:
                _log.logger.warning(
                    "[MQ] 结果发布失败 | alert_id={} | error={}", alert.alert_id, exc,
                )

        return {}

    # ------------------------------------------------------------------
    # Conditional edge routers（纯 state 函数，模块级同款逻辑）
    # ------------------------------------------------------------------

    def _route_after_manual(state: PipelineState) -> str:
        """manual_first 路由后：命中 → create_ticket，否则 → rule_engine。"""
        if state.get("route_decision") and state["route_decision"].route == "manual_first":
            return "ticket"
        return "next"

    def _route_after_rule(state: PipelineState) -> str:
        """规则引擎后：成功 → verify，否则 → knowledge。"""
        if state.get("matched_rule") and state.get("execution_success"):
            return "verify"
        return "next"

    def _route_after_eval(state: PipelineState) -> str:
        """评估结果路由。"""
        eval_result = state.get("eval_result")
        if not eval_result or not eval_result.is_passed:
            return "block"

        proposal = state.get("proposal")
        if proposal and proposal.plan.script_content:
            return "approve"
        return "no_script"

    def _route_after_approve(state: PipelineState) -> str:
        """审批后：通过 → execute，拒绝 → create_ticket。"""
        return "next" if state.get("approved") else "ticket"

    def _route_after_execute(state: PipelineState) -> str:
        """执行后：成功 → verify，失败 → create_ticket。"""
        return "verify" if state.get("execution_success") else "ticket"

    def _route_after_verify(state: PipelineState) -> str:
        """验证后路由：
        - 通过 → finish
        - 规则引擎执行后验证失败 → knowledge（降级到 LLM 诊断）
        - LLM 路径执行后验证失败 → create_ticket（已尝试过 LLM，转人工）
        """
        if state.get("verified"):
            return "done"

        # FR-1.3: L2 档关闭诊断回环（LLM 受限），验证失败直接转工单
        _degradation = getattr(pipeline, "_degradation", None)
        if _degradation is not None and _degradation.is_l2():
            _log.logger.info(
                "[验证] L2 降级档，验证失败直接转工单 | alert_id={}",
                state["alert"].alert_id,
            )
            return "ticket"

        # 规则引擎路径验证失败：降级到 LLM 诊断
        if state.get("matched_rule"):
            _log.logger.info(
                "[验证] 规则引擎验证失败，降级到 LLM 诊断路径 | alert_id={}",
                state["alert"].alert_id,
            )
            return "fallback"

        # LLM 路径验证失败：转人工
        return "ticket"

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------

    builder = StateGraph(PipelineState)

    builder.add_node("receive", _cancelable(receive))
    builder.add_node("route_intent", _cancelable(route_intent))
    builder.add_node("manual_check", _cancelable(manual_check))
    builder.add_node("rule_engine", _cancelable(rule_engine_node))
    builder.add_node("knowledge", _cancelable(knowledge))
    builder.add_node("skills", _cancelable(skills))
    builder.add_node("evaluate", _cancelable(evaluate))
    builder.add_node("block", _cancelable(block))
    builder.add_node("approve", _cancelable(approve))
    builder.add_node("execute", _cancelable(execute))
    builder.add_node("verify", _cancelable(verify))
    builder.add_node("create_ticket", _cancelable(create_ticket))
    builder.add_node("finish", _cancelable(finish))

    builder.add_edge(START, "receive")
    builder.add_edge("receive", "route_intent")
    builder.add_edge("route_intent", "manual_check")

    builder.add_conditional_edges(
        "manual_check", _route_after_manual,
        {"ticket": "create_ticket", "next": "rule_engine"},
    )

    builder.add_conditional_edges(
        "rule_engine", _route_after_rule,
        {"verify": "verify", "next": "knowledge"},
    )

    builder.add_edge("knowledge", "skills")
    builder.add_edge("skills", "evaluate")

    builder.add_conditional_edges(
        "evaluate", _route_after_eval,
        {"approve": "approve", "block": "block", "no_script": "finish"},
    )

    builder.add_edge("block", "create_ticket")

    builder.add_conditional_edges(
        "approve", _route_after_approve,
        {"next": "execute", "ticket": "create_ticket"},
    )

    builder.add_conditional_edges(
        "execute", _route_after_execute,
        {"verify": "verify", "ticket": "create_ticket"},
    )

    builder.add_conditional_edges(
        "verify", _route_after_verify,
        {"done": "finish", "ticket": "create_ticket", "fallback": "knowledge"},
    )

    builder.add_edge("create_ticket", "finish")
    builder.add_edge("finish", END)

    return builder.compile(checkpointer=checkpointer) if checkpointer else builder.compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    """判断异常是否可重试（网络瞬时故障、rate limit、服务不可用）。"""
    if isinstance(exc, PipelineCancelled):
        return False
    import httpx
    if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException)):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc)
        for code in ("503", "429", "502", "504"):
            if code in msg:
                return True
    return False


def _invoke_with_timeout(graph, invoke_input, config, timeout, cancel_event):
    """带超时的图执行：守护线程 + cooperative cancel。

    - timeout > 0：join(timeout) 超时后置 cancel_event（协作取消），返回 FuturesTimeout；
      在途节点结束后下一节点边界抛 PipelineCancelled，子线程自行终止
    - timeout <= 0：不限制，等同直接 invoke
    - 返回 (result, exc)：exc 为节点内异常（跨线程透传）或 FuturesTimeout，成功时 None
    """
    if timeout is None or timeout <= 0:
        try:
            return graph.invoke(invoke_input, config), None
        except Exception as exc:
            return None, exc

    holder: dict = {}

    def _target():
        try:
            holder["result"] = graph.invoke(invoke_input, config)
        except BaseException as exc:  # noqa: BLE001 —— 跨线程透传异常
            holder["exc"] = exc

    thread = threading.Thread(target=_target, name="pipeline-invoke", daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        # 超时：协作取消（在途节点结束后下一节点边界终止），调用方按失败处理
        if cancel_event is not None:
            cancel_event.set()
        return None, FuturesTimeout("pipeline 超时，已发起协作取消")
    if "exc" in holder:
        return None, holder["exc"]
    return holder.get("result"), None


def _write_dead_letter(alert: Alert, error: str, context: str):
    """将失败告警写入死信队列文件，供后续重处理。"""
    dlq_dir = os.getenv("AIOPS_DLQ_DIR", "data/dead_letters")
    try:
        dlq_path = Path(dlq_dir)
        dlq_path.mkdir(parents=True, exist_ok=True)
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{alert.alert_id}.json"
        entry = {
            "alert": alert.model_dump(),
            "error": error,
            "context": context,
            "timestamp": datetime.now().isoformat(),
        }
        (dlq_path / filename).write_text(
            json.dumps(entry, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        metrics.incr("pipeline.dlq.written")
        print(f"[Pipeline] [DLQ] 告警已写入死信: {filename}")
    except Exception as dlq_err:
        metrics.incr("pipeline.dlq.write_failed")
        print(f"[Pipeline] [DLQ] 死信写入失败: {dlq_err}")


def _fallback_ticket(pipeline, alert: Alert, reason: str) -> dict:
    """pipeline 崩溃时尝试创建兜底工单。"""
    try:
        plan = ActionPlan(
            root_cause_analysis=f"Pipeline 执行异常: {reason}",
            troubleshooting_steps=["人工检查 pipeline 日志", "查看死信队列"],
            confidence_score=0.0,
        )
        ticket_id = pipeline.ticketing.create_ticket(
            alert, plan, eval_reason=reason,
        )
        metrics.incr("pipeline.fallback_ticket.created")
        return {
            "alert": alert,
            "pipeline": pipeline,
            "ticket_id": ticket_id,
            "ticket_reason": reason,
            "ticket_route": "pipeline_error",
        }
    except Exception:
        metrics.incr("pipeline.fallback_ticket.failed")
        return {}


def _resume_run_id(pipeline, alert: Alert) -> Optional[str]:
    """FR-4：查该告警是否已存在未终结的 run（崩溃重启场景）。

    命中 → 返回其 run_id（作为 checkpoint thread_id 续跑）；
    未命中 → 返回 None（新 run）。
    """
    registry = getattr(pipeline, "run_registry", None)
    checkpointer = getattr(pipeline, "_checkpointer", None)
    if registry is None or checkpointer is None:
        return None
    try:
        existing = registry.find_open_run(alert.alert_id)
    except Exception as exc:
        metrics.incr("pipeline.run_registry.error")
        _log.logger.warning(
            "[Checkpoint] run 登记查询失败，按新 run 处理 | alert_id={} | error={}",
            alert.alert_id, exc,
        )
        return None
    return existing.run_id if existing else None


def run_pipeline(alert: Alert, pipeline: Any) -> PipelineState:
    """执行完整 Pipeline 主图，返回最终 PipelineState。

    包含四层保护：
    1. 持久化：AIOPS_CHECKPOINT_ENABLED=1 时接入 checkpointer（thread_id=pipeline_run_id），
       崩溃重启后按 pipeline_runs 登记从断点续跑，不重复执行已完成的 execute（FR-4）
    2. 超时：整条 pipeline 超过 AIOPS_PIPELINE_TIMEOUT_SECONDS 则协作取消
       （守护线程 + cancel_event，在途节点结束后下一节点边界终止，无僵尸线程副作用）
    3. 重试：对可重试异常（网络/rate limit）指数退避重试，有 checkpoint 时从断点续跑
    4. 兜底：所有重试耗尽后尝试创建工单，工单也失败则写死信队列
    """
    checkpointer = getattr(pipeline, "_checkpointer", None)
    registry = getattr(pipeline, "run_registry", None)

    # 修复：超时改为守护线程 + 协作取消。原 ThreadPoolExecutor + future.result(timeout)
    # 的问题：with 块退出时 shutdown(wait=True) 仍阻塞等待 invoke 完成，且在途
    # 流水线继续执行副作用（脚本/工单），调用方却已走 fallback —— 超时场景必然
    # 重复建单。现在超时后置 cancel_event，在途节点结束后下一节点边界抛
    # PipelineCancelled 终止，不再有僵尸线程继续执行。
    cancel_event = threading.Event()
    graph = build_pipeline_graph(pipeline, checkpointer=checkpointer, cancel_event=cancel_event)

    # FR-4：崩溃重启续跑 —— 命中未终结 run 则复用其 thread_id
    resumed_run_id = _resume_run_id(pipeline, alert)
    if resumed_run_id:
        run_id = resumed_run_id
        _log.logger.info(
            "[Checkpoint] 发现未终结 run，从断点续跑 | alert_id={} | run_id={}",
            alert.alert_id, run_id,
        )
        metrics.incr("pipeline.checkpoint.resumed")
    else:
        run_id = f"run-{_uuid.uuid4().hex[:12]}"

    if registry is not None:
        try:
            registry.register(run_id, alert.alert_id, status="running")
        except Exception as exc:
            metrics.incr("pipeline.run_registry.error")
            _log.logger.warning(
                "[Checkpoint] run 登记失败 | run_id={} | error={}", run_id, exc,
            )

    emitter = None
    if is_audit_enabled():
        emitter = AuditEmitter(store=get_audit_store())

    initial_state: dict = {
        "alert": alert,
        "pipeline_run_id": run_id,
    }

    config = {"configurable": {"thread_id": run_id}} if checkpointer else None

    timeout = int(os.getenv("AIOPS_PIPELINE_TIMEOUT_SECONDS", "0"))
    max_retries = int(os.getenv("AIOPS_PIPELINE_MAX_RETRIES", "2"))

    for attempt in range(max_retries + 1):
        try:
            if checkpointer:
                # 断点续跑语义：
                #   - 新 run（或无 checkpoint 可续）→ 带 initial_state 启动
                #   - 有 checkpoint（重试/续跑）→ input=None 从最后 checkpoint 继续
                if resumed_run_id and attempt == 0:
                    invoke_input = None
                elif attempt > 0:
                    invoke_input = None
                else:
                    invoke_input = initial_state
            else:
                invoke_input = initial_state

            result, invoke_exc = _invoke_with_timeout(
                graph, invoke_input, config, timeout, cancel_event,
            )
            if invoke_exc is not None:
                raise invoke_exc

            # 成功返回（可能含 __interrupt__，FR-3 人工挂起）
            if registry is not None:
                if isinstance(result, dict) and "__interrupt__" in result:
                    status = "suspended"
                else:
                    status = "completed"
                try:
                    registry.update_status(run_id, status)
                except Exception as exc:
                    metrics.incr("pipeline.run_registry.error")
                    _log.logger.warning(
                        "[Checkpoint] run 状态更新失败 | run_id={} | error={}", run_id, exc,
                    )
            return result

        except FuturesTimeout:
            metrics.incr("pipeline.error.timeout")
            reason = f"pipeline 超时 ({timeout}s)"
            print(f"[Pipeline] [ERROR] {reason} | alert_id={alert.alert_id}")
            # 协作取消已随 _invoke_with_timeout 置位；在途节点结束后即终止，
            # 不再有副作用继续执行，可安全走工单兑底
            break

        except PipelineCancelled:
            # 超时取消沿（防御：主线程不应收到，子线程在下一节点边界自行终止）
            metrics.incr("pipeline.error.cancelled")
            break

        except Exception as exc:
            metrics.incr(f"pipeline.error.{type(exc).__name__}")
            print(
                f"[Pipeline] [ERROR] {type(exc).__name__}: {exc} "
                f"| alert_id={alert.alert_id} | attempt={attempt + 1}"
            )

            if attempt < max_retries and _is_retryable(exc):
                delay = 2 ** attempt
                metrics.incr("pipeline.error.retried")
                print(f"[Pipeline] [RETRY] {delay}s 后重试...")
                time.sleep(delay)
                continue

            break

    # 失败：登记并兜底
    if registry is not None:
        try:
            registry.update_status(run_id, "failed")
        except Exception:
            pass

    reason = f"pipeline 执行失败 (尝试 {max_retries + 1} 次)"
    result = _fallback_ticket(pipeline, alert, reason)
    if result.get("ticket_id"):
        return result

    # 工单也失败：写死信队列
    _write_dead_letter(alert, reason, "pipeline_error")
    return initial_state


def resume_pipeline_run(pipeline: Any, run_id: str) -> Optional[dict]:
    """从 checkpoint 恢复指定 run（FR-4 断点续跑；供僵死巡检脚本 resume_stale_runs）。

    用 input=None 从最后 checkpoint 继续；无 checkpointer 或无 checkpoint 返回 None。
    返回最终 state；结果含 __interrupt__ 表示人工挂起（FR-3）。
    """
    checkpointer = getattr(pipeline, "_checkpointer", None)
    if checkpointer is None:
        return None

    registry = getattr(pipeline, "run_registry", None)
    graph = build_pipeline_graph(pipeline, checkpointer=checkpointer)
    config = {"configurable": {"thread_id": run_id}}

    try:
        result = graph.invoke(None, config)
    except Exception as exc:
        metrics.incr("pipeline.checkpoint.resume_failed")
        _log.logger.warning(
            "[Checkpoint] 断点续跑失败 | run_id={} | error={}", run_id, exc,
        )
        return None

    if registry is not None:
        status = "suspended" if isinstance(result, dict) and "__interrupt__" in result else "completed"
        try:
            registry.update_status(run_id, status)
        except Exception:
            pass

    metrics.incr("pipeline.checkpoint.resumed")
    return result


def resume_pipeline_with_decision(pipeline: Any, run_id: str, decision: str) -> Optional[dict]:
    """FR-3：用人工决策恢复挂起的 pipeline（approve 节点 interrupt 处继续）。

    - decision ∈ approved | rejected | timeout（超时 sweeper 置 expired 后由
      ApprovalResumer 映射为 timeout）
    - approve 节点从头重跑：submit 幂等护栏跳过重复登记，interrupt 返回决策值
    - 返回最终 state；结果含 __interrupt__ 表示再次挂起
    """
    checkpointer = getattr(pipeline, "_checkpointer", None)
    if checkpointer is None:
        return None

    registry = getattr(pipeline, "run_registry", None)
    graph = build_pipeline_graph(pipeline, checkpointer=checkpointer)
    config = {"configurable": {"thread_id": run_id}}

    try:
        result = graph.invoke(Command(resume=decision), config)
    except Exception as exc:
        metrics.incr("pipeline.approval.resume_failed")
        _log.logger.warning(
            "[审批] 恢复执行失败 | run_id={} | decision={} | error={}", run_id, decision, exc,
        )
        return None

    if registry is not None:
        status = (
            "suspended"
            if isinstance(result, dict) and "__interrupt__" in result
            else "completed"
        )
        try:
            registry.update_status(run_id, status)
        except Exception:
            pass

    metrics.incr("pipeline.approval.resumed")
    return result
