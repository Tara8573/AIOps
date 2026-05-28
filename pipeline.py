from core.models import Alert
from core.llm_engine import CognitiveEngine
from core.evaluator import SolutionEvaluator
from core.intent import AlertIntentClassifier
from core.logger import get_logger
from core.observability import metrics, pipeline_states
from core.routing import AlertRouteDecision, AlertRouter, sort_alerts_by_priority
from interfaces.base import (
    IKnowledgeBase,
    IExecutor,
    IApprovalGate,
    ITicketing,
    ILLMClient,
    IAlertSource,
    IAlertCleaner,
    IAlertFilter,
)
from core.skills import SkillRegistry
from typing import Any, Optional, List
import os
import time

_pipeline_logger = get_logger("pipeline")


class AIOpsPipeline:
    """AIOps 主干调度流水线"""

    def __init__(
        self,
        kb: IKnowledgeBase,
        llm: ILLMClient,
        executor: IExecutor,
        approval_gate: IApprovalGate,
        ticketing: ITicketing,
        skill_registry: Optional[SkillRegistry] = None,
        alert_source: Optional[IAlertSource] = None,
        alert_cleaner: Optional[IAlertCleaner] = None,
        alert_filter: Optional[IAlertFilter] = None,
        jira_knowledge_retriever: Optional[Any] = None,
    ):
        self.kb = kb
        self.engine = CognitiveEngine(llm)
        self.evaluator = SolutionEvaluator()
        self.executor = executor
        self.approval_gate = approval_gate
        self.ticketing = ticketing
        self.skill_registry = skill_registry or SkillRegistry()
        self.alert_source = alert_source
        self.alert_cleaner = alert_cleaner
        self.alert_filter = alert_filter
        self.jira_knowledge_retriever = jira_knowledge_retriever
        self.router = AlertRouter()
        self.intent_classifier = AlertIntentClassifier()

    def process_alert(
        self, alert: Alert, route_decision: Optional[AlertRouteDecision] = None
    ) -> str:
        """处理进入的告警，返回可能产生的工单ID"""
        started_at = time.perf_counter()
        print(f"\n[Pipeline] === 开始处理新告警: {alert.alert_id} - {alert.title} ===")
        _pipeline_logger.alert_received(alert.alert_id, alert.title, alert.level)
        pipeline_states.update(alert.alert_id, "received", level=alert.level)
        metrics.incr("pipeline.alert.received")
        route_decision = route_decision or self.router.decide(alert)
        intent_decision = self.intent_classifier.classify(alert)
        pipeline_states.update(
            alert.alert_id,
            "routed",
            route=route_decision.route,
            priority=route_decision.priority,
            intent=intent_decision.category,
        )
        metrics.incr(f"pipeline.route.{route_decision.route}")
        metrics.incr(f"pipeline.intent.{intent_decision.category}")
        _pipeline_logger.audit_event(
            "alert_routed",
            {
                "alert_id": alert.alert_id,
                "route": route_decision.route,
                "priority": route_decision.priority,
                "reason": route_decision.reason,
                "intent": intent_decision.category,
                "intent_reason": intent_decision.reason,
            },
        )
        print(
            "[Pipeline] 意图识别: "
            f"category={intent_decision.category} confidence={intent_decision.confidence} "
            f"reason={intent_decision.reason}"
        )

        if route_decision.route == "manual_first":
            ticket_id = self.ticketing.create_ticket(
                alert,
                self._build_manual_route_plan(route_decision.reason),
                eval_reason=route_decision.reason,
            )
            _pipeline_logger.ticket_created(
                alert.alert_id, ticket_id, route_decision.reason
            )
            pipeline_states.update(
                alert.alert_id,
                "ticket_created",
                route=route_decision.route,
                reason=route_decision.reason,
            )
            total_duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            metrics.observe("pipeline.alert.total.ms", total_duration_ms)
            _pipeline_logger.performance_metric(
                operation="process_alert",
                duration_ms=total_duration_ms,
                success=True,
            )
            print("[Pipeline] === 单次告警处理流转结束 ===\n")
            return ticket_id

        # 1. 查找历史经验
        pipeline_states.update(alert.alert_id, "knowledge_search")
        with metrics.timer(
            "pipeline.knowledge_search.ms",
            success_counter="pipeline.knowledge_search.success",
            failure_counter="pipeline.knowledge_search.failure",
        ):
            search_query = (
                f"category={intent_decision.category} "
                f"{alert.title} {alert.content}"
            )
            sops = self.kb.search_experience(search_query)
            jira_cases = self._search_jira_cases(alert)
            if jira_cases:
                sops = list(sops) + jira_cases

        # 1.1 调用可选 skills
        pipeline_states.update(alert.alert_id, "skills_running")
        skill_results = self.skill_registry.run(
            alert,
            {
                "history_sops": sops,
                "alert": alert.model_dump(),
                "intent": {
                    "category": intent_decision.category,
                    "confidence": intent_decision.confidence,
                    "reason": intent_decision.reason,
                    "tags": intent_decision.tags,
                },
            },
        )
        if skill_results:
            print(
                "[Pipeline] 命中 Skills: "
                + ", ".join(result.skill_name for result in skill_results)
            )

        # 2. 调用认知引擎分析
        print("[Pipeline] 提交 LLM 引擎分析...")
        _pipeline_logger.llm_analysis_start(alert.alert_id)
        pipeline_states.update(alert.alert_id, "llm_analyzing")
        with metrics.timer(
            "pipeline.llm_analysis.ms",
            success_counter="pipeline.llm_analysis.success",
            failure_counter="pipeline.llm_analysis.failure",
        ):
            proposal = self.engine.analyze_alert(alert, sops, skill_results)
        print(f"[Pipeline] LLM分析结案: 置信度 {proposal.plan.confidence_score}")
        print(f"[Pipeline] 根因推测: {proposal.plan.root_cause_analysis}")
        _pipeline_logger.llm_analysis_complete(
            alert.alert_id, proposal.plan.confidence_score
        )

        # 3. 评估方案合法性有效性
        print("[Pipeline] 提交内部安全评估器...")
        pipeline_states.update(alert.alert_id, "evaluating")
        with metrics.timer("pipeline.evaluation.ms"):
            eval_result = self.evaluator.evaluate(alert, proposal)
        _pipeline_logger.evaluation_result(
            alert.alert_id,
            eval_result.is_passed,
            eval_result.reason,
            eval_result.risk_level,
        )

        # 4. 路由与处置
        ticket_id = None
        if eval_result.is_passed:
            print(f"[Pipeline] [PASS] 评估通过允许执行 ({eval_result.reason})")
            if proposal.plan.script_content:
                print("[Pipeline] [APPROVAL] 脚本执行前进入人工确认环节...")
                pipeline_states.update(alert.alert_id, "awaiting_approval")
                approved = self.approval_gate.confirm_execution(
                    alert, proposal.plan, eval_result
                )
                if approved:
                    print("[Pipeline] [EXEC] 人工确认通过，触发物理集群自动修复...")
                    _pipeline_logger.execution_start(
                        alert.alert_id, proposal.plan.script_content
                    )
                    _pipeline_logger.audit_event(
                        "script_execution_approved",
                        {
                            "alert_id": alert.alert_id,
                            "risk_level": eval_result.risk_level,
                        },
                    )
                    pipeline_states.update(alert.alert_id, "executing")
                    execution_success = False
                    execution_failure_reason = ""
                    try:
                        with metrics.timer("pipeline.execution.ms"):
                            execution_success = bool(
                                self.executor.execute_script(
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
                        _pipeline_logger.execution_complete(alert.alert_id, True)
                        pipeline_states.update(
                            alert.alert_id, "resolved", route="auto_execute"
                        )
                    else:
                        metrics.incr("pipeline.execution.failure")
                        _pipeline_logger.execution_complete(alert.alert_id, False)
                        _pipeline_logger.audit_event(
                            "script_execution_failed",
                            {
                                "alert_id": alert.alert_id,
                                "reason": execution_failure_reason,
                                "risk_level": eval_result.risk_level,
                            },
                        )
                        print(
                            "[Pipeline] [FAIL] 自动修复执行失败 -> 提单至 Jira 人工辅助 "
                            f"({execution_failure_reason})"
                        )
                        pipeline_states.update(
                            alert.alert_id,
                            "execution_failed",
                            reason=execution_failure_reason,
                        )
                        ticket_id = self.ticketing.create_ticket(
                            alert,
                            proposal.plan,
                            eval_reason=f"自动执行失败: {execution_failure_reason}",
                        )
                        _pipeline_logger.ticket_created(
                            alert.alert_id, ticket_id, execution_failure_reason
                        )
                        pipeline_states.update(
                            alert.alert_id,
                            "ticket_created",
                            route="auto_execute_failed",
                            reason=execution_failure_reason,
                        )
                else:
                    print("[Pipeline] [BLOCK] 人工确认拒绝执行 -> 提单至 Jira 人工辅助")
                    _pipeline_logger.audit_event(
                        "script_execution_rejected",
                        {"alert_id": alert.alert_id, "reason": "approval_gate_rejected"},
                    )
                    ticket_id = self.ticketing.create_ticket(
                        alert, proposal.plan, eval_reason="人工确认未通过"
                    )
                    _pipeline_logger.ticket_created(
                        alert.alert_id, ticket_id, "人工确认未通过"
                    )
                    pipeline_states.update(
                        alert.alert_id, "ticket_created", route="manual_rejected"
                    )
            else:
                print(
                    "[Pipeline] [WARN] 评估通过，但大模型未提供明确修复脚本，流程止步。"
                )
                pipeline_states.update(alert.alert_id, "passed_without_script")
        else:
            print(
                f"[Pipeline] [BLOCK] 评估拦截危险/不确定 ({eval_result.reason}) -> 提单至 Jira 人工辅助"
            )
            _pipeline_logger.audit_event(
                "proposal_blocked",
                {
                    "alert_id": alert.alert_id,
                    "reason": eval_result.reason,
                    "risk_level": eval_result.risk_level,
                },
            )
            ticket_id = self.ticketing.create_ticket(
                alert, proposal.plan, eval_reason=eval_result.reason
            )
            _pipeline_logger.ticket_created(
                alert.alert_id, ticket_id, eval_result.reason
            )
            pipeline_states.update(alert.alert_id, "ticket_created", route="evaluator_block")

        total_duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        metrics.observe("pipeline.alert.total.ms", total_duration_ms)
        _pipeline_logger.performance_metric(
            operation="process_alert",
            duration_ms=total_duration_ms,
            success=True,
        )
        print("[Pipeline] === 单次告警处理流转结束 ===\n")
        return ticket_id or ""

    def run_from_source(self):
        """从告警数据源（如 Kafka）持续消费，经清洗/过滤后处理告警，阻塞运行"""
        if self.alert_source is None:
            raise RuntimeError("未配置 alert_source，无法启动持续消费模式")

        print("[Pipeline] === 启动持续消费模式 ===")
        ticket_ids: List[str] = []
        batch_size = max(1, int(getattr(self, "_batch_size", 0) or 0))
        if batch_size <= 1:
            batch_size = max(1, int(os.getenv("AIOPS_BATCH_SIZE", "1")))
        batch_buffer: List[Alert] = []

        try:
            for raw_msg in self.alert_source.consume():
                alert = self._clean_and_filter(raw_msg)
                if alert is None:
                    continue
                batch_buffer.append(alert)
                if len(batch_buffer) >= batch_size:
                    ticket_ids.extend(self._process_alert_batch(batch_buffer))
                    batch_buffer = []
        except KeyboardInterrupt:
            print("\n[Pipeline] 收到中断信号，正在停止...")
        finally:
            if batch_buffer:
                ticket_ids.extend(self._process_alert_batch(batch_buffer))
            self.alert_source.close()
            if ticket_ids:
                self.sync_feedbacks(ticket_ids)
            metrics.gauge("pipeline.feedback.pending_tickets", len(ticket_ids))
            print("[Pipeline] === 持续消费模式已停止 ===\n")

    def _clean_and_filter(self, raw_msg: dict) -> Optional[Alert]:
        """对原始消息执行清洗和过滤，返回 None 表示丢弃"""
        if self.alert_cleaner:
            alert = self.alert_cleaner.clean(raw_msg)
            if alert is None:
                print("[Pipeline] [CLEAN] 告警清洗后丢弃")
                return None
        else:
            try:
                alert = Alert(**raw_msg)
            except Exception as exc:
                print(
                    f"[Pipeline] [CLEAN] 无 cleaner 且原始消息无法直接转为 Alert: {exc}"
                )
                return None

        if self.alert_filter and not self.alert_filter.should_process(alert):
            print(f"[Pipeline] [FILTER] 告警被过滤 | alert_id={alert.alert_id}")
            return None

        return alert

    def sync_feedbacks(self, ticket_ids: List[str]):
        """异步拉取已完结状态，打通学习闭环"""
        print("\n[FeedbackSync] === 启动完结工单同步与知识回流 ===")
        for tid in ticket_ids:
            feedback = self.ticketing.get_resolution(tid)
            if feedback and feedback.is_successful:
                print(f"[FeedbackSync] 成功猎取票据 {tid} 的最终人工处理手段")
                self.kb.learn_new_experience(feedback)
                metrics.incr("pipeline.feedback.learned")
                _pipeline_logger.feedback_received(
                    feedback.alert_id, tid, feedback.is_successful
                )
                _pipeline_logger.knowledge_learned(
                    feedback.alert_id, feedback.resolution_steps
                )
        print("[FeedbackSync] === 同步完毕 ===\n")

    def _process_alert_batch(self, alerts: List[Alert]) -> List[str]:
        metrics.incr("pipeline.batch.received")
        metrics.gauge("pipeline.batch.size", len(alerts))
        routed = [(alert, self.router.decide(alert)) for alert in alerts]
        ordered = sort_alerts_by_priority(routed)
        ticket_ids: List[str] = []
        print(f"[Pipeline] [BATCH] 批量处理 {len(alerts)} 条告警，已按优先级排序")
        for alert, route_decision in ordered:
            print(
                "[Pipeline] [BATCH] "
                f"alert_id={alert.alert_id} route={route_decision.route} priority={route_decision.priority}"
            )
            tid = self.process_alert(alert, route_decision=route_decision)
            if tid:
                ticket_ids.append(tid)
        return ticket_ids

    def _search_jira_cases(self, alert: Alert) -> List[str]:
        if self.jira_knowledge_retriever is None:
            return []
        try:
            cases = self.jira_knowledge_retriever.search_for_alert(alert)
            formatted = self.jira_knowledge_retriever.format_cases(cases)
            if formatted:
                print(f"[Pipeline] 命中 Jira 历史案例 {len(cases)} 条")
            return formatted
        except Exception as exc:
            print(f"[Pipeline] [WARN] Jira 历史案例检索失败，已降级: {exc}")
            return []

    @staticmethod
    def _build_manual_route_plan(reason: str):
        from core.models import ActionPlan

        return ActionPlan(
            root_cause_analysis=f"路由器判定该告警应优先人工介入。原因: {reason}",
            troubleshooting_steps=["收集现场信息", "人工确认风险后处理"],
            script_content=None,
            confidence_score=1.0,
        )
