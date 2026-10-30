from core.models import Alert
from core.llm_engine import CognitiveEngine
from core.evaluator import SolutionEvaluator
from core.pipeline_graph import run_pipeline
from core.intent import AlertIntentClassifier
from core.rule_engine import LocalRuleEngine
from core.logger import get_logger
from core.observability import metrics, pipeline_states
from core.routing import AlertRouteDecision, AlertRouter, sort_alerts_by_priority
from agents import DiagnosisAgent, EvaluationAgent, FeedbackAgent
import threading
from interfaces.base import (
    IKnowledgeBase,
    IExecutor,
    IApprovalGate,
    ITicketing,
    ILLMClient,
    IAlertSource,
    IAlertCleaner,
    IAlertFilter,
    IRuleEngine,
    IDecisionClient,
)
from core.skills import SkillRegistry
from typing import Any, Optional, List
import os
import re
import time

_pipeline_logger = get_logger("pipeline")


def _extract_host(alert: Alert) -> str:
    """从告警中提取主机标识，用于关联分析"""
    text = f"{alert.title} {alert.content}"
    ip_match = re.search(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b", text)
    if ip_match:
        return ip_match.group(1)
    host_match = re.search(r"(?:host|node|server)\s*[:=]?\s*(\S+)", text, re.IGNORECASE)
    if host_match:
        return host_match.group(1)
    return ""


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
        log_querier: Optional[Any] = None,
        metrics_querier: Optional[Any] = None,
        rule_engine: Any = "default",
        diagnosis_llm: Optional[ILLMClient] = None,
        evaluation_llm: Optional[ILLMClient] = None,
        feedback_llm: Optional[ILLMClient] = None,
        message_producer: Optional[Any] = None,
        decision_client: Optional[IDecisionClient] = None,
        checkpointer: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        approval_store: Optional[Any] = None,
        correlation_store: Optional[Any] = None,
        runtime_config: Optional[Any] = None,
    ):
        self.kb = kb
        self.llm_client = llm
        self.engine = CognitiveEngine(llm)
        self.evaluator = SolutionEvaluator(
            llm_client=llm if os.getenv("AIOPS_LLM_JUDGE_ENABLED", "0") == "1" else None
        )
        self.executor = executor
        self.approval_gate = approval_gate
        self.approval_store = approval_store
        # FR-3：异步审批模式（sync=同步阻塞保持旧行为 | async=interrupt 挂起+Resumer 恢复）
        self.approval_mode = os.getenv("AIOPS_APPROVAL_MODE", "sync").strip().lower()
        self.ticketing = ticketing
        self.skill_registry = skill_registry or SkillRegistry()
        self.alert_source = alert_source
        self.alert_cleaner = alert_cleaner
        self.alert_filter = alert_filter
        self.jira_knowledge_retriever = jira_knowledge_retriever
        self.log_querier = log_querier
        self.metrics_querier = metrics_querier
        self.rule_engine = (
            LocalRuleEngine() if rule_engine == "default" else rule_engine
        )
        self.router = AlertRouter()
        self.intent_classifier = AlertIntentClassifier()

        # ── Decision Engine（判别式决策抽象层）──
        # 默认使用 RuleDecisionClient（确定性规则），零依赖零成本。
        # 可通过注入 JevDecisionClient 切换到概率语义模型。
        # 业务代码只依赖 IDecisionClient 接口，不感知具体实现。
        if decision_client is not None:
            self.decision_client = decision_client
        else:
            from plugins.rule_decision_client import RuleDecisionClient
            self.decision_client = RuleDecisionClient()
            _pipeline_logger.logger.info(
                "Decision Engine: 使用 RuleDecisionClient（确定性规则）"
            )

        # FR-2.1: 关联告警缓存外置（Redis 可用 → 多实例共享；否则本地，行为不变）
        from core.correlation_store import correlation_store_factory
        self._correlation_store = correlation_store or correlation_store_factory(
            extract_host_fn=_extract_host,
        )
        self._correlation_window_seconds = int(
            os.getenv("AIOPS_CORRELATION_WINDOW_SECONDS", "300")
        )
        self._iterative_max_retries = int(
            os.getenv("AIOPS_ITERATIVE_MAX_RETRIES", "1")
        )
        self._iterative_min_confidence = float(
            os.getenv("AIOPS_ITERATIVE_MIN_CONFIDENCE", "0.5")
        )
        self._worker_pool = None
        # FR-7: 运行时热配置提供者（PG 表 runtime_config + 5s 刷新）
        self._runtime_config = runtime_config
        # FR-1.1/1.2/1.3: 入口背压队列 / 风暴聚合器 / 降级控制器（run_from_source 装配）
        self._ingest_queue = None
        self._storm_aggregator = None
        self._degradation = None
        self.llm_breaker = None
        self.message_producer = message_producer

        # 独立 Agent 实例
        llm_judge_enabled = os.getenv("AIOPS_LLM_JUDGE_ENABLED", "0") == "1"
        self.diagnosis_agent = DiagnosisAgent(
            llm_client=diagnosis_llm or llm,
            kb=kb,
            skill_registry=self.skill_registry,
            jira_knowledge_retriever=jira_knowledge_retriever,
            log_querier=log_querier,
            metrics_querier=metrics_querier,
        )
        self.evaluation_agent = EvaluationAgent(
            llm_client=evaluation_llm or llm,
            min_confidence=float(os.getenv("AIOPS_MIN_CONFIDENCE", "0.7")),
            llm_judge_enabled=llm_judge_enabled,
            consistency_check_enabled=os.getenv("AIOPS_CONSISTENCY_CHECK_ENABLED", "1") == "1",
        )
        self.feedback_agent = FeedbackAgent(
            ticketing=ticketing,
            kb=kb,
            llm_client=feedback_llm,
        )

        # ── FR-4: Pipeline 状态持久化（断点续跑）──
        # 显式注入优先；未注入且 AIOPS_CHECKPOINT_ENABLED=1 时自动构建
        # （PG 失败则降级禁用并记指标，不阻断启动；run_id 作为 checkpoint thread_id）
        self._checkpointer = checkpointer
        self.run_registry = run_registry
        if self._checkpointer is None and os.getenv("AIOPS_CHECKPOINT_ENABLED", "0") == "1":
            try:
                from core.checkpointing import build_checkpointer
                from core.pipeline_run_registry import PgPipelineRunRegistry
                from core.config import pg_dsn
                _dsn = pg_dsn()
                self._checkpointer = build_checkpointer(_dsn)
                self.run_registry = PgPipelineRunRegistry(_dsn)
            except Exception as exc:
                metrics.incr("pipeline.checkpoint.init_failed")
                _pipeline_logger.logger.error(
                    "checkpoint 初始化失败，降级为不持久化 | error={}", exc
                )
                self._checkpointer = None
                self.run_registry = None
        if self._checkpointer is not None and self.run_registry is None:
            from core.pipeline_run_registry import InMemoryRunRegistry
            self.run_registry = InMemoryRunRegistry()

    def _find_correlated_alerts(self, alert: Alert) -> List[dict]:
        """查找时间窗口内来自同一主机/服务的关联告警（FR-2.1: 委托 store）"""
        return self._correlation_store.find_same_host(alert, self._correlation_window_seconds)

    def _prune_recent_alerts(self):
        """清理过期的关联告警缓存（FR-2.1: 委托 store）"""
        self._correlation_store.prune(self._correlation_window_seconds)

    def record_correlation(self, alert: Alert):
        """FR-2.1: 记录告警到关联缓存（store 委托，多实例共享）"""
        self._correlation_store.add(alert, self._correlation_window_seconds)

    def process_alert(
        self, alert: Alert, route_decision: Optional[AlertRouteDecision] = None
    ) -> str:
        """处理进入的告警，返回可能产生的工单ID。

        使用 LangGraph 主图执行完整流水线。
        """
        _pipeline_logger.alert_received(alert.alert_id, alert.title, alert.level)

        final_state = run_pipeline(alert=alert, pipeline=self)

        ticket_id = final_state.get("ticket_id")
        return ticket_id or ""

    def run_from_source(self):
        """从告警数据源（如 Kafka）持续消费，经清洗/过滤后处理告警，阻塞运行"""
        if self.alert_source is None:
            raise RuntimeError("未配置 alert_source，无法启动持续消费模式")

        print("[Pipeline] === 启动持续消费模式 ===")

        from core.concurrent_pool import AlertWorkerPool
        pool = AlertWorkerPool()
        if pool.max_workers > 0:
            pool.start()
            self._worker_pool = pool
        else:
            print("[Pipeline] AIOPS_MAX_WORKERS=0，使用串行模式")
            self._worker_pool = None

        # FR-1.1: 入口背压 —— Ingest 线程 + 有界队列（队列满不拉取，offset 不动）
        ingest_worker = None
        if os.getenv("AIOPS_INGEST_BACKPRESSURE_ENABLED", "1") == "1":
            from core.ingest import IngestQueue, IngestWorker
            level_map = self.alert_cleaner.level_map if self.alert_cleaner else None
            ingest_queue = IngestQueue(level_map=level_map)
            self._ingest_queue = ingest_queue
            ingest_worker = IngestWorker(self.alert_source, ingest_queue)
            ingest_worker.start()
            print(f"[Pipeline] [FR-1.1] 入口背压已启用 | queue_size={ingest_queue._maxsize}")

        # FR-1.2: 风暴聚合器（AIOPS_AGG_ENABLED=0 关闭 → 每条自成一个 incident，等价现状）
        if os.getenv("AIOPS_AGG_ENABLED", "1") == "1":
            from core.storm_aggregator import StormAggregator
            self._storm_aggregator = StormAggregator(
                intent_classifier=self.intent_classifier,
                extract_host_fn=_extract_host,
            )
            print(
                f"[Pipeline] [FR-1.2] 风暴聚合已启用 | "
                f"window={self._storm_aggregator.window_seconds}s"
            )

        # FR-1.3: 降级控制器（信号：kafka lag / 队列积压 / LLM 熔断器失败率）
        if os.getenv("AIOPS_DEGRADE_ENABLED", "1") == "1":
            from core.audit import AuditEmitter, get_audit_store
            from core.degradation import DegradationController
            self._degradation = DegradationController(
                alert_source=self.alert_source,
                queue=self._ingest_queue,
                breaker=getattr(self, "llm_breaker", None),
                emitter=AuditEmitter(store=get_audit_store()),
                config_provider=self._runtime_config,
            )
            print("[Pipeline] [FR-1.3] 降级控制器已启用")

        try:
            while True:
                # FR-1.3: 降级档评估（每轮采集信号，迟滞切换）
                if self._degradation:
                    self._degradation.refresh()

                from core.consumption_graph import run_consumption
                final_state = run_consumption(pipeline=self)

                ticket_ids = final_state.get("ticket_ids", [])

                if ticket_ids:
                    self.feedback_agent.learn(ticket_ids)

                    metrics.gauge("pipeline.feedback.pending_tickets", len(ticket_ids))

        except KeyboardInterrupt:
            print("\n[Pipeline] 收到中断信号，正在停止...")
        finally:
            if ingest_worker:
                ingest_worker.stop()
            self._ingest_queue = None
            if self._worker_pool:
                self._worker_pool.shutdown()
                self._worker_pool = None
            if self.alert_source:
                self.alert_source.close()
            self._report_diagnosis_accuracy()
            print("[Pipeline] === 持续消费模式已停止 ===\n")

    def sync_feedbacks(self, ticket_ids: List[str]):
        """同步工单反馈到知识库（使用 FeedbackAgent）"""
        if not ticket_ids:
            return

        result = self.feedback_agent.learn(ticket_ids)

        learned_count = result.get("learned_count", 0)
        print(f"[Pipeline] 反馈同步完成，学习 {learned_count} 条工单")

    def _report_diagnosis_accuracy(self):
        """输出诊断正确率报告"""
        snap = metrics.snapshot()
        counters = snap.get("counters", {})
        correct = counters.get("pipeline.diagnosis.correct", 0)
        incorrect = counters.get("pipeline.diagnosis.incorrect", 0)
        blocked = counters.get("pipeline.diagnosis.blocked", 0)
        passed = counters.get("pipeline.diagnosis.passed", 0)

        total = correct + incorrect + blocked
        if total == 0:
            return

        print("\n[Accuracy] === 诊断正确率报告 ===")
        print(f"  评估通过: {passed}")
        print(f"  执行成功: {correct}")
        print(f"  执行失败: {incorrect}")
        print(f"  评估拦截: {blocked}")
        if correct + incorrect > 0:
            exec_accuracy = correct / (correct + incorrect) * 100
            print(f"  执行正确率: {exec_accuracy:.1f}%")
        print(f"[Accuracy] ========================\n")

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
