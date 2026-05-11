from core.models import Alert
from core.llm_engine import CognitiveEngine
from core.evaluator import SolutionEvaluator
from core.logger import get_logger
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
from typing import Optional, List

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

    def process_alert(self, alert: Alert) -> str:
        """处理进入的告警，返回可能产生的工单ID"""
        print(f"\n[Pipeline] === 开始处理新告警: {alert.alert_id} - {alert.title} ===")
        _pipeline_logger.alert_received(alert.alert_id, alert.title, alert.level)

        # 1. 查找历史经验
        sops = self.kb.search_experience(f"{alert.title} {alert.content}")

        # 1.1 调用可选 skills
        skill_results = self.skill_registry.run(
            alert,
            {"history_sops": sops, "alert": alert.model_dump()},
        )
        if skill_results:
            print(
                "[Pipeline] 命中 Skills: "
                + ", ".join(result.skill_name for result in skill_results)
            )

        # 2. 调用认知引擎分析
        print("[Pipeline] 提交 LLM 引擎分析...")
        _pipeline_logger.llm_analysis_start(alert.alert_id)
        proposal = self.engine.analyze_alert(alert, sops, skill_results)
        print(f"[Pipeline] LLM分析结案: 置信度 {proposal.plan.confidence_score}")
        print(f"[Pipeline] 根因推测: {proposal.plan.root_cause_analysis}")
        _pipeline_logger.llm_analysis_complete(
            alert.alert_id, proposal.plan.confidence_score
        )

        # 3. 评估方案合法性有效性
        print("[Pipeline] 提交内部安全评估器...")
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
                approved = self.approval_gate.confirm_execution(
                    alert, proposal.plan, eval_result
                )
                if approved:
                    print("[Pipeline] [EXEC] 人工确认通过，触发物理集群自动修复...")
                    _pipeline_logger.execution_start(
                        alert.alert_id, proposal.plan.script_content
                    )
                    self.executor.execute_script(
                        proposal.plan.script_content, {"alert": alert.model_dump()}
                    )
                    _pipeline_logger.execution_complete(alert.alert_id, True)
                else:
                    print("[Pipeline] [BLOCK] 人工确认拒绝执行 -> 提单至 Jira 人工辅助")
                    ticket_id = self.ticketing.create_ticket(
                        alert, proposal.plan, eval_reason="人工确认未通过"
                    )
                    _pipeline_logger.ticket_created(
                        alert.alert_id, ticket_id, "人工确认未通过"
                    )
            else:
                print(
                    "[Pipeline] [WARN] 评估通过，但大模型未提供明确修复脚本，流程止步。"
                )
        else:
            print(
                f"[Pipeline] [BLOCK] 评估拦截危险/不确定 ({eval_result.reason}) -> 提单至 Jira 人工辅助"
            )
            ticket_id = self.ticketing.create_ticket(
                alert, proposal.plan, eval_reason=eval_result.reason
            )
            _pipeline_logger.ticket_created(
                alert.alert_id, ticket_id, eval_result.reason
            )

        print("[Pipeline] === 单次告警处理流转结束 ===\n")
        return ticket_id or ""

    def run_from_source(self):
        """从告警数据源（如 Kafka）持续消费，经清洗/过滤后处理告警，阻塞运行"""
        if self.alert_source is None:
            raise RuntimeError("未配置 alert_source，无法启动持续消费模式")

        print("[Pipeline] === 启动持续消费模式 ===")
        ticket_ids: List[str] = []

        try:
            for raw_msg in self.alert_source.consume():
                alert = self._clean_and_filter(raw_msg)
                if alert is None:
                    continue
                tid = self.process_alert(alert)
                if tid:
                    ticket_ids.append(tid)
        except KeyboardInterrupt:
            print("\n[Pipeline] 收到中断信号，正在停止...")
        finally:
            self.alert_source.close()
            if ticket_ids:
                self.sync_feedbacks(ticket_ids)
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
                _pipeline_logger.feedback_received(
                    feedback.alert_id, tid, feedback.is_successful
                )
                _pipeline_logger.knowledge_learned(
                    feedback.alert_id, feedback.resolution_steps
                )
        print("[FeedbackSync] === 同步完毕 ===\n")
