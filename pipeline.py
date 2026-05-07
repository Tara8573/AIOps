from core.models import Alert
from core.llm_engine import CognitiveEngine
from core.evaluator import SolutionEvaluator
from interfaces.base import IKnowledgeBase, IExecutor, IApprovalGate, ITicketing, ILLMClient

class AIOpsPipeline:
    """AIOps 主干调度流水线"""
    def __init__(
        self,
        kb: IKnowledgeBase,
        llm: ILLMClient,
        executor: IExecutor,
        approval_gate: IApprovalGate,
        ticketing: ITicketing
    ):
        self.kb = kb
        self.engine = CognitiveEngine(llm)
        self.evaluator = SolutionEvaluator()
        self.executor = executor
        self.approval_gate = approval_gate
        self.ticketing = ticketing

    def process_alert(self, alert: Alert) -> str:
        """处理进入的告警，返回可能产生的工单ID"""
        print(f"\n[Pipeline] === 开始处理新告警: {alert.alert_id} - {alert.title} ===")
        
        # 1. 查找历史经验
        sops = self.kb.search_experience(f"{alert.title} {alert.content}")
        
        # 2. 调用认知引擎分析
        print("[Pipeline] 提交 LLM 引擎分析...")
        proposal = self.engine.analyze_alert(alert, sops)
        print(f"[Pipeline] LLM分析结案: 置信度 {proposal.plan.confidence_score}")
        print(f"[Pipeline] 根因推测: {proposal.plan.root_cause_analysis}")
        
        # 3. 评估方案合法性有效性
        print("[Pipeline] 提交内部安全评估器...")
        eval_result = self.evaluator.evaluate(alert, proposal)
        
        # 4. 路由与处置
        ticket_id = None
        if eval_result.is_passed:
            print(f"[Pipeline] [PASS] 评估通过允许执行 ({eval_result.reason})")
            if proposal.plan.script_content:
                print("[Pipeline] [APPROVAL] 脚本执行前进入人工确认环节...")
                approved = self.approval_gate.confirm_execution(alert, proposal.plan, eval_result)
                if approved:
                    print("[Pipeline] [EXEC] 人工确认通过，触发物理集群自动修复...")
                    self.executor.execute_script(proposal.plan.script_content, {"alert": alert.model_dump()})
                else:
                    print("[Pipeline] [BLOCK] 人工确认拒绝执行 -> 提单至 Jira 人工辅助")
                    ticket_id = self.ticketing.create_ticket(alert, proposal.plan, eval_reason="人工确认未通过")
            else:
                print("[Pipeline] [WARN] 评估通过，但大模型未提供明确修复脚本，流程止步。")
        else:
            print(f"[Pipeline] [BLOCK] 评估拦截危险/不确定 ({eval_result.reason}) -> 提单至 Jira 人工辅助")
            ticket_id = self.ticketing.create_ticket(alert, proposal.plan, eval_reason=eval_result.reason)
            
        print("[Pipeline] === 单次告警处理流转结束 ===\n")
        return ticket_id
            
    def sync_feedbacks(self, ticket_ids: list[str]):
        """异步拉取已完结状态，打通学习闭环"""
        print("\n[FeedbackSync] === 启动完结工单同步与知识回流 ===")
        for tid in ticket_ids:
            feedback = self.ticketing.get_resolution(tid)
            if feedback and feedback.is_successful:
                print(f"[FeedbackSync] 成功猎取票据 {tid} 的最终人工处理手段")
                self.kb.learn_new_experience(feedback)
        print("[FeedbackSync] === 同步完毕 ===\n")
