"""FR-3：人工确认异步化验收测试。

验收标准（改造方案 §1.2）：
- 审批期间 worker 释放（interrupt 挂起，不占 worker）
- 审批通过后对应告警正确恢复执行
- 审批超时自动转 Jira 工单
- submit 幂等（interrupt 恢复时节点重跑不重复登记）
- take_decided 原子领取（多实例不重复捡起）
"""

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from core.approval_resumer import ApprovalResumer
from core.models import ActionPlan, EvaluationResult
from core.pipeline_graph import resume_pipeline_with_decision, run_pipeline
from core.pipeline_run_registry import InMemoryRunRegistry
from interfaces.base import IExecutor, ITicketing
from pipeline import AIOpsPipeline
from plugins.pg_approval_store import InMemoryApprovalRequestStore
from tests.test_pipeline_graph import MockKB, StaticLLM, _alert


class RecordingExecutor(IExecutor):
    def __init__(self):
        self.calls = 0

    def execute_script(self, script_content, context) -> bool:
        self.calls += 1
        return True


class RecordingTicketing(ITicketing):
    def __init__(self):
        self.reasons = []
        self.routes = []

    def create_ticket(self, alert, proposal, eval_reason: str) -> str:
        self.reasons.append(eval_reason)
        self.routes.append(getattr(alert, "_last_ticket_route", ""))
        return f"T-{len(self.reasons)}"

    def get_resolution(self, ticket_id):
        return None


class AsyncPipeline:
    """构建 async 审批模式的测试 pipeline（checkpointer + run 登记 + 审批存储）。"""

    def __init__(self, alert_id: str):
        self.saver = InMemorySaver()
        self.registry = InMemoryRunRegistry()
        self.store = InMemoryApprovalRequestStore()
        self.executor = RecordingExecutor()
        self.ticketing = RecordingTicketing()
        self.pipeline = AIOpsPipeline(
            kb=MockKB(),
            llm=StaticLLM(),
            executor=self.executor,
            approval_gate=None,  # async 模式不使用同步 gate
            ticketing=self.ticketing,
            rule_engine=None,  # 禁用规则引擎，强制走 LLM → 审批路径
            checkpointer=self.saver,
            run_registry=self.registry,
            approval_store=self.store,
        )
        self.pipeline.approval_mode = "async"
        self.alert = _alert(alert_id)


@pytest.fixture()
def async_env():
    env = AsyncPipeline("ALT-APPR-1")
    result = run_pipeline(alert=env.alert, pipeline=env.pipeline)
    return env, result


class TestAsyncApproval:
    def test_suspend_releases_worker_and_records_pending(self, async_env):
        """审批挂起：__interrupt__ 返回（worker 释放），审批记录落库 pending。"""
        env, result = async_env
        assert "__interrupt__" in result
        # worker 已释放：execute 未执行
        assert env.executor.calls == 0
        # 审批记录 pending 落库
        record = env.store.find_by_run(result["pipeline_run_id"])
        assert record is not None
        assert record.status == "pending"
        assert record.alert_id == env.alert.alert_id
        assert record.proposal.get("root_cause_analysis")

    def test_approve_then_resume_executes(self, async_env):
        """审批通过 → Resumer 恢复 → execute 正确执行（生产路径：poll_once）。"""
        env, result = async_env
        run_id = result["pipeline_run_id"]
        approval_id = env.store.find_by_run(run_id).approval_id

        # 人工通过（回调 API 写库）
        assert env.store.decide(approval_id, "approved", decider="ops-user") is True

        # Resumer 单轮：领取 + 恢复 + mark_resumed（生产路径）
        resumer = ApprovalResumer(env.pipeline, env.store, interval=999)
        resumed = resumer.poll_once()
        assert resumed == 1

        # execute 正确执行（interrupt 返回决策值后继续）
        assert env.executor.calls == 1
        # 审批记录终态 resumed
        assert env.store.get(approval_id).status == "resumed"
        assert env.store.get(approval_id).resumed_by

    def test_reject_then_resume_creates_ticket(self, async_env):
        """审批拒绝 → 恢复后转工单。"""
        env, result = async_env
        run_id = result["pipeline_run_id"]
        approval_id = env.store.find_by_run(run_id).approval_id

        env.store.decide(approval_id, "rejected", decider="ops-user", reason="风险过高")
        final = resume_pipeline_with_decision(env.pipeline, run_id, "rejected")

        assert final is not None
        assert env.executor.calls == 0  # 拒绝不执行
        assert final.get("ticket_id")
        assert final.get("ticket_route") == "manual_rejected"
        assert final.get("ticket_reason") == "人工确认未通过"

    def test_timeout_creates_ticket(self, async_env):
        """审批超时 → sweeper 置 expired → 恢复后转工单（ticket_route=approval_timeout）。"""
        env, result = async_env
        run_id = result["pipeline_run_id"]
        approval_id = env.store.find_by_run(run_id).approval_id

        # 模拟超时：把 timeout_at 拨回过去
        from datetime import datetime, timedelta
        record = env.store.get(approval_id)
        record.timeout_at = datetime.now() - timedelta(seconds=1)

        # Resumer 单轮：expire_overdue + take_decided + 恢复
        resumer = ApprovalResumer(env.pipeline, env.store, interval=999)
        resumed = resumer.poll_once()

        assert resumed == 1
        assert env.executor.calls == 0
        ticket_route = env.ticketing.reasons
        assert ticket_route == ["审批超时自动拒绝"]
        # 终态
        assert env.store.get(approval_id).status == "resumed"

    def test_submit_idempotent_on_interrupt_rerun(self, async_env):
        """interrupt 恢复时节点重跑，submit 幂等不重复登记。"""
        env, result = async_env
        run_id = result["pipeline_run_id"]
        first_id = env.store.find_by_run(run_id).approval_id

        # 模拟节点重跑：再次 submit 同 run（幂等护栏应返回既有 id）
        second_id = env.store.submit(
            pipeline_run_id=run_id,
            alert=env.alert,
            plan=ActionPlan.model_validate(env.store.get(first_id).proposal),
            evaluation=EvaluationResult(is_passed=True, reason="ok", risk_level="Low"),
            timeout_seconds=3600,
        )
        assert second_id == first_id
        # 仅一条 pending 记录
        assert len(env.store.list_by_status("pending")) == 1


class TestTakeDecidedAtomic:
    def test_take_decided_marks_and_no_double_take(self):
        """领取即标记 resumed_at —— 二次领取不重复（多实例不重复捡起）。"""
        env = AsyncPipeline("ALT-APPR-2")
        result = run_pipeline(alert=env.alert, pipeline=env.pipeline)
        run_id = result["pipeline_run_id"]
        approval_id = env.store.find_by_run(run_id).approval_id
        env.store.decide(approval_id, "approved", decider="ops")

        # 实例 A 领取
        taken_a = env.store.take_decided(limit=10, instance_id="instance-a")
        assert [r.approval_id for r in taken_a] == [approval_id]
        assert taken_a[0].resumed_by == "instance-a"

        # 实例 B 再领取 → 空（不重复捡起）
        taken_b = env.store.take_decided(limit=10, instance_id="instance-b")
        assert taken_b == []

    def test_take_decided_fifo_order(self):
        """多 条已决策按 decided_at FIFO 领取。"""
        env = AsyncPipeline("ALT-APPR-3")
        result = run_pipeline(alert=env.alert, pipeline=env.pipeline)
        run_id = result["pipeline_run_id"]
        approval_id = env.store.find_by_run(run_id).approval_id
        env.store.decide(approval_id, "approved", decider="ops")

        # 再造一条 rejected
        other = env.store.submit(
            pipeline_run_id="run-other", alert=env.alert,
            plan=ActionPlan.model_validate(env.store.get(approval_id).proposal),
            evaluation=EvaluationResult(is_passed=True, reason="ok", risk_level="Low"),
            timeout_seconds=3600,
        )
        env.store.decide(other, "rejected", decider="ops")

        taken = env.store.take_decided(limit=10, instance_id="i")
        assert [r.approval_id for r in taken] == [approval_id, other]


class TestSyncModeUnchanged:
    def test_sync_mode_still_uses_gate(self, monkeypatch):
        """sync 模式（默认）保持旧行为：同步调用 approval_gate。"""
        from interfaces.base import IApprovalGate

        class ApproveGate(IApprovalGate):
            def __init__(self):
                self.called = 0

            def confirm_execution(self, alert, proposal, evaluation) -> bool:
                self.called += 1
                return True

        gate = ApproveGate()
        pipeline = AIOpsPipeline(
            kb=MockKB(),
            llm=StaticLLM(),
            executor=RecordingExecutor(),
            approval_gate=gate,
            ticketing=RecordingTicketing(),
            rule_engine=None,  # 禁用规则引擎，强制走 LLM → 审批路径
        )
        assert pipeline.approval_mode == "sync"
        result = run_pipeline(alert=_alert("ALT-APPR-4"), pipeline=pipeline)
        assert gate.called == 1
        assert result.get("execution_success") is True
