"""FR-4：断点续跑验收测试。

验收标准（改造方案 §1.1）：
- 处理中途 kill 进程，重启后该告警从断点继续（不重复执行已完成的 execute）
- 结合 run_registry（pipeline_runs 登记表），崩溃后按 alert_id 找到未终结 run 续跑

崩溃模拟方式：进程内两阶段模拟 ——
- Phase 1: metrics 在 verify.passed 处抛 SystemExit（BaseException，run_pipeline
  不会捕获，等价于进程被 kill）；此时 execute 已完成且 checkpoint 已落
- Phase 2: 用同一 checkpointer/registry（等价持久化介质）重建 pipeline，
  run_pipeline 按 alert_id 找到未终结 run → input=None 从断点续跑
"""

import pytest
from langgraph.checkpoint.memory import InMemorySaver

import core.pipeline_graph
from core.pipeline_graph import resume_pipeline_run, run_pipeline
from core.pipeline_run_registry import InMemoryRunRegistry
from interfaces.base import IApprovalGate, IExecutor, ITicketing
from pipeline import AIOpsPipeline
from tests.test_pipeline_graph import MockKB, StaticLLM, _alert


class AlwaysApproveGate(IApprovalGate):
    def confirm_execution(self, alert, proposal, evaluation) -> bool:
        return True


class RecordingExecutor(IExecutor):
    """记录 execute 调用次数的执行器（跨阶段共享，断言 execute 不重复）。"""

    def __init__(self):
        self.calls = 0

    def execute_script(self, script_content, context) -> bool:
        self.calls += 1
        return True


class RecordingTicketing(ITicketing):
    def __init__(self):
        self.reasons = []

    def create_ticket(self, alert, proposal, eval_reason: str) -> str:
        self.reasons.append(eval_reason)
        return f"T-{len(self.reasons)}"

    def get_resolution(self, ticket_id):
        return None


class CrashAtVerifyMetrics:
    """包装真实 metrics：仅在 verify.passed 处抛 SystemExit，模拟 execute 完成后进程崩溃。"""

    def __init__(self, real):
        self._real = real
        self.crashed = False

    def incr(self, name, value=1):
        if name == "pipeline.verify.passed" and not self.crashed:
            self.crashed = True
            raise SystemExit("simulated crash after execute")
        return self._real.incr(name, value)

    def observe(self, *a, **k):
        return self._real.observe(*a, **k)

    def gauge(self, *a, **k):
        return self._real.gauge(*a, **k)

    def timer(self, *a, **k):
        return self._real.timer(*a, **k)

    def snapshot(self):
        return self._real.snapshot()


@pytest.fixture()
def crash_env(monkeypatch):
    saver = InMemorySaver()
    registry = InMemoryRunRegistry()
    executor = RecordingExecutor()
    ticketing = RecordingTicketing()

    def build(crash: bool):
        if crash:
            monkeypatch.setattr(
                core.pipeline_graph, "metrics", CrashAtVerifyMetrics(core.pipeline_graph.metrics)
            )
        return AIOpsPipeline(
            kb=MockKB(),
            llm=StaticLLM(),
            executor=executor,
            approval_gate=AlwaysApproveGate(),
            ticketing=ticketing,
            checkpointer=saver,
            run_registry=registry,
        )

    return build, executor, ticketing, registry


class TestCheckpointResume:
    def test_crash_then_resume_does_not_repeat_execute(self, crash_env):
        """kill 后重启：从断点续跑，已完成的 execute 不重复执行。"""
        build, executor, ticketing, registry = crash_env

        alert = _alert("ALT-RESUME-1")

        # Phase 1: execute 完成后在 verify 处"崩溃"（SystemExit 等价进程 kill）
        with pytest.raises(SystemExit):
            run_pipeline(alert=alert, pipeline=build(crash=True))

        # 崩溃点断言：execute 已执行 1 次，run 仍处于未终结状态
        assert executor.calls == 1
        open_run = registry.find_open_run(alert.alert_id)
        assert open_run is not None, "崩溃后 run 应保持未终结（可续跑）"

        # Phase 2: "重启" —— 同一 checkpointer/registry（持久化介质），健康环境
        state = run_pipeline(alert=alert, pipeline=build(crash=False))

        # 断点续跑：execute 不重复（仍 1 次），流程走到完成
        assert executor.calls == 1
        assert state.get("execution_success") is True
        assert not ticketing.reasons, "续跑不应创建兜底工单"
        # 终态：run 已终结
        assert registry.find_open_run(alert.alert_id) is None

    def test_resume_via_resume_pipeline_run(self, crash_env):
        """resume_pipeline_run 直接恢复指定 run（僵死巡检入口）。"""
        build, executor, _, registry = crash_env
        alert = _alert("ALT-RESUME-2")

        with pytest.raises(SystemExit):
            run_pipeline(alert=alert, pipeline=build(crash=True))

        run_id = registry.find_open_run(alert.alert_id).run_id
        result = resume_pipeline_run(build(crash=False), run_id)

        assert result is not None
        assert executor.calls == 1  # execute 不重复
        assert result.get("execution_success") is True
        assert registry.find_open_run(alert.alert_id) is None

    def test_completed_run_not_reused_by_new_alert(self, crash_env):
        """run 完结后，同 alert_id 的新告警启动新 run（不复用旧 thread）。"""
        build, executor, _, registry = crash_env
        alert = _alert("ALT-RESUME-3")

        # 正常跑完一次
        state = run_pipeline(alert=alert, pipeline=build(crash=False))
        assert state.get("execution_success") is True
        assert registry.find_open_run(alert.alert_id) is None

        # 新告警（同 ID）再次处理 → 新 run，execute 合法地再次执行
        state2 = run_pipeline(alert=alert, pipeline=build(crash=False))
        assert state2.get("execution_success") is True
        assert executor.calls == 2
