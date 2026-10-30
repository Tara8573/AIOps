"""FR-1.3：自动降级控制器验收测试。

验收标准（改造方案 §1.3.3）：
- 注入 lag 超阈值 → 自动切 L1
- L2 档下 LLM 仅 Critical 且无迭代
- 降级/恢复事件有审计记录
- 手动热开关切换降级档（FR-7 联动）
- 风暴结束后自动恢复全量模式（规格 4.2）
"""

import time

from core.audit import AuditEmitter
from core.degradation import DegradationController, L1, L2, NORMAL
from core.models import Alert
from interfaces.base import IApprovalGate, IExecutor, ITicketing
from langgraph.checkpoint.memory import InMemorySaver

from pipeline import AIOpsPipeline
from plugins.pg_approval_store import InMemoryApprovalRequestStore
from tests.test_pipeline_graph import MockKB, StaticLLM, _alert


class FakeSource:
    def __init__(self, lag_value=0):
        self._lag = lag_value

    def lag(self):
        return self._lag

    def consume(self):
        return iter(())

    def close(self):
        pass


class FakeQueue:
    def __init__(self, qsize=0, storm=0):
        self._qsize = qsize
        self._storm = storm

    def qsize(self):
        return self._qsize

    def storm_parked_size(self):
        return self._storm


class FakeBreaker:
    def __init__(self, rate=0.0):
        self._rate = rate

    def timeout_rate(self):
        return self._rate


class RecordingEmitter:
    """审计事件捕获。"""

    def __init__(self):
        self.events = []

    def emit(self, event_type, **kwargs):
        self.events.append((event_type, kwargs))


class RecordingExecutor(IExecutor):
    def __init__(self):
        self.calls = 0

    def execute_script(self, script_content, context):
        self.calls += 1
        return True


def _alert(alert_id: str, level: str = "Critical") -> Alert:
    """本地告警构造（支持 level 参数）。"""
    return Alert(
        alert_id=alert_id,
        title="Disk Space Critical",
        level=level,
        content="/var disk usage > 98%",
    )


class RecordingTicketing(ITicketing):
    def __init__(self):
        self.reasons = []

    def create_ticket(self, alert, proposal, eval_reason):
        self.reasons.append(eval_reason)
        return f"T-{len(self.reasons)}"

    def get_resolution(self, ticket_id):
        return None


class TestDegradationLevels:
    def test_l1_on_lag_threshold(self):
        """lag 超阈值 → 自动切 L1。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=1000), queue=FakeQueue(), breaker=FakeBreaker(0.0),
        )
        assert ctrl.refresh() == L1

    def test_normal_when_signals_low(self):
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=10), queue=FakeQueue(), breaker=FakeBreaker(0.0),
        )
        assert ctrl.refresh() == NORMAL

    def test_l2_on_severe_signals(self):
        """lag 超阈值 + LLM 失败率超 L2 阈值 → 直接 L2。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=1000), queue=FakeQueue(), breaker=FakeBreaker(0.7),
        )
        assert ctrl.refresh() == L2

    def test_l2_on_timeout_rate_alone(self):
        """LLM 失败率超 L2 阈值（0.6）→ L2。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=0), queue=FakeQueue(), breaker=FakeBreaker(0.7),
        )
        assert ctrl.refresh() == L2

    def test_l1_to_l2_after_duration(self):
        """L1 持续超时 + lag 仍超 → 升 L2（AIOPS_DEGRADE_L2_AFTER_SECONDS）。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=1000), queue=FakeQueue(), breaker=FakeBreaker(0.1),
        )
        assert ctrl.refresh() == L1
        # 模拟 L1 持续 200s
        ctrl._level_since = time.time() - 200
        assert ctrl.refresh() == L2

    def test_recovery_after_storm_ends(self):
        """风暴结束：信号连续 3 周期低于恢复阈值 → 逐级回退 NORMAL（规格 4.2）。"""
        src = FakeSource(lag_value=1000)
        ctrl = DegradationController(alert_source=src, queue=FakeQueue(), breaker=FakeBreaker(0.0))
        assert ctrl.refresh() == L1

        src._lag = 10  # 风暴结束
        for _ in range(3):
            ctrl.refresh()
        assert ctrl.level == NORMAL

    def test_audit_events_on_transitions(self):
        """降级/恢复事件有审计记录。"""
        emitter = RecordingEmitter()
        src = FakeSource(lag_value=1000)
        ctrl = DegradationController(alert_source=src, queue=FakeQueue(), breaker=FakeBreaker(0.0), emitter=emitter)
        ctrl.refresh()  # → L1（entered）
        src._lag = 10
        for _ in range(3):
            ctrl.refresh()  # → NORMAL（exited）

        types = [t for t, _ in emitter.events]
        assert "degradation.entered" in types
        assert "degradation.exited" in types


class TestManualOverride:
    def test_force_level_l2(self):
        """手动强制 L2（热开关）。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=0), queue=FakeQueue(), breaker=FakeBreaker(0.0),
        )
        ctrl.refresh()  # NORMAL
        ctrl.force_level(L2)
        assert ctrl.is_l2() is True
        assert ctrl.refresh() == L2  # 自动检测让位

    def test_force_level_release(self):
        """释放手动覆盖 → 恢复自动检测。"""
        ctrl = DegradationController(
            alert_source=FakeSource(lag_value=0), queue=FakeQueue(), breaker=FakeBreaker(0.0),
        )
        ctrl.force_level(L1)
        ctrl.force_level("")
        assert ctrl.refresh() == NORMAL

    def test_force_level_invalid(self):
        ctrl = DegradationController()
        try:
            ctrl.force_level("L9")
            assert False, "应拒绝非法档位"
        except ValueError:
            pass


class TestL2Behavior:
    def test_allows_level_l2_critical_only(self):
        """L2 档只处理 Critical。"""
        ctrl = DegradationController()
        assert ctrl.allows_level("Warning") is True  # NORMAL 档全放行
        ctrl.force_level(L2)
        assert ctrl.allows_level("Critical") is True
        assert ctrl.allows_level("Warning") is False
        assert ctrl.allows_level("Info") is False

    def test_l2_filter_only_critical_in_consumption(self):
        """消费图 filter 节点：L2 下 Info/Warning 暂存 Kafka（offset 不动）。"""
        from core.consumption_graph import run_consumption

        class MarkSource(FakeSource):
            def __init__(self):
                super().__init__(lag_value=0)
                self.marked = []

            def mark_processed(self, raw):
                self.marked.append(raw)

        pipeline = AIOpsPipeline(
            kb=MockKB(),
            llm=StaticLLM(),
            executor=RecordingExecutor(),
            approval_gate=None,
            ticketing=RecordingTicketing(),
            rule_engine=None,
            alert_source=MarkSource(),
            alert_cleaner=None,
            checkpointer=InMemorySaver(),
        )
        pipeline.approval_mode = "async"
        pipeline.approval_store = InMemoryApprovalRequestStore()

        degradation = DegradationController()
        degradation.force_level(L2)
        pipeline._degradation = degradation

        # 注入混合级别告警（直接塞 filtered_alerts 的上游 —— 用 raw 消息走 consume）
        pipeline.alert_cleaner = None  # 无 cleaner → raw 直接转 Alert

        # consume 无 ingest queue → 直接消费（fake source 空）——手工塞 state 测 filter
        from core.consumption_graph import filter_alerts
        state = {
            "pipeline": pipeline,
            "cleaned_alerts": [
                _alert("1", level="Critical"),
                _alert("2", level="Warning"),
                _alert("3", level="Info"),
            ],
        }
        result = filter_alerts(state)
        # L2 下只放行 Critical
        assert [a.alert_id for a in result["filtered_alerts"]] == ["1"]


class TestL1InfoSkip:
    def test_l1_skips_info_representative_diagnosis(self):
        """L1 档 Info 组代表不做 LLM 诊断（只诊断高优先级）。"""
        from core.consumption_graph import process_incidents
        from core.storm_aggregator import StormAggregator, StormIncident
        from core.intent import AlertIntentClassifier

        pipeline = AIOpsPipeline(
            kb=MockKB(),
            llm=StaticLLM(),
            executor=RecordingExecutor(),
            approval_gate=None,
            ticketing=RecordingTicketing(),
            rule_engine=None,
        )
        degradation = DegradationController()
        degradation.force_level(L1)
        pipeline._degradation = degradation
        pipeline._storm_aggregator = StormAggregator(
            intent_classifier=AlertIntentClassifier(), extract_host_fn=lambda a: "h",
        )

        inc = StormIncident(group_id="inc-x", signature="s", intent_category="disk", host="h")
        rep = _alert("1", level="Info")
        inc.representative = rep
        inc.members.append(rep)

        state = {"pipeline": pipeline, "incidents": [inc], "ticket_ids": []}
        result = process_incidents(state)

        assert inc.result is not None
        assert inc.result.get("skipped")  # 未做 LLM 诊断
        assert inc.broadcast_ids == {rep.alert_id}  # 已广播
