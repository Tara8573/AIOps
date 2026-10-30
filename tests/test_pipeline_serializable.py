"""FR-4：PipelineState 纯数据化验收测试。

验收标准（改造方案 §1.1）：
- state 可完整序列化（无不可序列化对象）
- state 不再内嵌 AIOpsPipeline 实例与审计 emitter
"""

import dataclasses
import json

from core.pipeline_graph import PipelineState, run_pipeline
from tests.test_pipeline_graph import MockKB, MockTicketing, StaticLLM, SuccessExecutor, _alert
from interfaces.base import IApprovalGate
from langgraph.checkpoint.memory import InMemorySaver

from pipeline import AIOpsPipeline


class AlwaysApproveGate(IApprovalGate):
    def confirm_execution(self, alert, proposal, evaluation) -> bool:
        return True


def _build_pipeline():
    return AIOpsPipeline(
        kb=MockKB(),
        llm=StaticLLM(),
        executor=SuccessExecutor(),
        approval_gate=AlwaysApproveGate(),
        ticketing=MockTicketing(),
        checkpointer=InMemorySaver(),
    )


def _to_jsonable(value):
    """把 state 值转为可 JSON 序列化结构（pydantic → model_dump，dataclass → asdict）。"""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    return value


class TestStatePurified:
    def test_state_schema_has_no_instance_fields(self):
        """PipelineState 类型不再声明 pipeline / _audit_emitter 实例字段。"""
        assert "pipeline" not in PipelineState.__annotations__
        assert "_audit_emitter" not in PipelineState.__annotations__

    def test_runtime_state_has_no_pipeline_instance(self):
        """运行期返回的 state 不含 AIOpsPipeline 实例与审计 emitter。"""
        pipeline = _build_pipeline()
        state = run_pipeline(alert=_alert("ALT-SER-1"), pipeline=pipeline)
        assert "pipeline" not in state
        assert "_audit_emitter" not in state


class TestStateSerializable:
    def test_full_state_json_roundtrip(self):
        """state 全字段可完整序列化（无不可序列化对象）。"""
        pipeline = _build_pipeline()
        state = run_pipeline(alert=_alert("ALT-SER-2"), pipeline=pipeline)

        # 所有字段必须是 pydantic / dataclass / 原始类型 / 其容器 —— 逐一转换后可 json.dumps
        jsonable = _to_jsonable(state)
        serialized = json.dumps(jsonable, ensure_ascii=False)
        assert isinstance(serialized, str) and len(serialized) > 0

    def test_state_fields_all_jsonable_types(self):
        """逐字段断言可序列化（失败时能定位到具体字段）。"""
        pipeline = _build_pipeline()
        state = run_pipeline(alert=_alert("ALT-SER-3"), pipeline=pipeline)
        for key, value in state.items():
            json.dumps(_to_jsonable(value), ensure_ascii=False), f"字段 {key} 不可序列化: {type(value)}"


class TestExternalApiUnchanged:
    def test_run_pipeline_signature_unchanged(self):
        """run_pipeline(alert, pipeline) 外部签名与返回语义不变（回归保护）。"""
        pipeline = _build_pipeline()
        state = run_pipeline(alert=_alert("ALT-SER-4"), pipeline=pipeline)
        assert state.get("execution_success") is True
        assert not state.get("ticket_id")
