"""审计体系测试。

覆盖：
- AuditEmitter 基本发射
- Hash chain 完整性
- audit_node 装饰器
- FileAuditStore 读写 + chain 校验
"""

import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from core.audit import AuditEmitter, audit_node
from core.models import AuditEvent
from plugins.audit_file_store import FileAuditStore
from plugins.mocks import MockAuditStore


class TestAuditEmitter:

    def test_emit_basic(self):
        emitter = AuditEmitter(enabled=True)
        event = emitter.emit("test.event", alert_id="ALT-001", severity="info")
        assert event is not None
        assert event.event_type == "test.event"
        assert event.alert_id == "ALT-001"
        assert event.severity == "info"

    def test_emit_disabled(self):
        emitter = AuditEmitter(enabled=False)
        event = emitter.emit("test.event")
        assert event is None

    def test_hash_chain(self):
        emitter = AuditEmitter(enabled=True)
        e1 = emitter.emit("event.1")
        e2 = emitter.emit("event.2")
        e3 = emitter.emit("event.3")

        assert e1.prev_hash is None
        assert e2.prev_hash is not None
        assert e3.prev_hash is not None
        assert e2.prev_hash != e3.prev_hash

    def test_events_accumulate(self):
        emitter = AuditEmitter(enabled=True)
        emitter.emit("a")
        emitter.emit("b")
        emitter.emit("c")
        assert len(emitter.events) == 3

    def test_reset(self):
        emitter = AuditEmitter(enabled=True)
        emitter.emit("a")
        assert len(emitter.events) == 1
        emitter.reset()
        assert len(emitter.events) == 0
        assert emitter.last_hash is None

    def test_emit_with_store(self):
        store = MockAuditStore()
        emitter = AuditEmitter(store=store, enabled=True)
        emitter.emit("test.event", alert_id="ALT-001")
        assert len(store.events) == 1
        assert store.events[0].event_type == "test.event"

    def test_emit_store_error_does_not_raise(self):
        store = MockAuditStore()
        store.emit = MagicMock(side_effect=RuntimeError("disk full"))
        emitter = AuditEmitter(store=store, enabled=True)
        event = emitter.emit("test.event")
        assert event is not None


class TestAuditNodeDecorator:

    def test_decorator_emits_start_and_complete(self):
        store = MockAuditStore()
        emitter = AuditEmitter(store=store, enabled=True)

        @audit_node("test_node")
        def my_node(state):
            return {"result": "ok"}

        from core.models import Alert
        alert = Alert(alert_id="ALT-001", title="test", level="Warning", content="test")
        state = {"_audit_emitter": emitter, "alert": alert, "pipeline_run_id": "run-001"}
        result = my_node(state)

        assert result == {"result": "ok"}
        assert len(store.events) == 2
        assert store.events[0].event_type == "test_node.start"
        assert store.events[1].event_type == "test_node.complete"
        assert store.events[1].outcome == "success"

    def test_decorator_emits_error_on_exception(self):
        store = MockAuditStore()
        emitter = AuditEmitter(store=store, enabled=True)

        @audit_node("fail_node")
        def fail_node(state):
            raise ValueError("boom")

        from core.models import Alert
        alert = Alert(alert_id="ALT-002", title="test", level="Warning", content="test")
        state = {"_audit_emitter": emitter, "alert": alert}

        with pytest.raises(ValueError, match="boom"):
            fail_node(state)

        assert len(store.events) == 2
        assert store.events[1].event_type == "fail_node.error"
        assert store.events[1].outcome == "failure"
        assert store.events[1].severity == "critical"

    def test_decorator_works_without_emitter(self):
        @audit_node("bare_node")
        def bare_node(state):
            return {"value": 42}

        result = bare_node({})
        assert result == {"value": 42}


class TestFileAuditStore:

    def test_emit_and_query(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FileAuditStore(log_dir=tmpdir)
            emitter = AuditEmitter(store=store, enabled=True)
            emitter.emit("alert.received", alert_id="ALT-001")
            emitter.emit("eval.passed", alert_id="ALT-001")
            emitter.emit("alert.received", alert_id="ALT-002")

            all_events = store.query()
            assert len(all_events) == 3

            alt1_events = store.query(alert_id="ALT-001")
            assert len(alt1_events) == 2

            type_events = store.query(event_type="alert.received")
            assert len(type_events) == 2

    def test_verify_chain_valid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FileAuditStore(log_dir=tmpdir)
            emitter = AuditEmitter(store=store, enabled=True)
            e1 = emitter.emit("event.1")
            e2 = emitter.emit("event.2")
            e3 = emitter.emit("event.3")

            assert store.verify_chain(e1.event_id, e3.event_id) is True

    def test_verify_chain_tampered(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FileAuditStore(log_dir=tmpdir)
            emitter = AuditEmitter(store=store, enabled=True)
            e1 = emitter.emit("event.1")
            e2 = emitter.emit("event.2")

            # 篡改中间事件
            day = e1.timestamp.strftime("%Y-%m-%d")
            path = os.path.join(tmpdir, f"audit_{day}.jsonl")
            lines = open(path, "r").readlines()
            lines[0] = lines[0].replace('"event.1"', '"event.1.TAMPERED"')
            open(path, "w").writelines(lines)

            assert store.verify_chain(e1.event_id, e2.event_id) is False

    def test_query_with_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FileAuditStore(log_dir=tmpdir)
            emitter = AuditEmitter(store=store, enabled=True)
            for i in range(10):
                emitter.emit(f"event.{i}")

            events = store.query(limit=5)
            assert len(events) == 5
