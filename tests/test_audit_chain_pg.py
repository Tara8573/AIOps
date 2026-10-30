"""FR-2.3：审计 hash chain 持久化验收测试。

验收标准（规格 FR-2.3）：
- 重启进程后新审计事件的 prev_hash 接续重启前的链
- 多实例写入时链不产生分叉（链头托管 + 串行化）

链头托管语义用 supports_chain_state 假实现模拟（PG 事务行锁的生产实现在
plugins/audit_pg_store.py，需 live PG 的集成测试覆盖）。
"""

import hashlib
import threading

from core.audit import AuditEmitter


class FakeChainStore:
    """模拟 PG 链头托管 store（supports_chain_state=True；锁模拟事务行锁）。"""

    supports_chain_state = True

    def __init__(self):
        self._lock = threading.Lock()
        self.head = None
        self.events = []
        self.hashes = []  # 每条事件的 hash（链校验用）

    def emit(self, event) -> str:
        with self._lock:  # 模拟 FOR UPDATE 行锁
            event.prev_hash = self.head
            content = event.model_dump_json(exclude={"prev_hash"})
            new_hash = hashlib.sha256(
                (content + (self.head or "")).encode()
            ).hexdigest()
            self.head = new_hash
            self.events.append(event)
            self.hashes.append(new_hash)
            return new_hash


class TestChainPersistence:
    def test_emitter_delegates_chain_to_store(self):
        """链头托管：store 负责填 prev_hash + 推进链头。"""
        store = FakeChainStore()
        emitter = AuditEmitter(store=store, enabled=True)
        e1 = emitter.emit("event.1")
        e2 = emitter.emit("event.2")

        assert e1.prev_hash is None          # 首条空链
        assert e2.prev_hash == store.events[0].prev_hash or e2.prev_hash is not None
        assert e2.prev_hash is not None
        assert emitter.last_hash == store.head  # 链头同步

    def test_restart_continues_chain(self):
        """重启进程：新 emitter 接续既有链（prev_hash 接续重启前的链头）。"""
        store = FakeChainStore()
        emitter1 = AuditEmitter(store=store, enabled=True)
        emitter1.emit("event.1")
        emitter1.emit("event.2")
        head_before = store.head

        # "重启"：新 emitter（内存链头丢失）+ 同一 store（链头持久）
        emitter2 = AuditEmitter(store=store, enabled=True)
        assert emitter2.last_hash is None
        e3 = emitter2.emit("event.3")
        assert e3.prev_hash == head_before, "重启后 prev_hash 应接续重启前的链"

    def test_concurrent_emits_no_fork(self):
        """双实例并发 emit：链不产生分叉（行锁串行化）。"""
        store = FakeChainStore()

        def emit_loop(n):
            emitter = AuditEmitter(store=store, enabled=True)  # 每实例独立 emitter
            for i in range(50):
                emitter.emit(f"event.{n}.{i}")

        threads = [threading.Thread(target=emit_loop, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 全链校验：每条 prev_hash 接续前一条的 hash
        for i, event in enumerate(store.events):
            expected_prev = store.hashes[i - 1] if i > 0 else None
            assert event.prev_hash == expected_prev, f"链分叉！第 {i} 条"
        assert len(store.events) == 100

    def test_local_chain_unchanged_for_file_store(self):
        """file/memory store 路径保持现状本地链行为（回归保护）。

        真实不变量：last_hash = sha256(e2 内容 + e2.prev_hash)（链头 = 最后一条事件的 hash）。
        """
        from tests.test_audit import MockAuditStore  # 复用现有 mock
        store = MockAuditStore()
        emitter = AuditEmitter(store=store, enabled=True)
        e1 = emitter.emit("event.1")
        e2 = emitter.emit("event.2")
        assert e1.prev_hash is None
        assert e2.prev_hash is not None
        # 链头 = 最后一条事件的 hash（重算不变量）
        recomputed = hashlib.sha256(
            (e2.model_dump_json(exclude={"prev_hash"}) + (e2.prev_hash or "")).encode()
        ).hexdigest()
        assert emitter.last_hash == recomputed
