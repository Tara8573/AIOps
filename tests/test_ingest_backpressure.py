"""FR-1.1：入口背压与风暴暂存验收测试。

验收标准（改造方案 §1.3.1）：
- 持续注入 10× 稳态流量，进程内存不无限增长（队列有界）
- 队列满时 Info 级告警不进入独立 LLM 诊断（进风暴暂存）
- Critical 级告警在风暴下仍被处理（不被丢弃）
- 队列与暂存都满时不拉取（offset 不动，at-least-once 配合）
"""

import threading
import time

import pytest

from core.ingest import IngestQueue, IngestWorker, raw_level_of


class CountingSource:
    """模拟告警源：按序 yield 原始消息，记录已拉取/已 mark 数量。"""

    def __init__(self, raws):
        self._raws = list(raws)
        self.consumed = 0
        self.marked = []

    def consume(self):
        for raw in self._raws:
            self.consumed += 1
            yield raw

    def mark_processed(self, raw):
        self.marked.append(raw)

    def close(self):
        pass


def _raw(i, level="Info"):
    return {"alert_id": f"ALT-{i}", "title": f"告警 {i}", "level": level, "content": f"c{i}"}


class TestIngestQueue:
    def test_queue_bounded_under_storm(self):
        """队列有界：100 条非 Critical 注入 → 队列只占 maxsize，其余进暂存。"""
        q = IngestQueue(maxsize=10, storm_maxsize=100)
        for i in range(100):
            result = q.put_raw(_raw(i), level="Info")
            assert result in ("queued", "storm_parked")

        assert q.qsize() == 10
        assert q.storm_parked_size() == 90
        assert q.full() is True
        # 内存有界：队列 + 暂存都封顶
        assert q.qsize() + q.storm_parked_size() <= 110

    def test_critical_kept_when_full(self):
        """队列满时 Critical 保留：挤掉最旧的 Info/Warning，不丢弃。"""
        q = IngestQueue(maxsize=5, storm_maxsize=100)
        for i in range(5):
            q.put_raw(_raw(i), level="Info")

        result = q.put_raw(_raw(99, level="Critical"), level="Critical")
        assert result == "queued"
        # Critical 在队列里
        levels = [raw_level_of(r) for r in q.get_many(max_items=10, timeout=0.1)]
        assert "Critical" in levels
        assert len(levels) == 5  # 最旧 Info 被挤掉到暂存

    def test_non_critical_parks_when_full(self):
        """队列满时 Info/Warning 进风暴暂存（不进入独立诊断）。"""
        q = IngestQueue(maxsize=3, storm_maxsize=10)
        for i in range(3):
            q.put_raw(_raw(i), level="Warning")
        result = q.put_raw(_raw(50), level="Info")
        assert result == "storm_parked"
        assert q.storm_parked_size() == 1

    def test_storm_overflow_marks_processed(self):
        """暂存也满：丢弃但推进 offset（mark_processed，不阻塞前缀提交）。"""
        q = IngestQueue(maxsize=1, storm_maxsize=2)
        src = CountingSource([])
        worker = IngestWorker(src, q)
        overflow = []
        for i in range(10):
            r = q.put_raw(_raw(i), level="Info")
            if r == "storm_overflow":
                overflow.append(r)
                worker._safe_mark_processed(_raw(i))
        assert q.qsize() == 1
        assert q.storm_parked_size() == 2
        assert len(overflow) == 7
        assert len(src.marked) == 7  # 丢弃的已确认 offset

    def test_critical_blocks_when_all_critical(self):
        """全队列都是 Critical：阻塞等待消费排水（背压语义）。"""
        q = IngestQueue(maxsize=2, storm_maxsize=10)
        for i in range(2):
            q.put_raw(_raw(i, level="Critical"), level="Critical")
        assert q.full()

        result_holder = {}

        def blocked_put():
            t0 = time.time()
            r = q.put_raw(_raw(99, level="Critical"), level="Critical")
            result_holder["result"] = r
            result_holder["elapsed"] = time.time() - t0

        t = threading.Thread(target=blocked_put, daemon=True)
        t.start()
        time.sleep(0.2)  # 让 blocked_put 进入等待
        assert t.is_alive(), "空间未释放时应阻塞"
        q.get_many(max_items=1, timeout=0.5)  # 排水一条
        t.join(timeout=2)
        assert result_holder.get("result") == "queued"
        assert result_holder.get("elapsed", 0) >= 0.15


class TestIngestWorker:
    def test_backpressure_stops_pulling(self):
        """队列与暂存都满 → 不拉取（consumed 停住，offset 不动）。"""
        raws = [_raw(i) for i in range(50)]
        src = CountingSource(raws)
        q = IngestQueue(maxsize=3, storm_maxsize=3)
        worker = IngestWorker(src, q, poll_batch=100)
        worker.start()

        # 等待填满（队列 3 + 暂存 3 = 6）
        deadline = time.time() + 3
        while time.time() < deadline and not (q.full() and q.storm_parked_full()):
            time.sleep(0.02)

        assert q.full() and q.storm_parked_full()
        pulled = src.consumed
        assert pulled <= 7  # 最多多拉一条在分流判断中

        # 排水后 worker 恢复拉取
        q.get_many(max_items=10, timeout=0.5)
        q.take_storm_parked(max_items=10)
        time.sleep(0.2)
        assert src.consumed > pulled

        worker.stop()

    def test_worker_level_routing(self):
        """worker 按级别分流：Critical 入队，Info 满时进暂存。"""
        raws = [_raw(i, level="Critical") for i in range(2)] + [_raw(i, level="Info") for i in range(10)]
        src = CountingSource(raws)
        q = IngestQueue(maxsize=2, storm_maxsize=20)
        worker = IngestWorker(src, q, poll_batch=100)
        worker.start()
        deadline = time.time() + 3
        while time.time() < deadline and src.consumed < len(raws):
            time.sleep(0.02)
        time.sleep(0.1)

        # 全部拉取完毕：队列满的是 Critical，Info 全部进暂存
        items = q.get_many(max_items=10, timeout=0.2)
        assert len(items) == 2
        assert all(raw_level_of(r) == "Critical" for r in items)
        assert q.storm_parked_size() >= 8
        worker.stop()
