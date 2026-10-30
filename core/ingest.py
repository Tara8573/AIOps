"""入口背压与风暴暂存（FR-1.1）。

IngestQueue: 有界接收队列
  - 队列有空间：直接入队
  - 队列满：Critical 保留（挤掉队首最旧的 Info/Warning）；其他级别进"风暴暂存"
    （有界 FIFO，仅聚合不单独诊断）；风暴暂存也满 → 丢弃并 mark_processed（offset 推进）
  - 与 at-least-once 配合：消费不过来时 IngestWorker 不拉取，offset 不动，留在 Kafka

IngestWorker: 入口消费线程（Kafka poll → 有界队列）
"""

import os
import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional

from core.logger import get_logger
from core.observability import metrics

logger = get_logger("ingest")


def raw_level_of(raw: Dict[str, Any], level_map: Optional[Dict[str, str]] = None) -> str:
    """从原始消息粗提级别（扁平字段，供入队分级决策；clean 阶段再做完整归一化）。"""
    level_map = level_map or {
        "critical": "Critical", "major": "Critical", "p1": "Critical",
        "p2": "Warning", "warning": "Warning",
        "minor": "Info", "info": "Info", "p3": "Info", "p4": "Info",
    }
    raw_level = str(
        raw.get("level") or raw.get("severity") or raw.get("priority") or ""
    ).strip().lower()
    return level_map.get(raw_level, "Info")


class IngestQueue:
    """有界接收队列 + 风暴暂存。线程安全（ingest 线程生产，worker 线程消费）。"""

    def __init__(
        self,
        maxsize: Optional[int] = None,
        storm_maxsize: Optional[int] = None,
        level_map: Optional[Dict[str, str]] = None,
    ):
        self._maxsize = max(1, maxsize if maxsize is not None else int(
            os.getenv("AIOPS_INGEST_QUEUE_SIZE", "500")))
        self._storm_maxsize = max(1, storm_maxsize if storm_maxsize is not None else int(
            os.getenv("AIOPS_STORM_PARKING_SIZE", "2000")))
        self._level_map = level_map or {}
        self._items: deque = deque()
        self._storm: deque = deque()
        self._cond = threading.Condition()
        self._closed = False

    # ── 生产侧（IngestWorker）──

    def put_raw(self, raw: Dict[str, Any], level: Optional[str] = None) -> str:
        """入队一条原始消息。

        Returns:
            "queued" 入队 | "storm_parked" 进风暴暂存 | "storm_overflow" 暂存满被丢弃
        """
        level = level or raw_level_of(raw, self._level_map or None)
        with self._cond:
            if len(self._items) < self._maxsize:
                self._items.append(raw)
                self._cond.notify_all()
                metrics.gauge("aiops_ingest_queue_depth", len(self._items))
                return "queued"

            if level != "Critical":
                # 风暴暂存（仅聚合，不单独诊断）
                if len(self._storm) >= self._storm_maxsize:
                    metrics.incr("aiops_ingest_storm_overflow_total")
                    return "storm_overflow"
                self._storm.append(raw)
                metrics.incr("aiops_ingest_storm_parked_total")
                metrics.gauge("aiops_ingest_storm_parked_depth", len(self._storm))
                return "storm_parked"

            # Critical 保留：挤掉队首最旧的 Info/Warning
            evicted = False
            for i, item in enumerate(self._items):
                if raw_level_of(item, self._level_map or None) != "Critical":
                    # 被挤掉的降级进风暴暂存
                    del self._items[i]
                    evicted = True
                    metrics.incr("aiops_ingest_evicted_total")
                    if len(self._storm) < self._storm_maxsize:
                        self._storm.append(item)
                        metrics.incr("aiops_ingest_storm_parked_total")
                    else:
                        metrics.incr("aiops_ingest_storm_overflow_total")
                    break
            if not evicted:
                # 全队列都是 Critical：阻塞等待消费排水（背压语义，内存有界）
                while len(self._items) >= self._maxsize and not self._closed:
                    self._cond.wait(0.1)
                if self._closed:
                    return "storm_overflow"

            self._items.append(raw)
            self._cond.notify_all()
            metrics.gauge("aiops_ingest_queue_depth", len(self._items))
            return "queued"

    # ── 消费侧（consumption_graph）──

    def get_many(self, max_items: int = 100, timeout: float = 0.5) -> List[Dict[str, Any]]:
        """批量取数（阻塞等待至 timeout）；无数据返回空列表。"""
        deadline = time.time() + max(0.0, timeout)
        with self._cond:
            while not self._items and not self._closed and time.time() < deadline:
                self._cond.wait(0.05)
            out = []
            while self._items and len(out) < max_items:
                out.append(self._items.popleft())
            if out:
                self._cond.notify_all()  # 唤醒等待空间的 put
                metrics.gauge("aiops_ingest_queue_depth", len(self._items))
            return out

    def take_storm_parked(self, max_items: int = 200) -> List[Dict[str, Any]]:
        """取走风暴暂存的原始消息（供聚合器收编为组员，不单独诊断）。"""
        with self._cond:
            out = []
            while self._storm and len(out) < max_items:
                out.append(self._storm.popleft())
            if out:
                metrics.gauge("aiops_ingest_storm_parked_depth", len(self._storm))
            return out

    def full(self) -> bool:
        with self._cond:
            return len(self._items) >= self._maxsize

    def storm_parked_full(self) -> bool:
        with self._cond:
            return len(self._storm) >= self._storm_maxsize

    def qsize(self) -> int:
        with self._cond:
            return len(self._items)

    def storm_parked_size(self) -> int:
        with self._cond:
            return len(self._storm)

    def close(self):
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class IngestWorker:
    """入口消费线程：Kafka poll → 有界队列（FR-1.1 背压）。

    队列满时不拉取（不消费 offset 不动，留在 Kafka，配合 at-least-once）。
    """

    def __init__(
        self,
        alert_source,
        queue: IngestQueue,
        level_of: Optional[Callable[[Dict[str, Any]], str]] = None,
        poll_batch: int = 100,
    ):
        self._source = alert_source
        self._queue = queue
        self._level_of = level_of or (lambda raw: raw_level_of(raw))
        self._poll_batch = max(1, poll_batch)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._error_count = 0

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="ingest-worker", daemon=True)
        self._thread.start()
        logger.logger.info("IngestWorker 启动 | queue_size={}", self._queue._maxsize)

    def stop(self):
        self._stop_event.set()
        self._queue.close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        logger.logger.info("IngestWorker 已停止")

    def _run(self):
        """持续消费：Kafka → 有界队列。

        背压策略（FR-1.1）：
        - 队列未满或暂存未满 → 继续拉取；满时按级别分流
          （非 Critical 进风暴暂存，Critical 挤掉最旧非 Critical 或阻塞）
        - 队列与暂存都满 → 不拉取（generator 挂在 yield 上，不 poll，offset 不动）

        修复：看门狗循环 —— 单轮消费异常不再永久退出线程（原 try 包住整个循环，
        任意一次 Kafka 瞬时异常线程即死亡，摄入静默中断）。现在单轮异常记数
        + 指数退避后重建 consumer 继续；连续失败有指标可观测。
        """
        while not self._stop_event.is_set():
            try:
                gen = self._source.consume()
                for raw in gen:
                    if self._stop_event.is_set():
                        return
                    # 背压：队列与暂存都满 → 不拉取（offset 不动，留在 Kafka）
                    while (
                        self._queue.full()
                        and self._queue.storm_parked_full()
                        and not self._stop_event.is_set()
                    ):
                        metrics.incr("aiops_ingest_backpressure_wait")
                        time.sleep(0.05)
                    if self._stop_event.is_set():
                        return
                    if not isinstance(raw, dict):
                        continue
                    level = self._level_of(raw)
                    result = self._queue.put_raw(raw, level)
                    if result == "storm_overflow":
                        # 暂存也满：丢弃但推进 offset（不阻塞前缀提交）
                        self._safe_mark_processed(raw)
                # consume() 正常结束（close/stop）→ 退出
                return
            except Exception as exc:
                self._error_count += 1
                metrics.incr("aiops_ingest.error")
                logger.logger.error(
                    "IngestWorker 消费异常（{}s 后重建 consumer 继续）| error={} | errors={}",
                    min(2 ** min(self._error_count, 5), 30), exc, self._error_count,
                )
                # 指数退避后重建 consumer 继续（consume() finally 已 close 旧实例）
                if self._stop_event.wait(min(2 ** min(self._error_count, 5), 30)):
                    return

    def _safe_mark_processed(self, raw: Dict[str, Any]):
        try:
            if hasattr(self._source, "mark_processed"):
                self._source.mark_processed(raw)
        except Exception:
            pass
