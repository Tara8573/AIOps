import json
import threading
from typing import Dict, Any, Generator, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from core.logger import get_logger
from core.observability import metrics
from interfaces.base import IAlertSource

logger = get_logger("kafka_source")


class KafkaAlertSource(IAlertSource):
    """Kafka 告警数据源：从指定 Topic 持续消费原始告警消息。"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "aiops-consumer",
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = False,
        poll_timeout_ms: int = 1000,
        max_poll_records: int = 100,
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._poll_timeout_ms = poll_timeout_ms
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._max_poll_records = max_poll_records
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password

        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._stop_event = threading.Event()
        # ── at-least-once: 待处理/已处理 offset 跟踪 ──
        # commit 只推进到"已处理完成"的 offset，未处理的消息崩溃后可重放。
        self._pending: Dict[Any, list] = {}          # tp -> 已 yield 但未确认处理的 offset 列表(升序)
        self._processed: Dict[Any, set] = {}          # tp -> 已确认处理的 offset 集合
        self._commit_lock = threading.Lock()

    def _create_consumer(self) -> KafkaConsumer:
        config = {
            "bootstrap_servers": self._bootstrap_servers,
            "group_id": self._group_id,
            "auto_offset_reset": self._auto_offset_reset,
            "enable_auto_commit": self._enable_auto_commit,
            "max_poll_records": self._max_poll_records,
            "consumer_timeout_ms": self._poll_timeout_ms,
            "value_deserializer": self._deserialize,
        }
        if self._security_protocol:
            config["security_protocol"] = self._security_protocol
        if self._sasl_mechanism:
            config["sasl_mechanism"] = self._sasl_mechanism
        if self._sasl_username:
            config["sasl_username"] = self._sasl_username
        if self._sasl_password:
            config["sasl_password"] = self._sasl_password

        consumer = KafkaConsumer(self._topic, **config)
        logger.info(
            "Kafka 消费者已创建 | servers={} | topic={} | group={}",
            self._bootstrap_servers,
            self._topic,
            self._group_id,
        )
        return consumer

    @staticmethod
    def _deserialize(raw_bytes: bytes) -> Optional[Dict[str, Any]]:
        if raw_bytes is None:
            return None
        try:
            return json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("消息反序列化失败 | error={}", exc)
            return None

    def consume(self) -> Generator[Dict[str, Any], None, None]:
        """消费消息。offset 不在此处 commit —— 仅在 yield 前登记 pending，
        由调用方处理完成后调用 mark_processed() 推进 commit 点（at-least-once）。"""
        self._consumer = self._create_consumer()
        self._running = True
        logger.info("开始消费 Kafka 告警消息...")

        try:
            while self._running and not self._stop_event.is_set():
                records = self._consumer.poll(timeout_ms=self._poll_timeout_ms)
                for tp, messages in records.items():
                    for msg in messages:
                        raw = msg.value
                        if raw is None:
                            # 修复：poison 消息（反序列化失败）显式登记 pending +
                            # processed —— commit 连续前缀确定性地越过该 offset，
                            # 不再隐式丢失且可观测（原来靠后续消息隐式越过）
                            with self._commit_lock:
                                self._pending.setdefault(tp, []).append(msg.offset)
                                self._processed.setdefault(tp, set()).add(msg.offset)
                            metrics.incr("kafka.consumer.poison_skipped")
                            logger.warning(
                                "poison 消息跳过（反序列化失败）| topic={} | partition={} | offset={}",
                                tp.topic, tp.partition, msg.offset,
                            )
                            continue
                        with self._commit_lock:
                            self._pending.setdefault(tp, []).append(msg.offset)
                        # 注入分区/offset 元信息，供处理完成后 mark_processed 使用
                        if isinstance(raw, dict):
                            raw["__kafka_tp"] = tp
                            raw["__kafka_offset"] = msg.offset
                        yield raw
                # 每批 poll 后尝试推进 commit（只提交已处理完成的连续前缀）
                if not self._enable_auto_commit:
                    self.commit_processed()

        except KafkaError as exc:
            logger.error("Kafka 消费异常 | error={}", exc)
            raise
        finally:
            self.close()


    def mark_processed(self, raw_msg: Dict[str, Any]) -> None:
        """标记一条消息已处理完成。raw_msg 需携带 __kafka_tp/__kafka_offset 元信息
        （由 consumption 层在消费时注入）；缺省时退化为按分区推进到最小 pending。"""
        tp = raw_msg.get("__kafka_tp")
        off = raw_msg.get("__kafka_offset")
        with self._commit_lock:
            if tp is not None and off is not None:
                self._processed.setdefault(tp, set()).add(off)

    def commit_processed(self) -> None:
        """把每个分区"已处理完成的连续前缀"提交为 commit offset。"""
        if self._consumer is None:
            return
        from kafka.structs import OffsetAndMetadata, TopicPartition
        offsets = {}
        with self._commit_lock:
            for tp, pend in list(self._pending.items()):
                if not pend:
                    continue
                done = self._processed.get(tp, set())
                # 连续前缀：pend 升序，从头起凡在 done 中即可推进
                adv = 0
                for o in pend:
                    if o in done:
                        adv = o + 1
                    else:
                        break
                if adv > 0:
                    offsets[TopicPartition(tp.topic, tp.partition)] = OffsetAndMetadata(adv, "")
                    # 清理已提交的
                    self._pending[tp] = [o for o in pend if o >= adv]
                    self._processed[tp] = {o for o in done if o >= adv}
        if offsets:
            try:
                self._consumer.commit(offsets=offsets)
                metrics.incr("kafka.consumer.committed")
            except KafkaError as exc:
                logger.warning("Kafka offset commit 失败 | error={}", exc)

    def lag(self) -> int:
        """消费滞后：各已分配分区 end_offset - committed 之和（FR-1.3 降级信号 / HPA 指标）。

        无 consumer 返回 -1（未知）；查询失败返回 -1。
        """
        if self._consumer is None:
            return -1
        try:
            partitions = list(self._consumer.assignment() or [])
            if not partitions:
                return 0
            end = self._consumer.end_offsets(partitions)
            total = 0
            for tp in partitions:
                committed = self._consumer.committed(tp)
                base = committed.offset if committed is not None else 0
                total += max(0, end.get(tp, 0) - base)
            return total
        except Exception as exc:
            logger.warning("查询 Kafka lag 失败 | error={}", exc)
            return -1

    def close(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._consumer is not None:
            try:
                self._consumer.close()
                logger.info("Kafka 消费者已关闭")
            except Exception as exc:
                logger.error("关闭 Kafka 消费者异常 | error={}", exc)
            self._consumer = None
