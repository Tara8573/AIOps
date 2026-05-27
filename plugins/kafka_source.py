import json
import threading
from typing import Dict, Any, Generator, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from core.logger import get_logger
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
        enable_auto_commit: bool = True,
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
                            continue
                        yield raw
        except KafkaError as exc:
            logger.error("Kafka 消费异常 | error={}", exc)
            raise
        finally:
            self.close()

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
