"""Kafka 消息生产者 — 将 Pipeline 处理结果、审计事件等发布到 Kafka Topic。"""

import json
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from core.logger import get_logger
from core.observability import metrics
from interfaces.base import IMessageProducer

logger = get_logger("kafka_producer")


class KafkaMessageProducer(IMessageProducer):
    """Kafka 消息生产者，支持 JSON 序列化和异步发送。"""

    def __init__(
        self,
        bootstrap_servers: str,
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        config = {
            "bootstrap_servers": bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "acks": "all",
            "retries": 3,
            "max_in_flight_requests_per_connection": 1,
        }
        if security_protocol:
            config["security_protocol"] = security_protocol
        if sasl_mechanism:
            config["sasl_mechanism"] = sasl_mechanism
        if sasl_username:
            config["sasl_username"] = sasl_username
        if sasl_password:
            config["sasl_password"] = sasl_password

        self._producer = KafkaProducer(**config)
        logger.info("Kafka 生产者已初始化 | servers={}", bootstrap_servers)

    def publish(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """发布消息到指定 topic。"""
        try:
            future = self._producer.send(topic, key=key, value=value)
            future.get(timeout=10)
            metrics.incr(f"kafka.producer.sent.{topic}")
            logger.debug(
                "[MQ] 消息已发布 | topic={} | key={}", topic, key,
            )
        except KafkaError as exc:
            metrics.incr(f"kafka.producer.error.{topic}")
            logger.error(
                "[MQ] 消息发布失败 | topic={} | key={} | error={}", topic, key, exc,
            )

    def close(self) -> None:
        """关闭生产者。"""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info("Kafka 生产者已关闭")
            except Exception as exc:
                logger.error("关闭 Kafka 生产者异常 | error={}", exc)
            self._producer = None
