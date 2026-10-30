"""远程执行器（FR-5）：脚本在目标设备/机房侧执行，而非 AIOps 机器本地。

MQ 模式（设备侧 agent，主方案 —— 复用现有 Kafka 基建，零新增中间件）:

  AIOps 实例 ──► Kafka aiops-exec-requests ──► 设备侧 aiops-exec-agent ──► 目标主机执行
  AIOps 实例 ◄── Kafka aiops-exec-results  ◄── agent（结果回传，request_id 关联）

- 同主机并发互斥: Redis 锁 lock:host:{host}（多实例下也成立）
- 超时: AIOPS_EXEC_TIMEOUT（默认 300s）；超时/失败 → 上层既有路径转工单
- 结果消费: 独立 consumer + 独立 group（不与告警消费相互影响）
- 本地 DockerSandbox 保留为 demo/测试实现（AIOPS_EXECUTOR_MODE=docker）

协议契约见 docs/executor-agent-协议.md。
"""

import os
import time
import uuid
from typing import Any, Dict, Optional

from core.host_lock import HostLock
from core.logger import get_logger
from core.observability import metrics
from interfaces.base import IExecutor

logger = get_logger("remote_executor")

EXEC_REQUEST_TOPIC = "aiops-exec-requests"
EXEC_RESULT_TOPIC = "aiops-exec-results"

# 危险命令正则硬拦截（规格 4.3：不可降级）—— 与沙箱 preflight 同款意图
DANGEROUS_COMMAND_PATTERNS = [
    r"rm\s+-rf\s+/",
    r"mkfs(\.\w+)?\s",
    r"dd\s+if=/dev/(zero|random)\s+of=/dev/",
    r":\(\)\{\s*:\|:&\s*\};:",          # fork bomb
    r"shutdown|reboot|init\s+[06]\b",
    r">\s*/dev/sd[a-z]",
]


def extract_host_from_context(context: Dict[str, Any]) -> str:
    """从执行上下文提取目标主机（alert dump 的 host/raw_data.host，或正文正则）。"""
    alert = context.get("alert") or {}
    raw = alert.get("raw_data") or {}
    host = raw.get("host") or alert.get("host") or ""
    if host:
        return str(host)
    # 正文提取：IP 或 host= 模式（与 _extract_host 同款意图）
    import re
    text = f"{alert.get('title', '')} {alert.get('content', '')}"
    ip = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", text)
    if ip:
        return ip.group(1)
    m = re.search(r"(?:host|node|server)\s*[:=]?\s*(\S+)", text, re.IGNORECASE)
    return m.group(1) if m else ""


class RemoteExecutor(IExecutor):
    """MQ 模式远程执行器。"""

    def __init__(
        self,
        producer=None,
        redis_client=None,
        timeout_seconds: Optional[int] = None,
        bootstrap_servers: Optional[str] = None,
        result_consumer=None,
    ):
        self._producer = producer
        self._timeout = timeout_seconds if timeout_seconds is not None else int(
            os.getenv("AIOPS_EXEC_TIMEOUT", "300"))
        self._bootstrap = bootstrap_servers or os.getenv("AIOPS__KAFKA__BOOTSTRAP_SERVERS",
                                                         os.getenv("AIOPS_KAFKA_BOOTSTRAP", "localhost:9092"))
        # 结果消费: 测试可注入 result_consumer；生产用独立 Kafka consumer + 独立 group
        # 修复：结果 consumer 实例级复用（原每次执行新建随机 group consumer，
        # publish 与 consumer 创建之间的竞窗内到达的结果被 latest 跳过 →
        # 远程成功被误判超时转工单；且每执行一次产生 group churn）
        self._result_consumer = result_consumer
        self._own_consumer = None  # 实例级自建 consumer（复用）
        # 主机互斥锁
        from plugins.redis_client import get_redis
        self._host_lock = HostLock(redis_client if redis_client is not None else get_redis())

    def close(self):
        """关闭实例级结果 consumer（进程退出/测试清理用）。"""
        if self._own_consumer is not None:
            try:
                self._own_consumer.close()
            except Exception:
                pass
            self._own_consumer = None

    # ── IExecutor ──

    def execute_script(self, script_content: str, context: dict) -> bool:
        """远程执行修复脚本：加锁 → 发请求 → 等结果 → 释放。"""
        host = extract_host_from_context(context)
        alert_id = (context.get("alert") or {}).get("alert_id", "")

        # 高危命令硬拦截（规格 4.3：不可降级）
        violations = self._dangerous_check(script_content)
        if violations:
            metrics.incr("pipeline.executor.dangerous_blocked")
            logger.logger.error(
                "[远程执行] 高危命令拦截 ✗ | alert_id={} | host={} | 命中={}",
                alert_id, host, violations,
            )
            return False

        # 1) 同主机互斥（多实例下也成立）
        token = self._host_lock.acquire(host, self._timeout)
        if token is None:
            metrics.incr("pipeline.executor.host_busy")
            logger.logger.warning(
                "[远程执行] 同主机已有执行（互斥拒绝）| alert_id={} | host={}", alert_id, host,
            )
            return False

        try:
            return self._execute_via_mq(script_content, context, host, alert_id)
        finally:
            self._host_lock.release(host, token)

    # ── 内部 ──

    def _execute_via_mq(self, script_content: str, context: dict, host: str, alert_id: str) -> bool:
        request_id = f"exec-{uuid.uuid4().hex[:12]}"
        payload = {
            "request_id": request_id,
            "alert_id": alert_id,
            "host": host,
            "script": script_content,
            "timeout_seconds": self._timeout,
            "requested_by": os.getenv("AIOPS_INSTANCE_ID", "aiops"),
            "created_at": time.time(),
        }

        if self._producer is None:
            metrics.incr("pipeline.executor.no_producer")
            logger.logger.error("[远程执行] 未配置消息生产者，无法下发 | alert_id={}", alert_id)
            return False

        # 修复：先建/复用结果 consumer 再 publish —— publish 与 consumer 创建
        # 之间的竞窗内到达的结果不会被 latest 跳过。
        # consumer 创建失败仅告警不阻断（与原行为一致：_wait_result 返回 None → 超时转工单）
        if self._result_consumer is None:
            self._ensure_own_consumer()

        try:
            self._producer.publish(topic=EXEC_REQUEST_TOPIC, key=host, value=payload)
        except Exception as exc:
            metrics.incr("pipeline.executor.publish_failed")
            logger.logger.error(
                "[远程执行] 执行请求发布失败 | alert_id={} | error={}", alert_id, exc,
            )
            return False
        metrics.incr("pipeline.executor.request_sent")

        # 3) 等结果（request_id 关联，超时处理）
        result = self._wait_result(request_id, self._timeout)
        if result is None:
            metrics.incr("pipeline.executor.timeout")
            logger.logger.warning(
                "[远程执行] 执行超时 ✗ | alert_id={} | request_id={} | timeout={}s",
                alert_id, request_id, self._timeout,
            )
            return False

        success = bool(result.get("success"))
        if success:
            metrics.incr("pipeline.executor.remote_success")
            logger.logger.info(
                "[远程执行] 执行成功 ✓ | alert_id={} | request_id={} | exit_code={}",
                alert_id, request_id, result.get("exit_code"),
            )
        else:
            metrics.incr("pipeline.executor.remote_failed")
            logger.logger.warning(
                "[远程执行] 执行失败 ✗ | alert_id={} | request_id={} | stderr={}",
                alert_id, request_id, str(result.get("stderr", ""))[:200],
            )
        return success

    def _wait_result(self, request_id: str, timeout_seconds: int) -> Optional[Dict[str, Any]]:
        """等待执行结果（request_id 关联）。

        测试注入 result_consumer；生产用实例级复用的 Kafka consumer（独立 group，
        auto_offset_reset=latest，publish 前已创建 —— 结果不会因竞窗被 latest 跳过）。
        """
        consumer = self._result_consumer
        if consumer is None:
            consumer = self._ensure_own_consumer()
        if consumer is None:
            return None

        deadline = time.time() + timeout_seconds
        try:
            while time.time() < deadline:
                batch = consumer.poll(timeout_ms=500)
                for _tp, messages in batch.items():
                    for msg in messages:
                        value = msg.value
                        if isinstance(value, bytes):
                            import json
                            try:
                                value = json.loads(value.decode("utf-8"))
                            except Exception:
                                continue
                        if not isinstance(value, dict):
                            continue
                        if value.get("request_id") == request_id:
                            return value
            return None
        except Exception:
            # consumer 异常（连接断开等）：关闭重建，下轮执行时恢复
            self.close()
            raise

    def _ensure_own_consumer(self):
        """实例级结果 consumer（懒创建，复用；publish 前调用确保不漏结果）。"""
        if self._own_consumer is not None:
            return self._own_consumer
        self._own_consumer = self._build_result_consumer()
        return self._own_consumer

    def _build_result_consumer(self):
        try:
            from kafka import KafkaConsumer
            return KafkaConsumer(
                EXEC_RESULT_TOPIC,
                bootstrap_servers=self._bootstrap,
                group_id=f"aiops-exec-results-{uuid.uuid4().hex[:8]}",  # 每实例独立 group（实例级复用）
                auto_offset_reset="latest",
                consumer_timeout_ms=500,
                value_deserializer=lambda b: b,
            )
        except Exception as exc:
            metrics.incr("pipeline.executor.result_consumer_failed")
            logger.logger.warning("[远程执行] 结果消费者创建失败 | error={}", exc)
            return None

    def _dangerous_check(self, script: str):
        """高危命令正则硬拦截（规格 4.3：不可降级）。"""
        import re
        issues = []
        for pattern in DANGEROUS_COMMAND_PATTERNS:
            if re.search(pattern, script, re.IGNORECASE):
                issues.append(pattern)
        return issues
