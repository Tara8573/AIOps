"""审批恢复线程（FR-3 人工确认异步化）。

每实例一个后台线程，每 interval 秒：
  1) expire_overdue()  超时审批置 expired（规格：超时默认拒绝并转工单）
  2) take_decided()    原子领取已决策未恢复的审批（SKIP LOCKED，多实例不重复捡起）
  3) 逐个 Command(resume=decision) 恢复挂起的 pipeline

checkpoint 在共享 PG 中，任何实例都能恢复任何实例挂起的 pipeline（天然 HA）。
"""

import os
import socket
import threading
from typing import Optional

from core.logger import get_logger
from core.observability import metrics
from interfaces.base import IApprovalRequestStore

logger = get_logger("approval_resumer")

# 已决策状态 → resume 值映射（approve 节点内处理）
DECISION_TO_RESUME = {
    "approved": "approved",
    "rejected": "rejected",
    "expired": "timeout",
}


class ApprovalResumer:
    """审批恢复线程。"""

    def __init__(
        self,
        pipeline,
        store: IApprovalRequestStore,
        interval: Optional[float] = None,
        instance_id: Optional[str] = None,
    ):
        self._pipeline = pipeline
        self._store = store
        self._interval = interval if interval is not None else float(
            os.getenv("AIOPS_APPROVAL_RESUME_INTERVAL", "2")
        )
        self._instance_id = instance_id or f"{socket.gethostname()}-{os.getpid()}"
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="approval-resumer", daemon=True,
        )
        self._thread.start()
        metrics.gauge("pipeline.approval.resumer_running", 1)
        logger.logger.info(
            "ApprovalResumer 启动 | interval={}s | instance={}", self._interval, self._instance_id,
        )

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        metrics.gauge("pipeline.approval.resumer_running", 0)
        logger.logger.info("ApprovalResumer 已停止")

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self.poll_once()
            except Exception as exc:
                metrics.incr("pipeline.approval.resumer_error")
                logger.logger.error("ApprovalResumer 轮询异常: {}", exc)
            self._stop_event.wait(self._interval)

    def poll_once(self) -> int:
        """单轮扫描：超时 sweeper + 领取已决策 + 恢复。返回本轮恢复数。"""
        # 1) 超时审批置 expired（下一 take_decided 即领取）
        try:
            overdue = self._store.expire_overdue()
            if overdue:
                metrics.incr("pipeline.approval.expired", len(overdue))
                for record in overdue:
                    logger.logger.warning(
                        "[审批] 超时自动拒绝 | approval_id={} | alert_id={}",
                        record.approval_id, record.alert_id,
                    )
        except Exception as exc:
            metrics.incr("pipeline.approval.resumer_error")
            logger.logger.error("expire_overdue 异常: {}", exc)

        # 2) 原子领取已决策未恢复的审批
        try:
            records = self._store.take_decided(limit=10, instance_id=self._instance_id)
        except Exception as exc:
            metrics.incr("pipeline.approval.resumer_error")
            logger.logger.error("take_decided 异常: {}", exc)
            return 0

        resumed = 0
        for record in records:
            decision = DECISION_TO_RESUME.get(record.status, "rejected")
            if self._resume_one(record, decision):
                resumed += 1
        return resumed

    def _resume_one(self, record, decision: str) -> bool:
        """恢复一条审批对应的挂起 pipeline。"""
        from core.pipeline_graph import resume_pipeline_with_decision

        result = resume_pipeline_with_decision(self._pipeline, record.pipeline_run_id, decision)
        if result is None:
            # 无 checkpointer / 无 checkpoint / 恢复异常 → 释放待下轮重试
            self._store.mark_resumed(record.approval_id, self._instance_id, success=False)
            metrics.incr("pipeline.approval.resume_failed")
            logger.logger.warning(
                "[审批] 恢复失败，待重试 | approval_id={} | run_id={}",
                record.approval_id, record.pipeline_run_id,
            )
            return False

        self._store.mark_resumed(record.approval_id, self._instance_id, success=True)
        metrics.incr("pipeline.approval.resumed")
        logger.logger.info(
            "[审批] 恢复执行 ✓ | approval_id={} | run_id={} | decision={}",
            record.approval_id, record.pipeline_run_id, decision,
        )
        return True
