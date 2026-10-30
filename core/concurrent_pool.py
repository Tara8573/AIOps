"""并发处理：告警 worker pool 与 LLM 并发限流器。"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Optional

from core.logger import get_logger
from core.observability import metrics
from interfaces.base import ILLMClient

logger = get_logger("concurrent")


class AlertWorkerPool:
    """线程池 worker pool，用于并行处理告警。

    通过 AIOPS_MAX_WORKERS 环境变量配置并发数（默认 4）。
    """

    def __init__(self, max_workers: Optional[int] = None):
        if max_workers is not None:
            self.max_workers = max_workers
        else:
            self.max_workers = int(os.getenv("AIOPS_MAX_WORKERS", "4"))
        self._executor: Optional[ThreadPoolExecutor] = None

    def start(self):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix="alert-worker",
            )
            metrics.gauge("pipeline.workers.max", self.max_workers)
            logger.logger.info("Worker pool 启动 | max_workers={}", self.max_workers)

    def submit(self, fn, *args, **kwargs) -> Future:
        if self._executor is None:
            raise RuntimeError("Worker pool 未启动，请先调用 start()")
        return self._executor.submit(fn, *args, **kwargs)

    def submit_batch(
        self,
        process_fn,
        alerts_with_routes: List[tuple],
    ) -> List[str]:
        """提交一批告警并行处理，阻塞等待全部完成后返回 ticket_id 列表。

        Args:
            process_fn: 处理函数，签名 (alert, route_decision) -> str
            alerts_with_routes: [(alert, route_decision), ...] 已排序的告警列表
        """
        if not alerts_with_routes:
            return []

        futures: List[Future] = []
        for alert, route_decision in alerts_with_routes:
            future = self.submit(process_fn, alert, route_decision=route_decision)
            futures.append(future)

        metrics.gauge("pipeline.workers.submitted", len(futures))

        ticket_ids = []
        for future in futures:
            try:
                tid = future.result()
                if tid:
                    ticket_ids.append(tid)
            except Exception as exc:
                logger.logger.error("Worker 异常: {}", exc)
                metrics.incr("pipeline.workers.error")

        return ticket_ids

    def shutdown(self, wait: bool = True):
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None
            logger.logger.info("Worker pool 已关闭")


class LLMConcurrencyLimiter(ILLMClient):
    """LLM 客户端包装器，用 Semaphore 限制同时进行的 LLM 调用数。

    防止并发 worker 同时打满 LLM API rate limit。
    通过 AIOPS_LLM_MAX_CONCURRENT 配置（默认 4）。
    """

    def __init__(self, wrapped: ILLMClient, max_concurrent: Optional[int] = None):
        self._wrapped = wrapped
        limit = max_concurrent if max_concurrent is not None else int(
            os.getenv("AIOPS_LLM_MAX_CONCURRENT", "4")
        )
        self._semaphore = threading.Semaphore(limit if limit > 0 else 999)
        logger.logger.info("LLM 并发限制: max_concurrent={}", limit)

    def generate_proposal(self, prompt: str) -> str:
        with self._semaphore:
            return self._wrapped.generate_proposal(prompt)
