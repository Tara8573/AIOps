"""Prometheus 指标导出（FR-6）。

- prometheus_client 的 start_http_server 暴露 /metrics（AIOPS_METRICS_PORT，默认 9090）
- 现有 MetricsCollector 点名按映射规则转为 Prom 指标（保留语义，零侵入现有打点代码）:
    counters  "pipeline.diagnosis.correct" → Counter aiops_pipeline_diagnosis_correct_total
    gauges    "pipeline.workers.max"       → Gauge   aiops_pipeline_workers_max
    latencies "pipeline.eval_loop.ms"      → Gauge   aiops_pipeline_eval_loop_ms_avg/max
- 外部信号 gauge（kafka lag / 降级档 / worker 积压 / 审批挂起）经 register_gauge_source 注册
- 定期（10s）从 metrics.snapshot() 同步到 Prom 注册表（多副本 Grafana 聚合全局视图）
"""

import os
import threading
from typing import Callable, Dict, Optional

from core.observability import metrics, pipeline_states
from core.logger import get_logger

logger = get_logger("metrics_export")

_PROM_PREFIX = "aiops_"


def _prom_name(name: str, kind: str = "gauge") -> str:
    """现有 MetricsCollector 点名 → Prom 指标名（'.'→'_'，counter 补 _total）。"""
    n = _PROM_PREFIX + name.replace(".", "_").replace("-", "_").lower()
    if kind == "counter" and not n.endswith("_total"):
        n += "_total"
    return n


class PrometheusExporter:
    """Prometheus 导出器：同步现有 MetricsCollector + 外部信号 gauge。"""

    def __init__(self, port: Optional[int] = None, interval: float = 10.0):
        self._port = port if port is not None else int(os.getenv("AIOPS_METRICS_PORT", "9090"))
        self._interval = interval
        # 独立 registry（隔离测试与多实例）
        from prometheus_client import CollectorRegistry
        self._registry = CollectorRegistry()
        self._counters: Dict[str, object] = {}
        self._gauges: Dict[str, object] = {}
        self._last_counters: Dict[str, int] = {}
        self._gauge_sources: Dict[str, Callable[[], float]] = {}
        self._thread: Optional[threading.Thread] = None
        self._server = None
        self._stop_event = threading.Event()
        # 修复：RLock（sync 全程持锁，_get_counter/_get_gauge 可重入；
        # 原 sync 无锁，/metrics 请求与 10s 后台线程并发调用会丢/重计数）
        self._lock = threading.RLock()

    # ── Prom 对象注册（懒创建，防重名）──

    def _get_counter(self, name: str):
        with self._lock:
            if name not in self._counters:
                from prometheus_client import Counter as PromCounter
                prom_name = _prom_name(name, "counter")
                self._counters[name] = PromCounter(
                    prom_name, f"AIOps counter {name}", registry=self._registry,
                )
            return self._counters[name]

    def _get_gauge(self, name: str):
        with self._lock:
            if name not in self._gauges:
                from prometheus_client import Gauge as PromGauge
                prom_name = _prom_name(name, "gauge")
                self._gauges[name] = PromGauge(
                    prom_name, f"AIOps gauge {name}", registry=self._registry,
                )
            return self._gauges[name]

    def register_gauge_source(self, name: str, fn: Callable[[], float]):
        """注册外部信号 gauge 采集源（kafka lag / 降级档 / 积压等）。"""
        self._gauge_sources[name] = fn

    # ── 同步 ──

    def sync(self):
        """一轮同步：counters（差值增量）/ gauges / 延迟统计 / 外部信号源 / 告警状态分布。

        全程持 RLock：/metrics scrape 与后台同步线程并发调用时 _last_counters
        读改不竞态（修复计数重复/丢失）。
        """
        with self._lock:
            snap = metrics.snapshot()

            for name, value in snap.get("counters", {}).items():
                prom = self._get_counter(name)
                last = self._last_counters.get(name, 0)
                delta = value - last
                if delta > 0:
                    prom.inc(delta)
                self._last_counters[name] = value

            for name, value in snap.get("gauges", {}).items():
                self._get_gauge(name).set(value)

            for name, stats in snap.get("latencies", {}).items():
                self._get_gauge(f"{name}.avg").set(stats.get("avg_ms", 0))
                self._get_gauge(f"{name}.max").set(stats.get("max_ms", 0))

            for name, fn in self._gauge_sources.items():
                try:
                    self._get_gauge(name).set(fn())
                except Exception as exc:
                    logger.logger.warning("gauge 采集源异常 | name={} | error={}", name, exc)

            # 告警状态分布（PipelineStateTracker）
            try:
                distribution: Dict[str, int] = {}
                for st in pipeline_states.snapshot().values():
                    state_name = st.get("state", "unknown")
                    distribution[state_name] = distribution.get(state_name, 0) + 1
                for state_name, count in distribution.items():
                    self._get_gauge(f"pipeline.alerts_state.{state_name}").set(count)
            except Exception:
                pass

    # ── 服务 ──

    def start(self):
        """启动 /metrics + /healthz HTTP 服务 + 同步线程。"""
        if self._server is not None:
            return
        from http.server import BaseHTTPRequestHandler, HTTPServer
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        exporter = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = self.path.rstrip("/")
                if path == "/metrics":
                    try:
                        exporter.sync()
                    except Exception:
                        pass
                    body = generate_latest(exporter._registry)
                    self.send_response(200)
                    self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                    self.end_headers()
                    self.wfile.write(body)
                elif path == "/healthz":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, *args):
                pass

        self.sync()
        self._server = HTTPServer(("0.0.0.0", self._port), Handler)
        threading.Thread(target=self._server.serve_forever, name="metrics-http", daemon=True).start()
        self._thread = threading.Thread(target=self._run, name="metrics-export", daemon=True)
        self._thread.start()
        logger.logger.info("Prometheus /metrics + /healthz 已启动 | port={}", self._port)

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self.sync()
            except Exception as exc:
                logger.logger.error("metrics 同步异常: {}", exc)
            self._stop_event.wait(self._interval)

    def stop(self):
        self._stop_event.set()
        if self._server:
            try:
                self._server.shutdown()
            except Exception:
                pass
            self._server = None


def start_metrics_server(pipeline=None, port: Optional[int] = None) -> Optional[PrometheusExporter]:
    """启动指标导出（AIOPS_METRICS_ENABLED=1 时），并注册运行时信号 gauge。"""
    if os.getenv("AIOPS_METRICS_ENABLED", "0") != "1":
        return None
    exporter = PrometheusExporter(port=port)
    if pipeline is not None:
        _register_runtime_gauges(exporter, pipeline)
    exporter.start()
    return exporter


def _register_runtime_gauges(exporter: PrometheusExporter, pipeline):
    """运行时信号 gauge 采集源（kafka lag / 降级档 / worker 积压 / 审批挂起 / LLM 配额）。"""
    exporter.register_gauge_source(
        "kafka_consumer_lag",
        lambda: (
            pipeline.alert_source.lag()
            if pipeline.alert_source is not None and hasattr(pipeline.alert_source, "lag")
            else -1
        ),
    )
    degradation = getattr(pipeline, "_degradation", None)
    if degradation is not None:
        exporter.register_gauge_source(
            "degradation_level",
            lambda: {"NORMAL": 0, "L1": 1, "L2": 2}.get(
                degradation._forced_level or degradation.level, 0
            ),
        )
    queue = getattr(pipeline, "_ingest_queue", None)
    if queue is not None:
        exporter.register_gauge_source(
            "worker_backlog",
            lambda: queue.qsize() + queue.storm_parked_size(),
        )
    approval_store = getattr(pipeline, "approval_store", None)
    if approval_store is not None:
        exporter.register_gauge_source(
            "approval_pending",
            lambda: len(approval_store.list_by_status("pending", limit=1000)),
        )
