import threading
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from statistics import mean
from typing import Any, DefaultDict, Dict, Iterator, List, Optional


class PipelineStateTracker:
    """Tracks latest processing state for each alert."""

    def __init__(self):
        self._states: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def update(self, alert_id: str, state: str, **extra: Any) -> None:
        payload = {"state": state, "updated_at": time.time(), **extra}
        with self._lock:
            self._states[alert_id] = payload

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._states)


class MetricsCollector:
    """In-process metrics collector for pipeline and plugin hotspots."""

    def __init__(self):
        self._counters: Counter[str] = Counter()
        self._latencies: DefaultDict[str, List[float]] = defaultdict(list)
        self._gauges: Dict[str, float] = {}
        self._lock = threading.Lock()

    def incr(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] += value

    def observe(self, name: str, value_ms: float) -> None:
        with self._lock:
            self._latencies[name].append(value_ms)

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    @contextmanager
    def timer(
        self,
        name: str,
        success_counter: Optional[str] = None,
        failure_counter: Optional[str] = None,
    ) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
            duration = (time.perf_counter() - start) * 1000
            self.observe(name, duration)
            if success_counter:
                self.incr(success_counter)
        except Exception:
            duration = (time.perf_counter() - start) * 1000
            self.observe(name, duration)
            if failure_counter:
                self.incr(failure_counter)
            raise

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            latency_stats = {}
            for name, values in self._latencies.items():
                if not values:
                    continue
                latency_stats[name] = {
                    "count": len(values),
                    "avg_ms": round(mean(values), 2),
                    "max_ms": round(max(values), 2),
                }
            return {
                "counters": dict(self._counters),
                "latencies": latency_stats,
                "gauges": dict(self._gauges),
            }


metrics = MetricsCollector()
pipeline_states = PipelineStateTracker()
