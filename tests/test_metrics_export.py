"""FR-6：Prometheus 指标导出验收测试。

验收标准（改造方案 §2.4 / 规格 FR-6）：
- 现有计数语义保留（pipeline.diagnosis.correct 等 → Prom 指标）
- 映射规则：'.'→'_'，counter 补 _total，前缀 aiops_
- 外部信号 gauge（kafka lag / 降级档）可注册
"""

from prometheus_client import REGISTRY

from core.observability import metrics
from core.metrics_export import PrometheusExporter, _prom_name


class TestPromNameMapping:
    def test_counter_mapping(self):
        """'pipeline.diagnosis.correct' → aiops_pipeline_diagnosis_correct_total。"""
        assert _prom_name("pipeline.diagnosis.correct", "counter") == \
            "aiops_pipeline_diagnosis_correct_total"

    def test_gauge_mapping(self):
        assert _prom_name("pipeline.workers.max") == "aiops_pipeline_workers_max"

    def test_keeps_existing_semantics(self):
        """现有 pipeline.diagnosis.{correct,incorrect,blocked} 计数语义保留。"""
        assert _prom_name("pipeline.diagnosis.incorrect", "counter") == \
            "aiops_pipeline_diagnosis_incorrect_total"
        assert _prom_name("pipeline.diagnosis.blocked", "counter") == \
            "aiops_pipeline_diagnosis_blocked_total"


class TestExporterSync:
    def test_counter_delta_sync(self):
        """counters 差值增量同步（不重复累计）。"""
        name = "test_export_counter_alpha"
        metrics.reset = None  # 现有 MetricsCollector 无 reset；用独立名称
        exporter = PrometheusExporter()
        metrics.incr(name, 3)
        exporter.sync()
        metrics.incr(name, 2)
        exporter.sync()

        prom = exporter._get_counter(name)
        assert prom._value.get() == 5.0  # 3 + 2（差值增量，不重复）

    def test_gauge_sync(self):
        name = "test_export_gauge_alpha"
        exporter = PrometheusExporter()
        metrics.gauge(name, 42)
        exporter.sync()
        prom = exporter._get_gauge(name)
        assert prom._value.get() == 42.0

    def test_gauge_source_registered(self):
        """外部信号 gauge（kafka lag / 降级档）注册与采集。"""
        exporter = PrometheusExporter()
        exporter.register_gauge_source("kafka_consumer_lag", lambda: 123)
        exporter.register_gauge_source("degradation_level", lambda: 1)
        exporter.sync()

        assert exporter._get_gauge("kafka_consumer_lag")._value.get() == 123.0
        assert exporter._get_gauge("degradation_level")._value.get() == 1.0

    def test_gauge_source_error_tolerated(self):
        """采集源异常不中断同步。"""
        exporter = PrometheusExporter()
        exporter.register_gauge_source("bad_source", lambda: 1 / 0)
        exporter.register_gauge_source("good_source", lambda: 7)
        exporter.sync()  # 不抛异常
        assert exporter._get_gauge("good_source")._value.get() == 7.0
