"""FR-7：配置热更新验收测试。

验收标准（改造方案 §3.2 / 规格 FR-7）：
- 修改 min_level 过滤阈值，不重启进程即生效
- patterns 带 config version 缓存重编译（version 不变零开销）
- 热开关（degradation.forced_level）可手动切换降级档
"""

import time

from core.degradation import DegradationController, L1, L2, NORMAL
from core.processor import AlertFilter


class FakeRuntimeConfig:
    """模拟 runtime_config 提供者（version + 快照）。"""

    def __init__(self, values=None, version=0):
        self._values = dict(values or {})
        self._version = version
        self.reads = 0

    def version(self):
        self.reads += 1
        return self._version

    def get(self, key, default=None):
        return self._values.get(key, default)

    def update(self, values, version=None):
        self._values.update(values)
        self._version = version if version is not None else self._version + 1


class TestHotFilterConfig:
    def test_min_level_hot_reload(self):
        """运行中修改 min_level → 不重启生效（Info 告警被过滤）。"""
        provider = FakeRuntimeConfig({"kafka.min_level": "Info"}, version=1)
        f = AlertFilter(min_level="Info", config_provider=provider)
        alert = _alert()

        assert f.should_process(alert) is True  # Info 放行

        # 热更新：min_level → Warning（Info 被过滤）
        provider.update({"kafka.min_level": "Warning"})
        assert f.should_process(alert) is False  # 不重启即生效
        assert f.min_level == "Warning"

    def test_patterns_hot_reload(self):
        """include/exclude patterns 热更新 + 重编译。"""
        provider = FakeRuntimeConfig({"kafka.include_patterns": ""}, version=1)
        f = AlertFilter(min_level="Info", config_provider=provider)
        alert = _alert()

        assert f.should_process(alert) is True

        # 热更新：新增 include 规则（不命中 → 过滤）
        provider.update({"kafka.include_patterns": "disk|memory"})
        assert f.should_process(alert) is False
        assert f.include_patterns  # 已重编译

    def test_version_unchanged_zero_recompile(self):
        """version 未变：不重编译（零开销路径）。"""
        provider = FakeRuntimeConfig({"kafka.min_level": "Info"}, version=5)
        f = AlertFilter(min_level="Info", config_provider=provider)
        f.should_process(_alert())   # 首次 reload（version 5）
        assert f._config_version == 5

        f.min_level = "Critical"     # 手工改（模拟外部状态）
        provider._values = {}        # 清空 provider 值
        f.should_process(_alert())   # version 未变 → 不 reload → min_level 保持手工值
        assert f.min_level == "Critical"

    def test_missing_keys_keep_current(self):
        """provider 缺失键 → 保持当前值（yaml 初始值不被清空）。"""
        provider = FakeRuntimeConfig({}, version=3)
        f = AlertFilter(min_level="Warning", config_provider=provider)
        f.should_process(_alert())   # 触发 reload
        assert f.min_level == "Warning"  # 缺失键保持构造值

    def test_no_provider_zero_overhead(self):
        """无 provider：should_process 行为不变（回归保护）。"""
        f = AlertFilter(min_level="Info")
        assert f.should_process(_alert()) is True


class TestHotDegradationSwitch:
    def test_forced_level_hot_switch(self):
        """热开关（degradation.forced_level）不重启切档。"""
        provider = FakeRuntimeConfig({"degradation.forced_level": ""}, version=1)
        ctrl = DegradationController(config_provider=provider)
        ctrl.refresh()
        assert ctrl.level == NORMAL

        # 热开关切 L2
        provider.update({"degradation.forced_level": "L2"})
        assert ctrl.refresh() == L2
        assert ctrl.is_l2() is True

    def test_forced_level_release_via_config(self):
        """热开关释放（空值）→ 恢复自动检测（迟滞：连续 3 个低信号周期）。"""
        provider = FakeRuntimeConfig({"degradation.forced_level": "L2"}, version=1)
        ctrl = DegradationController(config_provider=provider)
        assert ctrl.refresh() == L2

        provider._values = {"degradation.forced_level": ""}
        for _ in range(4):   # 释放 + 3 个恢复周期 → 逐级回退 L2→L1
            ctrl.refresh()
        assert ctrl.level == L1
        for _ in range(3):   # 再 3 个周期 → L1→NORMAL
            ctrl.refresh()
        assert ctrl.level == NORMAL  # 自动恢复全量模式（规格 4.2）


def _alert():
    from core.models import Alert
    return Alert(
        alert_id="ALT-HOT-1",
        title="磁盘 /var 使用率超过 90%",
        level="Info",
        content="host 10.1.2.3 /var usage 95%",
    )
