"""FR-4：pipeline_runs 登记表单元测试（InMemory 实现）。"""

from time import sleep

from core.pipeline_run_registry import InMemoryRunRegistry


class TestInMemoryRunRegistry:
    def test_register_and_find_open_run(self):
        reg = InMemoryRunRegistry()
        assert reg.find_open_run("ALT-1") is None

        reg.register("run-a", "ALT-1", "running")
        run = reg.find_open_run("ALT-1")
        assert run is not None
        assert run.run_id == "run-a"
        assert run.status == "running"

    def test_register_upsert_idempotent(self):
        """register 幂等（upsert），重复登记不产生新记录。"""
        reg = InMemoryRunRegistry()
        reg.register("run-a", "ALT-1", "running")
        reg.register("run-a", "ALT-1", "running")
        assert len(reg.all_runs()) == 1

    def test_update_status_finalizes(self):
        reg = InMemoryRunRegistry()
        reg.register("run-a", "ALT-1", "running")
        reg.update_status("run-a", "completed")
        assert reg.find_open_run("ALT-1") is None
        assert reg.all_runs()[0].status == "completed"

    def test_suspended_excluded_from_default_open_run(self):
        """suspended 不被新告警复用（suspended 只由审批恢复入口唤醒）。"""
        reg = InMemoryRunRegistry()
        reg.register("run-a", "ALT-1", "suspended")
        assert reg.find_open_run("ALT-1") is None
        # include_suspended=True 时可见（审批恢复入口用）
        assert reg.find_open_run("ALT-1", include_suspended=True) is not None

    def test_find_open_run_latest_wins(self):
        reg = InMemoryRunRegistry()
        reg.register("run-a", "ALT-1", "running")
        sleep(0.01)
        reg.register("run-b", "ALT-1", "running")
        assert reg.find_open_run("ALT-1").run_id == "run-b"

    def test_find_stale_runs(self):
        reg = InMemoryRunRegistry()
        reg.register("run-stale", "ALT-1", "running")
        reg.register("run-fresh", "ALT-2", "running")
        reg.register("run-done", "ALT-3", "completed")

        # 把 run-stale 的 updated_at 拨回过去
        stale = reg.all_runs()[0]
        from datetime import datetime, timedelta
        for r in reg.all_runs():
            if r.run_id == "run-stale":
                r.updated_at = datetime.now() - timedelta(seconds=3600)

        stale_runs = reg.find_stale_runs(stale_after_seconds=600)
        assert [r.run_id for r in stale_runs] == ["run-stale"]
