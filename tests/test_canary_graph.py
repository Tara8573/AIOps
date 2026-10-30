"""Tests for canary execution graph."""

import pytest
from unittest.mock import Mock, patch

from core.models import Alert, LLMProposal, ActionPlan
from core.canary_graph import (
    build_canary_graph,
    extract_hosts_from_alert,
    CanaryExecutionState,
)


class TestExtractHostsFromAlert:
    """测试从告警中提取主机列表。"""

    def test_extract_ip_list(self):
        """测试提取 IP 地址列表。"""
        alert = Alert(
            alert_id="ALT-001",
            title="Disk Space Critical",
            level="Critical",
            content="affected hosts: 10.10.1.1, 10.10.1.2, 10.10.1.3",
        )
        hosts = extract_hosts_from_alert(alert)
        assert len(hosts) == 3
        assert "10.10.1.1" in hosts
        assert "10.10.1.2" in hosts
        assert "10.10.1.3" in hosts

    def test_extract_hosts_keyword(self):
        """测试提取 hosts: 关键字。"""
        alert = Alert(
            alert_id="ALT-002",
            title="CPU High",
            level="Warning",
            content="hosts: server1, server2, server3",
        )
        hosts = extract_hosts_from_alert(alert)
        assert len(hosts) == 3
        assert "server1" in hosts
        assert "server2" in hosts
        assert "server3" in hosts

    def test_extract_single_ip_from_content(self):
        """测试从内容中提取单个 IP。"""
        alert = Alert(
            alert_id="ALT-003",
            title="Memory Leak",
            level="Warning",
            content="Detected memory leak on 192.168.1.100",
        )
        hosts = extract_hosts_from_alert(alert)
        assert len(hosts) == 1
        assert "192.168.1.100" in hosts

    def test_extract_from_source(self):
        """测试从 alert.source 提取。"""
        alert = Alert(
            alert_id="ALT-004",
            title="Service Down",
            level="Critical",
            content="Service is not responding",
            source="web-server-01",
        )
        hosts = extract_hosts_from_alert(alert)
        assert len(hosts) == 1
        assert "web-server-01" in hosts

    def test_no_hosts_found(self):
        """测试无法提取主机时返回空列表。"""
        alert = Alert(
            alert_id="ALT-005",
            title="Unknown Alert",
            level="Info",
            content="Something happened",
            source="",  # 明确指定空 source
        )
        hosts = extract_hosts_from_alert(alert)
        assert len(hosts) == 0


class TestCanaryGraph:
    """测试金丝雀执行图。"""

    @pytest.fixture
    def mock_executor(self):
        """创建 mock executor。"""
        executor = Mock()
        executor.execute_script = Mock(return_value=True)
        return executor

    @pytest.fixture
    def sample_alert(self):
        """创建示例告警。"""
        return Alert(
            alert_id="ALT-CANARY-001",
            title="Disk Space Critical",
            level="Critical",
            content="affected hosts: 10.10.1.1, 10.10.1.2, 10.10.1.3, 10.10.1.4",
        )

    @pytest.fixture
    def sample_proposal(self):
        """创建示例 LLM 提案。"""
        plan = ActionPlan(
            root_cause_analysis="Disk space is full due to log files",
            troubleshooting_steps=["Check disk usage", "Clean old logs"],
            script_content="find /var/log -name '*.log' -mtime +30 -delete",
            confidence_score=0.85,
        )
        return LLMProposal(alert_id="ALT-CANARY-001", plan=plan)

    def test_canary_graph_success(self, mock_executor, sample_alert, sample_proposal):
        """测试金丝雀执行成功场景。"""
        with patch("core.canary_graph.collect_health_metrics") as mock_metrics:
            # Mock 健康指标正常
            mock_metrics.return_value = {
                "cpu_usage": 45.0,
                "mem_usage": 60.0,
                "disk_usage": 50.0,
                "error_rate": 0.5,
                "service_status": "running",
            }

            with patch("core.canary_graph.time.sleep"):  # 跳过等待
                graph = build_canary_graph(mock_executor)

                initial_state: CanaryExecutionState = {
                    "alert": sample_alert,
                    "proposal": sample_proposal,
                    "script_content": sample_proposal.plan.script_content,
                    "all_targets": [],
                    "canary_targets": [],
                    "remaining_targets": [],
                    "canary_results": [],
                    "canary_success": False,
                    "canary_duration_ms": 0,
                    "health_metrics": {},
                    "should_rollback": False,
                    "rollback_reason": "",
                    "expand_results": [],
                    "total_success": False,
                    "rollback_executed": False,
                }

                final_state = graph.invoke(initial_state)

                # 验证执行成功
                assert final_state["total_success"] is True
                assert final_state["canary_success"] is True
                assert final_state["should_rollback"] is False

                # 验证识别了目标
                assert len(final_state["all_targets"]) == 4
                assert len(final_state["canary_targets"]) == 1  # 25% of 4 = 1
                assert len(final_state["remaining_targets"]) == 3

                # 验证执行器被调用
                assert mock_executor.execute_script.call_count == 4  # 1 canary + 3 expand

    def test_canary_graph_failure_rollback(self, mock_executor, sample_alert, sample_proposal):
        """测试金丝雀执行失败并回滚场景。"""
        # Mock executor 失败
        mock_executor.execute_script = Mock(return_value=False)

        with patch("core.canary_graph.time.sleep"):
            graph = build_canary_graph(mock_executor)

            initial_state: CanaryExecutionState = {
                "alert": sample_alert,
                "proposal": sample_proposal,
                "script_content": sample_proposal.plan.script_content,
                "all_targets": [],
                "canary_targets": [],
                "remaining_targets": [],
                "canary_results": [],
                "canary_success": False,
                "canary_duration_ms": 0,
                "health_metrics": {},
                "should_rollback": False,
                "rollback_reason": "",
                "expand_results": [],
                "total_success": False,
                "rollback_executed": False,
            }

            final_state = graph.invoke(initial_state)

            # 验证执行失败
            assert final_state["total_success"] is False
            assert final_state["canary_success"] is False
            assert final_state["should_rollback"] is True

            # 验证未执行扩展
            assert len(final_state["expand_results"]) == 0

    def test_canary_graph_unhealthy_metrics(self, mock_executor, sample_alert, sample_proposal):
        """测试金丝雀执行后健康指标异常场景。"""
        with patch("core.canary_graph.collect_health_metrics") as mock_metrics:
            # Mock 健康指标异常
            mock_metrics.return_value = {
                "cpu_usage": 95.0,  # CPU 过高
                "mem_usage": 60.0,
                "disk_usage": 50.0,
                "error_rate": 0.5,
                "service_status": "running",
            }

            with patch("core.canary_graph.time.sleep"):
                graph = build_canary_graph(mock_executor)

                initial_state: CanaryExecutionState = {
                    "alert": sample_alert,
                    "proposal": sample_proposal,
                    "script_content": sample_proposal.plan.script_content,
                    "all_targets": [],
                    "canary_targets": [],
                    "remaining_targets": [],
                    "canary_results": [],
                    "canary_success": False,
                    "canary_duration_ms": 0,
                    "health_metrics": {},
                    "should_rollback": False,
                    "rollback_reason": "",
                    "expand_results": [],
                    "total_success": False,
                    "rollback_executed": False,
                }

                final_state = graph.invoke(initial_state)

                # 验证触发回滚
                assert final_state["should_rollback"] is True
                assert "CPU" in final_state["rollback_reason"]

                # 验证未执行扩展
                assert len(final_state["expand_results"]) == 0

    def test_canary_graph_no_targets(self, mock_executor, sample_proposal):
        """测试无目标主机场景。"""
        alert = Alert(
            alert_id="ALT-NO-TARGET",
            title="Test Alert",
            level="Info",
            content="No hosts mentioned",
            source="",  # 明确指定空 source
        )

        graph = build_canary_graph(mock_executor)

        initial_state: CanaryExecutionState = {
            "alert": alert,
            "proposal": sample_proposal,
            "script_content": sample_proposal.plan.script_content,
            "all_targets": [],
            "canary_targets": [],
            "remaining_targets": [],
            "canary_results": [],
            "canary_success": False,
            "canary_duration_ms": 0,
            "health_metrics": {},
            "should_rollback": False,
            "rollback_reason": "",
            "expand_results": [],
            "total_success": False,
            "rollback_executed": False,
        }

        final_state = graph.invoke(initial_state)

        # 验证跳过执行
        assert final_state["total_success"] is False
        assert len(final_state["canary_targets"]) == 0
        assert mock_executor.execute_script.call_count == 0


class TestCanaryConfiguration:
    """测试金丝雀配置。"""

    def test_canary_ratio_calculation(self):
        """测试金丝雀比例计算。"""
        # 这个测试需要在 canary_graph.py 中实现具体的配置读取逻辑
        # 目前仅作为占位
        pass
