"""Phase 0 验证测试：Decision Engine 抽象层。

测试目标：
1. RuleDecisionClient 通过 IDecisionClient 接口正常工作
2. JevDecisionClientStub 降级行为正确
3. 抽象层正确隔离（业务代码不感知具体实现）
4. pipeline_graph.py 的 route_intent 节点输出新增的 Jev 字段
"""

import pytest

from core.models import DecisionResult, JudgeResult, NoulResult, ScoreResult
from interfaces.base import IDecisionClient


# ═══════════════════════════════════════════════════════════════
# 测试 1: RuleDecisionClient — 确定性规则实现
# ═══════════════════════════════════════════════════════════════


class TestRuleDecisionClient:
    """验证 RuleDecisionClient 符合 IDecisionClient 契约。"""

    @pytest.fixture
    def client(self):
        from plugins.rule_decision_client import RuleDecisionClient
        return RuleDecisionClient()

    def test_implements_interface(self, client):
        """必须实现 IDecisionClient 的所有方法。"""
        assert isinstance(client, IDecisionClient)

    def test_classify_disk_alert(self, client):
        """磁盘告警应被分类为 disk。"""
        result = client.classify(
            prompt="磁盘空间不足 /data 挂载点使用率 95%",
            options=["disk", "memory", "cpu", "network", "security", "general"],
        )
        assert isinstance(result, DecisionResult)
        assert result.choice == "disk"
        assert result.confidence > 0.5
        assert "磁盘" in result.reason or "disk" in result.reason

    def test_classify_unknown_alert(self, client):
        """无关键词命中时返回 general。"""
        result = client.classify(
            prompt="unknown strange event with no recognizable keywords",
            options=["disk", "memory", "cpu", "network", "general"],
        )
        assert result.choice == "general"
        assert result.confidence < 0.5

    def test_classify_unknown_without_general_option(self, client):
        """options 中没有 general 时应 fallback 到 general（作为兜底）。"""
        result = client.classify(
            prompt="unknown event xyz",
            options=["disk", "memory", "cpu"],
        )
        # 无命中 → fallback general
        assert result.choice == "general"
        assert result.confidence <= 0.4

    def test_classify_oom_as_memory(self, client):
        """OOM 告警应分类为 memory。"""
        result = client.classify(
            prompt="pod XYZ OOMKilled memory 内存不足 java heap leak",
            options=["disk", "memory", "cpu", "network", "general"],
        )
        assert result.choice == "memory"

    def test_noul_dangerous_script(self, client):
        """危险脚本应判 True。"""
        result = client.noul(
            prompt="rm -rf /var/tmp && reboot",
            question="该脚本是否包含危险/破坏性命令？",
        )
        assert isinstance(result, NoulResult)
        assert result.answer is True

    def test_noul_safe_script(self, client):
        """安全脚本应判 False。"""
        result = client.noul(
            prompt="df -h && systemctl restart nginx",
            question="该脚本是否包含危险/破坏性命令？",
        )
        assert result.answer is False

    def test_score_dangerous(self, client):
        """高危险脚本应得高分。"""
        result = client.score(
            prompt="rm -rf / && dd if=/dev/zero of=/dev/sda",
            min_val=0, max_val=10, instructions="评估脚本风险等级",
        )
        assert isinstance(result, ScoreResult)
        assert result.value >= 5.0  # 多个危险模式

    def test_score_safe(self, client):
        """安全脚本应得低分。"""
        result = client.score(
            prompt="df -h && top -bn1",
            min_val=0, max_val=10, instructions="评估脚本风险等级",
        )
        assert result.value <= 2.0

    def test_judge_safe_script(self, client):
        """安全脚本各维度应通过。"""
        dimensions = [
            {"name": "safety", "question": "脚本是否安全？", "weight": 1.0},
            {"name": "consistency", "question": "步骤是否匹配根因？", "weight": 0.5},
        ]
        result = client.judge(
            prompt="systemctl restart nginx",
            dimensions=dimensions,
        )
        assert isinstance(result, JudgeResult)
        assert len(result.dimensions) == 2
        # safety 维度应通过（无危险命令）
        safety = next(d for d in result.dimensions if d.name == "safety")
        assert safety.passed is True

    def test_health_check_always_true(self, client):
        """本地规则引擎始终可用。"""
        assert client.health_check() is True


# ═══════════════════════════════════════════════════════════════
# 测试 2: JevDecisionClientStub — 桩降级
# ═══════════════════════════════════════════════════════════════


class TestJevStubClient:
    """验证 Jev stub 降级行为。"""

    @pytest.fixture
    def stub(self):
        from plugins.jev_decision_client import JevDecisionClientStub
        return JevDecisionClientStub()

    def test_implements_interface(self, stub):
        """Stub 也必须实现完整接口。"""
        assert isinstance(stub, IDecisionClient)

    def test_health_check_returns_false(self, stub):
        """Stub 明确标记不可用，调用方据此决定是否采信结果。"""
        assert stub.health_check() is False

    def test_classify_returns_low_confidence(self, stub):
        """Stub 所有结果应带低置信度标记。"""
        result = stub.classify(
            prompt="磁盘空间不足",
            options=["disk", "memory", "cpu", "general"],
        )
        assert result.confidence < 0.5
        assert "[STUB]" in result.reason

    def test_noul_safety_conservative(self, stub):
        """安全类问题 stub 倾向保守判 Yes（宁可误拦不漏）。"""
        result = stub.noul(
            prompt="some command",
            question="该脚本是否有危险？",
        )
        # 安全类问题：stub 保守策略 → 判 Yes
        assert result.answer is True
        assert "[STUB]" in result.reason

    def test_score_returns_mid(self, stub):
        """Stub 评分返回中值。"""
        result = stub.score(prompt="anything", min_val=0, max_val=10)
        assert result.value == 5.0
        assert result.confidence < 0.5

    def test_judge_all_pass(self, stub):
        """Stub judge 所有维度默认通过（保守策略）。"""
        dimensions = [
            {"name": "safety", "question": "安全？"},
            {"name": "consistency", "question": "一致？"},
        ]
        result = stub.judge(prompt="any script", dimensions=dimensions)
        assert result.overall_passed is True
        assert result.overall_score == 0.5


# ═══════════════════════════════════════════════════════════════
# 测试 3: 抽象层隔离 — 业务代码只依赖 IDecisionClient
# ═══════════════════════════════════════════════════════════════


class TestAbstractionIsolation:
    """验证业务代码可以通过 IDecisionClient 接口透明切换实现。"""

    def test_rule_and_stub_both_satisfy_contract(self):
        """RuleDecisionClient 和 JevStub 都是合法的 IDecisionClient。"""
        from plugins.rule_decision_client import RuleDecisionClient
        from plugins.jev_decision_client import JevDecisionClientStub

        clients = [RuleDecisionClient(), JevDecisionClientStub()]
        for client in clients:
            assert isinstance(client, IDecisionClient)
            # 所有方法可调用、返回正确类型
            assert isinstance(client.classify("test", ["a", "b"]), DecisionResult)
            assert isinstance(client.judge("test", []), JudgeResult)
            assert isinstance(client.noul("test", "q"), NoulResult)
            assert isinstance(client.score("test", 0, 10), ScoreResult)
            assert isinstance(client.health_check(), bool)

    def test_same_code_works_with_any_implementation(self):
        """同一段业务代码，换实现后行为一致（接口级别）。"""

        def classify_alert(client: IDecisionClient, prompt: str) -> DecisionResult:
            return client.classify(prompt, ["disk", "memory", "cpu", "general"])

        from plugins.rule_decision_client import RuleDecisionClient
        from plugins.jev_decision_client import JevDecisionClientStub

        rule = RuleDecisionClient()
        stub = JevDecisionClientStub()

        r1 = classify_alert(rule, "磁盘空间不足")
        r2 = classify_alert(stub, "磁盘空间不足")

        # 结果类型一致
        assert isinstance(r1, DecisionResult)
        assert isinstance(r2, DecisionResult)
        # 规则实现能得到有意义的分类
        assert r1.choice == "disk"
        # stub 返回低置信度标记
        assert "[STUB]" in r2.reason


# ═══════════════════════════════════════════════════════════════
# 测试 4: pipeline_graph route_intent 叠加 Jev 字段
# ═══════════════════════════════════════════════════════════════


class TestPipelineRouteIntentJevFields:
    """验证 route_intent 节点输出新增的 intent_jev_* 字段。"""

    def test_route_intent_outputs_jev_fields(self):
        """route_intent 节点应输出 intent_jev_choice 和 intent_jev_confidence。"""
        from core.pipeline_graph import build_pipeline_graph
        from core.models import Alert

        # 构建最小 pipeline mock
        from unittest.mock import MagicMock
        pipeline = MagicMock()
        pipeline.router.decide.return_value = MagicMock(
            route="standard", priority=100, reason="test", tags=[]
        )
        pipeline.intent_classifier.classify.return_value = MagicMock(
            category="general", confidence=0.35,
            reason="test", tags=[], secondary_categories=[],
        )
        # decision_client: 使用 RuleDecisionClient（默认）
        from plugins.rule_decision_client import RuleDecisionClient
        pipeline.decision_client = RuleDecisionClient()

        alert = Alert(
            alert_id="test-001",
            title="磁盘空间不足",
            level="Warning",
            content="/data 分区使用率 95%",
            source="prometheus",
        )

        state = {"alert": alert}
        # FR-4：route_intent 变为图内闭包，经编译图调用节点（真实绑定依赖）
        result = build_pipeline_graph(pipeline).nodes["route_intent"].invoke(state)

        # 保留原有字段
        assert "intent_category" in result
        assert "intent_confidence" in result
        assert "route_decision" in result

        # 新增 Jev 字段
        assert "intent_jev_choice" in result
        assert "intent_jev_confidence" in result
        # RuleDecisionClient 能给出有意义的 Jev 分类
        assert result["intent_jev_choice"] == "disk"
        assert result["intent_jev_confidence"] > 0.5

    def test_route_intent_jev_graceful_degradation(self):
        """Jev 调用失败时不应抛异常，应静默降级。"""
        from core.pipeline_graph import build_pipeline_graph
        from core.models import Alert

        from unittest.mock import MagicMock
        pipeline = MagicMock()
        pipeline.router.decide.return_value = MagicMock(
            route="standard", priority=100, reason="test", tags=[]
        )
        pipeline.intent_classifier.classify.return_value = MagicMock(
            category="general", confidence=0.35,
            reason="test", tags=[], secondary_categories=[],
        )
        # decision_client.classify 抛异常 → 降级
        pipeline.decision_client.classify.side_effect = RuntimeError("Jev 不可用")

        alert = Alert(
            alert_id="test-002",
            title="测试告警",
            level="Info",
            content="test",
            source="prometheus",
        )

        state = {"alert": alert}
        # FR-4：经编译图调用节点（真实绑定依赖），不应抛异常
        result = build_pipeline_graph(pipeline).nodes["route_intent"].invoke(state)  # 不应抛异常

        assert result["intent_jev_choice"] == ""
        assert result["intent_jev_confidence"] == 0.0


# ═══════════════════════════════════════════════════════════════
# 测试 5: 工厂函数自动选择
# ═══════════════════════════════════════════════════════════════


class TestFactoryAutoSelection:
    """create_jev_client 自动选择 stub 或真实客户端。"""

    def test_no_api_key_returns_stub(self):
        """无 API key 时返回 stub。"""
        import os
        old_key = os.environ.pop("JEV_API_KEY", None)
        try:
            from plugins.jev_decision_client import create_jev_client, JevDecisionClientStub
            client = create_jev_client()
            assert isinstance(client, JevDecisionClientStub)
        finally:
            if old_key:
                os.environ["JEV_API_KEY"] = old_key

    def test_with_api_key_returns_real_client(self):
        """有 API key 时返回真实客户端。"""
        import os
        os.environ["JEV_API_KEY"] = "test-key-123"
        try:
            from plugins.jev_decision_client import create_jev_client, JevDecisionClient
            client = create_jev_client()
            assert isinstance(client, JevDecisionClient)
        finally:
            os.environ.pop("JEV_API_KEY", None)