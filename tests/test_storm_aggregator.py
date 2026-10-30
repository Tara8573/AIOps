"""FR-1.2：告警风暴聚合验收测试。

验收标准（改造方案 §1.3.2）：
- 1000 条同根因告警收敛为 ≤20 个 Incident（收敛比 ≥50:1）
- LLM 调用量 = Incident 数而非告警数（组代表之外不诊断）
- 组内每条告警都能查到对应的处置结果（广播）
"""

from pipeline import _extract_host
from core.intent import AlertIntentClassifier
from core.models import Alert
from core.storm_aggregator import InMemoryGroupStore, StormAggregator, storm_signature


def _alert(i, title="磁盘 /var 使用率超过 90%", host="10.1.2.3", level="Critical"):
    return Alert(
        alert_id=f"ALT-{i}",
        title=title,
        level=level,
        content=f"host {host} /var usage {90 + i % 10}%",
    )


def _aggregator(window=30, max_group=200):
    return StormAggregator(
        intent_classifier=AlertIntentClassifier(),
        extract_host_fn=_extract_host,
        group_store=InMemoryGroupStore(window_seconds=window),
        window_seconds=window,
        max_group=max_group,
    )


class TestStormConvergence:
    def test_1000_same_root_cause_converge_to_20_incidents(self):
        """1000 条同根因告警（同签名）收敛比 ≥50:1（≤20 Incident）。"""
        agg = _aggregator(max_group=200)
        total_incidents = 0
        # 模拟分批到达（每批 50）
        for batch_start in range(0, 1000, 50):
            batch = [_alert(batch_start + j, title=f"磁盘 /var 使用率超过 {90 + j % 10}%") for j in range(50)]
            work = agg.aggregate(batch)
            # 只有新组产生工作；代表已处理的组内晚到成员计入 members
            total_incidents += sum(1 for inc in work if inc.result is None)

        assert total_incidents <= 20, f"1000 条应收敛为 ≤20 Incident，实际 {total_incidents}"
        # 收敛比 ≥50:1
        assert 1000 / max(1, total_incidents) >= 50

    def test_llm_calls_equal_incident_count(self):
        """组代表之外不诊断 —— 每个签名只有 1 个代表（LLM 调用 = Incident 数）。"""
        agg = _aggregator()
        batch = [_alert(i) for i in range(100)]
        work = agg.aggregate(batch)
        # 100 条同签名 → 1 个新组 → 1 个代表
        assert len(work) == 1
        assert work[0].representative is not None
        assert len(work[0].members) == 100

    def test_different_hosts_dont_group(self):
        """不同主机的同文案告警不合并（签名含 host）。"""
        agg = _aggregator()
        batch = [_alert(i, host=f"10.1.2.{i}") for i in range(5)]
        work = agg.aggregate(batch)
        assert len(work) == 5  # 5 个独立 Incident

    def test_different_root_cause_dont_group(self):
        """不同根因（不同标题）不合并。"""
        agg = _aggregator()
        batch = [_alert(0, title="磁盘使用率过高"), _alert(1, title="内存泄漏 OOM")]
        work = agg.aggregate(batch)
        assert len(work) == 2


class TestBroadcast:
    def test_late_joiner_broadcasts(self):
        """代表结果已出，晚到成员入组后触发广播。"""
        agg = _aggregator()
        # 第一轮：代表
        work1 = agg.aggregate([_alert(0)])
        inc = work1[0]
        inc.result = {"ticket_id": "T-1", "execution_success": True}

        # 第二轮：晚到成员（同签名）
        work2 = agg.aggregate([_alert(1)])
        assert work2 and work2[0] is inc  # 同一组，待广播

        marked = []
        count = agg.broadcast(inc, commit_cb=lambda member, i: marked.append(member.alert_id))
        # 代表与晚到成员都尚未广播（broadcast_ids 为空）→ 两者都广播，
        # 组内每条告警都能查到对应处置结果
        assert sorted(marked) == ["ALT-0", "ALT-1"]

    def test_broadcast_idempotent(self):
        """广播幂等：重复调用不再重复处理成员。"""
        agg = _aggregator()
        work = agg.aggregate([_alert(0), _alert(1)])
        inc = work[0]
        inc.result = {"ticket_id": "T-1"}

        marked = []
        n1 = agg.broadcast(inc, commit_cb=lambda m, i: marked.append(m.alert_id))
        n2 = agg.broadcast(inc, commit_cb=lambda m, i: marked.append(m.alert_id))
        assert n1 == 2  # 两个成员（含代表）
        assert n2 == 0  # 幂等
        assert sorted(marked) == ["ALT-0", "ALT-1"]

    def test_broadcast_marks_group_members(self):
        """组内每条告警都能查到对应处置结果（broadcast_ids 全覆盖）。"""
        agg = _aggregator()
        batch = [_alert(i) for i in range(50)]
        work = agg.aggregate(batch)
        inc = work[0]
        inc.result = {"ticket_id": "T-1", "execution_success": True}

        marked = []
        agg.broadcast(inc, commit_cb=lambda m, i: marked.append(m.alert_id))
        assert set(marked) == {f"ALT-{i}" for i in range(50)}


class TestGroupLifecycle:
    def test_group_max_size_closes_group(self):
        """组满（AIOPS_AGG_MAX_GROUP）关闭，后续同签名开新组。"""
        agg = _aggregator(max_group=2)
        work = agg.aggregate([_alert(0), _alert(1)])
        assert len(work) == 1
        # 第 3 条：组已满关闭 → 新组
        work2 = agg.aggregate([_alert(2)])
        assert len(work2) == 1
        assert work2[0] is not work[0]
        assert work2[0].representative is not None

    def test_window_expiry_creates_new_group(self, monkeypatch):
        """窗口过期：组销毁，后续同签名 → 新 Incident。"""
        agg = _aggregator(window=1)
        work = agg.aggregate([_alert(0)])
        assert len(work) == 1

        # 模拟窗口过期：把组创建时间拨回过去
        import time as _t
        inc = work[0]
        inc.created_at = _t.time() - 10
        # expire 在 aggregate 内触发
        work2 = agg.aggregate([_alert(1)])
        assert len(work2) == 1
        assert work2[0] is not inc

    def test_signature_deterministic(self):
        """签名确定性：同意图+同主机+同实体 → 同签名；数字不同不改变签名。"""
        a1 = _alert(0, title="磁盘 /var 使用率超过 90%")
        a2 = _alert(1, title="磁盘 /var 使用率超过 96%")
        s1 = storm_signature(a1, "disk", "10.1.2.3")
        s2 = storm_signature(a2, "disk", "10.1.2.3")
        assert s1 == s2
        s3 = storm_signature(a1, "disk", "10.9.9.9")
        assert s1 != s3
        s4 = storm_signature(a1, "memory", "10.1.2.3")
        assert s1 != s4
