"""FR-2.1：状态外置到 Redis 验收测试。

验收标准（改造方案 §2.1 / 规格 FR-2.1）：
- 双实例共享 Redis：同一告警只被一个实例诊断（去重跨实例生效）
- 同主机告警被哈希到不同实例时，关联分析仍生效
- Redis 宕机时系统降级运行不崩溃（回落本地实现 + 记指标）
"""

import fakeredis
import pytest

from core.cache import RedisTTLCache, TTLCache, cache_factory
from core.correlation_store import (
    InMemoryCorrelationStore,
    RedisCorrelationStore,
    correlation_store_factory,
)
from core.dedup_store import (
    InMemoryDedupStore,
    RedisDedupStore,
    dedup_key_for,
    dedup_store_factory,
)
from core.models import Alert


def _alert(alert_id, host):
    return Alert(
        alert_id=alert_id,
        title="磁盘空间不足",
        level="Critical",
        content=f"host {host} /var usage 95%",
    )


class BrokenRedis:
    """模拟 Redis 宕机：所有操作抛异常。"""

    def __getattr__(self, name):
        raise ConnectionError("redis down")


class TestDedupCrossInstance:
    def test_same_alert_deduped_across_instances(self):
        """双实例共享 Redis：同一告警只被一个实例诊断。"""
        r = fakeredis.FakeRedis()
        instance_a = RedisDedupStore(r, prefix="dedup:")
        instance_b = RedisDedupStore(r, prefix="dedup:")

        key = dedup_key_for("磁盘空间不足", "host h /var usage 95%")
        assert instance_a.is_duplicate(key, 300) is False  # 实例 A 首次诊断
        assert instance_b.is_duplicate(key, 300) is True   # 实例 B 命中去重（不再诊断）

    def test_dedup_window_expiry(self):
        r = fakeredis.FakeRedis()
        store = RedisDedupStore(r)
        key = dedup_key_for("t", "c")
        assert store.is_duplicate(key, 1) is False
        assert store.is_duplicate(key, 1) is True
        # 窗口过后可再次处理
        store._redis.delete(f"dedup:{key}")
        assert store.is_duplicate(key, 1) is False

    def test_dedup_fallback_on_redis_down(self):
        """Redis 宕机：降级本地实现，不崩溃。"""
        store = RedisDedupStore(BrokenRedis())
        key = dedup_key_for("t", "c")
        assert store.is_duplicate(key, 300) is False  # 本地降级仍工作
        assert store.is_duplicate(key, 300) is True

    def test_factory_without_redis_url(self, monkeypatch):
        """未配置 AIOPS_REDIS_URL → 本地实现。"""
        monkeypatch.delenv("AIOPS_REDIS_URL", raising=False)
        store = dedup_store_factory(redis_client=None)
        assert isinstance(store, InMemoryDedupStore)


class TestCorrelationCrossInstance:
    def test_correlation_works_across_instances(self):
        """同主机告警被哈希到不同实例时，关联分析仍生效。"""
        r = fakeredis.FakeRedis()
        extract = lambda a: "10.1.2.3"
        instance_a = RedisCorrelationStore(r, extract_host_fn=extract)
        instance_b = RedisCorrelationStore(r, extract_host_fn=extract)

        # 实例 A 处理同主机告警 ALT-1
        instance_a.add(_alert("ALT-1", "10.1.2.3"), 300)
        # 实例 B 处理同主机告警 ALT-2 → 应能从 Redis 读到 ALT-1 的关联
        correlated = instance_b.find_same_host(_alert("ALT-2", "10.1.2.3"), 300)
        assert len(correlated) == 1
        assert correlated[0]["alert_id"] == "ALT-1"

    def test_correlation_window_expiry(self):
        r = fakeredis.FakeRedis()
        extract = lambda a: "10.1.2.3"
        store = RedisCorrelationStore(r, extract_host_fn=extract)
        store.add(_alert("ALT-1", "10.1.2.3"), 1)
        # 把 seen_at 拨回过去
        import json as _json
        raw = store._redis.lrange("correlation:10.1.2.3", 0, -1)
        entry = _json.loads(raw[0])
        entry["seen_at"] -= 999
        store._redis.delete("correlation:10.1.2.3")
        store._redis.rpush("correlation:10.1.2.3", _json.dumps(entry, ensure_ascii=False))
        correlated = store.find_same_host(_alert("ALT-2", "10.1.2.3"), 1)
        assert correlated == []  # 窗口外不关联

    def test_correlation_fallback_on_redis_down(self):
        store = RedisCorrelationStore(BrokenRedis(), extract_host_fn=lambda a: "h")
        store.add(_alert("ALT-1", "h"), 300)  # 本地降级
        correlated = store.find_same_host(_alert("ALT-2", "h"), 300)
        assert len(correlated) == 1  # 降级仍工作


class TestRedisTTLCache:
    def test_set_get_roundtrip(self):
        r = fakeredis.FakeRedis()
        cache = RedisTTLCache(r, ttl_seconds=60)
        cache.set("k1", {"sops": ["a", "b"]})
        assert cache.get("k1") == {"sops": ["a", "b"]}

    def test_cache_shared_across_instances(self):
        """多实例共享热点缓存。"""
        r = fakeredis.FakeRedis()
        c1 = RedisTTLCache(r, ttl_seconds=60)
        c2 = RedisTTLCache(r, ttl_seconds=60)
        c1.set("sop", ["s1", "s2"])
        assert c2.get("sop") == ["s1", "s2"]

    def test_cache_fallback_on_redis_down(self):
        cache = RedisTTLCache(BrokenRedis(), ttl_seconds=60)
        cache.set("k", "v")   # 降级缓存同步写入
        assert cache.get("k") == "v"

    def test_factory_returns_local_without_redis(self, monkeypatch):
        monkeypatch.delenv("AIOPS_REDIS_URL", raising=False)
        cache = cache_factory(redis_client=None)
        assert isinstance(cache, TTLCache)
