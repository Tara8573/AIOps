"""FR-5：远程执行验收测试。

验收标准（改造方案 §3.1 / 规格 FR-5）：
- 同主机并发互斥：多实例下也只有一个自动执行
- 执行超时/失败正确转工单路径（返回 False）
- 高危命令硬拦截（不可降级）
- 请求/结果经可靠通道（MQ）传递，request_id 关联
"""

import threading
import time

import pytest

from core.host_lock import HostLock
from plugins.remote_executor import RemoteExecutor, extract_host_from_context


class FakeProducer:
    """记录发布的执行请求。"""

    def __init__(self):
        self.published = []

    def publish(self, topic, key, value):
        self.published.append((topic, key, value))

    def close(self):
        pass


class FakeHostLock(HostLock):
    """可编程的假主机锁（模拟同主机已有执行）。"""

    def __init__(self, available=True):
        self._available = available
        self.acquired_hosts = []
        self.released = []

    def acquire(self, host, ttl_seconds):
        if not self._available:
            return None
        self.acquired_hosts.append(host)
        return "token-123"

    def release(self, host, token):
        self.released.append((host, token))
        return True


def _executor(producer=None, host_lock=None, result_provider=None, timeout=5):
    ex = RemoteExecutor(producer=producer, timeout_seconds=timeout)
    if host_lock is not None:
        ex._host_lock = host_lock
    if result_provider is not None:
        ex._result_consumer = None
        ex._wait_result = lambda request_id, timeout: result_provider(request_id)
    return ex


def _context(alert_id="ALT-1", host="10.1.2.3"):
    return {
        "alert": {
            "alert_id": alert_id,
            "title": "磁盘空间不足",
            "content": f"host {host} /var usage 95%",
            "raw_data": {"host": host},
        }
    }


class TestHostLock:
    def test_lock_acquire_release(self):
        """锁获取/释放（token 校验）。"""
        import fakeredis
        lock = HostLock(fakeredis.FakeRedis())
        token = lock.acquire("10.1.2.3", 300)
        assert token is not None
        # 第二次获取（同主机）→ 冲突
        assert lock.acquire("10.1.2.3", 300) is None
        # 释放后可再次获取
        assert lock.release("10.1.2.3", token) is True
        assert lock.acquire("10.1.2.3", 300) is not None

    def test_lock_ttl_bounded(self):
        """锁带 TTL 兜底（持有者崩溃后自动过期）。"""
        import fakeredis
        lock = HostLock(fakeredis.FakeRedis())
        lock.acquire("10.1.2.3", 300)
        ttl = lock._redis.ttl("lock:host:10.1.2.3")
        assert 0 < ttl <= 330

    def test_lock_none_redis(self):
        """无 Redis：不做互斥（上层转工单兜底）。"""
        lock = HostLock(None)
        assert lock.acquire("h", 300) is None

    def test_lock_release_wrong_token(self):
        """token 校验：不误释放他人锁。"""
        import fakeredis
        lock = HostLock(fakeredis.FakeRedis())
        lock.acquire("10.1.2.3", 300)
        assert lock.release("10.1.2.3", "wrong-token") is False
        # 锁仍在（未被误释放）
        assert lock.acquire("10.1.2.3", 300) is None


class TestRemoteExecutor:
    def test_same_host_mutex_rejects(self):
        """同主机已有执行 → 互斥拒绝（返回 False → 上层转工单）。"""
        producer = FakeProducer()
        lock = FakeHostLock(available=False)  # 模拟同主机已有执行
        ex = _executor(producer=producer, host_lock=lock)

        assert ex.execute_script("echo fix", _context()) is False
        assert producer.published == []  # 未下发请求

    def test_success_returns_true_and_publishes_request(self):
        """正常执行：加锁 → 发请求 → 等结果成功。"""
        producer = FakeProducer()
        lock = FakeHostLock(available=True)

        def result_provider(request_id):
            return {"request_id": request_id, "success": True, "exit_code": 0}

        ex = _executor(producer=producer, host_lock=lock, result_provider=result_provider)
        assert ex.execute_script("echo fix", _context()) is True
        # 请求已发布（topic/key/host 正确）
        assert producer.published[0][0] == "aiops-exec-requests"
        assert producer.published[0][1] == "10.1.2.3"
        # 锁已释放
        assert lock.released == [("10.1.2.3", "token-123")]

    def test_timeout_returns_false(self):
        """执行超时 → 返回 False（上层转工单）。"""
        producer = FakeProducer()
        ex = _executor(
            producer=producer, host_lock=FakeHostLock(),
            result_provider=lambda request_id: None,  # 超时未回结果
            timeout=0.2,
        )
        assert ex.execute_script("echo fix", _context()) is False

    def test_failure_returns_false(self):
        """远程执行失败（agent 回 success=false）→ 返回 False。"""
        producer = FakeProducer()
        ex = _executor(
            producer=producer, host_lock=FakeHostLock(),
            result_provider=lambda request_id: {"request_id": request_id, "success": False, "stderr": "cmd not found"},
        )
        assert ex.execute_script("bad-cmd", _context()) is False

    def test_dangerous_command_hard_blocked(self):
        """高危命令硬拦截（规格 4.3：不可降级）。"""
        producer = FakeProducer()
        ex = _executor(producer=producer, host_lock=FakeHostLock())
        for script in ["rm -rf /", "mkfs.ext4 /dev/sda", ":(){ :|:& };:"]:
            assert ex.execute_script(script, _context()) is False
        assert producer.published == []  # 拦截后不下发

    def test_no_producer_fails_fast(self):
        """未配置生产者 → 快速失败（不发请求）。"""
        ex = _executor(producer=None, host_lock=FakeHostLock())
        assert ex.execute_script("echo fix", _context()) is False

    def test_multi_instance_same_host_mutex(self):
        """双实例同时触发同主机 → 一成一拒（多实例下互斥成立）。

        barrier 保证两实例同时在场；持锁期 0.2s 保证竞态确定性 ——
        （若前一实例执行完释放后后者才获取，两者都成功属合法语义，不算冲突）。
        """
        import fakeredis
        r = fakeredis.FakeRedis()
        producer = FakeProducer()
        results = {}
        barrier = threading.Barrier(2)

        def instance(name):
            ex = RemoteExecutor(producer=producer, timeout_seconds=5)
            ex._host_lock = HostLock(r)

            def wait_result(request_id, timeout):
                time.sleep(0.2)  # 持锁期（另一实例必然撞互斥）
                return {"request_id": request_id, "success": True}

            ex._wait_result = wait_result
            barrier.wait(timeout=2)  # 两实例同时进入执行
            results[name] = ex.execute_script("echo fix", _context())

        t1 = threading.Thread(target=instance, args=("a",))
        t2 = threading.Thread(target=instance, args=("b",))
        t1.start(); t2.start(); t1.join(); t2.join()

        # 同主机并发执行只允许一个成功
        assert sorted(results.values()) == [False, True]


class TestHostExtraction:
    def test_extract_host_from_raw_data(self):
        assert extract_host_from_context(_context()) == "10.1.2.3"

    def test_extract_host_from_text(self):
        ctx = {"alert": {"alert_id": "A", "title": "主机 10.9.8.7 磁盘满", "content": "", "raw_data": None}}
        assert extract_host_from_context(ctx) == "10.9.8.7"
