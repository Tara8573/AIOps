"""LangGraph 金丝雀执行图。

在自动化修复时，先在小范围目标（金丝雀）上执行，验证安全后再扩展到全部目标。
适用于多主机批量修复场景。

节点流:
  identify_targets → canary_execute → monitor → decision
                                                  ↓
                                    ┌─────────────┴─────────────┐
                                    ↓                           ↓
                              expand_execute                rollback
                                    ↓                           ↓
                                   END                         END
"""

import functools
import os
import time
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from core.models import Alert, LLMProposal
from core.observability import metrics


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class CanaryExecutionState(TypedDict):
    """金丝雀执行图的状态。

    FR-4 纯数据化：executor 实例经图构建参数（partial）绑定，不进 state ——
    嵌套 canary 图会继承父 pipeline 的 checkpointer，state 内嵌实例会导致
    checkpoint 序列化失败。
    """
    # 输入
    alert: Alert
    proposal: LLMProposal
    script_content: str

    # 目标主机
    all_targets: List[str]
    canary_targets: List[str]
    remaining_targets: List[str]

    # 金丝雀执行结果
    canary_results: List[Dict[str, Any]]
    canary_success: bool
    canary_duration_ms: float

    # 监控结果
    health_metrics: Dict[str, Dict[str, Any]]
    should_rollback: bool
    rollback_reason: str

    # 扩展执行结果
    expand_results: List[Dict[str, Any]]
    total_success: bool

    # 回滚结果
    rollback_executed: bool


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def extract_hosts_from_alert(alert: Alert) -> List[str]:
    """从告警中提取受影响的主机列表。

    支持多种格式：
    - 告警内容中包含 IP 列表: "affected hosts: 10.10.1.1, 10.10.1.2, ..."
    - 告警内容中包含主机名: "hosts: server1, server2, ..."
    - 单个主机: 从 alert.source 或 content 中提取
    """
    content = alert.content.lower()

    # 尝试匹配 "hosts: ip1, ip2, ip3" 或 "affected hosts: ..."
    import re
    hosts_pattern = r'(?:affected\s+)?hosts?:\s*([0-9a-z\.\-\,\s]+)'
    match = re.search(hosts_pattern, content)

    if match:
        hosts_str = match.group(1)
        hosts = [h.strip() for h in hosts_str.split(',') if h.strip()]
        if hosts:
            return hosts

    # 尝试从 content 中提取 IP 地址
    ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    ips = re.findall(ip_pattern, content)
    if ips:
        return list(set(ips))  # 去重

    # 单个主机场景
    if alert.source:
        return [alert.source]

    return []


def execute_on_target(
    executor: Any,
    target: str,
    script: str,
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    """在指定目标上执行脚本。

    Args:
        executor: IExecutor 实例
        target: 目标主机（IP 或主机名）
        script: 要执行的脚本
        timeout_seconds: 超时时间（秒）

    Returns:
        执行结果字典: {target, success, output, error, duration_ms}
    """
    start_time = time.time()

    try:
        # 注入目标信息到脚本上下文
        context = {"target_host": target, "timeout": timeout_seconds}

        # 调用 executor（需要支持 target 参数）
        # 如果 executor 不支持 target，则在脚本中注入 target 变量
        script_with_target = f"TARGET_HOST='{target}'\n{script}"

        success = executor.execute_script(script_with_target, context)

        duration_ms = (time.time() - start_time) * 1000

        return {
            "target": target,
            "success": success,
            "output": "",
            "error": None,
            "duration_ms": duration_ms,
        }
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return {
            "target": target,
            "success": False,
            "output": "",
            "error": str(e),
            "duration_ms": duration_ms,
        }


def collect_health_metrics(target: str) -> Dict[str, Any]:
    """收集目标主机的健康指标。

    这里是一个简化的实现，实际应该从 Prometheus/Grafana 获取。
    """
    # TODO: 集成真实的监控系统
    # 目前返回模拟数据
    return {
        "cpu_usage": 45.0,  # CPU 使用率 (%)
        "mem_usage": 60.0,  # 内存使用率 (%)
        "disk_usage": 50.0,  # 磁盘使用率 (%)
        "error_rate": 0.5,  # 错误率 (%)
        "service_status": "running",  # 服务状态
    }


def generate_rollback_script(proposal: LLMProposal) -> Optional[str]:
    """生成回滚脚本。

    目前简化实现：如果 proposal 中包含 rollback_script 字段则使用，
    否则返回 None。
    """
    # TODO: 更智能的回滚脚本生成
    # 例如：如果是文件清理，回滚可能是从备份恢复

    # 尝试从 proposal 中获取
    if hasattr(proposal, 'rollback_script'):
        return proposal.rollback_script

    return None


def create_incident_ticket(alert: Alert, reason: str, canary_results: List[Dict]):
    """创建事故工单。

    当金丝雀执行失败时，创建工单以便人工介入。
    """
    # TODO: 集成工单系统
    print(f"[Canary] 创建事故工单: {reason}")
    print(f"[Canary] 告警: {alert.title}")
    print(f"[Canary] 金丝雀结果: {canary_results}")


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def identify_targets(state: CanaryExecutionState) -> dict:
    """识别所有受影响的主机，选择金丝雀目标。"""
    alert = state["alert"]

    # 提取所有受影响主机
    all_targets = extract_hosts_from_alert(alert)

    if not all_targets:
        print("[Canary] 未识别到目标主机，跳过金丝雀执行")
        return {
            "all_targets": [],
            "canary_targets": [],
            "remaining_targets": [],
        }

    # 计算金丝雀数量
    canary_ratio = float(os.getenv("AIOPS_CANARY_RATIO", "0.25"))
    min_hosts = int(os.getenv("AIOPS_CANARY_MIN_HOSTS", "1"))
    max_hosts = int(os.getenv("AIOPS_CANARY_MAX_HOSTS", "5"))

    canary_count = max(
        min_hosts,
        min(int(len(all_targets) * canary_ratio), max_hosts)
    )

    # 如果主机数 <= min_hosts，全部作为金丝雀
    if len(all_targets) <= min_hosts:
        canary_count = len(all_targets)

    canary_targets = all_targets[:canary_count]
    remaining_targets = all_targets[canary_count:]

    print(f"[Canary] 识别 {len(all_targets)} 台主机")
    print(f"[Canary] 金丝雀: {len(canary_targets)} 台 {canary_targets}")
    print(f"[Canary] 剩余: {len(remaining_targets)} 台")

    return {
        "all_targets": all_targets,
        "canary_targets": canary_targets,
        "remaining_targets": remaining_targets,
    }


def canary_execute(executor, state: CanaryExecutionState) -> dict:
    """在金丝雀主机上执行修复脚本。executor 经图构建参数绑定。"""
    script = state["script_content"]
    canary_targets = state["canary_targets"]

    if not canary_targets:
        print("[Canary] 无金丝雀目标，跳过执行")
        return {
            "canary_results": [],
            "canary_success": True,
            "canary_duration_ms": 0,
        }

    print(f"[Canary] 开始在 {len(canary_targets)} 台金丝雀主机上执行...")

    results = []
    start_time = time.time()

    for target in canary_targets:
        result = execute_on_target(
            executor=executor,
            target=target,
            script=script,
            timeout_seconds=300,
        )
        results.append(result)

        status = "✓ 成功" if result["success"] else "✗ 失败"
        print(f"[Canary] {target}: {status} ({result['duration_ms']:.0f}ms)")

        if result["error"]:
            print(f"[Canary]   错误: {result['error']}")

    duration_ms = (time.time() - start_time) * 1000
    all_success = all(r["success"] for r in results)

    print(f"[Canary] 金丝雀执行完成: {'全部成功' if all_success else '部分失败'}")
    print(f"[Canary] 总耗时: {duration_ms:.0f}ms")

    return {
        "canary_results": results,
        "canary_success": all_success,
        "canary_duration_ms": duration_ms,
    }


def monitor(state: CanaryExecutionState) -> dict:
    """监控金丝雀执行后的健康指标。"""
    canary_targets = state["canary_targets"]
    canary_success = state["canary_success"]

    # 如果金丝雀执行失败，直接标记需要回滚
    if not canary_success:
        print("[Canary] 金丝雀执行失败，标记回滚")
        return {
            "health_metrics": {},
            "should_rollback": True,
            "rollback_reason": "金丝雀执行失败",
        }

    if not canary_targets:
        # 无目标，不需要监控
        return {
            "health_metrics": {},
            "should_rollback": False,
            "rollback_reason": "",
        }

    # 等待监控时长
    monitor_duration = int(os.getenv("AIOPS_CANARY_MONITOR_DURATION_SECONDS", "120"))
    print(f"[Canary] 等待 {monitor_duration} 秒，监控健康指标...")
    time.sleep(monitor_duration)

    # 收集健康指标
    health_metrics = {}
    for target in canary_targets:
        metrics_data = collect_health_metrics(target)
        health_metrics[target] = metrics_data

        print(
            f"[Canary] {target}: "
            f"CPU={metrics_data['cpu_usage']}%, "
            f"Mem={metrics_data['mem_usage']}%, "
            f"Disk={metrics_data['disk_usage']}%, "
            f"Error={metrics_data['error_rate']}%"
        )

    # 判断是否需要回滚
    should_rollback = False
    rollback_reason = ""

    for target, metrics_data in health_metrics.items():
        cpu_ok = metrics_data["cpu_usage"] <= 90
        mem_ok = metrics_data["mem_usage"] <= 95
        error_ok = metrics_data["error_rate"] <= 5
        service_ok = metrics_data["service_status"] == "running"

        if not (cpu_ok and mem_ok and error_ok and service_ok):
            should_rollback = True
            issues = []
            if not cpu_ok:
                issues.append(f"CPU={metrics_data['cpu_usage']}%")
            if not mem_ok:
                issues.append(f"Mem={metrics_data['mem_usage']}%")
            if not error_ok:
                issues.append(f"Error={metrics_data['error_rate']}%")
            if not service_ok:
                issues.append(f"Service={metrics_data['service_status']}")

            rollback_reason = f"{target} 指标异常: {', '.join(issues)}"
            print(f"[Canary] {rollback_reason}")
            break

    if not should_rollback:
        print("[Canary] 所有金丝雀主机健康指标正常")

    return {
        "health_metrics": health_metrics,
        "should_rollback": should_rollback,
        "rollback_reason": rollback_reason,
    }


def expand_execute(executor, state: CanaryExecutionState) -> dict:
    """金丝雀成功，扩展到剩余主机。executor 经图构建参数绑定。"""
    script = state["script_content"]
    remaining_targets = state["remaining_targets"]

    if not remaining_targets:
        print("[Canary] 无剩余主机，金丝雀即为全部目标")
        return {
            "expand_results": [],
            "total_success": True,
        }

    print(f"[Canary] 金丝雀成功，开始扩展到 {len(remaining_targets)} 台主机...")

    # 分批执行
    batch_size = int(os.getenv("AIOPS_CANARY_BATCH_SIZE", "10"))
    batch_interval = int(os.getenv("AIOPS_CANARY_BATCH_INTERVAL_SECONDS", "30"))

    all_results = []

    for i in range(0, len(remaining_targets), batch_size):
        batch = remaining_targets[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(remaining_targets) + batch_size - 1) // batch_size

        print(f"[Canary] 执行 Batch {batch_num}/{total_batches}: {len(batch)} 台主机")

        batch_results = []
        for target in batch:
            result = execute_on_target(executor, target, script)
            batch_results.append(result)

            status = "✓" if result["success"] else "✗"
            print(f"[Canary]   {target}: {status}")

        all_results.extend(batch_results)

        # 批次间等待（最后一批不等待）
        if i + batch_size < len(remaining_targets):
            print(f"[Canary] 等待 {batch_interval} 秒后执行下一批...")
            time.sleep(batch_interval)

    total_success = all(r["success"] for r in all_results)

    print(f"[Canary] 扩展执行完成: {'全部成功' if total_success else '部分失败'}")

    return {
        "expand_results": all_results,
        "total_success": total_success,
    }


def rollback(executor, state: CanaryExecutionState) -> dict:
    """金丝雀失败，回滚已执行的修复。executor 经图构建参数绑定。"""
    canary_targets = state["canary_targets"]
    canary_results = state["canary_results"]
    proposal = state["proposal"]
    alert = state["alert"]
    rollback_reason = state["rollback_reason"]

    print(f"[Canary] 开始回滚: {rollback_reason}")

    # 尝试生成回滚脚本
    rollback_script = generate_rollback_script(proposal)

    rollback_executed = False

    if rollback_script:
        print(f"[Canary] 在 {len(canary_targets)} 台金丝雀主机上执行回滚脚本...")

        for target in canary_targets:
            result = execute_on_target(executor, target, rollback_script)
            status = "✓" if result["success"] else "✗"
            print(f"[Canary]   回滚 {target}: {status}")

        rollback_executed = True
    else:
        print("[Canary] 无回滚脚本，需要人工介入")

    # 创建事故工单
    create_incident_ticket(alert, rollback_reason, canary_results)

    print(f"[Canary] 回滚完成: {'已执行回滚脚本' if rollback_executed else '需要人工介入'}")

    return {
        "rollback_executed": rollback_executed,
        "total_success": False,
    }


# ---------------------------------------------------------------------------
# Conditional edges
# ---------------------------------------------------------------------------

def should_expand_or_rollback(state: CanaryExecutionState) -> str:
    """根据监控结果决定是扩展还是回滚。"""
    if state.get("should_rollback", False):
        return "rollback"
    else:
        return "expand"


def should_execute_canary(state: CanaryExecutionState) -> str:
    """判断是否需要执行金丝雀。"""
    canary_targets = state.get("canary_targets", [])

    if not canary_targets:
        # 无目标，直接结束
        return "skip"
    else:
        return "execute"


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=2)
def build_canary_graph(executor=None):
    """构建金丝雀执行图。

    FR-4 纯数据化：executor 经 functools.partial 绑定到节点，不进 state。
    """
    builder = StateGraph(CanaryExecutionState)

    # 添加节点（executor 绑定到使用它的三个节点）
    builder.add_node("identify_targets", identify_targets)
    builder.add_node("canary_execute", functools.partial(canary_execute, executor))
    builder.add_node("monitor", monitor)
    builder.add_node("expand_execute", functools.partial(expand_execute, executor))
    builder.add_node("rollback", functools.partial(rollback, executor))

    # 添加边
    builder.add_edge(START, "identify_targets")

    # 判断是否有目标
    builder.add_conditional_edges(
        "identify_targets",
        should_execute_canary,
        {
            "execute": "canary_execute",
            "skip": END,
        }
    )

    builder.add_edge("canary_execute", "monitor")

    # 根据监控结果决定扩展或回滚
    builder.add_conditional_edges(
        "monitor",
        should_expand_or_rollback,
        {
            "expand": "expand_execute",
            "rollback": "rollback",
        }
    )

    builder.add_edge("expand_execute", END)
    builder.add_edge("rollback", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_canary_execution(
    alert: Alert,
    proposal: LLMProposal,
    script_content: str,
    executor: Any,
) -> CanaryExecutionState:
    """运行金丝雀执行图，返回最终状态。

    Args:
        alert: 告警对象
        proposal: LLM 提案
        script_content: 要执行的脚本
        executor: IExecutor 实例

    Returns:
        CanaryExecutionState: 最终状态
    """
    graph = build_canary_graph(executor)

    initial_state = {
        "alert": alert,
        "proposal": proposal,
        "script_content": script_content,

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

    return graph.invoke(initial_state)
