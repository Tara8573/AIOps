"""LangGraph 告警消费图。

将 pipeline.py 中的 run_from_source() 和批处理逻辑改造为 LangGraph StateGraph。
处理 Kafka 消息消费、清洗、过滤、批量聚合、风暴合并和优先级排序。

节点流（FR-1.1/1.2 改造后）:
  consume → clean → filter → batch → aggregate(风暴聚合) → sort → process_incidents → END

FR-1.1: consume 从有界接收队列取数（IngestWorker 线程生产）；队列满时不拉取，
        offset 不动（at-least-once）。风暴暂存的告警随批进入聚合器收编为组员。
FR-1.2: aggregate 节点按根因签名聚类，同签名合并为一个 Incident，只诊断一次
        （组代表），结果广播到组内全部成员。
"""

import functools
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, NotRequired, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from core.models import Alert
from core.observability import metrics
from core.storm_aggregator import StormIncident

# 风暴暂存标记：IngestWorker 队列满时降级入暂存的原始消息（仅聚合不单独诊断）
STORM_PARKED_KEY = "__storm_parked"

# 代表级别优先级（Incident 处理顺序）
_INCIDENT_LEVEL_PRIORITY = {"Critical": 300, "Warning": 200, "Info": 100}


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class ConsumptionState(TypedDict):
    """告警消费图的状态。"""
    pipeline: Any  # AIOpsPipeline 实例（消费图不接 checkpoint，实例可留 state）
    raw_messages: List[Dict[str, Any]]  # 原始消息列表
    cleaned_alerts: List[Alert]  # 清洗后的告警
    filtered_alerts: List[Alert]  # 过滤后的告警
    batched_alerts: List[List[Alert]]  # 分批后的告警
    incidents: NotRequired[List[StormIncident]]  # FR-1.2 聚合 incident（本轮有工作要做的）
    ticket_ids: List[str]  # 生成的工单 ID


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def consume(state: ConsumptionState) -> dict:
    """从数据源消费原始消息。

    FR-1.1：配置了 IngestQueue 时从有界队列批量取数（含风暴暂存）；
    未配置（单机/测试）时保持直接消费行为。
    """
    pipeline = state["pipeline"]

    if not pipeline.alert_source:
        return {"raw_messages": []}

    ingest_queue = getattr(pipeline, "_ingest_queue", None)
    if ingest_queue is not None:
        # 有界队列批量取数 + 取走风暴暂存（随批清洗后进聚合器）
        raw_messages = ingest_queue.get_many(max_items=100, timeout=0.5)
        raw_messages.extend(ingest_queue.take_storm_parked())
        return {"raw_messages": raw_messages}

    # 直接消费（现状行为）
    raw_messages = []
    try:
        for raw_msg in pipeline.alert_source.consume():
            raw_messages.append(raw_msg)
            # 限制单次消费数量，避免阻塞过久
            if len(raw_messages) >= 100:
                break
    except Exception as e:
        print(f"[Consumption] 消费消息时出错: {e}")

    return {"raw_messages": raw_messages}


def clean(state: ConsumptionState) -> dict:
    """清洗原始消息，转换为 Alert 对象。"""
    pipeline = state["pipeline"]
    raw_messages = state["raw_messages"]

    cleaned_alerts = []
    for raw_msg in raw_messages:
        if pipeline.alert_cleaner:
            alert = pipeline.alert_cleaner.clean(raw_msg)
            if alert is None:
                print("[Consumption] [CLEAN] 告警清洗后丢弃")
                # 清洗丢弃的消息也算"已处理"，避免卡 commit
                if pipeline.alert_source and hasattr(pipeline.alert_source, "mark_processed"):
                    pipeline.alert_source.mark_processed(raw_msg)
                continue
        else:
            try:
                alert = Alert(**raw_msg)
            except Exception as exc:
                print(f"[Consumption] [CLEAN] 无 cleaner 且原始消息无法直接转为 Alert: {exc}")
                continue

        # 把 Kafka 元信息挂到 alert 上，处理完成后用于 offset 确认
        if isinstance(raw_msg, dict) and "__kafka_offset" in raw_msg:
            try:
                object.__setattr__(alert, "_kafka_raw", raw_msg)
            except Exception:
                pass
        cleaned_alerts.append(alert)

    return {"cleaned_alerts": cleaned_alerts}


def filter_alerts(state: ConsumptionState) -> dict:
    """过滤告警，保留需要处理的。

    FR-1.3：L2 档先于普通过滤执行 —— 只放行 Critical，其余暂存 Kafka
    （offset 不动 → 不 mark，恢复后重放）；与 alert_filter 是否配置无关。
    """
    pipeline = state["pipeline"]
    cleaned_alerts = state["cleaned_alerts"]
    degradation = getattr(pipeline, "_degradation", None)

    # FR-1.3: L2 档只处理 Critical（先于普通过滤，规格 FR-1.3 L2）
    if degradation is not None and degradation.is_l2():
        kept = [a for a in cleaned_alerts if a.level == "Critical"]
        dropped = len(cleaned_alerts) - len(kept)
        if dropped:
            metrics.incr("aiops_degradation_l2_filtered", dropped)
            print(f"[Consumption] [L2] {dropped} 条非 Critical 暂存 Kafka（offset 不动）")
        cleaned_alerts = kept

    if not pipeline.alert_filter:
        return {"filtered_alerts": cleaned_alerts}

    filtered_alerts = []
    for alert in cleaned_alerts:
        if pipeline.alert_filter.should_process(alert):
            filtered_alerts.append(alert)
        else:
            print(f"[Consumption] [FILTER] 告警被过滤 | alert_id={alert.alert_id}")
            raw = getattr(alert, "_kafka_raw", None)
            if raw and pipeline.alert_source and hasattr(pipeline.alert_source, "mark_processed"):
                pipeline.alert_source.mark_processed(raw)

    return {"filtered_alerts": filtered_alerts}


def batch(state: ConsumptionState) -> dict:
    """将告警分批。"""
    filtered_alerts = state["filtered_alerts"]

    # 从环境变量读取 batch_size
    batch_size = max(1, int(os.getenv("AIOPS_BATCH_SIZE", "1")))

    # 分批
    batched_alerts = []
    for i in range(0, len(filtered_alerts), batch_size):
        batched_alerts.append(filtered_alerts[i:i + batch_size])

    return {"batched_alerts": batched_alerts}


def aggregate(state: ConsumptionState) -> dict:
    """风暴聚合（FR-1.2）：按根因签名聚类，同签名合并为 Incident。

    - 配置了 StormAggregator：签名 = 意图类别 + 主机 + 关键实体；
      新签名建组（首条为代表，本轮即处理）；晚到成员入组（代表结果已出则广播）。
    - 未配置：每条告警自成一个单成员 Incident（等价现状行为）。
    """
    pipeline = state["pipeline"]
    aggregator = getattr(pipeline, "_storm_aggregator", None)
    batched = state.get("batched_alerts") or []
    alerts = [a for b in batched for a in b]

    if aggregator is None:
        incidents = []
        for alert in alerts:
            inc = StormIncident(
                group_id=f"inc-{uuid.uuid4().hex[:12]}",
                signature=alert.alert_id,
                intent_category="",
                host="",
            )
            inc.representative = alert
            inc.members.append(alert)
            incidents.append(inc)
        return {"incidents": incidents, "batched_alerts": []}

    work = aggregator.aggregate(alerts)
    return {"incidents": work, "batched_alerts": []}


def sort_incidents(state: ConsumptionState) -> dict:
    """对 incident 按代表优先级排序（Critical 优先处理）。"""
    pipeline = state["pipeline"]
    incidents = state.get("incidents") or []

    pending = [inc for inc in incidents if inc.representative is not None]
    routed = [(inc, pipeline.router.decide(inc.representative)) for inc in pending]
    from core.routing import sort_alerts_by_priority
    ordered_pairs = sort_alerts_by_priority(
        [(inc.representative, rd) for inc, rd in routed]
    )
    rep_to_incident = {inc.representative.alert_id: inc for inc, _ in routed}
    sorted_incidents = [
        rep_to_incident[rep.alert_id] for rep, _ in ordered_pairs
        if rep.alert_id in rep_to_incident
    ]

    return {"incidents": sorted_incidents}


def process_incidents(state: ConsumptionState) -> dict:
    """处理所有 incident（FR-1.2）：代表诊断一次，结果广播到组内。

    - 新组（result 为空）：诊断代表（worker pool 并行 / 串行），完成后广播
    - 已有结果 + 未广播成员（晚到）：仅广播
    - 本轮全部处理完毕后统一 commit offset（at-least-once 连续前缀）
    """
    pipeline = state["pipeline"]
    incidents = state.get("incidents") or []
    aggregator = getattr(pipeline, "_storm_aggregator", None)
    source = pipeline.alert_source
    worker_pool = getattr(pipeline, "_worker_pool", None)

    all_ticket_ids: List[str] = []

    def _commit_member(member: Alert, incident: StormIncident):
        """成员广播回调：mark_processed + 成员结果消息发布。"""
        raw = getattr(member, "_kafka_raw", None)
        if raw and source and hasattr(source, "mark_processed"):
            source.mark_processed(raw)
        producer = getattr(pipeline, "message_producer", None)
        if producer and member is not incident.representative:
            try:
                producer.publish(
                    topic="aiops-results",
                    key=member.alert_id,
                    value={
                        "alert_id": member.alert_id,
                        "title": member.title,
                        "level": member.level,
                        "outcome": (
                            "resolved_by_incident"
                            if incident.execution_success
                            else "incident_failed"
                        ),
                        "incident_id": incident.group_id,
                        "representative_alert_id": (
                            incident.representative.alert_id if incident.representative else None
                        ),
                        "ticket_id": incident.ticket_id or None,
                        "intent_category": incident.intent_category,
                        "host": incident.host,
                        "timestamp": datetime.now().isoformat(),
                    },
                )
            except Exception as exc:
                print(f"[Consumption] [BROADCAST] 成员结果发布失败: {exc}")

    def _process_one(incident: StormIncident):
        """诊断代表 + 广播组内全部成员。"""
        # FR-1.3: L1+ 档 Info 代表不做 LLM 诊断（只诊断高优先级，规格 FR-1.3 L1）
        degradation = getattr(pipeline, "_degradation", None)
        if (degradation is not None and degradation.is_l1_or_higher()
                and incident.representative.level == "Info"):
            incident.result = {
                "ticket_id": "",
                "execution_success": None,
                "skipped": "L1+ 降级档，低优先级不单独诊断",
            }
            metrics.incr("aiops_degradation_l1_skipped_low_priority")
            if aggregator is not None:
                aggregator.broadcast(incident, commit_cb=_commit_member)
            else:
                _commit_member(incident.representative, incident)
            return

        final_state = run_pipeline_safe(incident.representative, pipeline)
        incident.result = final_state
        ticket_id = final_state.get("ticket_id")
        if ticket_id:
            all_ticket_ids.append(ticket_id)
        if aggregator is not None:
            aggregator.broadcast(incident, commit_cb=_commit_member)
        else:
            _commit_member(incident.representative, incident)

    fresh = [inc for inc in incidents if inc.representative is not None and inc.result is None]

    if worker_pool is not None:
        futures = [worker_pool.submit(_process_one, inc) for inc in fresh]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                metrics.incr("pipeline.workers.error")
                print(f"[Consumption] [ERROR] Incident 处理异常: {exc}")
    else:
        for incident in fresh:
            _process_one(incident)

    # 晚到成员广播（broadcast_ids 去重，幂等）
    for incident in incidents:
        if incident.result is not None and aggregator is not None:
            aggregator.broadcast(incident, commit_cb=_commit_member)

    # 本轮全部处理完毕，确认 offset（at-least-once）
    if source and hasattr(source, "commit_processed"):
        source.commit_processed()

    return {"ticket_ids": all_ticket_ids}


def run_pipeline_safe(alert: Alert, pipeline: Any) -> dict:
    """单告警流水线入口（延迟导入，避免循环依赖）。"""
    from core.pipeline_graph import run_pipeline
    return run_pipeline(alert=alert, pipeline=pipeline)


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def build_consumption_graph():
    """构建告警消费图。"""
    builder = StateGraph(ConsumptionState)

    builder.add_node("consume", consume)
    builder.add_node("clean", clean)
    builder.add_node("filter", filter_alerts)
    builder.add_node("batch", batch)
    builder.add_node("aggregate", aggregate)
    builder.add_node("sort", sort_incidents)
    builder.add_node("process_incidents", process_incidents)

    builder.add_edge(START, "consume")
    builder.add_edge("consume", "clean")
    builder.add_edge("clean", "filter")
    builder.add_edge("filter", "batch")
    builder.add_edge("batch", "aggregate")
    builder.add_edge("aggregate", "sort")
    builder.add_edge("sort", "process_incidents")
    builder.add_edge("process_incidents", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_consumption(pipeline: Any) -> ConsumptionState:
    """运行告警消费图，返回最终状态。

    异常时返回空状态，不中断消费循环。
    """
    graph = build_consumption_graph()

    initial_state = {
        "pipeline": pipeline,
        "raw_messages": [],
        "cleaned_alerts": [],
        "filtered_alerts": [],
        "batched_alerts": [],
        "incidents": [],
        "ticket_ids": [],
    }

    try:
        return graph.invoke(initial_state)
    except Exception as exc:
        print(f"[Consumption] [ERROR] 消费图执行异常: {exc}")
        return initial_state
