import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from core.models import Alert
from core.circuit_breaker import CircuitBreakerLLM
from core.concurrent_pool import LLMConcurrencyLimiter
from core.llm_observed import ObservedLLMClient
from core.metrics_export import start_metrics_server
from core.config import load_settings
from core.logger import LogConfig, get_logger
from core.observability import metrics
from core.processor import AlertCleaner, AlertFilter
from core.skills import SkillRegistry
from plugins.mocks import (
    MockApprovalGate,
    MockJiraTicketing,
    MockKnowledgeBase,
    MockLogQuerier,
    MockMetricsQuerier,
)
from plugins.openai_client import LLMClientFactory
from plugins.skills import DiskCleanupSkill, MemoryDiagnosisSkill, NetworkDiagnosisSkill, ServiceHealthSkill
from pipeline import AIOpsPipeline

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
settings = load_settings()
LogConfig.setup(
    log_dir=settings.get("runtime.log_dir", "./logs"),
    log_level=settings.get("runtime.log_level", "INFO"),
    json_format=settings.get("runtime.log_json", False),
)
app_logger = get_logger("main")


class StartupViolation(RuntimeError):
    """STRICT 启动自检失败（FR-8）。"""


def _strict_startup_check():
    """FR-8: STRICT=1 启动硬自检 —— 沙箱依赖的执行面，Docker 不可用则拒绝启动。

    规格：STRICT=1 且 Docker 不可用时，进程启动即报错，而非降级本地执行。
    沙箱未启用（AIOPS_SANDBOX_ENABLED=0，mock 执行面）时不检查。
    """
    if os.getenv("AIOPS_SANDBOX_ENABLED", "0") != "1":
        return
    if os.getenv("AIOPS_SANDBOX_STRICT", "1") != "1":
        return
    from plugins.docker_sandbox import DockerSandbox
    if not DockerSandbox().is_available():
        raise StartupViolation(
            "STRICT=1 且 Docker 不可用，拒绝启动自动执行（规格 FR-8：启动即报错，"
            "不降级本地执行）。如需继续请设置 AIOPS_SANDBOX_STRICT=0（不推荐）"
            "或配置沙箱环境。"
        )


def resolve_secret_ref(value: Optional[str], strict: bool = False) -> Optional[str]:
    """解析 secretRef（FR-8 凭据集中管理）：

    - vault://path#key → Vault 凭据（用时取、不留存）
    - env://VAR_NAME   → 环境变量读取
    - 其他值原样返回（兼容明文，生产不建议）
    """
    if not value or "://" not in value:
        return value
    from plugins.credential_manager import credential_provider_factory
    if value.startswith("vault://"):
        path, _, key = value[len("vault://"):].partition("#")
        provider = credential_provider_factory(strict=strict)
        return provider.get_secret(path, key or None)
    if value.startswith("env://"):
        return os.getenv(value[len("env://"):], "")
    return value


def build_executor():
    """FR-5: 执行器装配（AIOPS_EXECUTOR_MODE=docker|remote）。

    - remote: MQ 模式远程执行（设备侧 agent，docs/executor-agent-协议.md）
    - docker: 本地沙箱（demo/测试实现）
    """
    mode = os.getenv("AIOPS_EXECUTOR_MODE", "docker").strip().lower()
    if mode == "remote":
        from plugins.remote_executor import RemoteExecutor
        from plugins.kafka_producer import KafkaMessageProducer
        producer = None
        if settings.get("kafka.producer_enabled", False) or os.getenv("AIOPS_EXEC_PRODUCER_ENABLED", "1") == "1":
            producer = KafkaMessageProducer(
                bootstrap_servers=settings.get("kafka.bootstrap_servers", "localhost:9092"),
                security_protocol=settings.get("kafka.security_protocol") or None,
                sasl_mechanism=settings.get("kafka.sasl_mechanism") or None,
                sasl_username=resolve_secret_ref(settings.get("kafka.sasl_username") or None),
                sasl_password=resolve_secret_ref(settings.get("kafka.sasl_password") or None),
            )
        return RemoteExecutor(producer=producer)
    from plugins.mocks import MockExecutor
    return MockExecutor()


def build_kb():
    if not settings.get("kb.use_pg", False):
        return MockKnowledgeBase()

    from plugins.postgres_kb import PostgresVectorKnowledgeBase

    kb = PostgresVectorKnowledgeBase(
        dsn=settings.get("kb.pg.dsn", ""),
        vector_dim=settings.get("kb.pg.vector_dim", 1024),
        top_k=settings.get("kb.pg.top_k", 3),
    )
    kb.seed_experience(
        alert_feature="disk usage /var critical",
        content="历史SOP: 发现磁盘空间满可清理 /var/log 并检查大文件来源",
    )
    return kb


def build_jira_retriever():
    if not settings.get("jira.enabled", False):
        return None

    from knowledge.jira_retriever import JiraKnowledgeRetriever
    from knowledge.jira_store import SQLiteJiraKnowledgeStore

    return JiraKnowledgeRetriever(
        store=SQLiteJiraKnowledgeStore(settings.get("jira.db", "data/jira_knowledge.db")),
        top_k=settings.get("jira.top_k", 5),
    )


def _build_llm_client(provider: str, api_key: str, base_url: str, model: str,
                       temperature: float, timeout: int, max_tokens: int = 2000):
    """根据 provider 创建 LLM 客户端。api_key 支持 secretRef（vault:// / env://）。"""
    api_key = resolve_secret_ref(api_key) or ""
    # LLMO-1.2: 观测层最内层（紧贴 provider client），早于 _wrap_llm 的熔断/配额链。
    # 最终链：熔断/配额链（_wrap_llm） → ObservedLLMClient → provider client。
    if provider == "openai":
        return ObservedLLMClient(LLMClientFactory.create(
            provider="openai",
            api_key=api_key,
            base_url=base_url or "https://api.openai.com/v1",
            model=model or "deepseek-chat",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        ))
    elif provider == "ollama":
        return ObservedLLMClient(LLMClientFactory.create(
            provider="ollama",
            model=model or "qwen2.5:7b",
            base_url=base_url or "http://localhost:11434",
            temperature=temperature,
            timeout=timeout,
        ))
    else:
        raise ValueError(f"LLM provider 仅支持 openai 或 ollama，当前: {provider}")


def _wrap_llm(client):
    """LLM 包装链（FR-1.3/2.2）：熔断 → 全局配额（Redis 信号量，未配 Redis 时跳过）→
    本地 Semaphore → client（LLMO-1.2：client 已是 ObservedLLMClient 观测层，见
    _build_llm_client）。所有 LLM 客户端（主/Agent）统一走此链。"""
    from core.llm_quota import RedisLLMConcurrencyLimiter
    from plugins.redis_client import get_redis
    inner = LLMConcurrencyLimiter(client)
    r = get_redis()
    limiter = RedisLLMConcurrencyLimiter(inner, redis_client=r) if r is not None else inner
    return CircuitBreakerLLM(limiter)


def build_llm():
    provider = settings.get("llm.provider", "openai").strip().lower()
    client = _build_llm_client(
        provider=provider,
        api_key=settings.get("llm.api_key", ""),
        base_url=settings.get("llm.base_url", "https://api.openai.com/v1"),
        model=settings.get("llm.model", ""),
        temperature=settings.get("llm.temperature", 0.2),
        timeout=settings.get("llm.timeout", 180),
        max_tokens=settings.get("llm.max_tokens", 2000),
    )
    return _wrap_llm(client)


def build_llm_for_agent(agent_name: str):
    """为指定 Agent 构建独立 LLM 客户端。

    如果 provider 为 'default' 或未设置，返回 None（使用共享 LLM）。
    """
    prefix = f"agents.{agent_name}.llm"
    provider = settings.get(f"{prefix}.provider", "default").strip().lower()

    if provider == "default":
        return None

    client = _build_llm_client(
        provider=provider,
        api_key=settings.get(f"{prefix}.api_key", "") or settings.get("llm.api_key", ""),
        base_url=settings.get(f"{prefix}.base_url", "") or settings.get("llm.base_url", ""),
        model=settings.get(f"{prefix}.model", "") or settings.get("llm.model", ""),
        temperature=settings.get(f"{prefix}.temperature") or settings.get("llm.temperature", 0.2),
        timeout=settings.get(f"{prefix}.timeout") or settings.get("llm.timeout", 180),
        max_tokens=settings.get(f"{prefix}.max_tokens") or settings.get("llm.max_tokens", 2000),
    )
    return _wrap_llm(client)


def build_kafka_source():
    from plugins.kafka_source import KafkaAlertSource

    return KafkaAlertSource(
        bootstrap_servers=settings.get("kafka.bootstrap_servers", "localhost:9092"),
        topic=settings.get("kafka.topic", "aiops-alerts"),
        group_id=settings.get("kafka.group_id", "aiops-consumer"),
        auto_offset_reset=settings.get("kafka.auto_offset_reset", "latest"),
        poll_timeout_ms=settings.get("kafka.poll_timeout_ms", 1000),
        max_poll_records=settings.get("kafka.max_poll_records", 100),
        security_protocol=settings.get("kafka.security_protocol") or None,
        sasl_mechanism=settings.get("kafka.sasl_mechanism") or None,
        sasl_username=settings.get("kafka.sasl_username") or None,
        sasl_password=resolve_secret_ref(settings.get("kafka.sasl_password") or None),  # FR-8 secretRef
    )


def build_cleaner():
    field_map = settings.get("kafka.field_map") or None
    dedup_window = settings.get("kafka.dedup_window", 300)

    return AlertCleaner(
        field_map=field_map,
        default_source="kafka",
        dedup_window=dedup_window,
    )


def build_filter(config_provider=None):
    return AlertFilter(
        min_level=settings.get("kafka.min_level", "Info"),
        include_patterns=settings.get("kafka.include_patterns") or None,
        exclude_patterns=settings.get("kafka.exclude_patterns") or None,
        exclude_sources=settings.get("kafka.exclude_sources") or None,
        title_exclude_patterns=settings.get("kafka.title_exclude_patterns") or None,
        config_provider=config_provider,  # FR-7: 热参数（min_level/patterns 不重启生效）
    )


def build_runtime_config_provider():
    """FR-7: 运行时热配置提供者（PG 表 runtime_config + 5s 刷新；表缺失回落默认值）。"""
    if os.getenv("AIOPS_RUNTIME_CONFIG_ENABLED", "1") != "1":
        return None
    from core.runtime_config import RuntimeConfigProvider
    try:
        provider = RuntimeConfigProvider()
        provider.start()
        return provider
    except Exception as exc:
        print(f"[Pipeline] [FR-7] 热配置未启用（回落 env/yaml 默认值）: {exc}")
        return None


def build_producer():
    """构建 Kafka 消息生产者（可选）。"""
    if not settings.get("kafka.producer_enabled", False):
        return None

    from plugins.kafka_producer import KafkaMessageProducer
    return KafkaMessageProducer(
        bootstrap_servers=settings.get("kafka.bootstrap_servers", "localhost:9092"),
        security_protocol=settings.get("kafka.security_protocol") or None,
        sasl_mechanism=settings.get("kafka.sasl_mechanism") or None,
        sasl_username=settings.get("kafka.sasl_username") or None,
        sasl_password=settings.get("kafka.sasl_password") or None,
    )


def build_approval_store():
    """FR-3：async 审批模式时装配审批 store（PG 优先，无 DSN 降级 InMemory）。"""
    if os.getenv("AIOPS_APPROVAL_MODE", "sync").strip().lower() != "async":
        return None
    from plugins.pg_approval_store import PgApprovalRequestStore, InMemoryApprovalRequestStore
    from core.config import pg_dsn
    dsn = pg_dsn()
    if dsn:
        try:
            store = PgApprovalRequestStore(dsn)
            print("[Pipeline] [FR-3] 审批 store：PG（异步审批）")
            return store
        except Exception as exc:
            print(f"[Pipeline] [FR-3] 审批 PG store 创建失败，降级 InMemory: {exc}")
    else:
        print("[Pipeline] [FR-3] 未配置 PG DSN，审批 store 降级 InMemory（仅进程内可恢复）")
    return InMemoryApprovalRequestStore()


def build_pipeline(alert_source=None, alert_cleaner=None, alert_filter=None, runtime_config=None):
    """构建主干 pipeline（demo/kafka 模式与僵死 run 巡检脚本共用装配）。"""
    # FR-8: STRICT=1 启动硬自检（沙箱依赖执行面，Docker 不可用拒绝启动）
    _strict_startup_check()

    # FR-4：AIOPS_CHECKPOINT_ENABLED=1 时 AIOpsPipeline 自动构建
    # PostgresSaver + pipeline_runs 登记（run_id 作为 checkpoint thread_id）。
    skill_registry = SkillRegistry([
        DiskCleanupSkill(),
        MemoryDiagnosisSkill(),
        NetworkDiagnosisSkill(),
        ServiceHealthSkill(),
    ])
    llm = build_llm()
    pipeline = AIOpsPipeline(
        kb=build_kb(),
        llm=llm,
        executor=build_executor(),  # FR-5: AIOPS_EXECUTOR_MODE=docker|remote（修复原硬编码 Mock）
        approval_gate=MockApprovalGate(),
        ticketing=MockJiraTicketing(),
        approval_store=build_approval_store(),  # FR-3: async 模式必需（修复原无装配点）
        skill_registry=skill_registry,
        alert_source=alert_source,
        alert_cleaner=alert_cleaner,
        alert_filter=alert_filter,
        runtime_config=runtime_config,  # FR-7: 热配置提供者
        jira_knowledge_retriever=build_jira_retriever(),
        log_querier=MockLogQuerier(),
        metrics_querier=MockMetricsQuerier(),
        diagnosis_llm=build_llm_for_agent("diagnosis"),
        evaluation_llm=build_llm_for_agent("evaluation"),
        feedback_llm=build_llm_for_agent("feedback"),
        message_producer=build_producer(),
    )
    # FR-1.3: 熔断器引用（降级控制器信号源）
    pipeline.llm_breaker = llm if isinstance(llm, CircuitBreakerLLM) else None
    return pipeline


def _maybe_start_resumer(pipeline):
    """FR-3：async 审批模式时启动 ApprovalResumer 恢复线程（含超时 sweeper）。"""
    if getattr(pipeline, "approval_mode", "sync") == "async" and getattr(pipeline, "approval_store", None):
        from core.approval_resumer import ApprovalResumer
        resumer = ApprovalResumer(pipeline, pipeline.approval_store)
        resumer.start()
        return resumer
    return None


def _maybe_start_approval_api(store=None, auth_provider=None):
    """FR-3：可选进程内启动审批回调 API（AIOPS_APPROVAL_API_ENABLED=1）。"""
    if os.getenv("AIOPS_APPROVAL_API_ENABLED", "0") == "1":
        from interfaces.approval_api import start_approval_api
        return start_approval_api(store=store, auth_provider=auth_provider)
    return None


def _maybe_start_web_api(pipeline=None, run_registry=None, audit_store=None):
    """前端 BFF：可选进程内启动 Web API（AIOPS_WEB_API_ENABLED=1，端口 9093）。

    规格：docs/前端展示与交互架构设计.md §3（读模型 + 审批命令复用，明文按 RBAC 分级）。
    """
    if os.getenv("AIOPS_WEB_API_ENABLED", "0") != "1":
        return None
    from interfaces.web_api import start_web_api
    from core.audit import get_audit_store
    from core.llm_call_log import llm_call_log  # ObservedLLMClient 使用的全局单例
    return start_web_api(
        store=getattr(pipeline, "approval_store", None),
        llm_log=llm_call_log,
        # 修复：传入 run_registry/audit_store（原恒 None → stale_runs/trace 审计事件恒空）
        run_registry=run_registry or getattr(pipeline, "run_registry", None),
        audit_store=audit_store or get_audit_store(),
        pipeline=pipeline,
    )


def demo():
    print("=" * 50)
    print("AIOps 智能告警全自动处理框架 LLM Demo")
    print("=" * 50)

    alert = Alert(
        alert_id="ALT-2023-0901",
        title="Payment Service - High Latency Alert",
        level="Critical",
        content="Service payment-gateway p99 latency > 5000ms, upstream connection pool exhausted, error rate 12%",
    )

    print("\n>>> 【真实 LLM 测试】 生成告警分析与处理建议")
    pipeline = build_pipeline()
    _maybe_start_resumer(pipeline)
    ticket_id = pipeline.process_alert(alert)

    if ticket_id:
        print("\n>>> 【知识回流测试】 同步工单反馈并沉淀经验")
        pipeline.sync_feedbacks([ticket_id])
    app_logger.health_snapshot()
    print(f"[Metrics] {metrics.snapshot()}")


def run_kafka():
    print("=" * 50)
    print("AIOps Kafka 告警持续消费模式")
    print("=" * 50)

    runtime_config = build_runtime_config_provider()
    kafka_source = build_kafka_source()
    cleaner = build_cleaner()
    alert_filter = build_filter(config_provider=runtime_config)

    pipeline = build_pipeline(
        alert_source=kafka_source,
        alert_cleaner=cleaner,
        alert_filter=alert_filter,
        runtime_config=runtime_config,
    )

    resumer = _maybe_start_resumer(pipeline)
    _maybe_start_approval_api(store=pipeline.approval_store)
    _maybe_start_web_api(pipeline=pipeline)
    # FR-6: Prometheus /metrics 导出（AIOPS_METRICS_ENABLED=1）
    exporter = start_metrics_server(pipeline)

    print(
        f"\n[Kafka] bootstrap_servers={settings.get('kafka.bootstrap_servers', 'localhost:9092')}"
    )
    print(f"[Kafka] topic={settings.get('kafka.topic', 'aiops-alerts')}")
    print(f"[Kafka] group_id={settings.get('kafka.group_id', 'aiops-consumer')}")
    print(f"[Cleaner] dedup_window={settings.get('kafka.dedup_window', 300)}s")
    print(f"[Filter] min_level={settings.get('kafka.min_level', 'Info')}")
    print("\n>>> 按 Ctrl+C 停止消费\n")

    pipeline.run_from_source()
    if resumer:
        resumer.stop()
    if exporter:
        exporter.stop()
    app_logger.health_snapshot()
    print(f"[Metrics] {metrics.snapshot()}")


if __name__ == "__main__":
    mode = settings.get("runtime.mode", "demo").strip().lower()
    if mode == "kafka":
        run_kafka()
    else:
        demo()
