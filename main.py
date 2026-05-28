import os
from pathlib import Path

from dotenv import load_dotenv

from core.models import Alert
from core.logger import LogConfig, get_logger
from core.observability import metrics
from core.processor import AlertCleaner, AlertFilter
from core.skills import SkillRegistry
from plugins.mocks import (
    MockApprovalGate,
    MockExecutor,
    MockJiraTicketing,
    MockKnowledgeBase,
)
from plugins.openai_client import LLMClientFactory
from plugins.skills import DiskCleanupSkill
from pipeline import AIOpsPipeline

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
LogConfig.setup(
    log_dir=os.getenv("AIOPS_LOG_DIR", "./logs"),
    log_level=os.getenv("AIOPS_LOG_LEVEL", "INFO"),
    json_format=os.getenv("AIOPS_LOG_JSON", "0") == "1",
)
app_logger = get_logger("main")


def build_kb():
    if os.getenv("AIOPS_USE_PG_KB", "0") != "1":
        return MockKnowledgeBase()

    from plugins.postgres_kb import PostgresVectorKnowledgeBase

    kb = PostgresVectorKnowledgeBase(
        dsn=os.getenv("AIOPS_PG_DSN"),
        vector_dim=int(os.getenv("AIOPS_PG_VECTOR_DIM", "1024")),
        top_k=int(os.getenv("AIOPS_PG_TOP_K", "3")),
    )
    kb.seed_experience(
        alert_feature="disk usage /var critical",
        content="历史SOP: 发现磁盘空间满可清理 /var/log 并检查大文件来源",
    )
    return kb


def build_jira_retriever():
    if os.getenv("AIOPS_USE_JIRA_KNOWLEDGE", "0") != "1":
        return None

    from knowledge.jira_retriever import JiraKnowledgeRetriever
    from knowledge.jira_store import SQLiteJiraKnowledgeStore

    return JiraKnowledgeRetriever(
        store=SQLiteJiraKnowledgeStore(os.getenv("JIRA_KNOWLEDGE_DB")),
        top_k=int(os.getenv("JIRA_KNOWLEDGE_TOP_K", "5")),
    )


def _resolve_env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        value = value.strip()
        if not value:
            continue
        if value.isupper() and "_" in value:
            indirect_value = os.getenv(value, "").strip()
            if indirect_value:
                return indirect_value
        return value
    return default


def build_llm():
    provider = os.getenv("AIOPS_LLM_PROVIDER", "openai").strip().lower()

    if provider == "openai":
        api_key = _resolve_env_value(
            "AIOPS_LLM_API_KEY",
        )
        base_url = _resolve_env_value(
            "AIOPS_LLM_BASE_URL",
            default="https://api.openai.com/v1",
        )
        model = _resolve_env_value(
            "AIOPS_LLM_MODEL",
            default="deepseek-chat",
        )
        return LLMClientFactory.create(
            provider="openai",
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=float(os.getenv("AIOPS_LLM_TEMPERATURE", "0.2")),
            max_tokens=int(os.getenv("AIOPS_LLM_MAX_TOKENS", "2000")),
            timeout=int(os.getenv("AIOPS_LLM_TIMEOUT", "60")),
        )

    if provider == "ollama":
        model = _resolve_env_value(
            "AIOPS_LLM_MODEL",
            "OLLAMA_MODEL",
            default="qwen2.5:7b",
        )
        base_url = _resolve_env_value(
            "AIOPS_LLM_BASE_URL",
            "OLLAMA_BASE_URL",
            default="http://localhost:11434",
        )
        return LLMClientFactory.create(
            provider="ollama",
            model=model,
            base_url=base_url,
            temperature=float(os.getenv("AIOPS_LLM_TEMPERATURE", "0.2")),
            timeout=int(os.getenv("AIOPS_LLM_TIMEOUT", "120")),
        )

    raise ValueError("AIOPS_LLM_PROVIDER 仅支持 openai 或 ollama")


def build_kafka_source():
    from plugins.kafka_source import KafkaAlertSource

    bootstrap_servers = os.getenv("AIOPS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("AIOPS_KAFKA_TOPIC", "aiops-alerts")
    group_id = os.getenv("AIOPS_KAFKA_GROUP_ID", "aiops-consumer")
    auto_offset_reset = os.getenv("AIOPS_KAFKA_AUTO_OFFSET_RESET", "latest")
    poll_timeout_ms = int(os.getenv("AIOPS_KAFKA_POLL_TIMEOUT_MS", "1000"))
    max_poll_records = int(os.getenv("AIOPS_KAFKA_MAX_POLL_RECORDS", "100"))

    security_protocol = os.getenv("AIOPS_KAFKA_SECURITY_PROTOCOL") or None
    sasl_mechanism = os.getenv("AIOPS_KAFKA_SASL_MECHANISM") or None
    sasl_username = os.getenv("AIOPS_KAFKA_SASL_USERNAME") or None
    sasl_password = os.getenv("AIOPS_KAFKA_SASL_PASSWORD") or None

    return KafkaAlertSource(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        poll_timeout_ms=poll_timeout_ms,
        max_poll_records=max_poll_records,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )


def build_cleaner():
    field_map_str = os.getenv("AIOPS_KAFKA_FIELD_MAP", "")
    field_map = {}
    if field_map_str:
        for pair in field_map_str.split(","):
            if ":" in pair:
                target, source = pair.split(":", 1)
                field_map[target.strip()] = source.strip()

    dedup_window = int(os.getenv("AIOPS_KAFKA_DEDUP_WINDOW", "300"))

    return AlertCleaner(
        field_map=field_map or None,
        default_source="kafka",
        dedup_window=dedup_window,
    )


def build_filter():
    min_level = os.getenv("AIOPS_KAFKA_MIN_LEVEL", "Info")
    include_patterns_str = os.getenv("AIOPS_KAFKA_INCLUDE_PATTERNS", "")
    exclude_patterns_str = os.getenv("AIOPS_KAFKA_EXCLUDE_PATTERNS", "")
    exclude_sources_str = os.getenv("AIOPS_KAFKA_EXCLUDE_SOURCES", "")
    title_exclude_str = os.getenv("AIOPS_KAFKA_TITLE_EXCLUDE_PATTERNS", "")

    include_patterns = [p.strip() for p in include_patterns_str.split(",") if p.strip()]
    exclude_patterns = [p.strip() for p in exclude_patterns_str.split(",") if p.strip()]
    exclude_sources = [s.strip() for s in exclude_sources_str.split(",") if s.strip()]
    title_exclude = [p.strip() for p in title_exclude_str.split(",") if p.strip()]

    return AlertFilter(
        min_level=min_level,
        include_patterns=include_patterns or None,
        exclude_patterns=exclude_patterns or None,
        exclude_sources=exclude_sources or None,
        title_exclude_patterns=title_exclude or None,
    )


def demo():
    print("=" * 50)
    print("AIOps 智能告警全自动处理框架 LLM Demo")
    print("=" * 50)

    kb = build_kb()
    llm = build_llm()
    executor = MockExecutor()
    approval_gate = MockApprovalGate()
    ticketing = MockJiraTicketing()
    skill_registry = SkillRegistry([DiskCleanupSkill()])
    jira_retriever = build_jira_retriever()

    alert = Alert(
        alert_id="ALT-2023-0901",
        title="Payment Service - Disk Space Critical",
        level="Critical",
        content="Host 10.10.1.2 disk usage > 98% in /var",
    )

    print("\n>>> 【真实 LLM 测试】 生成告警分析与处理建议")
    pipeline = AIOpsPipeline(
        kb=kb,
        llm=llm,
        executor=executor,
        approval_gate=approval_gate,
        ticketing=ticketing,
        skill_registry=skill_registry,
        jira_knowledge_retriever=jira_retriever,
    )
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

    kb = build_kb()
    llm = build_llm()
    executor = MockExecutor()
    approval_gate = MockApprovalGate()
    ticketing = MockJiraTicketing()
    skill_registry = SkillRegistry([DiskCleanupSkill()])
    jira_retriever = build_jira_retriever()

    kafka_source = build_kafka_source()
    cleaner = build_cleaner()
    alert_filter = build_filter()

    pipeline = AIOpsPipeline(
        kb=kb,
        llm=llm,
        executor=executor,
        approval_gate=approval_gate,
        ticketing=ticketing,
        skill_registry=skill_registry,
        alert_source=kafka_source,
        alert_cleaner=cleaner,
        alert_filter=alert_filter,
        jira_knowledge_retriever=jira_retriever,
    )

    print(
        f"\n[Kafka] bootstrap_servers={os.getenv('AIOPS_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}"
    )
    print(f"[Kafka] topic={os.getenv('AIOPS_KAFKA_TOPIC', 'aiops-alerts')}")
    print(f"[Kafka] group_id={os.getenv('AIOPS_KAFKA_GROUP_ID', 'aiops-consumer')}")
    print(f"[Cleaner] dedup_window={os.getenv('AIOPS_KAFKA_DEDUP_WINDOW', '300')}s")
    print(f"[Filter] min_level={os.getenv('AIOPS_KAFKA_MIN_LEVEL', 'Info')}")
    print("\n>>> 按 Ctrl+C 停止消费\n")

    pipeline.run_from_source()
    app_logger.health_snapshot()
    print(f"[Metrics] {metrics.snapshot()}")


if __name__ == "__main__":
    mode = os.getenv("AIOPS_MODE", "demo").strip().lower()
    if mode == "kafka":
        run_kafka()
    else:
        demo()
