# AIOps Plugins Package

from .mocks import (
    MockApprovalGate,
    MockExecutor,
    MockJiraTicketing,
    MockKnowledgeBase,
    MockLLMClient,
)

try:
    from .postgres_kb import PostgresVectorKnowledgeBase
except Exception:  # pragma: no cover
    PostgresVectorKnowledgeBase = None

try:
    from .kafka_source import KafkaAlertSource
except Exception:  # pragma: no cover
    KafkaAlertSource = None

__all__ = [
    "MockKnowledgeBase",
    "MockApprovalGate",
    "MockExecutor",
    "MockJiraTicketing",
    "MockLLMClient",
    "PostgresVectorKnowledgeBase",
    "KafkaAlertSource",
]
