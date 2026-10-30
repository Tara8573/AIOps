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

try:
    from .rule_decision_client import RuleDecisionClient
except Exception:  # pragma: no cover
    RuleDecisionClient = None

try:
    from .jev_decision_client import (
        JevDecisionClient,
        JevDecisionClientStub,
        create_jev_client,
    )
except Exception:  # pragma: no cover
    JevDecisionClient = None
    JevDecisionClientStub = None
    create_jev_client = None

__all__ = [
    "MockKnowledgeBase",
    "MockApprovalGate",
    "MockExecutor",
    "MockJiraTicketing",
    "MockLLMClient",
    "PostgresVectorKnowledgeBase",
    "KafkaAlertSource",
    "RuleDecisionClient",
    "JevDecisionClient",
    "JevDecisionClientStub",
    "create_jev_client",
]
