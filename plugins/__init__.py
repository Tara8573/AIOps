# AIOps Plugins Package

from .mocks import MockApprovalGate, MockExecutor, MockJiraTicketing, MockKnowledgeBase, MockLLMClient

try:
    from .postgres_kb import PostgresVectorKnowledgeBase
except Exception:  # pragma: no cover
    PostgresVectorKnowledgeBase = None

__all__ = [
    "MockKnowledgeBase",
    "MockApprovalGate",
    "MockExecutor",
    "MockJiraTicketing",
    "MockLLMClient",
    "PostgresVectorKnowledgeBase",
]
