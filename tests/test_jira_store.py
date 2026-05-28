from knowledge.jira_schema import JiraKnowledgeEntry, JiraKnowledgeQuery
from knowledge.jira_store import SQLiteJiraKnowledgeStore


def test_upsert_deduplicates_by_issue_key(tmp_path):
    store = SQLiteJiraKnowledgeStore(str(tmp_path / "jira.db"))
    entry = JiraKnowledgeEntry(
        issue_key="OPS-1",
        title="Payment latency",
        service="payment-api",
        environment="prod",
        alert_name="HighLatency",
        root_cause="Redis pool exhausted",
        resolution_steps=["increase pool"],
        tags=["redis"],
    )

    store.upsert(entry)
    store.upsert(entry.model_copy(update={"root_cause": "Downstream timeout"}))

    assert store.count() == 1
    results = store.search(
        JiraKnowledgeQuery(
            service="payment-api",
            environment="prod",
            alert_name="HighLatency",
            description="payment latency timeout",
            top_k=5,
        )
    )
    assert results[0]["issue_key"] == "OPS-1"
    assert results[0]["root_cause"] == "Downstream timeout"
