from core.models import Alert
from knowledge.jira_retriever import JiraKnowledgeRetriever


class FakeStore:
    def search(self, query):
        return [
            {
                "issue_key": "OPS-9",
                "title": "Payment latency",
                "similarity": 0.9,
                "service": query.service,
                "alert_name": query.alert_name,
                "root_cause": "Redis pool exhausted",
                "resolution_steps": ["increase pool"],
                "verification": "P99 recovered",
                "source_url": "https://jira.example.com/browse/OPS-9",
            }
        ]


def test_retriever_builds_query_from_alert_raw_data():
    retriever = JiraKnowledgeRetriever(store=FakeStore(), top_k=3)
    alert = Alert(
        alert_id="A-1",
        title="HighLatency",
        level="Critical",
        content="P99 latency high",
        raw_data={"service": "payment-api", "env": "prod"},
    )

    cases = retriever.search_for_alert(alert)
    formatted = retriever.format_cases(cases)

    assert cases[0]["service"] == "payment-api"
    assert "issue_key: OPS-9" in formatted[0]
