from knowledge.jira_extractor import JiraKnowledgeExtractor


def test_extract_rules_keeps_basic_fields_when_llm_absent():
    issue = {
        "issue_key": "OPS-123",
        "title": "payment-api HighLatency",
        "description": "service: payment-api\nenv: prod\nroot_cause: Redis pool exhausted\n1. enlarge pool",
        "labels": ["alert:HighLatency", "redis"],
        "components": ["payment"],
        "updated_at": "2026-05-01T00:00:00.000+0000",
    }

    entry = JiraKnowledgeExtractor(base_url="https://jira.example.com").extract(issue)

    assert entry.issue_key == "OPS-123"
    assert entry.service == "payment-api"
    assert entry.environment == "prod"
    assert entry.alert_name == "HighLatency"
    assert entry.root_cause == "Redis pool exhausted"
    assert entry.resolution_steps == ["enlarge pool"]
    assert entry.source_url == "https://jira.example.com/browse/OPS-123"


class BrokenLLM:
    def generate_proposal(self, prompt: str) -> str:
        return "not json"


def test_extract_falls_back_when_llm_json_invalid():
    issue = {
        "issue_key": "OPS-124",
        "title": "Disk Space Critical",
        "description": "root_cause: logs filled disk",
    }

    entry = JiraKnowledgeExtractor(llm_client=BrokenLLM()).extract(issue)

    assert entry.issue_key == "OPS-124"
    assert entry.root_cause == "logs filled disk"
