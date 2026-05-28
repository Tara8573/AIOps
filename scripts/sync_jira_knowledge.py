from dotenv import load_dotenv

from integrations.jira_client import JiraClient
from knowledge.jira_extractor import JiraKnowledgeExtractor
from knowledge.jira_store import SQLiteJiraKnowledgeStore


def main() -> None:
    load_dotenv()
    client = JiraClient()
    extractor = JiraKnowledgeExtractor()
    store = SQLiteJiraKnowledgeStore()
    total = 0
    success = 0
    failed = 0

    for issue in client.search_issues():
        total += 1
        try:
            entry = extractor.extract(issue)
            store.upsert(entry)
            success += 1
        except Exception as exc:
            failed += 1
            print(f"[JiraSync] failed issue={issue.get('issue_key', '')}: {exc}")

    print(f"[JiraSync] total={total} success={success} failed={failed} store_count={store.count()}")


if __name__ == "__main__":
    main()
