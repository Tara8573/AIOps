import argparse

from dotenv import load_dotenv

from knowledge.jira_schema import JiraKnowledgeQuery
from knowledge.jira_store import SQLiteJiraKnowledgeStore


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Search local Jira incident knowledge.")
    parser.add_argument("--service", default="")
    parser.add_argument("--env", default="")
    parser.add_argument("--alert", default="")
    parser.add_argument("--query", default="")
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()

    store = SQLiteJiraKnowledgeStore()
    results = store.search(
        JiraKnowledgeQuery(
            service=args.service,
            environment=args.env,
            alert_name=args.alert,
            description=args.query,
            top_k=args.top_k,
        )
    )
    for idx, item in enumerate(results, start=1):
        print(f"{idx}. {item.get('issue_key')} score={item.get('similarity')}")
        print(f"   title={item.get('title')}")
        print(f"   root_cause={item.get('root_cause')}")
        print(f"   resolution_steps={item.get('resolution_steps')}")
        print(f"   source_url={item.get('source_url')}")


if __name__ == "__main__":
    main()
