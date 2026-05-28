import os
import re
from typing import Any, List, Optional

from core.models import Alert
from knowledge.jira_schema import JiraKnowledgeQuery
from knowledge.jira_store import SQLiteJiraKnowledgeStore


class JiraKnowledgeRetriever:
    def __init__(self, store: Optional[SQLiteJiraKnowledgeStore] = None, top_k: Optional[int] = None):
        self.store = store or SQLiteJiraKnowledgeStore()
        self.top_k = top_k or int(os.getenv("JIRA_KNOWLEDGE_TOP_K", "5"))

    def search_for_alert(self, alert: Alert) -> List[dict[str, Any]]:
        query = self.build_query(alert)
        return self.store.search(query)

    def build_query(self, alert: Alert) -> JiraKnowledgeQuery:
        raw = alert.raw_data or {}
        return JiraKnowledgeQuery(
            service=str(raw.get("service") or raw.get("app") or self._extract_field(alert.content, "service") or ""),
            environment=str(raw.get("environment") or raw.get("env") or self._extract_field(alert.content, "env") or ""),
            alert_name=str(raw.get("alert_name") or alert.title),
            description=f"{alert.title}\n{alert.content}",
            metrics=raw.get("metrics") or {},
            tags=[alert.level, alert.source],
            top_k=self.top_k,
        )

    @staticmethod
    def format_cases(cases: List[dict[str, Any]]) -> List[str]:
        if not cases:
            return []
        lines = ["历史相似 Jira 案例："]
        for idx, case in enumerate(cases, start=1):
            steps = case.get("resolution_steps") or []
            if isinstance(steps, list):
                steps_text = "; ".join(str(step) for step in steps)
            else:
                steps_text = str(steps)
            lines.append(
                (
                    f"{idx}. issue_key: {case.get('issue_key', '')}\n"
                    f"   title: {case.get('title', '')}\n"
                    f"   similarity: {case.get('similarity', '')}\n"
                    f"   service: {case.get('service', '')}\n"
                    f"   alert_name: {case.get('alert_name', '')}\n"
                    f"   root_cause: {case.get('root_cause', '')}\n"
                    f"   resolution_steps: {steps_text}\n"
                    f"   verification: {case.get('verification', '')}\n"
                    f"   source_url: {case.get('source_url', '')}"
                )
            )
        lines.append("请基于这些历史案例辅助分析，但不要盲目套用；若存在差异，请明确指出并引用相关 issue_key。")
        return ["\n".join(lines)]

    @staticmethod
    def _extract_field(text: str, field: str) -> str:
        match = re.search(rf"(?im)^\s*{re.escape(field)}\s*[:：]\s*(.+?)\s*$", text)
        return match.group(1).strip() if match else ""
