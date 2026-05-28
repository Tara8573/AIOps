import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import httpx


@dataclass
class JiraClientConfig:
    base_url: str
    username: str
    api_token: str
    project_keys: List[str]
    jql: str
    max_results: int = 100
    timeout_seconds: float = 20.0

    @classmethod
    def from_env(cls) -> "JiraClientConfig":
        projects = [
            item.strip()
            for item in os.getenv("JIRA_PROJECT_KEYS", "").split(",")
            if item.strip()
        ]
        sync_days = int(os.getenv("JIRA_SYNC_DAYS", "365"))
        project_expr = ", ".join(projects) if projects else ""
        jql = os.getenv("JIRA_JQL", "").strip()
        if not jql:
            if not project_expr:
                raise ValueError("JIRA_PROJECT_KEYS or JIRA_JQL is required")
            jql = (
                f"project in ({project_expr}) AND statusCategory = Done "
                f"AND updated >= -{sync_days}d ORDER BY updated DESC"
            )

        return cls(
            base_url=os.getenv("JIRA_BASE_URL", "").rstrip("/"),
            username=os.getenv("JIRA_EMAIL", os.getenv("JIRA_USERNAME", "")),
            api_token=os.getenv("JIRA_API_TOKEN", ""),
            project_keys=projects,
            jql=jql,
            max_results=int(os.getenv("JIRA_MAX_RESULTS", "100")),
            timeout_seconds=float(os.getenv("JIRA_TIMEOUT_SECONDS", "20")),
        )


class JiraClient:
    """Small Jira REST client for syncing resolved incident tickets."""

    DEFAULT_FIELDS = [
        "summary",
        "description",
        "status",
        "resolution",
        "labels",
        "components",
        "priority",
        "assignee",
        "reporter",
        "created",
        "updated",
        "resolutiondate",
        "comment",
        "issuelinks",
    ]

    def __init__(self, config: Optional[JiraClientConfig] = None):
        self.config = config or JiraClientConfig.from_env()
        if not self.config.base_url:
            raise ValueError("JIRA_BASE_URL is required")
        if not self.config.username or not self.config.api_token:
            raise ValueError("JIRA_EMAIL/JIRA_USERNAME and JIRA_API_TOKEN are required")

    def search_issues(self, fields: Optional[List[str]] = None) -> Iterable[Dict[str, Any]]:
        start_at = 0
        fields = fields or self.DEFAULT_FIELDS
        while True:
            payload = {
                "jql": self.config.jql,
                "startAt": start_at,
                "maxResults": self.config.max_results,
                "fields": fields,
            }
            data = self._post("/rest/api/3/search", payload)
            issues = data.get("issues", [])
            for issue in issues:
                yield self.normalize_issue(issue)

            start_at += len(issues)
            total = int(data.get("total", 0))
            if not issues or start_at >= total:
                break

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        with httpx.Client(
            timeout=self.config.timeout_seconds,
            auth=(self.config.username, self.config.api_token),
        ) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

    @staticmethod
    def normalize_issue(issue: Dict[str, Any]) -> Dict[str, Any]:
        fields = issue.get("fields", {}) or {}
        comments = (
            fields.get("comment", {}).get("comments", [])
            if isinstance(fields.get("comment"), dict)
            else []
        )
        return {
            "issue_key": issue.get("key", ""),
            "title": fields.get("summary") or "",
            "description": JiraClient._extract_text(fields.get("description")),
            "comments": [JiraClient._extract_text(item.get("body")) for item in comments],
            "status": JiraClient._name(fields.get("status")),
            "resolution": JiraClient._name(fields.get("resolution")),
            "labels": fields.get("labels") or [],
            "components": [item.get("name", "") for item in fields.get("components") or []],
            "priority": JiraClient._name(fields.get("priority")),
            "assignee": JiraClient._display_name(fields.get("assignee")),
            "reporter": JiraClient._display_name(fields.get("reporter")),
            "created_at": fields.get("created"),
            "updated_at": fields.get("updated"),
            "resolved_at": fields.get("resolutiondate"),
            "issue_links": fields.get("issuelinks") or [],
            "raw": issue,
        }

    @staticmethod
    def _name(value: Any) -> str:
        return value.get("name", "") if isinstance(value, dict) else ""

    @staticmethod
    def _display_name(value: Any) -> str:
        return value.get("displayName", "") if isinstance(value, dict) else ""

    @staticmethod
    def _extract_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            if value.get("type") == "text":
                return value.get("text", "")
            parts: List[str] = []
            for child in value.get("content", []) or []:
                text = JiraClient._extract_text(child)
                if text:
                    parts.append(text)
            return "\n".join(parts)
        if isinstance(value, list):
            return "\n".join(JiraClient._extract_text(item) for item in value)
        return str(value)
