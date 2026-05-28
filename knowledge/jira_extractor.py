import json
import os
import re
from typing import Any, Dict, List, Optional, Union

from interfaces.base import ILLMClient
from knowledge.jira_schema import JiraIssue, JiraKnowledgeEntry


class JiraKnowledgeExtractor:
    """Extracts a compact incident knowledge record from a Jira issue."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        llm_client: Optional[ILLMClient] = None,
        max_comment_chars: int = 4000,
    ):
        self.base_url = (base_url or os.getenv("JIRA_BASE_URL", "")).rstrip("/")
        self.llm_client = llm_client
        self.max_comment_chars = max_comment_chars

    def extract(self, raw_issue: Union[Dict[str, Any], JiraIssue]) -> JiraKnowledgeEntry:
        issue = raw_issue if isinstance(raw_issue, JiraIssue) else JiraIssue(**raw_issue)
        base_entry = self._extract_by_rules(issue)
        if self.llm_client is None:
            return base_entry

        try:
            llm_data = self._extract_with_llm(issue)
            merged = base_entry.model_dump()
            for key, value in llm_data.items():
                if value not in (None, "", []):
                    merged[key] = value
            return JiraKnowledgeEntry(**merged)
        except Exception:
            return base_entry

    def _extract_by_rules(self, issue: JiraIssue) -> JiraKnowledgeEntry:
        labels = [item.strip() for item in issue.labels if item.strip()]
        components = [item.strip() for item in issue.components if item.strip()]
        text = "\n".join([issue.title, issue.description, *issue.comments[:3]])
        service = self._find_field(text, ["service", "服务", "应用"]) or self._first_prefixed(labels, "service:")
        environment = (
            self._find_field(text, ["environment", "env", "环境"])
            or self._first_prefixed(labels, "env:")
        )
        alert_name = (
            self._find_field(text, ["alert", "alert_name", "告警"])
            or self._first_prefixed(labels, "alert:")
            or issue.title
        )
        root_cause = self._find_field(text, ["root_cause", "root cause", "根因", "原因"])
        verification = self._find_field(text, ["verification", "验证", "恢复"])
        steps = self._extract_steps(text)
        tags = sorted({*labels, *components})
        confidence = "medium" if root_cause and steps else "low"

        return JiraKnowledgeEntry(
            issue_key=issue.issue_key,
            title=issue.title,
            service=service or "",
            environment=environment or "",
            alert_name=alert_name or "",
            symptoms=self._extract_symptoms(text),
            root_cause=root_cause or issue.resolution or "",
            resolution_steps=steps,
            verification=verification or "",
            tags=tags,
            confidence=confidence,
            source_url=self._source_url(issue.issue_key),
            updated_at=issue.updated_at,
            raw=issue.model_dump(),
        )

    def _extract_with_llm(self, issue: JiraIssue) -> Dict[str, Any]:
        text = self._build_issue_text(issue)
        prompt = f"""
请从以下 Jira 告警处理工单中抽取 AIOps 知识，严格输出 JSON，不要输出 Markdown。
字段:
issue_key,title,service,environment,alert_name,symptoms,root_cause,
resolution_steps,verification,related_metrics,tags,confidence

工单内容:
{text}
"""
        response = self.llm_client.generate_proposal(prompt)
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        data = json.loads(cleaned.strip())
        return data if isinstance(data, dict) else {}

    def _build_issue_text(self, issue: JiraIssue) -> str:
        comments = "\n".join(issue.comments)
        if len(comments) > self.max_comment_chars:
            comments = comments[: self.max_comment_chars] + "\n...[truncated]"
        return (
            f"issue_key: {issue.issue_key}\n"
            f"title: {issue.title}\n"
            f"description:\n{issue.description}\n"
            f"labels: {issue.labels}\n"
            f"components: {issue.components}\n"
            f"resolution: {issue.resolution}\n"
            f"comments:\n{comments}"
        )

    def _source_url(self, issue_key: str) -> str:
        if not self.base_url or not issue_key:
            return ""
        return f"{self.base_url}/browse/{issue_key}"

    @staticmethod
    def _find_field(text: str, names: List[str]) -> str:
        for name in names:
            pattern = rf"(?im)^\s*{re.escape(name)}\s*[:：]\s*(.+?)\s*$"
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return ""

    @staticmethod
    def _first_prefixed(values: List[str], prefix: str) -> str:
        for value in values:
            if value.lower().startswith(prefix.lower()):
                return value.split(":", 1)[1].strip()
        return ""

    @staticmethod
    def _extract_steps(text: str) -> List[str]:
        steps: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if re.match(r"^(\d+[\.\)、)]|-|\*)\s+", stripped):
                steps.append(re.sub(r"^(\d+[\.\)、)]|-|\*)\s+", "", stripped))
        return steps[:10]

    @staticmethod
    def _extract_symptoms(text: str) -> List[str]:
        symptoms: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if any(token in stripped.lower() for token in ["p99", "error", "latency", "cpu", "memory", "磁盘", "延迟", "错误率"]):
                symptoms.append(stripped)
        return symptoms[:8]
