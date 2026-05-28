from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JiraIssue(BaseModel):
    issue_key: str
    title: str = ""
    description: str = ""
    comments: List[str] = Field(default_factory=list)
    status: str = ""
    resolution: str = ""
    labels: List[str] = Field(default_factory=list)
    components: List[str] = Field(default_factory=list)
    priority: str = ""
    assignee: str = ""
    reporter: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    resolved_at: Optional[str] = None
    issue_links: List[Any] = Field(default_factory=list)
    raw: Dict[str, Any] = Field(default_factory=dict)


class JiraKnowledgeEntry(BaseModel):
    issue_key: str
    title: str = ""
    service: str = ""
    environment: str = ""
    alert_name: str = ""
    symptoms: List[str] = Field(default_factory=list)
    root_cause: str = ""
    resolution_steps: List[str] = Field(default_factory=list)
    verification: str = ""
    related_metrics: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    confidence: str = "low"
    source: str = "jira"
    source_url: str = ""
    updated_at: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)


class JiraKnowledgeQuery(BaseModel):
    service: str = ""
    environment: str = ""
    alert_name: str = ""
    description: str = ""
    metrics: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    top_k: int = 5
