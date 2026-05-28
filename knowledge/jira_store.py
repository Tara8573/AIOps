import json
import os
import sqlite3
from pathlib import Path
from typing import Any, List, Optional

from knowledge.jira_schema import JiraKnowledgeEntry, JiraKnowledgeQuery


class SQLiteJiraKnowledgeStore:
    """Local Jira knowledge store backed by SQLite FTS5."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("JIRA_KNOWLEDGE_DB", "data/jira_knowledge.db")
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def upsert(self, entry: JiraKnowledgeEntry) -> None:
        payload = entry.model_dump()
        search_text = self._build_search_text(entry)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jira_knowledge (
                    issue_key, title, service, environment, alert_name, root_cause,
                    verification, tags_json, payload_json, search_text, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_key) DO UPDATE SET
                    title=excluded.title,
                    service=excluded.service,
                    environment=excluded.environment,
                    alert_name=excluded.alert_name,
                    root_cause=excluded.root_cause,
                    verification=excluded.verification,
                    tags_json=excluded.tags_json,
                    payload_json=excluded.payload_json,
                    search_text=excluded.search_text,
                    updated_at=excluded.updated_at
                """,
                (
                    entry.issue_key,
                    entry.title,
                    entry.service,
                    entry.environment,
                    entry.alert_name,
                    entry.root_cause,
                    entry.verification,
                    json.dumps(entry.tags, ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                    search_text,
                    entry.updated_at,
                ),
            )
            row = conn.execute(
                "SELECT id FROM jira_knowledge WHERE issue_key = ?", (entry.issue_key,)
            ).fetchone()
            if row:
                conn.execute(
                    "DELETE FROM jira_knowledge_fts WHERE rowid = ?",
                    (row["id"],),
                )
                conn.execute(
                    "INSERT INTO jira_knowledge_fts(rowid, search_text) VALUES (?, ?)",
                    (row["id"], search_text),
                )

    def search(self, query: JiraKnowledgeQuery) -> List[dict[str, Any]]:
        terms = " ".join(
            item
            for item in [
                query.service,
                query.environment,
                query.alert_name,
                query.description,
                " ".join(query.tags),
            ]
            if item
        ).strip()
        if not terms:
            return []

        fts_query = self._to_fts_query(terms)
        params: List[Any] = [fts_query]
        filters = []
        if query.service:
            filters.append("(service = ? OR service = '')")
            params.append(query.service)
        if query.environment:
            filters.append("(environment = ? OR environment = '')")
            params.append(query.environment)
        if query.alert_name:
            filters.append("(alert_name = ? OR alert_name = '')")
            params.append(query.alert_name)
        where = " AND ".join(filters)
        if where:
            where = " AND " + where

        sql = f"""
            SELECT k.payload_json, bm25(jira_knowledge_fts) AS rank
            FROM jira_knowledge_fts
            JOIN jira_knowledge k ON k.id = jira_knowledge_fts.rowid
            WHERE jira_knowledge_fts MATCH ? {where}
            ORDER BY rank
            LIMIT ?
        """
        params.append(max(1, query.top_k))
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        results: List[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            score = 1.0 / (1.0 + abs(float(row["rank"])))
            payload["similarity"] = round(score, 4)
            results.append(payload)
        return results

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM jira_knowledge").fetchone()
        return int(row["count"])

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jira_knowledge (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_key TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL DEFAULT '',
                    service TEXT NOT NULL DEFAULT '',
                    environment TEXT NOT NULL DEFAULT '',
                    alert_name TEXT NOT NULL DEFAULT '',
                    root_cause TEXT NOT NULL DEFAULT '',
                    verification TEXT NOT NULL DEFAULT '',
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    payload_json TEXT NOT NULL,
                    search_text TEXT NOT NULL,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS jira_knowledge_fts
                USING fts5(search_text)
                """
            )

    @staticmethod
    def _build_search_text(entry: JiraKnowledgeEntry) -> str:
        return "\n".join(
            [
                entry.issue_key,
                entry.title,
                entry.service,
                entry.environment,
                entry.alert_name,
                "\n".join(entry.symptoms),
                entry.root_cause,
                "\n".join(entry.resolution_steps),
                entry.verification,
                " ".join(entry.related_metrics),
                " ".join(entry.tags),
            ]
        )

    @staticmethod
    def _to_fts_query(text: str) -> str:
        tokens = [token.replace('"', "") for token in text.split() if token.strip()]
        return " OR ".join(f'"{token}"' for token in tokens) or '""'
