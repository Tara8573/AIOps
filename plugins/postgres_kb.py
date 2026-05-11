import hashlib
import json
import math
import os
import re
import urllib.error
import urllib.request
from collections import Counter
from difflib import SequenceMatcher
from urllib.parse import urljoin
from typing import Any, List, Optional

import psycopg
from pgvector import Vector
from pgvector.psycopg import register_vector

from core.models import Feedback
from interfaces.base import IKnowledgeBase


class PostgresVectorKnowledgeBase(IKnowledgeBase):
    """PostgreSQL + pgvector-backed knowledge base implementation."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        vector_dim: int = 1024,
        top_k: int = 3,
    ):
        self.dsn = dsn or os.getenv(
            "AIOPS_PG_DSN",
            "postgresql://postgres:postgres@127.0.0.1:5432/aiops",
        )
        self.vector_dim = vector_dim
        self.top_k = top_k
        self.embedding_provider = (
            os.getenv("AIOPS_EMBEDDING_PROVIDER", "local").strip().lower()
        )
        self.embedding_api_url = self._get_service_url(
            "AIOPS_EMBEDDING_API_URL", "/embeddings"
        )
        self.embedding_api_key = self._get_env(
            "AIOPS_EMBEDDING_API_KEY", "AIOPS_REMOTE_API_KEY"
        ).strip()
        self.embedding_api_key_header = self._get_env(
            "AIOPS_EMBEDDING_API_KEY_HEADER",
            "AIOPS_REMOTE_API_KEY_HEADER",
            default="Authorization",
        ).strip()
        self.embedding_api_key_prefix = self._get_env(
            "AIOPS_EMBEDDING_API_KEY_PREFIX",
            "AIOPS_REMOTE_API_KEY_PREFIX",
            default="Bearer",
        ).strip()
        self.embedding_model = os.getenv("AIOPS_EMBEDDING_MODEL", "").strip()
        self.embedding_input_field = os.getenv(
            "AIOPS_EMBEDDING_INPUT_FIELD", "input"
        ).strip()
        self.embedding_model_field = os.getenv(
            "AIOPS_EMBEDDING_MODEL_FIELD", "model"
        ).strip()
        self.embedding_vector_path = os.getenv(
            "AIOPS_EMBEDDING_VECTOR_PATH", "data.0.embedding"
        ).strip()
        self.embedding_timeout_seconds = float(
            self._get_env(
                "AIOPS_EMBEDDING_TIMEOUT_SECONDS",
                "AIOPS_REMOTE_API_TIMEOUT_SECONDS",
                default="20",
            )
        )
        self.embedding_fallback_local = (
            os.getenv("AIOPS_EMBEDDING_FALLBACK_LOCAL", "1") == "1"
        )
        self.rerank_enabled = os.getenv("AIOPS_RERANK_ENABLED", "0") == "1"
        self.rerank_api_url = self._get_service_url("AIOPS_RERANK_API_URL", "/rerank")
        self.rerank_api_key = self._get_env(
            "AIOPS_RERANK_API_KEY", "AIOPS_REMOTE_API_KEY"
        ).strip()
        self.rerank_api_key_header = self._get_env(
            "AIOPS_RERANK_API_KEY_HEADER",
            "AIOPS_REMOTE_API_KEY_HEADER",
            default="Authorization",
        ).strip()
        self.rerank_api_key_prefix = self._get_env(
            "AIOPS_RERANK_API_KEY_PREFIX",
            "AIOPS_REMOTE_API_KEY_PREFIX",
            default="Bearer",
        ).strip()
        self.rerank_model = os.getenv("AIOPS_RERANK_MODEL", "").strip()
        self.rerank_query_field = os.getenv("AIOPS_RERANK_QUERY_FIELD", "query").strip()
        self.rerank_docs_field = os.getenv(
            "AIOPS_RERANK_DOCS_FIELD", "documents"
        ).strip()
        self.rerank_model_field = os.getenv("AIOPS_RERANK_MODEL_FIELD", "model").strip()
        self.rerank_scores_path = os.getenv(
            "AIOPS_RERANK_SCORES_PATH", "results"
        ).strip()
        self.rerank_score_field = os.getenv(
            "AIOPS_RERANK_SCORE_FIELD", "relevance_score"
        ).strip()
        self.rerank_index_field = os.getenv("AIOPS_RERANK_INDEX_FIELD", "index").strip()
        self.rerank_timeout_seconds = float(
            self._get_env(
                "AIOPS_RERANK_TIMEOUT_SECONDS",
                "AIOPS_REMOTE_API_TIMEOUT_SECONDS",
                default="20",
            )
        )
        self.rerank_candidate_k = int(
            os.getenv("AIOPS_RERANK_CANDIDATE_K", str(max(10, top_k * 3)))
        )
        self.rerank_fallback_vector = (
            os.getenv("AIOPS_RERANK_FALLBACK_VECTOR", "1") == "1"
        )
        self.approx_dedupe_enabled = (
            os.getenv("AIOPS_KB_APPROX_DEDUPE_ENABLED", "1") == "1"
        )
        self.approx_dedupe_candidate_k = int(
            os.getenv("AIOPS_KB_APPROX_DEDUPE_CANDIDATE_K", "5")
        )
        self.approx_dedupe_distance_threshold = float(
            os.getenv("AIOPS_KB_APPROX_DEDUPE_DISTANCE_THRESHOLD", "0.08")
        )
        self.approx_dedupe_overlap_threshold = float(
            os.getenv("AIOPS_KB_APPROX_DEDUPE_OVERLAP_THRESHOLD", "0.65")
        )
        self.approx_dedupe_text_similarity_threshold = float(
            os.getenv("AIOPS_KB_APPROX_DEDUPE_TEXT_SIMILARITY_THRESHOLD", "0.72")
        )
        self._ensure_schema()

    @staticmethod
    def _get_env(
        primary: str, fallback: Optional[str] = None, default: str = ""
    ) -> str:
        value = os.getenv(primary)
        if value is not None and value.strip():
            # 如果值是另一个环境变量名（如 DEEPSEEK_API_KEY），尝试读取它
            if value.isupper() and "_" in value:
                env_value = os.getenv(value)
                if env_value is not None and env_value.strip():
                    return env_value
            return value
        if fallback:
            fallback_value = os.getenv(fallback)
            if fallback_value is not None and fallback_value.strip():
                # 同样检查 fallback 值是否是环境变量名
                if fallback_value.isupper() and "_" in fallback_value:
                    env_value = os.getenv(fallback_value)
                    if env_value is not None and env_value.strip():
                        return env_value
                return fallback_value
        return default

    def _get_service_url(self, primary: str, default_path: str) -> str:
        value = self._get_env(primary).strip()
        if value:
            return value
        base_url = self._get_env("AIOPS_REMOTE_API_BASE_URL").strip()
        if not base_url:
            return ""
        return urljoin(f"{base_url.rstrip('/')}/", default_path.lstrip("/"))

    def _connect(self):
        conn = psycopg.connect(self.dsn, autocommit=True)
        register_vector(conn)
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS kb_experiences (
                        id BIGSERIAL PRIMARY KEY,
                        source VARCHAR(64) NOT NULL DEFAULT 'manual',
                        alert_feature TEXT NOT NULL,
                        content TEXT NOT NULL,
                        dedupe_key CHAR(32),
                        embedding vector({self.vector_dim}) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE kb_experiences
                    ADD COLUMN IF NOT EXISTS dedupe_key CHAR(32);
                    """
                )
                cur.execute(
                    """
                    UPDATE kb_experiences
                    SET dedupe_key = md5(
                        lower(
                            regexp_replace(trim(alert_feature), '\s+', ' ', 'g')
                        ) || '|' || lower(
                            regexp_replace(trim(content), '\s+', ' ', 'g')
                        )
                    )
                    WHERE dedupe_key IS NULL;
                    """
                )
                cur.execute(
                    """
                    DELETE FROM kb_experiences a
                    USING kb_experiences b
                    WHERE a.id < b.id
                      AND a.dedupe_key = b.dedupe_key;
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_kb_experiences_created_at
                    ON kb_experiences(created_at DESC);
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_experiences_dedupe_key
                    ON kb_experiences(dedupe_key);
                    """
                )

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return re.findall(r"[a-zA-Z0-9_/\-\.]+", text.lower())

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(text.strip().lower().split())

    def _build_dedupe_key(self, alert_feature: str, content: str) -> str:
        normalized = (
            f"{self._normalize_text(alert_feature)}|{self._normalize_text(content)}"
        )
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _compose_feedback_feature(feedback: Feedback) -> str:
        return feedback.actual_root_cause.strip()

    def _build_experience_content(
        self, actual_root_cause: str, resolution_steps: str
    ) -> str:
        return f"人工处置经验: root_cause={actual_root_cause}; resolution_steps={resolution_steps}"

    @staticmethod
    def _token_set(text: str) -> set[str]:
        return set(re.findall(r"[a-zA-Z0-9_\-\./\u4e00-\u9fff]+", text.lower()))

    def _text_overlap_ratio(self, left: str, right: str) -> float:
        left_tokens = self._token_set(left)
        right_tokens = self._token_set(right)
        if not left_tokens or not right_tokens:
            return 0.0
        intersection = len(left_tokens & right_tokens)
        base = min(len(left_tokens), len(right_tokens))
        if base == 0:
            return 0.0
        return intersection / base

    @staticmethod
    def _char_ngrams(text: str, size: int = 3) -> set[str]:
        normalized = re.sub(r"\s+", "", text.lower())
        if not normalized:
            return set()
        if len(normalized) <= size:
            return {normalized}
        return {
            normalized[idx : idx + size] for idx in range(len(normalized) - size + 1)
        }

    def _char_similarity(self, left: str, right: str) -> float:
        left_ngrams = self._char_ngrams(left)
        right_ngrams = self._char_ngrams(right)
        if not left_ngrams or not right_ngrams:
            return 0.0
        intersection = len(left_ngrams & right_ngrams)
        union = len(left_ngrams | right_ngrams)
        if union == 0:
            return 0.0
        return intersection / union

    @staticmethod
    def _sequence_similarity(left: str, right: str) -> float:
        normalized_left = re.sub(r"\s+", "", left.lower())
        normalized_right = re.sub(r"\s+", "", right.lower())
        if not normalized_left or not normalized_right:
            return 0.0
        return SequenceMatcher(None, normalized_left, normalized_right).ratio()

    def _find_approx_duplicate(
        self,
        source: str,
        alert_feature: str,
        content: str,
        dedupe_key: str,
        embedding: List[float],
    ) -> Optional[dict[str, Any]]:
        if not self.approx_dedupe_enabled:
            return None

        sql = """
            SELECT id, source, alert_feature, content, dedupe_key, created_at, embedding <=> %s AS distance
            FROM kb_experiences
            WHERE dedupe_key <> %s
            ORDER BY embedding <=> %s
            LIMIT %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        Vector(embedding),
                        dedupe_key,
                        Vector(embedding),
                        self.approx_dedupe_candidate_k,
                    ),
                )
                rows = cur.fetchall()

        incoming_text = f"{alert_feature}\n{content}"
        for row in rows:
            existing_text = f"{row[2]}\n{row[3]}"
            overlap_ratio = self._text_overlap_ratio(incoming_text, existing_text)
            char_similarity = self._char_similarity(incoming_text, existing_text)
            sequence_similarity = self._sequence_similarity(
                incoming_text, existing_text
            )
            distance = float(row[6])
            if (
                (
                    distance <= self.approx_dedupe_distance_threshold
                    and overlap_ratio >= self.approx_dedupe_overlap_threshold
                )
                or char_similarity >= self.approx_dedupe_text_similarity_threshold
                or sequence_similarity >= self.approx_dedupe_text_similarity_threshold
            ):
                return {
                    "id": row[0],
                    "source": row[1],
                    "alert_feature": row[2],
                    "content": row[3],
                    "dedupe_key": row[4],
                    "created_at": row[5],
                    "distance": distance,
                    "overlap_ratio": overlap_ratio,
                    "char_similarity": char_similarity,
                    "sequence_similarity": sequence_similarity,
                }
        return None

    def _insert_experience(self, source: str, alert_feature: str, content: str) -> bool:
        dedupe_key = self._build_dedupe_key(alert_feature, content)
        embedding = self._embed_text(f"{alert_feature} {content}")
        approx_duplicate = self._find_approx_duplicate(
            source=source,
            alert_feature=alert_feature,
            content=content,
            dedupe_key=dedupe_key,
            embedding=embedding,
        )
        if approx_duplicate is not None:
            print(
                "[PostgresKB] 跳过近似重复经验: "
                f"existing_id={approx_duplicate['id']}, "
                f"distance={approx_duplicate['distance']:.4f}, "
                f"overlap={approx_duplicate['overlap_ratio']:.2f}, "
                f"char_similarity={approx_duplicate['char_similarity']:.2f}, "
                f"sequence_similarity={approx_duplicate['sequence_similarity']:.2f}"
            )
            return False

        sql = """
            INSERT INTO kb_experiences(source, alert_feature, content, dedupe_key, embedding)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dedupe_key) DO NOTHING;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql, (source, alert_feature, content, dedupe_key, Vector(embedding))
                )
                return cur.rowcount > 0

    def _embed_text_local(self, text: str) -> List[float]:
        """Deterministic local embedding for fallback/demo."""
        vec = [0.0] * self.vector_dim
        tokens = self._tokenize(text)
        if not tokens:
            return vec
        token_counts = Counter(tokens)
        for token, count in token_counts.items():
            digest = hashlib.md5(token.encode("utf-8")).digest()
            idx = int.from_bytes(digest[:4], byteorder="little") % self.vector_dim
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            vec[idx] += sign * float(count)
        norm = math.sqrt(sum(v * v for v in vec))
        if norm == 0:
            return vec
        return [v / norm for v in vec]

    @staticmethod
    def _extract_path(data: Any, path: str) -> Any:
        cur = data
        for seg in path.split("."):
            if seg == "":
                continue
            if isinstance(cur, list):
                idx = int(seg)
                cur = cur[idx]
            elif isinstance(cur, dict):
                cur = cur[seg]
            else:
                raise ValueError(
                    f"invalid path segment '{seg}' for value type {type(cur)}"
                )
        return cur

    def _embed_text_custom_api(self, text: str) -> List[float]:
        if not self.embedding_api_url:
            raise ValueError("AIOPS_EMBEDDING_API_URL is empty")
        payload = {self.embedding_input_field: text}
        if self.embedding_model:
            payload[self.embedding_model_field] = self.embedding_model

        headers = {"Content-Type": "application/json"}
        if self.embedding_api_key:
            if self.embedding_api_key_prefix:
                headers[self.embedding_api_key_header] = (
                    f"{self.embedding_api_key_prefix} {self.embedding_api_key}"
                )
            else:
                headers[self.embedding_api_key_header] = self.embedding_api_key

        req = urllib.request.Request(
            url=self.embedding_api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(
            req, timeout=self.embedding_timeout_seconds
        ) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)

        vector = self._extract_path(data, self.embedding_vector_path)
        if not isinstance(vector, list) or not vector:
            raise ValueError("embedding vector is empty or not list")
        if len(vector) != self.vector_dim:
            raise ValueError(
                f"embedding dim mismatch: got {len(vector)}, expected {self.vector_dim}. "
                "Please align AIOPS_PG_VECTOR_DIM / table vector(dim)."
            )
        return [float(v) for v in vector]

    def _embed_text(self, text: str) -> List[float]:
        if self.embedding_provider == "local":
            return self._embed_text_local(text)
        if self.embedding_provider == "custom_api":
            try:
                return self._embed_text_custom_api(text)
            except (
                urllib.error.URLError,
                TimeoutError,
                ValueError,
                KeyError,
                IndexError,
            ) as exc:
                if not self.embedding_fallback_local:
                    raise
                print(
                    f"[PostgresKB] custom_api embedding failed, fallback local: {exc}"
                )
                return self._embed_text_local(text)
        raise ValueError(
            "unsupported AIOPS_EMBEDDING_PROVIDER. supported: local, custom_api"
        )

    def _rerank_docs_custom_api(self, query: str, documents: List[str]) -> List[str]:
        if not self.rerank_api_url:
            raise ValueError("AIOPS_RERANK_API_URL is empty")
        payload = {
            self.rerank_query_field: query,
            self.rerank_docs_field: documents,
        }
        if self.rerank_model:
            payload[self.rerank_model_field] = self.rerank_model

        headers = {"Content-Type": "application/json"}
        if self.rerank_api_key:
            if self.rerank_api_key_prefix:
                headers[self.rerank_api_key_header] = (
                    f"{self.rerank_api_key_prefix} {self.rerank_api_key}"
                )
            else:
                headers[self.rerank_api_key_header] = self.rerank_api_key

        req = urllib.request.Request(
            url=self.rerank_api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.rerank_timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)

        results = self._extract_path(data, self.rerank_scores_path)
        if not isinstance(results, list):
            raise ValueError("rerank results is not list")

        scored: List[tuple[float, int]] = []
        for idx, item in enumerate(results):
            if not isinstance(item, dict):
                continue
            doc_idx = int(item.get(self.rerank_index_field, idx))
            score = float(item.get(self.rerank_score_field, 0.0))
            if 0 <= doc_idx < len(documents):
                scored.append((score, doc_idx))
        if not scored:
            raise ValueError("rerank results is empty")

        scored.sort(key=lambda x: x[0], reverse=True)
        ordered = [documents[doc_idx] for _, doc_idx in scored]
        return ordered

    def search_experience(self, alert_feature: str) -> List[str]:
        embedding = self._embed_text(alert_feature)
        sql = """
            SELECT content
            FROM kb_experiences
            ORDER BY embedding <=> %s
            LIMIT %s;
        """
        candidate_k = self.top_k
        if self.rerank_enabled:
            candidate_k = max(self.top_k, self.rerank_candidate_k)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (Vector(embedding), candidate_k))
                rows = cur.fetchall()
        experiences = [row[0] for row in rows]
        if self.rerank_enabled and experiences:
            try:
                reranked = self._rerank_docs_custom_api(alert_feature, experiences)
                experiences = reranked[: self.top_k]
                print(
                    f"[PostgresKB] rerank 生效: candidate={candidate_k}, top_k={self.top_k}"
                )
            except (
                urllib.error.URLError,
                TimeoutError,
                ValueError,
                KeyError,
                IndexError,
            ) as exc:
                if not self.rerank_fallback_vector:
                    raise
                experiences = experiences[: self.top_k]
                print(f"[PostgresKB] rerank 失败, 回退向量排序: {exc}")
        else:
            experiences = experiences[: self.top_k]
        print(f"[PostgresKB] 检索经验 {len(experiences)} 条: {alert_feature}")
        return experiences

    def learn_new_experience(self, feedback: Feedback) -> bool:
        alert_feature = self._compose_feedback_feature(feedback)
        content = self._build_experience_content(
            actual_root_cause=feedback.actual_root_cause,
            resolution_steps=feedback.resolution_steps,
        )
        inserted = self._insert_experience("jira_feedback", alert_feature, content)
        if inserted:
            print(f"[PostgresKB] 已沉淀新经验: alert={feedback.alert_id}")
        else:
            print(f"[PostgresKB] 跳过重复经验: alert={feedback.alert_id}")
        return inserted

    def seed_experience(
        self, alert_feature: str, content: str, source: str = "seed"
    ) -> None:
        self._insert_experience(source, alert_feature, content)
