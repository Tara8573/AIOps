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

from core.cache import TTLCache, stable_cache_key
from core.models import Feedback
from core.observability import metrics
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
        self.embedding_enabled = os.getenv("AIOPS_EMBEDDING_ENABLED", "1") == "1"
        self.embedding_provider = (
            os.getenv("AIOPS_EMBEDDING_PROVIDER", "local").strip().lower()
            if self.embedding_enabled
            else "local"
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
        self.search_cache = TTLCache(
            ttl_seconds=int(os.getenv("AIOPS_KB_SEARCH_CACHE_TTL_SECONDS", "300")),
            max_size=int(os.getenv("AIOPS_KB_SEARCH_CACHE_MAX_SIZE", "256")),
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
        base_url = self._get_env("AIOPS_LLM_BASE_URL").strip()
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
                    "DROP TABLE IF EXISTS kb_experiences;"
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS kb_fault_patterns (
                        id BIGSERIAL PRIMARY KEY,
                        source VARCHAR(64) NOT NULL DEFAULT 'manual',
                        canonical_root_cause TEXT NOT NULL,
                        summary_content TEXT NOT NULL,
                        dedupe_key CHAR(32) NOT NULL,
                        embedding vector({self.vector_dim}) NOT NULL,
                        support_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS kb_incident_cases (
                        id BIGSERIAL PRIMARY KEY,
                        fault_pattern_id BIGINT NOT NULL
                            REFERENCES kb_fault_patterns(id) ON DELETE CASCADE,
                        source VARCHAR(64) NOT NULL DEFAULT 'manual',
                        alert_id TEXT,
                        ticket_id TEXT,
                        actual_root_cause TEXT NOT NULL,
                        resolution_steps TEXT NOT NULL,
                        case_key CHAR(32) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_fault_patterns_dedupe_key
                    ON kb_fault_patterns(dedupe_key);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_kb_fault_patterns_updated_at
                    ON kb_fault_patterns(updated_at DESC);
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_kb_incident_cases_case_key
                    ON kb_incident_cases(case_key);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_kb_incident_cases_pattern_id
                    ON kb_incident_cases(fault_pattern_id);
                    """
                )

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        lowered = text.lower()
        tokens = re.findall(r"[a-zA-Z0-9_/\-\.]+", lowered)
        chinese_segments = re.findall(r"[\u4e00-\u9fff]+", lowered)
        for segment in chinese_segments:
            if len(segment) == 1:
                tokens.append(segment)
                continue
            tokens.extend(
                segment[idx : idx + 2] for idx in range(len(segment) - 1)
            )
            if len(segment) > 2:
                tokens.extend(
                    segment[idx : idx + 3] for idx in range(len(segment) - 2)
                )
        return tokens

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(text.strip().lower().split())

    def _build_dedupe_key(self, alert_feature: str, content: str) -> str:
        normalized = (
            f"{self._normalize_text(alert_feature)}|{self._normalize_text(content)}"
        )
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()

    def _build_pattern_key(self, canonical_root_cause: str) -> str:
        return hashlib.md5(
            self._normalize_text(canonical_root_cause).encode("utf-8")
        ).hexdigest()

    def _build_case_key(
        self, alert_id: Optional[str], ticket_id: Optional[str], root_cause: str
    ) -> str:
        identity = ticket_id or alert_id
        if identity:
            raw = f"case|{self._normalize_text(identity)}"
        else:
            raw = f"case|{self._normalize_text(root_cause)}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _canonicalize_root_cause(root_cause: str) -> str:
        text = re.sub(r"\s+", "", root_cause.strip().lower())
        replacements = [
            ("服务异常的根因是", ""),
            ("系统", ""),
            ("服务异常", ""),
            ("导致", ""),
            ("造成", ""),
            ("引起", ""),
            ("的根因是", ""),
            ("根因是", ""),
            ("根因", ""),
            ("被", ""),
            ("空间", ""),
            ("文件", ""),
        ]
        for old, new in replacements:
            text = text.replace(old, new)

        synonym_groups = [
            (
                (
                    "日志打满磁盘",
                    "日志占满磁盘",
                    "磁盘被日志打满",
                    "磁盘日志打满",
                    "磁盘日志占满",
                ),
                "日志占满磁盘",
            ),
            (("磁盘满", "磁盘占满", "磁盘空间满"), "磁盘占满"),
            (("过期日志", "旧日志"), "过期日志"),
        ]
        for variants, canonical in synonym_groups:
            if any(variant in text for variant in variants):
                text = canonical
                break
        return text or root_cause.strip()

    def _build_pattern_content(
        self, canonical_root_cause: str, resolution_steps: str, support_count: int = 1
    ) -> str:
        return (
            "故障模式经验: "
            f"canonical_root_cause={canonical_root_cause}; "
            f"recommended_resolution={resolution_steps}; "
            f"related_cases={support_count}"
        )

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
            SELECT id, source, canonical_root_cause, summary_content, dedupe_key,
                   updated_at, support_count, embedding <=> %s AS distance
            FROM kb_fault_patterns
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
            distance = float(row[7])
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
                    "support_count": row[6],
                    "distance": distance,
                    "overlap_ratio": overlap_ratio,
                    "char_similarity": char_similarity,
                    "sequence_similarity": sequence_similarity,
                }
        return None

    def _upsert_fault_pattern(
        self, source: str, root_cause: str, resolution_steps: str
    ) -> tuple[int, bool]:
        canonical_root_cause = self._canonicalize_root_cause(root_cause)
        content = self._build_pattern_content(canonical_root_cause, resolution_steps)
        dedupe_key = self._build_pattern_key(canonical_root_cause)
        embedding = self._embed_text(f"{canonical_root_cause} {resolution_steps}")

        exact_sql = """
            SELECT id
            FROM kb_fault_patterns
            WHERE dedupe_key = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(exact_sql, (dedupe_key,))
                row = cur.fetchone()
                if row:
                    return int(row[0]), False

        approx_duplicate = self._find_approx_duplicate(
            source=source,
            alert_feature=canonical_root_cause,
            content=content,
            dedupe_key=dedupe_key,
            embedding=embedding,
        )
        if approx_duplicate is not None:
            print(
                "[PostgresKB] 归并到近似故障模式: "
                f"existing_id={approx_duplicate['id']}, "
                f"distance={approx_duplicate['distance']:.4f}, "
                f"overlap={approx_duplicate['overlap_ratio']:.2f}, "
                f"char_similarity={approx_duplicate['char_similarity']:.2f}, "
                f"sequence_similarity={approx_duplicate['sequence_similarity']:.2f}"
            )
            return int(approx_duplicate["id"]), False

        sql = """
            INSERT INTO kb_fault_patterns(
                source, canonical_root_cause, summary_content, dedupe_key, embedding
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        source,
                        canonical_root_cause,
                        content,
                        dedupe_key,
                        Vector(embedding),
                    ),
                )
                row = cur.fetchone()
                return int(row[0]), True

    def _insert_incident_case(
        self,
        fault_pattern_id: int,
        source: str,
        actual_root_cause: str,
        resolution_steps: str,
        alert_id: Optional[str] = None,
        ticket_id: Optional[str] = None,
    ) -> bool:
        case_key = self._build_case_key(alert_id, ticket_id, actual_root_cause)
        sql = """
            INSERT INTO kb_incident_cases(
                fault_pattern_id, source, alert_id, ticket_id,
                actual_root_cause, resolution_steps, case_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (case_key) DO NOTHING;
        """
        update_sql = """
            UPDATE kb_fault_patterns
            SET support_count = support_count + 1,
                summary_content = %s,
                updated_at = now()
            WHERE id = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        fault_pattern_id,
                        source,
                        alert_id,
                        ticket_id,
                        actual_root_cause,
                        resolution_steps,
                        case_key,
                    ),
                )
                inserted = cur.rowcount > 0
                if inserted:
                    cur.execute(
                        """
                        SELECT canonical_root_cause, support_count
                        FROM kb_fault_patterns
                        WHERE id = %s;
                        """,
                        (fault_pattern_id,),
                    )
                    row = cur.fetchone()
                    support_count = int(row[1]) + 1 if row else 1
                    summary = self._build_pattern_content(
                        row[0] if row else actual_root_cause,
                        resolution_steps,
                        support_count,
                    )
                    cur.execute(update_sql, (summary, fault_pattern_id))
                return inserted

    def _insert_experience(self, source: str, alert_feature: str, content: str) -> bool:
        pattern_id, pattern_created = self._upsert_fault_pattern(
            source, alert_feature, content
        )
        case_inserted = self._insert_incident_case(
            pattern_id,
            source,
            actual_root_cause=alert_feature,
            resolution_steps=content,
        )
        return pattern_created or case_inserted

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
        if not self.embedding_enabled:
            return self._embed_text_local(text)
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
        cache_key = stable_cache_key(
            "kb_search",
            alert_feature,
            self.top_k,
            self.rerank_enabled,
            self.rerank_candidate_k,
        )
        cached = self.search_cache.get(cache_key)
        if cached is not None:
            metrics.incr("kb.search.cache.hit")
            return list(cached)

        metrics.incr("kb.search.cache.miss")
        embedding = self._embed_text(alert_feature)
        sql = """
            SELECT summary_content
            FROM kb_fault_patterns
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
        if self.rerank_enabled and experiences and self.rerank_api_url:
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
            if self.rerank_enabled and experiences and not self.rerank_api_url:
                print("[PostgresKB] rerank 已开启但未配置 API URL，跳过重排")
            experiences = experiences[: self.top_k]
        print(f"[PostgresKB] 检索经验 {len(experiences)} 条: {alert_feature}")
        self.search_cache.set(cache_key, list(experiences))
        metrics.gauge("kb.search.cache.size", self.search_cache.stats()["size"])
        return experiences

    def learn_new_experience(self, feedback: Feedback) -> bool:
        pattern_id, pattern_created = self._upsert_fault_pattern(
            "jira_feedback",
            feedback.actual_root_cause,
            feedback.resolution_steps,
        )
        case_inserted = self._insert_incident_case(
            pattern_id,
            "jira_feedback",
            actual_root_cause=feedback.actual_root_cause,
            resolution_steps=feedback.resolution_steps,
            alert_id=feedback.alert_id,
            ticket_id=feedback.ticket_id,
        )
        inserted = pattern_created or case_inserted
        if inserted:
            print(
                "[PostgresKB] 已沉淀事件案例并关联故障模式: "
                f"alert={feedback.alert_id}, pattern_id={pattern_id}"
            )
        else:
            print(f"[PostgresKB] 跳过重复事件案例: alert={feedback.alert_id}")
        return inserted

    def seed_experience(
        self, alert_feature: str, content: str, source: str = "seed"
    ) -> None:
        self._insert_experience(source, alert_feature, content)
