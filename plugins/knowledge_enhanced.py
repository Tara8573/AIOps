import os
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DocumentChunk:
    """文档分块"""

    chunk_id: str
    content: str
    metadata: Dict[str, Any]
    embedding: Optional[List[float]] = None


class DocumentParser:
    """文档解析器，支持多种格式"""

    SUPPORTED_EXTENSIONS = {".txt", ".md", ".json", ".log", ".csv"}

    @staticmethod
    def parse_file(file_path: str) -> str:
        """解析单个文件"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        ext = path.suffix.lower()
        if ext not in DocumentParser.SUPPORTED_EXTENSIONS:
            raise ValueError(f"不支持的文件格式: {ext}")

        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def parse_directory(dir_path: str, recursive: bool = True) -> List[Dict[str, str]]:
        """解析目录下的所有支持文件"""
        path = Path(dir_path)
        if not path.exists():
            raise FileNotFoundError(f"目录不存在: {dir_path}")

        documents = []
        pattern = "**/*" if recursive else "*"

        for file_path in path.glob(pattern):
            if (
                file_path.is_file()
                and file_path.suffix.lower() in DocumentParser.SUPPORTED_EXTENSIONS
            ):
                try:
                    content = DocumentParser.parse_file(str(file_path))
                    documents.append(
                        {
                            "path": str(file_path),
                            "content": content,
                            "metadata": {
                                "filename": file_path.name,
                                "extension": file_path.suffix,
                                "size": file_path.stat().st_size,
                                "modified": datetime.fromtimestamp(
                                    file_path.stat().st_mtime
                                ).isoformat(),
                            },
                        }
                    )
                except Exception as e:
                    print(f"解析文件失败 {file_path}: {e}")

        return documents


class TextChunker:
    """文本分块器"""

    def __init__(
        self,
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        separators: Optional[List[str]] = None,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = separators or ["\n\n", "\n", "。", ".", "！", "!", "？", "?"]

    def chunk_text(
        self, text: str, metadata: Optional[Dict] = None
    ) -> List[DocumentChunk]:
        """将文本分块"""
        if not text.strip():
            return []

        chunks = []
        current_chunk = ""
        current_start = 0

        # 按分隔符分割文本
        sentences = self._split_text(text)

        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= self.chunk_size:
                current_chunk += sentence
            else:
                if current_chunk.strip():
                    chunk_id = hashlib.md5(current_chunk.encode()).hexdigest()[:12]
                    chunks.append(
                        DocumentChunk(
                            chunk_id=chunk_id,
                            content=current_chunk.strip(),
                            metadata={
                                **(metadata or {}),
                                "chunk_index": len(chunks),
                                "start_pos": current_start,
                                "end_pos": current_start + len(current_chunk),
                            },
                        )
                    )

                # 重叠处理
                overlap_text = (
                    current_chunk[-self.chunk_overlap :]
                    if len(current_chunk) > self.chunk_overlap
                    else ""
                )
                current_chunk = overlap_text + sentence
                current_start += len(current_chunk) - len(overlap_text)

        # 处理最后一个块
        if current_chunk.strip():
            chunk_id = hashlib.md5(current_chunk.encode()).hexdigest()[:12]
            chunks.append(
                DocumentChunk(
                    chunk_id=chunk_id,
                    content=current_chunk.strip(),
                    metadata={
                        **(metadata or {}),
                        "chunk_index": len(chunks),
                        "start_pos": current_start,
                        "end_pos": current_start + len(current_chunk),
                    },
                )
            )

        return chunks

    def _split_text(self, text: str) -> List[str]:
        """使用分隔符分割文本"""
        import re

        # 构建正则表达式
        pattern = "|".join(re.escape(sep) for sep in self.separators)
        sentences = re.split(f"({pattern})", text)

        # 合并分隔符到前一个句子
        result = []
        current = ""
        for i, part in enumerate(sentences):
            current += part
            if i % 2 == 1:  # 分隔符位置
                result.append(current)
                current = ""

        if current:
            result.append(current)

        return result


class KnowledgeGraphBuilder:
    """知识图谱构建器"""

    def __init__(self):
        self.entities: Dict[str, Dict] = {}
        self.relations: List[Dict] = []

    def extract_entities(self, text: str) -> List[Dict]:
        """从文本中提取实体（简化版本，使用规则匹配）"""
        import re

        entities = []

        # 提取告警相关实体
        patterns = {
            "ALERT_TYPE": r"(?:告警|错误|异常|故障|警告)[：:]\s*(.+?)(?:\n|$)",
            "SERVICE": r"(?:服务|组件|模块)[：:]\s*(.+?)(?:\n|$)",
            "METRIC": r"(?:指标|监控项|阈值)[：:]\s*(.+?)(?:\n|$)",
            "HOST": r"(?:主机|服务器|节点)[：:]\s*(.+?)(?:\n|$)",
            "ERROR_CODE": r"(?:错误码|异常码|状态码)[：:]\s*(\w+)",
        }

        for entity_type, pattern in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                entity_id = hashlib.md5(f"{entity_type}:{match}".encode()).hexdigest()[
                    :10
                ]
                entities.append(
                    {
                        "id": entity_id,
                        "type": entity_type,
                        "name": match.strip(),
                        "source_text": text[:100],
                    }
                )

        return entities

    def extract_relations(self, text: str, entities: List[Dict]) -> List[Dict]:
        """从文本中提取实体关系"""
        relations = []

        # 简化的关系提取：基于位置和共现
        for i, entity1 in enumerate(entities):
            for entity2 in entities[i + 1 :]:
                # 如果两个实体在同一个句子中出现，认为有关系
                relation = {
                    "source": entity1["id"],
                    "target": entity2["id"],
                    "type": "CO_OCCURS",
                    "context": text[:200],
                }
                relations.append(relation)

        return relations

    def add_document(self, text: str, metadata: Optional[Dict] = None):
        """添加文档到知识图谱"""
        entities = self.extract_entities(text)
        relations = self.extract_relations(text, entities)

        # 存储实体
        for entity in entities:
            if entity["id"] not in self.entities:
                self.entities[entity["id"]] = {
                    **entity,
                    "metadata": metadata or {},
                    "created_at": datetime.now().isoformat(),
                }

        # 存储关系
        self.relations.extend(relations)

    def query_related(self, entity_name: str) -> List[Dict]:
        """查询与实体相关的所有实体"""
        related = []

        # 查找匹配的实体
        for entity_id, entity in self.entities.items():
            if entity_name.lower() in entity["name"].lower():
                # 查找相关实体
                for relation in self.relations:
                    if relation["source"] == entity_id:
                        target_entity = self.entities.get(relation["target"])
                        if target_entity:
                            related.append(
                                {"relation": relation["type"], "entity": target_entity}
                            )
                    elif relation["target"] == entity_id:
                        source_entity = self.entities.get(relation["source"])
                        if source_entity:
                            related.append(
                                {"relation": relation["type"], "entity": source_entity}
                            )

        return related

    def export_graph(self) -> Dict:
        """导出知识图谱"""
        return {
            "entities": list(self.entities.values()),
            "relations": self.relations,
            "stats": {
                "entity_count": len(self.entities),
                "relation_count": len(self.relations),
            },
        }

    def save_to_file(self, file_path: str):
        """保存知识图谱到文件"""
        graph_data = self.export_graph()
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(graph_data, f, ensure_ascii=False, indent=2)

    def load_from_file(self, file_path: str):
        """从文件加载知识图谱"""
        with open(file_path, "r", encoding="utf-8") as f:
            graph_data = json.load(f)

        self.entities = {e["id"]: e for e in graph_data.get("entities", [])}
        self.relations = graph_data.get("relations", [])


class EnhancedKnowledgeBase:
    """增强知识库，整合文档解析、分块和知识图谱"""

    def __init__(self, storage_dir: str = "./knowledge_store"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self.parser = DocumentParser()
        self.chunker = TextChunker()
        self.graph = KnowledgeGraphBuilder()

        # 文档索引
        self.documents: Dict[str, Dict] = {}
        self.chunks: Dict[str, DocumentChunk] = {}

        # 加载已有数据
        self._load_index()

    def _load_index(self):
        """加载文档索引"""
        index_path = self.storage_dir / "index.json"
        if index_path.exists():
            with open(index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.documents = data.get("documents", {})

    def _save_index(self):
        """保存文档索引"""
        index_path = self.storage_dir / "index.json"
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(
                {"documents": self.documents, "updated_at": datetime.now().isoformat()},
                f,
                ensure_ascii=False,
                indent=2,
            )

    def import_document(self, file_path: str, metadata: Optional[Dict] = None) -> str:
        """导入单个文档"""
        content = self.parser.parse_file(file_path)
        doc_id = hashlib.md5(file_path.encode()).hexdigest()[:12]

        # 分块
        chunks = self.chunker.chunk_text(content, metadata)

        # 存储分块
        for chunk in chunks:
            self.chunks[chunk.chunk_id] = chunk

        # 更新文档索引
        self.documents[doc_id] = {
            "path": file_path,
            "chunk_ids": [c.chunk_id for c in chunks],
            "metadata": {
                **(metadata or {}),
                "imported_at": datetime.now().isoformat(),
                "chunk_count": len(chunks),
            },
        }

        # 添加到知识图谱
        self.graph.add_document(content, metadata)

        self._save_index()
        return doc_id

    def import_directory(
        self, dir_path: str, metadata: Optional[Dict] = None
    ) -> List[str]:
        """导入目录下的所有文档"""
        documents = self.parser.parse_directory(dir_path)
        doc_ids = []

        for doc in documents:
            try:
                # 分块
                chunks = self.chunker.chunk_text(doc["content"], doc["metadata"])

                # 存储分块
                for chunk in chunks:
                    self.chunks[chunk.chunk_id] = chunk

                # 生成文档ID
                doc_id = hashlib.md5(doc["path"].encode()).hexdigest()[:12]

                # 更新索引
                self.documents[doc_id] = {
                    "path": doc["path"],
                    "chunk_ids": [c.chunk_id for c in chunks],
                    "metadata": {
                        **(metadata or {}),
                        **doc["metadata"],
                        "imported_at": datetime.now().isoformat(),
                        "chunk_count": len(chunks),
                    },
                }

                # 添加到知识图谱
                self.graph.add_document(doc["content"], doc["metadata"])

                doc_ids.append(doc_id)

            except Exception as e:
                print(f"处理文档失败 {doc['path']}: {e}")

        self._save_index()
        return doc_ids

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        """搜索相关知识（简化版本，使用关键词匹配）"""
        results = []
        query_lower = query.lower()

        for chunk_id, chunk in self.chunks.items():
            content_lower = chunk.content.lower()

            # 计算简单的相关性分数
            score = 0
            for word in query_lower.split():
                if word in content_lower:
                    score += content_lower.count(word)

            if score > 0:
                results.append(
                    {
                        "chunk_id": chunk_id,
                        "content": chunk.content,
                        "score": score,
                        "metadata": chunk.metadata,
                    }
                )

        # 按分数排序
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]

    def get_related_knowledge(self, entity_name: str) -> List[Dict]:
        """获取与实体相关的知识"""
        return self.graph.query_related(entity_name)

    def export_stats(self) -> Dict:
        """导出统计信息"""
        return {
            "document_count": len(self.documents),
            "chunk_count": len(self.chunks),
            "graph_stats": self.graph.export_graph()["stats"],
        }
