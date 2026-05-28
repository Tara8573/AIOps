# AIOps 告警自动处理框架

## 业务目标
结合大模型(LLM)处理生产环境告警，实现：
1. **自动执行与人工辅助分流**：LLM 给出根因分析、排查建议以及解决方案脚本。经过系统对方案评估，若通过则自动执行修复，未通过或评估结果认为危险，则提工作流或 Jira 辅助人工。
2. **评估体系**：拦截并评估每次 LLM 单次解决方案是否“正确”与“安全”。
3. **接口解耦**：因为当前没有知识库接入，故框架采用接口注入和防腐层隔离模式。支持未来对接不同的工单(Jira)与知识库底座。

补充：
- 即便评估通过，只要涉及脚本执行，当前实现也会先进入人工确认环节；确认通过后才真正执行。
- 人工确认拒绝时，会自动转人工工单，避免“评估通过但无人兜底”的空档。

## 架构与工作流
1. 接收告警 (Alert Ingestion) -> [预留查找历史分析接口] -> LLM 分析 (CognitiveEngine)
2. 内部安全与正确性评估环节 (SolutionEvaluator)：
   - 判断【安全/合法】：调起物理执行引擎自动解决故障 (IExecutor)
   - 判断【危险/不可靠】：提单到 Jira 把故障与LLM的建议分发给运维人员 (ITicketing)
3. 不断学习的反馈闭环 (FeedbackLoop)：从 Jira 重新拉取解决后的实际处理方式，回流到知识库。

## 核心目录结构
- `core/`: 核心领域数据模型，LLM交互引擎与自审查评估器。
- `interfaces/`: 防腐层/接口顶层抽象定义。
- `plugins/`: 应对外界不同环境的具体调用实现或 Mock 类，也可承载 skill 实现。
- `core/skills.py`: skill 注册与调度中心。
- `pipeline.py`: 主体工作链条调度。
- `main.py`: 框架全链路模拟与单元实战入口。

## Skills 接入方式
项目现已支持在告警主流程中接入 `skills`，但设计上仍与现有 `plugins` 架构兼容：

1. 在 `interfaces/base.py` 中实现 `ISkill`
2. 在 `plugins/skills.py` 或独立模块中编写具体 skill
3. 在 `main.py` 中注册到 `SkillRegistry`
4. `pipeline.py` 会先执行匹配到的 skills，再把结果一并交给 LLM 分析

示例：

```python
from core.skills import SkillRegistry
from plugins.skills import DiskCleanupSkill

skill_registry = SkillRegistry([DiskCleanupSkill()])

pipeline = AIOpsPipeline(
    kb=kb,
    llm=llm,
    executor=executor,
    approval_gate=approval_gate,
    ticketing=ticketing,
    skill_registry=skill_registry,
)
```

`skill` 需要返回结构化结果，供 LLM 结合历史 SOP 一起决策，而不是直接替代原有评估与执行链路。

## PostgreSQL + pgvector 落地
已提供 `PostgresVectorKnowledgeBase`，可替换默认 Mock 知识库。

### 1) 安装依赖
```bash
pip install -r requirements.txt
```

### 2) 准备数据库
```sql
CREATE DATABASE aiops;
```
在 `aiops` 库执行：
```sql
\i sql/init_pgvector.sql
```

### 3) 配置环境变量
```bash
export AIOPS_USE_PG_KB=1
export AIOPS_PG_DSN="postgresql://postgres:postgres@127.0.0.1:5432/aiops"
export AIOPS_PG_VECTOR_DIM=1024
export AIOPS_PG_TOP_K=3
```

Windows PowerShell:
```powershell
$env:AIOPS_USE_PG_KB="1"
$env:AIOPS_PG_DSN="postgresql://postgres:postgres@127.0.0.1:5432/aiops"
$env:AIOPS_PG_VECTOR_DIM="1024"
$env:AIOPS_PG_TOP_K="3"
```

### 4) 运行 Demo
```bash
python main.py
```

说明：
- `AIOPS_USE_PG_KB=1` 时使用 Postgres 知识库；否则默认 `MockKnowledgeBase`。
- 当前 embedding 为项目内置的确定性本地向量化（无外部模型依赖），用于快速可运行验证。
- 运维经验写入知识库时会按规范化后的 `alert_feature + content` 生成去重键，重复经验会被自动跳过，不会持续灌入相同知识。
- 对“近似重复”经验，系统会在入库前结合向量距离和文本重合度做二次判定，避免同义改写反复入库。

## 语义检索（自定义 Embedding API）
`PostgresVectorKnowledgeBase` 支持通过 `.env` 切换 embedding 提供方，无需改代码。

### Provider 配置
```bash
AIOPS_EMBEDDING_ENABLED=1
AIOPS_EMBEDDING_PROVIDER=custom_api
AIOPS_EMBEDDING_API_URL=https://your-embedding-endpoint/v1/embeddings
AIOPS_EMBEDDING_API_KEY=your_key
AIOPS_EMBEDDING_API_KEY_HEADER=Authorization
AIOPS_EMBEDDING_API_KEY_PREFIX=Bearer
AIOPS_EMBEDDING_MODEL=your-model-name
AIOPS_EMBEDDING_INPUT_FIELD=input
AIOPS_EMBEDDING_MODEL_FIELD=model
AIOPS_EMBEDDING_VECTOR_PATH=data.0.embedding
AIOPS_EMBEDDING_TIMEOUT_SECONDS=20
AIOPS_EMBEDDING_FALLBACK_LOCAL=1
```

字段说明：
- `AIOPS_EMBEDDING_ENABLED`: `1` 表示启用外部 embedding 配置；`0` 表示不需要配置外部向量模型，使用内置确定性本地向量。
- `AIOPS_EMBEDDING_PROVIDER`: `local` 或 `custom_api`
- `AIOPS_EMBEDDING_VECTOR_PATH`: 从响应体提取向量的路径，支持 `a.b.0.c` 形式
- `AIOPS_EMBEDDING_FALLBACK_LOCAL=1`: API失败时自动回退本地向量；设为 `0` 则直接抛错
- `AIOPS_EMBEDDING_MODEL`: 可为空；为空时请求体不传 `model` 字段。

维度对齐要求：
- embedding 返回向量长度必须等于 `AIOPS_PG_VECTOR_DIM`，且与表中 `vector(dim)` 一致。

## 可选二阶段重排（Rerank）
支持先向量召回候选，再调用 rerank API 做语义重排。

```bash
AIOPS_RERANK_ENABLED=1
AIOPS_RERANK_API_URL=https://your-rerank-endpoint/v1/rerank
AIOPS_RERANK_API_KEY=your_key
AIOPS_RERANK_API_KEY_HEADER=Authorization
AIOPS_RERANK_API_KEY_PREFIX=Bearer
AIOPS_RERANK_MODEL=your-rerank-model
AIOPS_RERANK_QUERY_FIELD=query
AIOPS_RERANK_DOCS_FIELD=documents
AIOPS_RERANK_MODEL_FIELD=model
AIOPS_RERANK_SCORES_PATH=results
AIOPS_RERANK_SCORE_FIELD=relevance_score
AIOPS_RERANK_INDEX_FIELD=index
AIOPS_RERANK_TIMEOUT_SECONDS=20
AIOPS_RERANK_CANDIDATE_K=10
AIOPS_RERANK_FALLBACK_VECTOR=1
```

说明：
- `AIOPS_RERANK_ENABLED=0` 时不走重排，保持当前向量检索结果。
- `AIOPS_RERANK_MODEL` 可为空；为空时请求体不传 `model` 字段。
- `AIOPS_RERANK_ENABLED=1` 但没有配置 `AIOPS_RERANK_API_URL` 时，会尝试使用 `${AIOPS_LLM_BASE_URL}/rerank`；如果仍为空，则自动跳过重排。
- `AIOPS_RERANK_CANDIDATE_K` 表示向量初召回数量，rerank 后截断到 `AIOPS_PG_TOP_K`。

## 近似去重配置
知识沉淀时默认开启近似重复拦截，适合处理“根因相同、处置相近、措辞略有不同”的经验。

```bash
AIOPS_KB_APPROX_DEDUPE_ENABLED=1
AIOPS_KB_APPROX_DEDUPE_CANDIDATE_K=5
AIOPS_KB_APPROX_DEDUPE_DISTANCE_THRESHOLD=0.08
AIOPS_KB_APPROX_DEDUPE_OVERLAP_THRESHOLD=0.65
AIOPS_KB_APPROX_DEDUPE_TEXT_SIMILARITY_THRESHOLD=0.72
```

说明：
- `AIOPS_KB_APPROX_DEDUPE_CANDIDATE_K`: 先从向量空间取最近的多少条候选做近似去重判断。
- `AIOPS_KB_APPROX_DEDUPE_DISTANCE_THRESHOLD`: 向量距离阈值，越小越严格。
- `AIOPS_KB_APPROX_DEDUPE_OVERLAP_THRESHOLD`: 文本重合度阈值，越大越严格。
- `AIOPS_KB_APPROX_DEDUPE_TEXT_SIMILARITY_THRESHOLD`: 字符级相似度阈值，适合兜底中文同义改写场景。

## 框架优化开关
当前版本已补充一批高优先级框架优化，默认可直接使用，也可通过环境变量调节：

```bash
AIOPS_LOG_LEVEL=INFO
AIOPS_LOG_JSON=0
AIOPS_LOG_DIR=./logs

AIOPS_LLM_CACHE_TTL_SECONDS=300
AIOPS_LLM_CACHE_MAX_SIZE=256

AIOPS_KB_SEARCH_CACHE_TTL_SECONDS=300
AIOPS_KB_SEARCH_CACHE_MAX_SIZE=256

AIOPS_MIN_CONFIDENCE=0.7
AIOPS_DANGEROUS_SCRIPT_PATTERNS=

AIOPS_HTTP_MAX_CONNECTIONS=20
AIOPS_HTTP_MAX_KEEPALIVE_CONNECTIONS=10

AIOPS_BATCH_SIZE=1
AIOPS_ROUTER_MANUAL_PATTERNS=
AIOPS_ROUTER_FAST_TRACK_PATTERNS=
```

对应能力：

- `LLM 响应缓存`：对相似告警分析结果做 TTL 复用，降低重复模型调用
- `知识库检索缓存`：对相同检索请求做短期缓存，减少重复向量查询
- `处理状态追踪`：记录告警在接收、检索、分析、评估、执行、提单等阶段的流转
- `性能指标采集`：采集知识检索、LLM 分析、评估、执行、整单耗时等指标
- `脚本安全扫描`：对高危修复命令做执行前拦截
- `审计日志`：对关键决策事件额外输出 audit 日志
- `批量处理`：消费侧支持缓冲多条告警后统一按优先级排序处理
- `智能路由`：支持通过路由规则把高风险或特定类型告警直接导向人工处理链路

### 批量处理与路由说明

- `AIOPS_BATCH_SIZE`
  - 每次从持续消费链路缓冲多少条告警后再统一处理
  - 默认为 `1`，表示保持单条实时处理

- `AIOPS_ROUTER_MANUAL_PATTERNS`
  - 逗号分隔的正则列表
  - 命中后直接走 `manual-first` 路由，优先提单，不进入 LLM 与自动执行链路
  - 适合高风险、强人工依赖场景

- `AIOPS_ROUTER_FAST_TRACK_PATTERNS`
  - 逗号分隔的正则列表
  - 当前用于打上快速处理标签并提升优先级，后续可继续扩展成独立快速通道
