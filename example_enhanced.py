#!/usr/bin/env python3
"""
AIOps 增强功能使用示例
展示如何使用新的 LLM 客户端、知识库和日志系统
"""

import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入增强功能
from core.logger import LogConfig, get_logger, log_execution
from plugins.openai_client import LLMClientFactory
from plugins.knowledge_enhanced import EnhancedKnowledgeBase

# 初始化日志系统
LogConfig.setup(
    log_dir="./logs",
    log_level="INFO",
    console_output=True,
    file_output=True,
    json_format=False,
)

# 获取日志器
logger = get_logger("example")


@log_execution(logger)
def example_llm_client():
    """示例：使用真实的 LLM 客户端"""
    logger.logger.info("=== 示例1: LLM 客户端使用 ===")

    # 方式1: 使用 OpenAI
    try:
        client = LLMClientFactory.create(
            provider="openai", model="gpt-4", temperature=0.7
        )

        prompt = """
        分析以下告警并提供解决方案：
        告警名称: CPU使用率过高
        告警级别: P1
        告警详情: 服务器 web-01 CPU使用率达到 95%
        
        请以 JSON 格式返回：
        {
            "root_cause_analysis": "根因分析",
            "troubleshooting_steps": ["步骤1", "步骤2"],
            "script_content": "修复脚本",
            "confidence_score": 0.85
        }
        """

        response = client.generate_proposal(prompt)
        logger.logger.info(f"OpenAI 响应: {response[:200]}...")

    except Exception as e:
        logger.error_occurred("LLMError", str(e))
        logger.logger.warning("OpenAI 不可用，尝试使用 Ollama...")

        # 方式2: 使用 Ollama（本地模型）
        try:
            client = LLMClientFactory.create(
                provider="ollama", model="qwen2.5:7b", base_url="http://localhost:11434"
            )

            response = client.generate_proposal(prompt)
            logger.logger.info(f"Ollama 响应: {response[:200]}...")

        except Exception as e2:
            logger.error_occurred("OllamaError", str(e2))
            logger.logger.error("所有 LLM 服务不可用")


@log_execution(logger)
def example_knowledge_base():
    """示例：使用增强知识库"""
    logger.logger.info("\n=== 示例2: 增强知识库使用 ===")

    # 创建知识库实例
    kb = EnhancedKnowledgeBase(storage_dir="./knowledge_store")

    # 导入文档（如果有文档的话）
    # doc_id = kb.import_document("./docs/runbook.md")
    # logger.logger.info(f"导入文档: {doc_id}")

    # 导入目录（如果有的话）
    # doc_ids = kb.import_directory("./docs/")
    # logger.logger.info(f"导入 {len(doc_ids)} 个文档")

    # 搜索知识
    results = kb.search("CPU 使用率过高", top_k=3)
    logger.logger.info(f"搜索结果: {len(results)} 条")

    for i, result in enumerate(results, 1):
        logger.logger.info(
            f"  {i}. 分数: {result['score']}, 内容: {result['content'][:50]}..."
        )

    # 获取统计信息
    stats = kb.export_stats()
    logger.logger.info(f"知识库统计: {stats}")


@log_execution(logger)
def example_pipeline_integration():
    """示例：在 Pipeline 中集成新功能"""
    logger.logger.info("\n=== 示例3: Pipeline 集成 ===")

    # 这里展示如何在现有 Pipeline 中使用新功能
    # 实际集成需要修改 pipeline.py

    from core.models import Alert
    from core.llm_engine import CognitiveEngine
    from plugins.openai_client import LLMClientFactory
    from plugins.mocks import MockKnowledgeBase

    # 创建告警
    alert = Alert(
        alert_id="ALERT-001",
        title="磁盘空间不足",
        level="P2",
        content="服务器 db-01 /var/log 目录使用率超过 90%",
        timestamp="2024-01-15T10:30:00",
        source="Prometheus",
        raw_data={},
    )

    # 使用真实 LLM 客户端
    try:
        llm_client = LLMClientFactory.create(provider="ollama", model="qwen2.5:7b")
    except:
        from plugins.mocks import MockLLMClient

        llm_client = MockLLMClient()

    # 创建认知引擎
    engine = CognitiveEngine(llm_client)

    # 模拟知识检索
    kb = MockKnowledgeBase()
    history_sops = kb.search_experience(alert.title)

    # 分析告警
    logger.llm_analysis_start(alert.alert_id)
    proposal = engine.analyze_alert(alert, history_sops)
    logger.llm_analysis_complete(alert.alert_id, proposal.plan.confidence_score)

    logger.logger.info(f"根因分析: {proposal.plan.root_cause_analysis}")
    logger.logger.info(f"置信度: {proposal.plan.confidence_score}")


def main():
    """主函数"""
    logger.logger.info("AIOps 增强功能演示开始")

    # 运行示例
    example_llm_client()
    example_knowledge_base()
    example_pipeline_integration()

    logger.logger.info("\n演示完成！")


if __name__ == "__main__":
    main()
