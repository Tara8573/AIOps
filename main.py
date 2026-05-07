import os
from pathlib import Path

from dotenv import load_dotenv

from core.models import Alert
from plugins.mocks import MockApprovalGate, MockExecutor, MockJiraTicketing, MockKnowledgeBase, MockLLMClient
from pipeline import AIOpsPipeline

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")


def build_kb():
    if os.getenv("AIOPS_USE_PG_KB", "0") != "1":
        return MockKnowledgeBase()

    from plugins.postgres_kb import PostgresVectorKnowledgeBase

    kb = PostgresVectorKnowledgeBase(
        dsn=os.getenv("AIOPS_PG_DSN"),
        vector_dim=int(os.getenv("AIOPS_PG_VECTOR_DIM", "1024")),
        top_k=int(os.getenv("AIOPS_PG_TOP_K", "3")),
    )
    kb.seed_experience(
        alert_feature="disk usage /var critical",
        content="历史SOP: 发现磁盘空间满可清理 /var/log 并检查大文件来源",
    )
    return kb


def demo():
    print("="*50)
    print("AIOps 智能告警全自动处理框架 Demo")
    print("="*50)

    # 依赖组装 (Dependency Injection)
    kb = build_kb()
    executor = MockExecutor()
    approval_gate = MockApprovalGate()
    ticketing = MockJiraTicketing()
    
    # 模拟一条应用级告警
    alert = Alert(
        alert_id="ALT-2023-0901",
        title="Payment Service - Disk Space Critical",
        level="Critical",
        content="Host 10.10.1.2 disk usage > 98% in /var"
    )

    # ==========================================
    # 用例 1：LLM 给出了高危清理脚本（如 rm -rf / ）
    # ==========================================
    print("\n>>> 【演示用例 1】 触发非法高危命令拦截并提单辅助人工")
    bad_llm = MockLLMClient(override_script="rm -rf /var/log/old_logs/* ; rm -rf / ", confidence=0.9)
    pipeline_case1 = AIOpsPipeline(
        kb=kb,
        llm=bad_llm,
        executor=executor,
        approval_gate=approval_gate,
        ticketing=ticketing,
    )
    
    # 执行处理，因为高危，会被拦截提单
    ticket_id = pipeline_case1.process_alert(alert)

    # ==========================================
    # 用例 2：LLM 给出了正常安全的清理脚本
    # ==========================================
    print("\n>>> 【演示用例 2】 正常脚本先人工确认，再执行自动化物理修复")
    good_llm = MockLLMClient(override_script="find /var/log -type f -mtime +7 -delete", confidence=0.88)
    pipeline_case2 = AIOpsPipeline(
        kb=kb,
        llm=good_llm,
        executor=executor,
        approval_gate=approval_gate,
        ticketing=ticketing,
    )
    
    # 会评估通过，但仍需人工确认后才交由 MockExecutor 打印执行
    pipeline_case2.process_alert(alert)

    # ==========================================
    # 用例 3：知识库闭环学习反馈机制
    # ==========================================
    print("\n>>> 【演示用例 3】 异步反馈拉取与知识沉淀")
    if ticket_id:
        # 当运维人员在 Jira 上排查并解决问题关闭单子后，系统拉取最终经验灌入知识库
        pipeline_case1.sync_feedbacks([ticket_id])

if __name__ == "__main__":
    demo()
