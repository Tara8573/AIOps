"""DiagnosisAgent 单元测试。"""

import json
from typing import List, Optional

from agents.diagnosis_agent import DiagnosisAgent
from core.models import Alert, Feedback, LLMProposal
from core.skills import SkillRegistry
from interfaces.base import IKnowledgeBase, ILLMClient, ISkill
from plugins.skills import DiskCleanupSkill


class MockKB(IKnowledgeBase):
    def search_experience(self, alert_feature: str) -> List[str]:
        return ["SOP: 磁盘满时清理 /var/log"]

    def learn_new_experience(self, feedback: Feedback) -> bool:
        return True


class StaticLLM(ILLMClient):
    def generate_proposal(self, prompt: str) -> str:
        return json.dumps({
            "root_cause_analysis": "/var 磁盘空间被日志占满",
            "troubleshooting_steps": ["df -h 检查", "清理日志"],
            "script_content": "echo fix",
            "confidence_score": 0.9,
        }, ensure_ascii=False)


def _alert():
    return Alert(
        alert_id="ALT-DIAG-001",
        title="Disk Space Critical",
        level="Critical",
        content="/var disk usage > 98%",
    )


class TestDiagnosisAgent:

    def test_retrieve_knowledge_returns_sops(self):
        agent = DiagnosisAgent(
            llm_client=StaticLLM(),
            kb=MockKB(),
            skill_registry=SkillRegistry(),
        )
        sops = agent.retrieve_knowledge(_alert(), "disk")
        assert len(sops) == 1
        assert "清理" in sops[0]

    def test_run_skills_returns_results(self):
        agent = DiagnosisAgent(
            llm_client=StaticLLM(),
            kb=MockKB(),
            skill_registry=SkillRegistry([DiskCleanupSkill()]),
        )
        results = agent.run_skills(_alert(), {
            "history_sops": [],
            "alert": _alert().model_dump(),
            "intent": {"category": "disk"},
        })
        assert len(results) == 1
        assert results[0].skill_name == "disk_cleanup"

    def test_diagnose_full_pipeline(self):
        agent = DiagnosisAgent(
            llm_client=StaticLLM(),
            kb=MockKB(),
            skill_registry=SkillRegistry([DiskCleanupSkill()]),
        )
        result = agent.diagnose(
            alert=_alert(),
            intent_category="disk",
            intent_confidence=0.8,
            intent_reason="命中 disk 关键词",
        )
        assert "sops" in result
        assert "skill_results" in result
        assert "proposal" in result

        proposal = result["proposal"]
        assert isinstance(proposal, LLMProposal)
        assert proposal.plan.confidence_score == 0.9
        assert proposal.plan.script_content == "echo fix"

    def test_diagnose_with_retry_feedback(self):
        agent = DiagnosisAgent(
            llm_client=StaticLLM(),
            kb=MockKB(),
            skill_registry=SkillRegistry(),
        )
        result = agent.diagnose(
            alert=_alert(),
            retry_feedback="confidence too low",
            prev_plan=None,
        )
        assert result["proposal"] is not None
