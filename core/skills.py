from typing import Iterable, List, Optional

from core.models import Alert, SkillExecutionResult
from interfaces.base import ISkill


class SkillRegistry:
    """管理并调度可用 skills。"""

    def __init__(self, skills: Optional[Iterable[ISkill]] = None):
        self._skills: List[ISkill] = list(skills or [])

    def register(self, skill: ISkill) -> None:
        self._skills.append(skill)

    def list_skills(self) -> List[str]:
        return [skill.name() for skill in self._skills]

    def match(self, alert: Alert) -> List[ISkill]:
        return [skill for skill in self._skills if skill.can_handle(alert)]

    def run(self, alert: Alert, context: dict) -> List[SkillExecutionResult]:
        results: List[SkillExecutionResult] = []
        for skill in self.match(alert):
            try:
                result = skill.run(alert, context)
            except Exception as exc:
                result = SkillExecutionResult(
                    skill_name=skill.name(),
                    summary=f"skill 执行失败: {exc}",
                    confidence_score=0.0,
                    is_successful=False,
                )
            results.append(result)
        return results
