from core.models import Alert, SkillExecutionResult
from interfaces.base import ISkill


class DiskCleanupSkill(ISkill):
    """面向磁盘空间告警的示例 skill。"""

    def name(self) -> str:
        return "disk_cleanup"

    def can_handle(self, alert: Alert) -> bool:
        text = f"{alert.title} {alert.content}".lower()
        keywords = ("disk", "space", "/var", "usage", "磁盘")
        return any(keyword in text for keyword in keywords)

    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        return SkillExecutionResult(
            skill_name=self.name(),
            summary="识别为磁盘空间类告警，建议优先确认大目录与日志膨胀情况。",
            details={
                "check_commands": ["df -h", "du -sh /var/log/* | sort -h"],
                "cleanup_targets": ["/var/log", "/tmp"],
                "matched_alert": alert.alert_id,
            },
            recommended_actions=[
                "先执行 df -h 确认磁盘占用",
                "定位 /var/log 下的异常大文件",
                "确认日志是否可归档或清理",
            ],
            confidence_score=0.92,
            is_successful=True,
        )
