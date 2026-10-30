from core.models import Alert, SkillExecutionResult
from interfaces.base import ISkill


class DiskCleanupSkill(ISkill):
    """面向磁盘空间告警的示例 skill。"""

    def name(self) -> str:
        return "disk_cleanup"

    def can_handle(self, alert: Alert) -> bool:
        text = f"{alert.title} {alert.content}".lower()
        keywords = ("disk", "space", "/var", "usage", "磁盘", "inode", "iops", "storage")
        return any(keyword in text for keyword in keywords)

    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        return SkillExecutionResult(
            skill_name=self.name(),
            summary="识别为磁盘空间类告警，建议优先确认大目录与日志膨胀情况。",
            details={
                "check_commands": [
                    "df -h",
                    "df -i",
                    "du -sh /var/log/* | sort -rh | head -20",
                    "find /var -type f -size +100M -exec ls -lh {} \\;",
                ],
                "cleanup_targets": ["/var/log", "/tmp", "/var/cache"],
                "matched_alert": alert.alert_id,
            },
            recommended_actions=[
                "先执行 df -h 和 df -i 确认磁盘/inode 占用",
                "定位 /var/log 下的异常大文件",
                "检查 logrotate 是否正常工作",
                "确认日志是否可归档或清理",
            ],
            confidence_score=0.92,
            is_successful=True,
        )


class MemoryDiagnosisSkill(ISkill):
    """面向内存告警的诊断 skill。"""

    def name(self) -> str:
        return "memory_diagnosis"

    def can_handle(self, alert: Alert) -> bool:
        text = f"{alert.title} {alert.content}".lower()
        keywords = ("memory", "oom", "swap", "内存", "heap", "oomkilled", "leak")
        return any(keyword in text for keyword in keywords)

    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        return SkillExecutionResult(
            skill_name=self.name(),
            summary="识别为内存类告警，建议检查内存使用、OOM 记录和进程内存分布。",
            details={
                "check_commands": [
                    "free -m",
                    "cat /proc/meminfo | grep -E 'MemTotal|MemFree|MemAvailable|SwapTotal|SwapFree'",
                    "dmesg | grep -i 'oom\\|killed process' | tail -20",
                    "ps aux --sort=-%mem | head -15",
                    "vmstat 1 5",
                ],
                "common_causes": [
                    "Java heap 内存泄漏",
                    "容器 memory limit 设置过低",
                    "内存碎片化",
                    "Swap 空间不足",
                ],
                "matched_alert": alert.alert_id,
            },
            recommended_actions=[
                "使用 free -m 确认内存和 swap 使用情况",
                "检查 dmesg 中的 OOM killer 记录",
                "使用 ps 定位内存占用最高的进程",
                "对于 Java 应用检查 heap dump 和 GC 日志",
            ],
            confidence_score=0.88,
            is_successful=True,
        )


class NetworkDiagnosisSkill(ISkill):
    """面向网络告警的诊断 skill。"""

    def name(self) -> str:
        return "network_diagnosis"

    def can_handle(self, alert: Alert) -> bool:
        text = f"{alert.title} {alert.content}".lower()
        keywords = (
            "network", "timeout", "latency", "packet", "网络", "超时",
            "connection refused", "dns", "丢包", "packet loss", "connect timed out",
        )
        return any(keyword in text for keyword in keywords)

    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        return SkillExecutionResult(
            skill_name=self.name(),
            summary="识别为网络类告警，建议检查链路连通性、连接数和防火墙规则。",
            details={
                "check_commands": [
                    "ss -s",
                    "netstat -an | awk '/ESTABLISHED|TIME_WAIT|CLOSE_WAIT/ {print $6}' | sort | uniq -c | sort -rn",
                    "cat /proc/net/snmp | grep -E 'Tcp|Udp'",
                    "ip -s link show",
                ],
                "common_causes": [
                    "上游服务宕机或过载",
                    "连接池耗尽",
                    "防火墙规则变更",
                    "DNS 解析异常",
                    "网络带宽饱和",
                ],
                "matched_alert": alert.alert_id,
            },
            recommended_actions=[
                "使用 ping/traceroute 检查链路连通性",
                "使用 ss -s 检查连接数分布",
                "检查防火墙和 iptables 规则",
                "使用 tcpdump 抓包分析",
            ],
            confidence_score=0.85,
            is_successful=True,
        )


class ServiceHealthSkill(ISkill):
    """面向服务/容器告警的健康检查 skill。"""

    def name(self) -> str:
        return "service_health"

    def can_handle(self, alert: Alert) -> bool:
        text = f"{alert.title} {alert.content}".lower()
        keywords = (
            "service", "pod", "container", "服务", "进程",
            "crashloop", "restart", "deploy", "5xx", "503", "502",
        )
        return any(keyword in text for keyword in keywords)

    def run(self, alert: Alert, context: dict) -> SkillExecutionResult:
        return SkillExecutionResult(
            skill_name=self.name(),
            summary="识别为服务类告警，建议检查服务状态、资源限制和近期变更。",
            details={
                "check_commands": [
                    "systemctl status <service_name>",
                    "journalctl -u <service_name> --since '1 hour ago' --no-pager | tail -50",
                    "kubectl describe pod <pod_name> -n <namespace>",
                    "kubectl logs <pod_name> -n <namespace> --tail=100",
                    "kubectl get events -n <namespace> --sort-by='.lastTimestamp' | tail -20",
                ],
                "common_causes": [
                    "OOM 导致容器反复重启",
                    "Liveness/Readiness 探针配置不当",
                    "资源限制（CPU/Memory limit）过低",
                    "依赖服务不可用",
                    "近期部署引入的 bug",
                    "配置变更未生效",
                ],
                "matched_alert": alert.alert_id,
            },
            recommended_actions=[
                "检查服务/容器当前状态和重启次数",
                "查看最近的服务日志，定位错误根因",
                "确认资源限制（memory limit / cpu limit）",
                "检查近期是否有部署或配置变更",
            ],
            confidence_score=0.87,
            is_successful=True,
        )
