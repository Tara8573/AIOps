import json
import os
from typing import Any, Dict, List, Optional

from core.cache import cache_factory, TTLCache, stable_cache_key
from core.llm_call_log import llm_call_log
from core.llm_observed import llm_call_context
from core.models import Alert, ActionPlan, LLMProposal, SkillExecutionResult
from core.observability import metrics
from interfaces.base import ILLMClient

_MAX_SOP_ITEMS = int(os.getenv("AIOPS_MAX_SOP_ITEMS", "3"))
_MAX_SOP_CHARS = int(os.getenv("AIOPS_MAX_SOP_CHARS", "300"))
_DOMAIN_HINT_CONFIDENCE_THRESHOLD = float(
    os.getenv("AIOPS_DOMAIN_HINT_CONFIDENCE_THRESHOLD", "0.6")
)


DOMAIN_HINTS: Dict[str, str] = {
    "disk": (
        "该告警属于磁盘类问题。请优先从以下方向排查：\n"
        "1. 使用 df -h / df -i 确认磁盘空间和 inode 使用情况\n"
        "2. 使用 du -sh 定位占用最大的目录\n"
        "3. 检查日志轮转（logrotate）是否正常工作\n"
        "4. 确认是否有异常进程在大量写入\n"
        "5. 评估清理方案的安全性（不要删除正在使用的文件）"
    ),
    "cpu": (
        "该告警属于 CPU 类问题。请优先从以下方向排查：\n"
        "1. 使用 top / htop 确认 CPU 占用最高的进程\n"
        "2. 检查是否存在死循环、正则回溯或线程暴涨\n"
        "3. 对于 Java 应用，使用 jstack 查看线程堆栈\n"
        "4. 检查系统负载（uptime）和进程数\n"
        "5. 确认是否有定时任务或批量作业触发"
    ),
    "memory": (
        "该告警属于内存类问题。请优先从以下方向排查：\n"
        "1. 使用 free -m 确认内存和 swap 使用\n"
        "2. 使用 top / ps aux 定位内存占用最高的进程\n"
        "3. 检查 dmesg 中的 OOM killer 记录\n"
        "4. 对于 Java 应用，检查 heap 使用和 GC 日志（jmap / jstat）\n"
        "5. 评估是否存在内存泄漏（持续增长趋势）"
    ),
    "network": (
        "该告警属于网络类问题。请优先从以下方向排查：\n"
        "1. 使用 ping / traceroute 检查链路连通性\n"
        "2. 使用 ss -s / netstat -an 检查连接数和端口状态\n"
        "3. 检查防火墙规则和 iptables\n"
        "4. 使用 tcpdump / ethtool 排查丢包或网卡问题\n"
        "5. 确认 DNS 解析是否正常"
    ),
    "service": (
        "该告警属于服务类问题。请优先从以下方向排查：\n"
        "1. 使用 systemctl status / kubectl describe 检查服务状态\n"
        "2. 检查服务日志（journalctl / kubectl logs）\n"
        "3. 确认资源限制（memory limit / cpu limit）是否触达\n"
        "4. 检查上下游依赖服务的健康状态\n"
        "5. 确认最近是否有部署或配置变更"
    ),
    "database": (
        "该告警属于数据库类问题。请优先从以下方向排查：\n"
        "1. 检查连接数（pg_stat_activity / SHOW PROCESSLIST）\n"
        "2. 检查慢查询和锁等待\n"
        "3. 检查主从复制状态（SHOW SLAVE STATUS / pg_stat_replication）\n"
        "4. 确认磁盘空间和 I/O 性能\n"
        "5. 检查连接池配置和实际使用情况"
    ),
    "config": (
        "该告警属于配置类问题。请优先从以下方向排查：\n"
        "1. 对比当前配置与基线版本的差异\n"
        "2. 检查最近的变更记录（git / CMDB / Ansible）\n"
        "3. 验证配置语法是否正确\n"
        "4. 确认环境变量和密钥是否正确\n"
        "5. 评估回滚方案的影响范围"
    ),
    "security": (
        "该告警属于安全类问题。请优先从以下方向排查：\n"
        "1. 检查 auth.log / secure 日志确认异常登录\n"
        "2. 使用 last / who 检查当前和最近的登录会话\n"
        "3. 检查证书有效期和 TLS 配置\n"
        "4. 确认防火墙和 fail2ban 规则\n"
        "5. 评估是否需要立即封禁可疑 IP"
    ),
}


def _record_parse_failure(*, stage: str, role: str, alert_id: str,
                          error: Exception, raw_response: str) -> None:
    """LLMO-3.1: 解析失败补记调用事件（failure_type=parse_failure）。

    HTTP 调用本身成功，解析失败是质量事实；观测失败不影响主流程（规格 2.3 原则 6）。
    """
    try:
        llm_call_log.emit_parse_failure(
            stage=stage,
            role=role,
            alert_id=alert_id,
            error=f"{type(error).__name__}: {error}",
            raw_response=raw_response,
        )
    except Exception:
        pass


def parse_action_plan_json(response_text: str) -> ActionPlan:
    """解析 LLM 返回的 JSON 字符串为 ActionPlan。

    复用于 eval_runner，避免重复解析逻辑。
    """
    cleaned_text = response_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[7:]
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[3:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3]
    data = json.loads(cleaned_text.strip())
    return ActionPlan(**data)


def _format_correlated_alerts(alert: Alert) -> str:
    """格式化关联告警上下文"""
    if not alert.correlated_alerts:
        return "无关联告警"
    lines = []
    for ca in alert.correlated_alerts:
        lines.append(
            f"- [{ca.get('level', '?')}] {ca.get('title', '?')} | "
            f"{ca.get('content', '')[:200]}"
        )
    return "\n".join(lines)


class CognitiveEngine:
    """认知引擎：封装与大模型对话、Prompt工程及结构化解析的过程"""

    def __init__(self, llm_client: ILLMClient):
        self.llm_client = llm_client
        # FR-2.1: 提案缓存外置（Redis 可用 → 多实例共享；否则本地）
        self._proposal_cache = cache_factory(
            ttl_seconds=int(os.getenv("AIOPS_LLM_CACHE_TTL_SECONDS", "300")),
            max_size=int(os.getenv("AIOPS_LLM_CACHE_MAX_SIZE", "256")),
        )

    def analyze_alert(
        self,
        alert: Alert,
        history_sops: List[str],
        skill_results: Optional[List[SkillExecutionResult]] = None,
        intent_category: str = "general",
        intent_confidence: float = 0.0,
        intent_reason: str = "",
        retry_feedback: Optional[str] = None,
        prev_plan: Optional[ActionPlan] = None,
    ) -> LLMProposal:
        """接收告警和历史经验，提示 LLM 生成排查动作。

        Args:
            retry_feedback: 迭代修正时传入上一轮评估的失败原因。
            prev_plan: 迭代修正时传入上一轮的 ActionPlan，用于增量重试。
        """
        if retry_feedback and prev_plan:
            return self._incremental_retry(
                alert, prev_plan, retry_feedback
            )

        # ⚠ 键口径（有意权衡，已在 config/default.yaml llm.cache 显式声明）：
        # 键 = (title, level, content[:200], intent_category)，不含 history_sops/
        # skill_results —— 同告警不同技能结果共享方案，换更高命中率；
        # 如需按技能结果隔离，把 skill 摘要纳入下方 stable_cache_key 参数
        cache_key = stable_cache_key(
            alert.title,
            alert.level,
            alert.content[:200],
            intent_category,
        )
        cached_plan = self._proposal_cache.get(cache_key)
        if cached_plan is not None:
            metrics.incr("llm.cache.hit")
            return LLMProposal(alert_id=alert.alert_id, plan=ActionPlan(**cached_plan))

        metrics.incr("llm.cache.miss")
        sops_str = self._truncate_sops(history_sops)
        skill_results = skill_results or []
        skill_context = self._format_skill_context(skill_results)
        domain_hint = ""
        if intent_confidence >= _DOMAIN_HINT_CONFIDENCE_THRESHOLD:
            domain_hint = DOMAIN_HINTS.get(intent_category, "")
        correlated_context = _format_correlated_alerts(alert)
        nl = "\n"

        timestamp_info = alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        hour = alert.timestamp.hour
        time_period = "凌晨" if hour < 6 else ("上午" if hour < 12 else ("下午" if hour < 18 else "晚间"))

        prompt = f"""你是一个生产系统 AIOps 专家。请分析以下告警：

【告警名称】: {alert.title}
【告警级别】: {alert.level}
【告警时间】: {timestamp_info}（{time_period}）
【告警来源】: {alert.source}
【告警详情】: {alert.content}
【系统初步分类】: 类别={intent_category}（置信度={intent_confidence:.2f}），依据：{intent_reason or '未提供'}
【时间窗口内关联告警】:
{correlated_context}
【历史经验推荐】:
{sops_str}
【可用 Skills 执行结果】:
{skill_context}
{"【领域诊断指引】:" + nl + domain_hint if domain_hint else ""}

请按以下步骤进行分析：
1. 先列出 2-4 个可能的根因假设，按可能性从高到低排列
2. 逐个评估每个假设，说明支持或反对的证据
3. 综合以上分析得出最可能的根因
4. 给出具体的排查步骤和修复方案

请严格以 JSON 格式输出，包含以下字段：
- possible_hypotheses (list[object]): 可能的根因假设列表，每个包含 description(假设描述) 和 likelihood("high"/"medium"/"low")
- root_cause_analysis (str): 综合所有假设后的最终根因推测，说明为什么排除或选择了某个假设
- troubleshooting_steps (list[str]): 分步排查建议，每步要具体可执行
- script_content (str | null): 如果有明确且安全的修复手段，提供可执行脚本代码，否则为 null
- confidence_score (float): 你对该结论的置信度，0.0 到 1.0 的小数。要诚实评估，不确定时不要虚高。
"""

        # LLMO-5.2: 调用上下文（stage/role/alert_id 入调用事件，多 Agent 串联）
        with llm_call_context(alert_id=alert.alert_id, stage="analyze", role="diagnosis"):
            response_text = self.llm_client.generate_proposal(prompt)

        try:
            action_plan = parse_action_plan_json(response_text)
            self._proposal_cache.set(cache_key, action_plan.model_dump())
            return LLMProposal(alert_id=alert.alert_id, plan=action_plan)
        except Exception as e:
            metrics.incr("llm.parse.failure")
            _record_parse_failure(
                stage="analyze", role="diagnosis", alert_id=alert.alert_id,
                error=e, raw_response=response_text,
            )
            fallback_plan = ActionPlan(
                root_cause_analysis=f"解析LLM响应失败。Exception: {str(e)}\nRaw Response: {response_text[:100]}",
                script_content=None,
                confidence_score=0.0,
            )
            return LLMProposal(alert_id=alert.alert_id, plan=fallback_plan)

    def _incremental_retry(
        self, alert: Alert, prev_plan: ActionPlan, feedback: str
    ) -> LLMProposal:
        """增量重试：只发送上一轮方案 + 失败原因，不重发完整上下文"""
        metrics.incr("llm.incremental_retry")

        steps_str = "\n".join(
            f"  {i+1}. {s}" for i, s in enumerate(prev_plan.troubleshooting_steps)
        )
        prompt = f"""你是一个生产系统 AIOps 专家。你之前对以下告警给出了诊断方案，但被评估系统标记为存在问题。

【告警】: {alert.title} ({alert.level})
【告警详情】: {alert.content[:300]}

【你上一轮的方案】:
根因分析: {prev_plan.root_cause_analysis}
排查步骤:
{steps_str}
修复脚本: {(prev_plan.script_content or '无')[:300]}
置信度: {prev_plan.confidence_score}

【评估反馈】:
{feedback}

请根据以上反馈修正你的方案。重点修正被指出的问题，不要重复之前错误的分析。

请严格以 JSON 格式输出（字段同前）：
- possible_hypotheses (list[object]): 修正后的根因假设
- root_cause_analysis (str): 修正后的根因分析
- troubleshooting_steps (list[str]): 修正后的排查步骤
- script_content (str | null): 修正后的修复脚本
- confidence_score (float): 修正后的置信度
"""

        # LLMO-5.2: 调用上下文（迭代修正重试也是一次调用事实）
        with llm_call_context(alert_id=alert.alert_id, stage="retry", role="diagnosis"):
            response_text = self.llm_client.generate_proposal(prompt)

        try:
            action_plan = parse_action_plan_json(response_text)
            return LLMProposal(alert_id=alert.alert_id, plan=action_plan)
        except Exception as e:
            metrics.incr("llm.parse.failure")
            _record_parse_failure(
                stage="retry", role="diagnosis", alert_id=alert.alert_id,
                error=e, raw_response=response_text,
            )
            return LLMProposal(alert_id=alert.alert_id, plan=prev_plan)

    @staticmethod
    def _truncate_sops(history_sops: List[str]) -> str:
        """截断 SOP 列表：限制条数和每条长度"""
        if not history_sops:
            return "无历史参考"
        truncated = []
        for i, sop in enumerate(history_sops[:_MAX_SOP_ITEMS]):
            if len(sop) > _MAX_SOP_CHARS:
                sop = sop[:_MAX_SOP_CHARS] + "..."
            truncated.append(sop)
        remaining = len(history_sops) - _MAX_SOP_ITEMS
        result = "\n---\n".join(truncated)
        if remaining > 0:
            result += f"\n（另有 {remaining} 条历史经验已省略）"
        return result

    @staticmethod
    def _format_skill_context(skill_results: List[SkillExecutionResult]) -> str:
        if not skill_results:
            return "无可用 skill 输出"

        lines = []
        for result in skill_results:
            lines.append(
                f"- Skill={result.skill_name}; success={result.is_successful}; "
                f"summary={result.summary}; "
                f"recommended={result.recommended_actions}"
            )
        return "\n".join(lines)
