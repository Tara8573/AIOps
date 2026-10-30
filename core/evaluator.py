import os
import re
from enum import Enum
from typing import List, Optional

from core.llm_observed import llm_call_context
from core.models import Alert, EvaluationResult, LLMProposal
from core.observability import metrics
from interfaces.base import ILLMClient


class Reversibility(Enum):
    """脚本可逆性分类"""
    REVERSIBLE = "reversible"              # 本身可逆，不需要回滚脚本
    NEEDS_ROLLBACK = "needs_rollback"      # 不可逆但可补救，需要生成回滚脚本
    IRREVERSIBLE = "irreversible"          # 不可逆且无法补救，必须人工审批


class ScriptReversibilityAnalyzer:
    """分析脚本的可逆性，决定是否需要回滚脚本。

    三类判定：
    - REVERSIBLE: 操作本身可逆（重启、移动文件、改配置），不需要回滚
    - NEEDS_ROLLBACK: 操作破坏性但可补救（删除文件、DELETE数据），需要生成回滚脚本
    - IRREVERSIBLE: 操作不可逆且无法补救（DROP TABLE、发通知），必须人工审批
    """

    # 本身可逆的操作 — 不需要回滚脚本
    REVERSIBLE_PATTERNS = [
        r"\bsystemctl\s+(restart|start|stop|reload)\b",
        r"\bservice\s+\S+\s+(restart|start|stop|reload)\b",
        r"\bkill\s+-15\b",                    # SIGTERM，可被 systemd 重新拉起
        r"\bkillall\b",
        r"\bcp\s+",                           # 复制文件
        r"\bchmod\b",
        r"\bchown\b",
        r"\bmkdir\b",
        r"\becho\s+.*>>",                     # 追加写入
        r"\btouch\b",
        r"\bip\s+",                           # 网络配置
        r"\biptables\b",
        r"\bsysctl\b",
        r"\bexport\b",
        r"\bulimit\b",
    ]

    # 破坏性但可补救 — 需要生成回滚脚本
    DESTRUCTIVE_SALVAGEABLE = [
        r"\brm\s+(-[a-zA-Z]*f|-[a-zA-Z]*r|--recursive|--force)",
        r"\bunlink\b",
        r"\bshred\b",
        r"\bsed\s+-i\b",                     # 修复：无备份的原地编辑实际不可逆，需回滚保护
        r"\bmv\s+",                           # 修复：无备份的 mv 覆盖目标不可逆（原误判 REVERSIBLE）
        r"\bDELETE\s+FROM\b",
        r"\bUPDATE\s+\S+\s+SET\b",
        r"\bINSERT\s+INTO\b",
        r"\bALTER\s+TABLE\b",
        r"\bTRUNCATE\b",
        r"\bformat\b.*\b(dev|sd[a-z]|nvme)",
    ]

    # 不可逆且无法补救 — 必须人工审批
    TRULY_IRREVERSIBLE = [
        r"\bDROP\s+(TABLE|DATABASE|INDEX|SCHEMA)\b",
        r"\bdd\s+if=.*of=/dev/",
        r"\bmkfs\b",
        r">\s*/dev/sd[a-z]",
        r"\bPOST\b.*\b(notify|alert|email|send|webhook)\b",
        r"\bcurl\s+-X\s*POST\b.*\b(hook|notify|send|alert)",
    ]

    @classmethod
    def analyze(cls, script: str) -> Reversibility:
        """分析脚本可逆性。

        判定优先级：IRREVERSIBLE > NEEDS_ROLLBACK > REVERSIBLE
        """
        if not script or not script.strip():
            return Reversibility.REVERSIBLE

        # 最高优先级：不可逆且无法补救
        for pattern in cls.TRULY_IRREVERSIBLE:
            if re.search(pattern, script, re.IGNORECASE):
                return Reversibility.IRREVERSIBLE

        # 次优先级：破坏性但可补救
        for pattern in cls.DESTRUCTIVE_SALVAGEABLE:
            if re.search(pattern, script, re.IGNORECASE):
                # 检查是否已有保护措施（备份、归档）
                has_backup_guard = any(
                    re.search(g, script, re.IGNORECASE)
                    for g in [
                        r"\bcp\s+.*\.bak",
                        r"\btar\b.*czf",
                        r"BACKUP_DIR\s*=",
                        r"ROLLBACK_DIR\s*=",
                        r"\bmv\s+.*\.bak",
                    ]
                )
                if has_backup_guard:
                    return Reversibility.REVERSIBLE
                return Reversibility.NEEDS_ROLLBACK

        return Reversibility.REVERSIBLE


class ScriptSafetyScanner:
    """Performs conservative script safety checks before execution."""

    DEFAULT_PATTERNS = [
        r"rm\s+-rf\s+/",
        r"mkfs",
        r"dd\s+if=",
        r">\s*/dev/sd[a-z]",
        r"reboot\b",
        r"shutdown\b",
        r"curl\s+.*\|\s*(sh|bash)",
        r"wget\s+.*\|\s*(sh|bash)",
        r"chmod\s+777",
        r":\(\)\s*\{\s*:\|\:&\s*\};:",
    ]

    def __init__(self, patterns: Optional[List[str]] = None):
        pattern_text = os.getenv("AIOPS_DANGEROUS_SCRIPT_PATTERNS", "").strip()
        configured_patterns = [p.strip() for p in pattern_text.split(",") if p.strip()]
        self.patterns = patterns or configured_patterns or self.DEFAULT_PATTERNS

    def find_risks(self, script: str) -> List[str]:
        findings = []
        for pattern in self.patterns:
            if re.search(pattern, script, re.IGNORECASE):
                findings.append(pattern)
        return findings

    def preflight_check(self, script: str) -> List[str]:
        """深度静态分析：返回全部问题（容器逃逸/动态执行 + 混淆/编码）。

        扫描器只负责分析（全量报告）；阻断策略由评估层决定 —— 逃逸/动态执行类
        阻断（见 preflight_blocking），混淆类告警不拦截（见 preflight_warnings），
        避免命令替换等合法写法被误杀。
        """
        return self.preflight_blocking(script) + self.preflight_warnings(script)

    def preflight_blocking(self, script: str) -> List[str]:
        """阻断级问题：容器逃逸/动态执行。"""
        issues: List[str] = []

        escape_patterns = [
            (r"/var/run/docker\.sock", "访问 Docker socket（容器逃逸风险）"),
            (r"--privileged", "特权模式标志"),
            (r"\bnsenter\b", "nsenter 进入其他 namespace"),
            (r"/proc/1/ns", "访问宿主机 namespace"),
            (r"\bmount\s+-t\s+", "尝试挂载文件系统"),
            (r"\bunshare\b", "创建新 namespace"),
            (r"\bpivot_root\b", "更改根文件系统"),
            (r"\beval\b", "eval 动态执行"),
            (r"\bexec\b", "exec 替换进程"),
        ]
        for pattern, desc in escape_patterns:
            if re.search(pattern, script, re.IGNORECASE):
                issues.append(desc)

        return issues

    def preflight_warnings(self, script: str) -> List[str]:
        """告警级问题（不阻断）：混淆/编码/内建网络 —— 人工审计线索，不拦截合法脚本。"""
        warnings: List[str] = []
        obfuscation_patterns = [
            (r"\\x[0-9a-fA-F]{2}", "十六进制编码字符"),
            (r"\\[0-7]{3}", "八进制编码字符"),
            (r"\$\(", "命令替换（可能绕过静态分析）"),
            (r"`[^`]+`", "反引号命令替换"),
            (r"base64\s+(-d|--decode)", "base64 解码（可能隐藏恶意载荷）"),
            (r"/dev/tcp/", "Bash 内建网络连接"),
        ]
        for pattern, desc in obfuscation_patterns:
            if re.search(pattern, script, re.IGNORECASE):
                warnings.append(f"混淆/动态执行: {desc}")
        return warnings


class _ConsistencyChecker:
    """Check logical consistency between root cause analysis and troubleshooting steps."""

    # CJK 整句 token 的滑窗粒度：中文无空格分词，整句 token 无法精确命中，
    # 改为“任一 2 字滑窗出现在对方文本中即算该关键词命中”
    # （修复：中文 RCA/步骤相关也被误拦 weakly related）
    _CJK_RE = re.compile(r"[一-鿿]")

    @classmethod
    def _cjk_grams(cls, text: str) -> set:
        """提取文本中 CJK 连续串的 2 字滑窗集合。"""
        grams = set()
        for w in re.findall(r"[一-鿿]+", text):
            if len(w) < 2:
                continue
            grams.update(w[i:i + 2] for i in range(len(w) - 1))
        return grams

    @classmethod
    def check(cls, alert: Alert, proposal: LLMProposal) -> Optional[str]:
        plan = proposal.plan
        if not plan.troubleshooting_steps:
            return None

        rca_lower = plan.root_cause_analysis.lower()
        steps_text = " ".join(plan.troubleshooting_steps).lower()
        alert_text = f"{alert.title} {alert.content}".lower()

        rca_mentions_alert = any(
            kw in rca_lower
            for kw in alert_text.split()
            if len(kw) > 3
        )
        if not rca_mentions_alert:
            # 中文告警/中文 RCA：空格分词失效，退化为 CJK 2-gram 重叠
            alert_grams = cls._cjk_grams(alert_text)
            if alert_grams:
                rca_grams = cls._cjk_grams(rca_lower)
                overlap = sum(1 for g in alert_grams if g in rca_grams)
                rca_mentions_alert = overlap >= max(1, round(len(alert_grams) * 0.3))

        steps_address_rca = False
        rca_keywords = [
            w for w in re.findall(r"[a-zA-Z0-9_\-./一-鿿]+", rca_lower)
            if len(w) > 2
        ]
        if rca_keywords:
            hit_count = 0
            steps_grams = cls._cjk_grams(steps_text)
            for kw in rca_keywords:
                if kw in steps_text:
                    hit_count += 1
                elif cls._CJK_RE.search(kw) and steps_grams:
                    # CJK 整句 token：任一 2-gram 出现在步骤中即算命中
                    kw_grams = cls._cjk_grams(kw)
                    if kw_grams and (kw_grams & steps_grams):
                        hit_count += 1
            steps_address_rca = hit_count >= max(1, len(rca_keywords) * 0.2)

        issues = []
        if not rca_mentions_alert and len(rca_keywords) > 0:
            issues.append("root cause analysis does not reference key alert info")
        if rca_keywords and not steps_address_rca:
            issues.append("troubleshooting steps weakly related to root cause")

        if issues:
            return "; ".join(issues)
        return None


class _LLMJudge:
    """Use LLM to perform a second-pass review of the proposal."""

    JUDGE_PROMPT_TEMPLATE = (
        "You are an AIOps proposal reviewer. Evaluate whether the diagnosis plan "
        "is reasonable for the given alert.\n\n"
        "[Alert]: {alert_title} ({alert_level})\n"
        "[Alert Detail]: {alert_content}\n\n"
        "[Root Cause Analysis]: {root_cause}\n"
        "[Troubleshooting Steps]:\n{steps}\n"
        "[Fix Script]: {script_or_none}\n\n"
        "Evaluate these 3 aspects, each with pass/fail and a brief reason:\n"
        "1. Does the root cause reasonably explain the alert?\n"
        "2. Do the steps effectively verify or locate this root cause?\n"
        "3. Does the fix script (if any) target this root cause?\n\n"
        'Output strict JSON: '
        '{{"root_cause_verdict":"pass"/"fail","root_cause_reason":"...",'
        '"steps_verdict":"pass"/"fail","steps_reason":"...",'
        '"script_verdict":"pass"/"fail"/"n/a","script_reason":"..."}}'
    )

    def __init__(self, llm_client: ILLMClient):
        self.llm_client = llm_client

    def judge(self, alert: Alert, proposal: LLMProposal) -> Optional[str]:
        import json

        plan = proposal.plan
        prompt = self.JUDGE_PROMPT_TEMPLATE.format(
            alert_title=alert.title,
            alert_level=alert.level,
            alert_content=alert.content[:500],
            root_cause=plan.root_cause_analysis,
            steps="\n".join(
                f"  {i+1}. {s}" for i, s in enumerate(plan.troubleshooting_steps)
            ),
            script_or_none=(plan.script_content or "")[:500] or "none",
        )

        try:
            # LLMO-5.2: 调用上下文（stage=judge，role=evaluation，按 alert_id 串联）
            with llm_call_context(alert_id=alert.alert_id, stage="judge", role="evaluation"):
                response = self.llm_client.generate_proposal(prompt)
            cleaned = response.strip()
            if cleaned.lower().startswith("```json"):
                cleaned = cleaned[7:]
            elif cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            data = json.loads(cleaned.strip())

            failures = []
            if data.get("root_cause_verdict") == "fail":
                failures.append(f"root cause review failed: {data.get('root_cause_reason', '')}")
            if data.get("steps_verdict") == "fail":
                failures.append(f"steps review failed: {data.get('steps_reason', '')}")
            if data.get("script_verdict") == "fail":
                failures.append(f"script review failed: {data.get('script_reason', '')}")

            if failures:
                return "; ".join(failures)
            return None
        except Exception:
            metrics.incr("pipeline.evaluation.llm_judge_error")
            return None


class SolutionEvaluator:
    """Evaluator: validates LLM proposals for correctness and safety."""

    def __init__(
        self,
        min_confidence: Optional[float] = None,
        llm_client: Optional[ILLMClient] = None,
    ):
        self.min_confidence = (
            min_confidence
            if min_confidence is not None
            else float(os.getenv("AIOPS_MIN_CONFIDENCE", "0.7"))
        )
        self.script_scanner = ScriptSafetyScanner()
        self.llm_judge_enabled = os.getenv("AIOPS_LLM_JUDGE_ENABLED", "0") == "1"
        self.llm_judge: Optional[_LLMJudge] = None
        if self.llm_judge_enabled and llm_client:
            self.llm_judge = _LLMJudge(llm_client)
        self.consistency_check_enabled = (
            os.getenv("AIOPS_CONSISTENCY_CHECK_ENABLED", "1") == "1"
        )

    def evaluate(self, alert: Alert, proposal: LLMProposal) -> EvaluationResult:
        plan = proposal.plan

        if plan.confidence_score < self.min_confidence:
            metrics.incr("pipeline.evaluation.blocked.low_confidence")
            return EvaluationResult(
                is_passed=False,
                reason=(
                    f"blocked: confidence too low "
                    f"({plan.confidence_score} < {self.min_confidence})"
                ),
                risk_level="Unknown",
            )

        script = plan.script_content
        if script:
            findings = self.script_scanner.find_risks(script)
            if findings:
                metrics.incr("pipeline.evaluation.blocked.script_risk")
                return EvaluationResult(
                    is_passed=False,
                    reason=f"blocked: dangerous commands detected {findings}",
                    risk_level="High",
                )

            reversibility = ScriptReversibilityAnalyzer.analyze(script)
            if reversibility == Reversibility.IRREVERSIBLE:
                metrics.incr("pipeline.evaluation.blocked.irreversible")
                return EvaluationResult(
                    is_passed=False,
                    reason=(
                        "blocked: script contains irreversible operations "
                        "with no recovery path, requires manual approval"
                    ),
                    risk_level="High",
                )
            if reversibility == Reversibility.NEEDS_ROLLBACK:
                metrics.incr("pipeline.evaluation.blocked.needs_rollback")
                return EvaluationResult(
                    is_passed=False,
                    reason=(
                        "blocked: script contains destructive operations "
                        "without backup/rollback protection. "
                        "Add backup steps (e.g., cp/tar before rm) "
                        "or include a rollback script."
                    ),
                    risk_level="Medium",
                )

            preflight_issues = self.script_scanner.preflight_blocking(script)
            if preflight_issues:
                metrics.incr("pipeline.evaluation.blocked.preflight")
                return EvaluationResult(
                    is_passed=False,
                    reason=f"blocked: preflight check failed - {preflight_issues}",
                    risk_level="High",
                )

            # 告警级（不阻断）：混淆/编码类仅记指标，不拦截合法修复脚本
            preflight_warns = self.script_scanner.preflight_warnings(script)
            if preflight_warns:
                metrics.incr("pipeline.evaluation.warn.preflight_obfuscation")

        if not plan.troubleshooting_steps:
            metrics.incr("pipeline.evaluation.blocked.missing_steps")
            return EvaluationResult(
                is_passed=False,
                reason="blocked: no troubleshooting steps provided",
                risk_level="Medium",
            )

        if self.consistency_check_enabled:
            consistency_issue = _ConsistencyChecker.check(alert, proposal)
            if consistency_issue:
                metrics.incr("pipeline.evaluation.blocked.inconsistency")
                return EvaluationResult(
                    is_passed=False,
                    reason=f"blocked: consistency check failed - {consistency_issue}",
                    risk_level="Medium",
                )

        if self.llm_judge and self.llm_judge_enabled:
            metrics.incr("pipeline.evaluation.llm_judge_invoked")
            judge_issue = self.llm_judge.judge(alert, proposal)
            if judge_issue:
                metrics.incr("pipeline.evaluation.blocked.llm_judge")
                return EvaluationResult(
                    is_passed=False,
                    reason=f"blocked: LLM Judge rejected - {judge_issue}",
                    risk_level="Medium",
                )

        metrics.incr("pipeline.evaluation.passed")
        return EvaluationResult(
            is_passed=True,
            reason="passed: confidence OK, consistent, and safe",
            risk_level="Low",
        )
