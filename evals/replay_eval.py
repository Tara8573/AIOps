"""离线重放评测（LLMO-4.1，规格：docs/LLM调用可观测性与质量评估开发规格说明书.md）。

对历史 raw response 重新执行五维评分 / 一致性检查，零 LLM 调用、零网络（确定性重放）。

两种输入源：
1. --report evals/reports/eval_loop_result.json（主路径，LLMO-4.2 落盘的 raw_response）
   按 case_id 关联 fixtures 用例，重放评分并校验与在线评分一致（consistency 检查）。
2. --calls logs/llm_calls.jsonl（调用事件日志，LLMO-2.1）
   按 prompt_hash 匹配 fixtures 用例（模板渲染后指纹一致才计入），不匹配计入 skipped。

用途：调整 EvalScorer 权重/pass_threshold、SolutionEvaluator 阈值后，对历史调用
离线重放对比，无需花 token 重跑。

用法:
    python -m evals.replay_eval --report evals/reports/eval_loop_result.json --fixtures evals/fixtures.json
    python -m evals.replay_eval --calls logs/llm_calls.jsonl --fixtures evals/fixtures.json \
        --template evals/reports/eval_loop_result.json
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from statistics import mean

# 确保项目根目录在 sys.path 中
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from core.eval_models import CaseResult, ExpectedOutcome
from core.eval_runner import EvalRunner, EvalScorer, PromptTemplateManager
from core.llm_call_log import content_hash
from core.llm_engine import parse_action_plan_json
from core.models import Alert


# ---------------------------------------------------------------------------
# fixtures 与模板加载
# ---------------------------------------------------------------------------

def load_cases(path: str) -> dict:
    """加载 fixtures 用例，返回 case_id → EvalCase 映射。"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {case["case_id"]: case for case in data["cases"]}


def load_template_from_report(report_path: str) -> str:
    """从 eval 报告读取 prompt 模板（RunReport.prompt_template）。"""
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)
    template = report.get("prompt_template")
    if not template:
        raise SystemExit(f"[ReplayEval] 报告缺少 prompt_template: {report_path}")
    return template


def expected_prompt_hashes(template: str, cases: dict) -> dict:
    """按 eval 相同方式渲染 fixtures 用例 → prompt_hash → case 映射（确定性）。"""
    mgr = PromptTemplateManager(template)
    result = {}
    for case_id, case in cases.items():
        alert = Alert(**case["alert"])
        prompt = mgr.render(alert, [], "无可用 skill 输出")
        result[content_hash(prompt)] = case_id
    return result


# ---------------------------------------------------------------------------
# 重放评分
# ---------------------------------------------------------------------------

def score_raw_response(raw_response: str, case: dict, pass_threshold: float) -> CaseResult:
    """对 raw response 重放五维评分（与 EvalRunner 相同逻辑，零 LLM 调用）。"""
    scorer = EvalScorer(pass_threshold=pass_threshold)
    alert = Alert(**case["alert"])
    plan = parse_action_plan_json(raw_response)
    dimensions = scorer.score(plan, ExpectedOutcome(**case["expected"]))
    overall = scorer.overall_score(dimensions)
    return CaseResult(
        case_id=case["case_id"],
        alert_title=alert.title,
        alert_level=alert.level,
        dimensions=dimensions,
        overall_score=overall,
        passed=overall >= pass_threshold,
        generated_plan=plan.model_dump(),
        latency_ms=0.0,
    )


def replay_from_report(report_path: str, cases: dict, pass_threshold: float) -> dict:
    """主路径：重放 eval 报告中的 raw_response，并校验与在线评分一致。"""
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    all_results = []
    consistency = []
    for key in ("train_results", "test_results"):
        for stored in report.get(key, []):
            case = cases.get(stored.get("case_id"))
            raw = stored.get("raw_response")
            if case is None or not raw:
                all_results.append(_skipped_result(stored, reason="case 未匹配或无 raw_response"))
                consistency.append({"case_id": stored.get("case_id"), "consistent": None})
                continue
            replayed = score_raw_response(raw, case, pass_threshold)
            all_results.append(replayed)
            consistent = abs(replayed.overall_score - stored.get("overall_score", 0.0)) < 1e-6
            consistency.append({
                "case_id": stored.get("case_id"),
                "consistent": consistent,
                "online_score": stored.get("overall_score"),
                "replay_score": replayed.overall_score,
            })

    return _build_result(report_path, all_results, consistency)


def replay_from_calls(calls_path: str, cases: dict, template: str, pass_threshold: float) -> dict:
    """调用事件日志路径：按 prompt_hash 匹配用例，不匹配计入 skipped。"""
    expected = expected_prompt_hashes(template, cases)
    hash_to_case = {h: cases[cid] for h, cid in expected.items()}

    all_results = []
    skipped = 0
    total = 0
    with open(calls_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            event = json.loads(line)
            if event.get("outcome") != "success":
                skipped += 1
                continue
            case = hash_to_case.get(event.get("prompt_hash"))
            raw = event.get("response")
            if case is None or not raw:
                skipped += 1
                continue
            all_results.append(score_raw_response(raw, case, pass_threshold))

    result = _build_result(calls_path, all_results, [])
    result["total_events"] = total
    result["skipped"] = skipped
    return result


def _skipped_result(stored: dict, reason: str) -> CaseResult:
    return CaseResult(
        case_id=stored.get("case_id", "unknown"),
        alert_title=stored.get("alert_title", ""),
        alert_level=stored.get("alert_level", ""),
        dimensions=[],
        overall_score=0.0,
        passed=False,
        error=reason,
    )


def _build_result(source: str, results: list, consistency: list) -> dict:
    scored = [r for r in results if not r.error]
    # 复用 EvalRunner 聚合逻辑（纯计算，无 LLM/网络）
    runner = EvalRunner.__new__(EvalRunner)
    runner.scorer = EvalScorer()
    aggregate = runner._compute_aggregate(scored)
    return {
        "replay_run_id": uuid.uuid4().hex[:8],
        "source": source,
        "total_cases": len(results),
        "scored_cases": len(scored),
        "aggregate": aggregate.model_dump(),
        "consistency_check": consistency,
        "per_case": [
            {
                "case_id": r.case_id,
                "overall_score": r.overall_score,
                "passed": r.passed,
                "error": r.error,
            }
            for r in results
        ],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AIOps 离线重放评测（零 LLM 调用）")
    parser.add_argument("--report", default=None, help="eval 报告路径（主路径，raw_response 重放）")
    parser.add_argument("--calls", default=None, help="llm_calls.jsonl 路径（prompt_hash 匹配重放）")
    parser.add_argument("--fixtures", default="evals/fixtures.json")
    parser.add_argument("--template", default=None, help="prompt 模板文件或 eval 报告（calls 路径必填其一）")
    parser.add_argument("--threshold", type=float, default=0.6)
    parser.add_argument("--output", default="evals/reports/replay_result.json")
    args = parser.parse_args()

    if not args.report and not args.calls:
        parser.error("需提供 --report（raw_response 重放）或 --calls（事件日志重放）之一")

    cases = load_cases(args.fixtures)
    print(f"[ReplayEval] 加载 {len(cases)} 条 fixtures 用例")

    if args.report:
        result = replay_from_report(args.report, cases, args.threshold)
    else:
        template = args.template
        if not template:
            raise SystemExit("[ReplayEval] --calls 路径需提供 --template（模板文件或 eval 报告）")
        if os.path.exists(template):
            # 先尝试作为 eval 报告读取模板（RunReport.prompt_template）；失败则视为纯模板文本
            try:
                template = load_template_from_report(template)
            except (json.JSONDecodeError, KeyError, StopIteration):
                with open(template, "r", encoding="utf-8") as f:
                    template = f.read()
        result = replay_from_calls(args.calls, cases, template, args.threshold)

    # 落盘
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[ReplayEval] 报告已保存: {args.output}")

    # 摘要
    agg = result["aggregate"]
    print(f"\n{'=' * 60}")
    print(f"[Summary] 来源: {result['source']}")
    print(f"[Summary] 评分用例: {result['scored_cases']}/{result['total_cases']}")
    if "skipped" in result:
        print(f"[Summary] skipped（prompt_hash 不匹配/失败调用）: {result['skipped']}")
    print(f"[Summary] pass_rate: {agg['pass_rate']:.2%}")
    print(f"[Summary] avg_score: {agg['avg_overall_score']:.4f}")
    consistent = [c for c in result["consistency_check"] if c.get("consistent") is True]
    inconsistent = [c for c in result["consistency_check"] if c.get("consistent") is False]
    print(f"[Summary] 一致性校验: 一致 {len(consistent)} / 不一致 {len(inconsistent)}")


if __name__ == "__main__":
    main()
