"""CLI 入口：运行 AIOps ActionPlan 评估改进循环

用法:
    python -m evals.run_eval_loop [选项]

选项:
    --fixtures    评估用例文件路径 (默认: evals/fixtures.json)
    --iterations  最大迭代轮数 (默认: 5)
    --threshold   通过分数阈值 (默认: 0.6)
    --train-ratio 训练集比例 (默认: 0.6)
    --output      报告输出目录 (默认: evals/reports/)
    --template    初始 prompt 模板文件 (可选，默认用内置模板)
    --seed        随机种子 (默认: 42)
"""

import argparse
import json
import os
import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from dotenv import load_dotenv

load_dotenv(Path(_root) / ".env")

from core.eval_models import EvalCase, LoopResult
from core.eval_runner import EvalImproveLoop


def _build_llm():
    """构建 LLMClient，复用环境变量配置"""
    from plugins.openai_client import LLMClientFactory

    return LLMClientFactory.from_env()


def load_cases(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [EvalCase(**case) for case in data["cases"]]


def save_report(result: LoopResult, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "eval_loop_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            result.model_dump(), f, ensure_ascii=False, indent=2, default=str
        )
    print(f"\n[Report] JSON 报告已保存: {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="AIOps ActionPlan 评估改进循环"
    )
    parser.add_argument("--fixtures", default="evals/fixtures.json")
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--threshold", type=float, default=0.6)
    parser.add_argument("--train-ratio", type=float, default=0.6)
    parser.add_argument("--output", default="evals/reports/")
    parser.add_argument("--template", default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    cases = load_cases(args.fixtures)
    print(f"[EvalLoop] 加载 {len(cases)} 条评估用例")

    llm = _build_llm()

    initial_template = None
    if args.template:
        with open(args.template, "r", encoding="utf-8") as f:
            initial_template = f.read()

    loop = EvalImproveLoop(
        llm_client=llm,
        eval_cases=cases,
        max_iterations=args.iterations,
        pass_threshold=args.threshold,
        train_ratio=args.train_ratio,
        seed=args.seed,
    )

    result = loop.run(initial_template=initial_template)
    save_report(result, args.output)

    # 打印摘要
    print(f"\n{'=' * 60}")
    print(f"[Summary] 总迭代轮数: {result.iterations}")
    print(f"[Summary] 最佳轮次: {result.best_iteration}")
    print(f"[Summary] 最佳测试 pass_rate: {result.best_test_pass_rate:.2%}")
    print(f"[Summary] 最佳测试 avg_score: {result.best_test_avg_score:.4f}")
    print(f"[Summary] 改进幅度 (delta): {result.improvement_delta:+.4f}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
