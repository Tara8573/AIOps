"""僵死 run 巡检恢复（FR-4 断点续跑）。

扫描 pipeline_runs 中 running 状态但 updated_at 超过阈值的 run
（进程崩溃遗留），逐个从 checkpoint 断点续跑；无 checkpoint 可恢复的
标记 failed 转人工。生产环境以 sidecar/cron 运行，需与 worker 同参装配
（同 AIOPS_PG_DSN，AIOPS_CHECKPOINT_ENABLED=1）。

用法:
    python scripts/resume_stale_runs.py [--dry-run]
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:  # GBK 控制台兼容（✓/✗ 输出）
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from core.config import load_settings
from core.logger import LogConfig, get_logger
from core.pipeline_graph import resume_pipeline_run
from core.pipeline_run_registry import PgPipelineRunRegistry


def main():
    parser = argparse.ArgumentParser(description="僵死 run 巡检恢复（断点续跑）")
    parser.add_argument("--dry-run", action="store_true", help="仅列出僵死 run，不恢复")
    args = parser.parse_args()

    settings = load_settings()
    LogConfig.setup(
        log_dir=settings.get("runtime.log_dir", "./logs"),
        log_level=settings.get("runtime.log_level", "INFO"),
        json_format=settings.get("runtime.log_json", False),
    )
    logger = get_logger("resume_stale")

    threshold = int(os.getenv("AIOPS_CHECKPOINT_STALE_THRESHOLD_SECONDS", "600"))
    # 修复：DSN 收口到 pg_dsn()（AIOPS__PG__DSN > AIOPS_PG_DSN > kb.pg.dsn）
    from core.config import pg_dsn
    registry = PgPipelineRunRegistry(pg_dsn())

    stale = registry.find_stale_runs(threshold)
    if not stale:
        print("[ResumeStale] 无僵死 run")
        return

    print(f"[ResumeStale] 发现 {len(stale)} 个僵死 run (阈值 {threshold}s):")
    for run in stale:
        print(f"  run_id={run.run_id} alert_id={run.alert_id} updated_at={run.updated_at}")

    if args.dry_run:
        print("[ResumeStale] dry-run 模式，不执行恢复")
        return

    # 与 kafka 模式同参装配（含 checkpointer 自动构建）
    from main import build_pipeline
    pipeline = build_pipeline()

    recovered = 0
    for run in stale:
        print(f"[ResumeStale] 恢复 run_id={run.run_id} ...")
        result = resume_pipeline_run(pipeline, run.run_id)
        if result is None:
            # 无 checkpointer 或无 checkpoint 可恢复 → 标记 failed 转人工
            registry.update_status(run.run_id, "failed")
            print(f"  ✗ 无法恢复（无 checkpoint），已标记 failed 转人工")
        else:
            status = (
                "suspended"
                if isinstance(result, dict) and "__interrupt__" in result
                else "completed"
            )
            recovered += 1
            print(f"  ✓ 恢复完成: {status}")

    print(f"[ResumeStale] 巡检完成: 恢复 {recovered}/{len(stale)}")


if __name__ == "__main__":
    main()
