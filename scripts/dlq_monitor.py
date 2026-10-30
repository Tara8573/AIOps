"""死信巡检（NFR 4.4：死信文件监控告警）。

检查文件级死信（data/dead_letters，pipeline 崩溃兜底）与死信指标，积压超阈值告警退出码。
用法:
    python scripts/dlq_monitor.py [--threshold 100]
（Kafka 死信 topic aiops-alerts-dlq 的监控由 Prometheus/Grafana 侧配置，见 FR-6）
"""

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from core.observability import metrics

DLQ_DIR = os.getenv("AIOPS_DLQ_DIR", "data/dead_letters")


def main():
    parser = argparse.ArgumentParser(description="死信巡检")
    parser.add_argument("--threshold", type=int, default=100, help="积压告警阈值")
    args = parser.parse_args()

    dlq_path = Path(DLQ_DIR)
    if not dlq_path.exists():
        print(f"[DLQMonitor] ✓ 死信目录不存在（无死信）: {DLQ_DIR}")
        return

    files = sorted(dlq_path.glob("*.json"))
    now = time.time()
    recent = [f for f in files if (now - f.stat().st_mtime) < 3600]  # 最近 1h

    print(f"[DLQMonitor] 死信总数={len(files)} | 最近 1h={len(recent)} | dir={DLQ_DIR}")

    # 死信指标（进程内计数，Prometheus 侧亦可观察）
    written = metrics.snapshot().get("counters", {}).get("pipeline.dlq.written", 0)
    print(f"[DLQMonitor] 本进程累计写入={written}")

    if len(files) > args.threshold or len(recent) > 10:
        print(f"[DLQMonitor] ✗ 死信积压告警（阈值 {args.threshold}）——请排查 pipeline 错误日志")
        sys.exit(1)

    print(f"[DLQMonitor] ✓ 死信处于正常水位")


if __name__ == "__main__":
    main()
