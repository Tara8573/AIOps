"""审计 hash chain 全链校验（FR-2.3）。

从 PG 读取全部审计事件按时间序逐条校验：
  1. 链连续性：每条事件的 prev_hash == 前一条的 hash（首条 prev_hash=NULL）
  2. 防篡改：按写入路径同款公式重算 hash 并与存储值比对

用法:
    python scripts/verify_audit_chain.py [--from <event_id>] [--to <event_id>]
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:  # GBK 控制台兼容（✓/✗ 输出）
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from core.config import load_settings
from core.logger import LogConfig


def _recompute_hash(row) -> str:
    """按写入路径同款公式重算事件 hash。

    时间戳转本地朴素时间（与写入时 datetime.now() 的 naive 形态对齐）；
    payload 按写入时 json.dumps 形态。
    """
    ts = row[1]
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            ts = ts.astimezone().replace(tzinfo=None)
        ts_str = ts.isoformat()
    else:
        ts_str = str(ts)

    payload = row[8] if isinstance(row[8], dict) else json.loads(row[8] or "{}")
    event_data = {
        "event_id": str(row[0]),
        "timestamp": ts_str,
        "event_type": row[2],
        "actor": row[3],
        "alert_id": row[4],
        "pipeline_run_id": row[5],
        "category": row[6],
        "severity": row[7],
        "payload": payload,
        "outcome": row[9],
    }
    content = json.dumps(event_data, ensure_ascii=False, default=str)
    prev_hash = row[11] or ""
    return hashlib.sha256((content + prev_hash).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description="审计 hash chain 全链校验")
    parser.add_argument("--from", dest="from_id", default=None, help="起始 event_id（可选）")
    parser.add_argument("--to", dest="to_id", default=None, help="结束 event_id（可选）")
    args = parser.parse_args()

    settings = load_settings()
    LogConfig.setup(
        log_dir=settings.get("runtime.log_dir", "./logs"),
        log_level=settings.get("runtime.log_level", "INFO"),
    )

    from plugins.audit_pg_store import PgAuditStore
    store = PgAuditStore()
    pool = store._get_pool()
    with pool.connection() as conn:
        rows = conn.execute(
            f"SELECT event_id, timestamp, event_type, actor, alert_id, pipeline_run_id, "
            f"category, severity, payload, outcome, event_hash, prev_hash "
            f"FROM {store._table} ORDER BY timestamp ASC"
        ).fetchall()
    store.close()

    if args.from_id:
        rows = [r for r in rows if str(r[0]) >= args.from_id]
    if args.to_id:
        rows = [r for r in rows if str(r[0]) <= args.to_id]

    if not rows:
        print("[VerifyChain] 无审计事件可校验")
        return

    prev_hash = None
    verified = 0
    broken_at = None
    for row in rows:
        stored_prev = row[11]
        if (stored_prev or None) != prev_hash:
            broken_at = (row[0], f"prev_hash 链接断裂（期望 {prev_hash}，实际 {stored_prev}）")
            break
        recomputed = _recompute_hash(row)
        if recomputed != row[10]:
            broken_at = (row[0], "hash 重算不匹配（内容被篡改）")
            break
        verified += 1
        prev_hash = row[10]

    if broken_at is not None:
        print(f"[VerifyChain] ✗ 链校验失败 | 总事件={len(rows)} | 已验证={verified}")
        print(f"  断裂于 event_id={broken_at[0]}: {broken_at[1]}")
        sys.exit(1)

    print(f"[VerifyChain] ✓ 全链校验通过 | 事件数={len(rows)}")


if __name__ == "__main__":
    main()
