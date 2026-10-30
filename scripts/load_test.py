"""压测注入工具（NFR 4.1 性能验收）。

向 Kafka 注入告警消息，支持稳态速率与风暴模式（同根因大批量）。
用法:
    python scripts/load_test.py --rate 2 --duration 60            # 稳态 2 条/秒 × 60s
    python scripts/load_test.py --storm 1000 --burst-seconds 300  # 风暴：1000 条同根因
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from core.config import load_settings


def build_message(i: int, storm: bool = False, host: str = "10.1.2.3") -> dict:
    """构造契约合规的告警消息（docs/告警消息schema契约.md v1.0）。

    契约要点：扁平 canonical、title/level/timestamp/alert_id 必填、
    content 含主机区分信息、ISO 8601 UTC、分区键 host。
    """
    if storm:
        # 同根因：同签名（意图+主机+实体），数字变化（契约允许）
        title = f"磁盘 /var 使用率超过 {90 + i % 10}%"
        content = f"host {host} /var usage {90 + i % 10}%, inode 88%"
        alert_id = f"ALT-STORM-{i:06d}"
    else:
        title = f"主机 {host} 服务延迟告警 #{i}"
        content = f"host {host} p99 latency > {500 + i}ms"
        alert_id = f"ALT-LOAD-{int(time.time())}-{i:06d}"
    return {
        "schema_version": 1,
        "alert_id": alert_id,
        "title": title,
        "level": "critical" if storm else "warning",
        "content": content,
        "host": host,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "load-test",
    }


def main():
    parser = argparse.ArgumentParser(description="告警压测注入")
    parser.add_argument("--rate", type=float, default=2.0, help="稳态速率（条/秒）")
    parser.add_argument("--duration", type=int, default=60, help="稳态持续时间（秒）")
    parser.add_argument("--storm", type=int, default=0, help="风暴模式：注入条数（同根因）")
    parser.add_argument("--hosts", type=int, default=1, help="主机数（风暴按主机分流）")
    args = parser.parse_args()

    from plugins.kafka_producer import KafkaMessageProducer
    settings = load_settings()
    producer = KafkaMessageProducer(
        bootstrap_servers=os.getenv("AIOPS__KAFKA__BOOTSTRAP_SERVERS",
                                    settings.get("kafka.bootstrap_servers", "localhost:9092")),
        security_protocol=settings.get("kafka.security_protocol") or None,
        sasl_mechanism=settings.get("kafka.sasl_mechanism") or None,
        sasl_username=settings.get("kafka.sasl_username") or None,
        sasl_password=settings.get("kafka.sasl_password") or None,
    )

    sent = 0
    if args.storm > 0:
        print(f"[LoadTest] 风暴注入 {args.storm} 条（同根因，{args.hosts} 主机）...")
        t0 = time.time()
        for i in range(args.storm):
            host = f"10.1.{i % args.hosts}.3"
            msg = build_message(i, storm=True, host=host)
            producer.publish(topic="aiops-alerts", key=host, value=msg)
            sent += 1
        print(f"[LoadTest] 注入完成: {sent} 条，耗时 {time.time() - t0:.1f}s")
    else:
        print(f"[LoadTest] 稳态注入 {args.rate} 条/秒 × {args.duration}s ...")
        t0 = time.time()
        i = 0
        while time.time() - t0 < args.duration:
            msg = build_message(i, storm=False, host=f"10.1.2.{i % 3 + 1}")
            producer.publish(topic="aiops-alerts", key=msg["host"], value=msg)
            sent += 1
            i += 1
            time.sleep(1.0 / args.rate)
        print(f"[LoadTest] 注入完成: {sent} 条，耗时 {time.time() - t0:.1f}s")

    producer.close()


if __name__ == "__main__":
    main()
