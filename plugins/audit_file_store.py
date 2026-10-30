"""文件审计存储。

审计事件追加写入 JSONL 文件（按天分文件），支持 SHA256 hash chain 校验。
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from core.models import AuditEvent
from interfaces.base import IAuditStore


class FileAuditStore(IAuditStore):
    """JSONL 文件审计存储 + hash chain 防篡改。"""

    def __init__(self, log_dir: str = "./logs/audit"):
        self._dir = Path(log_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def emit(self, event: AuditEvent) -> None:
        day = event.timestamp.strftime("%Y-%m-%d")
        path = self._dir / f"audit_{day}.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            f.write(event.model_dump_json() + "\n")

    def query(
        self,
        event_type: Optional[str] = None,
        alert_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        results: List[AuditEvent] = []

        jsonl_files = sorted(self._dir.glob("audit_*.jsonl"))
        if start_time:
            jsonl_files = [
                f for f in jsonl_files
                if self._file_date(f) >= start_time.date()
            ]
        if end_time:
            jsonl_files = [
                f for f in jsonl_files
                if self._file_date(f) <= end_time.date()
            ]

        for filepath in jsonl_files:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = AuditEvent.model_validate_json(line)
                    except Exception:
                        continue

                    if event_type and not event.event_type.startswith(event_type):
                        continue
                    if alert_id and event.alert_id != alert_id:
                        continue
                    if start_time and event.timestamp < start_time:
                        continue
                    if end_time and event.timestamp > end_time:
                        continue

                    results.append(event)
                    if len(results) >= limit:
                        return results

        return results

    def verify_chain(self, from_id: str, to_id: str) -> bool:
        """验证 from_id 到 to_id 之间的审计链完整性。"""
        events = []
        collecting = False

        for filepath in sorted(self._dir.glob("audit_*.jsonl")):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = AuditEvent.model_validate_json(line)
                    except Exception:
                        return False

                    if event.event_id == from_id:
                        collecting = True
                    if collecting:
                        events.append(event)
                    if event.event_id == to_id:
                        break
            if events and events[-1].event_id == to_id:
                break

        if not events:
            return False
        if events[0].event_id != from_id or events[-1].event_id != to_id:
            return False

        prev_hash = events[0].prev_hash
        for event in events:
            if event.prev_hash != prev_hash:
                return False
            content = event.model_dump_json(exclude={"prev_hash"})
            computed = hashlib.sha256(
                (content + (prev_hash or "")).encode()
            ).hexdigest()
            prev_hash = computed

        return True

    @staticmethod
    def _file_date(filepath: Path):
        """从文件名提取日期：audit_2026-06-25.jsonl → date"""
        name = filepath.stem  # audit_2026-06-25
        date_str = name.replace("audit_", "")
        return datetime.strptime(date_str, "%Y-%m-%d").date()
