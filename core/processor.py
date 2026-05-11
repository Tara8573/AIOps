import re
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, List, Set

from core.models import Alert
from core.logger import get_logger
from interfaces.base import IAlertCleaner, IAlertFilter

logger = get_logger("processor")


class AlertCleaner(IAlertCleaner):
    """默认告警清洗器：将多种格式的原始消息映射为标准 Alert"""

    DEFAULT_LEVEL_MAP = {
        "critical": "Critical",
        "major": "Critical",
        "p1": "Critical",
        "p2": "Warning",
        "warning": "Warning",
        "minor": "Info",
        "info": "Info",
        "p3": "Info",
        "p4": "Info",
    }

    def __init__(
        self,
        field_map: Optional[Dict[str, str]] = None,
        level_map: Optional[Dict[str, str]] = None,
        default_source: str = "kafka",
        dedup_window: int = 300,
    ):
        self.field_map = field_map or {}
        self.level_map = level_map or self.DEFAULT_LEVEL_MAP
        self.default_source = default_source
        self._seen: Dict[str, datetime] = {}
        self._dedup_window = dedup_window

    def clean(self, raw: Dict[str, Any]) -> Optional[Alert]:
        try:
            mapped = self._apply_field_map(raw)
            alert_id = self._extract_alert_id(mapped, raw)
            title = self._extract_title(mapped)
            level = self._normalize_level(mapped)
            content = self._extract_content(mapped)
            timestamp = self._extract_timestamp(mapped)
            source = mapped.get("source", self.default_source)

            if not title:
                logger.logger.warning(
                    "清洗跳过: 缺少 title 字段 | raw={}", str(raw)[:200]
                )
                return None

            alert = Alert(
                alert_id=alert_id,
                title=title,
                level=level,
                content=content,
                timestamp=timestamp,
                source=source,
                raw_data=raw,
            )

            if self._is_duplicate(alert):
                logger.logger.debug("清洗跳过: 重复告警 | alert_id={}", alert.alert_id)
                return None

            return alert

        except Exception as exc:
            logger.logger.error("清洗失败 | error={} | raw={}", exc, str(raw)[:200])
            return None

    def _apply_field_map(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        mapped: Dict[str, Any] = {}
        for target_field, source_path in self.field_map.items():
            value = self._resolve_path(raw, source_path)
            if value is not None:
                mapped[target_field] = value
        for key, value in raw.items():
            if key not in mapped:
                mapped[key] = value
        return mapped

    @staticmethod
    def _resolve_path(data: Dict[str, Any], path: str) -> Any:
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _extract_alert_id(self, mapped: Dict[str, Any], raw: Dict[str, Any]) -> str:
        alert_id = mapped.get("alert_id") or mapped.get("id") or mapped.get("alertId")
        if alert_id:
            return str(alert_id)
        fingerprint = hashlib.md5(
            f"{mapped.get('title', '')}|{mapped.get('content', '')}|{mapped.get('host', '')}".encode()
        ).hexdigest()[:12]
        return f"ALT-{fingerprint.upper()}"

    def _extract_title(self, mapped: Dict[str, Any]) -> str:
        return str(
            mapped.get("title", "")
            or mapped.get("name", "")
            or mapped.get("alert_name", "")
        ).strip()

    def _normalize_level(self, mapped: Dict[str, Any]) -> str:
        raw_level = str(
            mapped.get("level", "")
            or mapped.get("severity", "")
            or mapped.get("priority", "")
        ).strip()
        return self.level_map.get(
            raw_level.lower(), raw_level.title() if raw_level else "Info"
        )

    def _extract_content(self, mapped: Dict[str, Any]) -> str:
        content = (
            mapped.get("content")
            or mapped.get("description")
            or mapped.get("message")
            or mapped.get("body", "")
        )
        if isinstance(content, dict):
            content = str(content)
        return str(content).strip()

    def _extract_timestamp(self, mapped: Dict[str, Any]) -> datetime:
        ts = mapped.get("timestamp") or mapped.get("time") or mapped.get("firedAt")
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    return datetime.strptime(ts, fmt)
                except ValueError:
                    continue
        return datetime.now()

    def _is_duplicate(self, alert: Alert) -> bool:
        now = datetime.now()
        dedup_key = hashlib.md5(f"{alert.title}|{alert.content}".encode()).hexdigest()
        last_seen = self._seen.get(dedup_key)
        if last_seen and (now - last_seen).total_seconds() < self._dedup_window:
            return True
        self._seen[dedup_key] = now
        expired = [
            k
            for k, v in self._seen.items()
            if (now - v).total_seconds() > self._dedup_window * 2
        ]
        for k in expired:
            del self._seen[k]
        return False


class AlertFilter(IAlertFilter):
    """可配置的告警过滤器：支持级别过滤、正则匹配、黑名单等多种规则"""

    def __init__(
        self,
        min_level: str = "Info",
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        exclude_sources: Optional[List[str]] = None,
        exclude_alert_ids: Optional[Set[str]] = None,
        title_exclude_patterns: Optional[List[str]] = None,
    ):
        self.min_level = min_level
        self._level_order = {"Info": 0, "Warning": 1, "Critical": 2}
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.exclude_sources = set(exclude_sources or [])
        self.exclude_alert_ids = exclude_alert_ids or set()
        self.title_exclude_patterns = [
            re.compile(p) for p in (title_exclude_patterns or [])
        ]

    def should_process(self, alert: Alert) -> bool:
        if alert.alert_id in self.exclude_alert_ids:
            logger.logger.debug("过滤拦截: 黑名单 alert_id={}", alert.alert_id)
            return False

        if alert.source in self.exclude_sources:
            logger.logger.debug("过滤拦截: 排除来源 source={}", alert.source)
            return False

        alert_level = self._level_order.get(alert.level, 0)
        min_level = self._level_order.get(self.min_level, 0)
        if alert_level < min_level:
            logger.logger.debug("过滤拦截: 级别低于阈值 level={}", alert.level)
            return False

        for pattern in self.title_exclude_patterns:
            if pattern.search(alert.title):
                logger.logger.debug(
                    "过滤拦截: 标题匹配排除规则 pattern={}", pattern.pattern
                )
                return False

        if self.include_patterns:
            text = f"{alert.title} {alert.content}"
            if not any(p.search(text) for p in self.include_patterns):
                logger.logger.debug("过滤拦截: 未命中包含规则")
                return False

        for pattern in self.exclude_patterns:
            text = f"{alert.title} {alert.content}"
            if pattern.search(text):
                logger.logger.debug(
                    "过滤拦截: 命中排除规则 pattern={}", pattern.pattern
                )
                return False

        return True
