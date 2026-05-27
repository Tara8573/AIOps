import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from loguru import logger
from core.observability import metrics, pipeline_states


class LogConfig:
    """日志配置类"""

    # 默认日志格式
    DEFAULT_FORMAT = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # JSON格式（用于生产环境）
    JSON_FORMAT = (
        '{{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", '
        '"level": "{level}", '
        '"module": "{name}", '
        '"function": "{function}", '
        '"line": {line}, '
        '"message": "{message}}"'
    )

    @staticmethod
    def setup(
        log_dir: str = "./logs",
        log_level: str = "INFO",
        console_output: bool = True,
        file_output: bool = True,
        json_format: bool = False,
        rotation: str = "10 MB",
        retention: str = "7 days",
        compression: str = "zip",
    ):
        """
        配置日志系统

        Args:
            log_dir: 日志目录
            log_level: 日志级别
            console_output: 是否输出到控制台
            file_output: 是否输出到文件
            json_format: 是否使用JSON格式
            rotation: 日志轮转大小
            retention: 日志保留时间
            compression: 日志压缩格式
        """
        # 移除默认处理器
        logger.remove()

        # 选择格式
        log_format = LogConfig.JSON_FORMAT if json_format else LogConfig.DEFAULT_FORMAT

        # 添加控制台输出
        if console_output:
            logger.add(
                sys.stderr, format=log_format, level=log_level, colorize=not json_format
            )

        # 添加文件输出
        if file_output:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)

            # 普通日志文件
            logger.add(
                str(log_path / "aiops_{time:YYYY-MM-DD}.log"),
                format=log_format,
                level=log_level,
                rotation=rotation,
                retention=retention,
                compression=compression,
                encoding="utf-8",
            )

            # 错误日志文件（只记录ERROR及以上级别）
            logger.add(
                str(log_path / "error_{time:YYYY-MM-DD}.log"),
                format=log_format,
                level="ERROR",
                rotation=rotation,
                retention=retention,
                compression=compression,
                encoding="utf-8",
            )

            logger.add(
                str(log_path / "audit_{time:YYYY-MM-DD}.log"),
                format=log_format,
                level="INFO",
                filter=lambda record: record["extra"].get("audit_event", False),
                rotation=rotation,
                retention=retention,
                compression=compression,
                encoding="utf-8",
            )

        return logger


class AIOpsLogger:
    """AIOps专用日志器"""

    def __init__(self, module_name: str):
        self.logger = logger.bind(module=module_name)
        self.module_name = module_name

    def alert_received(self, alert_id: str, title: str, level: str):
        """告警接收日志"""
        self.logger.info(
            "收到新告警 | alert_id={alert_id} | title={title} | level={level}",
            alert_id=alert_id,
            title=title,
            level=level,
        )

    def knowledge_search(self, query: str, result_count: int):
        """知识检索日志"""
        self.logger.info(
            "知识检索完成 | query={query} | 结果数={result_count}",
            query=query,
            result_count=result_count,
        )

    def llm_analysis_start(self, alert_id: str):
        """LLM分析开始日志"""
        self.logger.info("开始LLM分析 | alert_id={alert_id}", alert_id=alert_id)

    def llm_analysis_complete(self, alert_id: str, confidence: float):
        """LLM分析完成日志"""
        self.logger.info(
            "LLM分析完成 | alert_id={alert_id} | confidence={confidence}",
            alert_id=alert_id,
            confidence=confidence,
        )

    def evaluation_result(
        self, alert_id: str, passed: bool, reason: str, risk_level: str
    ):
        """评估结果日志"""
        log_func = self.logger.info if passed else self.logger.warning
        log_func(
            "安全评估完成 | alert_id={alert_id} | passed={passed} | reason={reason} | risk={risk_level}",
            alert_id=alert_id,
            passed=passed,
            reason=reason,
            risk_level=risk_level,
        )

    def execution_start(self, alert_id: str, script_preview: str):
        """脚本执行开始日志"""
        self.logger.info(
            "开始执行脚本 | alert_id={alert_id} | script_preview={script_preview}",
            alert_id=alert_id,
            script_preview=script_preview[:100] + "..."
            if len(script_preview) > 100
            else script_preview,
        )

    def execution_complete(self, alert_id: str, success: bool):
        """脚本执行完成日志"""
        log_func = self.logger.info if success else self.logger.error
        log_func(
            "脚本执行完成 | alert_id={alert_id} | success={success}",
            alert_id=alert_id,
            success=success,
        )

    def ticket_created(self, alert_id: str, ticket_id: str, reason: str):
        """工单创建日志"""
        self.logger.info(
            "创建人工工单 | alert_id={alert_id} | ticket_id={ticket_id} | reason={reason}",
            alert_id=alert_id,
            ticket_id=ticket_id,
            reason=reason,
        )

    def feedback_received(self, alert_id: str, ticket_id: str, is_successful: bool):
        """反馈接收日志"""
        self.logger.info(
            "收到处理反馈 | alert_id={alert_id} | ticket_id={ticket_id} | success={is_successful}",
            alert_id=alert_id,
            ticket_id=ticket_id,
            is_successful=is_successful,
        )

    def knowledge_learned(self, alert_id: str, experience_summary: str):
        """知识沉淀日志"""
        self.logger.info(
            "新知识沉淀 | alert_id={alert_id} | summary={summary}",
            alert_id=alert_id,
            summary=experience_summary[:100] + "..."
            if len(experience_summary) > 100
            else experience_summary,
        )

    def error_occurred(
        self, error_type: str, error_message: str, context: Optional[Dict] = None
    ):
        """错误日志"""
        self.logger.error(
            "发生错误 | type={error_type} | message={error_message} | context={context}",
            error_type=error_type,
            error_message=error_message,
            context=context or {},
        )

    def performance_metric(self, operation: str, duration_ms: float, success: bool):
        """性能指标日志"""
        self.logger.info(
            "性能指标 | operation={operation} | duration={duration_ms}ms | success={success}",
            operation=operation,
            duration_ms=duration_ms,
            success=success,
        )

    def audit_event(self, event_type: str, payload: Dict[str, Any]):
        """审计事件日志"""
        self.logger.bind(audit_event=True).info(
            "审计事件 | type={event_type} | payload={payload}",
            event_type=event_type,
            payload=payload,
        )

    def health_snapshot(self):
        """输出轻量健康快照"""
        self.logger.info(
            "健康快照 | metrics={metrics} | states={states_count}",
            metrics=metrics.snapshot(),
            states_count=len(pipeline_states.snapshot()),
        )


# 预定义的日志器实例
def get_logger(module_name: str) -> AIOpsLogger:
    """获取模块专用的日志器"""
    return AIOpsLogger(module_name)


# 便捷的日志装饰器
def log_execution(logger_instance: Optional[AIOpsLogger] = None):
    """记录函数执行的装饰器"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            import time

            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                duration = (time.time() - start_time) * 1000

                if logger_instance:
                    logger_instance.performance_metric(
                        operation=func.__name__,
                        duration_ms=round(duration, 2),
                        success=True,
                    )

                return result

            except Exception as e:
                duration = (time.time() - start_time) * 1000

                if logger_instance:
                    logger_instance.performance_metric(
                        operation=func.__name__,
                        duration_ms=round(duration, 2),
                        success=False,
                    )
                    logger_instance.error_occurred(
                        error_type=type(e).__name__, error_message=str(e)
                    )

                raise

        return wrapper

    return decorator
