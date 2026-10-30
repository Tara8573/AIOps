FROM python:3.11-slim

LABEL maintainer="AIOps Team"
LABEL description="AIOps 智能告警全自动处理流水线"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY sql/ sql/
COPY core/ core/
COPY agents/ agents/
COPY interfaces/ interfaces/
COPY plugins/ plugins/
COPY knowledge/ knowledge/
COPY pipeline.py .
COPY main.py .

RUN mkdir -p logs data/dead_letters data/jira_knowledge \
    && adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import core.config; print('ok')" || exit 1

ENTRYPOINT ["python", "main.py"]
