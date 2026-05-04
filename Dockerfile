# syntax=docker/dockerfile:1.7

# ---------- Stage 1: builder ----------
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt


# ---------- Stage 2: runtime ----------
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    HOME=/home/appuser \
    HOST=0.0.0.0 \
    PORT=8000 \
    WORKERS=4

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1001 appuser \
    && useradd --system --uid 1001 --gid appuser --create-home --home-dir /home/appuser --shell /bin/bash appuser \
    && chown -R appuser:appuser /home/appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=appuser:appuser serve.py start.sh ./
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser schemas/ ./schemas/

RUN chmod +x start.sh

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/docs > /dev/null || exit 1

CMD ["./start.sh"]
