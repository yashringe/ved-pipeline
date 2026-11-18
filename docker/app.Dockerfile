# Image for every Python service except Spark: API, simulator, loader, DQ report, dashboard.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

RUN groupadd --system app \
    && useradd --system --gid app --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[api,sim,load,dashboard]"

COPY sql ./sql
COPY dashboard ./dashboard

# Named volumes inherit the ownership of the mount point on first use.
RUN mkdir -p /data/ved && chown app:app /data/ved
ENV SQL_DIR=/app/sql \
    VED_DATA_DIR=/data/ved

USER app
EXPOSE 8000 8501
