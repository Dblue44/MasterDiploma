FROM python:3.12-slim AS base

ENV WORKDIR=/backend
WORKDIR $WORKDIR

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=90 \
    POETRY_VERSION=1.8.4 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV="/venv" \
    PATH="/opt/poetry/bin:/venv/bin:$PATH" \
    PYTHONPATH=$WORKDIR

FROM base AS builder

RUN apt-get update\
    && rm -rf /var/lib/apt/lists \
    && apt-get install -y --no-install-recommends  \
    ca-certificates \
    netbase \
    tzdata \
    curl \
    build-essential \
    git \
    && curl -sSL https://install.python-poetry.org | python - \
    && apt-get purge -y --auto-remove curl build-essential git \
    && apt-get dist-clean \

COPY poetry.lock pyproject.toml ./

RUN python -m venv $VIRTUAL_ENV && \
    poetry install --no-dev --no-interaction --no-ansi --with api

FROM python:3.12-slim AS prod

RUN groupadd -r appuser && useradd -r -g appuser appuser

ENV WORKDIR=/backend
WORKDIR $WORKDIR

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=on \
    VIRTUAL_ENV="/venv" \
    PATH="/venv/bin:$PATH" \
    PYTHONPATH=$WORKDIR

COPY --from=builder /venv /venv
COPY ./app ./app

RUN chown -R appuser:appuser $WORKDIR /venv

USER appuser

EXPOSE 8085

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8085/health', timeout=5)" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8085"]