FROM python:3.10-slim as base

ENV WORKDIR /backend
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

FROM base as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    git \
    && curl -sSL https://install.python-poetry.org | python - \
    && apt-get purge -y --auto-remove curl build-essential git \
    && rm -rf /var/lib/apt/lists/*

COPY poetry.lock pyproject.toml ./

RUN python -m venv $VIRTUAL_ENV && \
    poetry install --no-dev --no-interaction --no-ansi

FROM python:3.10-slim as prod

ENV WORKDIR /backend
WORKDIR $WORKDIR

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=on \
    VIRTUAL_ENV="/venv" \
    PATH="/venv/bin:$PATH" \
    PYTHONPATH=$WORKDIR

COPY --from=builder /venv /venv
COPY ./app ./app

EXPOSE 8085

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8085"]