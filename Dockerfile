FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system medalloan \
    && useradd --system --gid medalloan --create-home medalloan

COPY requirements-runtime.txt ./
RUN python -m pip install --no-cache-dir -r requirements-runtime.txt

COPY --chown=medalloan:medalloan src ./src
COPY --chown=medalloan:medalloan scripts ./scripts
COPY --chown=medalloan:medalloan migrations ./migrations
COPY --chown=medalloan:medalloan data ./data
COPY --chown=medalloan:medalloan tests ./tests
COPY --chown=medalloan:medalloan pytest.ini README.md ./

USER medalloan

CMD ["python", "scripts/run_pipeline.py", "--source", "/app/data"]
