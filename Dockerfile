FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY config ./config
COPY services ./services
COPY scripts ./scripts

RUN pip install --no-cache-dir -e ".[ml]"

CMD ["python", "-m", "services.gtfs_matching.matcher"]