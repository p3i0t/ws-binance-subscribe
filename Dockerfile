FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock .
# Copy application code (needed for package install)
COPY binance_live_ingestor/ ./binance_live_ingestor/

# Install the project (deps + console script entry point)
RUN uv pip install --system --no-cache .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["binance-live-ingestor"]
CMD []
