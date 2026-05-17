FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first for layer caching
COPY pyproject.toml .

# Install dependencies into the system Python (no venv needed in container)
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy application code
COPY main.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
