FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]" 2>/dev/null || pip install --no-cache-dir -e .

# Copy source
COPY src/ ./src/
COPY alarms.yml ./

# Data dir for SQLite
RUN mkdir -p /var/lib/berkeley

ENV PYTHONUNBUFFERED=1
EXPOSE 8084

CMD ["alarm-service"]
