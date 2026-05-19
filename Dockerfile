FROM python:3.12-slim

WORKDIR /app

# Install only what we need; DuckDB is statically linked, no system libs required.
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

COPY config.yaml ./

# Output dir will be a volume mount at runtime; create as placeholder.
RUN mkdir -p /app/output

ENTRYPOINT ["python", "-m", "taxi_top_trips"]
