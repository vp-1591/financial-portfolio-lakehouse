# Stage 1: builder — install dependencies
FROM python:3.11-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml .
# Create a minimal package so pip can resolve the install.
# The real source is copied in the runtime stage.
RUN mkdir -p pipeline && touch pipeline/__init__.py pipeline/run.py
RUN pip install --no-cache-dir ".[pipeline]"

# Stage 2: runtime
FROM python:3.11-slim-bookworm
RUN useradd -m -u 1000 pipeline
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY pipeline/ pipeline/
COPY pyproject.toml .
RUN mkdir -p /app/data /app/.secrets && chown -R pipeline:pipeline /app/data /app/.secrets
ENV PYTHONPATH=/app
USER pipeline
ENTRYPOINT ["python", "-m", "pipeline.run"]
CMD ["--help"]