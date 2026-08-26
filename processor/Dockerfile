FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv inside the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/processor

# Copy the processor metadata and its local kafka-lib dependency.
COPY processor/pyproject.toml processor/uv.lock ./
COPY kafka-lib /app/kafka-lib

# Install dependencies from the lockfile.
RUN uv sync --frozen --no-cache

# Copy the processor source.
COPY processor/processor.py ./

# Run the processor.
CMD ["uv", "run", "processor.py"]