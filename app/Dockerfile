FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv inside the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app/app

# Copy the app metadata and its local kafka-lib dependency.
COPY app/pyproject.toml app/uv.lock ./
COPY kafka-lib /app/kafka-lib

# Install dependencies from the lockfile.
RUN uv sync --frozen --no-cache

# Copy only the Python package. The repository also has a top-level app.py,
# which would shadow the app/ package when running `python -m app.app`.
COPY app/app ./app

# 4. Expose port 
EXPOSE 5000

# 5. Run the app
CMD ["uv", "run", "python", "-m", "app.app"]