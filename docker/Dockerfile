# =============================================================================
# Web Analytics ETL - Dockerfile
# =============================================================================
# Multi-stage build for PySpark ETL pipeline.
# Based on Python 3.11 with OpenJDK 17 (required by Spark 3.5).
# =============================================================================

FROM python:3.11-slim AS base

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY config/ config/
COPY src/ src/
COPY tests/ tests/
COPY .env.example .env

# Create data directories
RUN mkdir -p data/raw data/bronze data/silver data/gold metadata logs

# Default entrypoint
ENTRYPOINT ["python"]
CMD ["src/run_pipeline.py"]
