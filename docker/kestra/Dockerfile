# =============================================================================
# DVF+ Analytics — Custom Kestra image (Python + Docker CLI + psql)
# =============================================================================
# Extends the official Kestra image with the tools needed to run pipeline tasks:
#   - Python 3 + uv: run ingestion scripts and dbt
#   - Docker CLI + Compose plugin: manage the ephemeral PostgreSQL container
#   - psql: restore SQL dumps into PostgreSQL (used by ingestion/restore_dump.py)
#
# Build context: ./docker/kestra (from docker-compose.yml)
# =============================================================================

FROM kestra/kestra:v0.21.1

USER root

# Install Python 3, PostgreSQL client (psql), and curl for health checks
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 python3-pip python3-venv \
       postgresql-client \
       curl \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI (static binary — no distro-specific repo needed)
RUN ARCH=$(uname -m) \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${ARCH}/docker-26.1.4.tgz" \
       | tar xz -C /usr/local/bin --strip-components=1 docker/docker

# Install Docker Compose plugin
RUN ARCH=$(uname -m) \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/v2.29.1/docker-compose-linux-${ARCH}" \
       -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Install uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Working directory for the mounted project
WORKDIR /app/project
