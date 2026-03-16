# =============================================================================
# DVF+ Analytics — PostgreSQL 16 + PostGIS 3 (ephemeral ingestion database)
# =============================================================================
# This image extends the official PostgreSQL 16 image with PostGIS support.
# PostGIS is required because the DVF+ SQL dump from Cerema contains geometry
# columns (point coordinates for each property transaction).
#
# This container is ephemeral: it is started during ingestion, used to restore
# the SQL dump and export tables to CSV, then destroyed. No persistent volume
# is attached. See docker-compose.yml for orchestration details.
# =============================================================================

# Base image: PostgreSQL 16 on Debian Bookworm (slim)
FROM postgres:16-bookworm

# Install PostGIS 3 extension packages.
# - postgis:                  command-line tools and shared libraries
# - postgresql-16-postgis-3:  PostgreSQL extension module (CREATE EXTENSION postgis)
# --no-install-recommends:    skip optional dependencies to keep the image small
# rm -rf /var/lib/apt/lists:  clean up apt cache to reduce layer size
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       postgis \
       postgresql-16-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

# Bake the bulk-load-optimized postgresql.conf into the image.
# This avoids bind mounts, which fail when Kestra manages this container
# via Docker-from-Docker (the Docker daemon resolves paths from the host,
# not from inside the Kestra container).
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Auto-enable PostGIS when the database is created.
# The official postgres image runs all .sql files in /docker-entrypoint-initdb.d/
# on first startup (when the data directory is empty). The "01_" prefix ensures
# this runs before any other init scripts.
RUN echo "CREATE EXTENSION IF NOT EXISTS postgis;" > /docker-entrypoint-initdb.d/01_postgis.sql
