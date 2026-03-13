FROM postgres:16-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       postgis \
       postgresql-16-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

# Auto-enable PostGIS when the database is created
RUN echo "CREATE EXTENSION IF NOT EXISTS postgis;" > /docker-entrypoint-initdb.d/01_postgis.sql
