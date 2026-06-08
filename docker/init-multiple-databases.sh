#!/bin/bash
# Creates additional databases listed in POSTGRES_MULTIPLE_DATABASES (comma-separated).
# The primary database is already created by POSTGRES_DB; this script handles extras.
set -euo pipefail

if [ -z "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
    exit 0
fi

for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' '\n'); do
    db="$(echo "$db" | xargs)"  # trim whitespace
    [ -z "$db" ] && continue
    echo "init-multiple-databases: creating database '$db'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SELECT 'CREATE DATABASE "$db"'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done
