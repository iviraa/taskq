#!/usr/bin/env bash
# Applies each migration once, in order, tracked in schema_migrations.
set -euo pipefail

db="${1:-taskq}"
dir="$(cd "$(dirname "$0")/../migrations" && pwd)"

psql -qd "$db" -v ON_ERROR_STOP=1 -c "
    create table if not exists schema_migrations (
        version    text primary key,
        applied_at timestamptz not null default now()
    )"

for f in "$dir"/*.sql; do
    v="$(basename "$f")"
    if [ -n "$(psql -tAd "$db" -c "select 1 from schema_migrations where version = '$v'")" ]; then
        continue
    fi
    echo "applying $v to $db"
    # migration and its ledger row in one transaction
    psql -qd "$db" -v ON_ERROR_STOP=1 --single-transaction \
        -f "$f" \
        -c "insert into schema_migrations (version) values ('$v')"
done

echo "$db is at $(psql -tAd "$db" -c "select count(*) from schema_migrations") migrations"
