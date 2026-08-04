#!/usr/bin/env bash
# Samples queue state about 5x/sec into a CSV for viz/animate.py.
#   ./scripts/sample.sh viz/chaos.csv &
set -euo pipefail

out="${1:-viz/chaos.csv}"
db="${DB:-taskq}"
interval="${INTERVAL:-0.2}"

mkdir -p "$(dirname "$out")"
echo "t,worker,done,depth,in_flight,redelivered" > "$out"

now() { python3 -c 'import time; print(time.time())'; }
start=$(now)

# psql -c does not interpolate :variables
while true; do
    t=$(python3 -c "print(f'{$(now)-$start:.3f}')")

    psql -qtA -F',' -d "$db" -c "
        select coalesce(split_part(claimed_by, '#', 1), 'unclaimed'),
               count(*) filter (where state = 'succeeded'),
               (select count(*) from tasks
                where state in ('pending','running') and attempts < max_attempts),
               (select count(*) from tasks
                where state = 'running' and visible_at > now()),
               (select count(*) from tasks where attempts > 1)
        from tasks
        where claimed_by is not null
        group by 1
        order by 1" 2>/dev/null | sed "s/^/$t,/" >> "$out" || true

    sleep "$interval"
done
