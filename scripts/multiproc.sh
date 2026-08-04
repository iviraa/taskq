#!/usr/bin/env bash
# Drain the queue with several worker processes and audit the claims.
#   ./scripts/multiproc.sh [tasks] [processes] [goroutines-per-process]
set -euo pipefail

tasks="${1:-20000}"
procs="${2:-4}"
per="${3:-4}"
db="${DB:-taskq}"
root="$(cd "$(dirname "$0")/.." && pwd)"

go build -o "$root/bin/taskq" "$root/cmd/taskq"

"$root/bin/taskq" produce -reset -kind sleep -n "$tasks" -ms 0 >/dev/null 2>&1
echo "enqueued $tasks tasks, starting $procs processes x $per goroutines"

start=$(python3 -c 'import time; print(time.time())')
pids=()
for i in $(seq 1 "$procs"); do
    "$root/bin/taskq" work -id "proc$i" -n "$per" -drain -poll 10ms >/dev/null 2>&1 &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done
end=$(python3 -c 'import time; print(time.time())')

python3 -c "print(f'drained in {$end-$start:.3f}s, {$tasks/($end-$start):.0f} tasks/sec')"

echo
echo "tasks per process:"
psql -qd "$db" -c "
    select split_part(claimed_by, '#', 1) as process,
           count(*) as tasks,
           count(distinct claimed_by) as goroutines
    from tasks group by 1 order by 1"

echo "audit:"
psql -qd "$db" -c "
    select count(*) as total,
           count(*) filter (where state = 'succeeded')      as succeeded,
           count(*) filter (where state in ('pending','running')) as unfinished,
           count(*) filter (where claim_count = 0)          as never_claimed,
           count(*) filter (where claim_count > 1)          as double_claimed
    from tasks"
