#!/usr/bin/env bash
# SIGKILL some workers mid-run and check every task finishes with one side effect.
#   ./scripts/chaos.sh [tasks] [workers] [kill] [ms]
set -euo pipefail

tasks="${1:-10000}"
workers="${2:-5}"
kills="${3:-3}"
ms="${4:-20}"
linger="${5:-30}"
lease="${LEASE:-5s}"
per="${PER:-4}"
db="${DB:-taskq}"
root="$(cd "$(dirname "$0")/.." && pwd)"

cd "$root"
go build -o bin/taskq ./cmd/taskq

mkdir -p viz
: > "${KILLS:-viz/kills.csv}"
echo "enqueueing $tasks embed tasks"
./bin/taskq produce -reset -kind embed -n "$tasks" -ms "$ms" -linger "$linger" >/dev/null

now() { python3 -c 'import time; print(time.time())'; }

# sample queue state for the plot
"$(dirname "$0")/sample.sh" "${SAMPLE:-viz/chaos.csv}" &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

start=$(now)

pids=()
for i in $(seq 1 "$workers"); do
    ./bin/taskq work -id "w$i" -n "$per" -drain -lease "$lease" -poll 50ms -grace 0s >"/tmp/taskq-w$i.log" 2>&1 &
    pids+=($!)
done
echo "started $workers workers x $per goroutines, $lease lease"

# stagger the kills
sleep 1
for i in $(seq 1 "$kills"); do
    kill -9 "${pids[$((i-1))]}" 2>/dev/null || true
    killed_at=$(python3 -c "print(f'{$(now)-$start:.2f}')")
    echo "  SIGKILL worker w$i at t=${killed_at}s ($(psql -tAd "$db" -c 'select count(*) from documents') docs done)"
    echo "w$i,$killed_at" >> "${KILLS:-viz/kills.csv}"
    sleep 1
done

echo "waiting for the $((workers - kills)) survivors to drain the queue"
for i in $(seq $((kills+1)) "$workers"); do
    wait "${pids[$((i-1))]}" 2>/dev/null || true
done
end=$(now)

echo
python3 -c "print(f'drained {$tasks} tasks in {$end-$start:.2f}s, {$tasks/($end-$start):.0f} tasks/sec')"
echo
./bin/taskq stats

echo
echo "verdict:"
psql -qtA -d "$db" -F' ' -c "
with t as (
    select count(*) as total,
           count(*) filter (where state = 'succeeded')            as succeeded,
           count(*) filter (where state in ('pending','running')) as unfinished,
           count(*) filter (where state = 'dead')                 as dead,
           sum(attempts)                                          as deliveries
    from tasks
), d as (
    select count(*) as documents, count(distinct doc_id) as distinct_docs from documents
), n as (
    select count(*) as delivered_side_effects from documents_naive
)
select
    case when t.succeeded = t.total and t.unfinished = 0 and t.dead = 0
         then '  PASS  no task lost: ' || t.succeeded || '/' || t.total || ' succeeded'
         else '  FAIL  ' || t.unfinished || ' unfinished, ' || t.dead || ' dead' end
from t union all
select
    case when d.documents = d.distinct_docs and d.documents = (select total from t)
         then '  PASS  no duplicate side effect: ' || d.documents || ' documents from '
              || n.delivered_side_effects || ' handler runs'
         else '  FAIL  ' || d.documents || ' documents, ' || d.distinct_docs || ' distinct' end
from d, n union all
select
    case when n.delivered_side_effects > d.documents
         then '  PASS  idempotency was exercised: ' || n.delivered_side_effects
              || ' handler runs produced ' || d.documents || ' documents, '
              || (n.delivered_side_effects - d.documents) || ' duplicate side effects absorbed'
         else '  INCONCLUSIVE  ' || n.delivered_side_effects || ' handler runs for '
              || d.documents || ' documents. No side effect was ever attempted twice, '
              || 'so this run does not test idempotency at all' end
from d, n"
