-- visibility timeout: visible_at is when a row becomes claimable, so expired leases are reclaimed by the claim itself

alter table tasks add column if not exists visible_at  timestamptz not null default now();

-- regenerated on every claim to fence stale workers
alter table tasks add column if not exists lease_token uuid;

-- claim now orders by visible_at, oldest claimable first
drop index if exists tasks_pending_idx;

create index if not exists tasks_claimable_idx
    on tasks (visible_at, id)
    where state in ('pending', 'running');
