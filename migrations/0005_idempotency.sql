-- idempotency on both sides: delivery is at least once, so duplicates must be harmless

-- producer side: one task per key; NULL keys stay distinct
alter table tasks add column if not exists idempotency_key text;

create unique index if not exists tasks_idempotency_key_idx
    on tasks (idempotency_key);

-- consumer side: keyed on the business id, not the task id
create table if not exists documents (
    doc_id      bigint      primary key,
    embedding   bytea       not null,
    embedded_at timestamptz not null default now(),
    task_id     bigint      not null,
    attempt     integer     not null
);

-- control table: every delivery appends, so duplicates can be counted
create table if not exists documents_naive (
    seq     bigserial primary key,
    doc_id  bigint    not null,
    task_id bigint    not null,
    attempt integer   not null
);
