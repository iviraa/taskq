-- the queue table

create table if not exists tasks (
    id          bigserial   primary key,
    kind        text        not null,
    payload     jsonb       not null default '{}'::jsonb,
    state       text        not null default 'pending',
    created_at  timestamptz not null default now(),
    claimed_at  timestamptz,
    finished_at timestamptz,
    last_error  text,

    constraint tasks_state_check
        check (state in ('pending', 'running', 'succeeded', 'failed'))
);

-- partial index: only pending rows are scanned by the claim
create index if not exists tasks_pending_idx on tasks (id) where state = 'pending';
