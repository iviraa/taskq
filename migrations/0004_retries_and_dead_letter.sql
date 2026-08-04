-- retries with backoff and a dead-letter queue; claim_count becomes attempts

-- no rename column if exists in Postgres
do $$
begin
    if exists (select 1 from information_schema.columns
               where table_name = 'tasks' and column_name = 'claim_count') then
        alter table tasks rename column claim_count to attempts;
    end if;
end $$;

alter table tasks add column if not exists max_attempts integer not null default 3;

-- 'dead' is terminal; 'failed' is gone
alter table tasks drop constraint if exists tasks_state_check;
update tasks set state = 'dead' where state = 'failed';
alter table tasks add constraint tasks_state_check
    check (state in ('pending', 'running', 'succeeded', 'dead'));

-- exhausted tasks are no longer claim candidates
drop index if exists tasks_claimable_idx;

create index if not exists tasks_claimable_idx
    on tasks (visible_at, id) include (attempts, max_attempts)
    where state in ('pending', 'running');

create index if not exists tasks_dead_idx on tasks (id) where state = 'dead';
