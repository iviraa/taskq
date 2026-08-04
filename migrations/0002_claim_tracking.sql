-- claim_count is set inside the claim; after a drained run every row should be 1

alter table tasks add column if not exists claim_count integer not null default 0;
alter table tasks add column if not exists claimed_by  text;
