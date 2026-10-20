-- 06_dm_license_changes.sql
-- История смены лицензий по моделям (SCD2)

create table if not exists cdm.dm_license_changes (
    hf_id           text,
    repo_id         text,
    org             text,
    changed_on      date,
    old_license     text,
    new_license     text,
    loaded_at       timestamptz default now()
);

truncate cdm.dm_license_changes;

with ordered as (
    select
        hf_id,
        repo_id,
        split_part(repo_id, '/', 1) as org,
        valid_from,
        license,
        lag(license) over (partition by hf_id order by valid_from) as prev_license
    from dds.dim_model
)
insert into cdm.dm_license_changes
    (hf_id, repo_id, org, changed_on, old_license, new_license)
select
    hf_id,
    repo_id,
    org,
    valid_from                                      as changed_on,
    prev_license                                    as old_license,
    license                                         as new_license
from ordered
where prev_license is not null
  and prev_license is distinct from license
order by changed_on desc;