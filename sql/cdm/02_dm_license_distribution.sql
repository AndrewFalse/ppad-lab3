-- 02_dm_license_distribution.sql
-- Распределение текущих моделей по лицензиям и стратам

create table if not exists cdm.dm_license_distribution (
    license         text,
    org_stratum     text,
    models_count    bigint,
    total_downloads bigint,
    total_likes     bigint,
    loaded_at       timestamptz default now()
);

truncate cdm.dm_license_distribution;

insert into cdm.dm_license_distribution
    (license, org_stratum, models_count, total_downloads, total_likes)
with last_snapshot as (
    select max(business_date) as d from dds.fct_model_snapshot
)
select
    coalesce(nullif(d.license, ''), 'unknown')       as license,
    coalesce(o.stratum, 'unknown')                   as org_stratum,
    count(distinct d.hf_id)                          as models_count,
    coalesce(sum(f.downloads), 0)                    as total_downloads,
    coalesce(sum(f.likes), 0)                        as total_likes
from dds.dim_model d
left join dds.dim_org o
    on o.org = split_part(d.repo_id, '/', 1)
left join dds.fct_model_snapshot f
    on f.hf_id = d.hf_id
   and f.business_date = (select d from last_snapshot)
where d.is_current
group by 1, 2
order by models_count desc;