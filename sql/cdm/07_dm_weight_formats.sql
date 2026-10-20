-- 07_dm_weight_formats.sql
-- Популярность форматов весов моделей

create table if not exists cdm.dm_weight_formats (
    weight_format   text,
    org_stratum     text,
    models_count    bigint,
    total_downloads bigint,
    loaded_at       timestamptz default now()
);

truncate cdm.dm_weight_formats;

with last_snapshot as (
    select max(business_date) as d from dds.fct_model_snapshot
),
exploded as (
    select
        d.hf_id,
        f.downloads,
        split_part(d.repo_id, '/', 1) as org,
        fmt
    from dds.dim_model d
    left join dds.fct_model_snapshot f
        on f.hf_id = d.hf_id
       and f.business_date = (select d from last_snapshot)
    cross join lateral unnest(coalesce(d.weight_formats, array[]::text[])) as fmt
    where d.is_current
)
insert into cdm.dm_weight_formats
    (weight_format, org_stratum, models_count, total_downloads)
select
    e.fmt                                             as weight_format,
    coalesce(o.stratum, 'unknown')                    as org_stratum,
    count(distinct e.hf_id)                           as models_count,
    coalesce(sum(e.downloads), 0)                     as total_downloads
from exploded e
left join dds.dim_org o on o.org = e.org
group by 1, 2
order by models_count desc;