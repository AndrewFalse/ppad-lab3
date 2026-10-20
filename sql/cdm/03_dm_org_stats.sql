-- 03_dm_org_stats.sql
-- Сводка по организациям: модели, популярность, открытость, документированность

create table if not exists cdm.dm_org_stats (
    org                  text,
    stratum              text,
    models_count         bigint,
    total_downloads      bigint,
    total_likes          bigint,
    gated_models         bigint,
    documented_models    bigint,
    documented_share     numeric(5, 4),
    loaded_at            timestamptz default now()
);

truncate cdm.dm_org_stats;

insert into cdm.dm_org_stats
    (org, stratum, models_count, total_downloads, total_likes,
     gated_models, documented_models, documented_share)
with last_snapshot as (
    select max(business_date) as d from dds.fct_model_snapshot
),
current_models as (
    select
        split_part(repo_id, '/', 1)                  as org,
        hf_id,
        gated,
        (coalesce(has_readme, false) or coalesce(has_license_file, false))
                                                      as is_documented
    from dds.dim_model
    where is_current
)
select
    cm.org                                            as org,
    coalesce(o.stratum, 'unknown')                    as stratum,
    count(distinct cm.hf_id)                          as models_count,
    coalesce(sum(f.downloads), 0)                     as total_downloads,
    coalesce(sum(f.likes), 0)                         as total_likes,
    count(distinct case when cm.gated <> 'false' then cm.hf_id end)
                                                      as gated_models,
    count(distinct case when cm.is_documented then cm.hf_id end)
                                                      as documented_models,
    case
        when count(distinct cm.hf_id) = 0 then 0
        else round(
            count(distinct case when cm.is_documented then cm.hf_id end)::numeric
            / count(distinct cm.hf_id),
            4
        )
    end                                               as documented_share
from current_models cm
left join dds.dim_org o on o.org = cm.org
left join dds.fct_model_snapshot f
    on f.hf_id = cm.hf_id
   and f.business_date = (select d from last_snapshot)
group by cm.org, o.stratum
order by models_count desc;