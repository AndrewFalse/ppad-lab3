-- 05_dm_daily_metrics.sql
-- Ежедневные агрегаты: модели, downloads, likes, закрытые

create table if not exists cdm.dm_daily_metrics (
    business_date     date,
    models_count      bigint,
    total_downloads   bigint,
    total_likes       bigint,
    gated_models      bigint,
    private_models    bigint,
    loaded_at         timestamptz default now()
);

truncate cdm.dm_daily_metrics;

insert into cdm.dm_daily_metrics
    (business_date, models_count, total_downloads, total_likes,
     gated_models, private_models)
select
    business_date,
    count(*)                                          as models_count,
    coalesce(sum(downloads), 0)                       as total_downloads,
    coalesce(sum(likes), 0)                           as total_likes,
    count(*) filter (where gated <> 'false')          as gated_models,
    count(*) filter (where private)                   as private_models
from dds.fct_model_snapshot
group by business_date
order by business_date;