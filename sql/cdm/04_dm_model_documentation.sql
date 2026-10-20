-- 04_dm_model_documentation.sql
-- Документированность моделей: README, LICENSE, оба

create table if not exists cdm.dm_model_documentation (
    pipeline_tag          text,
    org_stratum           text,
    models_count          bigint,
    with_readme           bigint,
    with_license_file     bigint,
    with_both             bigint,
    readme_share          numeric(5, 4),
    license_file_share    numeric(5, 4),
    loaded_at             timestamptz default now()
);

truncate cdm.dm_model_documentation;

insert into cdm.dm_model_documentation
    (pipeline_tag, org_stratum, models_count,
     with_readme, with_license_file, with_both,
     readme_share, license_file_share)
select
    coalesce(nullif(d.pipeline_tag, ''), 'unknown')  as pipeline_tag,
    coalesce(o.stratum, 'unknown')                    as org_stratum,
    count(*)                                          as models_count,
    count(*) filter (where d.has_readme)              as with_readme,
    count(*) filter (where d.has_license_file)        as with_license_file,
    count(*) filter (where d.has_readme and d.has_license_file)
                                                      as with_both,
    case when count(*) = 0 then 0
         else round(count(*) filter (where d.has_readme)::numeric / count(*), 4)
    end                                               as readme_share,
    case when count(*) = 0 then 0
         else round(count(*) filter (where d.has_license_file)::numeric / count(*), 4)
    end                                               as license_file_share
from dds.dim_model d
left join dds.dim_org o
    on o.org = split_part(d.repo_id, '/', 1)
where d.is_current
group by 1, 2
order by models_count desc;