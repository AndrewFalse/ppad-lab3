-- 01_dim_license.sql
-- Справочник лицензий для витрин (человекочитаемые подписи)

create table if not exists cdm.dim_license (
    license_id  text primary key,
    label       text,
    loaded_at   timestamptz default now()
);

truncate cdm.dim_license;

insert into cdm.dim_license (license_id, label)
select
    license_id,
    coalesce(nullif(label, ''), license_id)  
from dds.dim_license
where license_id is not null
order by license_id;