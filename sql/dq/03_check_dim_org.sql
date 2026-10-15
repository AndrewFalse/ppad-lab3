-- 03_check_dim_org.sql
-- Проверки справочника организаций

SELECT
    (SELECT count(*) = 20 FROM dds.dim_org)                                     AS has_20_orgs,
    (SELECT count(*) = 0 FROM dds.dim_org WHERE org IS NULL OR org = '')         AS org_not_empty;
