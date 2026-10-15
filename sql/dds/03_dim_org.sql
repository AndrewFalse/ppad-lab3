-- 03_dim_org.sql
-- Справочник организаций (простой, без SCD2)

CREATE TABLE IF NOT EXISTS dds.dim_org (
    org_sk      serial PRIMARY KEY,
    org         text NOT NULL UNIQUE,
    stratum     text,                   -- labs / ru / quant
    loaded_at   timestamptz DEFAULT now()
);

-- Полная перезагрузка справочника (он маленький и стабильный)
TRUNCATE dds.dim_org;

INSERT INTO dds.dim_org (org, stratum)
SELECT org, stratum
FROM stg.hf_orgs
ORDER BY org;
