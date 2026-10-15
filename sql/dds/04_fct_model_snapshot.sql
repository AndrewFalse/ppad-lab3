-- 04_fct_model_snapshot.sql
-- Ежедневные факты по моделям (downloads, likes и т.д.)
-- Эти поля меняются каждый день → не версионируем, просто храним снимки

CREATE TABLE IF NOT EXISTS dds.fct_model_snapshot (
    snapshot_sk     bigserial PRIMARY KEY,
    business_date   date NOT NULL,
    hf_id           text NOT NULL,
    org             text,
    downloads       bigint,
    likes           integer,
    gated           text,
    private         boolean,
    disabled        boolean,
    load_id         text,
    loaded_at       timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS fct_model_snapshot_date_hf
    ON dds.fct_model_snapshot (business_date, hf_id);

CREATE INDEX IF NOT EXISTS fct_model_snapshot_hf
    ON dds.fct_model_snapshot (hf_id);

CREATE INDEX IF NOT EXISTS fct_model_snapshot_date
    ON dds.fct_model_snapshot (business_date);


-- Идемпотентная загрузка: удаляем данные за даты, которые есть в источнике, и вставляем заново
DELETE FROM dds.fct_model_snapshot
WHERE business_date IN (SELECT DISTINCT business_date FROM stg.hf_models);

INSERT INTO dds.fct_model_snapshot (
    business_date,
    hf_id,
    org,
    downloads,
    likes,
    gated,
    private,
    disabled
)
SELECT
    m.business_date,
    m.payload->>'_id'                               AS hf_id,
    m.org,
    (m.payload->>'downloads')::bigint               AS downloads,
    (m.payload->>'likes')::integer                  AS likes,
    COALESCE(m.payload->>'gated', 'false')          AS gated,
    (m.payload->>'private')::boolean                AS private,
    (m.payload->>'disabled')::boolean               AS disabled
FROM stg.hf_models m
WHERE m.payload IS NOT NULL;
