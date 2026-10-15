-- 05_dim_license.sql
-- Справочник лицензий Hugging Face

CREATE TABLE IF NOT EXISTS dds.dim_license (
    license_sk      serial PRIMARY KEY,
    license_id      text NOT NULL UNIQUE,   -- id лицензии (apache-2.0, mit, other и т.д.)
    label           text,
    loaded_at       timestamptz DEFAULT now()
);

TRUNCATE dds.dim_license;

INSERT INTO dds.dim_license (license_id, label)
SELECT DISTINCT
    t.elem->>'id'           AS license_id,
    t.elem->>'label'        AS label
FROM stg.hf_license_tags lt,
     jsonb_array_elements(lt.payload->'license') AS t(elem)
WHERE lt.business_date = (SELECT max(business_date) FROM stg.hf_license_tags)
  AND t.elem->>'id' IS NOT NULL
ORDER BY 1;
