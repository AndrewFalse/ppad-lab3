-- 02_load_dim_model.sql
-- Полная пересборка dds.dim_model с SCD Type 2 (идемпотентно)

BEGIN;

--------------------------------------------------------------------------------
-- 1. Все сырые версии
--------------------------------------------------------------------------------
CREATE TEMP TABLE tmp_raw ON COMMIT DROP AS
WITH revisions AS (
    SELECT
        r.hf_id,
        r.repo_id,
        r.payload->>'pipeline_tag'                          AS pipeline_tag,
        r.payload->>'library_name'                          AS library_name,
        COALESCE(r.payload->>'gated', 'false')              AS gated,
        CASE
            WHEN jsonb_typeof(r.payload->'cardData'->'license') = 'array'
                THEN r.payload->'cardData'->'license'->>0
            ELSE r.payload->'cardData'->>'license'
        END                                                 AS license,
        r.payload->'cardData'->>'license_name'              AS license_name,
        r.payload->'cardData'->>'license_link'              AS license_link,
        r.payload->'cardData'->'base_model'                 AS base_model,
        r.payload->'siblings'                               AS siblings,
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(r.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ILIKE 'readme.md'
        )                                                   AS has_readme,
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(r.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ILIKE 'license%'
        )                                                   AS has_license_file,
        COALESCE((
            SELECT array_agg(DISTINCT lower(regexp_replace(s->>'rfilename', '.*\.', '')))
            FROM jsonb_array_elements(COALESCE(r.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ~* '\.(safetensors|bin|gguf|pt|onnx|h5|msgpack)$'
        ), ARRAY[]::text[])                                 AS weight_formats,
        (r.payload->>'lastModified')::timestamptz::date     AS version_date,
        c.commit_number
    FROM stg.hf_revisions r
    JOIN stg.hf_commits c ON c.hf_id = r.hf_id AND c.sha = r.sha
    WHERE r.http_status = 200
      AND r.payload IS NOT NULL
),
snapshots AS (
    SELECT
        m.payload->>'_id'                                   AS hf_id,
        m.payload->>'id'                                    AS repo_id,
        m.payload->>'pipeline_tag'                          AS pipeline_tag,
        m.payload->>'library_name'                          AS library_name,
        COALESCE(m.payload->>'gated', 'false')              AS gated,
        CASE
            WHEN jsonb_typeof(m.payload->'cardData'->'license') = 'array'
                THEN m.payload->'cardData'->'license'->>0
            ELSE m.payload->'cardData'->>'license'
        END                                                 AS license,
        m.payload->'cardData'->>'license_name'              AS license_name,
        m.payload->'cardData'->>'license_link'              AS license_link,
        m.payload->'cardData'->'base_model'                 AS base_model,
        m.payload->'siblings'                               AS siblings,
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(m.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ILIKE 'readme.md'
        )                                                   AS has_readme,
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(m.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ILIKE 'license%'
        )                                                   AS has_license_file,
        COALESCE((
            SELECT array_agg(DISTINCT lower(regexp_replace(s->>'rfilename', '.*\.', '')))
            FROM jsonb_array_elements(COALESCE(m.payload->'siblings', '[]'::jsonb)) s
            WHERE s->>'rfilename' ~* '\.(safetensors|bin|gguf|pt|onnx|h5|msgpack)$'
        ), ARRAY[]::text[])                                 AS weight_formats,
        m.business_date                                     AS version_date,
        NULL::integer                                       AS commit_number
    FROM stg.hf_models m
    WHERE m.payload IS NOT NULL
)
SELECT * FROM revisions
UNION ALL
SELECT * FROM snapshots;


--------------------------------------------------------------------------------
-- 2. Одна запись на (hf_id, version_date)
--------------------------------------------------------------------------------
CREATE TEMP TABLE tmp_versions ON COMMIT DROP AS
SELECT DISTINCT ON (hf_id, version_date)
    hf_id,
    repo_id,
    pipeline_tag,
    library_name,
    gated,
    license,
    license_name,
    license_link,
    base_model,
    siblings,
    has_readme,
    has_license_file,
    weight_formats,
    version_date
FROM tmp_raw
WHERE version_date IS NOT NULL
ORDER BY hf_id, version_date, commit_number NULLS LAST;


--------------------------------------------------------------------------------
-- 3. Где атрибуты изменились → новая SCD2-версия
--------------------------------------------------------------------------------
CREATE TEMP TABLE tmp_scd ON COMMIT DROP AS
SELECT
    *,
    CASE
        WHEN lag(repo_id)          OVER w IS DISTINCT FROM repo_id
          OR lag(pipeline_tag)     OVER w IS DISTINCT FROM pipeline_tag
          OR lag(library_name)     OVER w IS DISTINCT FROM library_name
          OR lag(gated)            OVER w IS DISTINCT FROM gated
          OR lag(license)          OVER w IS DISTINCT FROM license
          OR lag(license_name)     OVER w IS DISTINCT FROM license_name
          OR lag(license_link)     OVER w IS DISTINCT FROM license_link
          OR lag(base_model)       OVER w IS DISTINCT FROM base_model
          OR lag(siblings)         OVER w IS DISTINCT FROM siblings
          OR lag(has_readme)       OVER w IS DISTINCT FROM has_readme
          OR lag(has_license_file) OVER w IS DISTINCT FROM has_license_file
          OR lag(weight_formats)   OVER w IS DISTINCT FROM weight_formats
          OR lag(hf_id)            OVER w IS NULL
        THEN 1 ELSE 0
    END AS is_new_version
FROM tmp_versions
WINDOW w AS (PARTITION BY hf_id ORDER BY version_date);


--------------------------------------------------------------------------------
-- 4. Финальные SCD2-строки (без array_agg по массивам)
--------------------------------------------------------------------------------
CREATE TEMP TABLE tmp_final ON COMMIT DROP AS
WITH versioned AS (
    SELECT
        *,
        sum(is_new_version) OVER (PARTITION BY hf_id ORDER BY version_date) AS version_group
    FROM tmp_scd
),
first_rows AS (
    -- берём первую строку каждой группы версий
    SELECT DISTINCT ON (hf_id, version_group)
        hf_id,
        version_group,
        repo_id,
        pipeline_tag,
        library_name,
        gated,
        license,
        license_name,
        license_link,
        base_model,
        siblings,
        has_readme,
        has_license_file,
        weight_formats,
        version_date AS valid_from
    FROM versioned
    ORDER BY hf_id, version_group, version_date
)
SELECT
    hf_id,
    repo_id,
    pipeline_tag,
    library_name,
    gated,
    license,
    license_name,
    license_link,
    base_model,
    siblings,
    has_readme,
    has_license_file,
    weight_formats,
    valid_from,
    (lead(valid_from) OVER (PARTITION BY hf_id ORDER BY valid_from) - INTERVAL '1 day')::date AS valid_to,
    (lead(valid_from) OVER (PARTITION BY hf_id ORDER BY valid_from) IS NULL) AS is_current
FROM first_rows;


--------------------------------------------------------------------------------
-- 5. Перезагрузка
--------------------------------------------------------------------------------
TRUNCATE dds.dim_model RESTART IDENTITY;

INSERT INTO dds.dim_model (
    hf_id, repo_id, pipeline_tag, library_name, gated,
    license, license_name, license_link, base_model, siblings,
    has_readme, has_license_file, weight_formats,
    valid_from, valid_to, is_current
)
SELECT
    hf_id, repo_id, pipeline_tag, library_name, gated,
    license, license_name, license_link, base_model, siblings,
    has_readme, has_license_file, weight_formats,
    valid_from, valid_to, is_current
FROM tmp_final
ORDER BY hf_id, valid_from;

COMMIT;
