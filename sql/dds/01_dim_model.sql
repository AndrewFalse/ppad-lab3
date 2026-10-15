-- 01_dim_model.sql
-- Измерение моделей с поддержкой SCD Type 2
-- История изменений: license, cardData, siblings, pipeline_tag, library_name, gated и т.д.

CREATE TABLE IF NOT EXISTS dds.dim_model (
    model_sk            bigserial PRIMARY KEY,          -- суррогатный ключ версии
    hf_id               text NOT NULL,                  -- бизнес-ключ модели (payload->>'_id')
    repo_id             text,                           -- имя репозитория (может меняться)
    
    -- Атрибуты, которые версионируем (SCD2)
    pipeline_tag        text,
    library_name        text,
    gated               text,                           -- false / auto / manual
    license             text,                           -- основная лицензия
    license_name        text,                           -- если license = other
    license_link        text,                           -- если license = other
    base_model          jsonb,                          -- строка или список → храним как jsonb
    siblings            jsonb,                          -- список файлов
    has_readme          boolean,                        -- есть ли README.md
    has_license_file    boolean,                        -- есть ли файл LICENSE*
    weight_formats      text[],                         -- форматы весов: safetensors, bin, gguf, pt и т.д.
    
    -- Технические поля SCD2
    valid_from          date NOT NULL,                  -- с какой даты версия актуальна
    valid_to            date,                           -- по какую дату (NULL = текущая)
    is_current          boolean NOT NULL DEFAULT true,  -- текущая версия?
    
    -- Служебные
    load_id             text,
    loaded_at           timestamptz DEFAULT now()
);

-- У одной модели только одна текущая версия
CREATE UNIQUE INDEX IF NOT EXISTS dim_model_hf_id_current
    ON dds.dim_model (hf_id)
    WHERE is_current;

-- Быстрый поиск по бизнес-ключу
CREATE INDEX IF NOT EXISTS dim_model_hf_id
    ON dds.dim_model (hf_id);

-- Поиск по лицензии (пригодится для витрин)
CREATE INDEX IF NOT EXISTS dim_model_license
    ON dds.dim_model (license)
    WHERE is_current;
