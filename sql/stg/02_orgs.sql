-- Охват: крупные лаборатории (labs), российские организации и авторы (ru)
-- и перераспространители квантизованных копий (quant), по ним берём только снимок без истории коммитов
-- это наша настройка, а не данные источника, поэтому source = seed
create table if not exists stg.hf_orgs (
    org text not null,
    stratum text,
    source text default 'seed sql/stg/02_orgs.sql',
    loaded_at timestamptz default now()
);

delete from stg.hf_orgs;

insert into stg.hf_orgs (org, stratum) values
    ('meta-llama', 'labs'),
    ('mistralai', 'labs'),
    ('Qwen', 'labs'),
    ('deepseek-ai', 'labs'),
    ('microsoft', 'labs'),
    ('ibm-granite', 'labs'),
    ('HuggingFaceTB', 'labs'),
    ('openai', 'labs'),
    ('nvidia', 'labs'),
    ('stabilityai', 'labs'),
    ('ai-forever', 'ru'),
    ('yandex', 'ru'),
    ('t-tech', 'ru'),
    ('deepvk', 'ru'),
    ('MTSAIR', 'ru'),
    ('Vikhrmodels', 'ru'),
    ('IlyaGusev', 'ru'),
    ('cointegrated', 'ru'),
    ('unsloth', 'quant'),
    ('bartowski', 'quant');
