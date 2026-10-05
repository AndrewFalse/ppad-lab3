-- Слой stg: ответы Hugging Face API как есть и служебные поля загрузки

-- снимок списка моделей организации за день
create table if not exists stg.hf_models (
    business_date date,
    org text,
    payload jsonb,
    source text default 'huggingface.co/api/models',
    load_id text,
    loaded_at timestamptz default now()
);

create index if not exists hf_models_date_org on stg.hf_models (business_date, org);
create unique index if not exists hf_models_date_id on stg.hf_models (business_date, (payload->>'_id'));

-- журнал снимков: сколько страниц и моделей пришло по организации
create table if not exists stg.hf_list_runs (
    business_date date,
    org text,
    pages integer,
    models integer,
    source text default 'huggingface.co/api/models',
    load_id text,
    loaded_at timestamptz default now()
);

create unique index if not exists hf_list_runs_date_org on stg.hf_list_runs (business_date, org);

-- коммиты моделей, hf_id это стабильный _id модели
create table if not exists stg.hf_commits (
    hf_id text not null,
    repo_id text not null,
    sha text not null,
    payload jsonb,
    source text default 'huggingface.co/api/models/{id}/commits/main',
    load_id text,
    loaded_at timestamptz default now(),
    unique (hf_id, sha)
);

-- порядковый номер коммита в истории модели: 1 самый первый, нужен, если у коммитов одинаковые даты
alter table stg.hf_commits add column if not exists commit_number integer;

-- состояние карточки и файлов модели на каждый коммит
create table if not exists stg.hf_revisions (
    hf_id text not null,
    repo_id text not null,
    sha text not null,
    http_status integer,
    payload jsonb,
    source text default 'huggingface.co/api/models/{id}/revision/{sha}',
    load_id text,
    loaded_at timestamptz default now(),
    unique (hf_id, sha)
);

-- базовые модели из карточек, которых нет в нашем охвате
create table if not exists stg.hf_base_models (
    repo_id text not null,
    http_status integer,
    payload jsonb,
    source text default 'huggingface.co/api/models/{id}',
    load_id text,
    loaded_at timestamptz default now(),
    unique (repo_id)
);

-- справочник лицензий Hugging Face за день
create table if not exists stg.hf_license_tags (
    business_date date,
    payload jsonb,
    source text default 'huggingface.co/api/models-tags-by-type?type=license',
    load_id text,
    loaded_at timestamptz default now()
);

create unique index if not exists hf_license_tags_date on stg.hf_license_tags (business_date);
