# ППАД, задание 3: конвейер Airflow, Postgres, Metabase

Тема: документация и лицензии открытых ИИ-моделей на Hugging Face.
Поток данных: API Hugging Face → `stg` → `dds` → `cdm` → дашборд в Metabase.

## Запуск

Нужен Docker (не меньше 4 ГБ памяти, лучше 6-8 ГБ). На Windows всё выполняется в терминале WSL2.

```bash
git clone https://github.com/AndrewFalse/ppad-lab3.git
cd ppad-lab3
bash make_env.sh
docker compose up -d
```

`make_env.sh` создаёт `.env` с паролями. Через 1-2 минуты `docker compose ps` покажет все сервисы `healthy`.

Затем положите в папку репозитория файл дампа `stg_20261006.dump` (его даёт роль 1), восстановите его и только потом включите DAG:

```bash
docker compose exec -T postgres pg_restore -U etl -d de --no-owner --clean --if-exists < stg_20261006.dump
docker compose exec airflow-scheduler airflow dags unpause hf_pipeline
```

Если включить DAG без дампа, начнётся загрузка всей истории на 6-7 часов.

| Что | Адрес | Доступ |
|---|---|---|
| Airflow | http://127.0.0.1:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` из `.env` |
| Metabase | http://127.0.0.1:3333 | учётная запись создаётся при первом входе |
| Postgres | 127.0.0.1:15432, база `de` | `etl` / `DWH_ETL_PASSWORD` из `.env` |

## DAG hf_pipeline

Запускается каждый день в 03:00 UTC (06:00 по Москве). Группы задач идут по очереди:

`extract` (сбор в stg, готово) → `dds` → `dq` → `cdm`

Группы `dds`, `dq` и `cdm` выполняют SQL-файлы из папок `sql/dds/`, `sql/dq/`, `sql/cdm/` по порядку имён. Python трогать не нужно: кладёте файл, через минуту он появляется в DAG.

## Кто что делает

| Роль | Где | Что |
|---|---|---|
| 2 | `sql/dds/` | слой dds из stg, версии SCD2 |
| 2 | `sql/dq/` | проверки dds |
| 3 | `sql/cdm/` | витрины из dds |
| 3 | Metabase | подключение: host `postgres`, port `5432`, база `de`, пользователь `bi_reader`, пароль `DWH_BI_PASSWORD`, схема `cdm`; дашборд |

## Правила для SQL-файлов

- Имя: `01_name.sql`, `02_name.sql`. Номер из двух цифр, только латиница, цифры и `_`, имя `start.sql` занято.
- Один файл выполняется в одной транзакции. Повторный запуск должен давать тот же результат: `create table if not exists`, перед вставкой `delete` или `truncate`.
- Всегда пишите схему: `dds.table`, `cdm.table`. Таблицы создаются только под ролью `etl`, тогда Metabase их видит.
- Дата снимка в SQL: `'{{ dag_run.run_after.strftime("%Y-%m-%d") }}'`. Шаблон `{{ ds }}` не работает.
- Проверка в `sql/dq/` это один `select`, который возвращает одну строку, где все значения `true`. Например: `select count(*) = 0 as ok from dds.dim_model where ...`. Если проверка не прошла, витрины не пересчитываются.
- Проверить новый файл: `docker compose exec airflow-scheduler airflow dags trigger hf_pipeline --conf '{"orgs": ["yandex"]}'` (сбор только по одной организации, займёт пару минут).

## Данные в stg

`payload` это JSON из ответа API как есть.

| Таблица | Что лежит | Ключ |
|---|---|---|
| `stg.hf_orgs` | 20 организаций и группа: `labs`, `ru`, `quant` | `org` |
| `stg.hf_models` | снимок моделей за день | `business_date`, `payload->>'_id'` |
| `stg.hf_list_runs` | сколько моделей пришло по организации за день | `business_date`, `org` |
| `stg.hf_commits` | коммиты моделей, `commit_number` 1 это первый коммит | `hf_id`, `sha` |
| `stg.hf_revisions` | карточка и список файлов модели на каждый коммит | `hf_id`, `sha` |
| `stg.hf_base_models` | базовые модели, которых нет в снимке | `repo_id` |
| `stg.hf_license_tags` | справочник лицензий Hugging Face | `business_date` |

Главное для dds:

- Ключ модели `hf_id`, он равен `payload->>'_id'` и не меняется при переименовании.
- История версий берётся из `stg.hf_revisions`: дата версии `payload->>'lastModified'`, порядок по `commit_number`. У закрытых моделей (`gated` равно `auto` или `manual`) и у группы `quant` коммитов нет, их история только по ежедневным снимкам.
- Лицензия: `payload->'cardData'->'license'`, бывает строкой или списком. При `other` смотрите `license_name` и `license_link`. `base_model` тоже строка или список.
- Для версий SCD2 берите `cardData`, `siblings` (файлы), `pipeline_tag`, `library_name`. `downloads` и `likes` меняются каждый день, это факты снимка.
- Полный снимок дня: в `stg.hf_list_runs` за дату 20 строк и у всех `models` больше 0. Проверочные запуски дают неполный снимок.
- `createdAt` равный `2022-03-02T23:29:0...` у старых моделей ненастоящий.
- Если `http_status` не 200, `payload` равен NULL.

## Если что-то сломалось

- Упала `extract.check_stg`: запустите DAG заново.
- Ошибка `this run is not from today`: не нажимайте Clear у вчерашних запусков, запустите новый.
- Упала задача в `dds`, `dq` или `cdm`: исправьте SQL и нажмите Clear у этой задачи в сегодняшнем запуске.
- `No space left on device`: освободите место на диске и перезапустите Docker Desktop.
- Не запускайте сбор одновременно с двух ноутбуков в одной сети.

Остановить: `docker compose down` (данные сохраняются). `docker compose down -v` удаляет базы.
