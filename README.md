# Конвейер данных на Airflow

Задание 3 по курсу "Прикладные пакеты для анализа данных".

Данные забираются из API, проходят слои stg, dds и cdm в Postgres, а потом выводятся на дашборд в Metabase.

## Структура

```
dags/        DAG-и Airflow
sql/init/    создание баз и схем при первом запуске Postgres
sql/stg/     таблицы слоя stg
sql/dds/     таблицы и загрузка слоя dds
sql/cdm/     витрины
```

## Роли

- Роль 1: docker compose, слой stg, сбор данных, оркестрация
- Роль 2: слой dds, версионирование SCD2, перенос из stg в dds
- Роль 3: витрины cdm, дашборды в Metabase

## Что нужно

- git
- bash и openssl для скрипта `make_env.sh` (есть в macOS, Linux и WSL)
- Docker Desktop (macOS, Windows с WSL2) или Docker Engine с плагином Compose (Linux). Нужен Docker Compose v2.14 или новее, проверить можно командой `docker compose version`
- Для Docker не меньше 4 ГБ памяти, лучше 6-8 ГБ. На macOS это Settings, Resources, Memory в Docker Desktop. На Windows с WSL2 это параметр `memory` в разделе `[wsl2]` файла `%UserProfile%\.wslconfig`. На Linux отдельной настройки нет
- Свободные порты 8080, 15432, 3333
- На Windows команды выполняются в терминале WSL2, а репозиторий лучше клонировать в файловую систему WSL

## Запуск

```bash
git clone https://github.com/AndrewFalse/ppad-lab3.git
cd ppad-lab3
bash make_env.sh
docker compose up -d
```

Первый запуск занимает несколько минут: скачиваются образы, создаются базы. Проверить состояние:

```bash
docker compose ps -a
```

Все сервисы должны быть `healthy`, а `airflow-init` в состоянии `Exited (0)`. Сразу после запуска сервисы ещё проходят проверки, поэтому подождите 1-2 минуты и повторите команду.

## Адреса и доступы

| Что | Адрес | Логин и пароль |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` из `.env` |
| Metabase | http://localhost:3333 | при первом входе Metabase просит создать свою учётную запись |
| Postgres с ноутбука (DBeaver) | localhost:15432, база `de` | `etl` / `DWH_ETL_PASSWORD` из `.env` |

Пароли и ключи в `.env` создаёт скрипт `make_env.sh`, сам `.env` в git не попадает (он в `.gitignore`). Если удалить `.env` и создать заново, пароли будут другими, поэтому базы придётся пересоздать командой `docker compose down -v`. Если меняете пароли вручную, используйте только латинские буквы и цифры, не короче 5 символов: пароли подставляются в SQL и в адреса подключений, а Airflow скрывает в логах только секреты от 5 символов.

Пароли ролей и логин администратора Airflow применяются один раз, при первом запуске. Если поменять их в `.env` позже, нужно пересоздать базы командой `docker compose down -v` (все данные удалятся).

## Базы и роли

| База | Владелец | Зачем |
|---|---|---|
| `de` | `etl` | хранилище: схемы `stg`, `dds`, `cdm` |
| `airflow` | `airflow` | служебная база Airflow |
| `metabase` | `metabase` | настройки и дашборды Metabase |

- `etl` владеет хранилищем, создаёт и наполняет таблицы всех слоёв. Задачи Airflow подключаются к хранилищу под ним.
- `bi_reader` может читать только схему `cdm`. Под ним Metabase подключается к хранилищу.

Базы, роли и схемы создаёт `sql/init/01_init.sh` при первом запуске Postgres. Если скрипт поменялся, базу нужно пересоздать (все данные удалятся):

```bash
docker compose down -v
docker compose up -d
```

Если при первом запуске скрипт упал, после автоматического перезапуска Postgres выполнять его уже не будет, и базы останутся не созданными. В этом случае найдите первую строку `ERROR` в `docker compose logs postgres`, исправьте причину и пересоздайте базы командами выше.

## Подключения

- В Airflow подключение к хранилищу называется `dwh`. Оно задано переменной `AIRFLOW_CONN_DWH` в `docker-compose.yml`, поэтому в разделе Admin, Connections его не видно.
- Логи задач Airflow хранятся в томе Docker `airflow-logs`, поэтому на Linux не нужно настраивать `AIRFLOW_UID`, как в официальной инструкции Airflow.
- В Metabase хранилище добавляется при первой настройке или позже в разделе Admin, Databases: тип PostgreSQL, host `postgres`, port `5432`, база `de`, пользователь `bi_reader`, пароль `DWH_BI_PASSWORD`, в настройке Schemas выбрать только `cdm`. Писать `localhost` нельзя: внутри контейнера это сам Metabase.

## Проверка

В Airflow есть служебный DAG `check_dwh`: он проверяет, что подключение `dwh` работает и схемы `stg`, `dds`, `cdm` на месте. Запустить его можно в интерфейсе (включить DAG переключателем и нажать Trigger) или командами:

```bash
docker compose exec airflow-scheduler airflow dags unpause check_dwh
docker compose exec airflow-scheduler airflow dags trigger check_dwh
```

Посмотреть результат:

```bash
docker compose exec airflow-scheduler airflow dags list-runs check_dwh
```

У запуска должно быть состояние `success`.

## Остановка

```bash
docker compose down
```

Данные сохраняются в томах Docker. Команда `docker compose down -v` удаляет их вместе с базами.
