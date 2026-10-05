#!/bin/bash
set -e

# Скрипт выполняется только при первом запуске, когда том с данными пустой

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<EOSQL
create role airflow login password '$AIRFLOW_DB_PASSWORD';
create role metabase login password '$METABASE_DB_PASSWORD';
create role etl login password '$DWH_ETL_PASSWORD';
create role bi_reader login password '$DWH_BI_PASSWORD';

create database airflow owner airflow;
create database metabase owner metabase;
create database de owner etl;

revoke connect on database airflow from public;
revoke connect on database metabase from public;
revoke connect on database de from public;
grant connect on database de to bi_reader;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname de <<EOSQL
create schema stg authorization etl;
create schema dds authorization etl;
create schema cdm authorization etl;

-- BI видит только витрины
grant usage on schema cdm to bi_reader;
alter default privileges for role etl in schema cdm grant select on tables to bi_reader;
EOSQL
