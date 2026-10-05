#!/bin/bash
# Создаёт .env из .env.example и заполняет пустые пароли и ключи случайными значениями
set -e
set -o pipefail

if [ -f .env ]; then
    echo ".env already exists"
    exit 0
fi

if ! command -v openssl > /dev/null; then
    echo "openssl not found"
    exit 1
fi

# сначала пишем во временный файл, чтобы при ошибке не остался недозаполненный .env
# если скрипт упадёт, временный файл удалится
trap 'rm -f .env.tmp .env.tmp.bak' EXIT
cp .env.example .env.tmp

for name in $(grep -E '^[A-Z0-9_]+=$' .env.tmp | tr -d '='); do
    if [ "$name" = "AIRFLOW_FERNET_KEY" ]; then
        # ключ Fernet это 32 байта в base64, где + и / заменены на - и _
        value=$(openssl rand -base64 32 | tr '+/' '-_')
    else
        value=$(openssl rand -hex 16)
    fi
    sed -i.bak "s|^$name=$|$name=$value|" .env.tmp
done
rm -f .env.tmp.bak

chmod 600 .env.tmp
mv .env.tmp .env

echo ".env created"
