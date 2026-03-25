#!/bin/sh
set -e

SEED_SQL=${SEED_SQL:-/workspace/output/mysql/ads_seed.sql}
SCHEMA_SQL=${SCHEMA_SQL:-/workspace/schema/01_wenyu_result.sql}
MYSQL_HOST=${MYSQL_HOST:-mysql}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-root123}
MYSQL_DATABASE=${MYSQL_DATABASE:-wenyu_result}

echo "Waiting for generated ADS SQL..."
until [ -f "$SEED_SQL" ]; do
  sleep 2
done

echo "Waiting for MySQL..."
until mysqladmin ping -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" --silent; do
  sleep 2
done

echo "Applying schema..."
mysql --default-character-set=utf8mb4 \
  -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
  < "$SCHEMA_SQL"

echo "Applying ADS seed data..."
mysql --default-character-set=utf8mb4 \
  -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" \
  < "$SEED_SQL"

echo "MySQL seed finished."
