#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"

hive -f sql/hive/00_create_databases.sql
hive -f sql/hive/01_ods.sql
