#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"

hive -f sql/hive/03_dws.sql
hive -f sql/hive/04_ads.sql
