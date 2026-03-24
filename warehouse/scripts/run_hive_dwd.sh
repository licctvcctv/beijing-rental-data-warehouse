#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"

hive -f sql/hive/02_dwd.sql
