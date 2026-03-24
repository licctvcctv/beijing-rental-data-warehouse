#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")" && pwd)

"$BASE_DIR/run_upload.sh"
"$BASE_DIR/run_mr_clean.sh"
"$BASE_DIR/run_hive_ods.sh"
"$BASE_DIR/run_hive_dwd.sh"
"$BASE_DIR/run_hive_dws_ads.sh"
"$BASE_DIR/run_sqoop_export.sh"
