#!/bin/sh
set -e

RAW_DIR=${RAW_DIR:-/workspace/crawler/data/export}
OUTPUT_DIR=${OUTPUT_DIR:-/workspace/output}

mkdir -p "$OUTPUT_DIR"

java -cp "target/local-classes:src/main/resources" \
  com.beijing.wenyu.runner.LocalOfflineWarehouseRunner \
  "$RAW_DIR" \
  "$OUTPUT_DIR"
