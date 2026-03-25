#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
RAW_DIR=${1:-../crawler/data/export}
OUTPUT_DIR=${2:-target/local-offline-output}

cd "$BASE_DIR"

mvn -q -DskipTests -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package
java -cp "target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar" \
  com.beijing.wenyu.runner.LocalOfflineWarehouseRunner \
  "$RAW_DIR" \
  "$OUTPUT_DIR"
