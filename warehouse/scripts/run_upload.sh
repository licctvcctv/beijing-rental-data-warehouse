#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"
JAR_PATH="target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [[ "${SKIP_MVN_PACKAGE:-false}" != "true" ]]; then
  mvn -q -DskipTests -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package
fi

JAVA_CP="$JAR_PATH"
if command -v hadoop >/dev/null 2>&1; then
  JAVA_CP="$JAR_PATH:$(hadoop classpath --glob)"
fi

java -cp "$JAVA_CP" com.beijing.wenyu.runner.UploadRunner true
