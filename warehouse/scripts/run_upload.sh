#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"

mvn -q -DskipTests -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package
java -cp "target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar" com.beijing.wenyu.runner.UploadRunner true
