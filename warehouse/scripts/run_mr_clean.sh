#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$BASE_DIR"

JAR_PATH="target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar"
mvn -q -DskipTests -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 package

hdfs dfs -rm -r -f /data/clean/scenic || true
hdfs dfs -rm -r -f /data/clean/show || true
hdfs dfs -rm -r -f /data/clean/ktv || true
hdfs dfs -rm -r -f /data/clean/movie || true
hdfs dfs -rm -r -f /data/clean/sport || true

hadoop jar "$JAR_PATH" com.beijing.wenyu.etl.cleaner.CleanScenicJob /data/logs/scenic /data/clean/scenic
hadoop jar "$JAR_PATH" com.beijing.wenyu.etl.cleaner.CleanShowJob /data/logs/show /data/clean/show
hadoop jar "$JAR_PATH" com.beijing.wenyu.etl.cleaner.CleanKtvJob /data/logs/ktv /data/clean/ktv
hadoop jar "$JAR_PATH" com.beijing.wenyu.etl.cleaner.CleanMovieJob /data/logs/movie /data/clean/movie
hadoop jar "$JAR_PATH" com.beijing.wenyu.etl.cleaner.CleanSportJob /data/logs/sport /data/clean/sport
