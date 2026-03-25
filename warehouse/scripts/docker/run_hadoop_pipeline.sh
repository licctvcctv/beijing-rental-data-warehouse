#!/bin/bash
set -euo pipefail

OUTPUT_DIR=/workspace/warehouse/target/docker-hadoop-output
PIPELINE_MARKER="${OUTPUT_DIR}/pipeline.success"
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-300}
SERVICE_HOST=${SERVICE_HOST:-$(hostname)}

log() {
  echo "[pipeline] $1"
}

on_error() {
  local exit_code=$?
  set +e
  log "Pipeline failed, writing failure summary"
  {
    echo "pipeline_status=FAILED"
    echo "run_time=$(date '+%F %T %z')"
    echo "hdfs_uri=hdfs://$(hostname):9820"
  } > "${OUTPUT_DIR}/summary.txt"
  exit "${exit_code}"
}

wait_for_hdfs() {
  /wait-for-it.sh -h "${SERVICE_HOST}" -p 9820 -t "${WAIT_TIMEOUT_SECONDS}"
  until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    sleep 3
  done
  hdfs dfs -mkdir -p /tmp >/dev/null 2>&1 || true
}

wait_for_yarn() {
  /wait-for-it.sh -h "${SERVICE_HOST}" -p 8030 -t "${WAIT_TIMEOUT_SECONDS}"
  until yarn node -list >/dev/null 2>&1; do
    sleep 3
  done
}

wait_for_hive() {
  /wait-for-it.sh -h "${SERVICE_HOST}" -p 9083 -t "${WAIT_TIMEOUT_SECONDS}"
  /wait-for-it.sh -h "${SERVICE_HOST}" -p 10000 -t "${WAIT_TIMEOUT_SECONDS}"
  until hive -S -e "show databases;" >/dev/null 2>&1; do
    sleep 5
  done
}

wait_for_mysql() {
  until mysqladmin ping -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" --silent >/dev/null 2>&1; do
    sleep 3
  done
}

main() {
  mkdir -p "${OUTPUT_DIR}"
  trap on_error ERR
  rm -f "${PIPELINE_MARKER}"
  rm -f "${OUTPUT_DIR}/summary.txt" "${OUTPUT_DIR}/mysql_counts.tsv" "${OUTPUT_DIR}/metabase.success" "${OUTPUT_DIR}"/ads_*.tsv

  log "Waiting for HDFS, YARN, Hive and MySQL"
  wait_for_hdfs
  wait_for_yarn
  wait_for_hive
  wait_for_mysql

  log "Running Java upload, MapReduce, Hive and Sqoop pipeline"
  cd /workspace/warehouse
  export SKIP_MVN_PACKAGE=true
  export WAREHOUSE_HDFS_URI="hdfs://$(hostname):9820"
  export WAREHOUSE_HDFS_USER=root
  export WAREHOUSE_LOCAL_RAW_DIR=/workspace/crawler/data/export
  bash scripts/run_all.sh

  log "Collecting verification artifacts"
  /workspace/warehouse/scripts/docker/write_pipeline_summary.sh
  touch "${PIPELINE_MARKER}"
  trap - ERR
  log "Pipeline completed successfully"
}

main "$@"
