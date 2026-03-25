#!/bin/bash
set -euo pipefail

PLATFORM_PID=""

cleanup() {
  if [[ -n "${PLATFORM_PID}" ]]; then
    kill "${PLATFORM_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT

/entrypoint.sh &
PLATFORM_PID=$!

/workspace/warehouse/scripts/docker/run_hadoop_pipeline.sh

trap - EXIT
wait "${PLATFORM_PID}"
