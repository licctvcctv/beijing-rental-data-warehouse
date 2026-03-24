#!/bin/bash
set -e

BASE_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$BASE_DIR/../.." && pwd)

"$BASE_DIR/run_all.sh"
cd "$PROJECT_ROOT"
docker compose up -d mysql metabase
python3 "$BASE_DIR/seed_ads_from_csv.py"
python3 "$BASE_DIR/init_metabase_dashboard.py"

echo "离线链路已执行，BI 面板已启动。"
echo "Metabase: http://localhost:3000"
echo "默认管理员: admin@wenyu.local / Admin@123456"
echo "预置仪表板: 北京娱乐方式离线数仓 BI 看板"
