#!/bin/bash
set -e

PROJECT_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$PROJECT_ROOT"

docker compose up -d mysql metabase
python3 "$SCRIPT_DIR/seed_ads_from_csv.py"
python3 "$SCRIPT_DIR/init_metabase_dashboard.py"

echo "MySQL: localhost:3306"
echo "Metabase: http://localhost:3000"
echo "默认管理员: admin@wenyu.local / Admin@123456"
echo "预置仪表板: 北京娱乐方式离线数仓 BI 看板"
