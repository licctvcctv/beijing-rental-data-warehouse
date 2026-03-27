#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=============================================="
echo "  北京租房数据离线数仓 - 一键部署"
echo "=============================================="
echo ""

# 1. 检查 Docker
if ! command -v docker &>/dev/null; then
    echo "[ERROR] 未安装 Docker，请先安装 Docker Desktop"
    exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "[ERROR] Docker 未启动，请先启动 Docker Desktop"
    exit 1
fi
echo "[OK] Docker 已就绪"

# 2. 生成租房数据（如果不存在）
if [ ! -f crawler/data/export/rental_raw.csv ]; then
    echo "[...] 生成租房模拟数据 ..."
    python3 crawler/gen_rental_data.py
else
    echo "[OK] 租房数据已存在 ($(wc -l < crawler/data/export/rental_raw.csv) 行)"
fi

# 3. Maven 构建（如果 jar 不存在）
if [ ! -f warehouse/target/wenyu-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar ]; then
    echo "[...] 构建 Java 数仓模块 ..."
    cd warehouse && mvn -q package -DskipTests && cd ..
    echo "[OK] Java 构建完成"
else
    echo "[OK] Java jar 已存在，跳过构建"
fi

# 4. 启动所有容器
echo ""
echo "[...] 启动 Docker 容器（首次需要拉镜像，请耐心等待）..."
docker compose up --build -d

echo ""
echo "=============================================="
echo "  部署已启动！各服务地址："
echo "----------------------------------------------"
echo "  Superset BI 看板:  http://localhost:8089"
echo "  账号: admin  密码: admin"
echo ""
echo "  HDFS Web UI:       http://localhost:9870"
echo "  YARN Web UI:       http://localhost:8088"
echo "  HiveServer2:       localhost:10000"
echo "=============================================="
echo ""
echo "提示: 数仓 ETL 流水线约需 10-20 分钟完成，"
echo "      完成后 Superset 看板会自动创建图表。"
echo ""
echo "查看日志:  docker compose logs -f"
echo "停止服务:  docker compose down"
echo ""

# 5. 等待 Superset 就绪并自动打开浏览器
echo "[...] 等待 Superset 启动 ..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:8089/health >/dev/null 2>&1; then
        echo "[OK] Superset 已就绪，正在打开浏览器 ..."
        if command -v open &>/dev/null; then
            open "http://localhost:8089"
        elif command -v xdg-open &>/dev/null; then
            xdg-open "http://localhost:8089"
        fi
        exit 0
    fi
    sleep 3
done
echo "[INFO] Superset 还在启动中，请稍后手动访问 http://localhost:8089"
