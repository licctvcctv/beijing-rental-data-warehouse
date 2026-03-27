# 基于 Hadoop 的北京地区租房数据离线数仓构建与可视化分析

基于 Hadoop 生态的北京租房数据离线数仓系统，包含数据采集、ETL 清洗、分层建模和 BI 可视化全流程。

## 技术栈

| 层级 | 技术 |
|------|------|
| 数据采集 | Python 爬虫 |
| 数据存储 | HDFS（三节点集群） |
| 数据清洗 | MapReduce / Java ETL |
| 数据仓库 | Hive（ODS → DWD → DWS → ADS） |
| 数据迁移 | Sqoop（Hive → MySQL） |
| 数据可视化 | Apache Superset |
| 容器化部署 | Docker Compose |

## 数据说明

采集北京 16 个行政区、100+ 商圈的租房数据，字段包括：

- 房源 ID、标题、类型（整租/合租/独栋）
- 行政区、商圈、经纬度
- 月租金、建筑面积
- 是否地铁房、装修情况
- 来源平台（链家/贝壳/58同城/安居客/自如）

## 数仓分层

```
ODS (ods_fy_jbxx)        ← 原始数据
  ↓
DWD (dwd_fy_mx)          ← 明细宽表，加租金单价
  ↓
DWS (6张汇总表)           ← 行政区/商圈/类型/地铁/装修/平台维度聚合
  ↓
ADS (8张应用表)           ← 直接供 BI 可视化使用
```

## BI 可视化（8 张图表）

1. 各行政区平均租金（柱状图）
2. 商圈房源数量 TOP10（柱状图）
3. 房源类型占比（饼图）
4. 地铁房 vs 非地铁房租金对比（分组柱状图）
5. 各装修情况平均租金（柱状图）
6. 各平台房源分布（饼图）
7. 各行政区房源数量（柱状图）
8. 租金热力图数据（表格）

## 一键部署

```bash
./start.sh
```

启动后访问：

| 服务 | 地址 |
|------|------|
| **Superset BI 看板** | http://localhost:8089 （admin / admin） |
| HDFS Web UI | http://localhost:9870 |
| YARN Web UI | http://localhost:8088 |

停止：

```bash
./stop.sh
```

## 项目结构

```
├── crawler/                  # 数据采集模块（Python）
│   ├── gen_rental_data.py    # 租房数据生成器
│   ├── src/                  # 爬虫源码
│   └── data/export/          # 输出 CSV
├── warehouse/                # 数仓 ETL 模块（Java）
│   ├── src/                  # Java 源码
│   └── sql/                  # Hive & MySQL 建表语句
├── superset/                 # BI 可视化模块
│   ├── Dockerfile
│   ├── superset_config.py
│   ├── bootstrap.sh
│   └── init_superset.py      # 自动创建看板脚本
├── docker-compose.yml        # 容器编排
├── start.sh                  # 一键启动
└── stop.sh                   # 一键停止
```
