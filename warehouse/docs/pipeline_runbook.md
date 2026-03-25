# warehouse 模块运行说明

## 1. 模块目标

该模块补齐项目主程序离线链路：

`原始 CSV -> HDFS -> Java MapReduce -> Hive -> Sqoop -> MySQL -> BI`

其中：
- `crawler/` 只负责原始采集与 CSV 导出
- `warehouse/` 负责上传、清洗、分层、导出与交付说明

## 1.1 当前新增的本地离线数仓链路

为了让项目在没有 Hadoop / Hive / Sqoop 环境时也能完整演示“离线数仓全流程”，现在额外补充了一条 **Java 本地离线链路**：

`原始 CSV -> Java 清洗 -> 本地 ODS -> 本地 DWD -> 本地 DWS -> 本地 ADS -> MySQL 导入 SQL`

说明：

- 这条链路只用于本机演示和联调，不替代正式的 Hadoop 离线链路。
- 这条链路同样只使用 Java 构建分层结果，Python 不参与 `warehouse` 主流程。
- 产物会落到 `warehouse/target/local-offline-output/`，便于答辩时直接展示每一层文件。

## 1.2 Docker 一键部署链路

项目根目录现在支持直接使用 Docker 完成以下自动化步骤：

`现成原始 CSV -> HDFS -> Java MapReduce -> Hive ODS/DWD/DWS/ADS -> Sqoop -> MySQL`

自动完成的内容包括：

- 不运行爬虫，直接使用 `crawler/data/export/` 下已有原始 CSV
- 自动启动 Hadoop、HDFS、YARN、Hive、Sqoop 运行环境
- 自动执行 Java HDFS 上传与 Java MapReduce 清洗
- 自动执行 Hive ODS / DWD / DWS / ADS 分层
- 自动执行 Sqoop 导出到 `wenyu_result`
- 自动启动 Metabase，并自动完成管理员、数据源、问题和仪表板初始化
- 自动生成链路摘要文件，便于答辩时展示结果

## 2. 目录说明

- `src/main/java/com/beijing/wenyu/hdfs`：HDFS 上传代码
- `src/main/java/com/beijing/wenyu/etl/cleaner`：5 个 MapReduce 清洗作业
- `sql/hive`：ODS / DWD / DWS / ADS SQL
- `sql/mysql`：MySQL 结果表 DDL
- `scripts`：批处理执行脚本

## 3. 前置条件

1. `crawler/data/export/` 下存在以下文件：
   - `scenic_raw.csv`
   - `show_raw.csv`
   - `ktv_raw.csv`
   - `movie_raw.csv`
   - `sport_raw.csv`
2. Hadoop / Hive / Sqoop / MySQL 已安装并可在命令行使用。
3. 根据实际环境修改 `src/main/resources/warehouse.properties` 中的 HDFS、MySQL 配置。

## 4. 执行步骤

### 4.0 无 Hadoop 环境时，先跑本地离线数仓链路

Linux / Git Bash:

```bash
cd warehouse
bash scripts/run_local_offline.sh
```

Windows PowerShell:

```powershell
cd warehouse
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_offline.ps1
```

执行完成后，会生成：

- `target/local-offline-output/raw/`：原始 CSV 快照
- `target/local-offline-output/clean/`：Java 清洗后的分主题明细
- `target/local-offline-output/ods/`：ODS 层 CSV
- `target/local-offline-output/dwd/`：DWD 层 CSV
- `target/local-offline-output/dws/`：DWS 层 CSV
- `target/local-offline-output/ads/`：ADS 层 CSV
- `target/local-offline-output/mysql/ads_seed.sql`：导入 MySQL 的结果 SQL
- `target/local-offline-output/summary.txt`：各层记录数摘要

这一步适合：

- 本机先验证“全链路是否齐”
- 没有 Hadoop 环境时先准备答辩展示材料
- 先确认清洗、分层、汇总指标是否合理，再切换到 Hadoop / Hive 正式链路

### 4.0.1 Docker 一键启动

在项目根目录执行：

```bash
docker compose up --build -d
```

默认会启动以下核心服务：

- `hive-metastore-db`：Hive 元数据库
- `mysql`：Sqoop 导出目标库 `wenyu_result`
- `warehouse-platform`：HDFS / YARN / Hive / Sqoop / Java MapReduce 一体化执行容器
- `metabase`：BI 展示面板
- `metabase-init`：Metabase 自动初始化任务，完成后会退出并显示 `Exited (0)`

执行完成后可直接检查：

- HDFS NameNode：`http://localhost:9870`
- YARN ResourceManager：`http://localhost:8088`
- HiveServer2：`localhost:10000`
- Metabase：`http://localhost:3000`

说明：

- MySQL 不再对宿主机暴露 `3306` 端口，避免和他人电脑上的本地 MySQL 冲突。
- 容器内部的 Hive、Sqoop、Metabase 仍会通过 Docker 网络访问 `mysql:3306`，不影响链路执行。

链路产物会输出到：

- `warehouse/target/docker-hadoop-output/summary.txt`
- `warehouse/target/docker-hadoop-output/mysql_counts.tsv`
- `warehouse/target/docker-hadoop-output/ads_region_entertainment_count.tsv`
- `warehouse/target/docker-hadoop-output/metabase.success`

其中：

- `pipeline.success` 表示离线链路执行成功
- `metabase.success` 中会记录当前看板地址 `dashboard_url=...`

### 4.1 原始数据上传

```bash
cd warehouse
bash scripts/run_upload.sh
```

验证：

```bash
hdfs dfs -ls /data/logs/scenic
hdfs dfs -ls /data/logs/show
hdfs dfs -ls /data/logs/ktv
hdfs dfs -ls /data/logs/movie
hdfs dfs -ls /data/logs/sport
```

### 4.2 MapReduce 清洗

```bash
bash scripts/run_mr_clean.sh
```

验证：

```bash
hdfs dfs -ls /data/clean/scenic
hdfs dfs -cat /data/clean/show/part-r-00000 | head
```

### 4.3 Hive 分层

```bash
bash scripts/run_hive_ods.sh
bash scripts/run_hive_dwd.sh
bash scripts/run_hive_dws_ads.sh
```

验证：

```sql
show databases;
select count(1) from wenyu_ods.ods_show_info;
select * from wenyu_dwd.dwd_show_detail limit 10;
select * from wenyu_ads.ads_show_price_top10 limit 10;
```

### 4.4 MySQL 建表与 Sqoop 导出

先在 MySQL 执行：

```sql
source warehouse/sql/mysql/01_wenyu_result.sql;
```

再执行：

```bash
bash scripts/run_sqoop_export.sh
```

## 5. BI 面板访问

BI 已经是默认核心功能，执行下面这条命令时会随离线链路一起启动并自动初始化：

```bash
docker compose up --build -d
```

访问地址：

- Metabase 登录页：`http://localhost:3000`
- 实际仪表板地址：以 `warehouse/target/docker-hadoop-output/metabase.success` 中的 `dashboard_url=` 为准

容器会自动完成：

- 创建 Metabase 管理员账号：`admin@wenyu.local / Admin@123456`
- 连接 MySQL 数据源 `wenyu_result`
- 自动创建预置问题
- 自动创建 `北京娱乐方式离线数仓 BI 看板`

推荐演示方式：

- 默认管理界面：直接打开 `dashboard_url`
- 推荐全屏展示：在 `dashboard_url` 后追加 `#fullscreen`

说明：

- Metabase 默认管理界面会保留一层导航壳，左右会有少量留白。
- 需要答辩展示时，优先使用 `#fullscreen` 模式，看板会更接近全屏展示。

详细说明见：`warehouse/docs/metabase_dashboard_guide.md`

## 6. BI 图表建议

建议直接连接 `wenyu_result` 库，至少做以下 6~8 张图：

| 图表 | MySQL 表 |
|---|---|
| 各区娱乐资源总量 | `ads_region_entertainment_count` |
| 电影评分分布 | `ads_movie_score_distribution` |
| 演出价格 Top10 | `ads_show_price_top10` |
| 演出售票状态占比 | `ads_show_status_ratio` |
| KTV 区域热度 | `ads_ktv_region_hotspot` |
| KTV 性价比 Top5 | `ads_ktv_cost_performance_top5` |
| 体育场馆类型 Top5 | `ads_sport_type_ratio_top5` |
| 景点免费占比 | `ads_scenic_free_ratio` |

## 7. 当前已知注意事项

1. 现在 5 类原始 CSV 已齐全，包括新增的 `crawler/data/export/movie_raw.csv`。
2. `scenic_raw.csv` 的价格字段可能包含长段门票政策文案；当前清洗优先抽取“门票/联票/成人票/通票”等片段里的价格区间，避免把政策编号误识别为票价。
3. 脚本默认依赖本机命令 `mvn`、`hive`、`hadoop`、`hdfs`、`sqoop`，如果安装路径不同，请在 shell 环境或脚本中调整。
4. 如果 Maven 依赖下载慢，可优先使用 `warehouse/pom.xml` 中已加入的阿里云镜像仓库，再执行脚本中的构建命令。
