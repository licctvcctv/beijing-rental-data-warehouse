# Metabase BI 面板说明

## 1. 启动方式

只启动 BI 面板：

```bash
cd warehouse
bash scripts/run_bi_stack.sh
```

执行完整离线链路并启动 BI 面板：

```bash
cd warehouse
bash scripts/run_all_with_bi.sh
```

这两个脚本都会自动：
- 启动 `mysql + metabase`
- 初始化 Metabase 管理员账号
- 自动连接 `wenyu_result` 数据库
- 自动创建问题和预置仪表板

## 2. 访问地址

- Metabase：`http://localhost:3000`
- MySQL：`localhost:3306`

默认数据库：
- database: `wenyu_result`
- username: `wenyu`
- password: `wenyu123`

## 3. 自动初始化结果

脚本默认会自动创建以下 Metabase 管理员账号：

- Email: `admin@wenyu.local`
- Password: `Admin@123456`

自动创建的数据源：
- Engine: `MySQL`
- Host: `mysql`
- Port: `3306`
- Database: `wenyu_result`
- Username: `wenyu`
- Password: `wenyu123`

自动创建的仪表板名称：
- `北京娱乐方式离线数仓 BI 看板`

如果你是在本机外访问 Metabase 容器，UI 地址仍是 `localhost:3000`，但 Metabase 容器内部连接 MySQL 时 Host 仍使用 `mysql`。

## 4. 推荐仪表板图表

| 图表名 | 数据表 | 推荐图形 | 维度/指标 |
|---|---|---|---|
| 各区娱乐资源总量 | `ads_region_entertainment_count` | 柱状图 | `region`, `entertainment_count` |
| 电影评分分布 | `ads_movie_score_distribution` | 饼图/环形图 | `score_level`, `movie_count` |
| 演出价格 Top10 | `ads_show_price_top10` | 条形图 | `name`, `price_max` |
| 演出售票状态占比 | `ads_show_status_ratio` | 饼图 | `status_std`, `status_ratio` |
| KTV 区域热度 | `ads_ktv_region_hotspot` | 柱状图 | `region`, `ktv_count` |
| KTV 性价比 Top5 | `ads_ktv_cost_performance_top5` | 横向条形图 | `name`, `cost_performance` |
| 体育场馆类型 Top5 | `ads_sport_type_ratio_top5` | 饼图 | `venue_type`, `venue_ratio` |
| 景点免费占比 | `ads_scenic_free_ratio` | 饼图 | `scenic_type`, `scenic_ratio` |

## 5. 说明

- `docker-compose.yml` 已提供 MySQL + Metabase 的 BI 面板运行环境。
- `warehouse/sql/mysql/01_wenyu_result.sql` 会在 MySQL 容器首次初始化时自动建库建表。
- `warehouse/scripts/init_metabase_dashboard.py` 会通过 Metabase API 自动完成管理员、数据源、问题和仪表板初始化。
- 你仍需先将 Hive ADS 结果通过 Sqoop 导入到 MySQL，Metabase 才能展示真实数据。
