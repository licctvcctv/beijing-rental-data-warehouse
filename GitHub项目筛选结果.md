# GitHub 项目筛选结果

筛选时间：2026-03-23

## 筛选条件

- 项目类型为离线数仓或离线大数据分析
- 核心链路尽量包含 HDFS、MapReduce、Hive、Sqoop
- 不优先考虑 Flume、HBase、Azkaban
- 除采集外尽量避免 Python 参与主处理流程
- 不优先考虑 Spring Boot + Vue 形式的网站项目

## 结论摘要

当前在 GitHub 上没有找到一个和需求完全一致、并且主题也接近“北京娱乐方式”的现成项目。现有仓库大致分为三类：

- 第一类是学习笔记或技术样例仓库，组件齐全，但不是完整业务项目。
- 第二类是课程设计或旧项目仓库，带有 MapReduce、Hive、SQL、可视化代码，能够作为技术实现参考。
- 第三类是“毕业设计分享”类型仓库，标题看起来相关，但大多混入了 Spring Boot、Flask、Spark、HBase、Azkaban 等不符合当前约束的内容。

## 推荐优先看

### 1. ShadowLim/course_design

链接：
https://github.com/ShadowLim/course_design

推荐原因：

- 这是一个大数据课程设计合集，不是单一项目。
- 其中 `ke_house` 子项目目录中可以看到真实的 `MapReduce` 代码、`hql.sql`、`可视化.sql` 和 `Visualization` 目录。
- `nowcoder_job` 子项目中可以看到 `NCJob-MR`、`shell`、`HBaseImport`、`NCJob-Web&JDBC` 等目录，说明仓库里确实有 MR 和可视化相关代码。

适合参考的部分：

- MapReduce 驱动类和业务统计写法
- SQL 和 HQL 的组织方式
- 图表页面拆分思路

不符合当前需求的地方：

- `nowcoder_job` 明确包含 HBase
- 主题不是北京娱乐方式
- Sqoop 痕迹不明显，需要我们自己补齐

综合评价：
最像真实学生项目，适合拿来参考目录结构和 MR 代码风格，但不能直接照搬。

### 2. heibaiying/BigData-Notes

链接：
https://github.com/heibaiying/BigData-Notes

推荐原因：

- 这是成熟的大数据学习仓库。
- 仓库中有 `code/Hadoop/hdfs-java-api` 和 `code/Hadoop/hadoop-word-count` 两个直接可用的 Java 示例。
- 文档里还覆盖了 Hive、Sqoop、HDFS Java API 等内容。

适合参考的部分：

- HDFS Java API 上传和文件操作
- MapReduce 项目 Maven 结构
- Hadoop、Hive、Sqoop 的基础配置和命令说明

不符合当前需求的地方：

- 它是学习仓库，不是完整业务项目
- 没有现成的“离线数仓 + BI 大屏”业务闭环

综合评价：
最适合作为底层技术实现参考，特别适合补 HDFS Java API 和 MapReduce 代码。

### 3. mahua06051998/Project-Haoop

链接：
https://github.com/mahua06051998/Project-Haoop

推荐原因：

- README 明确写了五个子项目，其中包括 `Map-Reduce`、`Sqoop Task: Loading Data from RDBMS to HDFS`、`Stocks Analysis using Hive`。
- 它更像一个 Hadoop 生态的练手项目集合。

适合参考的部分：

- Sqoop -> HDFS -> Hive 的思路
- 将不同组件拆成小实验的组织方式

不符合当前需求的地方：

- 不是完整主题项目
- 没有 BI 大屏
- 项目较老

综合评价：
适合参考技术链路，不适合直接作为毕设底稿。

## 可作为补充样例

### 4. AmanpreetSingh-GitHub/Hadoop-MapReduce-Sqoop-Pig-Hive-Samples

链接：
https://github.com/AmanpreetSingh-GitHub/Hadoop-MapReduce-Sqoop-Pig-Hive-Samples

特点：

- 有多组 MapReduce Java 示例
- 有 `Sqoop_Pig_Hive_Project1` 目录和 `sqooping.sh`
- 更像组件样例库

用途：
适合补 MapReduce、Sqoop、Hive 相关小样例，不适合直接做项目骨架。

### 5. RonKG/Data-Engineering-Hadoop

链接：
https://github.com/RonKG/Data-Engineering-Hadoop

特点：

- 仓库树里能看到 `Sqoop Job Scripts` 和 `Hive Scripts`
- 有从 RDBMS 到 HDFS 再到 Hive 的 ETL 思路

问题：

- 依赖 Oozie
- 不强调 MapReduce 清洗
- 偏数据工程演示，不是毕设型可视化项目

用途：
适合参考 Hive 脚本和 Sqoop 脚本写法。

## 不建议直接采用

### lazy-apple/BigData_Long

链接：
https://github.com/lazy-apple/BigData_Long

原因：

- README 写明用了 Python、Flask、Echarts、Flume
- 目录里能看到 `pydemo`、`EchartsDemo`、`MRDemo`
- 架构接近“爬虫 + MR + 图表”，但不符合“除采集外全部 Java、不要 Flume、不要前端网站”的约束

### ZLiang0913/bs

链接：
https://github.com/ZLiang0913/bs

原因：

- README 写明使用 HBase 或 JSON 存储，再用 Java + ECharts 展示
- 不符合“不要 HBase”的要求

### bys-eric-he/com-hadoop-bigdata-demo

链接：
https://github.com/bys-eric-he/com-hadoop-bigdata-demo

原因：

- 明确包含 Spring Boot、HBase、Azkaban、Kafka
- 超出当前项目边界，容易把项目做复杂

### junian455/BigDataCustomerAnalyse

链接：
https://github.com/junian455/BigDataCustomerAnalyse

原因：

- README 明确为 `SpringBoot + Hive + Sqoop + MySql + Echarts`
- 偏网站式可视化分析，不符合“只做 BI 面板、不做前端网站”的约束

## 建议结论

如果目的是找一个完全现成、可以直接拿来改成“北京娱乐方式离线数仓”的 GitHub 项目，目前没有找到真正完全满足条件的仓库。

如果目的是快速拼出一个符合要求的项目骨架，建议采用下面的组合方式：

- 用 `heibaiying/BigData-Notes` 参考 HDFS Java API 和 MapReduce 工程结构
- 用 `ShadowLim/course_design` 参考真实课程设计的 MR 指标、HQL 和可视化组织方式
- 用 `Project-Haoop` 或 `RonKG/Data-Engineering-Hadoop` 参考 Sqoop/Hive/HDFS 的脚本链路

这样拼出来的方案比直接抄一个不符合约束的仓库更稳，也更容易和我们当前这份项目文档保持一致。
