# 基于hadoop的北京文娱数仓设计与可视化

## 项目背景

随着北京文化娱乐产业的快速发展，各类文娱活动（如电影、演出、展览、体育赛事等）的数量和种类呈现爆发式增长。文化消费已成为北京居民日常生活的重要组成部分，同时文旅融合政策的推进也吸引了大量外地游客参与北京的文化娱乐活动。然而，当前北京市文娱数据分散在各个平台（如票务系统、社交网络、政府开放数据等），缺乏统一的整合与分析平台，导致行业管理者、企业决策者以及消费者难以全面把握市场动态。

该项目的实施将有助于提升北京市文娱行业的数字化管理水平，推动数据驱动的精细化运营。

## 项目分工

马继宇：表的采集、ETL、数据分析、可视化

石庭玮：表的采集、ETL、数据分析、可视化

吕浩：体育运动场馆表的采集、ETL、数据分析、可视化

王皓：表的采集、ETL、数据分析、可视化

张宏阳：表的采集、ETL、数据分析、可视化

## 项目技术架构

### 技术栈

#### Hadoop

Hadoop是基于分布式存储（HDFS）和计算（MapReduce/YARN）的框架，支持海量数据的分布式处理，具备高容错性和可扩展性。作为大数据架构的底层基石，它提供存储和计算资源的统一管理，支撑Hive、HBase等组件的运行。通过分片和冗余存储保障数据可靠性，利用并行计算加速批处理任务，是架构中数据处理的"基础设施"。其生态整合能力无可替代，其他组件无法脱离Hadoop的分布式环境独立运行。在技术架构中，Hadoop负责数据的分布式存储与批处理，确保大规模数据的可靠性和高效性。

#### Hive

Hive是基于Hadoop的类SQL查询引擎，提供结构化数据的批量分析能力，支持ETL（提取、转换、加载）任务。它将传统数据库的SQL语法引入Hadoop生态，降低用户学习成本，简化复杂数据查询。通过元数据存储（如MySQL）管理表结构，将SQL语句转换为MapReduce任务执行，适配复杂分析场景。在架构中，Hive充当数据仓库层，实现非技术人员对HDFS数据的高效查询与分析，弥补Hadoop原生接口的复杂性。其独特作用在于桥接Hadoop与传统数据处理需求，提升数据利用效率。

#### Hbase

HBase是基于Hadoop的分布式NoSQL数据库，支持实时读写和随机访问，专为高并发场景设计。它采用列式存储和分布式架构，通过RegionServer管理数据分片，与HDFS深度集成实现数据持久化。在架构中，HBase弥补了Hadoop在实时数据处理上的不足，满足低延迟查询需求，适用于物联网、日志分析等场景。其核心优势在于高吞吐量与弹性扩展，可处理PB级数据且无需预定义模式，与其他组件（如Flume）协同实现流数据的实时存储与分析。

#### Flume

Flume是一个高可用的日志采集、聚合和传输系统，支持数据从多种源（如日志文件、网络流）可靠地传输到HDFS或HBase。其核心特点包括分布式架构、数据容错（通过冗余和检查点机制）以及灵活的中间件配置（如拦截器、通道选择器）。在架构中，Flume负责实时或近实时数据的高效收集与传输，确保数据流的稳定性和低延迟，是数据进入Hadoop生态的第一道入口。其独特作用在于解决海量数据的可靠采集问题，兼容多源异构数据，降低数据接入复杂度。

#### Sqoop

Sqoop是关系型数据库与Hadoop之间的数据迁移工具，支持全量/增量数据导入导出，利用MapReduce实现高效并行处理。它通过事务机制保证数据一致性，支持Thrift服务器模式扩展功能，并兼容MySQL、Oracle等主流数据库。在架构中，Sqoop充当传统数据系统与Hadoop生态的桥梁，解决结构化数据迁移的性能与可靠性问题。其核心价值在于简化跨系统数据同步，确保数据在不同存储间的无缝流转，是构建混合数据架构的关键组件。

#### Azkaban

Azkaban是一个可视化工作流调度系统，用于编排Hadoop任务（如Hive、MapReduce）的依赖关系和执行流程。它通过DAG（有向无环图）定义任务依赖，提供Web界面监控任务状态，并支持容错和资源管理。在架构中，Azkaban负责自动化复杂数据处理流程（如ETL、批处理作业），减少人工干预，提升任务执行的可靠性和可维护性。其独特作用在于简化任务编排，整合多组件协作，是实现端到端大数据流水线的核心调度工具。

### 架构图

![](media/image1.jpeg){width="5.768055555555556in" height="4.225in"}

## 项目总体设计

### 数据处理流程图

![](media/image2.jpeg){width="5.768055555555556in" height="3.24375in"}

## Docker 一键部署（crawler）

当前仓库可直接一键容器化运行的模块是 `crawler/`。这部分负责离线采集原始数据，并为后续 Java / HDFS / MapReduce / Hive 主链路提供输入文件。

需要明确：

- 当前 Docker 化范围仅覆盖 `crawler`
- 不代表整条 Hadoop / Hive / Sqoop / MySQL / BI 链路已经被整体容器化
- Docker 在本项目中主要用于演示、交付和快速启动，不作为答辩主线

### 构建镜像

在仓库根目录执行：

```bash
docker compose build
```

### 默认一键运行

在仓库根目录执行：

```bash
docker compose up
```

默认会运行：

```bash
python src/main.py scenic --sample 10
```

### 输出目录

容器内目录与宿主机目录已做挂载：

- `/app/data` -> `./crawler/data`
- `/app/logs` -> `./crawler/logs`

因此运行后生成的数据仍会保存在本地项目目录：

- `crawler/data/export/`
- `crawler/data/interim/links/`

### 切换采集类别

可通过覆盖命令运行其他类别，例如：

```bash
docker compose run --rm crawler python src/main.py scenic --sample 5
docker compose run --rm crawler python src/main.py show --sample 5
docker compose run --rm crawler python src/main.py ktv --sample 5
```

当前支持的类别与现有 CLI 保持一致：

- `scenic`
- `show`
- `ktv`
- `movie`
- `sport`

## 项目实现

### 数据采集

#### 爬虫工具使用\--八爪鱼

##### 日志数据

**北京景点表（马继宇）**

**电影表（石庭玮）**

**北京体育运动场馆表（吕浩）**

**演出情况表（王皓）**

**KTV表（张宏阳）**

##### 业务数据

1.  **景点**

> Mysql建库、建表
>
> CREATE DATABASE IF NOT EXISTS \`jingdian\` DEFAULT CHARACTER SET utf8;
>
> USE \`jingdian\`;
>
> DROP TABLE IF EXISTS \`scenic\`;
>
> CREATE TABLE \`scenic\`(
>
> \`id\` BIGINT(20) NOT NULL AUTO_INCREMENT,
>
> \`place\` VARCHAR(1000) DEFAULT NULL,
>
> \`introduction\` VARCHAR(1000) DEFAULT NULL,
>
> \`phone\` VARCHAR(2000) DEFAULT NULL,
>
> \`best time to visit\` VARCHAR(1000) DEFAULT NULL,
>
> \`recommended length of visit\` VARCHAR(1000) DEFAULT NULL,
>
> \`price\` VARCHAR(4000) DEFAULT NULL,
>
> \`open time\` VARCHAR(1000) DEFAULT NULL,
>
> \`created_time\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
>
> \`updated_time\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE
> CURRENT_TIMESTAMP,
>
> PRIMARY KEY (\`id\`)
>
> ) ENGINE=INNODB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

![](media/image3.png){width="5.768055555555556in"
height="2.897222222222222in"}

![](media/image4.png){width="5.768055555555556in"
height="4.665972222222222in"}

![](media/image5.png){width="5.768055555555556in"
height="1.0763888888888888in"}

2.  **电影**

> Mysql建库、建表
>
> CREATE DATABASE IF NOT EXISTS \`wenyu\` DEFAULT CHARACTER SET utf8;
>
> CREATE TABLE \`sport1\`(
>
> \`id\` BIGINT(20) NOT NULL AUTO_INCREMENT,
>
> \`name\` VARCHAR(1000) DEFAULT NULL,
>
> \`score\` VARCHAR(1000) DEFAULT NULL,
>
> \`type\` VARCHAR(1000) DEFAULT NULL,
>
> \`country\` VARCHAR(1000) DEFAULT NULL,
>
> \`director\` VARCHAR(1000) DEFAULT NULL,
>
> \`star\` VARCHAR(1000) DEFAULT NULL,
>
> \`synopsis\` VARCHAR(1000) DEFAULT NULL
>
> PRIMARY KEY (\`id\`)
>
> )ENGINE=INNODB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

![](media/image6.png){width="5.759027777777778in"
height="2.7868055555555555in"}

![](media/image7.png){width="5.759027777777778in"
height="5.777777777777778in"}

![](media/image8.png){width="5.759027777777778in" height="2.01875in"}

3.  **体育运动场馆**

Mysql建库、建表

CREATE DATABASE IF NOT EXISTS \`xiangmu1\` DEFAULT CHARACTER SET utf8;

CREATE TABLE \`sport1\`(

\`id\` BIGINT(20) NOT NULL AUTO_INCREMENT,

\`type\` VARCHAR(255) DEFAULT NULL,

\`name\` VARCHAR(600) DEFAULT NULL,

\`address\` VARCHAR(600) DEFAULT NULL,

\`pictures_linking\` VARCHAR(600) DEFAULT NULL,

\`score\` VARCHAR(600) DEFAULT NULL,

\`evaluate\` VARCHAR(800) DEFAULT NULL,

\`per_capita\` VARCHAR(800) DEFAULT NULL,

\`city\` VARCHAR(800) DEFAULT NULL,

PRIMARY KEY (\`id\`)

)ENGINE=INNODB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

![](media/image9.png){width="5.768055555555556in" height="2.825in"}

![](media/image10.png){width="5.826087051618548in"
height="5.721202974628172in"}

![](media/image11.png){width="5.797551399825021in"
height="3.2163637357830273in"}

4.  **演出情况**

CREATE DATABASE IF NOT EXISTS \`travel\` DEFAULT CHARACTER SET utf8;

USE \`travel\`;

DROP TABLE IF EXISTS \`yanchu\`;

CREATE TABLE \`yanchu\` (

\`id\` BIGINT(20) NOT NULL AUTO_INCREMENT,

\`name\` VARCHAR(255) DEFAULT NULL,

\`time\` VARCHAR(255) DEFAULT NULL,

\`place\` VARCHAR(255) DEFAULT NULL,

\`price\` VARCHAR(255) DEFAULT NULL,

\`cond\` VARCHAR(255) DEFAULT NULL,

\`picture\` VARCHAR(255) DEFAULT NULL,

\`created_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,

\`updated_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE
CURRENT_TIMESTAMP,

PRIMARY KEY (\`id\`)

) **ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT
CHARSET=utf8;**![](media/image12.png){width="5.766175634295713in"
height="4.705265748031496in"}
![](media/image13.png){width="5.773997156605424in"
height="5.880232939632546in"}![](media/image14.png){width="5.759722222222222in"
height="3.2583333333333333in"}

5.  **KTV**

Mysql建库、建表

CREATE DATABASE IF NOT EXISTS \`xiangmu1\` DEFAULT CHARACTER SET utf8;

CREATE TABLE \`ktv.ktv_lzo\`(

\`name\` VARCHAR(600) DEFAULT NULL,

\`address\` VARCHAR(600) DEFAULT NULL,

\`comment\` VARCHAR(600) DEFAULT NULL,

\`consume\` VARCHAR(600) DEFAULT NULL,

\`service \` VARCHAR(800) DEFAULT NULL,

\`en\` VARCHAR(800) DEFAULT NULL,

\`cost\` VARCHAR(800) DEFAULT NULL,

PRIMARY KEY (\`id\`)

)ENGINE=INNODB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8;

![](media/image15.png){width="5.75625in" height="2.3555555555555556in"}

![](media/image16.png){width="5.768055555555556in"
height="2.6256944444444446in"}

#### HDFS的javaApi上传日志文件

日志文件统一存放在：**hdfs://node-1:8020/data/logs**

**编写代码**

> **景点**

####### 编写代码：

> package cn.zpark.hdfs;\
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.http.util.Args;\
> import org.junit.After;\
> import org.junit.Before;\
> import org.junit.Test;\
> import java.io.IOException;\
> import java.net.URI;\
> import java.net.URISyntaxException;\
> import java.util.Map;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:HDFSClient\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class HDFSClient2 {\
> static URI uri;\
> static Configuration configuration;\
> static FileSystem fileSystem;\
> static {\
> try {\
> uri = new URI(\"hdfs://node-1:8020\");\
> configuration = new Configuration();\
> } catch (URISyntaxException e) {\
> e.printStackTrace();\
> }\
> }\
> public static void init() throws Exception{\
> // TODO :初始化 FileSystem\
> fileSystem = FileSystem.newInstance(uri,configuration, \"zpark\");\
> }\
> public static void close() throws Exception{\
> // TODO : 关闭连接\
> if (fileSystem != null) {\
> fileSystem.close();\
> }\
> }\
> public static void put(Path input,Path output) throws Exception{\
> fileSystem.copyFromLocalFile(input, output);\
> System.out.println(\"长传成功\");\
> }\
> public static void sercive(Path input,Path output) throws Exception{\
> // input : Linux output : HDFS\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> if (fileSystem.exists(output)){\
> //上传\
> put(input, output);\
> }else {\
> //创建目录\
> boolean flag = fileSystem.mkdirs(output);\
> if (flag){\
> //上传\
> put(input,output);\
> }else {\
> System.out.println(\"上传失败!\");\
> }\
> }\
> }\
> public static void main(String\[\] args) throws Exception {\
> init();\
> if (args.length == 0) {\
> System.out.println(\"请提供参数！\");\
> return;\
> }\
> Path input = new Path(args\[0\]);\
> Path output = new Path(args\[1\]);\
> sercive(input,output);\
> close();\
> }\
> }

####### 打包运行：

![](media/image17.png){width="5.768055555555556in"
height="2.8444444444444446in"}

####### 运行命令：

####### 采集上的数据：

![](media/image18.png){width="5.768055555555556in" height="1.25625in"}

> **电影**

####### 编写代码：

> package cn.zpark.hdfs;\
> \
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.hadoop.hdfs.DFSUtil;\
> import org.apache.hadoop.hdfs.server.namenode.FsImageProto;\
> import org.junit.Test;\
> \
> import java.io.File;\
> import java.io.IOException;\
> import java.net.URI;\
> import java.net.URISyntaxException;\
> \
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:HDFSClient2\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class HDFSClient2 {\
> static URI uri;\
> static Configuration configuration;\
> static FileSystem fileSystem;\
> static{\
> try {\
> uri = new URI(\"hdfs://node-1:8020\");\
> configuration = new Configuration();\
> } catch (URISyntaxException e) {\
> e.printStackTrace();\
> }\
> }\
> public static void init() throws Exception{\
> // TODO : 初始化 FileSystem\
> fileSystem = FileSystem.newInstance(uri,configuration, \"zpark\");\
> }\
> public static void close() throws Exception{\
> // TODO : 关闭链接\
> if (fileSystem != null) {\
> fileSystem.close();\
> }\
> }\
> public static void put(Path input,Path output) throws Exception{\
> fileSystem.copyFromLocalFile(input, output);\
> System.out.println(\"上传成功\");\
> }\
> public static void service(Path input,Path output) throws Exception {\
> // input : Linux output:HDFS\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> if (fileSystem.exists(output)) {\
> // 上传\
> put(input, output);\
> }else{\
> //创建目录\
> boolean flag = fileSystem.mkdirs(output);\
> if (flag) {\
> // 上传\
> put(input, output);\
> }else {\
> System.out.println(\"上传失败！\");\
> }\
> }\
> }\
> public static void main(String\[\] args) throws Exception {\
> init();\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> Path input = new Path(args\[0\]);\
> Path output = new Path(args\[1\]);\
> service(input,output);\
> close();\
> }\
> }

####### 打包运行：

![](media/image17.png){width="5.768055555555556in"
height="2.8444444444444446in"}

####### 运行命令：

> hadoop jar /home/zpark/tmp/HDFS/example-hdfs-1.0-SNAPSHOT.jar
> cn.zpark.hadfs.HDFSClient2 /home/zpark/tmp/HDFS/dianying_new.csv
> /flume-datas/taildir

![](media/image19.png){width="2.685416666666667in"
height="1.3055555555555556in"}

####### 采集上的数据：

![](media/image20.png){width="5.759027777777778in"
height="0.9076388888888889in"}

> **体育运动场馆**

####### 编写代码：

package cn.zpark.hadfs;\
\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.junit.After;\
import org.junit.Before;\
import org.junit.Test;\
\
import java.io.IOException;\
import java.net.URI;\
import java.net.URISyntaxException;\
\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:HDFSClient\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class HDFSClient2 {\
static URI uri;\
static Configuration configuration;\
static FileSystem fileSystem;\
static {\
try {\
uri=new URI(\"hdfs://node-1:8020\");\
configuration = new Configuration();\
} catch (URISyntaxException e) {\
e.printStackTrace();\
}\
}\
public static void Init() throws Exception{\
// TODO :初始化 FileSystem\
fileSystem = FileSystem.newInstance(uri,configuration, \"zpark\");\
}\
public static void close () throws Exception{\
//关闭连接\
if (fileSystem != null) {\
fileSystem.close();\
}\
}\
public static void put(Path input,Path output) throws Exception{\
fileSystem.copyFromLocalFile(input, output);\
System.out.println(\"上传成功\");\
}\
public static void service(Path input,Path output) throws Exception{\
//input:Linux output:HDFS\
// 判断 data/logs 目录是否存在 存在则上传，不存在则创建\
if (fileSystem.exists(output)){\
//上传\
put(input, output );\
}else {\
//创建目录\
boolean flag = fileSystem.mkdirs(output);\
if(flag){\
//上传\
put(input, output);\
}else{\
System.out.println(\"上传失败！\");\
}\
}\
}\
public static void main(String\[\] args) throws Exception {\
Init();\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
service(input,output);\
close();\
}\
}

####### 打包运行：

![](media/image17.png){width="5.768055555555556in"
height="2.8444444444444446in"}

####### 运行命令：

hadoop jar /home/zpark/tmp/HDFS/example-hdfs-1.0-SNAPSHOT.jar
cn.zpark.hadfs.HDFSClient2 /home/zpark/tmp/HDFS/sport1_new.csv
/flume-datas/taildir

![](media/image21.png){width="2.7640310586176726in"
height="2.1737226596675416in"}

####### 采集上的数![](media/image22.png){width="5.768055555555556in" height="1.8479166666666667in"} 

> **演出情况**

####### 编写代码：

> public class HDFSClient2 {\
> static URI uri;\
> static Logger logger;\
> static FileSystem fileSystem ;\
> static Configuration configuration;\
> static {\
> try {\
> uri=new URI(\"hdfs://node-1:8020\");\
> configuration = new Configuration();\
> logger = Logger.getLogger(HDFSClient2.class);\
> } catch (URISyntaxException e) {\
> e.printStackTrace();\
> }\
> }\
> /\*\*\
> \* 功能描述： 初始化 FilesSystem、Logger\
> \*\*/\
> public static void init() throws Exception{\
> // TODO : 初始化 FilesSystem\
> fileSystem = FileSystem.newInstance(uri,configuration, \"zpark\");\
> logger.info(\"初始化成功！\");\
> }\
> /\*\*\
> \* 功能描述：\
> \*\*/\
> public static void close() throws Exception{\
> // TODO : 关闭连接\
> if (fileSystem != null) {\
> fileSystem.close();\
> }\
> if (logger!=null){\
> logger=null;\
> }\
> }\
> /\*\*\
> \* 功能描述：上传文件\
> \*\*/\
> public static void put(Path input,Path output) throws Exception{\
> fileSystem.copyFromLocalFile(input, output);\
> logger.info(\"上传成功\");\
> }\
> /\*\*\
> \* 功能描述： 上传文件业务\
> \*\*/\
> public static void service(Path input,Path output) throws Exception{\
> // input : Linux output:HDFS\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> if (fileSystem.exists(output)){\
> // 上传\
> put(input, output);\
> }else {\
> //创建目录\
> boolean flag = fileSystem.mkdirs(output);\
> if (flag){\
> // 上传\
> put(input, output);\
> }else {\
> logger.info(\"上传失败！\");\
> }\
> }\
> }\
> public static void main(String\[\] args) throws Exception {\
> init();\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> Path input = new Path(args\[0\]);\
> Path output = new Path(args\[1\]);\
> service(input,output);\
> close();\
> }\
> }

####### 打包运行![](media/image23.png){width="5.767361111111111in" height="3.1013888888888888in"} 

####### 运行命令：

> hadoop jar example-hdfs-1.0-SNAPSHOT.jar cn.zpark.hdfs.HDFSClient
> /home/zpark/tmp/猫眼演出new.csv /data/logs/

####### 采集上的数据![](media/image24.png){width="5.768055555555556in" height="1.60625in"}

> **KTV**

####### 编写代码：

> package cn.zpark.hdfs;\
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.http.util.Args;\
> import org.junit.After;\
> import org.junit.Before;\
> import org.junit.Test;\
> import java.io.IOException;\
> import java.net.URI;\
> import java.net.URISyntaxException;\
> import java.util.Map;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:HDFSClient\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class HDFSClient2 {\
> static URI uri;\
> static Configuration configuration;\
> static FileSystem fileSystem;\
> static {\
> try {\
> uri = new URI(\"hdfs://node-1:8020\");\
> configuration = new Configuration();\
> } catch (URISyntaxException e) {\
> e.printStackTrace();\
> }\
> }\
> public static void init() throws Exception{\
> // TODO :初始化 FileSystem\
> fileSystem = FileSystem.newInstance(uri,configuration, \"zpark\");\
> }\
> public static void close() throws Exception{\
> // TODO : 关闭连接\
> if (fileSystem != null) {\
> fileSystem.close();\
> }\
> }\
> public static void put(Path input,Path output) throws Exception{\
> fileSystem.copyFromLocalFile(input, output);\
> System.out.println(\"长传成功\");\
> }\
> public static void sercive(Path input,Path output) throws Exception{\
> // input : Linux output : HDFS\
> // 判断 /data/logs 目录是否存在 存在则上传 不存在创建\
> if (fileSystem.exists(output)){\
> //上传\
> put(input, output);\
> }else {\
> //创建目录\
> boolean flag = fileSystem.mkdirs(output);\
> if (flag){\
> //上传\
> put(input,output);\
> }else {\
> System.out.println(\"上传失败!\");\
> }\
> }\
> }\
> public static void main(String\[\] args) throws Exception {\
> init();\
> if (args.length == 0) {\
> System.out.println(\"请提供参数！\");\
> return;\
> }\
> Path input = new Path(args\[0\]);\
> Path output = new Path(args\[1\]);\
> sercive(input,output);\
> close();\
> }\
> }

####### 打包运行：

![](media/image17.png){width="5.768055555555556in"
height="2.8444444444444446in"}

####### 运行命令：

hadoop jar /home/zpark/tmp/HDFS/example-hdfs-1.0-SNAPSHOT.jar
cn.zpark.hadfs.HDFSClient2 /home/zpark/tmp/HDFS/ktv2-new.csv
/flume-datas/taildir

![](media/image25.png){width="3.858667979002625in"
height="0.32502843394575676in"}

####### 采集上的数据：

![](media/image26.png){width="5.768055555555556in" height="1.26875in"}

### 数据清洗

#### 清洗代码

##### 景点表数据清洗

###### CleanBean编写

> package cn.zpark.mr.project;\
> import org.apache.hadoop.io.Writable;\
> import java.io.DataInput;\
> import java.io.DataOutput;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanBean\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanBean implements Writable {\
> private String scenery_spot;\
> private String brief_introduction;\
> private String phone;\
> private String best_time_to_visit;\
> private String recommended_length_of_visit;\
> private String price;\
> private String open_time;\
> private String introduction;\
> private String address;\
> private String route;\
> public CleanBean() {\
> }\
> public CleanBean(String scenery_spot, String brief_introduction,
> String phone, String best_time_to_visit, String
> recommended_length_of_visit, String price, String open_time, String
> introduction, String address, String route) {\
> this.scenery_spot = scenery_spot;\
> this.brief_introduction = brief_introduction;\
> this.phone = phone;\
> this.best_time_to_visit = best_time_to_visit;\
> this.recommended_length_of_visit = recommended_length_of_visit;\
> this.price = price;\
> this.open_time = open_time;\
> this.introduction = introduction;\
> this.address = address;\
> this.route = route;\
> }\
> public String getScenery_spot() {\
> return scenery_spot;\
> }\
> public void setScenery_spot(String scenery_spot) {\
> this.scenery_spot = scenery_spot;\
> }\
> public String getBrief_introduction() {\
> return brief_introduction;\
> }\
> public void setBrief_introduction(String brief_introduction) {\
> this.brief_introduction = brief_introduction;\
> }\
> public String getPhone() {\
> return phone;\
> }\
> public void setPhone(String phone) {\
> this.phone = phone;\
> }\
> public String getBest_time_to_visit() {\
> return best_time_to_visit;\
> }\
> public void setBest_time_to_visit(String best_time_to_visit) {\
> this.best_time_to_visit = best_time_to_visit;\
> }\
> public String getRecommended_length_of_visit() {\
> return recommended_length_of_visit;\
> }\
> public void setRecommended_length_of_visit(String
> recommended_length_of_visit) {\
> this.recommended_length_of_visit = recommended_length_of_visit;\
> }\
> public String getPrice() {\
> return price;\
> }\
> public void setPrice(String price) {\
> this.price = price;\
> }\
> public String getOpen_time() {\
> return open_time;\
> }\
> public void setOpen_time(String open_time) {\
> this.open_time = open_time;\
> }\
> public String getIntroduction() {\
> return introduction;\
> }\
> public void setIntroduction(String introduction) {\
> this.introduction = introduction;\
> }\
> public String getAddress() {\
> return address;\
> }\
> public void setAddress(String address) {\
> this.address = address;\
> }\
> public String getRoute() {\
> return route;\
> }\
> public void setRoute(String route) {\
> this.route = route;\
> }\
> \@Override\
> public void write(DataOutput out) throws IOException {\
> out.writeUTF(scenery_spot);\
> out.writeUTF(brief_introduction);\
> out.writeUTF(phone);\
> out.writeUTF(best_time_to_visit);\
> out.writeUTF(recommended_length_of_visit);\
> out.writeUTF(price);\
> out.writeUTF(open_time);\
> out.writeUTF(introduction);\
> out.writeUTF(address);\
> out.writeUTF(route);\
> }\
> \@Override\
> public void readFields(DataInput in) throws IOException {\
> scenery_spot = in.readUTF();\
> brief_introduction = in.readUTF();\
> phone = in.readUTF();\
> best_time_to_visit = in.readUTF();\
> recommended_length_of_visit = in.readUTF();\
> price = in.readUTF();\
> open_time = in.readUTF();\
> introduction = in.readUTF();\
> address = in.readUTF();\
> route = in.readUTF();\
> }\
> \@Override\
> public String toString() {\
> return\
> scenery_spot + \'\\t\' +\
> brief_introduction + \'\\t\' +\
> phone + \'\\t\' +\
> best_time_to_visit + \'\\t\' +\
> recommended_length_of_visit + \'\\t\' +\
> price + \'\\t\' +\
> open_time + \'\\t\' +\
> introduction + \'\\t\' +\
> address + \'\\t\' +\
> route;\
> }\
> }

###### Mapper编写

> package cn.zpark.mr.project;\
> import cn.zpark.mr.project.CleanBean;\
> import cn.zpark.mr.project.CleanUtils;\
> import com.opencsv.CSVParser;\
> import com.opencsv.CSVParserBuilder;\
> import org.apache.avro.Schema;\
> import org.apache.avro.generic.GenericRecord;\
> import org.apache.hadoop.io.LongWritable;\
> import org.apache.hadoop.io.Text;\
> import org.apache.hadoop.mapreduce.Mapper;\
> import java.io.IOException;\
> public class CleanMapper extends Mapper\<LongWritable, Text, Void,
> GenericRecord\> {\
> CSVParser build = null;\
> Schema schema = null;\
> \@Override\
> protected void setup(Mapper\<LongWritable, Text, Void,
> GenericRecord\>.Context context) throws IOException,
> InterruptedException {\
> build = new CSVParserBuilder()\
> .withSeparator(\',\')\
> .withIgnoreQuotations(true)\
> .build();\
> schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
> System.out.println(\"Mapper Schema loaded: \" + schema); // 添加日志\
> }\
> \@Override\
> protected void map(LongWritable key, Text value, Mapper\<LongWritable,
> Text, Void, GenericRecord\>.Context context) throws IOException,
> InterruptedException {\
> String line = value.toString();\
> //String\[\] data = line.split(\",\");\
> String\[\] data = build.parseLine(line);\
> System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
> CleanBean bean=parse(data);\
> GenericRecord record = CleanUtils.getRecord(schema,bean);\
> if (bean!=null)\
> context.write(null, record);\
> }\
> /\*\*\
> \* 功能描述：数据清洗方法\
> \* 1.data\[1\] 无须处理\
> \* 2.data\[2\] 空 暂无\
> \* 3.data\[3\] 空、无 暂无\
> \* 4.data\[4\] 空 -1\
> \* 5.data\[5\] 空 -1，电话号清洗为-2\
> \* 6.data\[6\] 空 -1，去除门票价格中的开放时间，替换字符串 门票价格：
> 票价：\
> \* 7.data\[7\] 空 -1，替换字符串 开放时间：\
> \* 8.data\[8\] 无须处理\
> \* 9.data\[9\] 空 -1\
> \* 10.data\[10\] 空 暂无数据\
> \*\*/\
> private CleanBean parse(String\[\] data) {\
> // 检查数组长度是否足够\
> if (data == null \|\| data.length \< 11) {\
> // 如果数据不足11列，可以返回null或创建一个包含默认值的CleanBean\
> // 这里选择返回null，在map方法中会跳过这个记录\
> System.out.println(\"数据列数不足，跳过此记录。实际列数: \" + (data !=
> null ? data.length : \"null\"));\
> return null;\
> }\
> //bean为空时不写\
> CleanBean bean = new CleanBean(\
> data\[1\],\
> parseBrief_introduction(data\[2\]),\
> parsePhone(data\[3\]),\
> parseBest_time_to_visit(data\[4\]),\
> parseRecommended_length_of_visit(data\[5\]),\
> parsePrice(data\[6\]),\
> parseOpen_time(data\[7\]),\
> data\[8\],\
> parseAddress(data\[9\]),\
> parseRoute(data\[10\])\
> );\
> return bean;\
> }\
> private String parseRoute(String datum) {\
> if(datum == null \|\| datum.length()\<=0){\
> return \"暂无数据\";\
> }\
> System.out.println(\"data.length:\"+datum.length());\
> return datum;\
> }\
> //将data\[10\]中的空值替换为暂无数据\
> private String parseAddress(String datum) {\
> if(datum == null \|\| datum.length()\<=0){\
> return \"-1\";\
> }\
> return datum;\
> }\
> //将data\[9\]中空值替换为-1\
> private String parseOpen_time(String datum) {\
> if(datum == null \|\| datum.length()\<=0){\
> return \"不详\";\
> }\
> if (datum.contains(\"开放时间： \")){\
> datum=datum.replace(\"开放时间：\", \"\");\
> }\
> return datum;\
> }\
> //将data\[7\]中空值替换为不详，替换字符串 开放时间：\
> private String parsePrice(String datum) {\
> if(datum == null \|\| datum.length()\<=0){\
> return \"不知\";\
> }\
> if(datum.contains(\"开放时间：\")){\
> return \"-3\";\
> }\
> if (datum.contains(\"门票价格：\")){\
> datum=datum.replace(\"门票价格：\", \"\");\
> }\
> if (datum.contains(\"票价：\")){\
> datum=datum.replace(\"票价：\", \"\");\
> }\
> return datum;\
> }\
> //将data\[6\]中的空值替换为不知，带有"开放时间："这五个字的整行字符串变为-3，替换字符串
> 门票价格： 票价：\
> private String parseRecommended_length_of_visit(String datum) {\
> String PHONE =
> \"\\\\+?\\\\d{1,4}?\[-.\\\\s\]?\\\\(?\\\\d{1,3}?\\\\)?\[-.\\\\s\]?\\\\d{1,4}\[-.\\\\s\]?\\\\d{1,4}\[-.\\\\s\]?\\\\d{1,9}\";\
> if(datum == null \|\| datum.length()\<=0){\
> return \"-1\";\
> }\
> String cleaned = datum.replaceAll(\"\[\^0-9+\]\", \"\");\
> if (cleaned.matches(PHONE)){\
> return \"error\";\
> }\
> return datum;\
> }\
> //将data\[5\]中的空值变为-1，电话号清洗为error\
> private String parseBest_time_to_visit(String datum) {\
> if (datum == null \|\| datum.length()\<=0){\
> return \"-1\";\
> }\
> return datum;\
> }\
> //将data\[4\]中的空值变成-1\
> private String parsePhone(String datum) {\
> if (datum.length()\<=0\|\|\"无\".equals(datum)){\
> return \"暂无\";\
> }\
> return datum;\
> }\
> //将data\[3\]中的空值和无变成暂无\
> private String parseBrief_introduction(String datum) {\
> if (datum.length()\<=0){\
> return \"暂无\";\
> }\
> return datum;\
> }\
> //将data\[2\]中的空值变成暂无\
> }

###### Driver编写

> package cn.zpark.mr.project;\
> import cn.zpark.mr.project.CleanUtils;\
> import org.apache.avro.Schema;\
> import org.apache.avro.generic.GenericRecord;\
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.hadoop.mapreduce.Job;\
> import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
> import org.apache.parquet.avro.AvroParquetOutputFormat;\
> import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
> public class CleanDriver {\
> public static void main(String\[\] args) throws Exception {\
> Configuration conf = new Configuration();\
> Job job = Job.getInstance();\
> job.setJarByClass(CleanDriver.class);\
> job.setMapperClass(CleanMapper.class);\
> job.setNumReduceTasks(0);\
> job.setMapOutputKeyClass(Void.class);\
> job.setMapOutputValueClass(GenericRecord.class);\
> //Path input = new
> Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\input_project\");\
> //Path output = new
> Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\output_project\");\
> Path input = new Path(args\[0\]);\
> Path output = new Path(args\[1\]);\
> // TODO : 设置输出类\
> job.setOutputFormatClass(AvroParquetOutputFormat.class);\
> // TODO : 设置压缩\
> AvroParquetOutputFormat.setCompression(job,
> CompressionCodecName.LZO);\
> AvroParquetOutputFormat.setCompressOutput(job, true);\
> // 设置Avro Schema\
> Schema schema = new
> Schema.Parser().parse(CleanUtils.getAvscStream());\
> AvroParquetOutputFormat.setSchema(job, schema);\
> FileInputFormat.setInputPaths(job, input);\
> AvroParquetOutputFormat.setOutputPath(job, output);\
> FileSystem fileSystem = output.getFileSystem(new Configuration());\
> if (fileSystem.exists(output)) fileSystem.delete(output, true);\
> boolean flag = job.waitForCompletion(true);\
> System.exit(flag ? 0 : -1);\
> }\
> }

##### 电影表数据清洗

###### CleanBean编写

> package cn.zpark.mr.project;\
> import org.apache.hadoop.io.Writable;\
> import java.io.DataInput;\
> import java.io.DataOutput;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanBean\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanBean implements Writable {\
> private String name;\
> private String score;\
> private String type;\
> private String country;\
> private String director;\
> private String star;\
> private String synopsis;\
> public CleanBean() {\
> }\
> public CleanBean(String name, String score, String type, String
> country, String director, String star, String synopsis) {\
> this.name = name;\
> this.score = score;\
> this.type = type;\
> this.country = country;\
> this.director = director;\
> this.star = star;\
> this.synopsis = synopsis;\
> }\
> public String getName() {\
> return name;\
> }\
> public void setName(String name) {\
> this.name = name;\
> }\
> public String getScore() {\
> return score;\
> }\
> public void setScore(String score) {\
> this.score = score;\
> }\
> public String getType() {\
> return type;\
> }\
> public void setType(String type) {\
> this.type = type;\
> }\
> public String getCountry() {\
> return country;\
> }\
> public void setCountry(String country) {\
> this.country = country;\
> }\
> public String getDirector() {\
> return director;\
> }\
> public void setDirector(String director) {\
> this.director = director;\
> }\
> public String getStar() {\
> return star;\
> }\
> public void setStar(String star) {\
> this.star = star;\
> }\
> public String getSynopsis() {\
> return synopsis;\
> }\
> public void setSynopsis(String synopsis) {\
> this.synopsis = synopsis;\
> }\
> \@Override\
> public void write(DataOutput Out) throws IOException {\
> Out.writeUTF(name);\
> Out.writeUTF(score);\
> Out.writeUTF(type);\
> Out.writeUTF(country);\
> Out.writeUTF(director);\
> Out.writeUTF(star);\
> Out.writeUTF(synopsis);\
> }\
> \@Override\
> public void readFields(DataInput In) throws IOException {\
> name = In.readUTF();\
> score = In.readUTF();\
> type = In.readUTF();\
> country = In.readUTF();\
> director = In.readUTF();\
> star = In.readUTF();\
> synopsis = In.readUTF();\
> }\
> \@Override\
> public String toString() {\
> return name + \'\\001\' +\
> score + \'\\001\' +\
> type + \'\\001\' +\
> country + \'\\001\'+\
> director + \'\\001\' +\
> star + \'\\001\' +\
> synopsis + \'\\001\';\
> }\
> }

###### Mapper编写

package cn.zpark.mr.project;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
\
import java.io.IOException;\
public class CleanMapper extends Mapper\<LongWritable, Text,
NullWritable, CleanBean\> {\
CSVParser build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
\@Override\
protected void map(LongWritable key, Text value,Mapper\<LongWritable,
Text, NullWritable, CleanBean\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
String\[\] data = line.split(\",\");\
CleanBean bean = parse(data);\
if (bean != null)\
context.write( NullWritable.get(),bean);\
}\
/\*\*\
\* 功能描述：数据清洗方法\
\* data\[0\] 无需处理\
\* data\[1\] 空 -1 按照 分 切分 \[0\]\
\* data\[2\] 替换字符串 类型:\
\* data\[3\] 替换字符串 国家/地区：\
\* data\[4\] 替换字符串 导演:\
\* data\[5\] 替换字符串 主演:\
\* data\[6\] 空 暂无\
\*\*/\
private CleanBean parse(String\[\] data) {\
return new CleanBean(\
data\[1\],// 原始名称字段\
parseScore(data\[2\]), // 处理评分字段\
parseType(data\[3\]), // 处理类型字段\
parseCountry(data\[4\]),// 处理国家/地区字段\
parseDirector(data\[5\]),// 处理导演字段\
parseStar(data\[6\]),// 处理主演字段\
parseSynopsis(data\[7\])// 处理简介字段\
);\
}\
/\*\*\
\* 处理评分字段\
\* 将格式从\"x.x分\"变为\"x.x\"（从\"分\"切割取第一部分）\
\*/\
private String parseScore(String datum) {\
if (datum.length()\<=0 ) {\
return \"-1\";\
}\
if (datum.contains(\"分\")){\
datum=datum.split(\"分\")\[0\];\
}\
return datum;\
}\
/\*\*\
\* 处理类型字段\
\* 移除\"类型:\"前缀\
\*/\
private String parseType(String datum) {\
if (datum.contains(\"类型:\"))\
datum=datum.replace(\"类型:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理国家/地区字段\
\* 移除\"国家/地区:\"前缀\
\*/\
private String parseCountry(String datum) {\
if (datum.contains(\"国家/地区:\"))\
datum=datum.replace(\"国家/地区:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理导演字段\
\* 移除\"导演:\"前缀\
\*/\
private String parseDirector(String datum) {\
if (datum.contains(\"导演:\"))\
datum=datum.replace(\"导演:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理主演字段\
\* 移除\"主演:\"前缀\
\*/\
private String parseStar(String datum) {\
if (datum.contains(\"主演:\"))\
datum=datum.replace(\"主演:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理空字段\
\* 空值处理为\"暂无\"\
\*/\
private String parseSynopsis(String datum) {\
if (datum.length() \<= 0) {\
return \"暂无\";\
}\
return datum;\
}\
}

###### Driver编写

> package cn.zpark.mr.project;\
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.hadoop.io.NullWritable;\
> import org.apache.hadoop.mapreduce.Job;\
> import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
> import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanBean\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanDriver {\
> public static void main(String\[\] args) throws Exception {\
> Job job = Job.getInstance();\
> job.setJarByClass(CleanDriver.class);\
> job.setMapperClass(CleanMapper.class);\
> job.setOutputKeyClass(CleanBean.class);\
> job.setOutputValueClass(NullWritable.class);\
> job.setNumReduceTasks(0);\
> Path input = new Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\input_project\");\
> Path output = new
> Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\output_project\");\
> FileSystem fileSystem = output.getFileSystem(new Configuration());\
> if (fileSystem.exists(output))fileSystem.delete(output, true);\
> FileInputFormat.setInputPaths(job, input);\
> FileOutputFormat.setOutputPath(job, output);\
> boolean flag = job.waitForCompletion(true);\
> System.exit(flag?0:-1);\
> }\
> }

##### 体育运动场馆数据清洗

###### CleanBean编写

> package project;\
> import org.apache.hadoop.io.Writable;\
> import java.io.DataInput;\
> import java.io.DataOutput;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanBean\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanBean implements Writable {\
> private String type;\
> private String name;\
> private String address;\
> private String score;\
> private String city;\
> public CleanBean() {\
> }\
> public CleanBean(String type, String name, String address, String
> score, String city ) {\
> this.type = type;\
> this.name = name;\
> this.address = address;\
> this.score = score;\
> this.city = city;\
> }\
> public String getType() {\
> return type;\
> }\
> public void setType(String type) {\
> this.type = type;\
> }\
> public String getName() {\
> return name;\
> }\
> public void setName(String name) {\
> this.name = name;\
> }\
> public String getAddress() {\
> return address;\
> }\
> public void setAddress(String address) {\
> this.address = address;\
> }\
> public String getScore() {\
> return score;\
> }\
> public void setScore(String score) {\
> this.score = score;\
> }\
> public String getCity() {\
> return city;\
> }\
> public void setCity(String city) {\
> this.city = city;\
> }\
> \@Override\
> public void write(DataOutput out) throws IOException {\
> out.writeUTF(type);\
> out.writeUTF(name);\
> out.writeUTF(address);\
> out.writeUTF(score);\
> out.writeUTF(city);\
> }\
> \@Override\
> public void readFields(DataInput in) throws IOException {\
> type = in.readUTF();\
> name = in.readUTF();\
> address = in.readUTF();\
> score = in.readUTF();\
> city = in.readUTF();\
> }\
> \@Override\
> public String toString() {\
> return\
> type + \'\\t\' +\
> name + \'\\t\' +\
> address + \'\\t\' +\
> score + \'\\t\' +\
> city ;\
> }\
> }

###### Mapper编写

> package project;\
> import com.opencsv.CSVParser;\
> import com.opencsv.CSVParserBuilder;\
> import org.apache.hadoop.io.LongWritable;\
> import org.apache.hadoop.io.NullWritable;\
> import org.apache.hadoop.io.Text;\
> import org.apache.hadoop.mapreduce.Mapper;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanMapper\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanMapper extends Mapper \<LongWritable, Text,
> NullWritable,CleanBean\>{\
> CSVParser build = new CSVParserBuilder()\
> .withSeparator(\',\')\
> .withIgnoreQuotations(true)\
> .build();\
> \@Override\
> protected void map(LongWritable key, Text value, Mapper\<LongWritable,
> Text, NullWritable, CleanBean\>.Context context) throws IOException,
> InterruptedException {\
> String line = value.toString();\
> //String\[\] data = line.split(\",\");\
> String\[\] data = build.parseLine(line);\
> System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
> CleanBean bean = parse(data);\
> if (bean!=null)\
> context.write(NullWritable.get(), bean);\
> }\
> /\*\*\
> \* 功能描述：数据清洗方法\
> \* 1.data\[1\] 无需处理\
> \* 1.data\[2\] 无需处理\
> \* 1.data\[3\] 无需处理\
> \* 1.data\[5\] 空 -1\
> \* 1.data\[8\] 无需处理\
> \*\*/\
> private CleanBean parse(String\[\] data) {\
> CleanBean bean = new CleanBean(\
> data\[1\], data\[2\], data\[3\],\
> parseScore(data\[5\]),\
> data\[8\]\
> );\
> return bean;\
> }\
> private String parseScore(String datum) {\
> if (datum.length()\<=0){\
> return \"-1\";\
> }\
> return datum;\
> }\
> }

###### Driver编写

> package project;\
> import org.apache.hadoop.conf.Configuration;\
> import org.apache.hadoop.fs.FileSystem;\
> import org.apache.hadoop.fs.Path;\
> import org.apache.hadoop.io.NullWritable;\
> import org.apache.hadoop.mapreduce.Job;\
> import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
> import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanDriver\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanDriver {\
> public static void main(String\[\] args) throws Exception{\
> Configuration conf = new Configuration();\
> //开启loz输出压缩
> conf.set(\"mapreduce.output.fileoutputformat.compress\",\"true\");
> conf.set(\"mapreduce.output.fileoutputformat.compress.codec\",\"com.hadoop.compression.lzo.LozpCodec\");\
> //压缩类型
> conf.set(\"mapreduce.output.fileoutputformat.compress.type\",\"BLOCK\");\
> Job job = Job.getInstance(conf);\
> job.setJarByClass(CleanDriver.class);\
> job.setMapperClass(CleanMapper.class);\
> job.setMapOutputKeyClass(NullWritable.class);\
> job.setMapOutputValueClass(CleanBean.class);\
> Path input = new
> Path(\"E:\\\\dev\\\\tmp\\\\clean\\\\input_project\");\
> Path output = new
> Path(\"E:\\\\dev\\\\tmp\\\\clean\\\\output_project\");\
> //Path input = new Path(args\[0\]);\
> //Path output = new Path(args\[1\]);\
> FileSystem fileSystem = output.getFileSystem(conf);\
> if (fileSystem.exists(output))fileSystem.delete(output, true);\
> FileInputFormat.setInputPaths(job, input);\
> FileOutputFormat.setOutputPath(job, output);\
> boolean flag = job.waitForCompletion(true);\
> System.exit(flag?0:-1);\
> }\
> }

##### 演出数据清洗

###### CleanBean编写

package cn.zpark.mr.projcet;

import org.apache.commons.compress.utils.ByteUtils;

import org.apache.hadoop.io.Writable;

import org.checkerframework.checker.units.qual.g;

import org.codehaus.jackson.xc.DataHandlerJsonDeserializer;

import java.io.DataInput;

import java.io.DataOutput;

import java.io.IOException;

import java.io.OutputStreamWriter;

/\*\*

\* \@Auther:BigData-aw

\* \@ClassName:CleanBean

\* \@功能描述:

\* \@Version:1.0

\*/

public class CleanBeanyc implements Writable {

private String name;

private String time;

private String place;

private String price;

private String cond;

public CleanBeanyc() {

}

public CleanBeanyc(String name, String time, String place, String price,
String cond) {

this.name = name;

this.time = time;

this.place = place;

this.price = price;

this.cond = cond;

}

public String getName() {

return name;

}

public void setName(String name) {

this.name = name;

}

public String getTime() {

return time;

}

public void setTime(String time) {

this.time = time;

}

public String getPlace() {

return place;

}

public void setPlace(String place) {

this.place = place;

}

public String getPrice() {

return price;

}

public void setPrice(String price) {

this.price = price;

}

public String getCond() {

return cond;

}

public void setCond(String cond) {

this.cond = cond;

}

\@Override

public void write(DataOutput dataOutput) throws IOException {

dataOutput.writeUTF(name);

dataOutput.writeUTF(time);

dataOutput.writeUTF(place);

dataOutput.writeUTF(price);

dataOutput.writeUTF(cond);

}

\@Override

public void readFields(DataInput dataInput) throws IOException {

name = dataInput.readUTF();

time = dataInput.readUTF();

place = dataInput.readUTF();

price = dataInput.readUTF();

cond = dataInput.readUTF();

}

\@Override

public String toString() {

return

name + \'\\t\' +

time + \'\\t\' +

place + \'\\t\' +

price + \'\\t\' +

cond;

}

}

###### Mapper编写

> package cn.zpark.mr.projcet;\
> import com.opencsv.CSVParser;\
> import com.opencsv.CSVParserBuilder;\
> import org.apache.hadoop.io.LongWritable;\
> import org.apache.hadoop.io.NullWritable;\
> import org.apache.hadoop.io.Text;\
> import org.apache.hadoop.mapreduce.Mapper;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanMapper\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanMapper extends Mapper\<LongWritable, Text,
> NullWritable,CleanBeanyc\> {\
> CSVParser build = new CSVParserBuilder()\
> .withSeparator(\',\')\
> .withIgnoreQuotations(true)\
> .build();\
> protected void map(LongWritable key, Text value, Mapper\<LongWritable,
> Text, NullWritable,CleanBeanyc\>.Context context) throws IOException,
> InterruptedException {\
> String line = value.toString();\
> //String\[\] data = line.split(\",\");\
> String\[\] data = build.parseLine(line);\
> if (data == null \|\| data.length \< 8) {\
> System.out.println(\"Invalid line (字段不足): \" + line + \", length:
> \" + (data == null? 0 : data.length));\
> return; // 跳过无效行\
> }
> System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
> CleanBeanyc bean=parse(data);\
> if (bean!=null)\
> context.write(NullWritable.get(), bean);\
> }\
> /\*\*\
> \* 功能描述：数据清洗方法\
> \* 1.data\[3\] 无须处理\
> \* 1.data\[4\] 无须处理\
> \* 1.data\[5\] 无须处理\
> \* 1.data\[8\] 空、页面无显示 -1 ，按照 分 切分 \[0\]\
> \* 1.data\[9\] 空、页面无显示 -1 ，按照 条 切分 \[0\]\
> \*\*/\
> private CleanBeanyc parse(String\[\] data) {\
> CleanBeanyc bean = new CleanBeanyc(\
> parseName(data\[1\]),\
> parseTime(data\[2\]),\
> parsePlace(data\[3\]),\
> parsePrice(data\[4\]),\
> (data\[5\])\
> );\
> return bean;\
> }\
> //清洗地点场馆\
> private String parsePlace(String datum) {\
> //页面无显示 -1\
> if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
> return \"-1\";\
> }\
> //字符串替换空值\
> if (datum.contains(\"场馆：\")){\
> datum = datum.replace(\"场馆：\", \"\");\
> }\
> return datum;\
> }\
> //清洗演出时间\
> private String parseTime(String datum) {\
> //页面无显示 -1\
> if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
> return \"-1\";\
> }\
> //字符串替换空值\
> if (datum.contains(\"时间：\")){\
> datum = datum.replace(\"时间：\", \"\");\
> }\
> return datum;\
> }\
> //清洗名字符号\
> private String parseName(String datum) {\
> //页面无显示 -1\
> if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
> return \"-1\";\
> }\
> //字符串替换空值\
> if (datum.contains(\"✘\")){\
> datum = datum.replace(\"✘\", \"\");\
> }\
> return datum;\
> }\
> //清洗价格\
> private String parsePrice(String datum) {\
> //页面无显示 -1\
> if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
> return \"-1\";\
> }\
> //字符串替换空值\
> if (datum.contains(\"元\")){\
> datum = datum.replace(\"元\", \"\");\
> }\
> return datum;\
> }\
> }

###### Driver编写

public class CleanParDriveryc {

public static void main(String\[\] args) throws Exception {

Configuration conf = new Configuration();

Job job = Job.getInstance();

job.setJarByClass(CleanParDriveryc.class);

job.setMapperClass(CleanParMapperyc.class);

job.setNumReduceTasks(0);

job.setMapOutputKeyClass(Void.class);

job.setMapOutputValueClass(GenericRecord.class);

Path input = new Path(\"D:\\\\数据\\\\tmp\\\\mr\\\\clean\\\\input\");

Path output = new Path(\"D:\\\\数据\\\\tmp\\\\mr\\\\clean\\\\output\");

//Path input = new Path(args\[0\]);

//Path output = new Path(args\[1\]);

// TODO : 设置输出类

job.setOutputFormatClass(AvroParquetOutputFormat.class);

// TODO : 设置压缩

AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);

AvroParquetOutputFormat.setCompressOutput(job, true);

// 设置Avro Schema

Schema schema = new Schema.Parser().parse(CleanUtilsyc.getAvscStream());

AvroParquetOutputFormat.setSchema(job, schema);

FileInputFormat.setInputPaths(job, input);

AvroParquetOutputFormat.setOutputPath(job, output);

FileSystem fileSystem = output.getFileSystem(new Configuration());

if (fileSystem.exists(output)){

System.out.println(\"开始删除\");

System.out.println(\"\");

}

boolean flag = job.waitForCompletion(true);

System.exit(flag?0:-1);

}

}

##### ktv数据清洗

###### CleanBean编写

> package projite;\
> import org.apache.hadoop.io.Writable;\
> import java.io.DataInput;\
> import java.io.DataOutput;\
> import java.io.IOException;\
> /\*\*\
> \* \@Auther:BigData-aw\
> \* \@ClassName:CleanBean\
> \* \@功能描述:\
> \* \@Version:1.0\
> \*/\
> public class CleanBean implements Writable{\
> private String name;\
> private String Comment;\
> private String consume;\
> private String service;\
> private String enviroment;\
> private String Cost;\
> private String adress;\
> public CleanBean(String name, String comment, String consume, String
> service, String enviroment, String cost, String adress) {\
> this.name = name;\
> Comment = comment;\
> this.consume = consume;\
> this.service = service;\
> this.enviroment = enviroment;\
> Cost = cost;\
> this.adress = adress;\
> \
> }\
> public CleanBean() {\
> }\
> public String getName() {\
> return name;\
> }\
> public void setName(String name) {\
> this.name = name;\
> }\
> public String getComment() {\
> return Comment;\
> }\
> public void setComment(String comment) {\
> Comment = comment;\
> }\
> public String getConsume() {\
> return consume;\
> }\
> public void setConsume(String consume) {\
> this.consume = consume;\
> }\
> public String getService() {\
> return service;\
> }\
> public void setService(String service) {\
> this.service = service;\
> }\
> public String getEnviroment() {\
> return enviroment;\
> }\
> public void setEnviroment(String enviroment) {\
> this.enviroment = enviroment;\
> }\
> public String getCost() {\
> return Cost;\
> }\
> public void setCost(String cost) {\
> Cost = cost;\
> }\
> public String getAdress() {\
> return adress;\
> }\
> public void setAdress(String adress) {\
> this.adress = adress;\
> }\
> \@Override\
> public void write(DataOutput dataOutput) throws IOException {\
> }\
> \@Override\
> public void readFields(DataInput dataInput) throws IOException {\
> }\
> \@Override\
> public String toString() {\
> return\
> name + \'\\001\' +\
> Comment + \'\\001\' +\
> consume + \'\\001\' +\
> service + \'\\001\' + enviroment + \'\\001\' +\
> Cost + \'\\001\' +\
> adress + \'\\001\' ;\
> }\
> }

###### Mapper编写

package projite;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import com.sun.tools.internal.xjc.reader.gbind.ElementSets;\
import com.sun.xml.bind.v2.runtime.ClassBeanInfoImpl;\
import net.minidev.json.writer.BeansMapper;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import org.apache.log4j.chainsaw.Main;\
import org.checkerframework.framework.qual.FromByteCode;\
import java.io.DataInput;\
import java.io.IOException;\
import java.text.ParseException;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanMapper\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanMapper extends Mapper\<LongWritable, Text,
NullWritable,CleanBean\> {\
CSVParser build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, NullWritable, CleanBean\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
String\[\] data = build.parseLine(line);\
System.out.println(\"data.length\" + data.length);\
CleanBean bean = Parse(data);\
if (bean != null)\
context.write(NullWritable.get(), bean);\
}\
/\*清洗规则：\
data\[0\] 无需处理\
data\[1\] 按照条切分 取零号元素\
data\[2\] 将"消费 0"替换成"-1" 再按照空格分 取第一号元素 例如 "消费 800"
\-\--\> \"800\"\
data\[3\] 将"服务 0"替换成"-1" 再按照空格分 取第一号元素 例如 "服务 5"
\-\--\> \"5\"\
data\[4\] 将"环境 0"替换成"-1" 再按照空格分 取第一号元素 例如 "环境 5"
\-\--\> \"5\"\
data\[5\] 将"性价比 0"替换成"-1" 再按照空格分 取第一号元素 例如 "性价比
5" \-\--\> \"5\"\
data\[6\] 地址无需处理\
\*/\
private CleanBean Parse(String\[\] data) {\
CleanBean bean = new CleanBean(\
data\[1\],\
parseComment(data\[2\]),\
parseconsume(data\[3\]),\
parseservice(data\[4\]),\
parseenviroment(data\[5\]),\
parseCost(data\[6\]),\
data\[7\]\
);\
return bean;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseCost(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseenviroment(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseservice(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseconsume(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
按照条切分 取第零号元素。\
\*/\
private String parseComment(String datum) {\
if (datum == \"暂无点评\"\|\|datum ==\" \"){\
return \"不详\";\
}\
if (datum.contains(\"条\")) {\
datum = datum.split(\"条\")\[0\];\
}\
return datum;\
}\
}

###### Driver编写

package projite;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
import java.io.IOException;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanDriver\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanDriver {\
public static void main(String\[\] args) throws Exception {\
Job job = Job.getInstance();\
job.setJarByClass(CleanBean.class);\
job.setMapperClass(CleanMapper.class);\
job.setMapOutputValueClass(CleanBean.class);\
job.setMapOutputKeyClass(NullWritable.class);\
Path input = new Path(\"D:\\\\tmp\\\\input_csv\");\
Path output = new Path(\"D:\\\\tmp\\\\output_csv\");\
//Path input = new Path(args\[0\]);\
//Path output = new Path(args\[1\]);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output))fileSystem.delete(output, true);\
FileInputFormat.setInputPaths(job, input);\
FileOutputFormat.setOutputPath(job, output);\
boolean flag = job.waitForCompletion(true);\
System.exit(flag?0:-1);\
}\
}

#### Pom添加依赖

\<!\-- Avro 版本需 \>= 1.10.0 \--\>\
\<dependency\>\
\<groupId\>org.apache.avro\</groupId\>\
\<artifactId\>avro\</artifactId\>\
\<version\>1.11.3\</version\> \<!\-- 或更高 \--\>\
\</dependency\>\
\
\<!\-- Parquet 版本需与 Avro 兼容 \--\>\
\<dependency\>\
\<groupId\>org.apache.parquet\</groupId\>\
\<artifactId\>parquet-avro\</artifactId\>\
\<version\>1.12.3\</version\> \<!\-- 或更高 \--\>\
\</dependency\>\
\
\<dependency\>\
\<groupId\>com.hadoop.gplcompression\</groupId\>\
\<artifactId\>hadoop-lzo\</artifactId\>\
\<version\>0.4.20\</version\>\
\</dependency\>

注意：需要下载LZO包，在本地重新编译打包到本地仓库，重新刷新IDEA

![](media/image27.emf)

mvn install:install-file \\

-Dfile=hadoop-lzo-0.4.20.jar \\

-DgroupId=com.hadoop.gplcompression \\

-DartifactId=hadoop-lzo \\

-Dversion=0.4.20 \\

-Dpackaging=jar

#### 代码实现

1.  **景点表**

###### CleanBean.avsc编写

在Resources下编写CleanBean.avsc

{\
\"type\": \"record\",\
\"name\": \"CleanBean\",\
\"fields\": \[\
{\"name\": \"scenery_spot\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"brief_introduction\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"phone\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"best_time_to_visit\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"recommended_length_of_visit\", \"type\": \[\"string\",
\"null\"\]},\
{\"name\": \"price\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"open_time\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"introduction\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"address\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"route\", \"type\": \[\"string\", \"null\"\]}\
\]\
}

###### Mapper实现

package cn.zpark.mr.project;\
import cn.zpark.mr.project.CleanBean;\
import cn.zpark.mr.project.CleanUtils;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import java.io.IOException;\
public class CleanMapper extends Mapper\<LongWritable, Text, Void,
GenericRecord\> {\
CSVParser build = null;\
Schema schema = null;\
\@Override\
protected void setup(Mapper\<LongWritable, Text, Void,
GenericRecord\>.Context context) throws IOException,
InterruptedException {\
build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
System.out.println(\"Mapper Schema loaded: \" + schema); // 添加日志\
}\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, Void, GenericRecord\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
String\[\] data = build.parseLine(line);\
System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
CleanBean bean=parse(data);\
GenericRecord record = CleanUtils.getRecord(schema,bean);\
if (bean!=null)\
context.write(null, record);\
}\
/\*\*\
\* 功能描述：数据清洗方法\
\* 1.data\[1\] 无须处理\
\* 2.data\[2\] 空 暂无\
\* 3.data\[3\] 空、无 暂无\
\* 4.data\[4\] 空 -1\
\* 5.data\[5\] 空 -1，电话号清洗为-2\
\* 6.data\[6\] 空 -1，去除门票价格中的开放时间，替换字符串 门票价格：
票价：\
\* 7.data\[7\] 空 -1，替换字符串 开放时间：\
\* 8.data\[8\] 无须处理\
\* 9.data\[9\] 空 -1\
\* 10.data\[10\] 空 暂无数据\
\*\*/\
private CleanBean parse(String\[\] data) {\
// 检查数组长度是否足够\
if (data == null \|\| data.length \< 11) {\
// 如果数据不足11列，可以返回null或创建一个包含默认值的CleanBean\
// 这里选择返回null，在map方法中会跳过这个记录\
System.out.println(\"数据列数不足，跳过此记录。实际列数: \" + (data !=
null ? data.length : \"null\"));\
return null;\
}\
//bean为空时不写\
CleanBean bean = new CleanBean(\
data\[1\],\
parseBrief_introduction(data\[2\]),\
parsePhone(data\[3\]),\
parseBest_time_to_visit(data\[4\]),\
parseRecommended_length_of_visit(data\[5\]),\
parsePrice(data\[6\]),\
parseOpen_time(data\[7\]),\
data\[8\],\
parseAddress(data\[9\]),\
parseRoute(data\[10\])\
);\
return bean;\
}\
private String parseRoute(String datum) {\
if(datum == null \|\| datum.length()\<=0){\
return \"暂无数据\";\
}\
System.out.println(\"data.length:\"+datum.length());\
return datum;\
}\
//将data\[10\]中的空值替换为暂无数据\
private String parseAddress(String datum) {\
if(datum == null \|\| datum.length()\<=0){\
return \"-1\";\
}\
return datum;\
}\
//将data\[9\]中空值替换为-1\
private String parseOpen_time(String datum) {\
if(datum == null \|\| datum.length()\<=0){\
return \"不详\";\
}\
if (datum.contains(\"开放时间： \")){\
datum=datum.replace(\"开放时间：\", \"\");\
}\
return datum;\
}\
//将data\[7\]中空值替换为不详，替换字符串 开放时间：\
private String parsePrice(String datum) {\
if(datum == null \|\| datum.length()\<=0){\
return \"不知\";\
}\
if(datum.contains(\"开放时间：\")){\
return \"-3\";\
}\
if (datum.contains(\"门票价格：\")){\
datum=datum.replace(\"门票价格：\", \"\");\
}\
if (datum.contains(\"票价：\")){\
datum=datum.replace(\"票价：\", \"\");\
}\
return datum;\
}\
//将data\[6\]中的空值替换为不知，带有"开放时间："这五个字的整行字符串变为-3，替换字符串
门票价格： 票价：\
private String parseRecommended_length_of_visit(String datum) {\
String PHONE =
\"\\\\+?\\\\d{1,4}?\[-.\\\\s\]?\\\\(?\\\\d{1,3}?\\\\)?\[-.\\\\s\]?\\\\d{1,4}\[-.\\\\s\]?\\\\d{1,4}\[-.\\\\s\]?\\\\d{1,9}\";\
if(datum == null \|\| datum.length()\<=0){\
return \"-1\";\
}\
String cleaned = datum.replaceAll(\"\[\^0-9+\]\", \"\");\
if (cleaned.matches(PHONE)){\
return \"error\";\
}\
return datum;\
}\
//将data\[5\]中的空值变为-1，电话号清洗为error\
private String parseBest_time_to_visit(String datum) {\
if (datum == null \|\| datum.length()\<=0){\
return \"-1\";\
}\
return datum;\
}\
//将data\[4\]中的空值变成-1\
private String parsePhone(String datum) {\
if (datum.length()\<=0\|\|\"无\".equals(datum)){\
return \"暂无\";\
}\
return datum;\
}\
//将data\[3\]中的空值和无变成暂无\
private String parseBrief_introduction(String datum) {\
if (datum.length()\<=0){\
return \"暂无\";\
}\
return datum;\
}\
//将data\[2\]中的空值变成暂无\
}

###### Driver实现

package cn.zpark.mr.project;\
import cn.zpark.mr.project.CleanUtils;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.parquet.avro.AvroParquetOutputFormat;\
import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
public class CleanDriver {\
public static void main(String\[\] args) throws Exception {\
Configuration conf = new Configuration();\
Job job = Job.getInstance();\
job.setJarByClass(CleanDriver.class);\
job.setMapperClass(CleanMapper.class);\
job.setNumReduceTasks(0);\
job.setMapOutputKeyClass(Void.class);\
job.setMapOutputValueClass(GenericRecord.class);\
//Path input = new Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\input_project\");\
//Path output = new
Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\output_project\");\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
// TODO : 设置输出类\
job.setOutputFormatClass(AvroParquetOutputFormat.class);\
// TODO : 设置压缩\
AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);\
AvroParquetOutputFormat.setCompressOutput(job, true);\
// 设置Avro Schema\
Schema schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
AvroParquetOutputFormat.setSchema(job, schema);\
FileInputFormat.setInputPaths(job, input);\
AvroParquetOutputFormat.setOutputPath(job, output);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output)) fileSystem.delete(output, true);\
boolean flag = job.waitForCompletion(true);\
System.exit(flag ? 0 : -1);\
}\
}

###### 工具类实现

package cn.zpark.mr.project;\
import cn.zpark.mr.project.CleanBean;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericData;\
import org.apache.avro.generic.GenericRecord;\
import java.io.InputStream;\
public class CleanUtils {\
public static InputStream getAvscStream(){\
InputStream resourceAsStream =
CleanUtils.class.getClassLoader().getResourceAsStream(\"CleanBean.avsc\");\
return resourceAsStream;\
}\
public static GenericRecord getRecord(Schema schema, CleanBean bean){\
GenericRecord record = new GenericData.Record(schema);\
record.put(\"scenery_spot\", bean.getScenery_spot());\
record.put(\"brief_introduction\", bean.getBrief_introduction());\
record.put(\"phone\", bean.getPhone());\
record.put(\"best_time_to_visit\", bean.getBest_time_to_visit());\
record.put(\"recommended_length_of_visit\",
bean.getRecommended_length_of_visit());\
record.put(\"price\", bean.getPrice());\
record.put(\"open_time\", bean.getOpen_time());\
record.put(\"introduction\", bean.getIntroduction());\
record.put(\"address\", bean.getAddress());\
record.put(\"route\", bean.getRoute());\
return record;\
}\
}

2.  **电影表**

###### CleanBean.avsc编写

在Resources下编写CleanBean.avsc

{\
\"type\": \"record\",\
\"name\": \"CleanBean\",\
\"fields\": \[\
{\"name\": \"name\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"score\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"type\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"country\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"director\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"star\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"synopsis\", \"type\": \[\"string\", \"null\"\]}\
\]\
}

###### Mapper实现

package cn.zpark.mr.project;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import java.io.IOException;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanParMapper\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanParMapper extends Mapper\<LongWritable, Text,
Void,GenericRecord\> {\
CSVParser build = null;\
Schema schema = null;\
\@Override\
protected void setup(Mapper\<LongWritable, Text, Void,
GenericRecord\>.Context context) throws IOException,
InterruptedException {\
build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
System.out.println(\"Mapper Schema loaded: \" + schema); // 添加日志\
}\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, Void, GenericRecord\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
String\[\] data = build.parseLine(line);
System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
CleanBean bean=parse(data);\
GenericRecord record = CleanUtils.getRecord(schema,bean);\
if (bean!=null)\
context.write(null, record);\
}\
/\*\*\
\* 功能描述：数据清洗方法\
\* data\[0\] 无需处理\
\* data\[1\] 空 -1 按照 分 切分 \[0\]\
\* data\[2\] 替换字符串 类型:\
\* data\[3\] 替换字符串 国家/地区：\
\* data\[4\] 替换字符串 导演:\
\* data\[5\] 替换字符串 主演:\
\* data\[6\] 空 暂无\
\*\*/\
private CleanBean parse(String\[\] data) {\
return new CleanBean(\
data\[1\],// 原始名称字段\
parseScore(data\[2\]), // 处理评分字段\
parseType(data\[3\]), // 处理类型字段\
parseCountry(data\[4\]),// 处理国家/地区字段\
parseDirector(data\[5\]),// 处理导演字段\
parseStar(data\[6\]),// 处理主演字段\
parseSynopsis(data\[7\])// 处理简介字段\
);\
}\
/\*\*\
\* 处理评分字段\
\* 将格式从\"x.x分\"变为\"x.x\"（从\"分\"切割取第一部分）\
\*/\
private String parseScore(String datum) {\
if (datum.length()\<=0 ) {\
return \"-1\";\
}\
if (datum.contains(\"分\")){\
datum=datum.split(\"分\")\[0\];\
}\
return datum;\
}\
/\*\*\
\* 处理类型字段\
\* 移除\"类型:\"前缀\
\*/\
private String parseType(String datum) {\
if (datum.contains(\"类型:\"))\
datum=datum.replace(\"类型:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理国家/地区字段\
\* 移除\"国家/地区:\"前缀\
\*/\
private String parseCountry(String datum) {\
if (datum.contains(\"国家/地区:\"))\
datum=datum.replace(\"国家/地区:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理导演字段\
\* 移除\"导演:\"前缀\
\*/\
private String parseDirector(String datum) {\
if (datum.contains(\"导演:\"))\
datum=datum.replace(\"导演:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理主演字段\
\* 移除\"主演:\"前缀\
\*/\
private String parseStar(String datum) {\
if (datum.contains(\"主演:\"))\
datum=datum.replace(\"主演:\",\" \");\
return datum;\
}\
/\*\*\
\* 处理空字段\
\* 空值处理为\"暂无\"\
\*/\
private String parseSynopsis(String datum) {\
if (datum.length() \<= 0) {\
return \"暂无\";\
}\
return datum;\
}\
}

###### Driver实现

package cn.zpark.mr.project;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.parquet.avro.AvroParquetOutputFormat;\
import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanParDriver\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanParDriver {\
public static void main(String\[\] args) throws Exception {\
Configuration conf = new Configuration();\
Job job = Job.getInstance();\
job.setJarByClass(CleanParDriver.class);\
job.setMapperClass(CleanParMapper.class);\
job.setNumReduceTasks(0);\
job.setMapOutputKeyClass(Void.class);\
job.setMapOutputValueClass(GenericRecord.class);\
//Path input = new Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\input_project\");\
//Path output = new
Path(\"D:\\\\tmp\\\\mr\\\\clean\\\\output_project\");\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
// TODO : 设置输出类
job.setOutputFormatClass(AvroParquetOutputFormat.class);\
// TODO : 设置压缩\
AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);\
AvroParquetOutputFormat.setCompressOutput(job, true);\
// 设置Avro Schema\
Schema schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
AvroParquetOutputFormat.setSchema(job, schema);\
FileInputFormat.setInputPaths(job, input);\
AvroParquetOutputFormat.setOutputPath(job, output);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output))fileSystem.delete(output, true);\
boolean flag = job.waitForCompletion(true);\
System.exit(flag?0:-1);\
}\
}

###### 工具类实现

package cn.zpark.mr.project;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericData;\
import org.apache.avro.generic.GenericRecord;\
import java.io.InputStream;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanUtils\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanUtils {\
public static InputStream getAvscStream(){\
InputStream resourceAsStream =
CleanUtils.class.getClassLoader().getResourceAsStream(\"CleanBean.avsc\");\
return resourceAsStream;\
}\
public static GenericRecord getRecord(Schema schema, CleanBean bean){\
GenericRecord record = new GenericData.Record(schema);\
record.put(\"name\", bean.getName());\
record.put(\"score\", bean.getScore());\
record.put(\"type\", bean.getType());\
record.put(\"country\",bean.getCountry());\
record.put(\"director\", bean.getDirector());\
record.put(\"star\", bean.getStar());\
record.put(\"synopsis\", bean.getSynopsis());\
return record;\
}\
}

3.  **体育运动场馆**

###### CleanBean.avsc编写

在Resources下编写CleanBean.avsc

{\
\"type\": \"record\",\
\"name\": \"CleanBean\",\
\"fields\": \[\
{\"name\": \"type\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"name\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"address\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"score\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"city\", \"type\": \[\"string\", \"null\"\]}\
\]\
}

###### Mapper实现

package project;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import project.CleanBean;\
import java.io.IOException;\
public class CleanParMapper extends Mapper\<LongWritable, Text,
Void,GenericRecord\> {\
CSVParser build = null;\
Schema schema = null;\
\@Override\
protected void setup(Mapper\<LongWritable, Text, Void,
GenericRecord\>.Context context) throws IOException,
InterruptedException {\
build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
System.out.println(\"Mapper Schema loaded: \" + schema); // 添加日志\
}\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, Void, GenericRecord\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
String\[\] data = build.parseLine(line);
System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
CleanBean bean=parse(data);\
GenericRecord record = CleanUtils.getRecord(schema,bean);\
if (bean!=null)\
context.write(null, record);\
}\
/\*\*\
\* 功能描述：数据清洗方法\
\* 1.data\[1\] 无需处理\
\* 1.data\[2\] 空 -1\
\* 1.data\[3\] 空 -1\
\* 1.data\[5\] 空 -1\
\* 1.data\[8\] 无需处理\
\*\*/\
private CleanBean parse(String\[\] data) {\
CleanBean bean = new CleanBean(\
data\[1\],\
parseName(data\[2\]),\
parseAddress(data\[3\]),\
parseScore(data\[5\]),\
data\[8\]\
);\
return bean;\
}\
private String parseName(String datum) {\
if (datum.length()\<=0){\
return \"-1\";\
}\
return datum;\
}\
private String parseAddress(String datum) {\
if (datum.length()\<=0){\
return \"-1\";\
}\
return datum;\
}\
private String parseScore(String datum) {\
if (datum.length()\<=0){\
return \"-1\";\
}\
return datum;\
}\
}

###### Driver实现

package project;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.parquet.avro.AvroParquetOutputFormat;\
import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
import project.CleanParMapper;\
import project.CleanUtils;\
public class CleanParDriver {\
public static void main(String\[\] args) throws Exception {\
Configuration conf = new Configuration();\
Job job = Job.getInstance();\
job.setJarByClass(CleanParDriver.class);\
job.setMapperClass(CleanParMapper.class);\
job.setNumReduceTasks(0);\
job.setMapOutputKeyClass(Void.class);\
job.setMapOutputValueClass(GenericRecord.class);\
//Path input = new
Path(\"E:\\\\dev\\\\tmp\\\\clean\\\\input_project\");\
//Path output = new
Path(\"E:\\\\dev\\\\tmp\\\\clean\\\\output_project\");\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
// TODO : 设置输出类\
job.setOutputFormatClass(AvroParquetOutputFormat.class);\
// TODO : 设置压缩\
AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);\
AvroParquetOutputFormat.setCompressOutput(job, true);\
// 设置Avro Schema\
Schema schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
AvroParquetOutputFormat.setSchema(job, schema);\
FileInputFormat.setInputPaths(job, input);\
AvroParquetOutputFormat.setOutputPath(job, output);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output))fileSystem.delete(output, true);\
boolean flag = job.waitForCompletion(true);\
System.exit(flag?0:-1);\
}\
}

###### 工具类实现

package project;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericData;\
import org.apache.avro.generic.GenericRecord;\
import java.io.InputStream;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanUtils\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanUtils {\
public static InputStream getAvscStream(){\
InputStream resourceAsStream =
CleanUtils.class.getClassLoader().getResourceAsStream(\"CleanBean.avsc\");\
return resourceAsStream;\
}\
public static GenericRecord getRecord(Schema schema, CleanBean bean){\
GenericRecord record = new GenericData.Record(schema);\
record.put(\"type\", bean.getType());\
record.put(\"name\", bean.getName());\
record.put(\"address\", bean.getAddress());\
record.put(\"score\", bean.getScore());\
record.put(\"city\", bean.getCity());\
return record;\
}\
}

4.  **演出表**

###### CleanBean.avsc编写

在Resources下编写CleanBean.avsc

{

\"type\": \"record\",

\"name\": \"CleanBean\",

\"fields\": \[

{\"name\": \"name\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"time\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"place\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"price\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"cond\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"picture\", \"type\": \[\"string\", \"null\"\]},

{\"name\": \"method\", \"type\": \[\"string\", \"null\"\]}

\]

}

###### Mapper编写

package cn.zpark.mr.projcet;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import java.io.IOException;\
// MapReduce任务的Map阶段实现\
public class CleanParMapperyc extends Mapper\<LongWritable, Text, Void,
GenericRecord\> {\
//声明一个 CSV解析器对象，初始化为空（null）
用途：将输入的文本数据（如CSV格式的每行文本）解析为结构化字段\
CSVParser build = null;\
//声明一个数据模式对象，初始化为空（null）用途：定义输出数据的结构化格式（字段名、类型等），确保生成的数据符合规范。\
Schema schema = null;\
\@Override\
//初始化Map任务执行前的环境和资源\
protected void setup(Mapper\<LongWritable, Text, Void,
GenericRecord\>.Context context) throws IOException,
InterruptedException {\
// 构建一个自定义配置的CSV解析器\
build = new CSVParserBuilder()\
.withSeparator(\',\')//指定字段分隔符为逗号（默认也是逗号）\
.withIgnoreQuotations(true)//禁用引号处理，强制将双引号视为普通字符\
.build();\
//动态解析Avro数据格式的Schema定义文件\
schema = new Schema.Parser().parse(CleanUtilsyc.getAvscStream());\
//在控制台输出Mapper Schema加载成功的日志信息\
System.out.println(\"Mapper Schema loaded: \" + schema);\
}\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, Void, GenericRecord\>.Context context) throws IOException,
InterruptedException {\
//将Text类型转换为Java的String类型\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
// 使用OpenCSV库的解析器对CSV格式的行数据进行结构化分割
这行代码的作用应该是将每一行的CSV数据解析成字符串数组\
String\[\] data = build.parseLine(line);\
//输出解析后的CSV字段数组长度
System.out.println(\"\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
//将解析后的CSV字段数组转换为结构化的Java Bean对象\
CleanBeanyc bean=parse(data);\
//将清洗后的Java Bean对象转换为Avro格式的通用数据记录\
GenericRecord record = CleanUtilsyc.getRecord(schema,bean);\
//过滤无效数据并输出有效记录\
if (bean!=null)\
context.write(null, record);\
}\
/\*\*\
\* \@功能描述:数据清洗方法\
\* 1.data \[1\] 演出名称 空、页面无显示 -1 删除 ✘\
\* 1.data \[2\] 演出时间 空、页面无显示 -1 替换字符串 时间：\
\* 1.data \[3\] 演出地点 空、页面无显示 -1 替换字符串 场馆：\
\* 1.data \[4\] 不处理\
\* 1.data \[5\] 不处理\
\*/\
// 将原始数据字段按规则清洗后封装为结构化对象\
private CleanBeanyc parse(String\[\] data) {\
CleanBeanyc bean = new CleanBeanyc(\
parseName(data\[1\]),\
parseTime(data\[2\]),\
parsePlace(data\[3\]),\
parsePrice(data\[4\]),\
(data\[5\])\
);\
return bean;\
}\
//清洗地点场馆\
private String parsePlace(String datum) {\
//页面无显示 -1\
if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
return \"-1\";\
}\
//字符串替换空值\
if (datum.contains(\"场馆：\")){\
datum = datum.replace(\"场馆：\", \"\");\
}\
return datum;\
}\
//清洗演出时间\
private String parseTime(String datum) {\
//页面无显示 -1\
if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
return \"-1\";\
}\
//字符串替换空值\
if (datum.contains(\"时间：\")){\
datum = datum.replace(\"时间：\", \"\");\
}\
return datum;\
}\
//清洗名字符号\
private String parseName(String datum) {\
//页面无显示 -1\
if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
return \"-1\";\
}\
//字符串替换空值\
if (datum.contains(\"✘\")){\
datum = datum.replace(\"✘\", \"\");\
}\
return datum;\
}\
//清洗价钱\
private String parsePrice(String datum) {\
//页面无显示 -1\
if (datum.length()\<=0\|\|\"页面无显示\".equals(datum)){\
return \"-1\";\
}\
//字符串替换空值\
if (datum.contains(\"元\")){\
datum = datum.replace(\"元\", \"\");\
}\
return datum;\
}\
}

###### Driver实现

package cn.zpark.mr.projcet;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.parquet.avro.AvroParquetOutputFormat;\
import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanParDriver\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanParDriveryc {\
public static void main(String\[\] args) throws Exception {\
Configuration conf = new Configuration();\
Job job = Job.getInstance();\
job.setJarByClass(CleanParDriveryc.class);\
job.setMapperClass(CleanParMapperyc.class);\
job.setNumReduceTasks(0);\
job.setMapOutputKeyClass(Void.class);\
job.setMapOutputValueClass(GenericRecord.class);\
//Path input = new Path(\"D:\\\\数据\\\\tmp\\\\mr\\\\clean\\\\input\");\
//Path output = new
Path(\"D:\\\\数据\\\\tmp\\\\mr\\\\clean\\\\output\");\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
// TODO : 设置输出类
job.setOutputFormatClass(AvroParquetOutputFormat.class);\
// TODO : 设置压缩\
AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);\
AvroParquetOutputFormat.setCompressOutput(job, true);\
// 设置Avro Schema\
Schema schema = new
Schema.Parser().parse(CleanUtilsyc.getAvscStream());\
AvroParquetOutputFormat.setSchema(job, schema);\
FileInputFormat.setInputPaths(job, input);\
AvroParquetOutputFormat.setOutputPath(job, output);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output)){\
System.out.println(\"开始删除\");\
System.out.println(\"\");\
}\
boolean flag = job.waitForCompletion(true);\
System.exit(flag?0:-1);\
}\
}

###### 工具类实现

package cn.zpark.mr.projcet;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericData;\
import org.apache.avro.generic.GenericRecord;\
import java.io.InputStream;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanUtils\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanUtilsyc {\
public static InputStream getAvscStream(){\
InputStream resourceAsStream =
CleanUtilsyc.class.getClassLoader().getResourceAsStream(\"CleanBean.avsc\");\
return resourceAsStream;\
}\
public static GenericRecord getRecord(Schema schema, CleanBeanyc bean){\
GenericRecord record = new GenericData.Record(schema);\
record.put(\"name\", bean.getName());\
record.put(\"time\", bean.getTime());\
record.put(\"place\", bean.getPlace());\
record.put(\"price\", bean.getPrice());\
record.put(\"cond\", bean.getCond());\
return record;\
}\
}

5.  **ktv**

###### CleanBean.avsc编写

在Resources下编写CleanBean.avsc

{\
\"type\": \"record\",\
\"name\": \"CleanBean\",\
\"fields\": \[\
{\"name\": \"name\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"Comment\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"service\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"consume\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"enviroment\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"Cost\", \"type\": \[\"string\", \"null\"\]},\
{\"name\": \"adress\", \"type\": \[\"string\", \"null\"\]},

\]

}

###### Mapper实现

package projite;\
import com.opencsv.CSVParser;\
import com.opencsv.CSVParserBuilder;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
import java.io.IOException;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanParMapper\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanParMapper extends Mapper\<LongWritable, Text, Void,
GenericRecord\> {\
CSVParser build = null;\
Schema schema = null;\
\@Override\
protected void setup(Mapper\<LongWritable, Text, Void,
GenericRecord\>.Context context) throws IOException,
InterruptedException {\
build = new CSVParserBuilder()\
.withSeparator(\',\')\
.withIgnoreQuotations(true)\
.build();\
schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
System.out.println(\"Mapper Schema loaded: \" + schema); // 添加日志\
}\
\@Override\
protected void map(LongWritable key, Text value, Mapper\<LongWritable,
Text, Void, GenericRecord\>.Context context) throws IOException,
InterruptedException {\
String line = value.toString();\
//String\[\] data = line.split(\",\");\
String\[\] data = build.parseLine(line);
System.out.println(\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*data.length:\"+data.length);\
CleanBean bean=parse(data);\
GenericRecord record = CleanUtils.getRecord(schema,bean);\
if (bean!=null)\
context.write(null, record);\
}\
private CleanBean parse(String\[\] data) {\
CleanBean bean = new CleanBean(\
data\[1\],\
parseComment(data\[2\]),\
parseconsume(data\[3\]),\
parseservice(data\[4\]),\
parseenviroment(data\[5\]),\
parseCost(data\[6\]),\
data\[7\]\
);\
return bean;\
}\
/\*清洗规则：\
data\[0\] 无需处理\
data\[1\] 按照条切分 取零号元素\
data\[2\] 将"消费 0"替换成"-1" 再按照空格分 取第一号元素 例如 "消费 800"
\-\--\> \"800\"\
data\[3\] 将"服务 0"替换成"-1" 再按照空格分 取第一号元素 例如 "服务 5"
\-\--\> \"5\"\
data\[4\] 将"环境 0"替换成"-1" 再按照空格分 取第一号元素 例如 "环境 5"
\-\--\> \"5\"\
data\[5\] 将"性价比 0"替换成"-1" 再按照空格分 取第一号元素 例如 "性价比
5" \-\--\> \"5\"\
data\[6\] 地址无需处理\
\*/\
private CleanBean Parse(String\[\] data) {\
CleanBean bean = new CleanBean(\
data\[1\],\
parseComment(data\[2\]),\
parseconsume(data\[3\]),\
parseservice(data\[4\]),\
parseenviroment(data\[5\]),\
parseCost(data\[6\]),\
data\[7\]\
);\
return bean;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseCost(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseenviroment(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseservice(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
此数组我们只需要按照空格分隔后的一号元素\
\*/\
private String parseconsume(String datum) {\
if (datum.contains(\" \")) {\
datum = datum.split(\" \")\[1\];\
}\
return datum;\
}\
/\*\
按照条切分 取第零号元素。\
\*/\
private String parseComment(String datum) {\
if (datum == \"暂无点评\"\|\|datum ==\" \"){\
return \"不详\";\
}\
if (datum.contains(\"条\")) {\
datum = datum.split(\"条\")\[0\];\
}\
return datum;\
}\
}

###### Driver实现

package projite;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericRecord;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.io.NullWritable;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
import org.apache.parquet.avro.AvroParquetOutputFormat;\
import org.apache.parquet.hadoop.metadata.CompressionCodecName;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanParDriver\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanParDriver {\
public static void main(String\[\] args) throws Exception {\
Configuration conf = new Configuration();\
Job job = Job.getInstance();\
job.setJarByClass(CleanParDriver.class);\
job.setMapperClass(CleanParMapper.class);\
job.setNumReduceTasks(0);\
job.setMapOutputKeyClass(Void.class);\
job.setMapOutputValueClass(GenericRecord.class);\
//Path input = new Path(\"D:\\\\tmp\\\\input_csv\");\
//Path output = new Path(\"D:\\\\tmp\\\\output_csv\");\
Path input = new Path(args\[0\]);\
Path output = new Path(args\[1\]);\
// TODO : 设置输出类\
job.setOutputFormatClass(AvroParquetOutputFormat.class);\
// TODO : 设置压缩\
AvroParquetOutputFormat.setCompression(job, CompressionCodecName.LZO);\
AvroParquetOutputFormat.setCompressOutput(job, true);\
// 设置Avro Schema\
Schema schema = new Schema.Parser().parse(CleanUtils.getAvscStream());\
AvroParquetOutputFormat.setSchema(job, schema);\
FileInputFormat.setInputPaths(job, input);\
AvroParquetOutputFormat.setOutputPath(job, output);\
FileSystem fileSystem = output.getFileSystem(new Configuration());\
if (fileSystem.exists(output))fileSystem.delete(output, true);\
boolean flag = job.waitForCompletion(true);\
System.exit(flag?0:-1);\
}\
}

###### 工具类实现

package projite;\
import org.apache.avro.Schema;\
import org.apache.avro.generic.GenericData;\
import org.apache.avro.generic.GenericRecord;\
import java.io.InputStream;\
/\*\*\
\* \@Auther:BigData-aw\
\* \@ClassName:CleanUntils\
\* \@功能描述:\
\* \@Version:1.0\
\*/\
public class CleanUtils {\
public static InputStream getAvscStream(){\
InputStream resourceAsStream =
CleanUtils.class.getClassLoader().getResourceAsStream(\"CleanBean.avsc\");\
return resourceAsStream;\
}\
public static GenericRecord getRecord(Schema schema, CleanBean bean){\
GenericRecord record = new GenericData.Record(schema);\
record.put(\"name\", bean.getName());\
record.put(\"Comment\",bean.getComment());\
record.put(\"consume\",bean.getConsume());\
record.put(\"service\",bean.getService());\
record.put(\"enviroment\",bean.getEnviroment());\
record.put(\"Cost\",bean.getCost());\
record.put(\"adress\",bean.getAdress());\
return record;\
}\
}

6.  **集群环境修改**

##### i）LZO压缩的配置 {#ilzo压缩的配置 .标题6}

将编译之后的jar包上传

将编译之后的jar包上传到node-1节点的[\`\${HADOOP_HOME}/share/hadoop/common\`]{.underline}目录下

注意！**同步到其他机器**

core-site.xml当中配置压缩方式node-1修改core-site.xml文件

cd \${HADOOP_HOME}/etc/hadoop

vim core-site.xml

添加以下配置

\<property\>

\<name\>io.compression.codecs\</name\>

\<value\>

org.apache.hadoop.io.compress.GzipCodec,

org.apache.hadoop.io.compress.DefaultCodec,

org.apache.hadoop.io.compress.BZip2Codec,

org.apache.hadoop.io.compress.SnappyCodec,

com.hadoop.compression.lzo.LzoCodec,

com.hadoop.compression.lzo.LzopCodec

\</value\>

\</property\>

\<property\>

\<name\>io.compression.codec.lzo.class\</name\>

\<value\>com.hadoop.compression.lzo.LzoCodec\</value\>

\</property\>

同步core-site.xml到其他机器

重新启动hdfs集群

##### ii）Hive配置LZO压缩 {#iihive配置lzo压缩 .标题6}

直接将 \`hadoop-lzo-0.4.20.jar\` 这个jar包拷贝到hive的lib目录下即可

Node-1执行以下命令，将jar包拷贝到hive的lib目录下

cd /opt/apps/hadoop-3.2.1/share/hadoop/common/libAVRO包升级

###### iii) avro包升级

Hadoop3.2.1的集群上的avro是1.7.7，需要更换

![](media/image28.emf)

修改集群avro-1.7.7 到 1.11.3 远程发送 三台重启hadoop

\[zpark@node-1 \~\]\$ cd /opt/apps/hadoop-3.2.1/share/hadoop/common/lib/

\[zpark@node-1 lib\]\$ ll\|grep avro

-rw-r\--r\-- 1 zpark zpark 644626 Mar 25 19:28 avro-1.11.3.jar

-rw-r\--r\-- 1 zpark zpark 436303 Sep 10 2019 avro-1.7.7.jar.bak

打包集群提交

hadoop jar example-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar
cn.zpark.mr.project.CleanParDriver /data/logs2/
/data/clean/hotel/**etl_times=\`date -d -1hour +%H\`**

### 数据仓库搭建

ODS建模规则：

  -----------------------------------------------------------------------
  建模维度       建模规则
  -------------- --------------------------------------------------------
  **存储优化**   LZO压缩（支持切分），兼顾空间与查询效率

  **分区策略**   按时间（日/小时）分区，避免全表扫描

  **数据管理**   保留原始数据，仅做轻量ETL；支持历史数据回溯

  **格式选择**   结构化数据优先Parquet，半结构化数据可选JSON+压缩
  -----------------------------------------------------------------------

#### ODS

##### jd表

> **景点**

create database if not exists mjy;\
CREATE external TABLE mjy.wc (\
scenery_spot String,\
brief_introduction String,\
phone String,\
best_time_to_visit String,\
recommended_length_of_visit String,\
price String,\
open_time String,\
introduction String,\
address String,\
route String\
) comment \'wcnm\'\
partitioned by (etl_times string)\
row format delimited null defined as \'\\\\N\'\
stored as parquet\
location \"/date/clean/mjy/\"\
tblproperties (\'parquet.compression\' = \'lzo\');\
alter table mjy.wc add partition(etl_times=\'18\') ;\
\
\
CREATE external TABLE \`mjy.wc_ods\` (\
\`purl\` string COMMENT \'省连接\',\
\`pname\` string COMMENT \'省名称\',\
\`curl\` string COMMENT \'市连接\',\
\`ccode\` string COMMENT \'市代码\',\
\`cname\` string COMMENT \'市名称\',\
\`rurl\` string COMMENT \'区连接\',\
\`rcode\` string COMMENT \'区代码\',\
\`rname\` string COMMENT \'区名称\',\
\`surl\` string COMMENT \'街道连接\',\
\`scode\` string COMMENT \'街道代码\',\
\`sname\` string COMMENT \'街道名称\',\
\`ncurl\` string COMMENT \'居委会连接\',\
\`nccode\` string COMMENT \'城乡分类代码\',\
\`ncname\` string COMMENT \'居委会名称\'\
) row format delimited fields terminated by \',\';

\-- 加载 CSV 文件\
hadoop fs -put /home/zpark/tmp/data/\*
/user/hive/warehouse/mjy.db/wc_ods

![](media/image29.png){width="5.768055555555556in"
height="3.2395833333333335in"}

##### dianying表

> **电影**

CREATE external TABLE wenyu.dianying (\
name String,\
score String,\
type String,\
country String ,\
director String ,\
star String,\
synopsis String\
) comment \'dianying\'\
partitioned by (dianying string)\
row format delimited null defined as \'\\\\N\'\
stored as parquet\
location \"/data/clean/\"\
tblproperties (\'parquet.compression\' = \'lzo\');\
alter table wenyu.dianying add partition(dianying=\'15\');

![](media/image30.png){width="5.768055555555556in"
height="5.749307742782152in"}

##### sport表

**体育运动场馆**

create database ods_sport;

CREATE external TABLE ods_sport.sport_new (

type String,

name String,

address String,

score String,

city String

) comment \'体育场馆\'

partitioned by (etl_times string)

row format delimited null defined as \'\\\\N\'

stored as parquet

location \"/xiangmu/data/clean\"

tblproperties (\'parquet.compression\' = \'lzo\');

alter table ods_sport.sport_new add
partition(etl_times=\'14\');![](media/image31.png){width="5.768055555555556in"
height="4.000694444444444in"}

**National表**

爬取全国数据

CREATE TABLE \`ods_sport.national\` (

\`purl\` string COMMENT \'省连接\',

\`pname\` string COMMENT \'省名称\',

\`curl\` string COMMENT \'市连接\',

\`ccode\` string COMMENT \'市代码\',

\`cname\` string COMMENT \'市名称\',

\`rurl\` string COMMENT \'区连接\',

\`rcode\` string COMMENT \'区代码\',

\`rname\` string COMMENT \'区名称\',

\`surl\` string COMMENT \'街道连接\',

\`scode\` string COMMENT \'街道代码\',

\`sname\` string COMMENT \'街道名称\',

\`ncurl\` string COMMENT \'居委会连接\',

\`nccode\` string COMMENT \'城乡分类代码\',

\`ncname\` string COMMENT \'居委会名称\'

) row format delimited fields terminated by \',\';\
\-- 加载 CSV 文件\
hadoop fs -put /home/zpark/tmp/data/\*
/user/hive/warehouse/ods_sport.db/national

![](media/image32.png){width="5.771113298337708in"
height="2.855358705161855in"}

##### yanchu表

> **体育运动场馆**

\-\--ODS原字段表：\-\-\--\
\-- name 名字，\
\-- time 时间，\
\-- place 地点，\
\-- price 价格，\
\-- cond 情况，\
create database travel_ods;\
CREATE external TABLE travel_ods.travel_ods_yanchu (\
name String,\
\`time\` String,\
place String,\
price String,\
cond String\
) comment \'演出表\'\
partitioned by (etl_times string)\
row format delimited null defined as \'\\\\N\'\
stored as parquet\
location \"/data/clean/yanchu/\"\
tblproperties (\'parquet.compression\' = \'lzo\');\
alter table travel_ods.travel_ods_yanchu add partition(etl_times=\'13\')
;

![](media/image33.png){width="5.7659722222222225in"
height="4.013194444444444in"}

##### ktv表

> **KTV**

CREATE TABLE if not exists ktv.ktv_lzo (\
name String,\
Comment String,\
consume String,\
service String ,\
enviroment String ,\
Cost String,\
adress String\
) comment \'KTV\'\
partitioned by (etl_times string)\
row format delimited null defined as \'\\\\N\'\
stored as parquet\
location \"/data/clean/ktv/\"\
tblproperties (\'parquet.compression\' = \'lzo\');\
alter table ktv.ktv_lzo add
partition(etl_times=\'\${hivevar:etl_times}\') ;

**National表**

CREATE TABLE ktv.ktv_loc_ods (\
\`purl\` string COMMENT \'省连接\',\
\`pname\` string COMMENT \'省名称\',\
\`curl\` string COMMENT \'市连接\',\
\`ccode\` string COMMENT \'市代码\',\
\`cname\` string COMMENT \'市名称\',\
\`rurl\` string COMMENT \'区连接\',\
\`rcode\` string COMMENT \'区代码\',\
\`rname\` string COMMENT \'区名称\',\
\`surl\` string COMMENT \'街道连接\',\
\`scode\` string COMMENT \'街道代码\',\
\`sname\` string COMMENT \'街道名称\',\
\`ncurl\` string COMMENT \'居委会连接\',\
\`nccode\` string COMMENT \'城乡分类代码\',\
\`ncname\` string COMMENT \'居委会名称\'\
) row format delimited fields terminated by \',\';

\-- 加载 CSV 文件\
hadoop fs -put /home/zpark/tmp/data/\*
/user/hive/warehouse/ods_sport.db/national

#### DIM层

##### 景点

###### 地域维度表

create database if not exists mjy_dim;\
create table if not exists mjy_dim.mjy_dim_wc as\
SELECT\
row_number() over(order by 1)+100 id,\
REPLACE(pname,\'\"\',\'\') pname,cname,rname,sname,\
case\
when ncname like \'%社区居民委员会%\' then
replace(ncname,\'社区居民委员会\',\'\')\
when ncname like \'%居民委员会%\' then
replace(ncname,\'居民委员会\',\'\')\
when ncname like \'%社区居委会%\' then
replace(ncname,\'社区居委会\',\'\')\
when ncname like \'%村民委员会%\' then
replace(ncname,\'村民委员会\',\'\')\
when ncname like \'%村委会%\' then replace(ncname,\'村委会\',\'\')\
else ncname\
end ncname\
from mjy.wc_ods\
where (replace(pname,\'\"\',\'\') in (\'北京市\', \'天津市\',
\'河北省\'))and ncname != \'名称\' ;

![](media/image34.png){width="5.768055555555556in"
height="5.361805555555556in"}

##### 体育运动场馆

###### 地域维度表

create database dim_sport;\
\
create table dim_sport.national as\
SELECT\
row_number() over(order by 1)+100 id,\
REPLACE(pname,\'\"\',\'\') pname,cname,rname,sname,\
case\
when ncname like \'%社区居民委员会%\' then
replace(ncname,\'社区居民委员会\',\'\')\
when ncname like \'%居民委员会%\' then
replace(ncname,\'居民委员会\',\'\')\
when ncname like \'%社区居委会%\' then
replace(ncname,\'社区居委会\',\'\')\
when ncname like \'%村民委员会%\' then
replace(ncname,\'村民委员会\',\'\')\
when ncname like \'%村委会%\' then replace(ncname,\'村委会\',\'\')\
else ncname\
end ncname\
from ods_sport.national\
where pname =\'\"北京市\"\' and ncname !=\'名称\';

![](media/image34.png){width="5.768055555555556in"
height="5.361805555555556in"}

##### KTV

###### 地域维度表

create table dim_ktv.loc as\
SELECT\
row_number() over(order by 1)+100 id,\
REPLACE(pname,\'\"\',\'\') pname,cname,rname,sname,\
case\
when ncname like \'%社区居民委员会%\' then
replace(ncname,\'社区居民委员会\',\'\')\
when ncname like \'%居民委员会%\' then
replace(ncname,\'居民委员会\',\'\')\
when ncname like \'%社区居委会%\' then
replace(ncname,\'社区居委会\',\'\')\
when ncname like \'%村民委员会%\' then
replace(ncname,\'村民委员会\',\'\')\
when ncname like \'%村委会%\' then replace(ncname,\'村委会\',\'\')\
else ncname\
end ncname\
from ktv.ktv_loc_ods\
where (replace(pname,\'\"\',\'\') in (\'北京市\'))and ncname != \'名称\'
;

![](media/image34.png){width="5.768055555555556in"
height="5.361805555555556in"}

#### DWD

**景点**

##### I）拉宽路线字段 {#i拉宽路线字段 .标题6}

\-- 开启 非严格模式\
set hive.exec.dynamic.partition.mode=nonstrict;\
\-- 拉宽基本信息\
create table if not exists mjy_dwd.route_dwd as\
with t1 as (\
SELECT\
wc.\* ,\
CASE\
WHEN route = \'暂无数据\' THEN 0\
WHEN route LIKE \'%号%\' THEN 1\
ELSE 0\
END AS subway,\
CASE\
WHEN route = \'暂无数据\' THEN 0\
WHEN route LIKE \'%号%\' AND route LIKE \'%路%\' THEN 1\
WHEN route NOT LIKE \'%号%\' THEN 1\
ELSE 0\
END AS bus\
FROM mjy.wc\
) select \* from t1;

![](media/image35.png){width="5.768055555555556in"
height="5.160416666666666in"}

##### ii）拉宽地址字段 {#ii拉宽地址字段 .标题6}

\-- 创建外部表，指定字符集为 UTF-8\
CREATE EXTERNAL TABLE IF NOT EXISTS mjy_dwd.wc_dwd (\
address String,\
province STRING,\
city STRING,\
district STRING,\
detail_address STRING\
)\
ROW FORMAT DELIMITED\
FIELDS TERMINATED BY \'\\t\'\
STORED AS TEXTFILE\
TBLPROPERTIES (\'charset\'=\'UTF-8\');\
\-- 插入数据\
INSERT OVERWRITE TABLE mjy_dwd.wc_dwd\
SELECT\
address,\
\-- 提取省（增加调试信息）\
CASE\
WHEN address = \'-1\' OR address IS NULL OR trim(address) = \'\' THEN
NULL\
WHEN regexp_extract(address, \'\^(北京市\|上海市\|天津市\|重庆市)\', 1)
!= \'\'\
THEN regexp_extract(address, \'\^(北京市\|上海市\|天津市\|重庆市)\', 1)\
WHEN regexp_extract(address, \'\^(.\*?省)\', 1) != \'\'\
THEN regexp_extract(address, \'\^(.\*?省)\', 1)\
ELSE NULL\
END AS province,\
\-- 提取市（增加调试信息）\
CASE\
WHEN address = \'-1\' OR address IS NULL OR trim(address) = \'\' THEN
NULL\
WHEN regexp_extract(address, \'\^(北京市\|上海市\|天津市\|重庆市)\', 1)
!= \'\'\
THEN regexp_extract(address, \'\^(北京市\|上海市\|天津市\|重庆市)\', 1)\
WHEN regexp_extract(address, \'\^.\*?省(.\*?市)\', 1) != \'\'\
THEN regexp_extract(address, \'\^.\*?省(.\*?市)\', 1)\
WHEN regexp_extract(address, \'\^(.\*?市)\', 1) != \'\'\
THEN regexp_extract(address, \'\^(.\*?市)\', 1)\
ELSE NULL\
END AS city,\
\-- 提取区（增加调试信息）\
CASE\
WHEN address = \'-1\' OR address IS NULL OR trim(address) = \'\' THEN
NULL\
WHEN regexp_extract(address,
\'\^(北京市\|上海市\|天津市\|重庆市)(.\*?区)\', 2) != \'\'\
THEN regexp_extract(address,
\'\^(北京市\|上海市\|天津市\|重庆市)(.\*?区)\', 2)\
WHEN regexp_extract(address, \'\^.\*?市(.\*?区)\', 1) != \'\'\
THEN regexp_extract(address, \'\^.\*?市(.\*?区)\', 1)\
WHEN regexp_extract(address, \'\^(.\*?区)\', 1) != \'\'\
THEN regexp_extract(address, \'\^(.\*?区)\', 1)\
ELSE NULL\
END AS district,\
\-- 提取详细地址（保持不变）\
CASE\
WHEN address = \'-1\' OR address IS NULL OR trim(address) = \'\' THEN
NULL\
WHEN regexp_extract(address,
\'\^(北京市\|上海市\|天津市\|重庆市).\*?区\', 0) != \'\'\
THEN regexp_replace(address,
\'\^(北京市\|上海市\|天津市\|重庆市).\*?区\', \'\')\
WHEN regexp_extract(address, \'\^.\*?区\', 0) != \'\'\
THEN regexp_replace(address, \'\^.\*?区\', \'\')\
WHEN regexp_extract(address, \'\^.\*?市\', 0) != \'\'\
THEN regexp_replace(address, \'\^.\*?市\', \'\')\
WHEN regexp_extract(address, \'\^.\*?省\', 0) != \'\'\
THEN regexp_replace(address, \'\^.\*?省\', \'\')\
ELSE address\
END AS detail_address\
FROM mjy.wc;

![](media/image36.png){width="5.768055555555556in"
height="3.4055555555555554in"}

##### iii）整合所有dwd {#iii整合所有dwd .标题6}

CREATE TABLE mjy_dwd.wczh_dwd_final AS\
SELECT DISTINCT\
r.scenery_spot,\
r.brief_introduction,\
r.phone,\
r.best_time_to_visit,\
r.recommended_length_of_visit,\
r.price,\
r.open_time,\
r.introduction,\
COALESCE(r.address, w.address) AS address,\
r.route,\
r.etl_times,\
w.province,\
w.city,\
w.district,\
w.detail_address,\
r.subway,\
r.bus\
FROM\
mjy_dwd.route_dwd r\
LEFT JOIN\
mjy_dwd.wc_dwd w\
ON\
r.address = w.address;

![](media/image37.png){width="5.768055555555556in"
height="4.054166666666666in"}

**电影**

##### i）拉宽类型字段 {#i拉宽类型字段 .标题6}

drop table if exists wenyu_dwd.dianying_dwd_parquent;\
create table wenyu_dwd.dianying_dwd_parquent (\
name STRING,\
score STRING,\
type STRING,\
country STRING,\
director STRING,\
star STRING,\
synopsis STRING,\
Plot_of_a_play_or_opera STRING,\
Comedy STRING,\
Actions STRING,\
Love STRING,\
Science_fiction STRING,\
Suspense STRING,\
Commit_a_crime STRING,\
Anime STRING,\
English STRING,\
Fantastic STRING,\
Chilling STRING,\
Costume STRING\
)comment \'电影信息整合表\'\
STORED AS PARQUET;\
\--插入数据\
insert OVERWRITE TABLE wenyu_dwd.dianying_dwd_parquent\
select name,score,type,country,director,star,synopsis,\
case when type like \'%剧情%\' then \'1\' else \'0\' end as
Plot_of_a_play_or_opera,\
case when type like \'%喜剧%\' then \'1\' else \'0\' end as Comedy,\
case when type like \'%动作%\' then \'1\' else \'0\' end as Actions,\
case when type like \'%爱情%\' then \'1\' else \'0\' end as Love,\
case when type like \'%科幻%\' then \'1\' else \'0\' end as
Science_fiction,\
case when type like \'%悬疑%\' then \'1\' else \'0\' end as Suspense,\
case when type like \'%犯罪%\' then \'1\' else \'0\' end as
Commit_a_crime,\
case when type like \'%动画%\' then \'1\' else \'0\' end as Anime,\
case when type like \'%英语%\' then \'1\' else \'0\' end as English,\
case when type like \'%奇幻%\' then \'1\' else \'0\' end as Fantastic,\
case when type like \'%惊悚%\' then \'1\' else \'0\' end as Chilling,\
case when type like \'%古装%\' then \'1\' else \'0\' end as Costume\
from wenyu.dianying;

![](media/image38.png){width="5.768055555555556in"
height="5.406988188976378in"}

**体育运动场馆**

##### i）拉宽地址字段 {#i拉宽地址字段 .标题6}

create table dwd_sport.address as\
with t1 as (\
\-- 从 dim_sport.national 表中选择城市级别的名称，去重\
select rname\
from dim_sport.national\
group by rname\
),\
t2 as (\
\-- 选择市级和省级信息\
select rname, sname\
from dim_sport.national\
group by rname, sname\
),\
t3 as (\
\-- 选择市、省、乡镇信息\
select rname, sname, ncname\
from dim_sport.national\
group by rname, sname, ncname\
)\
,t4 AS (\
SELECT\
s.\*,\
COALESCE(t3.rname, t2.rname, t1.rname, \'未知\') AS region,\
COALESCE(t3.sname, t2.sname, \'未知\') AS street,\
COALESCE(t3.ncname, \'未知\') AS township,\
ROW_NUMBER() OVER (\
PARTITION BY s.name\
ORDER BY\
CASE WHEN t3.ncname IS NOT NULL THEN 3\
WHEN t2.sname IS NOT NULL THEN 2\
WHEN t1.rname IS NOT NULL THEN 1\
ELSE 0 END DESC\
) AS rn\
FROM ods_sport.sport_new s\
LEFT JOIN (SELECT rname FROM dim_sport.national GROUP BY rname) t1\
ON INSTR(s.address, t1.rname) \> 0\
LEFT JOIN (SELECT rname, sname FROM dim_sport.national GROUP BY rname,
sname) t2\
ON INSTR(s.address, t2.sname) \> 0\
LEFT JOIN (SELECT rname, sname, ncname FROM dim_sport.national GROUP BY
rname, sname, ncname) t3\
ON INSTR(s.address, t3.ncname) \> 0\
)SELECT\
\*\
FROM t4\
WHERE rn = 1;

![](media/image39.png){width="5.768055555555556in" height="4.23125in"}

**演出**

##### i）拉宽价格字段 {#i拉宽价格字段 .标题6}

CREATE table travel_dwd.tmp_price_yanchu AS\
SELECT \*\
,\
\-- 提取起始价格（去除前后空格并转换为数值）\
CAST(TRIM(SUBSTRING_INDEX(price, \'-\', 1))AS DECIMAL(10,2)) AS
min_price,\
\-- 提取结束价格（兼容无分隔符的异常数据）\
CASE\
WHEN LOCATE(\'-\', price) \> 0\
THEN CAST(TRIM(SUBSTRING_INDEX(price, \'-\', -1)) AS DECIMAL(10,2))\
ELSE CAST(TRIM(price) AS DECIMAL(10,2)) \-- 若无分隔符，视为单价格\
END AS max_price\
FROM travel_ods.travel_ods_yanchu;

![](media/image40.png){width="5.768055555555556in"
height="1.1840277777777777in"}

##### ii）拉宽售票状态字段 {#ii拉宽售票状态字段 .标题6}

CREATE table travel_dwd.tmp_price_cond_yanchu AS\
SELECT \*,\
CASE WHEN cond = \'即将预售\' THEN 1 ELSE 0 END AS is_coming_presale,\
CASE WHEN cond = \'预售\' THEN 1 ELSE 0 END AS is_presale,\
CASE WHEN cond = \'即将开售\' THEN 1 ELSE 0 END AS is_coming_sale,\
CASE WHEN cond = \'在售中\'THEN 1 ELSE 0 END AS is_onsale\
FROM travel_dwd.tmp_price_yanchu;\
create table if not exists travel_dwd.travel_dwd_yanchu(\
name String,\
\`time\` String,\
place String,\
price String,\
cond String,\
etl_times String,\
min_price String,\
max_price String,\
is_coming_presale String,\
is_presale String,\
is_coming_sale String,\
is_onsale String\
)comment \'演出信息明细表\'\
stored as parquet;\
insert overwrite table travel_dwd.travel_dwd_yanchu\
select \* from
travel_dwd.tmp_price_cond_yanchu;![](media/image41.png){width="5.768055555555556in"
height="1.8944444444444444in"}

**KTV**

##### i）拉宽地址字段 {#i拉宽地址字段-1 .标题6}

CREATE table dim_ktv.ktv_dwd STORED AS PARQUET as\
with t1 as (\
select rname\
from dim_ktv.loc\
group by rname\
),t2 as (\
select rname,sname\
from dim_ktv.loc\
group by rname,sname\
),t3 as (\
select rname,sname,ncname\
from dim_ktv.loc\
group by rname,sname,ncname\
)select\
l.adress,\
COALESCE(COALESCE(COALESCE(t1.rname, t2.rname),t3.rname),\'未知\') AS
city,\
COALESCE(COALESCE(t2.sname, t3.sname),\'未知\') AS region,\
COALESCE(t3.ncname, \'未知\') AS township\
from ktv.ktv_lzo l\
left join t1 ON INSTR(l.adress,t1.rname) \<\> 0\
left join t2 ON INSTR(l.adress,t2.sname) \<\> 0\
left join t3 ON INSTR(l.adress,t3.ncname) \<\> 0;\
CREATE TABLE dwd_ktv.ktv_dwd (\
name STRING,\
comment STRING,\
consume STRING,\
service STRING,\
enviroment STRING,\
cost STRING,\
adress STRING,\
etl_times STRING,\
city STRING,\
region STRING,\
township STRING\
);\
\-- 插入数据到 dwd_ktv.ktv_dwd 表\
INSERT INTO TABLE dwd_ktv.ktv_dwd\
SELECT\
k.name,\
k.comment,\
k.consume,\
k.service,\
k.enviroment,\
k.cost,\
k.adress,\
k.etl_times,\
t1.city,\
t1.region,\
t1.township\
FROM (\
SELECT d.city ,d.region,d.township,d.adress\
FROM dim_ktv.ktv_dwd d\
) AS t1\
JOIN ktv.ktv_lzo k ON k.adress = t1.adress;\
\-- 创建一个新表来存储清洗后的数据\
CREATE TABLE dwd_ktv.ktv_dwd_a AS\
SELECT\
\*\
FROM (\
SELECT\
\*,\
ROW_NUMBER() OVER (PARTITION BY name ORDER BY etl_times) as rn\
FROM\
dwd_ktv.ktv_dwd\
) t\
WHERE\
rn = 1;

![](media/image42.png){width="5.768055555555556in"
height="2.4347222222222222in"}

#### DWS

**景点**

\-- 创建库

create database if not exists mjy_dws;\
\-- 开启 非严格模式\
set hive.exec.dynamic.partition.mode=nonstrict;\
create table if not exists mjy_dws.wczh_dwd_final(\
open_time String comment \'开放时间\',\
address String comment \'地址\',\
province String comment \'省\',\
city String comment \'市\',\
district String comment \'区\',\
detail_address String comment \'详细地址\',\
route String comment \'路线\',\
subway Int comment \'有地铁\',\
bus Int comment \'有公交\',\
price String comment \'景区价格\',\
best_time_to_visit_ratio String comment \'全年，四季占比\',\
recommended_length_of_visit_ratio STRING comment \'游览时两小时占比\',\
bus_ratio Int comment \'公交占比\',\
subway_ratio Int comment \'地铁占比\',\
no_phone_ratio String comment \'无电话占比\',\
free_price_ratio String comment \'免费景区占比\'\
)\
comment \'景点局部聚合表\'\
partitioned by (etl_time String)\
stored as parquet;\
insert into mjy_dws.wczh_dwd_final partition(etl_time)\
select\
open_time,\
address,\
province,\
city,\
district,\
detail_address,\
route ,\
subway ,\
bus,\
price,\
ROUND(SUM(CASE WHEN best_time_to_visit RLIKE
\'全年\|四季皆宜\|全年皆宜\' THEN 1 ELSE 0 END) \* 100.0 / COUNT(\*), 2)
best_time_to_visit_ratio,\
ROUND(SUM(CASE WHEN recommended_length_of_visit RLIKE
\'两小时\|2小时\|约两小时\|两小时左右\' THEN 1 ELSE 0 END) \* 100.0 /
COUNT(\*),\
2) AS percentage,\
ROUND(SUM(bus) \* 100.0 / COUNT(\*), 2) AS bus_ratio,\
ROUND(SUM(subway) \* 100.0 / COUNT(\*), 2) AS busway_ratio,\
ROUND(SUM(CASE WHEN phone = \'暂无\' THEN 1 ELSE 0 END) \* 100.0 /
COUNT(\*), 2) AS no_phone_ratio,\
ROUND(SUM(CASE WHEN price LIKE \'%免费%\' THEN 1 ELSE 0 END) \* 100.0 /
COUNT(\*), 2) AS free_price_ratio,\
\
\'\${hivevar:etl_times}\' AS etl_time \-- 使用脚本传入的时间变量\
from mjy_dwd.wczh_dwd_final\
group by open_time,\
address,\
province,\
city,\
district,\
detail_address,\
route,\
subway,\
bus,\
price;

![](media/image43.png){width="5.768055555555556in"
height="3.2243055555555555in"}

**电影**

\--创建表

SET hive.exec.dynamic.partition.mode=nonstrict;\
drop table if exists wenyu_dws.dianying_dws_parquet;\
CREATE TABLE if NOT EXISTS wenyu_dws.dianying_dws_parquet(\
name String comment \'电影名称\',\
director String comment \'导演\',\
max_score String comment \'所有电影最高评分\',\
min_score String comment \'所有电影最低评分\',\
avg_score String comment \'所有电影平均评分\',\
Plot_of_a_play_or_opera_ratio String comment \'剧情类型电影占比\',\
Comedy_ratio String comment \'喜剧类型电影占比\',\
Actions_ratio String comment \'动作类型电影占比\',\
Love_ratio String comment \'爱情类型电影占比\',\
Science_fiction_ratio String comment \'科幻类型电影占比\',\
Suspense_ratio String comment \'悬疑类型电影占比\',\
Commit_a_crime_ratio String comment \'犯罪类型电影占比\',\
Anime_ratio String comment \'动画类型电影占比\',\
English_ratio String comment \'英语类型电影占比\',\
Fantastic_ratio String comment \'奇幻类型电影占比\',\
Chilling_ratio String comment \'惊悚类型电影占比\',\
Costume_ratio String comment \'古装类型电影占比\'\
)comment \'电影信息局部聚合表\'\
partitioned by(etl_time String)\
STORED AS PARQUET;\
\--加载数据\`\
insert into wenyu_dws.dianying_dws_parquet partition(etl_time)\
select\
name,\
director,\
max(score) max_score ,\
min(score) min_score,\
avg(score) avg_score,\
sum(Plot_of_a_play_or_opera)/count(\*) Plot_of_a_play_or_opera_ratio,\
sum(Comedy)/count(\*) Comedy_ratio,\
sum(Actions)/count(\*) Actions_ratio,\
sum(Love)/count(\*) Love_ratio,\
sum(Science_fiction)/count(\*) Science_fiction_ratio,\
sum(Suspense)/count(\*) Suspense_ratio,\
sum(Commit_a_crime)/count(\*) Commit_a_crime_ratio,\
sum(Anime)/count(\*) Anime_ratio,\
sum(English)/count(\*) English_ratio,\
sum(Fantastic)/count(\*) Fantastic_ratio,\
sum(Chilling)/count(\*) Chilling_ratio,\
sum(Costume)/count(\*) Costume_ratio,\
date_format(CURRENT_DATE(),\'yyyy-MM-dd\')\
from wenyu_dwd.dianying_dwd_parquent\
group by name,director;

![](media/image44.png){width="5.768055555555556in"
height="3.2958333333333334in"}

**体育运动场馆**

设计思路

按类型维度汇总的运动场馆信息表

按城市维度汇总的运动场馆信息表

按城市、区域和运动类型维度汇总

###### 按类型维度汇总的运动场馆信息表

create table if not exists dws_sport.type_summary (\
type string comment \'类型\',\
sport_count int comment \'运动场馆数量\',\
region_count int comment \'覆盖市区数量\'\
) comment \'按类型维度汇总的运动场馆信息表\'\
partitioned by (etl_times string)\
stored as parquet;\
SET hive.exec.dynamic.partition.mode=nonstrict;\
insert overwrite table dws_sport.type_summary partition(etl_times)\
select\
type,\
count(\*) as sport_count,\
count(distinct address) as region_count,\
date_format(current_timestamp(),\"yyyy-mm-dd hh\") as etl_times\
from dwd_sport.address\
where type != \'暂无\'\
group by type;

![](media/image45.png){width="5.768055555555556in"
height="2.817361111111111in"}

###### 按城市维度汇总的运动场馆信息表

create table if not exists dws_sport.city_summary (\
city string comment \'城市\',\
region string comment \'所在区\',\
sport_count int comment \'运动场馆数量\'\
) comment \'按城市维度汇总的运动场馆信息表\'\
partitioned by (etl_times string)\
stored as parquet;\
SET hive.exec.dynamic.partition.mode=nonstrict;\
insert overwrite table dws_sport.city_summary partition(etl_times)\
select\
city as city,\
region as region,\
count(\*) as sport_count,\
date_format(current_timestamp(),\"yyyy-mm-dd hh\") as etl_times\
from dwd_sport.address\
group by city,region;

![](media/image46.png){width="5.768055555555556in"
height="4.9118055555555555in"}

###### 按城市、区域和运动类型维度汇总

CREATE TABLE IF NOT EXISTS dws_sport.sport_summary (\
city STRING COMMENT \'城市名称，如\"北京市\"\',\
region STRING COMMENT \'所在行政区，如\"朝阳区\"\',\
sport_type STRING COMMENT \'运动场馆类型，如\"篮球馆\"、\"游泳馆\"等\',\
venue_count INT COMMENT \'该类型运动场馆数量\',\
type_percentage DECIMAL(5,2) COMMENT
\'该运动类型在本区域内的占比百分比\',\
\-- 数据质量监控字段\
data_date STRING COMMENT \'数据日期(yyyy-MM-dd)\',\
data_hour STRING COMMENT \'数据小时(HH)\'\
) COMMENT \'运动场馆统计分析表-按城市、区域和运动类型维度汇总\'\
PARTITIONED BY (etl_date STRING COMMENT \'ETL处理日期(yyyy-MM-dd)\')\
STORED AS PARQUET\
TBLPROPERTIES (\
\'parquet.compression\'=\'SNAPPY\',\
\'auto.purge\'=\'true\'\
);\
INSERT OVERWRITE TABLE dws_sport.sport_summary PARTITION(etl_date)\
SELECT\
city,\
region,\
sport_type,\
venue_count,\
type_percentage,\
data_date,\
data_hour,\
etl_date\
FROM (\
SELECT\
city,\
region,\
type AS sport_type,\
COUNT(\*) AS venue_count,\
ROUND(COUNT(\*) \* 100.0 / SUM(COUNT(\*)) OVER(PARTITION BY city,
region), 2) AS type_percentage,\
DATE_FORMAT(current_timestamp(), \'yyyy-MM-dd\') AS data_date,\
DATE_FORMAT(current_timestamp(), \'HH\') AS data_hour,\
DATE_FORMAT(current_timestamp(), \'yyyy-MM-dd\') AS etl_date\
FROM dwd_sport.address\
WHERE city IS NOT NULL\
AND region IS NOT NULL\
AND type IS NOT NULL\
GROUP BY city, region, type\
HAVING COUNT(\*) \> 0\
) venue_stats;

![](media/image47.png){width="5.768055555555556in"
height="3.076388888888889in"}

**演出**

###### 按演出地点维度汇总

create database if not exists travel_dws;\
create table if not exists travel_dws.travel_dws_yanchu_place(\
place String comment\'演出地点\',\
name_count int comment \'演出数量\'\
)comment\'演出信息局部聚合表\'\
partitioned by(etl_time String)\
stored as parquet;\
SET hive.exec.dynamic.partition = true;\
\-- 切换为非严格模式\
SET hive.exec.dynamic.partition.mode = nonstrict;\
insert into travel_dws.travel_dws_yanchu_place partition(etl_time)\
select\
place,\
count(name) name_count,\
date_format(current_date(),\'yyyy-MM-dd\') as etl_time\
from travel_dwd.travel_dwd_yanchu\
group by place;

![](media/image48.png){width="5.7625in" height="5.720138888888889in"}

###### 按演出名称维度汇总

create table travel_dws.travel_dws_yanchu_name(\
name String comment\'演出名称\',\
min_price decimal(10,2) comment\'最低价格\',\
max_price decimal(10,2) comment\'最高价格\'\
)comment\'演出名称信息局部聚合表\'\
partitioned by(etl_time String)\
stored as parquet;\
SET hive.exec.dynamic.partition = true;\
\-- 切换为非严格模式\
SET hive.exec.dynamic.partition.mode = nonstrict;\
insert into travel_dws.travel_dws_yanchu_name partition(etl_time)\
select\
name,\
min(min_price) min_price,\
max(max_price) max_price,\
date_format(current_date(),\'yyyy-MM-dd\') as elt_time\
from travel_dwd.travel_dwd_yanchu\
group by name;![](media/image49.png){width="5.759722222222222in"
height="4.585416666666666in"}

**KTV**

\--把 Hive 动态分区模式设定为非严格模式。

SET hive.exec.dynamic.partition.mode=nonstrict;

create table if not exists ktv_dws.ktv_dwd_c(

city string ,

max_consume string,

min_consume string,

max_service string,

min_service string ,

max_enviroment string ,

min_enviroment string ,

max_cost string,

min_cost string ,

sum_comment string ,

comment_ratio string ,

avg_consume string ,

avg_service string ,

avg_emviroment string ,

avg_cost string ,

avg_comment string

)PARTITIONED BY (etl_time STRING)

stored as parquet;

INSERT OVERWRITE TABLE ktv_dws.ktv_dwd_c PARTITION (etl_time)

SELECT

city,

max(consume) AS max_consume,

min(consume) AS min_consume,

max(service) AS max_service,

min(service) AS min_service,

max(enviroment) AS max_enviroment,

min(enviroment) AS min_enviroment,

max(cost) AS max_cost,

min(cost) AS min_cost,

sum(comment) AS sum_comment,

count(comment) / count(\*) AS comment_ratio,

avg(consume) AS avg_consume,

avg(service) AS avg_service,

avg(enviroment) AS avg_emviroment,

avg(cost) AS avg_cost,

avg(comment) AS avg_comment,

date_format(CURRENT_DATE(), \'yyyy-MM-dd\') AS etl_time

FROM

dwd_ktv.ktv_dwd_a

GROUP BY

city;

#### ADS

景点

###### 朝阳区公交覆盖情况分析表

> \-- 创建朝阳区公交覆盖分析表\
> create database if not exists mjy_ads;\
> CREATE TABLE IF NOT EXISTS mjy_ads.cy_bus (\
> district STRING COMMENT \'区\',\
> bus_ratio DECIMAL(5,2) COMMENT \'公交覆盖率\',\
> category STRING COMMENT \'分类(完全覆盖/其他)\',\
> count INT COMMENT \'数量\',\
> percentage DECIMAL(5,2) COMMENT \'占比\'\
> )\
> COMMENT \'朝阳区公交覆盖情况分析表\';\
> \-- 计算朝阳区公交覆盖情况饼图数据\
> INSERT OVERWRITE TABLE mjy_ads.cy_bus\
> SELECT\
> \'朝阳区\' AS district,\
> CASE\
> WHEN s.category = \'完全覆盖(100%)\' THEN 100.00\
> ELSE NULL\
> END AS bus_ratio,\
> s.category,\
> s.count,\
> ROUND(s.count \* 100.0 / t.total_count, 2) AS percentage\
> FROM (\
> SELECT\
> CASE\
> WHEN bus_ratio = 100 THEN \'完全覆盖(100%)\'\
> ELSE \'部分覆盖(\<100%)\'\
> END AS category,\
> COUNT(\*) AS count\
> FROM (\
> \-- 获取所有朝阳区数据\
> SELECT\
> district,\
> bus_ratio\
> FROM mjy_dws.wczh_dwd_final\
> WHERE district = \'朝阳区\'\
> ) cy_data\
> GROUP BY\
> CASE\
> WHEN bus_ratio = 100 THEN \'完全覆盖(100%)\'\
> ELSE \'部分覆盖(\<100%)\'\
> END\
> ) s\
> CROSS JOIN (\
> \-- 计算总数\
> SELECT COUNT(\*) AS total_count\
> FROM mjy_dws.wczh_dwd_final\
> WHERE district = \'朝阳区\'\
> ) t;

###### 最佳游览时间分析表

> \-- 创建最佳游览时间分析表\
> CREATE TABLE IF NOT EXISTS mjy_ads.jd_qn (\
> category STRING COMMENT \'分类(全年适宜/非全年适宜)\',\
> record_count INT COMMENT \'记录数量\',\
> percentage DECIMAL(5,2) COMMENT \'占比百分比\',\
> etl_date STRING COMMENT \'ETL处理日期\'\
> )\
> COMMENT \'景点全年适宜游览时间占比分析\';\
> \-- 计算全年适宜占比数据\
> INSERT OVERWRITE TABLE mjy_ads.jd_qn\
> SELECT\
> CASE\
> WHEN best_time_to_visit_ratio = 100 THEN \'全年适宜(100%)\'\
> ELSE \'非全年适宜(\<100%)\'\
> END AS category,\
> COUNT(\*) AS record_count,\
> ROUND(COUNT(\*) \* 100.0 / SUM(COUNT(\*)) OVER(), 2) AS percentage,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> FROM mjy_dws.wczh_dwd_final\
> GROUP BY\
> CASE\
> WHEN best_time_to_visit_ratio = 100 THEN \'全年适宜(100%)\'\
> ELSE \'非全年适宜(\<100%)\'\
> END;

###### 区域免费景点分析表

> \-- 创建区域免费景点分析表（柱状图专用）\
> CREATE TABLE IF NOT EXISTS mjy_ads.free_zb (\
> district STRING COMMENT \'行政区\',\
> free_attraction_percent DECIMAL(5,2) COMMENT \'完全免费景点百分比\',\
> total_attractions INT COMMENT \'景点总数\',\
> free_attractions INT COMMENT \'完全免费景点数\',\
> etl_date STRING COMMENT \'ETL处理日期\'\
> )\
> COMMENT \'五大城区完全免费景点占比分析(柱状图数据)\';\
> \-- 计算五大城区完全免费景点占比\
> \-- 计算五大城区完全免费景点占比\
> INSERT OVERWRITE TABLE mjy_ads.free_zb\
> SELECT\
> district,\
> ROUND(free_attractions \* 100.0 / total_attractions, 2) AS
> free_attraction_percent,\
> total_attractions,\
> free_attractions,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> FROM (\
> SELECT\
> district,\
> COUNT(\*) AS total_attractions,\
> SUM(CASE WHEN free_price_ratio = 100 THEN 1 ELSE 0 END) AS
> free_attractions\
> FROM mjy_dws.wczh_dwd_final\
> WHERE district IN (\'朝阳区\', \'海淀区\', \'东城区\', \'西城区\',
> \'丰台区\')\
> GROUP BY district\
> ) subquery\
> ORDER BY free_attraction_percent DESC;

###### 公交地铁覆盖分析表

> \-- 创建公交地铁覆盖分析表\
> CREATE TABLE IF NOT EXISTS mjy_ads.bus_subway (\
> district STRING COMMENT \'行政区\',\
> bus_only_percent DECIMAL(5,2) COMMENT \'仅公交全覆盖百分比\',\
> subway_only_percent DECIMAL(5,2) COMMENT \'仅地铁全覆盖百分比\',\
> both_percent DECIMAL(5,2) COMMENT \'公交地铁双覆盖百分比\',\
> neither_percent DECIMAL(5,2) COMMENT \'无全覆盖百分比\',\
> total_locations INT COMMENT \'地点总数\',\
> etl_date STRING COMMENT \'ETL处理日期\'\
> )\
> COMMENT \'五大城区公交地铁全覆盖占比分析\';\
> \-- 设置动态分区参数\
> SET hive.exec.dynamic.partition=true;\
> SET hive.exec.dynamic.partition.mode=nonstrict;\
> \-- 计算五大城区公交地铁全覆盖占比\
> INSERT OVERWRITE TABLE mjy_ads.bus_subway\
> SELECT\
> district,\
> ROUND(bus_only \* 100.0 / total_locations, 2) AS bus_only_percent,\
> ROUND(subway_only \* 100.0 / total_locations, 2) AS
> subway_only_percent,\
> ROUND(both_covered \* 100.0 / total_locations, 2) AS both_percent,\
> ROUND(neither_covered \* 100.0 / total_locations, 2) AS
> neither_percent,\
> total_locations,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> FROM (\
> SELECT\
> district,\
> COUNT(\*) AS total_locations,\
> SUM(CASE WHEN bus_ratio = 100 AND subway_ratio \< 100 THEN 1 ELSE 0
> END) AS bus_only,\
> SUM(CASE WHEN subway_ratio = 100 AND bus_ratio \< 100 THEN 1 ELSE 0
> END) AS subway_only,\
> SUM(CASE WHEN bus_ratio = 100 AND subway_ratio = 100 THEN 1 ELSE 0
> END) AS both_covered,\
> SUM(CASE WHEN bus_ratio \< 100 AND subway_ratio \< 100 THEN 1 ELSE 0
> END) AS neither_covered\
> FROM mjy_dws.wczh_dwd_final\
> WHERE district IN (\'朝阳区\', \'海淀区\', \'东城区\', \'西城区\',
> \'丰台区\')\
> GROUP BY district\
> ) subquery\
> ORDER BY both_percent DESC;

###### 全年适宜游览时间分析表

> \-- 创建全年适宜游览时间分析表\
> CREATE TABLE IF NOT EXISTS mjy_ads.qnsy (\
> district STRING COMMENT \'行政区\',\
> year_round_percent DECIMAL(5,2) COMMENT \'全年适宜景点百分比\',\
> total_attractions INT COMMENT \'景点总数\',\
> year_round_count INT COMMENT \'全年适宜景点数\',\
> etl_date STRING COMMENT \'ETL处理日期\'\
> )\
> COMMENT \'六大城区全年适宜游览景点占比分析\';\
> \-- 计算六大城区全年适宜游览占比\
> INSERT OVERWRITE TABLE mjy_ads.qnsy\
> SELECT\
> district,\
> ROUND(year_round_count \* 100.0 / total_attractions, 2) AS
> year_round_percent,\
> total_attractions,\
> year_round_count,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> FROM (\
> SELECT\
> district,\
> COUNT(\*) AS total_attractions,\
> SUM(\
> CASE\
> WHEN best_time_to_visit_ratio = 100 THEN 1\
> ELSE 0\
> END\
> ) AS year_round_count\
> FROM mjy_dws.wczh_dwd_final\
> WHERE district IN (\'朝阳区\', \'海淀区\', \'东城区\', \'西城区\',
> \'丰台区\', \'延庆区\')\
> GROUP BY district\
> ) subquery\
> ORDER BY year_round_percent DESC;

电影

###### 评分前一百的电影及其类型

> \--one建表（评分大于8.5的电影及其类型）\
> drop table if exists wenyu_ads.dianying_ads_top;\
> create table wenyu_ads.dianying_ads_top\
> (\
> id string COMMENT \'自增主键ID\',\
> name String comment \'电影名称\',\
> score String comment \'评分\',\
> type String comment \'类型\',\
> etl_times string COMMENT \'etl时间\'\
> )\
> comment \'评分前一百的电影及其类型\';\
> \--插入数据\
> insert into wenyu_ads.dianying_ads_top\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> max_score,\
> CASE\
> WHEN plot_of_a_play_or_opera_ratio= \'1.0\' THEN \'剧情类\'\
> WHEN Comedy_ratio= \'1.0\' THEN \'喜剧类\'\
> WHEN Actions_ratio= \'1.0\' THEN \'动作类\'\
> WHEN Love_ratio= \'1.0\' THEN \'爱情类\'\
> WHEN Science_fiction_ratio= \'1.0\' THEN \'科幻类\'\
> WHEN Suspense_ratio= \'1.0\' THEN \'悬疑类\'\
> WHEN commit_a_crime_ratio= \'1.0\' THEN \'犯罪类\'\
> WHEN Anime_ratio= \'1.0\' THEN \'动画类\'\
> WHEN English_ratio= \'1.0\' THEN \'英语类\'\
> WHEN Fantastic_ratio= \'1.0\' THEN \'奇幻类\'\
> WHEN Chilling_ratio= \'1.0\' THEN \'惊悚类\'\
> WHEN Costume_ratio= \'1.0\' THEN \'古装类\'\
> ELSE \'多种类型\'\
> END AS type,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM wenyu_dws.dianying_dws_parquet\
> WHERE max_score \>= \'8.5\'\
> ORDER BY max_score DESC;

###### 评分前十五的电影

> \--two建表（评分前十五电影）\
> drop table if exists wenyu_ads.dianying_ads_q15;\
> create table wenyu_ads.dianying_ads_q15\
> (\
> id string COMMENT \'自增主键ID\',\
> name String comment \'电影名称\',\
> score String comment \'评分\',\
> etl_times string COMMENT \'etl时间\'\
> )\
> comment \'评分前十五电影\';\
> \--插入数据\
> insert into wenyu_ads.dianying_ads_q15\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> max_score,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM wenyu_dws.dianying_dws_parquet\
> ORDER BY max_score DESC\
> LIMIT 15;

###### 评分前二十的科幻类电影

> \--three建表（评分前二十的科幻类电影）\
> drop table if exists wenyu_ads.dianying_ads_sctop20;\
> create table wenyu_ads.dianying_ads_sctop20\
> (\
> id string COMMENT \'自增主键ID\',\
> name String comment \'电影名称\',\
> score String comment \'评分\',\
> Science_fiction String comment \'科幻类型\',\
> etl_times string COMMENT \'etl时间\'\
> )\
> comment \'评分前二十的科幻类电影\';\
> \--插入数据\
> insert into wenyu_ads.dianying_ads_sctop20\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,max_score,\
> Science_fiction_ratio,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM wenyu_dws.dianying_dws_parquet\
> WHERE Science_fiction_ratio = \'1.0\' \--科幻类电影\
> ORDER BY max_score DESC\
> LIMIT 20;

###### 不同评分区间转换后的电影

> \--four创建表（不同评分区间转换后的电影）\
> drop table if exists wenyu_ads.dianying_ads_pingfen;\
> create table wenyu_ads.dianying_ads_pingfen\
> (\
> id string COMMENT \'自增主键ID\',\
> name String comment \'电影名称\',\
> score String comment \'转换后的评分\',\
> etl_times string COMMENT \'etl时间\'\
> )\
> comment \'不同评分区间转换后的电影\';\
> \
> \-- 插入数据\
> insert into wenyu_ads.dianying_ads_pingfen\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> CASE\
> WHEN max_score \>= \'8.5\' THEN \'评分较高\'\
> WHEN max_score \>= \'7.0\' AND max_score \< \'8.5\' THEN \'评分中等\'\
> WHEN max_score \<= \'7.0\' then \'评分较低\'\
> END AS score,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM wenyu_dws.dianying_dws_parquet\
> WHERE name !=\' \' \--电影名称不为空值\
> ORDER BY max_score DESC;

###### 评分8.0以上的喜剧类电影

> \--five建表（评分8.0以上的喜剧类电影）\
> drop table if exists wenyu_ads.dianying_ads_xjtop;\
> create table wenyu_ads.dianying_ads_xjtop\
> (\
> id string COMMENT \'自增主键ID\',\
> name String comment \'电影名称\',\
> score String comment \'评分\',\
> Comedy String comment \'喜剧类型\',\
> etl_times string COMMENT \'etl时间\'\
> )\
> comment \'评分8.0以上的喜剧类电影\';\
> \--插入数据\
> insert into wenyu_ads.dianying_ads_xjtop\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> max_score,\
> comedy_ratio,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM wenyu_dws.dianying_dws_parquet\
> WHERE Comedy_ratio = \'1.0\' and max_score \>= \'8.0\'\
> ORDER BY max_score DESC;

体育运动场馆

###### 北京市各区运动场馆TOP8排名表

> CREATE TABLE if not exists ads_sport.sport_top8 (\
> id string COMMENT \'自增主键ID\',\
> region STRING COMMENT \'地区名称\',\
> sport_count BIGINT COMMENT \'运动场馆数量\',\
> city_rank BIGINT COMMENT \'城市排名\',\
> etl_times string COMMENT \'etl时间\'\
> )COMMENT \'北京市各区运动场馆TOP8排名表\';\
> insert overwrite table ads_sport.sport_top8\
> select\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> region,sport_count,\
> rank() over (order by sport_count desc) as city_rank,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> from dws_sport.city_summary\
> where region != \'未知\'\
> order by sport_count desc\
> limit 8;

###### 北京市运动场馆数量TOP5

> CREATE TABLE if not exists ads_sport.share_top5 (\
> id string COMMENT \'自增主键ID\',\
> type STRING COMMENT \'运动场馆类型名称\',\
> sport_count bigint COMMENT \'运动场馆数量\',\
> brand_percent decimal(5,1) COMMENT \'类型占比（百分比）\',\
> etl_times string COMMENT \'etl时间\'\
> )COMMENT \'北京市运动场馆数量TOP5\';\
> insert overwrite table ads_sport.share_top5\
> select sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> trim(type),\
> sport_count,\
> round(sport_count\*100/sum(sport_count) over(), 1) as brand_percent,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> from dws_sport.type_summary\
> order by sport_count desc\
> limit 5;

###### 北京市运动场馆区域集中度TOP5

> CREATE TABLE IF NOT EXISTS ads_sport.city_region_concentration_top5 (\
> id STRING COMMENT \'自增主键ID\',\
> city STRING COMMENT \'城市名称\',\
> top_region STRING COMMENT \'最集中区域\',\
> concentration_percent DECIMAL(5,2) COMMENT \'集中度(%)\',\
> etl_times STRING COMMENT \'ETL时间\'\
> ) COMMENT \'北京市运动场馆区域集中度TOP5\';\
> \
> INSERT OVERWRITE TABLE ads_sport.city_region_concentration_top5\
> SELECT\
> sha2(concat(unix_timestamp(), city, rand()), 256) AS id,\
> city,\
> region AS top_region,\
> ROUND(MAX(venue_count)\*100/SUM(venue_count), 2) AS
> concentration_percent,\
> DATE_FORMAT(current_timestamp(), \'yyyy-MM-dd-HH\') AS etl_times\
> FROM dws_sport.sport_summary\
> GROUP BY city, region\
> ORDER BY concentration_percent DESC\
> LIMIT 5;

###### 北京区域乒乓球馆占比TOP3

> CREATE TABLE IF NOT EXISTS ads_sport.city_ping_pang_ratio_top3 (\
> id STRING COMMENT \'自增主键ID\',\
> region STRING COMMENT \'区域名称\',\
> ping_pang_percent DECIMAL(5,2) COMMENT \'乒乓球馆占比(%)\',\
> ratio_rank BIGINT COMMENT \'占比排名\',\
> etl_times STRING COMMENT \'ETL时间\'\
> ) COMMENT \'北京区域乒乓球馆占比TOP3\';\
> INSERT OVERWRITE TABLE ads_sport.city_ping_pang_ratio_top3\
> SELECT\
> sha2(concat(unix_timestamp(), region, rand()), 256) AS id,\
> region,\
> ROUND(SUM(CASE WHEN sport_type = \'乒乓球馆\' THEN venue_count ELSE 0
> END)\*100/SUM(venue_count), 2) AS ping_pang_percent,\
> RANK() OVER (ORDER BY ROUND(SUM(CASE WHEN sport_type = \'乒乓球馆\'
> THEN venue_count ELSE 0 END)\*100/SUM(venue_count), 2) DESC) AS
> ratio_rank,\
> DATE_FORMAT(current_timestamp(), \'yyyy-MM-dd-HH\') AS etl_times\
> FROM dws_sport.sport_summary\
> where region!=\'未知\'\
> GROUP BY region\
> HAVING SUM(venue_count) \> 50 \-- 只统计场馆总数超过50的城市\
> ORDER BY ping_pang_percent DESC\
> LIMIT 3;

###### 北京最受欢迎运动类型TOP3

> CREATE TABLE IF NOT EXISTS ads_sport.popular_sport_top3 (\
> id STRING COMMENT \'自增主键ID\',\
> sport_type STRING COMMENT \'运动类型\',\
> total_venues BIGINT COMMENT \'总场馆数\',\
> national_percent DECIMAL(5,2) COMMENT \'全国占比(%)\',\
> etl_times STRING COMMENT \'ETL时间\'\
> ) COMMENT \'北京最受欢迎运动类型TOP3\';\
> INSERT OVERWRITE TABLE ads_sport.popular_sport_top3\
> SELECT\
> sha2(concat(unix_timestamp(), sport_type, rand()), 256) AS id,\
> sport_type,\
> SUM(venue_count) AS total_venues,\
> ROUND(SUM(venue_count)\*100/(SELECT SUM(venue_count) FROM
> dws_sport.sport_summary), 2) AS national_percent,\
> DATE_FORMAT(current_timestamp(), \'yyyy-MM-dd-HH\') AS etl_times\
> FROM dws_sport.sport_summary\
> GROUP BY sport_type\
> ORDER BY total_venues DESC\
> LIMIT 3;

演出

###### 价格最高的演出top10

> create table if not exists travel_ads.ads_max_price_name(\
> id string COMMENT \'自增主键ID\',\
> name string comment\'演出名称\',\
> max_price decimal(10,2) comment \'售票价格\',\
> etl_time string comment \'etl处理日期\'\
> )\
> comment\'价格最高的演出top10\';\
> \--插入数据\
> insert overwrite table travel_ads.ads_max_price_name\
> select\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> \-\--将 max_price 字段的值转换为 DECIMAL(10,2)
> 类型。DECIMAL(10,2)：表示一个固定精度和小数位数 的十进制数：\
> \-\--10 是总位数（整数部分 + 小数部分）， 提供 8 位整数容量，覆盖 1000
> 以上数据\
> \-\--2 是小数位数，即小数点后保留 2 位\
> CAST(max_price AS DECIMAL(10,2)) AS max_price,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> from travel_dws.travel_dws_yanchu_name\
> where travel_dws_yanchu_name.name != \'null\'\
> order by max_price desc\
> limit 10;

###### 价格最低的演出top10

> \-\--价格最低的演出top10\
> create table if not exists travel_ads.ads_min_price_name (\
> id string COMMENT \'自增主键ID\',\
> name string comment\'演出名称\',\
> min_price decimal(10,2) comment \'售票价格\',\
> etl_time string comment \'etl处理日期\'\
> )\
> comment\'价格最低的演出top10\';\
> \-\--插入数据\
> insert overwrite table travel_ads.ads_min_price_name\
> select\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> name,\
> min_price,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> from travel_dws.travel_dws_yanchu_name\
> where travel_dws_yanchu_name.name != \'null\'\
> AND CAST(min_price AS DECIMAL(10,2)) BETWEEN 0 AND 9.99 \--
> 筛选个位数价格\
> order by min_price\
> limit 10;

###### 演出场次最多的场馆top10

> \-\-- 演出场次最多的演出场馆top10\
> create table if not exists travel_ads.ads_max_name_count_place (\
> id string COMMENT \'自增主键ID\',\
> place string comment\'演出场馆\',\
> name_count int comment \'演出数量\',\
> etl_time string comment \'etl处理日期\'\
> )\
> comment\'演出场次最多的场馆top10\';\
> \--插入数据\
> insert overwrite table travel_ads.ads_max_name_count_place\
> select\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> place,\
> name_count,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> from travel_dws.travel_dws_yanchu_place\
> where travel_dws_yanchu_place.place != \'null\' and name_count \> 10\
> order by name_count desc\
> limit 10;

###### 演出场次少的场馆top10

> \-\-- 演出场次最少的演出场馆top10\
> create table if not exists travel_ads.ads_min_name_count_place(\
> id string COMMENT \'自增主键ID\',\
> place string comment\'演出场馆\',\
> name_count int comment \'演出数量\',\
> etl_time string comment \'etl处理日期\'\
> )\
> comment\'演出场次少的场馆top10\';\
> \--插入数据\
> insert overwrite table travel_ads.ads_min_name_count_place\
> select\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> place,\
> name_count,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> from travel_dws.travel_dws_yanchu_place\
> where travel_dws_yanchu_place.place != \'null\' and name_count \< 10\
> order by name_count\
> limit 10;

###### 各演出情况的占比

> \-\--各演出情况的占比\
> create table if not exists travel_ads.ads_cond_count (\
> id string COMMENT \'自增主键ID\',\
> cond string comment\'演出情况\',\
> name_count int comment \'演出数量\',\
> proportion decimal(5,2) comment\'情况占比\',\
> etl_time string comment \'etl处理日期\'\
> )\
> comment\'各演出情况的占比\';\
> \--插入数据\
> insert overwrite table travel_ads.ads_cond_count\
> SELECT\
> sha2(concat(unix_timestamp(),uuid(),rand()),256) AS id,\
> cond,\
> COUNT(\*) count,\
> CONCAT(ROUND(COUNT(\*) \* 100.0 / total, 2)) AS proportion,\
> date_format(current_date(), \'yyyy-MM-dd\') AS etl_date\
> FROM travel_dwd.travel_dwd_yanchu\
> CROSS JOIN (\
> SELECT COUNT(\*) AS total\
> FROM travel_dwd.travel_dwd_yanchu\
> ) t\
> GROUP BY cond, total\
> ORDER BY count desc;

KTV

###### 消费大于500的KTV所在的区和平均评论

> \--消费大于500的KTV所在的区和平均评论\
> \--创建表\
> create table if not exists ktv_ads.ktv_ads_consume_avg_comment( id
> string comment \'自增主键\',\
> name String comment\'ktv名称\',\
> city string comment \'区\',\
> consume bigint comment \'消费\',\
> avg_comment double comment\'平均评论数量\',\
> etl_times string comment \'etl时间\'\
> ) comment \'消费大于500的KTV所在的区和平均评论\';\
> insert overwrite table ktv_ads.ktv_ads_consume_avg_comment\
> SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) as id,\
> subquery.name, k.city, subquery.consume, k.avg_comment,\
> date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
> FROM (\
> SELECT d.name, d.city, d.consume\
> FROM dwd_ktv.ktv_dwd_a d\
> WHERE d.consume \> 500\
> ) AS subquery\
> JOIN ktv_dws.ktv_dwd_c k ON subquery.city = k.city;

###### 平均性价比前五的地区的KTV

\--平均性价比前五的地区的KTV\
\--创建表\
create table if not exists ktv_ads.ktv_ads_avg_cost(\
id string comment \'自增主键\',\
name string comment\'KTV名称\',\
city string comment\'区\',\
avg_cost string comment \'平均性价比\',\
etl_times string comment \'etl时间\'\
)comment \'平均性价比前五的地区的KTV\';\
\--加载数据\
insert overwrite table ktv_ads.ktv_ads_avg_cost\
SELECT sha2(concat(unix_timestamp(),uuid(),rand()),256) as id,\
d.name, t1.city, t1.avg_cost,\
date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
FROM (\
SELECT k.city, k.avg_cost\
FROM ktv_dws.ktv_dwd_c k\
ORDER BY k.avg_cost DESC\
LIMIT 5\
) AS t1\
JOIN dwd_ktv.ktv_dwd_a d ON d.city = t1.city;

###### 评论大于20的ktv所在地区的平均性价比

\--评论大于20的ktv所在地区的平均性价比\
create table if not exists ktv_ads.ktv_ads_comment_avg_cost(\
id string comment \'自增主键\',\
name string comment\'KTV名称\',\
city string comment\'区\',\
comment bigint comment\'评论数量\',\
avg_cost double comment\'平均性价比\',\
etl_times string comment \'etl时间\'\
)comment \'评论大于20的ktv所在地区的平均性价比\';\
\--加载数据\
insert overwrite table ktv_ads.ktv_ads_comment_avg_cost\
select sha2(concat(unix_timestamp(),uuid(),rand()),256) as id,\
t1.name,t1.city,t1.comment,k.avg_cost,\
date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
from (\
select d.name,d.city,d.comment\
from dwd_ktv.ktv_dwd_a d\
where comment \>20 and city !=\'未知\'\
)as t1\
join ktv_dws.ktv_dwd_c k on t1.city=k.city ;

###### 评论占比排名前五的地区的的KTV的消费

\--评论占比排名前五的地区的的KTV的消费\
create table if not exists ktv_ads.ktv_das_comment_ratiotop5(\
id string comment \'自增主键\',\
name string comment \'ktv名称\',\
city string comment \'区\',\
consume bigint comment \'消费\',\
comment_ratio double comment \'评论占比\',\
etl_times string comment \'etl时间\'\
)comment \'评论占比排名前五的地区的的KTV的消费\';\
insert overwrite table ktv_ads.ktv_das_comment_ratiotop5\
select sha2(concat(unix_timestamp(),uuid(),rand()),256) as id,\
d.name,d.city,d.consume,t1.comment_ratio,\
date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
from (\
select k.city,k.comment_ratio\
from ktv_dws.ktv_dwd_c k\
order by comment_ratio desc\
limit 5\
)as t1\
join dwd_ktv.ktv_dwd_a d on t1.city= d.city\
where consume != \'0\';

###### 平均服务评分和平均环境评分相等的地区的KTV的消费

\-- 平均服务评分和平均环境评分相等的地区的KTV的消费\
create table if not exists ktv_ads.ktv_das_service_envrioment(\
id string comment \'自增主键\',\
name string comment \'ktv名称\',\
city string comment \'区\',\
consume bigint comment \'消费\',\
avg_service double comment\'平均服务评分\',\
avg_envrioment double comment\'平均环境评分\',\
etl_times string comment \'etl时间\'\
)comment \'平均服务评分和平均环境评分相等的地区的KTV的消费\';\
insert overwrite table ktv_ads.ktv_das_service_envrioment\
select sha2(concat(unix_timestamp(),uuid(),rand()),256) as id,\
d.name,t1.city,d.consume,t1.avg_service,t1.avg_emviroment,\
date_format(current_timestamp(),\'yyyy-MM-dd-HH\') as etl_times\
from (\
select k.avg_service,k.avg_emviroment,k.city\
from ktv_dws.ktv_dwd_c k\
where avg_service = avg_emviroment and city !=\'未知\'\
)as t1\
join dwd_ktv.ktv_dwd_a d on t1.city=d.city\
where consume !=\'0\';

### 初始化指标库

**景点**

create database if not exists jd_result DEFAULT CHARACTER SET utf8;\
SHOW VARIABLES LIKE \'character%\';\
SET character_set_client=utf8;\
SET character_set_connection =utf8;\
SET character_set_database =utf8;\
SET character_set_results=utf8;\
SET character_set_server=utf8;\
CREATE TABLE IF NOT EXISTS jd_result.cy_bus (\
district varchar(255) COMMENT \'区\',\
bus_ratio DECIMAL(5,2) COMMENT \'公交覆盖率\',\
category varchar(255) COMMENT \'分类(完全覆盖/其他)\',\
count INT COMMENT \'数量\',\
percentage DECIMAL(5,2) COMMENT \'占比\'\
)\
ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT
\'朝阳区公交覆盖情况分析表\';\
CREATE TABLE IF NOT EXISTS jd_result.jd_qn (\
category varchar(255) COMMENT \'分类(全年适宜/非全年适宜)\',\
record_count INT COMMENT \'记录数量\',\
percentage DECIMAL(5,2) COMMENT \'占比百分比\',\
etl_date varchar(255) COMMENT \'ETL处理日期\'\
)\
ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT
\'景点全年适宜游览时间占比分析\';\
CREATE TABLE IF NOT EXISTS jd_result.free_zb (\
district varchar(255) COMMENT \'行政区\',\
free_attraction_percent DECIMAL(5,2) COMMENT \'完全免费景点百分比\',\
total_attractions INT COMMENT \'景点总数\',\
free_attractions INT COMMENT \'完全免费景点数\',\
etl_date varchar(255) COMMENT \'ETL处理日期\'\
)\
ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT
\'五大城区完全免费景点占比分析(柱状图数据)\';\
CREATE TABLE IF NOT EXISTS jd_result.bus_subway (\
district varchar(255) COMMENT \'行政区\',\
bus_only_percent DECIMAL(5,2) COMMENT \'仅公交全覆盖百分比\',\
subway_only_percent DECIMAL(5,2) COMMENT \'仅地铁全覆盖百分比\',\
both_percent DECIMAL(5,2) COMMENT \'公交地铁双覆盖百分比\',\
neither_percent DECIMAL(5,2) COMMENT \'无全覆盖百分比\',\
total_locations INT COMMENT \'地点总数\',\
etl_date varchar(255) COMMENT \'ETL处理日期\'\
)\
ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT
\'五大城区公交地铁全覆盖占比分析\';\
CREATE TABLE IF NOT EXISTS jd_result.qnsy (\
district varchar(255) COMMENT \'行政区\',\
year_round_percent DECIMAL(5,2) COMMENT \'全年适宜景点百分比\',\
total_attractions INT COMMENT \'景点总数\',\
year_round_count INT COMMENT \'全年适宜景点数\',\
etl_date varchar(255) COMMENT \'ETL处理日期\'\
)\
ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT
\'六大城区全年适宜游览景点占比分析\';

数据导出

#!/bin/bash\
sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop\
#date_str=\`date -d -1hour \"+%Y-%m-%d-%H\"\`\
export_data(){\
\$sqoop export \\\
\--connect
\"jdbc:mysql://node-1:3306/jd_result?useUnicode=true&characterEncoding=utf-8\"
\\\
\--username root \--password root \\\
\--table \$1 \\\
\--export-dir /user/hive/warehouse/\$3/\$1/ \\\
\--update-key \$2 \\\
\--update-mode allowinsert \\\
\--input-fields-terminated-by \'\\001\' \\\
\--input-null-string \'\\\\N\' \\\
\--input-null-non-string \'\\\\N\'\
}\
case \$1 in\
\"bus_subway\")\
export_data \"bus_subway\" id mjy_ads.db\
;;\
\"cy_bus\")\
export_data \"cy_bus\" id mjy_ads.db\
;;\
\"free_zb\")\
export_data \"free_zb\" id mjy_ads.db\
;;\
\"jd_qn\")\
export_data \"jd_qn\" id mjy_ads.db\
;;\
\"qnsy\")\
export_data \"qnsy\" id mjy_ads.db\
;;\
\*)\
echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"\
export_data \"bus_subway\" id mjy_ads.db\
export_data \"cy_bus\" id mjy_ads.db\
export_data \"free_zb\" id mjy_ads.db\
export_data \"jd_qn\" id mjy_ads.db\
export_data \"qnsy\" id mjy_ads.db\
;;\
esac

**电影**

create database if not exists wenyu_ads DEFAULT CHARACTER SET utf8;

SHOW VARIABLES LIKE \'character%\';

SET character_set_client=utf8;

SET character_set_connection =utf8;

SET character_set_database =utf8;

SET character_set_results=utf8;

SET character_set_server=utf8;

CREATE TABLE \`wenyu_ads\`.\`dianying_ads_top\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'电影名称\',

\`score\` decimal(3,1) COMMENT \'电影评分\',

\`type\` VARCHAR(255) COMMENT \'电影类型\',

\`etl_times\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'评分大于8.5的电影及其类型\';

CREATE TABLE \`wenyu_ads\`.\`dianying_ads_q15\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'电影名称\',

\`score\` decimal(3,1) COMMENT \'电影评分\',

\`etl_times\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT=\'评分前十五电影\';

CREATE TABLE \`wenyu_ads\`.\`dianying_ads_sctop20\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'电影名称\',

\`score\` decimal(3,1) COMMENT \'电影评分\',

\`science_fiction\` decimal(3,1) COMMENT \'科幻类占比\',

\`etl_times\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'评分前二十的科幻类电影\';

CREATE TABLE \`wenyu_ads\`.\`dianying_ads_pingfen\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'电影名称\',

\`score\` VARCHAR(255) COMMENT \'电影评分\',

\`etl_times\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'不同评分区间转换后的电影\';

CREATE TABLE \`wenyu_ads\`.\`dianying_ads_xjtop\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'电影名称\',

\`score\` decimal(3,1) COMMENT \'电影评分\',

\`comedy\` decimal(3,1) COMMENT \'喜剧类占比\',

\`etl_times\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'评分8.0以上的喜剧类电影\';

**体育运动场馆**

CREATE DATABASE IF NOT EXISTS sport_result DEFAULT CHARACTER SET utf8;

SHOW VARIABLES LIKE \'character%\';

SET character_set_client=utf8;

SET character_set_connection =utf8;

SET character_set_database =utf8;

SET character_set_results=utf8;

SET character_set_server=utf8;

CREATE TABLE IF NOT EXISTS sport_result.sport_top8 (

id VARCHAR(255) COMMENT \'自增主键ID\',

region VARCHAR(255) COMMENT \'地区名称\',

sport_count BIGINT COMMENT \'运动场馆数量\',

city_rank BIGINT COMMENT \'城市排名\',

etl_times VARCHAR(255) COMMENT \'etl时间\'

)COMMENT \'北京市各区运动场馆TOP8排名表\';

CREATE TABLE IF NOT EXISTS sport_result.share_top5 (

id VARCHAR(255) COMMENT \'自增主键ID\',

TYPE VARCHAR(255) COMMENT \'运动场馆类型名称\',

sport_count BIGINT COMMENT \'运动场馆数量\',

brand_percent DECIMAL(5,1) COMMENT \'类型占比（百分比）\',

etl_times VARCHAR(255) COMMENT \'etl时间\'

)COMMENT \'北京市运动场馆数量TOP5\';

CREATE TABLE IF NOT EXISTS sport_result.city_region_concentration_top5 (

id VARCHAR(255) COMMENT \'自增主键ID\',

city VARCHAR(255) COMMENT \'城市名称\',

top_region VARCHAR(255) COMMENT \'最集中区域\',

concentration_percent DECIMAL(5,2) COMMENT \'集中度(%)\',

etl_times VARCHAR(255) COMMENT \'ETL时间\'

) COMMENT \'北京市运动场馆区域集中度TOP5\';

CREATE TABLE IF NOT EXISTS sport_result.city_ping_pang_ratio_top3 (

id VARCHAR(255) COMMENT \'自增主键ID\',

region VARCHAR(255) COMMENT \'区域名称\',

ping_pang_percent DECIMAL(5,2) COMMENT \'乒乓球馆占比(%)\',

ratio_rank BIGINT COMMENT \'占比排名\',

etl_times VARCHAR(255) COMMENT \'ETL时间\'

) COMMENT \'北京区域乒乓球馆占比TOP3\';

CREATE TABLE IF NOT EXISTS sport_result.popular_sport_top3 (

id VARCHAR(255) COMMENT \'自增主键ID\',

sport_type VARCHAR(255) COMMENT \'运动类型\',

total_venues BIGINT COMMENT \'总场馆数\',

national_percent DECIMAL(5,2) COMMENT \'全国占比(%)\',

etl_times VARCHAR(255) COMMENT \'ETL时间\'

) COMMENT \'北京最受欢迎运动类型TOP3\';

**演出**

create database if not exists yanchu_result DEFAULT CHARACTER SET utf8;

SHOW VARIABLES LIKE \'character%\';

SET character_set_client=utf8;

SET character_set_connection=utf8;

SET character_set_database =utf8;

SET character_set_results=utf8;

SET character_set_server=utf8;

CREATE TABLE \`yanchu_result\`.\`ads_max_price_name\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'演出名称\',

\`max_price\` DECIMAL(10,2) COMMENT \'最高售票价格\',

\`etl_time\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'价格最高的演出top10排名\';

CREATE TABLE \`yanchu_result\`.\`ads_min_price_name\` (

\`id\` VARCHAR(255) primary key ,

\`name\` VARCHAR(255) NOT NULL COMMENT \'演出数量\',

\`min_price\` DECIMAL(10,2) COMMENT \'最低售票价格\',

\`etl_time\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'价格最低的演出top10排名\';

CREATE TABLE \`yanchu_result\`.\`ads_max_name_count_place\` (

\`id\` VARCHAR(255) primary key ,

\`place\` VARCHAR(255) NOT NULL COMMENT \'演出场馆\',

\`name_count\` int COMMENT \'演出数量\',

\`etl_time\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'演出场次最多的演出场馆top10\';

CREATE TABLE \`yanchu_result\`.\`ads_min_name_count_place\` (

\`id\` VARCHAR(255) primary key ,

\`place\` VARCHAR(255) NOT NULL COMMENT \'演出场馆\',

\`name_count\` int COMMENT \'演出数量\',

\`etl_time\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT=\'演出场次最少的演出场馆top10\';

CREATE TABLE \`yanchu_result\`.\`ads_cond_count\` (

\`id\` VARCHAR(255) primary key ,

\`cond\` VARCHAR(255) NOT NULL COMMENT \'演出情况\',

\`name_count\` int COMMENT \'演出数量\',

\`proportion\` DECIMAL(5,2) COMMENT \'各个情况占比\',

\`etl_time\` VARCHAR(255)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT=\'各演出情况的占比\';

**KTV**

DROP DATABASE IF EXISTS ktv_ads;

CREATE DATABASE IF NOT EXISTS ktv_ads DEFAULT CHARACTER SET utf8;

SHOW VARIABLES LIKE \'character%\';

SET character_set_client=utf8;

SET character_set_connection =utf8;

SET character_set_database =utf8;

SET character_set_results=utf8;

SET character_set_server=utf8;

\-- 消费大于500的KTV所在的区和平均评论

CREATE TABLE if not exists \`ktv_ads\`.\`ktv_ads_consume_avg_comment\`(

\`id\` VARCHAR(255) PRIMARY KEY ,

\`name\` VARCHAR(255) NOT NULL COMMENT\'ktv名称\',

\`city\` VARCHAR(255) NOT NULL COMMENT\'区\',

\`consume\` BIGINT COMMENT \'消费\',

\`avg_comment\` decimal(10,2) COMMENT\'平均评论数量\',

\`etl_times\` VARCHAR(255)

) ENGINE=INNODB DEFAULT CHARSET=utf8mb4
COMMENT=\'消费大于500的KTV所在的区和平均评论\';

\-- 平均性价比前五的地区的KTV

CREATE TABLE if not exists \`ktv_ads\`.\`ktv_ads_avg_cost\`(

\`id\` VARCHAR(255) PRIMARY KEY ,

\`name\` VARCHAR(255) NOT NULL COMMENT\'ktv名称\',

\`city\` VARCHAR(255) NOT NULL COMMENT\'区\',

\`avg_cost\` decimal(10,2) COMMENT\'平均性价比\',

\`etl_times\` VARCHAR(255)

)ENGINE=INNODB DEFAULT CHARSET=utf8mb4
COMMENT=\'平均性价比前五的地区的KTV\';

\-- 评论大于20的ktv所在地区的平均性价比

CREATE TABLE if not exists \`ktv_ads\`.\`ktv_ads_comment_avg_cost\` (

\`id\` VARCHAR(255) PRIMARY KEY ,

\`name\` VARCHAR(255) NOT NULL COMMENT\'ktv名称\',

\`city\` VARCHAR(255) NOT NULL COMMENT\'区\',

\`comment\` BIGINT COMMENT\'评论数量\',

\`avg_cost\` decimal(10,2) COMMENT\'平均性价比\',

\`etl_times\` VARCHAR(255)

)ENGINE=INNODB DEFAULT CHARSET=utf8mb4
COMMENT=\'评论大于20的ktv所在地区的平均性价比\';

\-- 评论占比排名前五的地区的的KTV的消费

CREATE TABLE if not exists \`ktv_ads\`.\`ktv_das_comment_ratiotop5\`(

\`id\` VARCHAR(255) PRIMARY KEY ,

\`name\` VARCHAR(255) NOT NULL COMMENT\'ktv名称\',

\`city\` VARCHAR(255) NOT NULL COMMENT\'区\',

\`consume\` BIGINT COMMENT \'消费\',

\`comment_ratio\` decimal(10,2) COMMENT \'评论占比\'

)ENGINE=INNODB DEFAULT CHARSET=utf8mb4
COMMENT=\'评论占比排名前五的地区的的KTV的消费\';

\-- 平均服务评分和平均环境评分相等的地区的KTV的消费

CREATE TABLE if not exists \`ktv_ads\`.\`ktv_das_service_envrioment\`(

\`id\` VARCHAR(255) PRIMARY KEY ,

\`name\` VARCHAR(255) NOT NULL COMMENT\'ktv名称\',

\`city\` VARCHAR(255) NOT NULL COMMENT\'区\',

\`consume\` BIGINT COMMENT \'消费\',

\`avg_service\` decimal(10,2) COMMENT\'平均服务评分\',

\`avg_envrioment\` decimal(10,2) COMMENT\'平均环境评分\'

) ENGINE=INNODB DEFAULT CHARSET=utf8mb4
COMMENT=\'平均服务评分和平均环境评分相等的地区的KTV的消费\';

### 数据导出

**景点**

#!/bin/bash

sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop

#date_str=\`date -d -1hour \"+%Y-%m-%d-%H\"\`

export_data(){

\$sqoop export \\

\--connect
\"jdbc:mysql://node-1:3306/jd_result?useUnicode=true&characterEncoding=utf-8\"
\\

\--username root \--password root \\

\--table \$1 \\

\--export-dir /user/hive/warehouse/\$3/\$1/ \\

\--update-key \$2 \\

\--update-mode allowinsert \\

\--input-fields-terminated-by \'\\001\' \\

\--input-null-string \'\\\\N\' \\

\--input-null-non-string \'\\\\N\'

}

case \$1 in

\"bus_subway\")

export_data \"bus_subway\" id mjy_ads.db

;;

\"cy_bus\")

export_data \"cy_bus\" id mjy_ads.db

;;

\"free_zb\")

export_data \"free_zb\" id mjy_ads.db

;;

\"jd_qn\")

export_data \"jd_qn\" id mjy_ads.db

;;

\"qnsy\")

export_data \"qnsy\" id mjy_ads.db

;;

\*)

echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"

export_data \"bus_subway\" id mjy_ads.db

export_data \"cy_bus\" id mjy_ads.db

export_data \"free_zb\" id mjy_ads.db

export_data \"jd_qn\" id mjy_ads.db

export_data \"qnsy\" id mjy_ads.db

;;

esac

**电影**

#!/bin/bash

sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop

#date_str=\`date -d -1hour \"+%Y-%m-%d-%H\"\`

export_data(){

\$sqoop export \\

\--connect
\"jdbc:mysql://node-1:3306/wenyu_ads?useUnicode=true&characterEncoding=utf-8\"
\\

\--username root \--password root \\

\--table \$1 \\

\--export-dir /user/hive/warehouse/\$3/\$1/000000_0 \\

\--update-key \$2 \\

\--update-mode allowinsert \\

\--input-fields-terminated-by \'\\001\' \\

\--input-null-string \'\\\\N\' \\

\--input-null-non-string \'\\\\N\'

}

case \$1 in

\"dianying_ads_top\")

export_data \"dianying_ads_top\" id wenyu_ads.db

;;

\"dianying_ads_q15\")

export_data \"dianying_ads_q15\" id wenyu_ads.db

;;

\"dianying_ads_sctop20\")

export_data \"dianying_ads_sctop20\" id wenyu_ads.db

;;

\"dianying_ads_pingfen\")

export_data \"dianying_ads_pingfen\" id wenyu_ads.db

;;

\"dianying_ads_xjtop\")

export_data \"dianying_ads_xjtop\" id wenyu_ads.db

;;

\*)

echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"

export_data \"dianying_ads_top\" id wenyu_ads.db

export_data \"dianying_ads_q15\" id wenyu_ads.db

export_data \"dianying_ads_sctop20\" id wenyu_ads.db

export_data \"dianying_ads_pingfen\" id wenyu_ads.db

export_data \"dianying_ads_xjtop\" id wenyu_ads.db

;;

esac

**体育运动场馆**

#!/bin/bash

sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop

#date_str=\`date -d -1hour \"+%Y-%m-%d-%H\"\`

export_data(){

\$sqoop export \\

\--connect
\"jdbc:mysql://node-1:3306/sport_result?useUnicode=true&characterEncoding=utf-8\"
\\

\--username root \--password root \\

\--table \$1 \\

\--export-dir /user/hive/warehouse/\$3/\$1/ \\

\--update-key \$2 \\

\--update-mode allowinsert \\

\--input-fields-terminated-by \'\\001\' \\

\--input-null-string \'\\\\N\' \\

\--input-null-non-string \'\\\\N\'

}

case \$1 in

\"sport_top8\")

export_data \"sport_top8\" id ads_sport.db

;;

\"share_top5\")

export_data \"share_top5\" id ads_sport.db

;;

\"city_region_concentration_top5\")

export_data \"city_region_concentration_top5\" id ads_sport.db

;;

\"city_ping_pang_ratio_top3\")

export_data \"city_ping_pang_ratio_top3\" id ads_sport.db

;;

\"popular_sport_top3\")

export_data \"popular_sport_top3\" id ads_sport.db

;;

\*)

echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"

export_data \"sport_top8\" id ads_sport.db

export_data \"share_top5\" id ads_sport.db

export_data \"city_region_concentration_top5\" id ads_sport.db

export_data \"city_ping_pang_ratio_top3\" id ads_sport.db

export_data \"popular_sport_top3\" id ads_sport.db

;;

esac

**演出**

#!/bin/bash

sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop

#date_str=\`date -d -1hour \"+%Y-%m-%d-%H\"\`

export_data(){

\$sqoop export \\

\--connect
\"jdbc:mysql://node-1:3306/yanchu_result?useUnicode=true&characterEncoding=utf-8\"
\\

\--username root \--password root \\

\--table \$1 \\

\--export-dir /user/hive/warehouse/\$3/\$1/ \\

\--update-key \$2 \\

\--update-mode allowinsert \\

\--input-fields-terminated-by \'\\001\' \\

\--input-null-string \'\\\\N\' \\

\--input-null-non-string \'\\\\N\'

}

case \$1 in

\"ads_cond_count\")

export_data \"ads_cond_count\" id travel_ads.db

;;

\"ads_max_name_count_place\")

export_data \"ads_max_name_count_place\" id travel_ads.db

;;

\"ads_max_price_name\")

export_data \"ads_max_price_name\" id travel_ads.db

;;

\"ads_min_name_count_place\")

export_data \"ads_min_name_count_place\" id travel_ads.db

;;

\"ads_min_price_name\")

export_data \"ads_min_price_name\" id travel_ads.db

;;

\*)

echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"

export_data \"ads_cond_count\" id travel_ads.db

export_data \"ads_max_name_count_place\" id travel_ads.db

export_data \"ads_max_price_name\" id travel_ads.db

export_data \"ads_min_name_count_place\" id travel_ads.db

export_data \"ads_min_price_name\" id travel_ads.db

;;

esac

**KTV**

#!/bin/bash

\# 设置 Sqoop 路径

sqoop=/opt/apps/sqoop-1.4.6/bin/sqoop

\# 获取前一小时的时间字符串

date_str=\$(date -d -1hour \"+%Y-%m-%d-%H\")

\# 定义导出函数

export_data() {

\$sqoop export \\

\--connect
\"jdbc:mysql://node-1:3306/ktv_ads?useSSL=false&useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false\"
\\

\--username root \\

\--password root \\

\--table \$1 \\

\--export-dir /user/hive/warehouse/ktv_ads.db/\$1 \\

\--update-mode allowinsert \\

\--fields-terminated-by \'\\001\' \\

\--null-string \'\\\\N\' \\

\--null-non-string \'\\\\N\'

}

\# 根据传入的参数执行相应的导出任务

case \$1 in

\"ktv_ads_consume_avg_comment\")

export_data \"ktv_ads_consume_avg_comment\" id ktv_ads

;;

\"ktv_ads_avg_cost\")

export_data \"ktv_ads_avg_cost\" id ktv_ads

;;

\"ktv_ads_comment_avg_cost\")

export_data \"ktv_ads_comment_avg_cost\" id ktv_ads

;;

\"ktv_das_comment_ratiotop5\")

export_data \"ktv_das_comment_ratiotop5\" id ktv_ads

;;

\"ktv_das_service_envrioment\")

export_data \"ktv_das_service_envrioment\" id ktv_ads

;;

\*)

echo
\"\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*all\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\"

export_data \"ktv_ads_consume_avg_comment\" id ktv_ads

export_data \"ktv_ads_avg_cost\" id ktv_ads

export_data \"ktv_ads_comment_avg_cost\" id ktv_ads

export_data \"ktv_das_comment_ratiotop5\" id ktv_ads

export_data \"ktv_das_service_envrioment\" id ktv_ads

;;

esac

### 任务调度

![](media/image50.png){width="5.768055555555556in" height="4.36875in"}

### 数据可视化

![](media/image51.png){width="5.768055555555556in"
height="6.658333333333333in"}
