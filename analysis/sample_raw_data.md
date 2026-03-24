# 北京娱乐方式原始样例数据

> 说明：以下为从公开网页页面手动提取的样例数据，用于验证毕设数据字段设计与后续 CSV 结构。当前只整理景点、KTV、演出、体育场馆、电影的少量样例，后续可继续扩充。

## 1. 景点样例（scenic_raw.csv）

建议字段：name,level,region,address,price,open_time,visit_duration,best_visit_time,source_url

| name | level | region | address | price | open_time | visit_duration | best_visit_time | source_url |
|---|---|---|---|---|---|---|---|---|
| 天坛公园景区 | 5A | 东城区 | \N | \N | \N | \N | \N | https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html |
| 故宫博物院 | 5A | 东城区 | \N | \N | \N | \N | \N | https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html |
| 北京奥林匹克公园景区 | 5A | 朝阳区 | \N | \N | \N | \N | \N | https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html |
| 颐和园景区 | 5A | 海淀区 | \N | \N | \N | \N | \N | https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html |
| 八达岭长城景区 | 5A | 延庆区 | \N | \N | \N | \N | \N | https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html |

## 2. KTV 样例（ktv_raw.csv）

建议字段：name,region,address,avg_cost,service_score,env_score,overall_score,popularity,business_hours,source_url

| name | region | address | avg_cost | service_score | env_score | overall_score | popularity | business_hours | source_url |
|---|---|---|---|---|---|---|---|---|---|
| 北京金钻KTV | 海淀区 | 北京市海淀区花园东路 | \N | 4.5 | \N | 88 | 10145 | 晚上19:00至凌晨3:30 | https://www.cityhui.com/shop/103506.html |
| 北京百富怡KTV | 东城区 | 北京市东城区东直门外大街 | \N | \N | \N | \N | \N | \N | https://www.cityhui.com/shop/103506.html |
| 北京明日五洲KTV | 西城区 | 北京市西城区马连道南口 | \N | \N | \N | \N | \N | \N | https://www.cityhui.com/shop/103506.html |
| 北京鹏润国际KTV | 朝阳区 | 北京市朝阳区霄云路 | \N | \N | \N | \N | \N | \N | https://www.cityhui.com/shop/103506.html |
| 北京鑫耀国际KTV | 朝阳区 | 北京市朝阳区小营北路 | \N | \N | \N | \N | \N | \N | https://www.cityhui.com/shop/103506.html |

## 3. 演出样例（show_raw.csv）

建议字段：name,show_time,venue,region,price_range,status,attention,source_url

| name | show_time | venue | region | price_range | status | attention | source_url |
|---|---|---|---|---|---|---|---|
| 田震「玩儿个痛快」演唱会北京站 | 2025-12-31 19:30 | 国家体育馆 | 北京 | 480,680,880,1080,1380 | 项目已结束 | 7.0 | https://www.dahepiao.com/yanchupiaowu1/ych/20251208539276.html |
| 黑豹乐队“漆黑的爆破”北京演唱会 | 2026-05-23 19:30 | 北京展览馆剧场 | 北京 | \N | \N | \N | https://www.dahepiao.com/yanchupiaowu1/ych/20251208539276.html |
| 陈佳邓丽君经典金曲专场音乐会北京站 | 2026-07-26 19:30 | 北京中山音乐堂 | 北京 | \N | \N | \N | https://www.dahepiao.com/yanchupiaowu1/ych/20251208539276.html |
| 北京华语流行金曲室内乐沙龙音乐会 | 2026-05-02 14:30 | 五棵松·爱乐汇艺术空间·都市音乐厅 | 北京 | \N | \N | \N | https://www.dahepiao.com/yanchupiaowu1/ych/20251208539276.html |
| 如了意乐队北京演唱会 | 2026-04-12 19:30 | 五棵松·爱乐汇艺术空间·都市剧场 | 北京 | \N | \N | \N | https://www.dahepiao.com/yanchupiaowu1/ych/20251208539276.html |

## 4. 体育场馆样例（sport_raw.csv）

建议字段：name,venue_type,region,address,score,comment_count,avg_cost,open_time,source_url

| name | venue_type | region | address | score | comment_count | avg_cost | open_time | source_url |
|---|---|---|---|---|---|---|---|---|
| 北京首都体育馆 | 综合体育馆 | 海淀区 | 北京市海淀区中关村南大街56号 | \N | \N | \N | 全年 10:00-19:30开放 | https://you.ctrip.com/sight/beijing1/52656.html |
| 北京首都体育馆 | 综合体育馆 | 海淀区 | 北京市海淀区西直门外白石桥5号 | \N | \N | \N | \N | https://s.visitbeijing.com.cn/attraction/117978 |
| 东单体育中心 | 体育中心 | 东城区 | 北京市东城区崇文门内大街108号 | \N | \N | 60元/人次（游泳馆散客） | 周二至周日10:00-22:00，周一12:00起 | https://baike.baidu.com/item/%E4%B8%9C%E5%8D%95%E4%BD%93%E8%82%B2%E4%B8%AD%E5%BF%83/10955619 |
| 国家网球中心钻石球场 | 网球场馆 | 朝阳区 | 北京市朝阳区林翠路5号 | \N | \N | \N | \N | https://m.ztpiao.cn/cg169.html |

## 5. 电影样例（movie_raw.csv）

建议字段：name,score,category,country_region,director,actors,intro,source_url

| name | score | category | country_region | director | actors | intro | source_url |
|---|---|---|---|---|---|---|---|
| 长津湖 | 7.4 | 剧情/历史/战争 | 中国大陆/中国香港 | 陈凯歌/徐克/林超贤 | 吴京/易烊千玺/段奕宏/朱亚文/李晨/韩东君/胡军/张涵予 | 电影以抗美援朝战争第二次战役中的长津湖战役为背景，讲述了一段波澜壮阔的历史。 | https://movie.douban.com/subject/25845392/ |
| 星际特工：千星之城 | 6.9 | 动作/科幻/冒险 | 法国/中国大陆/比利时/德国/阿联酋/美国/加拿大/新西兰/新加坡/英国/泰国 | 吕克·贝松 | 戴恩·德哈恩/卡拉·迪瓦伊/克里夫·欧文/蕾哈娜/伊桑·霍克 | 人类和众多外星种族共同生活在一个名为千星之城阿尔法的繁华星际大都市。 | https://movie.douban.com/subject/11502973/ |

## 6. 当前可行性结论

目前已经验证：
- 景点：可从北京市文旅局等官方列表页抽取基础清单。
- KTV：可从聚合生活服务页抓取名称、地址、评分、营业时间等字段。
- 演出：可从票务页抓取名称、时间、场馆、票价、状态。
- 体育场馆：可从旅游/百科/场馆页抓取名称、类型、地址、开放时间。
- 电影：可从电影详情页抓取评分、类型、导演、主演、简介。

下一步适合做的事：
1. 把这份样例转换成 5 个 CSV 文件。
2. 每类扩充到 30~100 条记录。
3. 再进入 HDFS / MapReduce / Hive / Sqoop 主链路。
