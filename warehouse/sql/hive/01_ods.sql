USE wenyu_ods;

DROP TABLE IF EXISTS ods_scenic_info;
CREATE EXTERNAL TABLE ods_scenic_info (
    name STRING,
    level STRING,
    region STRING,
    address STRING,
    price STRING,
    price_min DECIMAL(10,2),
    price_max DECIMAL(10,2),
    open_time STRING,
    visit_duration STRING,
    best_visit_time STRING,
    source_url STRING,
    source_site STRING,
    crawl_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/scenic';

DROP TABLE IF EXISTS ods_show_info;
CREATE EXTERNAL TABLE ods_show_info (
    name STRING,
    show_time STRING,
    venue STRING,
    region STRING,
    price_range STRING,
    price_min DECIMAL(10,2),
    price_max DECIMAL(10,2),
    status STRING,
    attention DECIMAL(10,2),
    source_url STRING,
    source_site STRING,
    crawl_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/show';

DROP TABLE IF EXISTS ods_ktv_info;
CREATE EXTERNAL TABLE ods_ktv_info (
    name STRING,
    region STRING,
    address STRING,
    avg_cost DECIMAL(10,2),
    service_score DECIMAL(10,2),
    env_score DECIMAL(10,2),
    overall_score DECIMAL(10,2),
    popularity BIGINT,
    business_hours STRING,
    source_url STRING,
    source_site STRING,
    crawl_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/ktv';

DROP TABLE IF EXISTS ods_movie_info;
CREATE EXTERNAL TABLE ods_movie_info (
    name STRING,
    score DECIMAL(10,2),
    category STRING,
    country_region STRING,
    director STRING,
    actors STRING,
    intro STRING,
    source_url STRING,
    source_site STRING,
    crawl_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/movie';

DROP TABLE IF EXISTS ods_sport_info;
CREATE EXTERNAL TABLE ods_sport_info (
    name STRING,
    venue_type STRING,
    region STRING,
    address STRING,
    score DECIMAL(10,2),
    comment_count BIGINT,
    avg_cost DECIMAL(10,2),
    open_time STRING,
    source_url STRING,
    source_site STRING,
    crawl_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/sport';
