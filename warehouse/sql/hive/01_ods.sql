USE rental_ods;

DROP TABLE IF EXISTS ods_fy_jbxx;
CREATE EXTERNAL TABLE ods_fy_jbxx (
    fy_id STRING COMMENT '房源唯一ID',
    fy_title STRING COMMENT '房源标题',
    fy_type STRING COMMENT '房源类型',
    fy_status STRING COMMENT '房源状态',
    platform STRING COMMENT '来源平台',
    xzq STRING COMMENT '行政区',
    sq STRING COMMENT '商圈',
    jd DECIMAL(9,6) COMMENT '经度',
    wd DECIMAL(9,6) COMMENT '纬度',
    month_zj INT COMMENT '月租金(元)',
    jzmj DECIMAL(5,1) COMMENT '建筑面积(平方米)',
    is_dt STRING COMMENT '是否地铁房',
    zx_qk STRING COMMENT '装修情况'
) COMMENT '北京租房房源基础信息原始表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/clean/rental';
