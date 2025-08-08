--MaxCompute SQL
--********************************************************************--
--author: 田家齐
--create time: 2025-08-07 16:58:37
--********************************************************************--
-- ********************************************************************--
-- author: 田家齐
-- create time: 2025-08-07 16:30:00
-- 说明：DIM层维度表，从ODS层抽取并整合维度数据
-- ********************************************************************--

-- 1. 商品维度表（全量快照）
CREATE TABLE IF NOT EXISTS dim_product_info (
    product_id STRING COMMENT '商品唯一标识',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    brand STRING COMMENT '商品品牌（从JSON属性中提取）',
    price DECIMAL(10,2) COMMENT '商品价格（从JSON属性中提取）',
    color STRING COMMENT '商品颜色（从JSON属性中提取）',
    putaway_time TIMESTAMP COMMENT '上架时间',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
    ) COMMENT '商品维度表'
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='存储商品基础维度信息，每日全量更新');

-- 从ODS层加载商品维度数据
INSERT OVERWRITE TABLE dim_product_info
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    get_json_object(product_attributes, '$.品牌') AS brand,
    cast(get_json_object(product_attributes, '$.价格') AS DECIMAL(10,2)) AS price,
    get_json_object(product_attributes, '$.颜色') AS color,
    putaway_time,
    shop_id,
    '2025-08-07' AS dt
FROM ods_product_info;



-- 2. 用户维度表（全量快照）
CREATE TABLE IF NOT EXISTS dim_user_info (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '性别：male/female/unknown',
    age_group STRING COMMENT '年龄组：0-18/19-25/26-35/36+',
    register_time TIMESTAMP COMMENT '注册时间',
    user_level STRING COMMENT '用户等级：v1/v2/v3',
    city STRING COMMENT '所在城市',
    city_level STRING COMMENT '城市等级（一线城市/二线城市/其他）',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '用户维度表'
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='存储用户基础维度信息，每日全量更新');

-- 从ODS层加载用户维度数据
INSERT OVERWRITE TABLE dim_user_info
SELECT
    user_id,
    user_name,
    gender,
    age_group,
    register_time,
    user_level,
    city,
    CASE
        WHEN city IN ('北京','上海','广州','深圳') THEN '一线城市'
        WHEN city IN ('杭州','成都','武汉','南京','西安','重庆') THEN '二线城市'
        ELSE '其他城市'
        END AS city_level,
    '2025-08-07' AS dt
FROM ods_user_info;
DROP TABLE dim_user_info ;
-- 3. 渠道维度表（全量快照）
DROP TABLE dim_channel_info ;
CREATE TABLE IF NOT EXISTS dim_channel_info (
    channel_id STRING COMMENT '渠道ID',
    channel_name STRING COMMENT '渠道名称',
    channel_type STRING COMMENT '渠道类型（自有/第三方）',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '渠道维度表'
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='存储流量渠道维度信息，每日全量更新');

-- 从ODS层加载渠道维度数据
INSERT OVERWRITE TABLE dim_channel_info
SELECT
    channel_id,
    channel_name,
    CASE
        WHEN channel_id IN ('chn_001','chn_002','chn_003','chn_004') THEN '自有渠道'
        ELSE '第三方渠道'
        END AS channel_type,
    '${bizdate}' AS dt
FROM (
         SELECT DISTINCT channel_id, channel_name
         FROM ods_traffic_channel
     ) t;

-- 4. 时间维度表（全量预生成）
CREATE TABLE IF NOT EXISTS dim_time (
    time_id STRING COMMENT '时间ID（yyyyMMdd）',
    the_date DATE COMMENT '日期',
    year STRING COMMENT '年份（yyyy）',
    quarter STRING COMMENT '季度（Q1/Q2/Q3/Q4）',
    month STRING COMMENT '月份（yyyyMM）',
    day STRING COMMENT '日（dd）',
    week STRING COMMENT '周（yyyyww）',
    is_weekend STRING COMMENT '是否周末（Y/N）',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '时间维度表'
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='存储时间维度信息，预生成3年数据');

-- 生成时间维度数据（示例：生成2023-2025年数据）
INSERT OVERWRITE TABLE dim_time
SELECT
    date_format(the_date, 'yyyyMMdd') AS time_id,
    the_date,
    date_format(the_date, 'yyyy') AS year,
    concat('Q', quarter(the_date)) AS quarter,
    date_format(the_date, 'yyyyMM') AS month,
    date_format(the_date, 'dd') AS day,
    date_format(the_date, 'yyyyww') AS week,
    CASE WHEN dayofweek(the_date) IN (1,7) THEN 'Y' ELSE 'N' END AS is_weekend,
    '${bizdate}' AS dt
FROM (
    SELECT sequence(to_date('2023-01-01'), to_date('2025-12-31'), interval 1 day) AS dates
) t
LATERAL VIEW explode(dates) tmp AS the_date;
