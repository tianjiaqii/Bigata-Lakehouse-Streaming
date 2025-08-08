--MaxCompute SQL
--********************************************************************--
--author: 田家齐
--create time: 2025-08-08 09:59:59
--********************************************************************--
-- ********************************************************************--
-- ADS层：应用数据层
-- 基于DWS层汇总数据，直接产出业务指标
-- ********************************************************************--

-- 1. 商品访客数
CREATE TABLE IF NOT EXISTS ads_product_visitor_count (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    visitor_count_1d BIGINT COMMENT '1天访客数',
    visitor_count_7d BIGINT COMMENT '7天访客数',
    visitor_count_30d BIGINT COMMENT '30天访客数'
) COMMENT '商品访客数指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_visitor_count PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    visitor_count_1d,
    visitor_count_7d,
    visitor_count_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 2. 商品浏览量
CREATE TABLE IF NOT EXISTS ads_product_view_count (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    view_count_1d BIGINT COMMENT '1天浏览量',
    view_count_7d BIGINT COMMENT '7天浏览量',
    view_count_30d BIGINT COMMENT '30天浏览量'
) COMMENT '商品浏览量指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_view_count PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    view_count_1d,
    view_count_7d,
    view_count_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 3. 商品支付金额排名
CREATE TABLE IF NOT EXISTS ads_product_pay_rank (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    pay_amount_1d DECIMAL(12,2) COMMENT '1天支付金额',
    rank_1d INT COMMENT '1天支付金额排名',
    pay_amount_7d DECIMAL(12,2) COMMENT '7天支付金额',
    rank_7d INT COMMENT '7天支付金额排名',
    pay_amount_30d DECIMAL(12,2) COMMENT '30天支付金额',
    rank_30d INT COMMENT '30天支付金额排名'
    ) COMMENT '商品支付金额排名指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_pay_rank PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    pay_amount_1d,
    -- 显式转换为INT类型，与表定义匹配
    CAST(RANK() OVER (ORDER BY pay_amount_1d DESC) AS INT) AS rank_1d,
    pay_amount_7d,
    CAST(RANK() OVER (ORDER BY pay_amount_7d DESC) AS INT) AS rank_7d,
    pay_amount_30d,
    CAST(RANK() OVER (ORDER BY pay_amount_30d DESC) AS INT) AS rank_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 4. 商品支付金额
CREATE TABLE IF NOT EXISTS ads_product_pay_amount (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    pay_amount_1d DECIMAL(12,2) COMMENT '1天支付金额',
    pay_amount_7d DECIMAL(12,2) COMMENT '7天支付金额',
    pay_amount_30d DECIMAL(12,2) COMMENT '30天支付金额'
    ) COMMENT '商品支付金额指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_pay_amount PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    pay_amount_1d,
    pay_amount_7d,
    pay_amount_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 5. 商品支付买家数
CREATE TABLE IF NOT EXISTS ads_product_buyer_count (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    pay_buyer_count_1d BIGINT COMMENT '1天支付买家数',
    pay_buyer_count_7d BIGINT COMMENT '7天支付买家数',
    pay_buyer_count_30d BIGINT COMMENT '30天支付买家数'
) COMMENT '商品支付买家数指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_buyer_count PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    pay_buyer_count_1d,
    pay_buyer_count_7d,
    pay_buyer_count_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 6. 商品支付件数
CREATE TABLE IF NOT EXISTS ads_product_pay_quantity (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    pay_quantity_1d BIGINT COMMENT '1天支付件数',
    pay_quantity_7d BIGINT COMMENT '7天支付件数',
    pay_quantity_30d BIGINT COMMENT '30天支付件数'
) COMMENT '商品支付件数指标表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_product_pay_quantity PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    product_name,
    category_name,
    pay_quantity_1d,
    pay_quantity_7d,
    pay_quantity_30d
FROM dws_product_stats
WHERE dt = '2025-08-07';

-- 7. 月流量来源渠道统计
CREATE TABLE IF NOT EXISTS ads_monthly_channel_stats (
    channel_id STRING COMMENT '渠道ID',
    channel_name STRING COMMENT '渠道名称',
    channel_type STRING COMMENT '渠道类型',
    month STRING COMMENT '统计月份',
    session_count BIGINT COMMENT '会话数',
    visitor_count BIGINT COMMENT '访客数',
    total_stay_time BIGINT COMMENT '总停留时间(秒)',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时间(秒)',
    session_ratio DECIMAL(5,2) COMMENT '会话占比'
    ) COMMENT '月流量来源渠道统计表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_monthly_channel_stats PARTITION (dt = '2025-08-07')
SELECT
    channel_id,
    channel_name,
    channel_type,
    month,
    session_count,
    visitor_count,
    total_stay_time,
    avg_stay_time,
    session_count / SUM(session_count) OVER () AS session_ratio
FROM dws_channel_stats
WHERE dt = '2025-08-07';

-- 8. 月关键词搜索的排名
CREATE TABLE IF NOT EXISTS ads_monthly_keyword_rank (
    keyword STRING COMMENT '搜索关键词',
    month STRING COMMENT '统计月份',
    search_count BIGINT COMMENT '搜索次数',
    rank INT COMMENT '搜索排名'
) COMMENT '月关键词搜索排名表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_monthly_keyword_rank PARTITION (dt = '2025-08-07')
SELECT
    keyword,
    month,
    search_count,
    -- 将RANK()返回的BIGINT显式转换为INT
    CAST(RANK() OVER (ORDER BY search_count DESC) AS INT) AS rank
FROM dws_search_stats
WHERE dt = '2025-08-07';

-- 9. 月关键词搜索的访问量
CREATE TABLE IF NOT EXISTS ads_monthly_keyword_visits (
    keyword STRING COMMENT '搜索关键词',
    month STRING COMMENT '统计月份',
    search_count BIGINT COMMENT '搜索次数',
    visitor_count BIGINT COMMENT '搜索访客数',
    click_count BIGINT COMMENT '点击次数',
    click_rate DECIMAL(5,2) COMMENT '点击率'
    ) COMMENT '月关键词搜索访问量表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_monthly_keyword_visits PARTITION (dt = '2025-08-07')
SELECT
    keyword,
    month,
    search_count,
    visitor_count,
    click_count,
    click_rate
FROM dws_search_stats
WHERE dt = '2025-08-07';

-- 10. 各用户人群访问占比
CREATE TABLE IF NOT EXISTS ads_user_crowd_visit_ratio (
    user_crowd STRING COMMENT '用户人群',
    age_group STRING COMMENT '年龄组',
    gender STRING COMMENT '性别',
    city_level STRING COMMENT '城市等级',
    visit_count BIGINT COMMENT '访问次数',
    visitor_count BIGINT COMMENT '访客数',
    visit_ratio DECIMAL(5,2) COMMENT '访问占比'
    ) COMMENT '各用户人群访问占比表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ALIORC;

INSERT OVERWRITE TABLE ads_user_crowd_visit_ratio PARTITION (dt = '2025-08-07')
SELECT
    user_crowd,
    age_group,
    gender,
    city_level,
    visit_count,
    visitor_count,
    visit_ratio
FROM dws_user_crowd_stats
WHERE dt = '2025-08-07';
