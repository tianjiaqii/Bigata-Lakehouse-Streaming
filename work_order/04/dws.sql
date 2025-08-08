--MaxCompute SQL
--********************************************************************--
--author: 田家齐
--create time: 2025-08-08 09:58:53
--********************************************************************--
-- ********************************************************************--
-- DWS层：数据汇总层
-- 基于DWD层数据，按分析维度进行汇总计算
-- ********************************************************************--

-- 1. 商品统计汇总表（支持指标1-6）
CREATE TABLE IF NOT EXISTS dws_product_stats (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    brand STRING COMMENT '品牌',
    -- 1天指标
    visitor_count_1d BIGINT COMMENT '1天访客数',
    view_count_1d BIGINT COMMENT '1天浏览量',
    pay_amount_1d DECIMAL(12,2) COMMENT '1天支付金额',
    pay_buyer_count_1d BIGINT COMMENT '1天支付买家数',
    pay_quantity_1d BIGINT COMMENT '1天支付件数',
    -- 7天指标
    visitor_count_7d BIGINT COMMENT '7天访客数',
    view_count_7d BIGINT COMMENT '7天浏览量',
    pay_amount_7d DECIMAL(12,2) COMMENT '7天支付金额',
    pay_buyer_count_7d BIGINT COMMENT '7天支付买家数',
    pay_quantity_7d BIGINT COMMENT '7天支付件数',
    -- 30天指标
    visitor_count_30d BIGINT COMMENT '30天访客数',
    view_count_30d BIGINT COMMENT '30天浏览量',
    pay_amount_30d DECIMAL(12,2) COMMENT '30天支付金额',
    pay_buyer_count_30d BIGINT COMMENT '30天支付买家数',
    pay_quantity_30d BIGINT COMMENT '30天支付件数'
    ) COMMENT '商品统计汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWS层商品统计汇总表，按1/7/30天汇总');

INSERT OVERWRITE TABLE dws_product_stats PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    MAX(product_name) AS product_name,
    MAX(category_id) AS category_id,
    MAX(category_name) AS category_name,
    MAX(brand) AS brand,
    -- 1天统计
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) = 0 THEN user_id END) AS visitor_count_1d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) = 0 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_1d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_1d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_1d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_1d,
    -- 7天统计
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 6 THEN user_id END) AS visitor_count_7d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 6 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_7d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_7d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_7d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_7d,
    -- 30天统计
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 29 THEN user_id END) AS visitor_count_30d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 29 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_30d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_30d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_30d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_30d
FROM (
         -- 关联用户行为和订单数据：在JOIN条件中直接限定dwd_order的分区
         SELECT
             COALESCE(ub.product_id, o.product_id) AS product_id,
             o.product_name,
             COALESCE(ub.category_id, o.category_id) AS category_id,
             COALESCE(ub.category_name, o.category_name) AS category_name,
             COALESCE(ub.brand, o.brand) AS brand,
             ub.user_id,
             ub.behavior_time,
             ub.behavior_type,
             o.pay_time,
             o.pay_status,
             o.product_pay_amount,
             o.buy_quantity
         FROM dwd_user_behavior ub
                  FULL JOIN dwd_order o
                            ON ub.product_id = o.product_id
                                AND ub.dt = o.dt
                                AND o.dt = '2025-08-07'  -- 在JOIN条件中直接限定dwd_order的分区
         WHERE ub.dt = '2025-08-07'  -- 限定dwd_user_behavior的分区
     ) t
GROUP BY product_id;

SELECT DISTINCT pay_status FROM dwd_order WHERE dt = '2025-08-07';

INSERT OVERWRITE TABLE dws_product_stats PARTITION (dt = '2025-08-07')
SELECT
    product_id,
    MAX(product_name) AS product_name,
    MAX(category_id) AS category_id,
    MAX(category_name) AS category_name,
    MAX(brand) AS brand,
    -- 1天统计（不变）
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) = 0 THEN user_id END) AS visitor_count_1d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) = 0 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_1d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_1d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_1d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) = 0 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_1d,
    -- 7天统计（不变）
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 6 THEN user_id END) AS visitor_count_7d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 6 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_7d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_7d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_7d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 6 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_7d,
    -- 30天统计（不变）
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 29 THEN user_id END) AS visitor_count_30d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(behavior_time)) BETWEEN 0 AND 29 AND behavior_type = 'browse' THEN 1 ELSE 0 END) AS view_count_30d,
    CAST(SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN product_pay_amount ELSE 0 END) AS DECIMAL(12,2)) AS pay_amount_30d,
    COUNT(DISTINCT CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN user_id END) AS pay_buyer_count_30d,
    SUM(CASE WHEN DATEDIFF('2025-08-07', to_date(pay_time)) BETWEEN 0 AND 29 AND pay_status = 'paid' THEN buy_quantity ELSE 0 END) AS pay_quantity_30d
FROM (
         -- 修复关联逻辑：改用 LEFT JOIN + UNION ALL，确保订单数据不丢失
         SELECT
             ub.product_id,
             o.product_name,
             ub.category_id,
             ub.category_name,
             ub.brand,
             ub.user_id,
             ub.behavior_time,
             ub.behavior_type,
             o.pay_time,
             o.pay_status,
             o.product_pay_amount,
             o.buy_quantity
         FROM dwd_user_behavior ub
                  LEFT JOIN dwd_order o
                            ON ub.product_id = o.product_id
                                AND ub.dt = o.dt
         WHERE ub.dt = '2025-08-07'  -- 限定行为数据分区

         UNION ALL

         -- 补充：只有订单数据，无用户行为数据的场景
         SELECT
             o.product_id,
             o.product_name,
             o.category_id,
             o.category_name,
             o.brand,
             NULL AS user_id,
             NULL AS behavior_time,
             NULL AS behavior_type,
             o.pay_time,
             o.pay_status,
             o.product_pay_amount,
             o.buy_quantity
         FROM dwd_order o
         WHERE o.dt = '2025-08-07'
           AND o.pay_status = 'paid'
           AND o.product_pay_amount > 0
           AND NOT EXISTS (
                 SELECT 1 FROM dwd_user_behavior ub
                 WHERE ub.product_id = o.product_id
                   AND ub.dt = '2025-08-07'
             )  -- 过滤已关联的订单，避免重复
     ) t
GROUP BY product_id;

-- 2. 渠道统计汇总表（支持指标7）
CREATE TABLE IF NOT EXISTS dws_channel_stats (
    channel_id STRING COMMENT '渠道ID',
    channel_name STRING COMMENT '渠道名称',
    channel_type STRING COMMENT '渠道类型',
    month STRING COMMENT '统计月份',
    session_count BIGINT COMMENT '会话数',
    visitor_count BIGINT COMMENT '访客数',
    total_stay_time BIGINT COMMENT '总停留时间(秒)',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时间(秒)'
    ) COMMENT '渠道统计汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWS层渠道统计汇总表，按月汇总');

INSERT OVERWRITE TABLE dws_channel_stats PARTITION (dt = '2025-08-07')
SELECT
    channel_id,
    channel_name,
    channel_type,
    -- 将字符串转换为TIMESTAMP类型后再使用date_format函数
    date_format(TO_TIMESTAMP('2025-08-07', 'yyyy-MM-dd'), 'yyyy-MM') AS month,
    COUNT(DISTINCT session_id) AS session_count,
    COUNT(DISTINCT user_id) AS visitor_count,
    SUM(stay_time) AS total_stay_time,
    CASE WHEN COUNT(DISTINCT session_id) > 0 THEN SUM(stay_time) / COUNT(DISTINCT session_id) ELSE 0 END AS avg_stay_time
FROM dwd_traffic_channel
WHERE dt = '2025-08-07'
    -- 同样需要将entry_time转换为TIMESTAMP类型后再进行格式转换比较（如果entry_time是STRING类型）
    -- 如果entry_time已经是TIMESTAMP类型，则不需要转换
    AND date_format(TO_TIMESTAMP(entry_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM') = date_format(TO_TIMESTAMP('2025-08-07', 'yyyy-MM-dd'), 'yyyy-MM')
GROUP BY channel_id, channel_name, channel_type;

-- 3. 搜索统计汇总表（支持指标8-9）
CREATE TABLE IF NOT EXISTS dws_search_stats (
    keyword STRING COMMENT '搜索关键词',
    month STRING COMMENT '统计月份',
    search_count BIGINT COMMENT '搜索次数',
    click_count BIGINT COMMENT '点击次数',
    click_rate DECIMAL(5,2) COMMENT '点击率',
    visitor_count BIGINT COMMENT '搜索访客数'
    ) COMMENT '搜索统计汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWS层搜索统计汇总表，按月汇总');

INSERT OVERWRITE TABLE dws_search_stats PARTITION (dt = '2025-08-07')
SELECT
    keyword,
    -- 将字符串日期转为TIMESTAMP后再格式化
    date_format(TO_TIMESTAMP('2025-08-07', 'yyyy-MM-dd'), 'yyyy-MM') AS month,
    COUNT(search_id) AS search_count,
    SUM(CASE WHEN click_product_id != '' THEN 1 ELSE 0 END) AS click_count,
    CASE WHEN COUNT(search_id) > 0 THEN SUM(CASE WHEN click_product_id != '' THEN 1 ELSE 0 END) / COUNT(search_id) ELSE 0 END AS click_rate,
    COUNT(DISTINCT user_id) AS visitor_count
FROM dwd_search_behavior
WHERE dt = '2025-08-07'
    -- 处理search_time字段，转为TIMESTAMP后再格式化比较
    AND date_format(TO_TIMESTAMP(search_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM') = date_format(TO_TIMESTAMP('2025-08-07', 'yyyy-MM-dd'), 'yyyy-MM')
GROUP BY keyword;

-- 4. 用户人群统计汇总表（支持指标10）
CREATE TABLE IF NOT EXISTS dws_user_crowd_stats (
    user_crowd STRING COMMENT '用户人群',
    age_group STRING COMMENT '年龄组',
    gender STRING COMMENT '性别',
    city_level STRING COMMENT '城市等级',
    visit_count BIGINT COMMENT '访问次数',
    visitor_count BIGINT COMMENT '访客数',
    visit_ratio DECIMAL(5,2) COMMENT '访问占比'
    ) COMMENT '用户人群统计汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWS层用户人群统计汇总表，按天汇总');

INSERT OVERWRITE TABLE dws_user_crowd_stats PARTITION (dt = '2025-08-07')
SELECT
    user_crowd,
    age_group,
    gender,
    city_level,
    COUNT(*) AS visit_count,
    COUNT(DISTINCT user_id) AS visitor_count,
    -- 计算占比
    COUNT(*) / SUM(COUNT(*)) OVER () AS visit_ratio
FROM dwd_user_behavior
WHERE dt = '2025-08-07'
GROUP BY user_crowd, age_group, gender, city_level;