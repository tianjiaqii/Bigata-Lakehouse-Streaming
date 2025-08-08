--MaxCompute SQL
--********************************************************************--
--author: 田家齐
--create time: 2025-08-07 16:58:26
--********************************************************************--
-- ********************************************************************--
-- DWD层：数据明细层
-- 基于ODS层和DIM层数据，进行清洗、转换和维度关联
-- ********************************************************************--

-- 1. 用户行为明细事实表
CREATE TABLE IF NOT EXISTS dwd_user_behavior (
    user_id STRING COMMENT '用户唯一标识',
    product_id STRING COMMENT '商品ID',
    behavior_type STRING COMMENT '行为类型：visit-访问, browse-浏览, pay-支付, search-搜索',
    behavior_time TIMESTAMP COMMENT '行为发生时间',
    session_id STRING COMMENT '会话ID',
    device_id STRING COMMENT '设备ID（匿名用户）',
    shop_id STRING COMMENT '店铺ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '用户性别',
    age_group STRING COMMENT '用户年龄组',
    user_level STRING COMMENT '用户等级',
    city STRING COMMENT '用户所在城市',
    city_level STRING COMMENT '城市等级',
    category_id STRING COMMENT '商品品类ID',
    category_name STRING COMMENT '商品品类名称',
    brand STRING COMMENT '商品品牌',
    user_crowd STRING COMMENT '用户人群分组',
    dt STRING COMMENT '分区日期'
) COMMENT '用户行为明细事实表'
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWD层用户行为明细事实表，关联用户、商品维度信息');

INSERT OVERWRITE TABLE dwd_user_behavior
SELECT
    oub.user_id,
    oub.product_id,
    oub.behavior_type,
    oub.behavior_time,
    oub.session_id,
    oub.device_id,
    oub.shop_id,
    dui.user_name,
    dui.gender,
    dui.age_group,
    dui.user_level,
    dui.city,
    dui.city_level,
    dpi.category_id,
    dpi.category_name,
    dpi.brand,
    -- 用户人群分组
    CASE
        WHEN dui.age_group = '0-18' THEN '青少年'
        WHEN dui.age_group = '19-25' THEN '青年'
        WHEN dui.age_group = '26-35' THEN '中青年'
        WHEN dui.age_group = '36-45' THEN '中年'
        ELSE '中老年'
        END AS user_crowd,
    '2025-08-07' AS dt
FROM ods_user_behavior oub
         JOIN dim_user_info dui
              ON oub.user_id = dui.user_id
         JOIN dim_product_info dpi
              ON oub.product_id = dpi.product_id ;

DROP TABLE dwd_user_behavior ;

-- 2. 订单明细事实表（修改为分区表，符合数仓规范）
CREATE TABLE IF NOT EXISTS dwd_order (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    order_create_time TIMESTAMP COMMENT '订单创建时间',
    pay_time TIMESTAMP COMMENT '支付时间',
    total_pay_amount DECIMAL(10,2) COMMENT '订单总支付金额',
    pay_status STRING COMMENT '支付状态：paid-已支付, unpaid-未支付',
    shop_id STRING COMMENT '店铺ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '用户性别',
    age_group STRING COMMENT '用户年龄组',
    user_level STRING COMMENT '用户等级',
    city STRING COMMENT '用户所在城市',
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '商品品类ID',
    category_name STRING COMMENT '商品品类名称',
    brand STRING COMMENT '商品品牌',
    buy_quantity INT COMMENT '购买件数',
    product_price DECIMAL(10,2) COMMENT '商品单价',
    product_pay_amount DECIMAL(10,2) COMMENT '商品实际支付金额'
    ) COMMENT '订单明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式yyyy-MM-dd')  -- 分区列单独声明
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWD层订单明细事实表，关联用户、商品维度信息');
DROP TABLE dwd_order ;

-- 插入数据（修改参数和关联逻辑，确保数据能匹配）
INSERT OVERWRITE TABLE dwd_order PARTITION (dt = '2025-08-07')
SELECT
    oo.order_id,
    oo.user_id,
    oo.order_create_time,
    oo.pay_time,
    oo.total_pay_amount,
    oo.pay_status,
    oo.shop_id,
    -- 优先从维度表取数，禁用默认值，强制关联
    dui.user_name,
    dui.gender,
    dui.age_group,
    dui.user_level,
    dui.city,
    dpi.product_id,
    dpi.product_name,
    dpi.category_id,
    dpi.category_name,
    dpi.brand,
    ood.buy_quantity,
    ood.product_price,
    ood.product_pay_amount
FROM ods_order oo
-- 关联订单明细（必须关联，补充商品粒度信息）
         INNER JOIN ods_order_detail ood
                    ON oo.order_id = ood.order_id
                        AND oo.dt = ood.dt  -- 确保主表与明细表同分区
-- 关联用户维度（必须关联成功，否则数据丢弃）
         INNER JOIN dim_user_info dui
                    ON oo.user_id = dui.user_id
                        AND oo.dt = dui.dt  -- 维度表与事实表同分区
-- 关联商品维度（必须关联成功，否则数据丢弃）
         INNER JOIN dim_product_info dpi
                    ON ood.product_id = dpi.product_id
                        AND ood.dt = dpi.dt  -- 维度表与明细表同分区
WHERE oo.dt = '2025-08-07';  -- 限定事实表分区

-- 3. 流量渠道明细事实表
CREATE TABLE IF NOT EXISTS dwd_traffic_channel (
    session_id STRING COMMENT '会话ID',
    channel_id STRING COMMENT '渠道ID',
    channel_name STRING COMMENT '渠道名称',
    channel_type STRING COMMENT '渠道类型（自有/第三方）',
    entry_time TIMESTAMP COMMENT '进入时间',
    exit_time TIMESTAMP COMMENT '离开时间',
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '用户性别',
    age_group STRING COMMENT '用户年龄组',
    user_level STRING COMMENT '用户等级',
    city STRING COMMENT '用户所在城市',
    shop_id STRING COMMENT '店铺ID',
    stay_time INT COMMENT '停留时间(秒)'  -- 表定义为INT
) COMMENT '流量渠道明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWD层流量渠道明细事实表，关联用户、渠道维度信息');

DROP TABLE dwd_traffic_channel ;

INSERT OVERWRITE TABLE dwd_traffic_channel PARTITION (dt = '2025-08-07')
SELECT
    otc.session_id,
    otc.channel_id,
    otc.channel_name,
    dci.channel_type,
    otc.entry_time,
    otc.exit_time,
    otc.user_id,
    dui.user_name,
    dui.gender,
    dui.age_group,
    dui.user_level,
    dui.city,
    otc.shop_id,
    -- 显式转换为INT类型，与表定义匹配
    CAST(UNIX_TIMESTAMP(otc.exit_time) - UNIX_TIMESTAMP(otc.entry_time) AS INT) AS stay_time
FROM ods_traffic_channel otc
         LEFT JOIN dim_channel_info dci
                   ON otc.channel_id = dci.channel_id
                       AND otc.dt = dci.dt
         LEFT JOIN dim_user_info dui
                   ON otc.user_id = dui.user_id
                       AND otc.dt = dui.dt
WHERE otc.dt = '2025-08-07';

-- 4. 搜索行为明细事实表
CREATE TABLE IF NOT EXISTS dwd_search_behavior (
    search_id STRING COMMENT '搜索记录ID',
    user_id STRING COMMENT '用户ID',
    keyword STRING COMMENT '搜索关键词',
    search_time TIMESTAMP COMMENT '搜索时间',
    click_product_id STRING COMMENT '点击的商品ID',
    session_id STRING COMMENT '会话ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '用户性别',
    age_group STRING COMMENT '用户年龄组',
    user_level STRING COMMENT '用户等级',
    city STRING COMMENT '用户所在城市',
    shop_id STRING COMMENT '店铺ID',
    category_id STRING COMMENT '商品品类ID',
    category_name STRING COMMENT '商品品类名称',
    brand STRING COMMENT '商品品牌'
) COMMENT '搜索行为明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = 'DWD层搜索行为明细事实表，关联用户、商品维度信息');

INSERT OVERWRITE TABLE dwd_search_behavior PARTITION (dt = '2025-08-07')
SELECT
    osb.search_id,
    osb.user_id,
    osb.keyword,
    osb.search_time,
    osb.click_product_id,
    osb.session_id,
    dui.user_name,
    dui.gender,
    dui.age_group,
    dui.user_level,
    dui.city,
    osb.shop_id,
    CASE WHEN osb.click_product_id != '' THEN dpi.category_id ELSE NULL END AS category_id,
    CASE WHEN osb.click_product_id != '' THEN dpi.category_name ELSE NULL END AS category_name,
    CASE WHEN osb.click_product_id != '' THEN dpi.brand ELSE NULL END AS brand
FROM ods_search_behavior osb
         JOIN dim_user_info dui
              ON osb.user_id = dui.user_id
                  AND osb.dt = dui.dt
         LEFT JOIN dim_product_info dpi
                   ON osb.click_product_id = dpi.product_id
                       AND osb.dt = dpi.dt
WHERE osb.dt = '2025-08-07';
