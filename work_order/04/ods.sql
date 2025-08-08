--MaxCompute SQL
--********************************************************************--
--author: 田家齐
--create time: 2025-08-07 15:17:24
--********************************************************************--
-- 用户行为表
CREATE TABLE IF NOT EXISTS ods_user_behavior (
    user_id STRING COMMENT '用户唯一标识',
    product_id STRING COMMENT '商品ID',
    behavior_type STRING COMMENT '行为类型：visit-访问, browse-浏览, pay-支付, search-搜索',
    behavior_time TIMESTAMP COMMENT '行为发生时间',
    session_id STRING COMMENT '会话ID',
    device_id STRING COMMENT '设备ID（匿名用户）',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '用户行为原始数据表';

-- 商品信息表
CREATE TABLE IF NOT EXISTS ods_product_info (
    product_id STRING COMMENT '商品唯一标识',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    product_attributes STRING COMMENT '商品属性，JSON格式',
    putaway_time TIMESTAMP COMMENT '上架时间',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '商品基础信息原始表';

-- 订单表
CREATE TABLE IF NOT EXISTS ods_order (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    order_create_time TIMESTAMP COMMENT '订单创建时间',
    pay_time TIMESTAMP COMMENT '支付时间',
    total_pay_amount DECIMAL(10,2) COMMENT '订单总支付金额',
    pay_status STRING COMMENT '支付状态：paid-已支付, unpaid-未支付',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
    ) COMMENT '订单主表原始数据';

-- 订单明细表
CREATE TABLE IF NOT EXISTS ods_order_detail (
    order_detail_id STRING COMMENT '订单明细ID',
    order_id STRING COMMENT '关联订单ID',
    product_id STRING COMMENT '商品ID',
    buy_quantity INT COMMENT '购买件数',
    product_price DECIMAL(10,2) COMMENT '商品单价',
    product_pay_amount DECIMAL(10,2) COMMENT '商品实际支付金额',
    pay_time TIMESTAMP COMMENT '支付时间',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
    ) COMMENT '订单明细表原始数据';

-- 流量渠道表
CREATE TABLE IF NOT EXISTS ods_traffic_channel (
    session_id STRING COMMENT '会话ID',
    channel_id STRING COMMENT '渠道ID',
    channel_name STRING COMMENT '渠道名称（如：app、pc、小程序）',
    entry_time TIMESTAMP COMMENT '进入时间',
    exit_time TIMESTAMP COMMENT '离开时间',
    user_id STRING COMMENT '用户ID',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '流量来源渠道原始数据';

-- 搜索行为表
CREATE TABLE IF NOT EXISTS ods_search_behavior (
    search_id STRING COMMENT '搜索记录ID',
    user_id STRING COMMENT '用户ID',
    keyword STRING COMMENT '搜索关键词',
    search_time TIMESTAMP COMMENT '搜索时间',
    click_product_id STRING COMMENT '点击的商品ID',
    session_id STRING COMMENT '会话ID',
    shop_id STRING COMMENT '店铺ID',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '用户搜索行为原始数据';

-- 用户信息表
CREATE TABLE IF NOT EXISTS ods_user_info (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    gender STRING COMMENT '性别：male/female/unknown',
    age_group STRING COMMENT '年龄组：0-18/19-25/26-35/36+',
    register_time TIMESTAMP COMMENT '注册时间',
    user_level STRING COMMENT '用户等级：v1/v2/v3',
    city STRING COMMENT '所在城市',
    dt STRING COMMENT '分区日期，格式yyyy-MM-dd'
) COMMENT '用户基础信息原始表';

