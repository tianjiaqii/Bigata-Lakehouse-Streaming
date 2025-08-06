use test03_02;
-- 1. 商品流量汇总表（按日+商品+终端汇总，支撑指标1-6）
DROP TABLE IF EXISTS dws_product_traffic_sum;
CREATE TABLE dws_product_traffic_sum (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    terminal_type VARCHAR(20) NOT NULL COMMENT '终端类型(terminal/wireless)',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    -- 核心流量指标（按日汇总）
    total_visitor_count INT NOT NULL DEFAULT 0 COMMENT '当日访客数',
    total_detail_visitor INT NOT NULL DEFAULT 0 COMMENT '当日详情页访客数',
    total_pv INT NOT NULL DEFAULT 0 COMMENT '当日浏览量',
    avg_stay_time DECIMAL(6,2) COMMENT '当日平均停留时长(秒)',
    avg_bounce_rate DECIMAL(5,4) COMMENT '当日详情页平均跳出率',
    total_add_cart INT NOT NULL DEFAULT 0 COMMENT '当日加购人数',
    PRIMARY KEY (product_id, terminal_type, ds),
    KEY idx_product (product_id),
    KEY idx_terminal (terminal_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量按日汇总表，支撑终端/无线的流量指标';

-- 从DWD层聚合数据
INSERT INTO dws_product_traffic_sum (
    product_id, product_name, terminal_type, ds,
    total_visitor_count, total_detail_visitor, total_pv,
    avg_stay_time, avg_bounce_rate, total_add_cart
)
SELECT
    product_id,
    product_name,
    terminal_type,
    ds,
    SUM(visitor_count) AS total_visitor_count,  -- 汇总当日访客数
    SUM(detail_visitor_count) AS total_detail_visitor,  -- 汇总详情页访客
    SUM(pv) AS total_pv,  -- 汇总浏览量
    AVG(avg_stay_time) AS avg_stay_time,  -- 平均停留时长（日均）
    AVG(bounce_rate) AS avg_bounce_rate,  -- 平均跳出率（日均）
    SUM(add_cart_count) AS total_add_cart  -- 汇总加购人数
FROM dwd_product_traffic_detail
GROUP BY product_id, product_name, terminal_type, ds;





-- 2. 商品颜色交易汇总表（按日+商品+颜色汇总，支撑指标7-11）
DROP TABLE IF EXISTS dws_product_color_trade_sum;
CREATE TABLE dws_product_color_trade_sum (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    color VARCHAR(20) NOT NULL COMMENT '商品颜色',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    -- 核心交易指标（按日汇总）
    total_pay_amount DECIMAL(12,2) NOT NULL DEFAULT 0 COMMENT '当日支付金额',
    total_pay_quantity INT NOT NULL DEFAULT 0 COMMENT '当日支付件数',
    total_pay_buyer INT NOT NULL DEFAULT 0 COMMENT '当日支付买家数',
    PRIMARY KEY (product_id, color, ds),
    KEY idx_product (product_id),
    KEY idx_color (color)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品颜色交易按日汇总表，支撑颜色维度的交易指标';

-- 从DWD层聚合数据
INSERT INTO dws_product_color_trade_sum (
    product_id, product_name, color, ds,
    total_pay_amount, total_pay_quantity, total_pay_buyer
)
SELECT
    product_id,
    product_name,
    color,
    ds,
    SUM(pay_amount) AS total_pay_amount,  -- 汇总当日支付金额
    SUM(pay_quantity) AS total_pay_quantity,  -- 汇总支付件数
    SUM(pay_buyer_count) AS total_pay_buyer  -- 汇总支付买家数
FROM dwd_product_color_trade_detail
GROUP BY product_id, product_name, color, ds;






-- 3. 商品渠道流量汇总表（按日+商品+渠道汇总，支撑指标12）
DROP TABLE IF EXISTS dws_product_channel_sum;
CREATE TABLE dws_product_channel_sum (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    channel VARCHAR(50) NOT NULL COMMENT '流量渠道',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    -- 渠道流量指标（按日汇总）
    visitor_ratio DECIMAL(5,4) COMMENT '当日访客占比',
    PRIMARY KEY (product_id, channel, ds),
    KEY idx_product (product_id),
    KEY idx_channel (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品渠道流量按日汇总表，支撑渠道访客占比指标';

-- 从DWD层聚合数据（渠道占比取日均或直接复用汇总值）
INSERT INTO dws_product_channel_sum (
    product_id, product_name, channel, ds, visitor_ratio
)
SELECT
    product_id,
    product_name,
    channel,
    ds,
    AVG(visitor_ratio) AS visitor_ratio  -- 渠道访客占比（日均）
FROM dwd_product_channel_detail
GROUP BY product_id, product_name, channel, ds;






-- 4. 商品内容引流汇总表（按日+商品+内容类型汇总，支撑指标13）
DROP TABLE IF EXISTS dws_product_content_sum;
CREATE TABLE dws_product_content_sum (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    content_type VARCHAR(20) NOT NULL COMMENT '内容类型(live/video/image)',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    -- 内容引流指标（按日汇总）
    total_click_count INT NOT NULL DEFAULT 0 COMMENT '当日点击次数',
    total_collect_count INT NOT NULL DEFAULT 0 COMMENT '当日引导收藏次数',
    total_add_cart_content INT NOT NULL DEFAULT 0 COMMENT '当日引导加购次数',
    PRIMARY KEY (product_id, content_type, ds),
    KEY idx_product (product_id),
    KEY idx_content_type (content_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品内容引流按日汇总表，支撑内容引流TOP50指标';

-- 从DWD层聚合数据
INSERT INTO dws_product_content_sum (
    product_id, product_name, content_type, ds,
    total_click_count, total_collect_count, total_add_cart_content
)
SELECT
    product_id,
    product_name,
    content_type,
    ds,
    SUM(click_count) AS total_click_count,  -- 汇总当日点击次数
    SUM(collect_count) AS total_collect_count,  -- 汇总收藏次数
    SUM(add_cart_from_content) AS total_add_cart_content  -- 汇总引导加购次数
FROM dwd_product_content_detail
GROUP BY product_id, product_name, content_type, ds;






-- 5. 商品评价汇总表（按日+商品+用户类型+评分等级汇总，支撑指标14-15）
DROP TABLE IF EXISTS dws_product_comment_sum;
CREATE TABLE dws_product_comment_sum (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    user_type VARCHAR(10) NOT NULL COMMENT '用户类型(all/old/new)',
    score_level INT NOT NULL COMMENT '评分等级(1-5)',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    -- 评价指标（按日汇总）
    total_comment_count INT NOT NULL DEFAULT 0 COMMENT '当日评价数',
    total_positive_comment INT NOT NULL DEFAULT 0 COMMENT '当日正面评价数',
    total_negative_comment INT NOT NULL DEFAULT 0 COMMENT '当日负面评价数',
    total_active_comment INT NOT NULL DEFAULT 0 COMMENT '当日主动评价数',
    PRIMARY KEY (product_id, user_type, score_level, ds),
    KEY idx_product (product_id),
    KEY idx_user_type (user_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品评价按日汇总表，支撑评价相关指标';

-- 从DWD层聚合数据
INSERT INTO dws_product_comment_sum (
    product_id, product_name, user_type, score_level, ds,
    total_comment_count, total_positive_comment, total_negative_comment, total_active_comment
)
SELECT
    product_id,
    product_name,
    user_type,
    score_level,
    ds,
    SUM(comment_count) AS total_comment_count,  -- 汇总当日评价数
    SUM(positive_comment) AS total_positive_comment,  -- 汇总正面评价
    SUM(negative_comment) AS total_negative_comment,  -- 汇总负面评价
    SUM(active_comment) AS total_active_comment  -- 汇总主动评价
FROM dwd_product_comment_detail
GROUP BY product_id, product_name, user_type, score_level, ds;

