-- 1. 商品流量明细事实表（支撑指标1-6：终端/无线的访客数、浏览量等）
DROP TABLE IF EXISTS dwd_product_traffic_detail;
CREATE TABLE dwd_product_traffic_detail (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称（冗余自DIM层，便于查询）',
    terminal_type VARCHAR(20) NOT NULL COMMENT '终端类型(terminal/wireless)',
    visitor_count INT COMMENT '访客数',
    detail_visitor_count INT COMMENT '详情页访客数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(6,2) COMMENT '平均停留时长(秒)',
    bounce_rate DECIMAL(5,4) COMMENT '跳出率',
    add_cart_count INT COMMENT '加购人数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, terminal_type, ds),
    -- 关联DIM层，确保维度一致性
    KEY idx_product (product_id),
    KEY idx_terminal (terminal_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量明细事实表，关联商品和终端维度';

-- 从ODS层抽取并关联DIM层清洗数据
INSERT INTO dwd_product_traffic_detail (
    product_id, product_name, terminal_type,
    visitor_count, detail_visitor_count, pv,
    avg_stay_time, bounce_rate, add_cart_count, ds
)
SELECT
    t.product_id,
    b.product_name,  -- 从商品基础维度表获取名称（确保一致性）
    t.terminal_type,
    -- 清洗：空值填充为0
    IFNULL(t.visitor_count, 0) AS visitor_count,
    IFNULL(t.detail_visitor_count, 0) AS detail_visitor_count,
    IFNULL(t.pv, 0) AS pv,
    -- 清洗：平均停留时长异常值（如<0）填充为0
    IF(t.avg_stay_time < 0, 0, IFNULL(t.avg_stay_time, 0)) AS avg_stay_time,
    -- 清洗：跳出率限制在0-1之间
    IF(t.bounce_rate < 0, 0, IF(t.bounce_rate > 1, 1, IFNULL(t.bounce_rate, 0))) AS bounce_rate,
    IFNULL(t.add_cart_count, 0) AS add_cart_count,
    t.ds
FROM ods_product_traffic t
-- 关联商品基础维度表（过滤无效商品ID，避免后续聚合错误）
INNER JOIN dim_product_basic b ON t.product_id = b.product_id
-- 关联终端维度表（确保终端类型标准化）
INNER JOIN dim_product_terminal ter ON t.product_id = ter.product_id AND t.terminal_type = ter.terminal_type;






-- 2. 商品颜色交易明细事实表（支撑指标7-11：不同颜色的支付金额、件数等）
DROP TABLE IF EXISTS dwd_product_color_trade_detail;
CREATE TABLE dwd_product_color_trade_detail (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    color VARCHAR(20) NOT NULL COMMENT '商品颜色',
    pay_amount DECIMAL(12,2) COMMENT '支付金额',
    pay_quantity INT COMMENT '支付件数',
    pay_buyer_count INT COMMENT '支付买家数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, color, ds),
    -- 关联DIM层
    KEY idx_product (product_id),
    KEY idx_color (color)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品颜色交易明细事实表，关联商品和颜色维度';

-- 从ODS层抽取并清洗数据
INSERT INTO dwd_product_color_trade_detail (
    product_id, product_name, color,
    pay_amount, pay_quantity, pay_buyer_count, ds
)
SELECT
    t.product_id,
    b.product_name,  -- 冗余商品名称
    t.color,
    -- 清洗：支付金额为负或空值填充为0
    IF(t.pay_amount < 0, 0, IFNULL(t.pay_amount, 0)) AS pay_amount,
    IFNULL(t.pay_quantity, 0) AS pay_quantity,
    IFNULL(t.pay_buyer_count, 0) AS pay_buyer_count,
    t.ds
FROM ods_product_color_trade t
-- 关联商品基础维度表（过滤无效商品）
INNER JOIN dim_product_basic b ON t.product_id = b.product_id
-- 关联颜色维度表（确保颜色标准化）
INNER JOIN dim_product_color c ON t.color = c.color;









-- 3. 商品渠道流量明细事实表（支撑指标12：不同渠道访客占比）
DROP TABLE IF EXISTS dwd_product_channel_detail;
CREATE TABLE dwd_product_channel_detail (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    channel VARCHAR(50) NOT NULL COMMENT '流量渠道',
    visitor_ratio DECIMAL(5,4) COMMENT '访客占比',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, channel, ds),
    -- 关联DIM层
    KEY idx_product (product_id),
    KEY idx_channel (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品渠道流量明细事实表，关联商品和渠道维度';

-- 从ODS层抽取并清洗数据
INSERT INTO dwd_product_channel_detail (
    product_id, product_name, channel, visitor_ratio, ds
)
SELECT
    t.product_id,
    b.product_name,
    t.channel,
    -- 清洗：占比异常值（<0或>1）填充为0
    IF(t.visitor_ratio < 0 OR t.visitor_ratio > 1, 0, IFNULL(t.visitor_ratio, 0)) AS visitor_ratio,
    t.ds
FROM ods_product_channel t
-- 关联商品基础维度表
         INNER JOIN dim_product_basic b ON t.product_id = b.product_id
-- 关联渠道维度表（确保渠道标准化）
         INNER JOIN dim_product_channel c ON t.channel = c.channel;







-- 4. 商品内容引流明细事实表（支撑指标13：直播/短视频/图文的引流数据）
DROP TABLE IF EXISTS dwd_product_content_detail;
CREATE TABLE dwd_product_content_detail (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    content_type VARCHAR(20) NOT NULL COMMENT '内容类型(live/video/image)',
    click_count INT COMMENT '点击次数',
    collect_count INT COMMENT '引导收藏次数',
    add_cart_from_content INT COMMENT '引导加购次数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, content_type, ds),
    -- 关联DIM层
    KEY idx_product (product_id),
    KEY idx_content_type (content_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品内容引流明细事实表，关联商品和内容类型维度';

-- 从ODS层抽取并清洗数据
INSERT INTO dwd_product_content_detail (
    product_id, product_name, content_type,
    click_count, collect_count, add_cart_from_content, ds
)
SELECT
    t.product_id,
    b.product_name,
    t.content_type,
    IFNULL(t.click_count, 0) AS click_count,
    IFNULL(t.collect_count, 0) AS collect_count,
    IFNULL(t.add_cart_from_content, 0) AS add_cart_from_content,
    t.ds
FROM ods_product_content t
-- 关联商品基础维度表
INNER JOIN dim_product_basic b ON t.product_id = b.product_id
-- 关联内容类型维度表（确保内容类型标准化）
INNER JOIN dim_product_content_type ct ON t.content_type = ct.content_type;







-- 5. 商品评价明细事实表（支撑指标14-15：不同人群/分数的评价数据）
DROP TABLE IF EXISTS dwd_product_comment_detail;
CREATE TABLE dwd_product_comment_detail (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    user_type VARCHAR(10) NOT NULL COMMENT '用户类型(all/old/new)',
    score_level INT NOT NULL COMMENT '评分等级(1-5)',
    comment_count INT COMMENT '评价数',
    positive_comment INT COMMENT '正面评价数',
    negative_comment INT COMMENT '负面评价数',
    active_comment INT COMMENT '主动评价数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, user_type, score_level, ds),
    -- 关联DIM层
    KEY idx_product (product_id),
    KEY idx_comment_dim (user_type, score_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品评价明细事实表，关联商品和评价辅助维度';

-- 从ODS层抽取并清洗数据
INSERT INTO dwd_product_comment_detail (
    product_id, product_name, user_type, score_level,
    comment_count, positive_comment, negative_comment, active_comment, ds
)
SELECT
    t.product_id,
    b.product_name,
    t.user_type,
    t.score_level,
    IFNULL(t.comment_count, 0) AS comment_count,
    IFNULL(t.positive_comment, 0) AS positive_comment,
    IFNULL(t.negative_comment, 0) AS negative_comment,
    IFNULL(t.active_comment, 0) AS active_comment,
    t.ds
FROM ods_product_comment t
-- 关联商品基础维度表
INNER JOIN dim_product_basic b ON t.product_id = b.product_id
-- 关联评价辅助维度表（确保用户类型和评分等级标准化）
INNER JOIN dim_product_comment_dim cd ON t.user_type = cd.user_type AND t.score_level = cd.score_level;




