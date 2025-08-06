use test03_02;
-- 商品基础维度表
CREATE TABLE dim_product_basic (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    color VARCHAR(20) COMMENT '商品颜色',
    category VARCHAR(50) COMMENT '商品类目',
    brand VARCHAR(50) COMMENT '商品品牌',
    price DECIMAL(10,2) COMMENT '商品单价',
    shelf_time DATE COMMENT '上架时间',
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品基础维度表，整合 ods_product_info 核心信息';

-- 从 ods 层同步数据（可根据实际增量/全量需求调整，这里先全量示例）
INSERT INTO dim_product_basic (
    product_id,
    product_name,
    color,
    category,
    brand,
    price,
    shelf_time
)
SELECT
    product_id,
    product_name,
    color,
    category,
    brand,
    price,
    shelf_time
FROM ods_product_info;



-- 商品 - 终端维度关联表
CREATE TABLE dim_product_terminal (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    terminal_type VARCHAR(20) NOT NULL COMMENT '终端类型(terminal/wireless)',
    PRIMARY KEY (product_id, terminal_type),
    CONSTRAINT fk_product_terminal FOREIGN KEY (product_id) REFERENCES dim_product_basic(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品与终端类型关联维度表，关联 ods_product_traffic 终端信息';

-- 从 ods 层抽取终端类型数据（去重）
-- 只插入在 dim_product_basic 中存在的 product_id，避免外键冲突
INSERT INTO dim_product_terminal (product_id, terminal_type)
SELECT DISTINCT
    t.product_id,
    t.terminal_type
FROM ods_product_traffic t
-- 关联 dim_product_basic，过滤掉不存在的 product_id
INNER JOIN dim_product_basic b ON t.product_id = b.product_id;


-- 商品颜色维度表
CREATE TABLE dim_product_color (
    color VARCHAR(20) NOT NULL COMMENT '商品颜色',
    PRIMARY KEY (color)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品颜色维度表，从 ods_product_info 及 ods_product_color_trade 抽离颜色';

-- 从 ods 层抽取颜色数据（去重）
INSERT INTO dim_product_color (color)
SELECT DISTINCT color
FROM ods_product_info
UNION
SELECT DISTINCT color
FROM ods_product_color_trade;




-- 商品渠道维度表
CREATE TABLE dim_product_channel (
    channel VARCHAR(50) NOT NULL COMMENT '流量渠道',
    PRIMARY KEY (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量渠道维度表，从 ods_product_channel 抽离渠道';

-- 从 ods 层抽取渠道数据（去重）
INSERT INTO dim_product_channel (channel)
SELECT DISTINCT channel
FROM ods_product_channel;



-- 商品内容类型维度表
CREATE TABLE dim_product_content_type (
    content_type VARCHAR(20) NOT NULL COMMENT '内容类型(live/video/image)',
    PRIMARY KEY (content_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品内容类型维度表，从 ods_product_content 抽离内容类型';

-- 从 ods 层抽取内容类型数据（去重）
INSERT INTO dim_product_content_type (content_type)
SELECT DISTINCT content_type
FROM ods_product_content;



-- 商品评价维度辅助表
CREATE TABLE dim_product_comment_dim (
    user_type VARCHAR(10) NOT NULL COMMENT '用户类型(all/old/new)',
    score_level INT NOT NULL COMMENT '评分等级(1-5)',
    PRIMARY KEY (user_type, score_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品评价维度辅助表，从 ods_product_comment 抽离用户类型、评分等级';

-- 从 ods 层抽取评价维度数据（去重）
INSERT INTO dim_product_comment_dim (user_type, score_level)
SELECT DISTINCT
    user_type,
    score_level
FROM ods_product_comment;