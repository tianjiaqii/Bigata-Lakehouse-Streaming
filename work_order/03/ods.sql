use test03_02;
-- 1. 商品维度表（保持结构不变）
CREATE TABLE ods_product_info (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    color VARCHAR(20) COMMENT '商品颜色',
    category VARCHAR(50) COMMENT '商品类目',
    brand VARCHAR(50) COMMENT '商品品牌',
    price DECIMAL(10,2) COMMENT '商品单价',
    shelf_time DATE COMMENT '上架时间',
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品基础信息维度表';

-- 2. 商品流量事实表（结构不变）
CREATE TABLE ods_product_traffic (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    terminal_type VARCHAR(20) NOT NULL COMMENT '终端类型(terminal/wireless)',
    visitor_count INT COMMENT '访客数',
    detail_visitor_count INT COMMENT '详情页访客数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(6,2) COMMENT '平均停留时长(秒)',
    bounce_rate DECIMAL(5,4) COMMENT '跳出率',
    add_cart_count INT COMMENT '加购人数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, terminal_type, ds)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量事实表';

-- 3. 商品颜色交易事实表（结构不变）
CREATE TABLE ods_product_color_trade (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    color VARCHAR(20) NOT NULL COMMENT '商品颜色',
    pay_amount DECIMAL(12,2) COMMENT '支付金额',
    pay_quantity INT COMMENT '支付件数',
    pay_buyer_count INT COMMENT '支付买家数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, color, ds)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品颜色交易事实表';

-- 4. 商品渠道流量表（结构不变）
CREATE TABLE ods_product_channel (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    channel VARCHAR(50) NOT NULL COMMENT '流量渠道',
    visitor_ratio DECIMAL(5,4) COMMENT '访客占比',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, channel, ds)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品渠道流量表';

-- 5. 商品内容引流表（结构不变）
CREATE TABLE ods_product_content (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    content_type VARCHAR(20) NOT NULL COMMENT '内容类型(live/video/image)',
    click_count INT COMMENT '点击次数',
    collect_count INT COMMENT '引导收藏次数',
    add_cart_from_content INT COMMENT '引导加购次数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, content_type, ds)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品内容引流表';

-- 6. 商品评价表（结构不变）
CREATE TABLE ods_product_comment (
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    user_type VARCHAR(10) NOT NULL COMMENT '用户类型(all/old/new)',
    score_level INT NOT NULL COMMENT '评分等级(1-5)',
    comment_count INT COMMENT '评价数',
    positive_comment INT COMMENT '正面评价数',
    negative_comment INT COMMENT '负面评价数',
    active_comment INT COMMENT '主动评价数',
    ds VARCHAR(8) NOT NULL COMMENT '日期分区(yyyyMMdd)',
    PRIMARY KEY (product_id, user_type, score_level, ds)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品评价表';

-- 生成商品维度表数据（仅5个商品，名称符合实际场景）
INSERT INTO ods_product_info (product_id, product_name, color, category, brand, price, shelf_time)
SELECT
    CONCAT('prod_', LPAD(CAST(t.id AS CHAR), 6, '0')),  -- 商品ID：prod_000001 至 prod_000005
    -- 实际场景商品名称（5个）
    CASE t.id
        WHEN 1 THEN '纯棉宽松短袖T恤'
        WHEN 2 THEN '无线蓝牙耳机'
        WHEN 3 THEN '全自动洗衣机'
        WHEN 4 THEN '复古真皮斜挎包'
        ELSE '夏季透气运动鞋'
        END AS product_name,
    -- 颜色随机（与商品匹配）
    CASE
        WHEN t.id = 1 THEN CASE t.color % 5 WHEN 0 THEN '白色' WHEN 1 THEN '黑色' WHEN 2 THEN '灰色' WHEN 3 THEN '蓝色' ELSE '红色' END
        WHEN t.id = 2 THEN CASE t.color % 3 WHEN 0 THEN '白色' WHEN 1 THEN '黑色' ELSE '银色' END
        WHEN t.id = 3 THEN CASE t.color % 2 WHEN 0 THEN '白色' ELSE '银色' END
        WHEN t.id = 4 THEN CASE t.color % 4 WHEN 0 THEN '黑色' WHEN 1 THEN '棕色' WHEN 2 THEN '酒红色' ELSE '米色' END
        ELSE CASE t.color % 4 WHEN 0 THEN '白色' WHEN 1 THEN '黑色' WHEN 2 THEN '蓝色' ELSE '绿色' END
        END AS color,
    -- 类目（与商品匹配）
    CASE t.id
        WHEN 1 THEN '男装'
        WHEN 2 THEN '数码'
        WHEN 3 THEN '家电'
        WHEN 4 THEN '鞋包'
        ELSE '运动鞋'
        END AS category,
    -- 品牌（与商品匹配）
    CASE t.id
        WHEN 1 THEN '优衣库'
        WHEN 2 THEN '华为'
        WHEN 3 THEN '海尔'
        WHEN 4 THEN 'Coach'
        ELSE '耐克'
        END AS brand,
    -- 价格（与商品匹配，合理区间）
    CASE t.id
        WHEN 1 THEN 59.9 + FLOOR(RAND() * 40)  -- 59.9-99.9元
        WHEN 2 THEN 199 + FLOOR(RAND() * 300)  -- 199-499元
        WHEN 3 THEN 1599 + FLOOR(RAND() * 1000)  -- 1599-2599元
        WHEN 4 THEN 899 + FLOOR(RAND() * 600)  -- 899-1499元
        ELSE 499 + FLOOR(RAND() * 500)  -- 499-999元
        END AS price,
    -- 上架时间（近1年）
    DATE_SUB('2025-07-05', INTERVAL FLOOR(RAND() * 365) DAY) AS shelf_time
FROM (
         -- 仅生成5条数据（id=1至5）
         SELECT
             @row := @row + 1 AS id,
             FLOOR(RAND() * 10) AS color  -- 用于随机颜色
         FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) tmp,
              (SELECT @row := 0) r
     ) t;

-- 生成日期维度数据(2025-07-05至2025-08-05)（保持不变）
DROP TABLE IF EXISTS tmp_dates;
CREATE TEMPORARY TABLE tmp_dates (
                                     ds VARCHAR(8) NOT NULL,
                                     date DATE NOT NULL
);

DELIMITER //
CREATE PROCEDURE generate_dates()
BEGIN
    DECLARE curr_date DATE DEFAULT '2025-07-05';
    WHILE curr_date <= '2025-08-05' DO
            INSERT INTO tmp_dates (ds, date) VALUES (DATE_FORMAT(curr_date, '%Y%m%d'), curr_date);
            SET curr_date = DATE_ADD(curr_date, INTERVAL 1 DAY);
        END WHILE;
END //
DELIMITER ;
CALL generate_dates();

-- 生成商品流量事实表数据（仅关联5个商品）
DROP TEMPORARY TABLE IF EXISTS tmp_traffic_data;
CREATE TEMPORARY TABLE tmp_traffic_data (
    product_id VARCHAR(20),
    terminal_type VARCHAR(20),
    visitor_count INT,
    detail_visitor_count INT,
    pv INT,
    avg_stay_time DECIMAL(6,2),
    bounce_rate DECIMAL(5,4),
    add_cart_count INT,
    ds VARCHAR(8),
    UNIQUE KEY (product_id, terminal_type, ds)
);

DELIMITER //
CREATE PROCEDURE generate_traffic_data()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE max_records INT DEFAULT 10000;
    DECLARE product_num VARCHAR(20);
    DECLARE terminal VARCHAR(20);
    DECLARE date_str VARCHAR(8);

    WHILE i < max_records DO
            -- 仅从5个商品中随机选择（prod_000001至prod_000005）
            SET product_num = CONCAT('prod_', LPAD(CAST(FLOOR(RAND() * 5) + 1 AS CHAR), 6, '0'));
            SET terminal = CASE WHEN FLOOR(RAND() * 3) = 0 THEN 'terminal' ELSE 'wireless' END;
            SET date_str = (SELECT ds FROM tmp_dates ORDER BY RAND() LIMIT 1);

            IF NOT EXISTS (
                    SELECT 1 FROM tmp_traffic_data
                    WHERE product_id = product_num AND terminal_type = terminal AND ds = date_str
                ) THEN
                INSERT INTO tmp_traffic_data (
                    product_id, terminal_type, visitor_count, detail_visitor_count,
                    pv, avg_stay_time, bounce_rate, add_cart_count, ds
                )
                SELECT
                    product_num,
                    terminal,
                    CASE WHEN RAND() < 0.8 THEN FLOOR(RAND() * 100)
                         WHEN RAND() < 0.95 THEN 100 + FLOOR(RAND() * 900)
                         ELSE 1000 + FLOOR(RAND() * 9000) END,
                    FLOOR(visitor_count * (0.7 + RAND() * 0.2)),
                    FLOOR(visitor_count * (1 + RAND() * 4)),
                    ROUND(10 + RAND() * 290, 2),
                    ROUND(0.1 + RAND() * 0.7, 4),
                    FLOOR(visitor_count * (0.05 + RAND() * 0.25)),
                    date_str
                FROM (
                         SELECT CASE WHEN RAND() < 0.8 THEN FLOOR(RAND() * 100)
                                     WHEN RAND() < 0.95 THEN 100 + FLOOR(RAND() * 900)
                                     ELSE 1000 + FLOOR(RAND() * 9000) END AS visitor_count
                     ) t;
                SET i = i + 1;
            END IF;
        END WHILE;
END //
DELIMITER ;
CALL generate_traffic_data();
INSERT INTO ods_product_traffic SELECT * FROM tmp_traffic_data;
DROP TEMPORARY TABLE IF EXISTS tmp_traffic_data;
DROP PROCEDURE IF EXISTS generate_traffic_data;

-- 生成商品颜色交易事实表数据（仅关联5个商品）
DROP TEMPORARY TABLE IF EXISTS tmp_color_trade;
CREATE TEMPORARY TABLE tmp_color_trade (
    product_id VARCHAR(20),
    color VARCHAR(20),
    pay_amount DECIMAL(12,2),
    pay_quantity INT,
    pay_buyer_count INT,
    ds VARCHAR(8),
    UNIQUE KEY (product_id, color, ds)
);

DELIMITER //
CREATE PROCEDURE generate_color_trade_data()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE max_records INT DEFAULT 10000;
    WHILE i < max_records DO
            -- 仅从ods_product_info的5个商品中选择
            INSERT IGNORE INTO tmp_color_trade
            SELECT
                p.product_id,
                p.color,
                ROUND(p.price * (1 + FLOOR(RAND() * 50)), 2),
                1 + FLOOR(RAND() * 50),
                1 + FLOOR(RAND() * LEAST(1 + FLOOR(RAND() * 50), 20)),
                d.ds
            FROM ods_product_info p  -- 仅5个商品
                     CROSS JOIN tmp_dates d
            WHERE RAND() < 0.65
            LIMIT 1;
            SET i = i + ROW_COUNT();
        END WHILE;
END //
DELIMITER ;
CALL generate_color_trade_data();
INSERT INTO ods_product_color_trade SELECT * FROM tmp_color_trade;
DROP TEMPORARY TABLE IF EXISTS tmp_color_trade;
DROP PROCEDURE IF EXISTS generate_color_trade_data;

-- 生成商品渠道流量表数据（仅关联5个商品）
DROP TEMPORARY TABLE IF EXISTS tmp_channel_data;
CREATE TEMPORARY TABLE tmp_channel_data (
    product_id VARCHAR(20),
    channel VARCHAR(50),
    ratio DECIMAL(10,4),
    ds VARCHAR(8),
    total_ratio DECIMAL(10,4)
);

INSERT INTO tmp_channel_data (product_id, channel, ratio, ds)
SELECT
    -- 仅5个商品
    CONCAT('prod_', LPAD(CAST(FLOOR(RAND() * 5) + 1 AS CHAR), 6, '0')) AS product_id,
    c.channel,
    1 + FLOOR(RAND() * 10) AS ratio,
    d.ds
FROM tmp_dates d
         CROSS JOIN (
    SELECT 'search' AS channel UNION SELECT 'recommend' UNION
    SELECT 'direct' UNION SELECT 'activity' UNION SELECT 'other'
) c
         JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t
LIMIT 10000;

-- 计算渠道占比（保持逻辑不变）
UPDATE tmp_channel_data t
    JOIN (SELECT product_id, ds, SUM(ratio) AS sum_ratio FROM tmp_channel_data GROUP BY product_id, ds) s
    ON t.product_id = s.product_id AND t.ds = s.ds
SET t.total_ratio = s.sum_ratio;

CREATE TEMPORARY TABLE tmp_channel_totals AS
SELECT product_id, ds, SUM(ratio) AS total_ratio FROM tmp_channel_data GROUP BY product_id, ds;

INSERT INTO ods_product_channel (product_id, channel, visitor_ratio, ds)
SELECT
    d.product_id,
    d.channel,
    ROUND(AVG(d.ratio / t.total_ratio), 4) AS visitor_ratio,
    d.ds
FROM tmp_channel_data d
         JOIN tmp_channel_totals t ON d.product_id = t.product_id AND d.ds = t.ds
GROUP BY d.product_id, d.channel, d.ds;

DROP TEMPORARY TABLE IF EXISTS tmp_channel_totals;

-- 生成商品内容引流表数据（仅关联5个商品）
DROP TEMPORARY TABLE IF EXISTS tmp_content_data;
CREATE TEMPORARY TABLE tmp_content_data (
    product_id VARCHAR(20),
    content_type VARCHAR(20),
    click_count INT,
    collect_count INT,
    add_cart_from_content INT,
    ds VARCHAR(8),
    UNIQUE KEY (product_id, content_type, ds)
);

DELIMITER //
CREATE PROCEDURE generate_content_data()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE max_records INT DEFAULT 10000;
    WHILE i < max_records DO
            INSERT IGNORE INTO tmp_content_data
            SELECT
                -- 仅5个商品
                CONCAT('prod_', LPAD(CAST(FLOOR(RAND() * 5) + 1 AS CHAR), 6, '0')),
                CASE FLOOR(RAND() * 3) WHEN 0 THEN 'live' WHEN 1 THEN 'video' ELSE 'image' END,
                CASE WHEN RAND() < 0.7 THEN FLOOR(RAND() * 100)
                     WHEN RAND() < 0.9 THEN 100 + FLOOR(RAND() * 900)
                     ELSE 1000 + FLOOR(RAND() * 4000) END,
                FLOOR(click_count * (0.01 + RAND() * 0.09)),
                FLOOR(click_count * (0.02 + RAND() * 0.13)),
                (SELECT ds FROM tmp_dates ORDER BY RAND() LIMIT 1)
            FROM (
                     SELECT CASE WHEN RAND() < 0.7 THEN FLOOR(RAND() * 100)
                                 WHEN RAND() < 0.9 THEN 100 + FLOOR(RAND() * 900)
                                 ELSE 1000 + FLOOR(RAND() * 4000) END AS click_count
                 ) t;
            SET i = i + ROW_COUNT();
        END WHILE;
END //
DELIMITER ;
CALL generate_content_data();
INSERT INTO ods_product_content SELECT * FROM tmp_content_data;
DROP TEMPORARY TABLE IF EXISTS tmp_content_data;
DROP PROCEDURE IF EXISTS generate_content_data;

-- 生成商品评价表数据（仅关联5个商品）
INSERT IGNORE INTO ods_product_comment (  -- 增加IGNORE关键字，重复数据会被自动跳过
    product_id, user_type, score_level, comment_count,
    positive_comment, negative_comment, active_comment, ds
)
SELECT
    -- 仅5个商品
    CONCAT('prod_', LPAD(CAST(FLOOR(RAND() * 5) + 1 AS CHAR), 6, '0')) AS product_id,
    user_type,
    score_level,
    comment_count,
    CASE WHEN score_level >= 4 THEN FLOOR(comment_count * (0.7 + RAND() * 0.3))
         WHEN score_level = 3 THEN FLOOR(comment_count * (0.4 + RAND() * 0.2))
         ELSE FLOOR(comment_count * (0.1 + RAND() * 0.2)) END,
    CASE WHEN score_level <= 2 THEN FLOOR(comment_count * (0.6 + RAND() * 0.3))
         WHEN score_level = 3 THEN FLOOR(comment_count * (0.2 + RAND() * 0.2))
         ELSE FLOOR(comment_count * (0.05 + RAND() * 0.15)) END,
    FLOOR(comment_count * (0.3 + RAND() * 0.4)),
    ds
FROM (
         SELECT
             d.ds,
             CASE t.utype WHEN 0 THEN 'all' WHEN 1 THEN 'old' ELSE 'new' END AS user_type,
             1 + FLOOR(RAND() * 5) AS score_level,
             1 + FLOOR(RAND() * 200) AS comment_count
         FROM tmp_dates d
                  CROSS JOIN (SELECT 0 AS utype UNION SELECT 1 UNION SELECT 2) t
                  JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3) t2
         LIMIT 10000
     ) t;


-- 清理临时表
DROP TEMPORARY TABLE IF EXISTS tmp_dates;
DROP TEMPORARY TABLE IF EXISTS tmp_channel_data;
DROP PROCEDURE IF EXISTS generate_dates;

-- 查看生成的5个商品
SELECT * FROM ods_product_info;