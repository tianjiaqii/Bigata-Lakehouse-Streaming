USE test03_02;  -- 切换到test03数据库

-- 1. 终端/无线商品流量指标表（支撑指标1-6）
DROP TABLE IF EXISTS ads_product_terminal_stats;
CREATE TABLE ads_product_terminal_stats (
    time_dim VARCHAR(5) NOT NULL COMMENT '时间维度(day/7d/30d)',
    stat_date DATE NOT NULL COMMENT '统计日期',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    terminal_type VARCHAR(20) NOT NULL COMMENT '终端类型(terminal/wireless)',
    -- 指标1：访客数
    total_visitor_count INT NOT NULL DEFAULT 0 COMMENT '总访客数',
    -- 指标2：详情页访客数
    total_detail_visitor INT NOT NULL DEFAULT 0 COMMENT '详情页访客数',
    -- 指标3：浏览量
    total_pv INT NOT NULL DEFAULT 0 COMMENT '总浏览量',
    -- 指标4：平均停留时长(秒)
    avg_stay_duration DECIMAL(6,2) COMMENT '平均停留时长',
    -- 指标5：详情页跳出率
    avg_bounce_rate DECIMAL(5,4) COMMENT '平均跳出率',
    -- 指标6：加购人数
    total_add_cart_count INT NOT NULL DEFAULT 0 COMMENT '总加购人数',
    PRIMARY KEY (time_dim, stat_date, product_id, terminal_type),
    KEY idx_product (product_id),
    KEY idx_terminal (terminal_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '终端/无线商品流量指标表，直接供FineReport展示';

-- 插入数据（1天/7天/30天聚合，固定日期为20250805）
INSERT INTO ads_product_terminal_stats (
    time_dim, stat_date, product_id, product_name, terminal_type,
    total_visitor_count, total_detail_visitor, total_pv,
    avg_stay_duration, avg_bounce_rate, total_add_cart_count
)
-- 1天数据（固定为20250805）
SELECT
    'day' AS time_dim,
    '2025-08-05' AS stat_date,  -- 固定统计日期
    product_id,
    product_name,
    terminal_type,
    SUM(total_visitor_count) AS total_visitor_count,
    SUM(total_detail_visitor) AS total_detail_visitor,
    SUM(total_pv) AS total_pv,
    AVG(avg_stay_time) AS avg_stay_duration,
    AVG(avg_bounce_rate) AS avg_bounce_rate,
    SUM(total_add_cart) AS total_add_cart_count  -- 与DWS层字段匹配
FROM dws_product_traffic_sum
WHERE ds = '20250805'  -- 筛选固定日期的数据
GROUP BY product_id, product_name, terminal_type

UNION ALL

-- 7天数据（20250730至20250805）
SELECT
    '7d' AS time_dim,
    '2025-08-05' AS stat_date,  -- 以最后一天为统计日期
    product_id,
    product_name,
    terminal_type,
    SUM(total_visitor_count) AS total_visitor_count,
    SUM(total_detail_visitor) AS total_detail_visitor,
    SUM(total_pv) AS total_pv,
    AVG(avg_stay_time) AS avg_stay_duration,
    AVG(avg_bounce_rate) AS avg_bounce_rate,
    SUM(total_add_cart) AS total_add_cart_count
FROM dws_product_traffic_sum
WHERE ds BETWEEN '20250730' AND '20250805'  -- 近7天范围
GROUP BY product_id, product_name, terminal_type

UNION ALL

-- 30天数据（20250706至20250805）
SELECT
    '30d' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    terminal_type,
    SUM(total_visitor_count) AS total_visitor_count,
    SUM(total_detail_visitor) AS total_detail_visitor,
    SUM(total_pv) AS total_pv,
    AVG(avg_stay_time) AS avg_stay_duration,
    AVG(avg_bounce_rate) AS avg_bounce_rate,
    SUM(total_add_cart) AS total_add_cart_count
FROM dws_product_traffic_sum
WHERE ds BETWEEN '20250706' AND '20250805'  -- 近30天范围
GROUP BY product_id, product_name, terminal_type;

-- 查询结果验证
SELECT * FROM ads_product_terminal_stats;









-- 2. 商品颜色交易指标表（支撑指标7-11）
DROP TABLE IF EXISTS ads_product_color_trade_stats;
CREATE TABLE ads_product_color_trade_stats (
    time_dim VARCHAR(5) NOT NULL COMMENT '时间维度(day/7d/30d)',
    stat_date DATE NOT NULL COMMENT '统计日期',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    color VARCHAR(20) NOT NULL COMMENT '商品颜色',
    -- 指标7：支付金额
    total_pay_amount DECIMAL(12,2) NOT NULL DEFAULT 0 COMMENT '总支付金额',
    -- 指标8：支付金额占比(%)
    pay_amount_ratio DECIMAL(5,2) COMMENT '支付金额占比',
    -- 指标9：支付件数
    total_pay_quantity INT NOT NULL DEFAULT 0 COMMENT '总支付件数',
    -- 指标10：支付买家数
    total_pay_buyer INT NOT NULL DEFAULT 0 COMMENT '总支付买家数',
    -- 指标11：支付买家数占比(%)
    pay_buyer_ratio DECIMAL(5,2) COMMENT '支付买家数占比',
    PRIMARY KEY (time_dim, stat_date, product_id, color),
    KEY idx_product (product_id),
    KEY idx_color (color)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品颜色交易指标表，直接供FineReport展示';

-- 插入数据（1天/7天/30天聚合，固定基准日期为20250714）
INSERT INTO ads_product_color_trade_stats (
    time_dim, stat_date, product_id, product_name, color,
    total_pay_amount, pay_amount_ratio,
    total_pay_quantity, total_pay_buyer, pay_buyer_ratio
)
-- 1天数据（固定为20250714）
SELECT
    'day' AS time_dim,
    '2025-07-14' AS stat_date,  -- 固定统计日期为20250714
    t.product_id,
    t.product_name,
    t.color,
    t.total_pay_amount,
    -- 计算金额占比：当前颜色金额/商品总金额（基于当日数据）
    ROUND(t.total_pay_amount / s.total_product_pay * 100, 2) AS pay_amount_ratio,
    t.total_pay_quantity,
    t.total_pay_buyer,
    -- 计算买家占比：当前颜色买家数/商品总买家数（基于当日数据）
    ROUND(t.total_pay_buyer / s.total_product_buyer * 100, 2) AS pay_buyer_ratio
FROM (
         -- 按商品+颜色汇总20250714当日数据
         SELECT
             product_id, product_name, color,
             SUM(total_pay_amount) AS total_pay_amount,
             SUM(total_pay_quantity) AS total_pay_quantity,
             SUM(total_pay_buyer) AS total_pay_buyer
         FROM dws_product_color_trade_sum
         WHERE ds = '20250714'  -- 固定当日ds为20250714
         GROUP BY product_id, product_name, color
     ) t
-- 关联商品总金额/总买家数（同日期，用于计算占比）
         JOIN (
    SELECT
        product_id,
        SUM(total_pay_amount) AS total_product_pay,
        SUM(total_pay_buyer) AS total_product_buyer
    FROM dws_product_color_trade_sum
    WHERE ds = '20250714'  -- 与主表筛选日期一致
    GROUP BY product_id
) s ON t.product_id = s.product_id

UNION ALL

-- 7天数据（20250708至20250714）
SELECT
    '7d' AS time_dim,
    '2025-07-14' AS stat_date,  -- 以最后一天为统计日期
    t.product_id,
    t.product_name,
    t.color,
    t.total_pay_amount,
    ROUND(t.total_pay_amount / s.total_product_pay * 100, 2) AS pay_amount_ratio,
    t.total_pay_quantity,
    t.total_pay_buyer,
    ROUND(t.total_pay_buyer / s.total_product_buyer * 100, 2) AS pay_buyer_ratio
FROM (
         SELECT
             product_id, product_name, color,
             SUM(total_pay_amount) AS total_pay_amount,
             SUM(total_pay_quantity) AS total_pay_quantity,
             SUM(total_pay_buyer) AS total_pay_buyer
         FROM dws_product_color_trade_sum
         WHERE ds BETWEEN '20250708' AND '20250714'  -- 近7天（含20250714）
         GROUP BY product_id, product_name, color
     ) t
         JOIN (
    SELECT
        product_id,
        SUM(total_pay_amount) AS total_product_pay,
        SUM(total_pay_buyer) AS total_product_buyer
    FROM dws_product_color_trade_sum
    WHERE ds BETWEEN '20250708' AND '20250714'  -- 与主表时间范围一致
    GROUP BY product_id
) s ON t.product_id = s.product_id

UNION ALL

-- 30天数据（20250615至20250714）
SELECT
    '30d' AS time_dim,
    '2025-07-14' AS stat_date,
    t.product_id,
    t.product_name,
    t.color,
    t.total_pay_amount,
    ROUND(t.total_pay_amount / s.total_product_pay * 100, 2) AS pay_amount_ratio,
    t.total_pay_quantity,
    t.total_pay_buyer,
    ROUND(t.total_pay_buyer / s.total_product_buyer * 100, 2) AS pay_buyer_ratio
FROM (
         SELECT
             product_id, product_name, color,
             SUM(total_pay_amount) AS total_pay_amount,
             SUM(total_pay_quantity) AS total_pay_quantity,
             SUM(total_pay_buyer) AS total_pay_buyer
         FROM dws_product_color_trade_sum
         WHERE ds BETWEEN '20250615' AND '20250714'  -- 近30天（含20250714）
         GROUP BY product_id, product_name, color
     ) t
         JOIN (
    SELECT
        product_id,
        SUM(total_pay_amount) AS total_product_pay,
        SUM(total_pay_buyer) AS total_product_buyer
    FROM dws_product_color_trade_sum
    WHERE ds BETWEEN '20250615' AND '20250714'  -- 与主表时间范围一致
    GROUP BY product_id
) s ON t.product_id = s.product_id;

select * from ads_product_color_trade_stats;








-- 3. 商品渠道访客占比表（支撑指标12）
DROP TABLE IF EXISTS ads_product_channel_ratio;
CREATE TABLE ads_product_channel_ratio (
    time_dim VARCHAR(5) NOT NULL COMMENT '时间维度(day/7d/30d)',
    stat_date DATE NOT NULL COMMENT '统计日期',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    channel VARCHAR(50) NOT NULL COMMENT '流量渠道',
    visitor_ratio DECIMAL(5,2) COMMENT '访客占比(%)',  -- 指标12
    PRIMARY KEY (time_dim, stat_date, product_id, channel),
    KEY idx_product (product_id),
    KEY idx_channel (channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品渠道访客占比表，直接供FineReport展示';

-- 插入数据（1天/7天/30天聚合，固定基准日期为20250805）
INSERT INTO ads_product_channel_ratio (
    time_dim, stat_date, product_id, product_name, channel, visitor_ratio
)
-- 1天数据（固定为20250805）
SELECT
    'day' AS time_dim,
    '2025-08-05' AS stat_date,  -- 固定统计日期
    product_id,
    product_name,
    channel,
    ROUND(AVG(visitor_ratio) * 100, 2) AS visitor_ratio  -- 平均占比（百分比）
FROM dws_product_channel_sum
WHERE ds = '20250805'  -- 固定当日ds为20250805
GROUP BY product_id, product_name, channel

UNION ALL

-- 7天数据（20250730至20250805）
SELECT
    '7d' AS time_dim,
    '2025-08-05' AS stat_date,  -- 以最后一天为统计日期
    product_id,
    product_name,
    channel,
    ROUND(AVG(visitor_ratio) * 100, 2) AS visitor_ratio
FROM dws_product_channel_sum
WHERE ds BETWEEN '20250730' AND '20250805'  -- 近7天范围
GROUP BY product_id, product_name, channel

UNION ALL

-- 30天数据（20250706至20250805）
SELECT
    '30d' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    channel,
    ROUND(AVG(visitor_ratio) * 100, 2) AS visitor_ratio
FROM dws_product_channel_sum
WHERE ds BETWEEN '20250706' AND '20250805'  -- 近30天范围
GROUP BY product_id, product_name, channel;

select * from ads_product_channel_ratio;








-- 4. 商品内容引流TOP50表（支撑指标13）
DROP TABLE IF EXISTS ads_product_content_top50;
CREATE TABLE ads_product_content_top50 (
    time_dim VARCHAR(5) NOT NULL COMMENT '时间维度(day/7d/30d)',
    stat_date DATE NOT NULL COMMENT '统计日期',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    content_type VARCHAR(20) NOT NULL COMMENT '内容类型(live/video/image)',
    top_rank INT NOT NULL COMMENT '引流排名(1-50)',
    total_click_count INT NOT NULL DEFAULT 0 COMMENT '总点击次数',
    total_collect_count INT NOT NULL DEFAULT 0 COMMENT '总引导收藏次数',
    total_add_cart_count INT NOT NULL DEFAULT 0 COMMENT '总引导加购次数',
    PRIMARY KEY (time_dim, stat_date, product_id, content_type),
    KEY idx_product (product_id),
    KEY idx_content_type (content_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品内容引流TOP50表，直接供FineReport展示';

-- 插入数据（1天/7天/30天，按点击次数取TOP50，兼容低版本MySQL）
-- 1天数据（固定为20250805，用变量模拟排名）
INSERT INTO ads_product_content_top50 (
    time_dim, stat_date, product_id, product_name, content_type,
    top_rank, total_click_count, total_collect_count, total_add_cart_count
)
SELECT
    'day' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    content_type,
    top_rank,
    total_click_count,
    total_collect_count,
    total_add_cart_count
FROM (
         -- 用变量计算排名（按内容类型分区，点击量降序）
         SELECT
             t.*,
             @rank := IF(@curr_type = content_type, @rank + 1, 1) AS top_rank,
             @curr_type := content_type  -- 记录当前内容类型，用于分区
         FROM (
                  -- 先汇总当日数据
                  SELECT
                      product_id,
                      product_name,
                      content_type,
                      SUM(total_click_count) AS total_click_count,
                      SUM(total_collect_count) AS total_collect_count,
                      SUM(total_add_cart_content) AS total_add_cart_count
                  FROM dws_product_content_sum
                  WHERE ds = '20250805'  -- 固定当日为20250805
                  GROUP BY product_id, product_name, content_type
                  ORDER BY content_type, total_click_count DESC  -- 按内容类型和点击量排序
              ) t,
              -- 初始化变量（@curr_type记录当前内容类型，@rank记录排名）
              (SELECT @curr_type := '', @rank := 0) AS init
     ) ranked
WHERE top_rank <= 50;  -- 取TOP50

-- 7天数据（20250730至20250805）
INSERT INTO ads_product_content_top50 (
    time_dim, stat_date, product_id, product_name, content_type,
    top_rank, total_click_count, total_collect_count, total_add_cart_count
)
SELECT
    '7d' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    content_type,
    top_rank,
    total_click_count,
    total_collect_count,
    total_add_cart_count
FROM (
         SELECT
             t.*,
             @rank7 := IF(@curr_type7 = content_type, @rank7 + 1, 1) AS top_rank,
             @curr_type7 := content_type
         FROM (
                  SELECT
                      product_id,
                      product_name,
                      content_type,
                      SUM(total_click_count) AS total_click_count,
                      SUM(total_collect_count) AS total_collect_count,
                      SUM(total_add_cart_content) AS total_add_cart_count
                  FROM dws_product_content_sum
                  WHERE ds BETWEEN '20250730' AND '20250805'  -- 近7天
                  GROUP BY product_id, product_name, content_type
                  ORDER BY content_type, total_click_count DESC
              ) t,
              (SELECT @curr_type7 := '', @rank7 := 0) AS init
     ) ranked
WHERE top_rank <= 50;

-- 30天数据（20250706至20250805）
INSERT INTO ads_product_content_top50 (
    time_dim, stat_date, product_id, product_name, content_type,
    top_rank, total_click_count, total_collect_count, total_add_cart_count
)
SELECT
    '30d' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    content_type,
    top_rank,
    total_click_count,
    total_collect_count,
    total_add_cart_count
FROM (
         SELECT
             t.*,
             @rank30 := IF(@curr_type30 = content_type, @rank30 + 1, 1) AS top_rank,
             @curr_type30 := content_type
         FROM (
                  SELECT
                      product_id,
                      product_name,
                      content_type,
                      SUM(total_click_count) AS total_click_count,
                      SUM(total_collect_count) AS total_collect_count,
                      SUM(total_add_cart_content) AS total_add_cart_count
                  FROM dws_product_content_sum
                  WHERE ds BETWEEN '20250706' AND '20250805'  -- 近30天
                  GROUP BY product_id, product_name, content_type
                  ORDER BY content_type, total_click_count DESC
              ) t,
              (SELECT @curr_type30 := '', @rank30 := 0) AS init
     ) ranked
WHERE top_rank <= 50;

select * from ads_product_content_top50;





-- 5. 商品评价指标表（支撑指标14-15）
DROP TABLE IF EXISTS ads_product_comment_stats;
CREATE TABLE ads_product_comment_stats (
    time_dim VARCHAR(5) NOT NULL COMMENT '时间维度(day/7d/30d)',
    stat_date DATE NOT NULL COMMENT '统计日期',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID',
    product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
    user_type VARCHAR(10) NOT NULL COMMENT '用户类型(all/old/new)',
    score_level INT NOT NULL COMMENT '评分等级(1-5)',
    -- 指标14：不同人群+分数层的评价数
    total_comment_count INT NOT NULL DEFAULT 0 COMMENT '总评价数',
    -- 指标15：正面/负面/主动评价数
    total_positive_comment INT NOT NULL DEFAULT 0 COMMENT '总正面评价数',
    total_negative_comment INT NOT NULL DEFAULT 0 COMMENT '总负面评价数',
    total_active_comment INT NOT NULL DEFAULT 0 COMMENT '总主动评价数',
    PRIMARY KEY (time_dim, stat_date, product_id, user_type, score_level),
    KEY idx_product (product_id),
    KEY idx_user_score (user_type, score_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品评价指标表，直接供FineReport展示';

-- 插入数据（1天/7天/30天聚合，固定基准日期为20250805）
INSERT INTO ads_product_comment_stats (
    time_dim, stat_date, product_id, product_name, user_type, score_level,
    total_comment_count, total_positive_comment, total_negative_comment, total_active_comment
)
-- 1天数据（固定为20250805）
SELECT
    'day' AS time_dim,
    '2025-08-05' AS stat_date,  -- 固定统计日期
    product_id,
    product_name,
    user_type,
    score_level,
    SUM(total_comment_count) AS total_comment_count,
    SUM(total_positive_comment) AS total_positive_comment,
    SUM(total_negative_comment) AS total_negative_comment,
    SUM(total_active_comment) AS total_active_comment
FROM dws_product_comment_sum
WHERE ds = '20250805'  -- 固定当日ds为20250805
GROUP BY product_id, product_name, user_type, score_level

UNION ALL

-- 7天数据（20250730至20250805）
SELECT
    '7d' AS time_dim,
    '2025-08-05' AS stat_date,  -- 以最后一天为统计日期
    product_id,
    product_name,
    user_type,
    score_level,
    SUM(total_comment_count) AS total_comment_count,
    SUM(total_positive_comment) AS total_positive_comment,
    SUM(total_negative_comment) AS total_negative_comment,
    SUM(total_active_comment) AS total_active_comment
FROM dws_product_comment_sum
WHERE ds BETWEEN '20250730' AND '20250805'  -- 近7天范围
GROUP BY product_id, product_name, user_type, score_level

UNION ALL

-- 30天数据（20250706至20250805）
SELECT
    '30d' AS time_dim,
    '2025-08-05' AS stat_date,
    product_id,
    product_name,
    user_type,
    score_level,
    SUM(total_comment_count) AS total_comment_count,
    SUM(total_positive_comment) AS total_positive_comment,
    SUM(total_negative_comment) AS total_negative_comment,
    SUM(total_active_comment) AS total_active_comment
FROM dws_product_comment_sum
WHERE ds BETWEEN '20250706' AND '20250805'  -- 近30天范围
GROUP BY product_id, product_name, user_type, score_level;

select * from ads_product_comment_stats;

