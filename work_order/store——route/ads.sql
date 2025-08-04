USE test2;

-- 1. 页面访问综合分析报表
DROP TABLE IF EXISTS ads_page_analysis_report;
CREATE TABLE ads_page_analysis_report (
    `report_id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '报表ID',
    `time_dimension` ENUM('day', '7day', '30day', 'month') NOT NULL COMMENT '时间维度',
    `start_date` DATE NOT NULL COMMENT '开始日期',
    `end_date` DATE NOT NULL COMMENT '结束日期',
    `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID',
    `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称',
    `page_type` ENUM('store_page', 'item_detail', 'other_page')  COMMENT '页面类型',
    `device_type` ENUM('wireless', 'pc')   COMMENT '设备类型(NULL表示全部)',
    `pv` INT NOT NULL DEFAULT 0 COMMENT '访问量',
    `uv` INT NOT NULL DEFAULT 0 COMMENT '访客数',
    `avg_stay_duration` DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '平均停留时长(秒)',
    `entry_count` INT NOT NULL DEFAULT 0 COMMENT '进店次数',
    `entry_rate` DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '进店率(%)',
    `order_buyer_num` INT NOT NULL DEFAULT 0 COMMENT '下单买家数',
    `conversion_rate` DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '转化率(%)',
    `bounce_rate` DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '跳出率(%)',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `idx_unique` (`time_dimension`, `start_date`, `end_date`, `page_id`, `device_type`),
    KEY `idx_page_type` (`page_type`),
    KEY `idx_time` (`start_date`, `end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '页面访问综合分析报表-工单:大数据-电商数仓-09-流量主题店内路径看板';
select * from ads_page_analysis_report;

-- 2. PC端来源页面TOP20分析表
DROP TABLE IF EXISTS ads_pc_source_analysis;
CREATE TABLE ads_pc_source_analysis (
    `report_id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '报表ID',
    `time_dimension` ENUM('day', '7day', '30day', 'month') NOT NULL COMMENT '时间维度',
    `start_date` DATE NOT NULL COMMENT '开始日期',
    `end_date` DATE NOT NULL COMMENT '结束日期',
    `target_page_id` VARCHAR(50) NOT NULL COMMENT '目标页面ID',
    `target_page_name` VARCHAR(100) NOT NULL COMMENT '目标页面名称',
    `source_page_id` VARCHAR(50) NOT NULL COMMENT '来源页面ID',
    `source_page_name` VARCHAR(100) NOT NULL COMMENT '来源页面名称',
    `source_count` INT NOT NULL DEFAULT 0 COMMENT '来源次数',
    `source_ratio` varchar(255) NOT NULL DEFAULT 0 COMMENT '来源占比(%)',
    `rank_num` INT NOT NULL COMMENT '排名',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `idx_unique` (`time_dimension`, `start_date`, `end_date`, `target_page_id`, `source_page_id`),
    KEY `idx_target_page` (`target_page_id`),
    KEY `idx_time` (`start_date`, `end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'PC端来源页面TOP20分析-工单:大数据-电商数仓-09-流量主题店内路径看板';
select * from ads_pc_source_analysis;

-- 3. 店内路径流转分析表
DROP TABLE IF EXISTS ads_page_path_analysis;
CREATE TABLE ads_page_path_analysis (
    `report_id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '报表ID',
    `time_dimension` ENUM('day', '7day', '30day', 'month') NOT NULL COMMENT '时间维度',
    `start_date` DATE NOT NULL COMMENT '开始日期',
    `end_date` DATE NOT NULL COMMENT '结束日期',
    `from_page_id` VARCHAR(50) NOT NULL COMMENT '来源页面ID',
    `from_page_name` VARCHAR(100) NOT NULL COMMENT '来源页面名称',
    `to_page_id` VARCHAR(50) NOT NULL COMMENT '去向页面ID',
    `to_page_name` VARCHAR(100) NOT NULL COMMENT '去向页面名称',
    `path_count` INT NOT NULL DEFAULT 0 COMMENT '流转次数',
    `path_ratio` DECIMAL(5,2) NOT NULL DEFAULT 0 COMMENT '流转占比(%)',
    `avg_stay_duration` DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '平均停留时长(秒)',
    `device_type` ENUM('wireless', 'pc') COMMENT '设备类型',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `idx_unique` (`time_dimension`, `start_date`, `end_date`, `from_page_id`, `to_page_id`, `device_type`),
    KEY `idx_from_page` (`from_page_id`),
    KEY `idx_to_page` (`to_page_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '店内路径流转分析-工单:大数据-电商数仓-09-流量主题店内路径看板';
select * from ads_page_path_analysis;







-- 1. 页面访问综合分析报表数据生成
INSERT INTO ads_page_analysis_report (
    time_dimension, start_date, end_date, page_id, page_name, page_type,
    device_type, pv, uv, avg_stay_duration, entry_count, entry_rate,
    order_buyer_num, conversion_rate, bounce_rate
)
-- 日维度数据
SELECT
    'day' AS time_dimension,
    CURRENT_DATE AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date = CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date = CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date = CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 7天维度数据
SELECT
    '7day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 30天维度数据
SELECT
    '30day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 月维度数据
SELECT
    'month' AS time_dimension,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AS start_date,
    LAST_DAY(CURRENT_DATE) AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
GROUP BY page_id, page_name, page_type

-- 设备维度数据（日维度示例，其他时间维度类似）
UNION ALL
SELECT
    'day' AS time_dimension,
    CURRENT_DATE AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date = CURRENT_DATE
                       AND page_id = d.page_id
                       AND device_type = d.device_type
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date = CURRENT_DATE
                           AND device_type = d.device_type
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date = CURRENT_DATE
GROUP BY page_id, page_name, page_type, device_type

ON DUPLICATE KEY UPDATE
                     pv = VALUES(pv),
                     uv = VALUES(uv),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     entry_count = VALUES(entry_count),
                     entry_rate = VALUES(entry_rate),
                     order_buyer_num = VALUES(order_buyer_num),
                     conversion_rate = VALUES(conversion_rate),
                     bounce_rate = VALUES(bounce_rate),
                     update_time = CURRENT_TIMESTAMP;


select * from ads_page_analysis_report;










-- 3. 店内路径流转分析（修正版）






#################################################################################
INSERT INTO ads_page_path_analysis (
    time_dimension, start_date, end_date,
    from_page_id, from_page_name,
    to_page_id, to_page_name,
    path_count, path_ratio, avg_stay_duration, device_type
)
-- 日维度路径分析（全部设备）
SELECT
    'day' AS time_dimension,
    CURRENT_DATE AS start_date,
    CURRENT_DATE AS end_date,
    d1.page_id AS from_page_id,
    d1.page_name AS from_page_name,
    d2.page_id AS to_page_id,
    d2.page_name AS to_page_name,
    COUNT(*) AS path_count,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM (
                 SELECT a.user_id, a.page_id, MIN(b.visit_time) AS next_visit_time
                 FROM dwd_page_visit_detail a
                          LEFT JOIN dwd_page_visit_detail b ON
                             a.user_id = b.user_id AND
                             b.visit_time > a.visit_time
                 WHERE a.visit_date = CURRENT_DATE
                 GROUP BY a.user_id, a.page_id
             ) t
        WHERE t.next_visit_time IS NOT NULL
    ), 2) AS path_ratio,
    AVG(TIMESTAMPDIFF(SECOND, d1.visit_time, d2.visit_time)) AS avg_stay_duration,
    NULL AS device_type
FROM
    dwd_page_visit_detail d1
        JOIN dwd_page_visit_detail d2 ON
                d1.user_id = d2.user_id AND
                d2.visit_time = (
                    SELECT MIN(visit_time)
                    FROM dwd_page_visit_detail
                    WHERE user_id = d1.user_id AND visit_time > d1.visit_time
                )
WHERE
        d1.visit_date = CURRENT_DATE AND
        d2.visit_date = CURRENT_DATE
GROUP BY
    d1.page_id, d1.page_name, d2.page_id, d2.page_name

UNION ALL

-- 日维度路径分析（按设备）
SELECT
    'day' AS time_dimension,
    CURRENT_DATE AS start_date,
    CURRENT_DATE AS end_date,
    d1.page_id AS from_page_id,
    d1.page_name AS from_page_name,
    d2.page_id AS to_page_id,
    d2.page_name AS to_page_name,
    COUNT(*) AS path_count,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM (
                 SELECT a.user_id, a.page_id, MIN(b.visit_time) AS next_visit_time
                 FROM dwd_page_visit_detail a
                          LEFT JOIN dwd_page_visit_detail b ON
                             a.user_id = b.user_id AND
                             a.device_type = b.device_type AND  -- 关联设备类型
                             b.visit_time > a.visit_time
                 WHERE a.visit_date = CURRENT_DATE
                 GROUP BY a.user_id, a.page_id
             ) t
        WHERE t.next_visit_time IS NOT NULL
    ), 2) AS path_ratio,
    AVG(TIMESTAMPDIFF(SECOND, d1.visit_time, d2.visit_time)) AS avg_stay_duration,
    d1.device_type
FROM
    dwd_page_visit_detail d1
        JOIN dwd_page_visit_detail d2 ON
                d1.user_id = d2.user_id AND
                d1.device_type = d2.device_type AND  -- 关联设备类型
                d2.visit_time = (
                    SELECT MIN(visit_time)
                    FROM dwd_page_visit_detail
                    WHERE user_id = d1.user_id
                      AND device_type = d1.device_type  -- 限定设备类型
                      AND visit_time > d1.visit_time
                )
WHERE
        d1.visit_date = CURRENT_DATE AND
        d2.visit_date = CURRENT_DATE
GROUP BY
    d1.page_id, d1.page_name, d2.page_id, d2.page_name, d1.device_type

UNION ALL

-- 7天维度路径分析（全部设备）
SELECT
    '7day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    d1.page_id AS from_page_id,
    d1.page_name AS from_page_name,
    d2.page_id AS to_page_id,
    d2.page_name AS to_page_name,
    COUNT(*) AS path_count,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM (
                 SELECT a.user_id, a.page_id, MIN(b.visit_time) AS next_visit_time
                 FROM dwd_page_visit_detail a
                          LEFT JOIN dwd_page_visit_detail b ON
                             a.user_id = b.user_id AND
                             b.visit_time > a.visit_time
                 WHERE a.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                 GROUP BY a.user_id, a.page_id
             ) t
        WHERE t.next_visit_time IS NOT NULL
    ), 2) AS path_ratio,
    AVG(TIMESTAMPDIFF(SECOND, d1.visit_time, d2.visit_time)) AS avg_stay_duration,
    NULL AS device_type
FROM
    dwd_page_visit_detail d1
        JOIN dwd_page_visit_detail d2 ON
                d1.user_id = d2.user_id AND
                d2.visit_time = (
                    SELECT MIN(visit_time)
                    FROM dwd_page_visit_detail
                    WHERE user_id = d1.user_id AND visit_time > d1.visit_time
                )
WHERE
    d1.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE AND
    d2.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
GROUP BY
    d1.page_id, d1.page_name, d2.page_id, d2.page_name

UNION ALL

-- 30天维度路径分析（全部设备）
SELECT
    '30day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    d1.page_id AS from_page_id,
    d1.page_name AS from_page_name,
    d2.page_id AS to_page_id,
    d2.page_name AS to_page_name,
    COUNT(*) AS path_count,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM (
                 SELECT a.user_id, a.page_id, MIN(b.visit_time) AS next_visit_time
                 FROM dwd_page_visit_detail a
                          LEFT JOIN dwd_page_visit_detail b ON
                             a.user_id = b.user_id AND
                             b.visit_time > a.visit_time
                 WHERE a.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                 GROUP BY a.user_id, a.page_id
             ) t
        WHERE t.next_visit_time IS NOT NULL
    ), 2) AS path_ratio,
    AVG(TIMESTAMPDIFF(SECOND, d1.visit_time, d2.visit_time)) AS avg_stay_duration,
    NULL AS device_type
FROM
    dwd_page_visit_detail d1
        JOIN dwd_page_visit_detail d2 ON
                d1.user_id = d2.user_id AND
                d2.visit_time = (
                    SELECT MIN(visit_time)
                    FROM dwd_page_visit_detail
                    WHERE user_id = d1.user_id AND visit_time > d1.visit_time
                )
WHERE
    d1.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE AND
    d2.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
GROUP BY
    d1.page_id, d1.page_name, d2.page_id, d2.page_name

UNION ALL

-- 月维度路径分析（全部设备）
SELECT
    'month' AS time_dimension,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AS start_date,
    LAST_DAY(CURRENT_DATE) AS end_date,
    d1.page_id AS from_page_id,
    d1.page_name AS from_page_name,
    d2.page_id AS to_page_id,
    d2.page_name AS to_page_name,
    COUNT(*) AS path_count,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM (
                 SELECT a.user_id, a.page_id, MIN(b.visit_time) AS next_visit_time
                 FROM dwd_page_visit_detail a
                          LEFT JOIN dwd_page_visit_detail b ON
                             a.user_id = b.user_id AND
                             b.visit_time > a.visit_time
                 WHERE a.visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                 GROUP BY a.user_id, a.page_id
             ) t
        WHERE t.next_visit_time IS NOT NULL
    ), 2) AS path_ratio,
    AVG(TIMESTAMPDIFF(SECOND, d1.visit_time, d2.visit_time)) AS avg_stay_duration,
    NULL AS device_type
FROM
    dwd_page_visit_detail d1
        JOIN dwd_page_visit_detail d2 ON
                d1.user_id = d2.user_id AND
                d2.visit_time = (
                    SELECT MIN(visit_time)
                    FROM dwd_page_visit_detail
                    WHERE user_id = d1.user_id AND visit_time > d1.visit_time
                )
WHERE
    d1.visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE) AND
    d2.visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
GROUP BY
    d1.page_id, d1.page_name, d2.page_id, d2.page_name

ON DUPLICATE KEY UPDATE
                     path_count = VALUES(path_count),
                     path_ratio = VALUES(path_ratio),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     update_time = CURRENT_TIMESTAMP;

select * from ads_page_path_analysis;






-- 2. PC端来源页面TOP20分析（修正版）
INSERT INTO ads_pc_source_analysis (
    time_dimension, start_date, end_date,
    target_page_id, target_page_name,
    source_page_id, source_page_name,
    source_count, source_ratio, rank_num
)
-- 日维度数据
SELECT
    rs.time_dimension,
    rs.start_date,
    rs.end_date,
    rs.target_page_id,
    rs.target_page_name,
    rs.source_page_id,
    rs.source_page_name,
    rs.source_count,
    rs.source_ratio,
    rs.rank_num
FROM (
         SELECT
             tmp.*,
             @rank := IF(@current_page = tmp.target_page_id, @rank + 1, 1) AS rank_num,
             @current_page := tmp.target_page_id
         FROM (
                  -- 基础数据查询
                  SELECT
                      'day' AS time_dimension,
                      CURRENT_DATE AS start_date,
                      CURRENT_DATE AS end_date,
                      d.page_id AS target_page_id,
                      d.page_name AS target_page_name,
                      d.refer_page_id AS source_page_id,
                      r.page_name AS source_page_name,
                      COUNT(*) AS source_count,
                      ROUND(COUNT(*) * 100.0 / (
                          SELECT COUNT(*)
                          FROM dwd_page_visit_detail
                          WHERE visit_date = CURRENT_DATE
                            AND page_id = d.page_id
                            AND refer_page_id IS NOT NULL
                            AND refer_page_id != 'N/A'
                      ), 2) AS source_ratio
                  FROM
                      dwd_page_visit_detail d
                          JOIN dwd_page_visit_detail r ON d.refer_page_id = r.page_id
                  WHERE
                          d.visit_date = CURRENT_DATE
                    AND d.refer_page_id IS NOT NULL
                    AND d.refer_page_id != 'N/A'
                  GROUP BY
                      d.page_id, d.page_name, d.refer_page_id, r.page_name
                  ORDER BY
                      d.page_id, source_count DESC  -- 关键：先排序再排名
              ) tmp,
              (SELECT @rank := 0, @current_page := '') vars
     ) rs
WHERE rs.rank_num <= 20

UNION ALL

-- 7天维度数据
SELECT
    rs.time_dimension,
    rs.start_date,
    rs.end_date,
    rs.target_page_id,
    rs.target_page_name,
    rs.source_page_id,
    rs.source_page_name,
    rs.source_count,
    rs.source_ratio,
    rs.rank_num
FROM (
         SELECT
             tmp.*,
             @rank7 := IF(@current_page7 = tmp.target_page_id, @rank7 + 1, 1) AS rank_num,
             @current_page7 := tmp.target_page_id
         FROM (
                  SELECT
                      '7day' AS time_dimension,
                      DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AS start_date,
                      CURRENT_DATE AS end_date,
                      d.page_id AS target_page_id,
                      d.page_name AS target_page_name,
                      d.refer_page_id AS source_page_id,
                      r.page_name AS source_page_name,
                      COUNT(*) AS source_count,
                      ROUND(COUNT(*) * 100.0 / (
                          SELECT COUNT(*)
                          FROM dwd_page_visit_detail
                          WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                            AND page_id = d.page_id
                            AND refer_page_id IS NOT NULL
                            AND refer_page_id != 'N/A'
                      ), 2) AS source_ratio
                  FROM
                      dwd_page_visit_detail d
                          JOIN dwd_page_visit_detail r ON d.refer_page_id = r.page_id
                  WHERE
                      d.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                    AND d.refer_page_id IS NOT NULL
                    AND d.refer_page_id != 'N/A'
                  GROUP BY
                      d.page_id, d.page_name, d.refer_page_id, r.page_name
                  ORDER BY
                      d.page_id, source_count DESC
              ) tmp,
              (SELECT @rank7 := 0, @current_page7 := '') vars
     ) rs
WHERE rs.rank_num <= 20

UNION ALL

-- 30天维度数据
SELECT
    rs.time_dimension,
    rs.start_date,
    rs.end_date,
    rs.target_page_id,
    rs.target_page_name,
    rs.source_page_id,
    rs.source_page_name,
    rs.source_count,
    rs.source_ratio,
    rs.rank_num
FROM (
         SELECT
             tmp.*,
             @rank30 := IF(@current_page30 = tmp.target_page_id, @rank30 + 1, 1) AS rank_num,
             @current_page30 := tmp.target_page_id
         FROM (
                  SELECT
                      '30day' AS time_dimension,
                      DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AS start_date,
                      CURRENT_DATE AS end_date,
                      d.page_id AS target_page_id,
                      d.page_name AS target_page_name,
                      d.refer_page_id AS source_page_id,
                      r.page_name AS source_page_name,
                      COUNT(*) AS source_count,
                      ROUND(COUNT(*) * 100.0 / (
                          SELECT COUNT(*)
                          FROM dwd_page_visit_detail
                          WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                            AND page_id = d.page_id
                            AND refer_page_id IS NOT NULL
                            AND refer_page_id != 'N/A'
                      ), 2) AS source_ratio
                  FROM
                      dwd_page_visit_detail d
                          JOIN dwd_page_visit_detail r ON d.refer_page_id = r.page_id
                  WHERE
                      d.visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                    AND d.refer_page_id IS NOT NULL
                    AND d.refer_page_id != 'N/A'
                  GROUP BY
                      d.page_id, d.page_name, d.refer_page_id, r.page_name
                  ORDER BY
                      d.page_id, source_count DESC
              ) tmp,
              (SELECT @rank30 := 0, @current_page30 := '') vars
     ) rs
WHERE rs.rank_num <= 20

UNION ALL

-- 月维度数据
SELECT
    rs.time_dimension,
    rs.start_date,
    rs.end_date,
    rs.target_page_id,
    rs.target_page_name,
    rs.source_page_id,
    rs.source_page_name,
    rs.source_count,
    rs.source_ratio,
    rs.rank_num
FROM (
         SELECT
             tmp.*,
             @rankm := IF(@current_pagem = tmp.target_page_id, @rankm + 1, 1) AS rank_num,
             @current_pagem := tmp.target_page_id
         FROM (
                  SELECT
                      'month' AS time_dimension,
                      DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AS start_date,
                      LAST_DAY(CURRENT_DATE) AS end_date,
                      d.page_id AS target_page_id,
                      d.page_name AS target_page_name,
                      d.refer_page_id AS source_page_id,
                      r.page_name AS source_page_name,
                      COUNT(*) AS source_count,
                      ROUND(COUNT(*) * 100.0 / (
                          SELECT COUNT(*)
                          FROM dwd_page_visit_detail
                          WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                            AND page_id = d.page_id
                            AND refer_page_id IS NOT NULL
                            AND refer_page_id != 'N/A'
                      ), 2) AS source_ratio
                  FROM
                      dwd_page_visit_detail d
                          JOIN dwd_page_visit_detail r ON d.refer_page_id = r.page_id
                  WHERE
                      d.visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                    AND d.refer_page_id IS NOT NULL
                    AND d.refer_page_id != 'N/A'
                  GROUP BY
                      d.page_id, d.page_name, d.refer_page_id, r.page_name
                  ORDER BY
                      d.page_id, source_count DESC
              ) tmp,
              (SELECT @rankm := 0, @current_pagem := '') vars
     ) rs
WHERE rs.rank_num <= 20

ON DUPLICATE KEY UPDATE
                     source_count = VALUES(source_count),
                     source_ratio = VALUES(source_ratio),
                     rank_num = VALUES(rank_num),
                     update_time = CURRENT_TIMESTAMP;

-- 查看结果
SELECT * FROM ads_pc_source_analysis;
















































































































-- 1. 页面访问综合分析报表数据生成
INSERT INTO ads_page_analysis_report (
    time_dimension, start_date, end_date, page_id, page_name, page_type,
    device_type, pv, uv, avg_stay_duration, entry_count, entry_rate,
    order_buyer_num, conversion_rate, bounce_rate
)
-- 日维度数据（不含设备维度，即代表全部设备）
SELECT
    'day' AS time_dimension,
    CURRENT_DATE AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date = CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date = CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date = CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 7天维度数据（不含设备维度，即代表全部设备）
SELECT
    '7day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 30天维度数据（不含设备维度，即代表全部设备）
SELECT
    '30day' AS time_dimension,
    DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AS start_date,
    CURRENT_DATE AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE
GROUP BY page_id, page_name, page_type

UNION ALL

-- 月维度数据（不含设备维度，即代表全部设备）
SELECT
    'month' AS time_dimension,
    DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AS start_date,
    LAST_DAY(CURRENT_DATE) AS end_date,
    page_id,
    page_name,
    page_type,
    NULL AS device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                       AND page_id = d.page_id
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT d.user_id), 0), 2
        ) AS bounce_rate
FROM dwd_page_visit_detail d
WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
GROUP BY page_id, page_name, page_type

UNION ALL

-- 设备维度数据（分设备统计，覆盖所有时间维度 ）
SELECT
    time_dimension,
    start_date,
    end_date,
    page_id,
    page_name,
    page_type,
    device_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration,
    SUM(is_entry) AS entry_count,
    ROUND(SUM(is_entry) * 100.0 / NULLIF(COUNT(*), 0), 2) AS entry_rate,
    SUM(order_buyer_num) AS order_buyer_num,
    ROUND(SUM(order_buyer_num) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS conversion_rate,
    ROUND(
                    (SELECT COUNT(DISTINCT user_id)
                     FROM dwd_page_visit_detail
                     WHERE visit_date BETWEEN sub.start_date AND sub.end_date
                       AND page_id = sub.page_id
                       AND device_type = sub.device_type
                       AND user_id IN (
                         SELECT user_id
                         FROM dwd_page_visit_detail
                         WHERE visit_date BETWEEN sub.start_date AND sub.end_date
                           AND device_type = sub.device_type
                         GROUP BY user_id
                         HAVING COUNT(DISTINCT page_id) = 1
                     )) * 100.0 / NULLIF(COUNT(DISTINCT sub.user_id), 0), 2
        ) AS bounce_rate
FROM (
         -- 构建包含时间维度、设备类型等基础数据的子查询
         SELECT
             'day' AS time_dimension,
             CURRENT_DATE AS start_date,
             CURRENT_DATE AS end_date,
             page_id,
             page_name,
             page_type,
             user_id,
             stay_duration,
             is_entry,
             order_buyer_num,
             device_type
         FROM dwd_page_visit_detail
         WHERE visit_date = CURRENT_DATE

         UNION ALL
         SELECT
             '7day' AS time_dimension,
             DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AS start_date,
             CURRENT_DATE AS end_date,
             page_id,
             page_name,
             page_type,
             user_id,
             stay_duration,
             is_entry,
             order_buyer_num,
             device_type
         FROM dwd_page_visit_detail
         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 6 DAY) AND CURRENT_DATE

         UNION ALL
         SELECT
             '30day' AS time_dimension,
             DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AS start_date,
             CURRENT_DATE AS end_date,
             page_id,
             page_name,
             page_type,
             user_id,
             stay_duration,
             is_entry,
             order_buyer_num,
             device_type
         FROM dwd_page_visit_detail
         WHERE visit_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 29 DAY) AND CURRENT_DATE

         UNION ALL
         SELECT
             'month' AS time_dimension,
             DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AS start_date,
             LAST_DAY(CURRENT_DATE) AS end_date,
             page_id,
             page_name,
             page_type,
             user_id,
             stay_duration,
             is_entry,
             order_buyer_num,
             device_type
         FROM dwd_page_visit_detail
         WHERE visit_date BETWEEN DATE_FORMAT(CURRENT_DATE, '%Y-%m-01') AND LAST_DAY(CURRENT_DATE)
     ) AS sub
GROUP BY time_dimension, start_date, end_date, page_id, page_name, page_type, device_type

ON DUPLICATE KEY UPDATE
                     pv = VALUES(pv),
                     uv = VALUES(uv),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     entry_count = VALUES(entry_count),
                     entry_rate = VALUES(entry_rate),
                     order_buyer_num = VALUES(order_buyer_num),
                     conversion_rate = VALUES(conversion_rate),
                     bounce_rate = VALUES(bounce_rate),
                     update_time = CURRENT_TIMESTAMP;

-- 查询验证
SELECT * FROM ads_page_analysis_report;












