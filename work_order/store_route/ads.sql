USE test2;

-- 1. 页面访问多维度指标表（支持 day/7d/30d）
DROP TABLE IF EXISTS ads_page_multi_dim_stats;
CREATE TABLE ads_page_multi_dim_stats (
                                          `time_dim` VARCHAR(10) NOT NULL COMMENT '时间维度（day/7d/30d）',
                                          `stat_date` DATE NOT NULL COMMENT '统计基准日期',
                                          `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID',
                                          `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称',
                                          `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型',
    -- 核心指标
                                          `total_uv` INT NOT NULL DEFAULT 0 COMMENT '总访客数（排行依据）',
                                          `total_pv` INT NOT NULL DEFAULT 0 COMMENT '总浏览量（排序依据）',
                                          `entry_uv` INT NOT NULL DEFAULT 0 COMMENT '进店访客数',
                                          `avg_stay_duration` DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    -- 分设备指标
                                          `wireless_uv` INT DEFAULT 0 COMMENT '无线端访客数',
                                          `pc_uv` INT DEFAULT 0 COMMENT 'PC端访客数',
    -- 转化指标
                                          `entry_rate` DECIMAL(5,2) COMMENT '进店率（%）',
                                          `order_conversion_rate` DECIMAL(5,2) COMMENT '下单转化率（%）',
    -- 排序辅助字段
                                          `uv_rank` INT COMMENT '同页面类型内UV排名',
                                          `pv_rank` INT COMMENT '全局PV排名',
                                          `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据生成时间',
                                          PRIMARY KEY (`time_dim`, `stat_date`, `page_id`),
                                          KEY `idx_type_rank` (`time_dim`, `page_type`, `uv_rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '多维度页面指标表';

-- 插入日维度数据
INSERT INTO ads_page_multi_dim_stats (
    time_dim, stat_date, page_id, page_name, page_type,
    total_uv, total_pv, entry_uv, avg_stay_duration,
    wireless_uv, pc_uv, entry_rate, order_conversion_rate,
    uv_rank, pv_rank
)
SELECT
    'day' AS time_dim,
    CURDATE() AS stat_date,
    page_id,
    page_name,
    page_type,
    total_uv,
    total_pv,
    entry_uv,
    avg_stay_duration,
    wireless_uv,
    pc_uv,
    entry_rate,
    order_conversion_rate,
    -- 计算UV排名
    ROW_NUMBER() OVER (PARTITION BY page_type ORDER BY total_uv DESC) AS uv_rank,
    -- 计算PV排名
    ROW_NUMBER() OVER (ORDER BY total_pv DESC) AS pv_rank
FROM (
         SELECT
             dws.page_id,
             dws.page_name,
             dws.page_type,
             SUM(dws.uv) AS total_uv,
             SUM(dws.pv) AS total_pv,
             SUM(dws.entry_count) AS entry_uv,
             AVG(dws.avg_stay_duration) AS avg_stay_duration,
             SUM(CASE WHEN dws.device_type = 'wireless' THEN dws.uv ELSE 0 END) AS wireless_uv,
             SUM(CASE WHEN dws.device_type = 'pc' THEN dws.uv ELSE 0 END) AS pc_uv,
             (SUM(dws.entry_count)/SUM(dws.pv)*100) AS entry_rate,
             (SUM(dws.total_order_buyer)/SUM(dws.uv)*100) AS order_conversion_rate
         FROM dws_page_visit_summary dws
         WHERE dws.stat_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND CURDATE()
         GROUP BY dws.page_id, dws.page_name, dws.page_type
     ) AS day_stats
ON DUPLICATE KEY UPDATE
                     total_uv = VALUES(total_uv),
                     total_pv = VALUES(total_pv),
                     entry_uv = VALUES(entry_uv),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     wireless_uv = VALUES(wireless_uv),
                     pc_uv = VALUES(pc_uv),
                     entry_rate = VALUES(entry_rate),
                     order_conversion_rate = VALUES(order_conversion_rate),
                     uv_rank = VALUES(uv_rank),
                     pv_rank = VALUES(pv_rank),
                     create_time = CURRENT_TIMESTAMP;

-- 插入7天维度数据
INSERT INTO ads_page_multi_dim_stats (
    time_dim, stat_date, page_id, page_name, page_type,
    total_uv, total_pv, entry_uv, avg_stay_duration,
    wireless_uv, pc_uv, entry_rate, order_conversion_rate,
    uv_rank, pv_rank
)
SELECT
    '7d' AS time_dim,
    CURDATE() AS stat_date,
    page_id,
    page_name,
    page_type,
    total_uv,
    total_pv,
    entry_uv,
    avg_stay_duration,
    wireless_uv,
    pc_uv,
    entry_rate,
    order_conversion_rate,
    ROW_NUMBER() OVER (PARTITION BY page_type ORDER BY total_uv DESC) AS uv_rank,
    ROW_NUMBER() OVER (ORDER BY total_pv DESC) AS pv_rank
FROM (
         SELECT
             dws.page_id,
             dws.page_name,
             dws.page_type,
             SUM(dws.uv) AS total_uv,
             SUM(dws.pv) AS total_pv,
             SUM(dws.entry_count) AS entry_uv,
             AVG(dws.avg_stay_duration) AS avg_stay_duration,
             SUM(CASE WHEN dws.device_type = 'wireless' THEN dws.uv ELSE 0 END) AS wireless_uv,
             SUM(CASE WHEN dws.device_type = 'pc' THEN dws.uv ELSE 0 END) AS pc_uv,
             (SUM(dws.entry_count)/SUM(dws.pv)*100) AS entry_rate,
             (SUM(dws.total_order_buyer)/SUM(dws.uv)*100) AS order_conversion_rate
         FROM dws_page_visit_summary dws
         WHERE dws.stat_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
         GROUP BY dws.page_id, dws.page_name, dws.page_type
     ) AS seven_day_stats
ON DUPLICATE KEY UPDATE
                     total_uv = VALUES(total_uv),
                     total_pv = VALUES(total_pv),
                     entry_uv = VALUES(entry_uv),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     wireless_uv = VALUES(wireless_uv),
                     pc_uv = VALUES(pc_uv),
                     entry_rate = VALUES(entry_rate),
                     order_conversion_rate = VALUES(order_conversion_rate),
                     uv_rank = VALUES(uv_rank),
                     pv_rank = VALUES(pv_rank),
                     create_time = CURRENT_TIMESTAMP;

-- 插入30天维度数据
INSERT INTO ads_page_multi_dim_stats (
    time_dim, stat_date, page_id, page_name, page_type,
    total_uv, total_pv, entry_uv, avg_stay_duration,
    wireless_uv, pc_uv, entry_rate, order_conversion_rate,
    uv_rank, pv_rank
)
SELECT
    '30d' AS time_dim,
    CURDATE() AS stat_date,
    page_id,
    page_name,
    page_type,
    total_uv,
    total_pv,
    entry_uv,
    avg_stay_duration,
    wireless_uv,
    pc_uv,
    entry_rate,
    order_conversion_rate,
    ROW_NUMBER() OVER (PARTITION BY page_type ORDER BY total_uv DESC) AS uv_rank,
    ROW_NUMBER() OVER (ORDER BY total_pv DESC) AS pv_rank
FROM (
         SELECT
             dws.page_id,
             dws.page_name,
             dws.page_type,
             SUM(dws.uv) AS total_uv,
             SUM(dws.pv) AS total_pv,
             SUM(dws.entry_count) AS entry_uv,
             AVG(dws.avg_stay_duration) AS avg_stay_duration,
             SUM(CASE WHEN dws.device_type = 'wireless' THEN dws.uv ELSE 0 END) AS wireless_uv,
             SUM(CASE WHEN dws.device_type = 'pc' THEN dws.uv ELSE 0 END) AS pc_uv,
             (SUM(dws.entry_count)/SUM(dws.pv)*100) AS entry_rate,
             (SUM(dws.total_order_buyer)/SUM(dws.uv)*100) AS order_conversion_rate
         FROM dws_page_visit_summary dws
         WHERE dws.stat_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
         GROUP BY dws.page_id, dws.page_name, dws.page_type
     ) AS thirty_day_stats
ON DUPLICATE KEY UPDATE
                     total_uv = VALUES(total_uv),
                     total_pv = VALUES(total_pv),
                     entry_uv = VALUES(entry_uv),
                     avg_stay_duration = VALUES(avg_stay_duration),
                     wireless_uv = VALUES(wireless_uv),
                     pc_uv = VALUES(pc_uv),
                     entry_rate = VALUES(entry_rate),
                     order_conversion_rate = VALUES(order_conversion_rate),
                     uv_rank = VALUES(uv_rank),
                     pv_rank = VALUES(pv_rank),
                     create_time = CURRENT_TIMESTAMP;


-- 2. PC端来源页面TOP20表
DROP TABLE IF EXISTS ads_pc_source_top20;
CREATE TABLE ads_pc_source_top20 (
                                     `time_dim` VARCHAR(10) NOT NULL COMMENT '时间维度（day/7d/30d）',
                                     `stat_date` DATE NOT NULL COMMENT '统计基准日期',
                                     `source_rank` INT NOT NULL COMMENT '来源页排名（1-20）',
                                     `source_page_id` VARCHAR(50) COMMENT '来源页面ID',
                                     `source_page_name` VARCHAR(100) COMMENT '来源页面名称',
                                     `pc_uv` INT DEFAULT 0 COMMENT 'PC端来源访客数',
                                     `pc_pv` INT DEFAULT 0 COMMENT 'PC端来源浏览量',
                                     `source_ratio` DECIMAL(5,2) COMMENT '来源占比（%）',
                                     `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据生成时间',
                                     PRIMARY KEY (`time_dim`, `stat_date`, `source_rank`),
                                     KEY `idx_source_name` (`source_page_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'PC端来源TOP20表';

-- 插入日维度PC来源数据
INSERT INTO ads_pc_source_top20 (
    time_dim, stat_date, source_rank,
    source_page_id, source_page_name,
    pc_uv, pc_pv, source_ratio
)
SELECT
    'day' AS time_dim,
    CURDATE() AS stat_date,
    source_rank,
    source_page_id,
    source_page_name,
    pc_uv,
    pc_pv,
    source_ratio
FROM (
         SELECT
             source_page_id,
             source_page_name,
             pc_uv,
             pc_pv,
             source_ratio,
             ROW_NUMBER() OVER (ORDER BY pc_pv DESC) AS source_rank
         FROM (
                  SELECT
                      opvl.refer_page_id AS source_page_id,
                      optd.page_name AS source_page_name,
                      COUNT(DISTINCT opvl.user_id) AS pc_uv,
                      COUNT(opvl.log_id) AS pc_pv,
                      (COUNT(opvl.log_id) / (
                          SELECT COUNT(log_id)
                          FROM ods_page_visit_log
                          WHERE device_type = 'pc'
                            AND visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND CURDATE()
                      ) * 100) AS source_ratio
                  FROM ods_page_visit_log opvl
                           LEFT JOIN dim_page optd ON opvl.refer_page_id = optd.page_id
                  WHERE opvl.device_type = 'pc'
                    AND opvl.refer_page_id IS NOT NULL
                    AND opvl.visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND CURDATE()
                  GROUP BY opvl.refer_page_id, optd.page_name
              ) AS source_data
     ) AS ranked_source
WHERE source_rank <= 20
ON DUPLICATE KEY UPDATE
                     source_page_id = VALUES(source_page_id),
                     source_page_name = VALUES(source_page_name),
                     pc_uv = VALUES(pc_uv),
                     pc_pv = VALUES(pc_pv),
                     source_ratio = VALUES(source_ratio),
                     create_time = CURRENT_TIMESTAMP;

-- 插入7天维度PC来源数据
INSERT INTO ads_pc_source_top20 (
    time_dim, stat_date, source_rank,
    source_page_id, source_page_name,
    pc_uv, pc_pv, source_ratio
)
SELECT
    '7d' AS time_dim,
    CURDATE() AS stat_date,
    source_rank,
    source_page_id,
    source_page_name,
    pc_uv,
    pc_pv,
    source_ratio
FROM (
         SELECT
             source_page_id,
             source_page_name,
             pc_uv,
             pc_pv,
             source_ratio,
             ROW_NUMBER() OVER (ORDER BY pc_pv DESC) AS source_rank
         FROM (
                  SELECT
                      opvl.refer_page_id AS source_page_id,
                      optd.page_name AS source_page_name,
                      COUNT(DISTINCT opvl.user_id) AS pc_uv,
                      COUNT(opvl.log_id) AS pc_pv,
                      (COUNT(opvl.log_id) / (
                          SELECT COUNT(log_id)
                          FROM ods_page_visit_log
                          WHERE device_type = 'pc'
                            AND visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
                      ) * 100) AS source_ratio
                  FROM ods_page_visit_log opvl
                           LEFT JOIN dim_page optd ON opvl.refer_page_id = optd.page_id
                  WHERE opvl.device_type = 'pc'
                    AND opvl.refer_page_id IS NOT NULL
                    AND opvl.visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
                  GROUP BY opvl.refer_page_id, optd.page_name
              ) AS source_data
     ) AS ranked_source
WHERE source_rank <= 20
ON DUPLICATE KEY UPDATE
                     source_page_id = VALUES(source_page_id),
                     source_page_name = VALUES(source_page_name),
                     pc_uv = VALUES(pc_uv),
                     pc_pv = VALUES(pc_pv),
                     source_ratio = VALUES(source_ratio),
                     create_time = CURRENT_TIMESTAMP;

-- 插入30天维度PC来源数据
INSERT INTO ads_pc_source_top20 (
    time_dim, stat_date, source_rank,
    source_page_id, source_page_name,
    pc_uv, pc_pv, source_ratio
)
SELECT
    '30d' AS time_dim,
    CURDATE() AS stat_date,
    source_rank,
    source_page_id,
    source_page_name,
    pc_uv,
    pc_pv,
    source_ratio
FROM (
         SELECT
             source_page_id,
             source_page_name,
             pc_uv,
             pc_pv,
             source_ratio,
             ROW_NUMBER() OVER (ORDER BY pc_pv DESC) AS source_rank
         FROM (
                  SELECT
                      opvl.refer_page_id AS source_page_id,
                      optd.page_name AS source_page_name,
                      COUNT(DISTINCT opvl.user_id) AS pc_uv,
                      COUNT(opvl.log_id) AS pc_pv,
                      (COUNT(opvl.log_id) / (
                          SELECT COUNT(log_id)
                          FROM ods_page_visit_log
                          WHERE device_type = 'pc'
                            AND visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
                      ) * 100) AS source_ratio
                  FROM ods_page_visit_log opvl
                           LEFT JOIN dim_page optd ON opvl.refer_page_id = optd.page_id
                  WHERE opvl.device_type = 'pc'
                    AND opvl.refer_page_id IS NOT NULL
                    AND opvl.visit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()
                  GROUP BY opvl.refer_page_id, optd.page_name
              ) AS source_data
     ) AS ranked_source
WHERE source_rank <= 20
ON DUPLICATE KEY UPDATE
                     source_page_id = VALUES(source_page_id),
                     source_page_name = VALUES(source_page_name),
                     pc_uv = VALUES(pc_uv),
                     pc_pv = VALUES(pc_pv),
                     source_ratio = VALUES(source_ratio),
                     create_time = CURRENT_TIMESTAMP;
