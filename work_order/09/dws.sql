USE test2;

-- DWS层：页面访问汇总表（按页面、设备、日期聚合）
DROP TABLE IF EXISTS dws_page_visit_summary;
CREATE TABLE dws_page_visit_summary (
    `stat_date` DATE NOT NULL COMMENT '统计日期',
    `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID',
    `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称',
    `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型',
    `device_type` ENUM('wireless', 'pc') NOT NULL COMMENT '设备类型',
    `pv` INT NOT NULL DEFAULT 0 COMMENT '页面访问量',
    `uv` INT NOT NULL DEFAULT 0 COMMENT '独立访客数',
    `avg_stay_duration` DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    `entry_count` INT NOT NULL DEFAULT 0 COMMENT '进店次数',
    `total_order_buyer` INT NOT NULL DEFAULT 0 COMMENT '总下单买家数',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (`stat_date`, `page_id`, `device_type`),
    KEY `idx_page_type` (`page_type`),
    KEY `idx_stat_date` (`stat_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层-页面访问按日、页面、设备汇总表';



-- 从DWD层抽取数据并聚合
INSERT INTO dws_page_visit_summary (
    stat_date, page_id, page_name, page_type, device_type,
    pv, uv, avg_stay_duration, entry_count, total_order_buyer
)
SELECT
    visit_date AS stat_date,
    page_id,
    page_name,
    page_type,
    device_type,
    COUNT(log_id) AS pv,  -- 访问量=日志记录数
    COUNT(DISTINCT user_id) AS uv,  -- 独立访客数=去重用户数
    AVG(stay_duration) AS avg_stay_duration,  -- 平均停留时长
    SUM(CASE WHEN is_entry = 1 THEN 1 ELSE 0 END) AS entry_count,  -- 进店次数
    SUM(order_buyer_num) AS total_order_buyer  -- 总下单买家数
FROM
    dwd_page_visit_detail
GROUP BY
    visit_date, page_id, page_name, page_type, device_type
ON DUPLICATE KEY UPDATE
    pv = VALUES(pv),
    uv = VALUES(uv),
    avg_stay_duration = VALUES(avg_stay_duration),
    entry_count = VALUES(entry_count),
    total_order_buyer = VALUES(total_order_buyer),
    update_time = CURRENT_TIMESTAMP;
select * from dws_page_visit_summary;

-- DWS层：用户访问路径汇总表（按用户、日期聚合）
DROP TABLE IF EXISTS dws_user_path_summary;
CREATE TABLE dws_user_path_summary (
    `stat_date` DATE NOT NULL COMMENT '统计日期',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID',
    `device_type` ENUM('wireless', 'pc') NOT NULL COMMENT '设备类型',
    `visit_count` INT NOT NULL DEFAULT 0 COMMENT '访问次数',
    `first_visit_page` VARCHAR(50) COMMENT '首次访问页面ID',
    `last_visit_page` VARCHAR(50) COMMENT '末次访问页面ID',
    `total_stay_duration` INT NOT NULL DEFAULT 0 COMMENT '总停留时长（秒）',
    `order_buyer_num` INT NOT NULL DEFAULT 0 COMMENT '下单买家数',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (`stat_date`, `user_id`, `device_type`),
    KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层-用户访问路径按日汇总表';

-- 从DWD层抽取数据并聚合用户行为
INSERT INTO dws_user_path_summary (
    stat_date, user_id, device_type, visit_count,
    first_visit_page, last_visit_page, total_stay_duration, order_buyer_num
)
SELECT
    visit_date AS stat_date,
    user_id,
    device_type,
    COUNT(log_id) AS visit_count,  -- 访问次数=用户当日访问记录数
    MIN(page_id) AS first_visit_page,  -- 首次访问页面（按时间排序取第一个）
    MAX(page_id) AS last_visit_page,  -- 末次访问页面（按时间排序取最后一个）
    SUM(stay_duration) AS total_stay_duration,  -- 总停留时长
    SUM(order_buyer_num) AS order_buyer_num  -- 下单单买家数
FROM
    dwd_page_visit_detail
GROUP BY
    visit_date, user_id, device_type
ON DUPLICATE KEY UPDATE
    visit_count = VALUES(visit_count),
    first_visit_page = VALUES(first_visit_page),
    last_visit_page = VALUES(last_visit_page),
    total_stay_duration = VALUES(total_stay_duration),
    order_buyer_num = VALUES(order_buyer_num),
    update_time = CURRENT_TIMESTAMP;

select * from dws_user_path_summary;