use test33;
-- DWS层-店铺页面日汇总（与之前逻辑一致，仅数据源改为DWD层）
DROP TABLE IF EXISTS dws_store_page_daily;
CREATE TABLE dws_store_page_daily (
    `stat_date` INT NOT NULL COMMENT '统计日期（yyyyMMdd）',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `page_type` ENUM('home', 'custom', 'item') NOT NULL COMMENT '页面类型',
    `visit_count` BIGINT DEFAULT 0 COMMENT '访问次数',
    `visit_user_count` BIGINT DEFAULT 0 COMMENT '访问用户数（去重）',
    `click_count` BIGINT DEFAULT 0 COMMENT '点击次数',
    `click_user_count` BIGINT DEFAULT 0 COMMENT '点击用户数（去重）',
    `pay_count` BIGINT DEFAULT 0 COMMENT '支付行为次数（guide_pay_amount>0）',
    `total_pay_amount` DECIMAL(16,2) DEFAULT 0.00 COMMENT '总支付金额',
    PRIMARY KEY (`stat_date`, `store_id`, `page_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层-店铺页面日汇总';

INSERT INTO dws_store_page_daily
SELECT
    ods_dt AS stat_date,
    store_id,
    page_type,
    SUM(CASE WHEN behavior_type = 'visit' THEN 1 ELSE 0 END) AS visit_count,
    COUNT(DISTINCT CASE WHEN behavior_type = 'visit' THEN user_id END) AS visit_user_count,
    SUM(CASE WHEN behavior_type = 'click' THEN 1 ELSE 0 END) AS click_count,
    COUNT(DISTINCT CASE WHEN behavior_type = 'click' THEN user_id END) AS click_user_count,
    SUM(CASE WHEN guide_pay_amount > 0 THEN 1 ELSE 0 END) AS pay_count,
    SUM(CASE WHEN guide_pay_amount > 0 THEN guide_pay_amount ELSE 0 END) AS total_pay_amount
FROM dwd_page_behavior_detail
GROUP BY ods_dt, store_id, page_type;

select * from dws_store_page_daily;