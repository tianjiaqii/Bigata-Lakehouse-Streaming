use test33;
-- ADS层-店铺运营看板（与之前逻辑一致，数据源为DWS层）
DROP TABLE IF EXISTS ads_store_operation;
CREATE TABLE ads_store_operation (
    `stat_date` INT NOT NULL COMMENT '统计日期（yyyyMMdd）',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称',
    `home_visit_count` BIGINT DEFAULT 0 COMMENT '首页访问次数',
    `home_click_rate` DECIMAL(5,2) COMMENT '首页点击率（点击/访问）',
    `item_visit_count` BIGINT DEFAULT 0 COMMENT '商品页访问次数',
    `item_pay_rate` DECIMAL(5,2) COMMENT '商品页支付转化率（支付次数/访问）',
    `total_pay_amount` DECIMAL(16,2) DEFAULT 0.00 COMMENT '总支付金额',
    `total_user_count` BIGINT DEFAULT 0 COMMENT '总访问用户数（去重）',
    PRIMARY KEY (`stat_date`, `store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层-店铺运营看板';

INSERT INTO ads_store_operation
SELECT
    d.stat_date,
    d.store_id,
    s.store_name,
    SUM(CASE WHEN d.page_type = 'home' THEN d.visit_count ELSE 0 END) AS home_visit_count,
    ROUND(
                SUM(CASE WHEN d.page_type = 'home' THEN d.click_count ELSE 0 END)
                / NULLIF(SUM(CASE WHEN d.page_type = 'home' THEN d.visit_count ELSE 0 END), 0),
                2
        ) AS home_click_rate,
    SUM(CASE WHEN d.page_type = 'item' THEN d.visit_count ELSE 0 END) AS item_visit_count,
    ROUND(
                SUM(CASE WHEN d.page_type = 'item' THEN d.pay_count ELSE 0 END)
                / NULLIF(SUM(CASE WHEN d.page_type = 'item' THEN d.visit_count ELSE 0 END), 0),
                2
        ) AS item_pay_rate,
    SUM(d.total_pay_amount) AS total_pay_amount,
    COUNT(DISTINCT CASE WHEN d.visit_user_count > 0 THEN CONCAT(d.store_id, d.stat_date) END) AS total_user_count
FROM dws_store_page_daily d
         JOIN dim_store s ON d.store_id = s.store_id
GROUP BY d.stat_date, d.store_id, s.store_name;


select * from ads_store_operation;