use test33;


-- DWS层-店铺页面日汇总（与之前逻辑一致，仅数据源改为DWD层）
DROP TABLE IF EXISTS dws_store_daily_summary;
CREATE TABLE dws_store_daily_summary (
    `stat_date` DATE NOT NULL COMMENT '统计日期（yyyy-MM-dd）',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称',
    -- 点击指标
    `total_click_cnt` INT DEFAULT 0 COMMENT '总点击量（所有页面类型，未去重）',
    `total_click_uv` INT DEFAULT 0 COMMENT '总点击人数（去重独立用户数）',
    -- 访问指标（仅首页+自定义页）
    `visit_cnt` INT DEFAULT 0 COMMENT '总访问次数（首页+自定义页，未去重）',
    `visit_uv` INT DEFAULT 0 COMMENT '访问人数（首页+自定义页，去重独立用户数）',
    -- 金额指标
    `guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '引导支付总金额',
    PRIMARY KEY (`stat_date`, `store_id`) COMMENT '按日期+店铺唯一标识'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层-店铺日度汇总表（点击/访问/金额）';


-- 插入数据：从DWD层汇总
INSERT INTO dws_store_daily_summary
SELECT
    DATE(d.behavior_time) AS stat_date,
    d.store_id,
    d.store_name,
    -- 总点击量：所有页面类型的click行为
    SUM(CASE WHEN d.behavior_type = 'click' THEN 1 ELSE 0 END) AS total_click_cnt,
    -- 总点击人数：去重用户数（基于click行为）
    COUNT(DISTINCT CASE WHEN d.behavior_type = 'click' THEN d.user_id END) AS total_click_uv,
    -- 访问次数：仅首页+自定义页的visit行为
    SUM(CASE WHEN d.behavior_type = 'visit' AND d.page_type IN ('home', 'custom') THEN 1 ELSE 0 END) AS visit_cnt,
    -- 访问人数：仅首页+自定义页的去重用户（基于visit行为）
    COUNT(DISTINCT CASE WHEN d.behavior_type = 'visit' AND d.page_type IN ('home', 'custom') THEN d.user_id END) AS visit_uv,
    -- 引导支付总金额
    SUM(d.guide_pay_amount) AS guide_pay_amt
FROM dwd_page_behavior_detail d
GROUP BY DATE(d.behavior_time), d.store_id, d.store_name;

select * from dws_store_daily_summary;





-- 商品维度日汇总表（新增商品访问人数字段）
DROP TABLE IF EXISTS dws_item_daily_summary;
CREATE TABLE dws_item_daily_summary (
    `stat_date` DATE NOT NULL COMMENT '统计日期（yyyy-MM-dd）',
    `store_id` VARCHAR(20) NOT NULL COMMENT '所属店铺ID',
     store_name VARCHAR(50) NOT NULL COMMENT '所属店铺名称',
    `item_id` VARCHAR(50) NOT NULL COMMENT '商品ID',
    `item_name` VARCHAR(100) NOT NULL COMMENT '商品名称',
    -- 点击指标（仅商品详情页）
    `click_cnt` INT DEFAULT 0 COMMENT '商品点击量（商品页click，未去重）',
    `click_uv` INT DEFAULT 0 COMMENT '商品点击人数（去重独立用户数）',
    -- 访问指标（仅商品详情页）
    `visit_cnt` INT DEFAULT 0 COMMENT '商品访问次数（商品页visit，未去重）',
    `visit_uv` INT DEFAULT 0 COMMENT '商品访问人数（去重独立用户数）', -- 新增：访问人数
    -- 金额指标
    `guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '商品引导支付金额',
    PRIMARY KEY (`stat_date`, `store_id`, `item_id`) COMMENT '按日期+店铺+商品唯一标识'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层-商品日度汇总表（点击/访问/金额）';

-- 插入数据：从DWD层汇总（仅商品页数据，包含访问人数计算）
INSERT INTO dws_item_daily_summary
SELECT
    DATE(d.behavior_time) AS stat_date,
    d.store_id,
    d.store_name ,
    d.item_id,
    d.item_name,
    -- 商品点击量：商品页的click行为
    SUM(CASE WHEN d.behavior_type = 'click' THEN 1 ELSE 0 END) AS click_cnt,
    -- 商品点击人数：商品页click的去重用户
    COUNT(DISTINCT CASE WHEN d.behavior_type = 'click' THEN d.user_id END) AS click_uv,
    -- 商品访问次数：商品页的visit行为
    SUM(CASE WHEN d.behavior_type = 'visit' THEN 1 ELSE 0 END) AS visit_cnt,
    -- 商品访问人数：商品页visit的去重用户（新增逻辑）
    COUNT(DISTINCT CASE WHEN d.behavior_type = 'visit' THEN d.user_id END) AS visit_uv,
    -- 商品引导支付金额
    SUM(d.guide_pay_amount) AS guide_pay_amt
FROM dwd_page_behavior_detail d
WHERE d.page_type = 'item' AND d.item_id IS NOT NULL -- 仅商品页数据
GROUP BY DATE(d.behavior_time), d.store_id,d.store_name, d.item_id, d.item_name;


select * from dws_item_daily_summary;















