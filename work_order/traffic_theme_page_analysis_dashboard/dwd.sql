use test33;

-- DWD层：页面行为明细（关联DIM层，清洗后）
DROP TABLE IF EXISTS dwd_page_behavior_detail;
CREATE TABLE dwd_page_behavior_detail (
    `log_id` BIGINT PRIMARY KEY COMMENT '日志ID（关联ODS）',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称（关联DIM）',
    `page_type` ENUM('home', 'custom', 'item') NOT NULL COMMENT '页面类型',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID',
    `behavior_type` ENUM('click', 'visit') NOT NULL COMMENT '行为类型',
    `behavior_time` DATETIME NOT NULL COMMENT '行为时间',
    `guide_pay_amount` DECIMAL(16,2) NOT NULL DEFAULT 0.00 COMMENT '引导支付金额（清洗后）',
    `item_id` VARCHAR(50) COMMENT '商品ID（与item_name同空/同非空）',
    `item_name` VARCHAR(100) COMMENT '商品名称（关联DIM）',
    `ods_dt` INT NOT NULL COMMENT '分区：yyyyMMdd'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWD层-页面行为明细（关联DIM）';

-- ODS→DWD：关联DIM层补充维度属性
INSERT INTO dwd_page_behavior_detail
SELECT
    o.log_id,
    o.store_id,
    s.store_name, -- 从DIM层获取店铺名称，确保一致性
    o.page_type,
    o.user_id,
    o.behavior_type,
    o.behavior_time,
    CASE WHEN o.guide_pay_amount < 0 THEN 0.00 ELSE o.guide_pay_amount END AS guide_pay_amount,
    o.item_id,
    i.item_name, -- 从DIM层获取商品名称，确保一致性
    o.ods_dt
FROM ods_page_behavior o
-- 关联店铺维度
         JOIN dim_store s ON o.store_id = s.store_id
-- 关联商品维度（仅商品页）
         LEFT JOIN dim_item i ON o.item_id = i.item_id AND o.store_id = i.store_id
-- 过滤item_id/item_name不一致的异常数据
WHERE (o.item_id IS NULL AND o.item_name IS NULL)
   OR (o.item_id IS NOT NULL AND i.item_name IS NOT NULL); -- 商品名称从DIM层校验



select * from dwd_page_behavior_detail;