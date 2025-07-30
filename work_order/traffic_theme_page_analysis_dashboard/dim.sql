use test33;
-- 1. 店铺维度表（从ODS层提取并去重）
DROP TABLE IF EXISTS dim_store;
CREATE TABLE dim_store (
    `store_id` VARCHAR(20) PRIMARY KEY COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称',
    `create_time` DATETIME COMMENT '维度首次出现时间',
    `is_valid` TINYINT DEFAULT 1 COMMENT '是否有效（1-有效，0-无效）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '店铺维度表（从ODS提取）';

-- 从ODS层初始化店铺维度
INSERT INTO dim_store (store_id, store_name, create_time)
SELECT DISTINCT
    store_id,
    store_name,
    MIN(behavior_time) -- 取首次出现时间作为维度创建时间
FROM ods_page_behavior
GROUP BY store_id, store_name;

select * from dim_store;

-- 2. 商品维度表（从ODS层提取并去重，仅保留商品页数据）
DROP TABLE IF EXISTS dim_item;
CREATE TABLE dim_item (
    `item_id` VARCHAR(50) PRIMARY KEY COMMENT '商品ID',
    `item_name` VARCHAR(100) NOT NULL COMMENT '商品名称',
    `store_id` VARCHAR(20) NOT NULL COMMENT '所属店铺ID',
    `create_time` DATETIME COMMENT '维度首次出现时间',
    FOREIGN KEY (`store_id`) REFERENCES dim_store(`store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品维度表（从ODS提取）';

-- 从ODS层初始化商品维度（仅商品页数据）
INSERT INTO dim_item (item_id, item_name, store_id, create_time)
SELECT DISTINCT
    item_id,
    item_name,
    store_id,
    MIN(behavior_time)
FROM ods_page_behavior
WHERE page_type = 'item' -- 仅商品页数据
  AND item_id IS NOT NULL
GROUP BY item_id, item_name, store_id;

select * from dim_item;

-- 3. 用户维度表（从ODS层提取并去重）
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    `user_id` VARCHAR(50) PRIMARY KEY COMMENT '用户ID',
    `create_time` DATETIME COMMENT '维度首次出现时间',
    `is_valid` TINYINT DEFAULT 1 COMMENT '是否有效（1-有效，0-无效）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '用户维度表（从ODS提取）';

-- 从ODS层初始化用户维度
INSERT INTO dim_user (user_id, create_time)
SELECT DISTINCT
    user_id,
    MIN(behavior_time)
FROM ods_page_behavior
GROUP BY user_id;

select * from dim_user;