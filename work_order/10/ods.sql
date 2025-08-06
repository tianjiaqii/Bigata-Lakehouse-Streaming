use test33;

-- 存储页面行为原始数据，保留全量信息
-- 调整后的ODS层：存储页面行为原始数据，移除page_id、page_name、module_id相关字段，分区格式为yyyyMMdd
-- ODS层：页面行为原始表（强制item_id/item_name同空同非空）
DROP TABLE IF EXISTS ods_page_behavior;
CREATE TABLE ods_page_behavior (
    `log_id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '日志ID',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称',
    `page_type` ENUM('home', 'custom', 'item') NOT NULL COMMENT '页面类型',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID',
    `behavior_type` ENUM('click', 'visit') NOT NULL COMMENT '行为类型',
    `behavior_time` DATETIME NOT NULL COMMENT '行为时间（yyyy-MM-dd HH:mm:ss）',
    `guide_pay_amount` DECIMAL(16,2) COMMENT '引导支付金额',
    `item_id` VARCHAR(50) COMMENT '商品ID（商品页非空，非商品页空）',
    `item_name` VARCHAR(100) COMMENT '商品名称（与item_id同空/同非空）',
    `ods_dt` INT GENERATED ALWAYS AS (CAST(DATE_FORMAT(behavior_time, '%Y%m%d') AS UNSIGNED)) STORED COMMENT '分区：yyyyMMdd'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ODS层-页面行为（item_id/item_name严格一致）';




-- 生成逻辑示例（实际可通过存储过程批量生成）
-- 生成5家店铺
-- 直接生成ods_page_behavior数据（不依赖维度表）
DELIMITER $$
DROP PROCEDURE IF EXISTS generate_ods_data_direct;
CREATE PROCEDURE generate_ods_data_direct(IN num_records INT)
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE start_time DATETIME DEFAULT '2025-06-29 00:00:00';
    DECLARE end_time DATETIME DEFAULT '2025-07-30 00:00:00';
    DECLARE time_diff INT;
    DECLARE rand_seconds INT;
    DECLARE rand_store INT;
    DECLARE rand_page_type INT;
    DECLARE rand_behavior_type INT;
    DECLARE rand_user INT;
    DECLARE rand_item INT;
    DECLARE current_store_id VARCHAR(20);
    DECLARE current_store_name VARCHAR(50);
    DECLARE current_page_type ENUM('home', 'custom', 'item');
    DECLARE current_item_id VARCHAR(50);
    DECLARE current_item_name VARCHAR(100);

    SET time_diff = TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- 店铺信息映射表
    DROP TEMPORARY TABLE IF EXISTS temp_stores;
    CREATE TEMPORARY TABLE temp_stores (
        store_idx INT,
        store_id VARCHAR(20),
        store_name VARCHAR(50)
    );
    INSERT INTO temp_stores VALUES
        (1, 'store01', '时尚精品店'),
        (2, 'store02', '生鲜超市'),
        (3, 'store03', '数码科技'),
        (4, 'store04', '家居生活'),
        (5, 'store05', '母婴世界');

    -- 商品信息映射表（按店铺分组）
    DROP TEMPORARY TABLE IF EXISTS temp_items;
    CREATE TEMPORARY TABLE temp_items (
        store_id VARCHAR(20),
        item_idx INT,
        item_id VARCHAR(50),
        item_name VARCHAR(100)
    );
    INSERT INTO temp_items VALUES
        -- 时尚精品店商品
        ('store01', 1, 'item01001', '时尚连衣裙'),
        ('store01', 2, 'item01002', '休闲运动鞋'),
        ('store01', 3, 'item01003', '时尚挎包'),
        ('store01', 4, 'item01004', '羊毛外套'),
        ('store01', 5, 'item01005', '丝绸围巾'),
        -- 生鲜超市商品
        ('store02', 1, 'item02001', '新鲜苹果'),
        ('store02', 2, 'item02002', '精品牛肉'),
        ('store02', 3, 'item02003', '深海带鱼'),
        ('store02', 4, 'item02004', '有机蔬菜'),
        ('store02', 5, 'item02005', '巴氏牛奶'),
        -- 数码科技商品
        ('store03', 1, 'item03001', '智能手机'),
        ('store03', 2, 'item03002', '无线耳机'),
        ('store03', 3, 'item03003', '平板电脑'),
        ('store03', 4, 'item03004', '智能手表'),
        ('store03', 5, 'item03005', '移动电源'),
        -- 家居生活商品
        ('store04', 1, 'item04001', '舒适抱枕'),
        ('store04', 2, 'item04002', '简约台灯'),
        ('store04', 3, 'item04003', '抗菌毛巾'),
        ('store04', 4, 'item04004', '防滑地垫'),
        ('store04', 5, 'item04005', '收纳盒'),
        -- 母婴世界商品
        ('store05', 1, 'item05001', '婴儿奶粉'),
        ('store05', 2, 'item05002', '儿童绘本'),
        ('store05', 3, 'item05003', '婴儿推车'),
        ('store05', 4, 'item05004', '纸尿裤'),
        ('store05', 5, 'item05005', '学步鞋');

    WHILE i <= num_records DO
            -- 随机选择店铺
            SET rand_store = FLOOR(RAND() * 5) + 1;
            SELECT store_id, store_name INTO current_store_id, current_store_name
            FROM temp_stores WHERE store_idx = rand_store;

            -- 随机选择页面类型（40%非商品页，60%商品页）
            SET rand_page_type = FLOOR(RAND() * 10);
            IF rand_page_type < 4 THEN
                -- 非商品页：item_id和item_name均为空
                SET current_page_type = ELT(rand_page_type % 2 + 1, 'home', 'custom');
                SET current_item_id = NULL;
                SET current_item_name = NULL;
            ELSE
                -- 商品页：随机选择当前店铺的商品
                SET rand_item = FLOOR(RAND() * 5) + 1;
                SELECT item_id, item_name INTO current_item_id, current_item_name
                FROM temp_items
                WHERE store_id = current_store_id AND item_idx = rand_item;
                SET current_page_type = 'item';
            END IF;

            -- 生成其他随机字段
            SET rand_behavior_type = FLOOR(RAND() * 2) + 1; -- 1=click, 2=visit
            SET rand_user = FLOOR(RAND() * 200) + 1; -- 200个用户
            SET rand_seconds = FLOOR(RAND() * time_diff);

            -- 插入ODS表
            INSERT INTO ods_page_behavior (
                store_id, store_name, page_type,
                user_id, behavior_type, behavior_time,
                guide_pay_amount, item_id, item_name
            ) VALUES (
                current_store_id,
                current_store_name,
                current_page_type,
                CONCAT('user_', LPAD(rand_user, 3, '0')),
                ELT(rand_behavior_type, 'click', 'visit'),
                DATE_ADD(start_time, INTERVAL rand_seconds SECOND),
                CASE WHEN RAND() > 0.7 THEN ROUND(RAND() * 1000, 2) ELSE 0 END, -- 30%概率有支付金额
                current_item_id,
                current_item_name
                     );

            SET i = i + 1;
        END WHILE;

    -- 清理临时表
    DROP TEMPORARY TABLE IF EXISTS temp_stores;
    DROP TEMPORARY TABLE IF EXISTS temp_items;
END$$
DELIMITER ;

-- 调用存储过程生成10000条数据
CALL generate_ods_data_direct(10000);

-- 验证数据规则
-- 1. 检查非商品页item_id/item_name是否均为空
SELECT COUNT(*) AS 非商品页异常数
FROM ods_page_behavior
WHERE page_type IN ('home', 'custom')
  AND (item_id IS NOT NULL OR item_name IS NOT NULL);

-- 2. 检查商品页item_id/item_name是否均非空
SELECT COUNT(*) AS 商品页异常数
FROM ods_page_behavior
WHERE page_type = 'item'
  AND (item_id IS NULL OR item_name IS NULL);

-- 3. 检查时间范围是否正确
SELECT MIN(behavior_time) AS 最小时间, MAX(behavior_time) AS 最大时间
FROM ods_page_behavior;

select * from ods_page_behavior;