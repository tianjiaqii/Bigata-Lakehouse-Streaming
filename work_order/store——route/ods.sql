USE test2;

-- 1. 日志表（保持不变）
DROP TABLE IF EXISTS ods_page_visit_log;
CREATE TABLE ods_page_visit_log (
    `log_id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '日志ID',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID',
    `device_type` ENUM('wireless', 'pc') NOT NULL COMMENT '设备类型（无线端/PC端）',
    `visit_time` DATETIME NOT NULL COMMENT '访问时间',
    `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID',
    `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称（如首页、活动页）',
    `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    `sub_type` VARCHAR(50) NOT NULL COMMENT '页面子类型（如首页、活动页、订阅页等）',
    `refer_page_id` VARCHAR(50) COMMENT '来源页面ID（上一页面）',
    `stay_duration` INT NOT NULL COMMENT '停留时长（秒）',
    `is_entry` TINYINT NOT NULL COMMENT '是否进店页（1-是，0-否）',
    `visitor_count` INT NOT NULL DEFAULT 1 COMMENT '访客数',
    `order_buyer_num` INT DEFAULT 0 COMMENT '下单买家数',
    `ods_dt` INT GENERATED ALWAYS AS (CAST(DATE_FORMAT(visit_time, '%Y%m%d') AS UNSIGNED)) STORED COMMENT '分区：yyyyMMdd'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ODS层-页面访问日志（含路径信息）';

-- 2. 字典表（保持不变）
DROP TABLE IF EXISTS ods_page_type_dict;
CREATE TABLE ods_page_type_dict (
    `dict_id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '字典ID',
    `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型',
    `type_desc` VARCHAR(100) NOT NULL COMMENT '类型描述',
    `sub_type` VARCHAR(50) NOT NULL COMMENT '子类型',
    `sub_type_desc` VARCHAR(100) NOT NULL COMMENT '子类型描述',
    `sample_page_id` VARCHAR(50) NOT NULL COMMENT '示例页面ID（用于生成数据）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ODS层-页面类型字典表（含示例ID）';

-- 插入字典数据（手动确保无特殊字符）
INSERT INTO ods_page_type_dict (page_type, type_desc, sub_type, sub_type_desc, sample_page_id) VALUES
    ('store_page', '店铺内的首页、活动页等', 'home', '店铺首页', 'store_page_home_001'),
    ('store_page', '店铺内的首页、活动页等', 'activity', '活动页', 'store_page_activity_001'),
    ('store_page', '店铺内的首页、活动页等', 'category', '分类页', 'store_page_category_001'),
    ('store_page', '店铺内的首页、活动页等', 'baby', '宝贝页', 'store_page_baby_001'),
    ('store_page', '店铺内的首页、活动页等', 'new', '新品页', 'store_page_new_001'),
    ('item_detail', '商品的基础详情页面', 'base', '基础详情页', 'item_detail_base_001'),
    ('other_page', '订阅页、直播页等', 'subscribe', '订阅页', 'other_page_subscribe_001'),
    ('other_page', '订阅页、直播页等', 'live', '直播页', 'other_page_live_001');

-- 3. 精确生成 10000 条数据（用循环控制）
DELIMITER $$
DROP PROCEDURE IF EXISTS generate_exact_10000;
CREATE PROCEDURE generate_exact_10000()
BEGIN
    DECLARE i INT DEFAULT 0;
    WHILE i < 10000 DO
            -- 随机用户（1-200）
            SET @user_id = CONCAT('user_', LPAD(FLOOR(RAND()*200)+1, 3, '0'));
            -- 随机设备（无线/PC）
            SET @device_type = ELT(FLOOR(RAND()*2)+1, 'wireless', 'pc');
            -- 随机时间（7月1日-7月30日）
            SET @visit_time = DATE_ADD('2025-07-01 00:00:00', INTERVAL FLOOR(RAND()*(30*24*3600)) SECOND);
            -- 随机页面类型
            SET @rand_page_type = FLOOR(RAND() * 3) + 1;

            -- 从字典表取数据
            IF @rand_page_type = 1 THEN
                SELECT page_type, sub_type, sub_type_desc, sample_page_id
                INTO @page_type, @sub_type, @page_name, @page_id
                FROM ods_page_type_dict
                WHERE page_type = 'store_page'
                ORDER BY RAND() LIMIT 1;
            ELSEIF @rand_page_type = 2 THEN
                SELECT page_type, sub_type, sub_type_desc, sample_page_id
                INTO @page_type, @sub_type, @page_name, @page_id
                FROM ods_page_type_dict
                WHERE page_type = 'item_detail'
                ORDER BY RAND() LIMIT 1;
            ELSE
                SELECT page_type, sub_type, sub_type_desc, sample_page_id
                INTO @page_type, @sub_type, @page_name, @page_id
                FROM ods_page_type_dict
                WHERE page_type = 'other_page'
                ORDER BY RAND() LIMIT 1;
            END IF;

            -- 随机来源页
            IF RAND() > 0.3 THEN
                SELECT sample_page_id INTO @refer_page_id
                FROM ods_page_type_dict
                WHERE page_type != @page_type
                ORDER BY RAND() LIMIT 1;
                SET @is_entry = 0;
            ELSE
                SET @refer_page_id = NULL;
                SET @is_entry = 1;
            END IF;

            -- 停留时长
            SET @stay_duration = FLOOR(RAND() * 290) + 10;
            -- 下单买家数
            SET @order_buyer_num = IF(@is_entry AND RAND() > 0.8, FLOOR(RAND() * 5) + 1, 0);

            -- 插入数据
            INSERT INTO ods_page_visit_log (
                user_id, device_type, visit_time, page_id, page_name,
                page_type, sub_type, refer_page_id, stay_duration,
                is_entry, order_buyer_num
            ) VALUES (
                         @user_id, @device_type, @visit_time, @page_id, @page_name,
                         @page_type, @sub_type, @refer_page_id, @stay_duration,
                         @is_entry, @order_buyer_num
                     );

            -- 计数+1
            SET i = i + 1;
        END WHILE;
END$$
DELIMITER ;

-- 执行存储过程（确保10000条）
CALL generate_exact_10000();

-- 验证数据量
SELECT COUNT(*) FROM ods_page_visit_log;