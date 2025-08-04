USE test2;

-- DIM层：页面维度表（整合页面基础信息 + 字典表维度）
DROP TABLE IF EXISTS dim_page;
CREATE TABLE dim_page (
    `page_sk` BIGINT AUTO_INCREMENT COMMENT '页面维度代理键',
    `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID（ODS层关联键）',
    `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称',
    `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型',
    `sub_type` VARCHAR(50) NOT NULL COMMENT '页面子类型',
    `type_desc` VARCHAR(100) COMMENT '类型描述（来自字典表）',
    `is_valid` TINYINT DEFAULT 1 COMMENT '是否有效（1:有效 0:无效）',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '维度创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '维度更新时间',
    PRIMARY KEY (`page_sk`),
    UNIQUE KEY `idx_page_id` (`page_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DIM层-页面维度表';

-- 从ODS层 + 字典表抽取维度数据
INSERT INTO dim_page (page_id, page_name, page_type, sub_type, type_desc)
SELECT
    DISTINCT opvl.page_id,
             opvl.page_name,
             opvl.page_type,
             opvl.sub_type,
             optd.type_desc
FROM
    ods_page_visit_log opvl
        JOIN
    ods_page_type_dict optd
    ON
                opvl.page_type = optd.page_type
            AND opvl.sub_type = optd.sub_type;


select * from dim_page;




-- DIM层：用户维度表（基础用户信息，可扩展更多属性）
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    `user_sk` BIGINT AUTO_INCREMENT COMMENT '用户维度代理键',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID（ODS层关联键）',
    `is_new_user` TINYINT COMMENT '是否新用户（可基于业务逻辑扩展）',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '维度创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '维度更新时间',
    PRIMARY KEY (`user_sk`),
    UNIQUE KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DIM层-用户维度表';

-- 从ODS层抽取用户基础信息（可扩展更多字段）
INSERT INTO dim_user (user_id, is_new_user)
SELECT
    DISTINCT user_id,
             -- 增加长度判断，避免SUBSTRING截取内容不合法
             CASE
                 WHEN LENGTH(user_id) >= 6
                     AND CAST(SUBSTRING(user_id, 6) AS UNSIGNED) < 100
                     THEN 1
                 ELSE 0
                 END
FROM
    ods_page_visit_log;


select * from dim_user;





-- DIM层：设备维度表（区分无线/PC设备）
DROP TABLE IF EXISTS dim_device;
CREATE TABLE dim_device (
    `device_sk` BIGINT AUTO_INCREMENT COMMENT '设备维度代理键',
    `device_type` ENUM('wireless', 'pc') NOT NULL COMMENT '设备类型（ODS层关联键）',
    `device_desc` VARCHAR(50) COMMENT '设备描述（如：移动端、PC端）',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '维度创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '维度更新时间',
    PRIMARY KEY (`device_sk`),
    UNIQUE KEY `idx_device_type` (`device_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DIM层-设备维度表';

-- 初始化设备维度（枚举值固定，可直接插入）
INSERT INTO dim_device (device_type, device_desc)
VALUES
    ('wireless', '无线端（移动端）'),
    ('pc', 'PC端');

select * from dim_device;