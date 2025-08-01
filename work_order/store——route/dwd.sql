USE test2;

-- DWD层：页面访问明细事实表（不使用代理键）
DROP TABLE IF EXISTS dwd_page_visit_detail;
CREATE TABLE dwd_page_visit_detail (
    `log_id` BIGINT PRIMARY KEY COMMENT '日志ID（关联ODS层）',
    `user_id` VARCHAR(50) NOT NULL COMMENT '用户ID（直接使用ODS层业务键）',
    `device_type` ENUM('wireless', 'pc') NOT NULL COMMENT '设备类型（直接使用ODS层业务键）',
    `page_id` VARCHAR(50) NOT NULL COMMENT '页面ID（直接使用ODS层业务键）',
    `page_name` VARCHAR(100) NOT NULL COMMENT '页面名称（来自维度表）',
    `page_type` ENUM('store_page', 'item_detail', 'other_page') NOT NULL COMMENT '页面类型',
    `sub_type` VARCHAR(50) NOT NULL COMMENT '页面子类型',
    `type_desc` VARCHAR(100) COMMENT '页面类型描述（来自字典表）',
    `visit_time` DATETIME NOT NULL COMMENT '访问时间',
    `visit_date` DATE GENERATED ALWAYS AS (DATE(visit_time)) STORED COMMENT '访问日期',
    `refer_page_id` VARCHAR(50) COMMENT '来源页面ID',
    `stay_duration` INT NOT NULL COMMENT '停留时长（秒）',
    `is_entry` TINYINT NOT NULL COMMENT '是否进店页（1-是，0-否）',
    `visitor_count` INT NOT NULL DEFAULT 1 COMMENT '访客数',
    `order_buyer_num` INT DEFAULT 0 COMMENT '下单买家数',
    `ods_dt` INT NOT NULL COMMENT '分区字段（yyyyMMdd，来自ODS层）',
    -- 针对常用查询字段建索引
    KEY `idx_user_id` (`user_id`),
    KEY `idx_page_id` (`page_id`),
    KEY `idx_device_type` (`device_type`),
    KEY `idx_ods_dt` (`ods_dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWD层-页面访问明细事实表（无代理键）';

-- 从ODS层关联维度信息，直接使用业务键（不通过代理键关联）
INSERT INTO dwd_page_visit_detail (
    log_id, user_id, device_type, page_id,
    page_name, page_type, sub_type, type_desc,
    visit_time, refer_page_id, stay_duration,
    is_entry, visitor_count, order_buyer_num, ods_dt
)
SELECT
    opvl.log_id,
    opvl.user_id,  -- 直接使用ODS层user_id
    opvl.device_type,  -- 直接使用ODS层device_type
    opvl.page_id,  -- 直接使用ODS层page_id
    opvl.page_name,  -- 页面名称来自ODS层
    opvl.page_type,
    opvl.sub_type,
    optd.type_desc,  -- 类型描述来自字典表
    opvl.visit_time,
    -- 清洗来源页空值
    IF(opvl.refer_page_id IS NULL, 'N/A', opvl.refer_page_id),
    -- 清洗停留时长（确保非负）
    IF(opvl.stay_duration > 0, opvl.stay_duration, 0),
    opvl.is_entry,
    opvl.visitor_count,
    -- 清洗下单数（确保非负）
    IF(opvl.order_buyer_num >= 0, opvl.order_buyer_num, 0),
    opvl.ods_dt
FROM
    ods_page_visit_log opvl
-- 直接关联字典表补充维度描述（不通过DIM层代理键）
        JOIN ods_page_type_dict optd
             ON opvl.page_type = optd.page_type
                 AND opvl.sub_type = optd.sub_type
-- 过滤异常数据
WHERE
        opvl.stay_duration >= 0
  AND opvl.log_id IS NOT NULL;

-- 验证数据
SELECT * FROM dwd_page_visit_detail;