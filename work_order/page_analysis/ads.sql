
-- 店铺维度指标表（含访问时间窗口指标）
DROP TABLE IF EXISTS ads_store_indicators;
CREATE TABLE ads_store_indicators (
    `indicator_date` DATE NOT NULL COMMENT '指标计算日期',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(50) NOT NULL COMMENT '店铺名称',
    -- 总累计指标
    `total_click_cnt` INT DEFAULT 0 COMMENT '总点击量',
    `total_click_uv` INT DEFAULT 0 COMMENT '总点击人数',
    `total_visit_cnt` INT DEFAULT 0 COMMENT '总访问次数',
    `total_visit_uv` INT DEFAULT 0 COMMENT '总访问人数',
    `total_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '总引导支付金额',
    -- 1天指标（2025-07-29）
    `day1_click_cnt` INT DEFAULT 0 COMMENT '1天点击量',
    `day1_click_uv` INT DEFAULT 0 COMMENT '1天点击人数',
    `day1_visit_cnt` INT DEFAULT 0 COMMENT '1天访问次数',  -- 新增
    `day1_visit_uv` INT DEFAULT 0 COMMENT '1天访问人数',   -- 新增
    `day1_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '1天引导支付金额',
    -- 7天指标（2025-07-23至2025-07-29）
    `day7_click_cnt` INT DEFAULT 0 COMMENT '7天点击量',
    `day7_click_uv` INT DEFAULT 0 COMMENT '7天点击人数',
    `day7_visit_cnt` INT DEFAULT 0 COMMENT '7天访问次数',   -- 新增
    `day7_visit_uv` INT DEFAULT 0 COMMENT '7天访问人数',    -- 新增
    `day7_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '7天引导支付金额',
    -- 30天指标（2025-07-01至2025-07-29）
    `day30_click_cnt` INT DEFAULT 0 COMMENT '30天点击量',
    `day30_click_uv` INT DEFAULT 0 COMMENT '30天点击人数',
    `day30_visit_cnt` INT DEFAULT 0 COMMENT '30天访问次数',  -- 新增
    `day30_visit_uv` INT DEFAULT 0 COMMENT '30天访问人数',   -- 新增
    `day30_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '30天引导支付金额',
    PRIMARY KEY (`indicator_date`, `store_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层-店铺维度指标汇总（含访问时间窗口）';

-- 计算店铺指标（补充访问时间窗口逻辑）
INSERT INTO ads_store_indicators
SELECT
    '2025-07-29' AS indicator_date,
    s.store_id,
    s.store_name,
    -- 总累计指标
    SUM(s.total_click_cnt) AS total_click_cnt,
    SUM(s.total_click_uv) AS total_click_uv,
    SUM(s.visit_cnt) AS total_visit_cnt,
    SUM(s.visit_uv) AS total_visit_uv,
    SUM(s.guide_pay_amt) AS total_guide_pay_amt,
    -- 1天指标（2025-07-29）
    SUM(CASE WHEN s.stat_date = '2025-07-29' THEN s.total_click_cnt ELSE 0 END) AS day1_click_cnt,
    SUM(CASE WHEN s.stat_date = '2025-07-29' THEN s.total_click_uv ELSE 0 END) AS day1_click_uv,
    SUM(CASE WHEN s.stat_date = '2025-07-29' THEN s.visit_cnt ELSE 0 END) AS day1_visit_cnt,  -- 新增
    SUM(CASE WHEN s.stat_date = '2025-07-29' THEN s.visit_uv ELSE 0 END) AS day1_visit_uv,   -- 新增
    SUM(CASE WHEN s.stat_date = '2025-07-29' THEN s.guide_pay_amt ELSE 0 END) AS day1_guide_pay_amt,
    -- 7天指标（2025-07-23至2025-07-29）
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN s.total_click_cnt ELSE 0 END) AS day7_click_cnt,
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN s.total_click_uv ELSE 0 END) AS day7_click_uv,
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN s.visit_cnt ELSE 0 END) AS day7_visit_cnt,   -- 新增
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN s.visit_uv ELSE 0 END) AS day7_visit_uv,    -- 新增
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN s.guide_pay_amt ELSE 0 END) AS day7_guide_pay_amt,
    -- 30天指标（2025-07-01至2025-07-29）
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN s.total_click_cnt ELSE 0 END) AS day30_click_cnt,
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN s.total_click_uv ELSE 0 END) AS day30_click_uv,
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN s.visit_cnt ELSE 0 END) AS day30_visit_cnt,  -- 新增
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN s.visit_uv ELSE 0 END) AS day30_visit_uv,   -- 新增
    SUM(CASE WHEN s.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN s.guide_pay_amt ELSE 0 END) AS day30_guide_pay_amt
FROM dws_store_daily_summary s
GROUP BY s.store_id, s.store_name;

select * from ads_store_indicators;




-- 商品维度指标表（含访问时间窗口指标）
DROP TABLE IF EXISTS ads_item_indicators;
CREATE TABLE ads_item_indicators (
    `indicator_date` DATE NOT NULL COMMENT '指标计算日期',
    `store_id` VARCHAR(20) NOT NULL COMMENT '店铺ID',
     store_name VARCHAR(50) NOT NULL COMMENT '店铺名称',
    `item_id` VARCHAR(50) NOT NULL COMMENT '商品ID',
    `item_name` VARCHAR(100) NOT NULL COMMENT '商品名称',
    -- 总累计指标
    `total_click_cnt` INT DEFAULT 0 COMMENT '总点击量',
    `total_click_uv` INT DEFAULT 0 COMMENT '总点击人数',
    `total_visit_cnt` INT DEFAULT 0 COMMENT '总访问次数',
    `total_visit_uv` INT DEFAULT 0 COMMENT '总访问人数',
    `total_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '总引导支付金额',
    -- 占比指标（相对于店铺）
    `click_ratio` DECIMAL(5,2) DEFAULT 0.00 COMMENT '点击量占店铺总点击比(%)',
    `visit_ratio` DECIMAL(5,2) DEFAULT 0.00 COMMENT '访问量占店铺总访问比(%)',
    `pay_ratio` DECIMAL(5,2) DEFAULT 0.00 COMMENT '支付金额占店铺总支付比(%)',
    -- 1天指标（2025-07-29）
    `day1_click_cnt` INT DEFAULT 0 COMMENT '1天点击量',
    `day1_click_uv` INT DEFAULT 0 COMMENT '1天点击人数',
    `day1_visit_cnt` INT DEFAULT 0 COMMENT '1天访问次数',  -- 新增
    `day1_visit_uv` INT DEFAULT 0 COMMENT '1天访问人数',   -- 新增
    `day1_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '1天引导支付金额',
    -- 7天指标（2025-07-23至2025-07-29）
    `day7_click_cnt` INT DEFAULT 0 COMMENT '7天点击量',
    `day7_click_uv` INT DEFAULT 0 COMMENT '7天点击人数',
    `day7_visit_cnt` INT DEFAULT 0 COMMENT '7天访问次数',   -- 新增
    `day7_visit_uv` INT DEFAULT 0 COMMENT '7天访问人数',    -- 新增
    `day7_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '7天引导支付金额',
    -- 30天指标（2025-07-01至2025-07-29）
    `day30_click_cnt` INT DEFAULT 0 COMMENT '30天点击量',
    `day30_click_uv` INT DEFAULT 0 COMMENT '30天点击人数',
    `day30_visit_cnt` INT DEFAULT 0 COMMENT '30天访问次数',  -- 新增
    `day30_visit_uv` INT DEFAULT 0 COMMENT '30天访问人数',   -- 新增
    `day30_guide_pay_amt` DECIMAL(16,2) DEFAULT 0.00 COMMENT '30天引导支付金额',
    PRIMARY KEY (`indicator_date`, `store_id`, `item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层-商品维度指标汇总（含访问时间窗口）';

-- 计算商品指标（补充访问时间窗口逻辑）
INSERT INTO ads_item_indicators
SELECT
    '2025-07-29' AS indicator_date,
    i.store_id,
    i.store_name ,
    i.item_id,
    i.item_name,
    -- 商品总累计指标
    SUM(i.click_cnt) AS total_click_cnt,
    SUM(i.click_uv) AS total_click_uv,
    SUM(i.visit_cnt) AS total_visit_cnt,
    SUM(i.visit_uv) AS total_visit_uv,
    SUM(i.guide_pay_amt) AS total_guide_pay_amt,
    -- 占比计算（商品/店铺）
    ROUND(SUM(i.click_cnt)/NULLIF(SUM(s.total_click_cnt),0)*100,2) AS click_ratio,
    ROUND(SUM(i.visit_cnt)/NULLIF(SUM(s.visit_cnt),0)*100,2) AS visit_ratio,
    ROUND(SUM(i.guide_pay_amt)/NULLIF(SUM(s.guide_pay_amt),0)*100,2) AS pay_ratio,
    -- 1天指标（2025-07-29）
    SUM(CASE WHEN i.stat_date = '2025-07-29' THEN i.click_cnt ELSE 0 END) AS day1_click_cnt,
    SUM(CASE WHEN i.stat_date = '2025-07-29' THEN i.click_uv ELSE 0 END) AS day1_click_uv,
    SUM(CASE WHEN i.stat_date = '2025-07-29' THEN i.visit_cnt ELSE 0 END) AS day1_visit_cnt,  -- 新增
    SUM(CASE WHEN i.stat_date = '2025-07-29' THEN i.visit_uv ELSE 0 END) AS day1_visit_uv,   -- 新增
    SUM(CASE WHEN i.stat_date = '2025-07-29' THEN i.guide_pay_amt ELSE 0 END) AS day1_guide_pay_amt,
    -- 7天指标（2025-07-23至2025-07-29）
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN i.click_cnt ELSE 0 END) AS day7_click_cnt,
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN i.click_uv ELSE 0 END) AS day7_click_uv,
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN i.visit_cnt ELSE 0 END) AS day7_visit_cnt,   -- 新增
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN i.visit_uv ELSE 0 END) AS day7_visit_uv,    -- 新增
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-23' AND '2025-07-29' THEN i.guide_pay_amt ELSE 0 END) AS day7_guide_pay_amt,
    -- 30天指标（2025-07-01至2025-07-29）
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN i.click_cnt ELSE 0 END) AS day30_click_cnt,
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN i.click_uv ELSE 0 END) AS day30_click_uv,
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN i.visit_cnt ELSE 0 END) AS day30_visit_cnt,  -- 新增
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN i.visit_uv ELSE 0 END) AS day30_visit_uv,   -- 新增
    SUM(CASE WHEN i.stat_date BETWEEN '2025-07-01' AND '2025-07-29' THEN i.guide_pay_amt ELSE 0 END) AS day30_guide_pay_amt
FROM dws_item_daily_summary i
-- 关联店铺表获取店铺总指标用于计算占比
         LEFT JOIN dws_store_daily_summary s
                   ON i.store_id = s.store_id
                       AND i.stat_date = s.stat_date
GROUP BY i.store_id,i.store_name, i.item_id, i.item_name;

select * from ads_item_indicators;







