create database tms01;
use tms01;
--小区维度表
drop table if exists dim_complex_full;
create external table dim_complex_full(
    `id` bigint comment '小区ID',
    `complex_name` string comment '小区名称',
    `courier_emp_ids` array<string> comment '负责快递员IDS',
    `province_id` bigint comment '省份ID',
    `province_name` string comment '省份名称',
    `city_id` bigint comment '城市ID',
    `city_name` string comment '城市名称',
    `district_id` bigint comment '区（县）ID',
    `district_name` string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_complex_full'
    tblproperties('orc.compress'='snappy');

with cx as (
    select id,
           complex_name,
           province_id,
           city_id,
           district_id,
           district_name
    from ods_base_complex where ds='20250717' and is_deleted='0'
),pv as (
    select id,
           name
    from ods_base_region_info where ds='20200623' and is_deleted='0'
),cy as (
    select id,
           name
    from ods_base_region_info where ds='20200623' and is_deleted='0'
),ex as (
    select
        collect_set(cast(courier_emp_id as string)) courier_emp_ids,
        complex_id
    from ods_express_courier_complex where ds='20250717' and is_deleted='0'
    group by complex_id
)

insert overwrite table dim_complex_full partition (ds='20250717')
select
    cx.id,
    complex_name,
    courier_emp_ids,
    province_id,
    pv.name,
    city_id,
    cy.name,
    district_id,
    district_name
from cx left join pv
on cx.province_id=pv.id
left join cy
on cx.city_id = cy.id
left join ex
on cx.id = ex.complex_id;

select * from dim_complex_full;



--机构维度表
drop table if exists dim_organ_full;
create external table dim_organ_full(
        `id` bigint COMMENT '机构ID',
        `org_name` string COMMENT '机构名称',
        `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
        `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
        `region_name` string COMMENT '地区名称',
        `region_code` string COMMENT '地区编码（行政级别）',
        `org_parent_id` bigint COMMENT '父级机构ID',
        `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_organ_full'
    tblproperties('orc.compress'='snappy');

with og as(
    select id,
           org_name,
           org_level,
           region_id,
           org_parent_id
    from ods_base_organ where ds='20250717' and is_deleted='0'
),rg as (
    select  id,
            name,
            dict_code
    from ods_base_region_info where ds='20200623' and is_deleted='0'
)
insert overwrite table dim_organ_full partition (ds='20250717')
select
    a.id,
    a.org_name,
    a.org_level,
    a.region_id,
    rg.name,
    dict_code,
    a.org_parent_id,
    pog.org_name
from og a left join rg
on a.region_id=rg.id
left join og pog
on a.org_parent_id=pog.id;
select * from dim_organ_full;


--地区维度表
drop table if exists dim_region_full;
create external table dim_region_full(
    `id` bigint COMMENT '地区ID',
    `parent_id` bigint COMMENT '上级地区ID',
    `name` string COMMENT '地区名称',
    `dict_code` string COMMENT '编码（行政级别）',
    `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_region_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_region_full partition (ds='20250717')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info where ds='20200623' and is_deleted='0';

select * from dim_region_full;




--快递员维度表
drop table if exists dim_express_courier_full;
create external table dim_express_courier_full(
    `id` bigint COMMENT '快递员ID',
    `emp_id` bigint COMMENT '员工ID',
    `org_id` bigint COMMENT '所属机构ID',
    `org_name` string COMMENT '机构名称',
    `working_phone` string COMMENT '工作电话',
    `express_type` string COMMENT '快递员类型（收货；发货）',
    `express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_express_courier_full'
    tblproperties('orc.compress'='snappy');

with ex as (
    select id,
           emp_id,
           org_id,
           working_phone,
           express_type
    from ods_express_courier where ds='20240717' and is_deleted='0'
),rg as (
    select id,
           org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
),dc as (
    select id,
           name
    from ods_base_dic where ds='20220708' and is_deleted='0'
)
insert overwrite table dim_express_courier_full partition (ds='20250717')
select
    ex.id,
    emp_id,
    org_id,
    rg.org_name,
    working_phone,
    express_type,
    dc.name
from ex left join rg
on ex.org_id = rg.id
left join dc
on ex.express_type = dc.id;

select * from dim_express_courier_full;





--班次维度表
drop table if exists dim_shift_full;
create external table dim_shift_full(
    `id` bigint COMMENT '班次ID',
    `line_id` bigint COMMENT '线路ID',
    `line_name` string COMMENT '线路名称',
    `line_no` string COMMENT '线路编号',
    `line_level` string COMMENT '线路级别',
    `org_id` bigint COMMENT '所属机构',
    `transport_line_type_id` string COMMENT '线路类型ID',
    `transport_line_type_name` string COMMENT '线路类型名称',
    `start_org_id` bigint COMMENT '起始机构ID',
    `start_org_name` string COMMENT '起始机构名称',
    `end_org_id` bigint COMMENT '目标机构ID',
    `end_org_name` string COMMENT '目标机构名称',
    `pair_line_id` bigint COMMENT '配对线路ID',
    `distance` decimal(10,2) COMMENT '直线距离',
    `cost` decimal(10,2) COMMENT '公路里程',
    `estimated_time` bigint COMMENT '预计时间（分钟）',
    `start_time` string COMMENT '班次开始时间',
    `driver1_emp_id` bigint COMMENT '第一司机',
    `driver2_emp_id` bigint COMMENT '第二司机',
    `truck_id` bigint COMMENT '卡车ID',
    `pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)'
) comment '班次维度表'
    partitioned by (`ds` string comment '统计周期')
    stored as orc
    location '/warehouse/tms/dim/dim_shift_full'
    tblproperties('orc.compress'='snappy');


with sf as (
    select id,
           line_id,
           start_time,
           driver1_emp_id,
           driver2_emp_id,
           truck_id,
           pair_shift_id
    from ods_line_base_shift where ds='20250717' and is_deleted='0'
),le as (
    select id,
           name,
           line_no,
           line_level,
           org_id,
           transport_line_type_id,
           end_org_id,
           end_org_name,
           start_org_id,
           start_org_name,
           pair_line_id,
           distance,
           cost,
           estimated_time,
           status
    from ods_line_base_info where ds='20250717' and is_deleted='0'
),bc as (
    select id,
           name
    from ods_base_dic where ds='20220708' and is_deleted='0'
)
insert overwrite table dim_shift_full partition (ds='20250717')
select
    sf.id,
    line_id,
    le.name,
    line_no,
    line_level,
    org_id,
    transport_line_type_id,
    bc.name,
    start_org_id,
    start_org_name,
    end_org_id,
    end_org_name,
    pair_line_id,
    distance,
    cost,
    estimated_time,
    start_time,
    driver1_emp_id,
    driver2_emp_id,truck_id,
    pair_shift_id
from sf left join le
on sf.line_id = le.id
left join bc
on le.transport_line_type_id = bc.id;



select * from dim_shift_full;



--司机维度表
drop table if exists dim_truck_driver_full;
create external table dim_truck_driver_full(
    `id` bigint COMMENT '司机信息ID',
    `emp_id` bigint COMMENT '员工ID',
    `org_id` bigint COMMENT '所属机构ID',
    `org_name` string COMMENT '所属机构名称',
    `team_id` bigint COMMENT '所属车队ID',
    `tream_name` string COMMENT '所属车队名称',
    `license_type` string COMMENT '准驾车型',
    `init_license_date` string COMMENT '初次领证日期',
    `expire_date` string COMMENT '有效截止日期',
    `license_no` string COMMENT '驾驶证号',
    `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '司机维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');

with dv as (
    select id,
           emp_id,
           org_id,
           team_id,
           license_type,
           init_license_date,
           expire_date,
           license_no,
           license_picture_url,
           is_enabled
    from ods_truck_driver where ds='20250717' and is_deleted='0'
),og as (
    select id,
           org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
),tm as (
    select id,
           name
    from ods_truck_team where ds='20250717' and is_deleted='0'
)
insert overwrite table dim_truck_driver_full partition (ds='20250717')
select
    dv.id ,
    emp_id ,
    org_id ,
    org_name,
    team_id,
    tm.name,
    license_type,
    init_license_date,
    expire_date,
    license_no,
    is_enabled
from dv left join og
on dv.org_id=og.id
left join tm
on dv.team_id=tm.id;

select * from dim_truck_driver_full;




--卡车维度表
drop table if exists dim_truck_full;
create external table dim_truck_full(
    `id` bigint COMMENT '卡车ID',
    `team_id` bigint COMMENT '所属车队ID',
    `team_name` string COMMENT '所属车队名称',
    `team_no` string COMMENT '车队编号',
    `org_id` bigint COMMENT '所属机构',
    `org_name` string COMMENT '所属机构名称',
    `manager_emp_id` bigint COMMENT '负责人',
    `truck_no` string COMMENT '车牌号码',
    `truck_model_id` string COMMENT '型号',
    `truck_model_name` string COMMENT '型号名称',
    `truck_model_type` string COMMENT '型号类型',
    `truck_model_type_name` string COMMENT '型号类型名称',
    `truck_model_no` string COMMENT '型号编码',
    `truck_brand` string COMMENT '品牌',
    `truck_brand_name` string COMMENT '品牌名称',
    `truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
    `load_weight` decimal(16,2) COMMENT '额定载重（吨）',
    `total_weight` decimal(16,2) COMMENT '总质量（吨）',
    `eev` string COMMENT '排放标准',
    `boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
    `boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
    `boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
    `max_speed` bigint COMMENT '最高时速（千米/时）',
    `oil_vol` bigint COMMENT '油箱容积（升）',
    `device_gps_id` string COMMENT 'GPS设备ID',
    `engine_no` string COMMENT '发动机编码',
    `license_registration_date` string COMMENT '注册时间',
    `license_last_check_date` string COMMENT '最后年检日期',
    `license_expire_date` string COMMENT '失效日期',
    `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '卡车维度表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_full'
    tblproperties('orc.compress'='snappy');

with tk as (
    select id,
           team_id,
           truck_no,
           truck_model_id,
           device_gps_id,
           engine_no,
           license_registration_date,
           license_last_check_date,
           license_expire_date,
           picture_url,
           is_enabled
    from ods_truck_info where ds='20250717' and is_deleted='0'
),tm as (
    select id,
           name,
           team_no,
           org_id,
           manager_emp_id
    from ods_truck_team where ds='20250717' and is_deleted='0'
),og as (
    select id,
           org_name
    from ods_base_organ where ds='20250717' and is_deleted='0'
),bc as (
    select id,
           name
    from ods_base_dic where ds='20220708' and is_deleted='0'
),td as (
    select id,
           model_name,
           model_type,
           model_no,
           brand,
           truck_weight,
           load_weight,
           total_weight,
           eev,
           boxcar_len,
           boxcar_wd,
           boxcar_hg,
           max_speed,
           oil_vol
    from ods_truck_model where ds='20220618' and is_deleted='0'
)
insert overwrite table dim_truck_full partition (ds='20250717')
select
    tk.id,
    team_id,
    tm.name,
    team_no,
    org_id,
    org_name,
    manager_emp_id,
    truck_no,
    truck_model_id,
    td.model_name,
    model_type,
    mtp.name,
    model_no,
    brand,
    bd.name,
    truck_weight,
    load_weight,
    total_weight,
    eev,
    boxcar_len,
    boxcar_wd,
    boxcar_hg,
    max_speed,
    oil_vol,
    device_gps_id,
    engine_no,
    license_registration_date,
    license_last_check_date,
    license_expire_date,
    is_enabled
from tk left join  tm
on tk.team_id = tm.id
left join og
on tm.org_id = og.id
left join td
on tk.truck_model_id = td.id
left join bc mtp
on td.model_type = mtp.id
left join bc bd
on td.brand = bd.id;


select * from dim_truck_full;





--用户维度表
drop table if exists dim_user_zip;
create external table dim_user_zip(
    `id` bigint COMMENT '用户地址信息ID',
    `login_name` string COMMENT '用户名称',
    `nick_name` string COMMENT '用户昵称',
    `passwd` string COMMENT '用户密码',
    `real_name` string COMMENT '用户姓名',
    `phone_num` string COMMENT '手机号',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户级别',
    `birthday` string COMMENT '用户生日',
    `gender` string COMMENT '性别 M男,F女',
    `start_date` string COMMENT '起始日期',
    `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');


---首日数据装载---

insert overwrite table dim_user_zip partition (ds='9999-12-31')
select
    id,
    login_name,
    nick_name,
    passwd,
    real_name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    date_format(nvl(create_time,update_time),'yyyy-MM-dd'),
    '9999-12-31'
from ods_user_info where ds='20250717' and is_deleted='0';

select * from dim_user_zip;


---每日数据装载---
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (ds)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-17', -1)) end_date,
       if(rk = 1, end_date, date_add('2025-07-17', -1)) dt
from (select id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   login_name,
                   nick_name,
                   passwd,
                   real_name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   start_date,
                   end_date
            from dim_user_zip
            where ds = '9999-12-31'
            union
            select id,
                   login_name,
                   nick_name,
                   md5(passwd) passwd,
                   md5(real_name) realname,
                   md5(if(phone_num regexp
                          '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                          phone_num, null)) phone_num,
                   md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
                   user_level,
                   cast(date_add('1970-01-01', cast(birthday as int)) as string) birthday,
                   gender,
                   '2025-07-17' start_date,
                   '9999-12-31' end_date
            from (select id,
                         login_name,
                         nick_name,
                         passwd,
                         real_name,
                         phone_num,
                         email,
                         user_level,
                         birthday,
                         gender,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_info
                  where ds = '20250717'
                    and is_deleted = '0'
                 ) inc
            where rn = 1) full_info) final_info;


select * from dim_user_zip;




--用户地址维度表
drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
    `id` bigint COMMENT '地址ID',
    `user_id` bigint COMMENT '用户ID',
    `phone` string COMMENT '电话号',
    `province_id` bigint COMMENT '所属省份ID',
    `city_id` bigint COMMENT '所属城市ID',
    `district_id` bigint COMMENT '所属区县ID',
    `complex_id` bigint COMMENT '所属小区ID',
    `address` string COMMENT '详细地址',
    `is_default` tinyint COMMENT '是否默认',
    `start_date` string COMMENT '起始日期',
    `end_date` string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (`ds` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');


--首日数据装载
insert overwrite table dim_user_address_zip
    partition (ds = '9999-12-31')
select id,
       user_id,
       md5(if(phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone, null))               phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       concat(substr(create_time, 1, 10), ' ',
              substr(create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address
where ds = '20250717'
  and is_deleted = '0';


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_address_zip
    partition (ds)
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-17', -1)) end_date,
       if(rk = 1, end_date, date_add('2025-07-17', -1)) dt
from (select id,
             user_id,
             phone,
             province_id,
             city_id,
             district_id,
             complex_id,
             address,
             is_default,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   start_date,
                   end_date
            from dim_user_address_zip
            where ds = '9999-12-31'
            union
            select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   '2025-07-17' start_date,
                   '9999-12-31' end_date
            from (select id,
                         user_id,
                         phone,
                         province_id,
                         city_id,
                         district_id,
                         complex_id,
                         address,
                         cast(is_default as tinyint)                          is_default,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_address
                  where ds = '20250717'
                    and is_deleted = '0') inc
            where rn = 1
           ) union_info
     ) with_rk;


select * from dim_user_address_zip;