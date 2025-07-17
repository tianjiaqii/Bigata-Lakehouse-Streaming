from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.client.socket.timeout", "300") \
    .config("spark.network.timeout", "600s") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
spark.sql("use gmall;")
spark.sql("show tables in gmall").show(100)

print(spark.conf.get("hive.metastore.uris"))
print(spark.conf.get("spark.sql.warehouse.dir"))


#交易域加购事务事实表
spark.sql(f'''
insert overwrite table dwd_trade_cart_add_inc partition (ds)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    date_format(create_time, 'yyyy-MM-dd')
from ods_cart_info
    where ds = '20250629';
''').show()
spark.sql('select * from dwd_trade_cart_add_inc limit 10').show()
#交易域下单事务事实表
spark.sql(f'''
insert overwrite table dwd_trade_order_detail_inc partition (ds)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        id,
        order_id,
        sku_id,
        create_time,
        sku_num,
        sku_num * order_price split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    from ods_order_detail
    where ds = '20250629'
) od
left join
(
    select
        id,
        user_id,
        province_id
    from ods_order_info
    where ds = '20250629'
) oi
on od.order_id = oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from ods_order_detail_activity
    where ds = '20250629'
) act
on od.id = act.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from ods_order_detail_coupon
    where ds = '20250629'
) cou
on od.id = cou.order_detail_id;
''')
spark.sql('select * from dwd_trade_order_detail_inc limit 10').show()
#交易域支付成功事务事实表
spark.sql(f'''
insert overwrite table dwd_trade_pay_detail_suc_inc partition (ds)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
(
    select
        id,
        order_id,
        sku_id,
        sku_num,
        sku_num * order_price split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    from ods_order_detail
    where ds = '20250629'
) od
join
(
    select
        user_id,
        order_id,
        payment_type,
        callback_time
    from ods_payment_info
    where ds='20250629'
    and payment_status='1602'
) pi
on od.order_id=pi.order_id
left join
(
    select
        id,
        province_id
    from ods_order_info
    where ds = '20250629'
) oi
on od.order_id = oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from ods_order_detail_activity
    where ds = '20250629'
) act
on od.id = act.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from ods_order_detail_coupon
    where ds = '20250629'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic
    where ds='20211214'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code;
''')
spark.sql('select * from dwd_trade_pay_detail_suc_inc limit 10').show()
#交易域购物车周期快照事实表
spark.sql(f'''
insert overwrite table dwd_trade_cart_full partition(ds='20250629')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='20250629'
and is_ordered='0';
''')
spark.sql('select * from dwd_trade_cart_full limit 10').show()
#交易域交易流程累积快照事实表
spark.sql(f'''
insert overwrite table dwd_trade_trade_flow_acc partition(ds)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31')
from
(
    select
        id,
        user_id,
        province_id,
        create_time,
        original_total_amount,
        activity_reduce_amount,
        coupon_reduce_amount,
        total_amount
    from ods_order_info
    where ds='20250629'
)oi
left join
(
    select
        order_id,
        callback_time,
        total_amount payment_amount
    from ods_payment_info
    where ds='20250629'
    and payment_status='1602'
)pi
on oi.id=pi.order_id
left join
(
    select
        order_id,
        create_time finish_time
    from ods_order_status_log
    where ds='20250629'
    and order_status='1004'
)log
on oi.id=log.order_id;
''')
spark.sql('select * from dwd_trade_trade_flow_acc limit 10').show()
#工具域优惠券使用(支付)事务事实表
spark.sql(f'''
insert overwrite table dwd_tool_coupon_used_inc partition(ds)
select
    id,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time,
    date_format(used_time,'yyyy-MM-dd')
from ods_coupon_use
where ds='20250629'
and used_time is not null;
''')
spark.sql('select * from dwd_tool_coupon_used_inc limit 10').show()
#互动域收藏商品事务事实表
spark.sql(f'''
insert overwrite table dwd_interaction_favor_add_inc partition(ds)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from ods_favor_info
where ds='20250629';
''')
spark.sql('select * from dwd_interaction_favor_add_inc limit 10').show()
#流量域页面浏览事务事实表
spark.sql(f'''
insert into table dwd_traffic_page_view_inc partition(ds='20250629')
SELECT
    get_json_object(log, '$.common.ar') AS province_id,
    get_json_object(log, '$.common.ba') AS brand,
    get_json_object(log, '$.common.ch') AS channel,
    get_json_object(log, '$.common.is_new') AS is_new,
    get_json_object(log, '$.common.md') AS model,
    get_json_object(log, '$.common.mid') AS mid_id,
    get_json_object(log, '$.common.os') AS operate_system,
    get_json_object(log, '$.common.uid') AS user_id,
    get_json_object(log, '$.common.vc') AS version_code,
    get_json_object(log, '$.page.item') AS page_item,
    get_json_object(log, '$.page.item_type') AS page_item_type,
    get_json_object(log, '$.page.last_page_id') AS last_page_id,
    get_json_object(log, '$.page.page_id') AS page_id,
    get_json_object(log, '$.page.from_pos_id') AS from_pos_id,
    get_json_object(log, '$.page.from_pos_seq') AS from_pos_seq,
    get_json_object(log, '$.page.refer_id') AS refer_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), 'GMT+8'), 'yyyy-MM-dd') AS date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    get_json_object(log, '$.common.sid') AS session_id,
    get_json_object(log, '$.page.during_time') AS during_time
FROM ods_z_log
WHERE ds = '20250629'
  AND get_json_object(log, '$.page') IS NOT NULL;
''')
spark.sql('select * from dwd_traffic_page_view_inc limit 10').show()
#用户域用户注册事务事实表
spark.sql(f'''
insert overwrite table dwd_user_register_inc partition(ds)
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        id user_id,
        create_time
    from ods_user_info
    where ds='20220608'
)ui
left join
(
    select
        get_json_object(log, '$.common.ar') AS province_id,
        get_json_object(log, '$.common.ba') AS brand,
        get_json_object(log, '$.common.ch') As channel,
        get_json_object(log, '$.common.md') AS model,
        get_json_object(log, '$.common.mid') AS mid_id,
        get_json_object(log, '$.common.os') AS operate_system,
        get_json_object(log, '$.common.uid') AS user_id,
        get_json_object(log, '$.common.vc') AS version_code
    from ods_z_log
    where ds='20250629'
    and get_json_object(log, '$.page.page_id')='register'
    and get_json_object(log, '$.common.uid') is not null
)log
on ui.user_id=log.user_id;
''')
spark.sql('select * from dim_user_zip limit 10').show()
#用户域用户登录事务事实表
#设置动态分区模式
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict;')
spark.sql(f'''
insert overwrite table dwd_user_login_inc partition (ds = '20250629')
select
    user_id,
    from_unixtime(cast(ts/1000 as bigint), 'yyyy-MM-dd') as date_id,
    from_unixtime(cast(ts/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
         select
             user_id,
             channel,
             province_id,
             version_code,
             mid_id,
             brand,
             model,
             operate_system,
             ts
         from (
                  select
                      get_json_object(log, '$.common.uid') as user_id,
                      get_json_object(log, '$.common.ch') as channel,
                      get_json_object(log, '$.common.ar') as province_id,
                      get_json_object(log, '$.common.vc') as version_code,
                      get_json_object(log, '$.common.mid') as mid_id,
                      get_json_object(log, '$.common.ba') as brand,
                      get_json_object(log, '$.common.md') as model,
                      get_json_object(log, '$.common.os') as operate_system,
                      get_json_object(log, '$.ts') as ts,
                      row_number() over (partition by get_json_object(log, '$.common.sid') order by get_json_object(log, '$.ts')) as rn
                  from ods_z_log
                  where ds = '20250629'
                    and get_json_object(log, '$.page') is not null
                    and get_json_object(log, '$.common.uid') is not null
              ) t1
         where rn = 1
     ) t2;
''')
spark.sql('select * from dwd_user_login_inc limit 10').show()

