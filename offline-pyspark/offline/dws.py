from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
spark.sql("use gmall;")
spark.sql("show tables in gmall").show(100)



spark.sql('set hive.vectorized.execution.enabled = false;')
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict;')


#交易域用户商品粒度订单最近1日汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_sku_order_1d partition(ds)
select
    user_id,
    id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    ds
from
(
    select
        ds,
        user_id,
        sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    group by ds,user_id,sku_id
)od
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where ds='20250626'
)sku
on od.sku_id=sku.id;
''').show()
spark.sql('select * from dws_trade_user_sku_order_1d limit 10').show()



#交易域用户粒度订单最近1日汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_order_1d partition(ds)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    ds
from dwd_trade_order_detail_inc
group by user_id,ds;
''').show()
spark.sql('select * from dws_trade_user_order_1d limit 10')



#交易域用户粒度加购最近1日汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_cart_add_1d partition(ds)
select
    user_id,
    count(*),
    sum(sku_num),
    ds
from dwd_trade_cart_add_inc
group by user_id,ds;
''').show()
spark.sql('select * from dws_trade_user_cart_add_1d limit 10').show()


#交易域用户粒度支付最近1日汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_payment_1d partition(ds)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    ds
from dwd_trade_pay_detail_suc_inc
group by user_id,ds;
''').show()
spark.sql('select * from dws_trade_user_payment_1d limit 10').show()



#交易域省份粒度订单最近1日汇总表
spark.sql(f'''
insert overwrite table dws_trade_province_order_1d partition(ds)
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    ds
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d,
        ds
    from dwd_trade_order_detail_inc
    group by province_id,ds
)o
left join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_full
    where ds='20250627'
)p
on o.province_id=p.id;
''').show()
spark.sql('select * from dws_trade_province_order_1d limit 10').show()



#工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表
spark.sql(f'''
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(ds)
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count,
    ds
from
(
    select
        ds,
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    group by ds,user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where ds='20250627'
)t2
on t1.coupon_id=t2.id;
''').show()
spark.sql('select * from dws_tool_user_coupon_coupon_used_1d limit 10').show()



#互动域商品粒度收藏商品最近1日汇总表
spark.sql(f'''
insert overwrite table dws_interaction_sku_favor_add_1d partition(ds)
select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count,
    ds
from
(
    select
        ds,
        sku_id,
        count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    group by ds,sku_id
)favor
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where ds='20250626'
)sku
on favor.sku_id=sku.id;
''').show()

spark.sql('select * from dws_interaction_sku_favor_add_1d limit 10').show()



#流量域会话粒度页面浏览最近1日汇总表
spark.sql(f'''
insert overwrite table dws_traffic_session_page_view_1d partition(ds='20250629')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where ds='20250629'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
''').show()
spark.sql('select * from dws_traffic_session_page_view_1d limit 10').show()



#流量域访客页面粒度页面浏览最近1日汇总表
spark.sql(f'''
insert overwrite table dws_traffic_page_visitor_page_view_1d partition(ds='20250629')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view_inc
where ds='20250629'
group by mid_id,brand,model,operate_system,page_id;
''').show()
spark.sql('select * from dws_traffic_page_visitor_page_view_1d limit 10').show()



#交易域用户商品粒度订单最近n日汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_sku_order_nd partition(ds='2025-06-29')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(ds>=date_add('2025-06-29',-6),order_count_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),order_num_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where ds>=date_add('2025-06-29',-29)
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
''').show()
spark.sql('select * from dws_trade_user_sku_order_nd limit 10')



#交易域省份粒度订单最近n日汇总表
spark.sql(f'''
insert overwrite table dws_trade_province_order_nd partition(ds='2025-06-29')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(ds>=date_add('2025-06-29',-6),order_count_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-29',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where ds>=date_add('2025-06-29',-29)
and ds<='2025-06-29'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
''').show()
spark.sql('select * from dws_trade_province_order_nd limit 10').show()



#交易域用户粒度订单历史至今汇总表
spark.sql(f'''
insert overwrite table dws_trade_user_order_td partition(ds='2025-06-29')
select
    user_id,
    min(ds) order_date_first,
    max(ds) order_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws_trade_user_order_1d
group by user_id;
''').show()
spark.sql('select * from dws_trade_user_order_td limit 10').show()


#用户域用户粒度登录历史至今汇总表
spark.sql(f'''
insert overwrite table dws_user_user_login_td partition (ds = '20250629')
select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
         select id,
                create_time
         from dim_user_zip
         where ds = '20250627'
     ) u
         left join
     (
         select user_id,
                max(ds)  login_date_last,
                count(*) login_count_td
         from dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;
''').show()

spark.sql('select * from dws_user_user_login_td limit 10').show()
