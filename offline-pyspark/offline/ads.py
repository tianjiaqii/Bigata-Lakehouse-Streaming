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


#各渠道流量统计
spark.sql(f'''
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select
    '20250629' ds,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where ds>=date_add('20250629',-recent_days+1)
group by recent_days,channel;
''').show()
spark.sql('select * from ads_traffic_stats_by_channel limit 10').show()



#路径分析
spark.sql(f'''
insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '20250629' ds,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from dwd_traffic_page_view_inc
        where ds='20250629'
    )t1
)t2
group by source,target;
''').show()
spark.sql('select * from ads_page_path limit 10').show()



#用户变动统计
spark.sql(f'''
insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.ds,
    user_churn_count,
    user_back_count
from
(
    select
        '20250629' ds,
        count(*) user_churn_count
    from dws_user_user_login_td
    where ds='20250629'
    and login_date_last=date_add('20250629',-7)
)churn
join
(
    select
        '20250629' ds,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where ds='20250629'
        and login_date_last = '20250629'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where ds=date_add('20250629',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.ds=back.ds;
''').show()
spark.sql('select * from ads_user_change limit 10').show()




#用户留存率
spark.sql(f'''
insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '2022-06-08' dt,
       login_date_first create_date,
       datediff('2022-06-08', login_date_first) retention_day,
       sum(if(login_date_last = '2022-06-08', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '2022-06-08', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where ds = '2022-06-08'
           and login_date_first >= date_add('2022-06-08', -7)
           and login_date_first < '2022-06-08'
     ) t1
group by login_date_first;
''').show()


#用户新增活跃统计
spark.sql(f'''
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '20250629' ds,
       recent_days,
       sum(if(login_date_first >= date_add('20250629', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where ds = '20250629'
  and login_date_last >= date_add('20250629', -recent_days + 1)
group by recent_days;
''').show()
spark.sql('select * from ads_user_stats limit 10').show()



#用户行为漏斗分析
spark.sql(f'''
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '20250629' ds,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    where ds='20250629'
    and page_id in ('home','good_detail')
)page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from dws_trade_user_cart_add_1d
    where ds='2025-06-29'
)cart
on page.recent_days=cart.recent_days
join
(
    select
        1 recent_days,
        count(*) order_count
    from dws_trade_user_order_1d
    where ds='2025-06-29'
)ord
on page.recent_days=ord.recent_days
join
(
    select
        1 recent_days,
        count(*) payment_count
    from dws_trade_user_payment_1d
    where ds='2025-06-29'
)pay
on page.recent_days=pay.recent_days;
''').show()
spark.sql('select * from ads_user_action limit 10').show()



#新增下单用户统计
spark.sql(f'''
insert overwrite table ads_new_order_user_stats
select * from ads_new_order_user_stats
union
select
    '2025-06-29' ds,
    recent_days,
    count(*) new_order_user_count
from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where ds='2025-06-29'
and order_date_first>=date_add('2025-06-29',-recent_days+1)
group by recent_days;
''').show()
spark.sql('select * from ads_new_order_user_stats limit 10').show()



#最近7日内连续3日下单用户数
spark.sql(f'''
insert overwrite table ads_order_continuously_user_count
select * from ads_order_continuously_user_count
union
select
    '2025-06-29',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(ds,2,'9999-12-31') over(partition by user_id order by ds),ds) diff
    from dws_trade_user_order_1d
    where ds>=date_add('2025-06-29',-6)
)t1
where diff=2;
''').show()
spark.sql('select * from ads_order_continuously_user_count limit 10').show()



#最近30日各品牌复购率
spark.sql(f'''
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '2025-06-29',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from dws_trade_user_sku_order_nd
    where ds='2025-06-29'
    group by user_id, tm_id,tm_name
)t1
group by tm_id,tm_name;
''').show()
spark.sql('select * from ads_repeat_purchase_by_tm limit 10').show()


#各品牌商品下单统计
spark.sql(f'''
insert overwrite table ads_order_stats_by_tm
select * from ads_order_stats_by_tm
union
select
    '2025-06-29' ds,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where ds='2025-06-29'
    group by tm_id,tm_name
    union all
    select
        recent_days,
        tm_id,
        tm_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where ds='2025-06-29'
    )t1
    group by recent_days,tm_id,tm_name
)odr;
''').show()
spark.sql('select * from ads_order_stats_by_tm limit 10').show()


#各品类商品下单统计
spark.sql(f'''
insert overwrite table ads_order_stats_by_cate
select * from ads_order_stats_by_cate
union
select
    '2025-06-29' ds,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where ds='2025-06-29'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where ds='2025-06-29'
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
)odr;
''').show()

spark.sql('select * from ads_order_stats_by_cate limit 10').show()



#各品类商品购物车存量Top3
spark.sql(f'''
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '20250629' ds,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        where ds='20250629'
        group by sku_id
    )cart
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
            category3_name
        from dim_sku_full
        where ds='20250626'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;
''').show()
spark.sql('select * from ads_sku_cart_num_top3_by_cate limit 10').show()

#各品牌商品收藏次数Top3
spark.sql(f'''
insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select
    '2025-06-29' ds,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from dws_interaction_sku_favor_add_1d
    where ds='2025-06-29'
)t1
where rk<=3;
''').show()
spark.sql('select * from ads_sku_favor_count_top3_by_tm limit 10').show()


#下单到支付时间间隔平均值
spark.sql(f'''
insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '2025-06-29',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd_trade_trade_flow_acc
where ds in ('9999-12-31','2025-06-29')
and payment_date_id='2025-06-29';
''').show()
spark.sql('select * from ads_order_to_pay_interval_avg limit 10').show()


#各省份交易统计
spark.sql(f'''
insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
select
    '2025-06-29' ds,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where ds='2025-06-29'
union
select
    '2025-06-29' ds,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where ds='2025-06-29';
''').show()
spark.sql('select * from ads_order_by_province limit 10').show()




#优惠券使用统计
spark.sql(f'''
insert overwrite table ads_coupon_stats
select * from ads_coupon_stats
union
select
    '2025-06-29' ds,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from dws_tool_user_coupon_coupon_used_1d
where ds='2025-06-29'
group by coupon_id,coupon_name;
''').show()
spark.sql('select * from ads_coupon_stats limit 10').show()