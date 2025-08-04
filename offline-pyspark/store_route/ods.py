from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# 初始化SparkSession（配置Hive连接）
spark = SparkSession.builder \
    .appName("ODS_Layer_ShopPath_Generator") \
    .config("spark.sql.warehouse.dir", "hdfs://cdh01:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 工具函数
def generate_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

def get_partition_ds(date_obj):
    return date_obj.strftime("%Y%m%d")

# 1. 用户维度表（约100条）
def generate_user_dim_data(date_range, total_users=100):
    # 按日期平均分配100条数据
    daily_users = max(1, total_users // len(date_range))
    schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("user_name", StringType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType()),
        StructField("register_dt", StringType()),
        StructField("register_platform", StringType()),
        StructField("ds", StringType())
    ])
    data = []
    genders = ["male", "female", "unknown"]
    platforms = ["app", "h5", "pc"]
    for date in date_range:
        ds = get_partition_ds(date)
        # 控制总数据量接近100
        count = daily_users if len(data) + daily_users <= total_users else total_users - len(data)
        if count <= 0:
            break
        for _ in range(count):
            user_id = f"U{random.randint(100000, 999999)}"
            data.append((
                user_id, f"user_{random.randint(100, 999)}",
                random.choice(genders), random.randint(18, 60),
                date.strftime("%Y-%m-%d"), random.choice(platforms), ds
            ))
    return spark.createDataFrame(data, schema)

# 2. 商品维度表（约100条）
def generate_product_dim_data(date_range, total_products=100):
    daily_products = max(1, total_products // len(date_range))
    schema = StructType([
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType()),
        StructField("category_id", StringType()),
        StructField("price", DoubleType()),
        StructField("brand", StringType()),
        StructField("status", StringType()),
        StructField("ds", StringType())
    ])
    data = []
    categories = [f"C{random.randint(1000, 9999)}" for _ in range(5)]
    brands = ["brand_A", "brand_B", "brand_C"]
    status_list = ["on_sale", "off_sale"]
    for date in date_range:
        ds = get_partition_ds(date)
        count = daily_products if len(data) + daily_products <= total_products else total_products - len(data)
        if count <= 0:
            break
        for _ in range(count):
            product_id = f"P{random.randint(10000, 99999)}"
            data.append((
                product_id, f"product_{random.randint(100, 999)}",
                random.choice(categories), round(random.uniform(10.0, 500.0), 2),
                random.choice(brands), random.choice(status_list), ds
            ))
    return spark.createDataFrame(data, schema)

# 3. 店铺维度表（约100条）
def generate_shop_dim_data(date_range, total_shops=100):
    daily_shops = max(1, total_shops // len(date_range))
    schema = StructType([
        StructField("shop_id", StringType(), nullable=False),
        StructField("shop_name", StringType()),
        StructField("shop_type", StringType()),
        StructField("seller_id", StringType()),
        StructField("rating", DoubleType()),
        StructField("province", StringType()),
        StructField("ds", StringType())
    ])
    data = []
    shop_types = ["旗舰店", "专营店", "个人店"]
    provinces = ["北京", "上海", "广东"]
    for date in date_range:
        ds = get_partition_ds(date)
        count = daily_shops if len(data) + daily_shops <= total_shops else total_shops - len(data)
        if count <= 0:
            break
        for _ in range(count):
            shop_id = f"S{random.randint(1000, 9999)}"
            data.append((
                shop_id, f"shop_{random.randint(100, 999)}",
                random.choice(shop_types), f"B{random.randint(10000, 99999)}",
                round(random.uniform(3.0, 5.0), 1), random.choice(provinces), ds
            ))
    return spark.createDataFrame(data, schema)

# 4. 订单事实表（控制总量）
def generate_order_fact_data(date_range, max_records=5000):
    daily_orders = max(1, max_records // len(date_range))
    schema = StructType([
        StructField("order_id", StringType(), nullable=False),
        StructField("user_id", StringType()),
        StructField("product_id", StringType()),
        StructField("shop_id", StringType()),
        StructField("order_amount", DoubleType()),
        StructField("pay_time", StringType()),
        StructField("order_status", StringType()),
        StructField("ds", StringType())
    ])
    data = []
    status_list = ["待支付", "已支付", "已完成", "已取消"]
    for date in date_range:
        ds = get_partition_ds(date)
        if len(data) >= max_records:
            break
        count = daily_orders if len(data) + daily_orders <= max_records else max_records - len(data)
        for _ in range(count):
            order_id = f"O{random.randint(1000000, 9999999)}"
            data.append((
                order_id, f"U{random.randint(100000, 999999)}",
                f"P{random.randint(10000, 99999)}", f"S{random.randint(1000, 9999)}",
                round(random.uniform(50.0, 2000.0), 2),
                date.replace(hour=random.randint(0,23), minute=random.randint(0,59), second=random.randint(0,59)).strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(status_list), ds
            ))
    return spark.createDataFrame(data, schema)

# 5. 用户行为日志表（核心流量数据，控制总量）
def generate_user_behavior_data(date_range, max_records=14700):  # 确保总数据量不超2万
    daily_records = max(1, max_records // len(date_range))
    schema = StructType([
        StructField("log_id", StringType(), nullable=False),
        StructField("user_id", StringType()),
        StructField("page_url", StringType()),
        StructField("referrer_url", StringType()),
        StructField("stay_time", IntegerType()),
        StructField("action_type", StringType()),
        StructField("ds", StringType())
    ])
    data = []
    action_types = ["view", "click", "purchase"]
    # 页面路径符合文档中店内路径分析需求
    page_urls = ["/shop/index", "/shop/activity", "/item/detail", "/shop/category", "/shop/subscribe", "/shop/live"]  # 参考文档页面分类
    for date in date_range:
        ds = get_partition_ds(date)
        if len(data) >= max_records:
            break
        count = daily_records if len(data) + daily_records <= max_records else max_records - len(data)
        for _ in range(count):
            log_id = f"L{random.randint(10000000, 99999999)}"
            data.append((
                log_id, f"U{random.randint(100000, 999999)}",
                random.choice(page_urls),
                random.choice(page_urls + ["direct"]),  # 来源页面，含直接进店
                random.randint(5, 300),  # 停留时长，符合文档统计需求
                random.choice(action_types),
                ds
            ))
    return spark.createDataFrame(data, schema)

# 执行数据生成与写入
if __name__ == "__main__":
    date_range = generate_date_range("2025-07-01", "2025-07-31")  # 31天

    # 维度表各约100条，共300条
    user_df = generate_user_dim_data(date_range)
    product_df = generate_product_dim_data(date_range)
    shop_df = generate_shop_dim_data(date_range)

    # 事实表总数据约19700条（5000+14700），确保整体不超2万
    order_df = generate_order_fact_data(date_range)
    behavior_df = generate_user_behavior_data(date_range)

    # 写入Hive表（使用test11库）
    table_configs = [
        ("ods_user_dim", user_df),
        ("ods_product_dim", product_df),
        ("ods_shop_dim", shop_df),
        ("ods_order_fact", order_df),
        ("ods_user_behavior_log", behavior_df)
    ]

    for table_name, df in table_configs:
        df.write.mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable(f"test11.{table_name}")
        print(f"✅ {table_name} 数据写入完成，实际数据量：{df.count()}")

    spark.stop()