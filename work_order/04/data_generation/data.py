import csv
import random
import json
from datetime import datetime, timedelta
import os

# 常量定义
TODAY = "2025-08-07"
FACT_TABLE_ROWS = 3000
PRODUCT_COUNT = 6
OUTPUT_DIR = "standard_ods_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# 辅助函数：确保字符串安全
def safe_string(s):
    """处理可能包含特殊字符的字符串"""
    if not s:
        return ""
    # 替换引号
    s = str(s).replace('"', '""')
    # 包含逗号或换行符时添加引号
    if ',' in s or '\n' in s or '"' in s:
        return f'"{s}"'
    return s


# 基础数据池（精简版）
user_pool = [f"user_{10000 + i}" for i in range(500)]
shop_pool = [f"shop_{100 + i}" for i in range(10)]
product_pool = []
for i in range(PRODUCT_COUNT):
    product_pool.append({
        "product_id": f"prod_{1000 + i}",
        "product_name": f"商品_{i + 1}",
        "category_id": f"cat_{i % 6 + 1:03d}",
        "category_name": ["智能手机", "笔记本电脑", "男士服装", "女士鞋包", "家居家电", "零食饮料"][i % 6],
        "product_attributes": json.dumps({"品牌": f"品牌{i % 5 + 1}", "价格": round(random.uniform(99, 9999), 2)}),
        "putaway_time": f"{TODAY} 00:00:00",
        "shop_id": random.choice(shop_pool),
        "dt": TODAY
    })


# 生成用户行为表（示例，其他表类似处理）
def generate_user_behavior():
    filename = f"{OUTPUT_DIR}/ods_user_behavior.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        # 写入表头
        headers = ["user_id", "product_id", "behavior_type", "behavior_time", "session_id", "device_id", "shop_id",
                   "dt"]
        f.write(",".join(headers) + "\n")

        # 写入数据
        behaviors = ["visit", "browse", "pay", "search"]
        for _ in range(FACT_TABLE_ROWS):
            user_id = random.choice(user_pool)
            product = random.choice(product_pool)
            row = [
                safe_string(user_id),
                safe_string(product["product_id"]),
                safe_string(random.choice(behaviors)),
                safe_string(
                    f"{TODAY} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"),
                safe_string(f"sess_{random.randint(1000, 9999)}"),
                safe_string(f"device_{random.randint(1000, 9999)}"),
                safe_string(product["shop_id"]),
                safe_string(TODAY)
            ]
            f.write(",".join(row) + "\n")


# 生成其他表（省略类似代码，采用相同的safe_string处理）
generate_user_behavior()
# 生成商品信息表、订单表等...

print(f"标准格式CSV文件已生成在 {OUTPUT_DIR} 目录")
print("特点：1. 处理特殊字符 2. 统一UTF-8编码 3. 符合CSV规范")
