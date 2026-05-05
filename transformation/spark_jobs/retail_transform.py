# ============================================
# RETAIL ETL - PYSPARK TRANSFORMATION JOB
# Reads raw CSVs from S3, produces:
#   - weekly_fact table
#   - store_dim, product_dim, calendar_dim
# Output: Parquet files in S3 processed bucket
# ============================================

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType,
    DoubleType, BooleanType
)

# ── Argument parsing ─────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--params", required=True, help="JSON string of S3 paths")
args   = parser.parse_args()
params = json.loads(args.params)

# ── Spark session ────────────────────────────
spark = SparkSession.builder \
    .appName("retail_etl_transform") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✓ Spark session started")

# ════════════════════════════════════════════
# SCHEMAS
# ════════════════════════════════════════════

store_schema = StructType([
    StructField("store_key",       IntegerType(), True),
    StructField("store_num",       IntegerType(), True),
    StructField("store_desc",      StringType(),  True),
    StructField("addr",            StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("region",          StringType(),  True),
    StructField("cntry_cd",        StringType(),  True),
    StructField("cntry_nm",        StringType(),  True),
    StructField("postal_zip_cd",   StringType(),  True),
    StructField("prov_state_desc", StringType(),  True),
    StructField("prov_state_cd",   StringType(),  True),
    StructField("store_type_cd",   StringType(),  True),
    StructField("store_type_desc", StringType(),  True),
    StructField("frnchs_flg",      StringType(),  True),
    StructField("store_size",      StringType(),  True),
    StructField("market_key",      IntegerType(), True),
    StructField("market_name",     StringType(),  True),
    StructField("submarket_key",   IntegerType(), True),
    StructField("submarket_name",  StringType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("longitude",       DoubleType(),  True),
])

product_schema = StructType([
    StructField("prod_key",          IntegerType(), True),
    StructField("prod_name",         StringType(),  True),
    StructField("vol",               DoubleType(),  True),
    StructField("wgt",               DoubleType(),  True),
    StructField("brand_name",        StringType(),  True),
    StructField("status_code",       IntegerType(), True),
    StructField("status_code_name",  StringType(),  True),
    StructField("category_key",      IntegerType(), True),
    StructField("category_name",     StringType(),  True),
    StructField("subcategory_key",   IntegerType(), True),
    StructField("subcategory_name",  StringType(),  True),
])

calendar_schema = StructType([
    StructField("cal_dt",         DateType(),    False),
    StructField("cal_type_desc",  StringType(),  True),
    StructField("day_of_wk_num",  IntegerType(), True),
    StructField("day_of_wk_desc", StringType(),  True),
    StructField("yr_num",         IntegerType(), True),
    StructField("wk_num",         IntegerType(), True),
    StructField("yr_wk_num",      IntegerType(), True),
    StructField("mnth_num",       IntegerType(), True),
    StructField("yr_mnth_num",    IntegerType(), True),
    StructField("qtr_num",        IntegerType(), True),
    StructField("yr_qtr_num",     IntegerType(), True),
])

sales_schema = StructType([
    StructField("trans_id",    IntegerType(), True),
    StructField("prod_key",    IntegerType(), True),
    StructField("store_key",   IntegerType(), True),
    StructField("trans_dt",    DateType(),    True),
    StructField("trans_time",  IntegerType(), True),
    StructField("sales_qty",   DoubleType(),  True),
    StructField("sales_price", DoubleType(),  True),
    StructField("sales_amt",   DoubleType(),  True),
    StructField("discount",    DoubleType(),  True),
    StructField("cost_amt",    DoubleType(),  True),
])

inventory_schema = StructType([
    StructField("cal_dt",                  DateType(),    True),
    StructField("store_key",               IntegerType(), True),
    StructField("prod_key",                IntegerType(), True),
    StructField("inventory_on_hand_qty",   DoubleType(),  True),
    StructField("inventory_on_order_qty",  DoubleType(),  True),
    StructField("out_of_stock_flg",        BooleanType(), True),
    StructField("waste_qty",               DoubleType(),  True),
    StructField("promotion_flg",           BooleanType(), True),
    StructField("low_stock_flg",           BooleanType(), True),
])

# ════════════════════════════════════════════
# READ RAW DATA FROM S3
# ════════════════════════════════════════════
print("✓ Reading raw data from S3...")

store_df = spark.read.option("header", True) \
    .schema(store_schema).csv(params["store_path"])

product_df = spark.read.option("header", True) \
    .schema(product_schema).csv(params["product_path"])

calendar_df = spark.read.option("header", True) \
    .schema(calendar_schema).csv(params["calendar_path"])

sales_df = spark.read.option("header", True) \
    .schema(sales_schema).csv(params["sales_path"])

inventory_df = spark.read.option("header", True) \
    .schema(inventory_schema).csv(params["inventory_path"])

print(f"  sales rows     : {sales_df.count():,}")
print(f"  inventory rows : {inventory_df.count():,}")

# ════════════════════════════════════════════
# DIMENSION TABLES
# ════════════════════════════════════════════
print("✓ Building dimension tables...")

# store_dim — drop operational columns not needed in warehouse
store_dim_df = store_df \
    .withColumnRenamed("prov_state_desc", "prov_name") \
    .withColumnRenamed("prov_state_cd",   "prov_code") \
    .drop("frnchs_flg", "store_size")

# product_dim — as-is
product_dim_df = product_df

# calendar_dim — rename for clarity
calendar_dim_df = calendar_df \
    .withColumnRenamed("yr_num",      "year_num") \
    .withColumnRenamed("wk_num",      "week_num") \
    .withColumnRenamed("mnth_num",    "month_num") \
    .withColumnRenamed("yr_wk_num",   "year_wk_num") \
    .withColumnRenamed("yr_mnth_num", "year_month_num") \
    .drop("day_of_wk_desc")

# ════════════════════════════════════════════
# WEEKLY FACT TABLE
# ════════════════════════════════════════════
print("✓ Building weekly fact table...")

# Register temp views
sales_df.createOrReplaceTempView("sales")
inventory_df.createOrReplaceTempView("inventory")
calendar_df.createOrReplaceTempView("calendar")
store_df.createOrReplaceTempView("store")
product_df.createOrReplaceTempView("product")

# Step 1 — join sales to calendar to get week number
daily_sales = spark.sql("""
    SELECT
        s.store_key,
        s.prod_key,
        c.yr_wk_num                          AS year_wk_num,
        SUM(s.sales_qty)                     AS total_sales_qty,
        SUM(s.sales_amt)                     AS total_sales_amt,
        SUM(s.cost_amt)                      AS total_cost_amt,
        SUM(s.sales_amt) / NULLIF(SUM(s.sales_qty), 0) AS avg_sales_price
    FROM sales s
    JOIN calendar c ON s.trans_dt = c.cal_dt
    GROUP BY s.store_key, s.prod_key, c.yr_wk_num
""")
daily_sales.createOrReplaceTempView("weekly_sales")

# Step 2 — end-of-week inventory snapshot
# Get the last cal_dt per week per store+product
end_of_week_inv = spark.sql("""
    SELECT
        i.store_key,
        i.prod_key,
        c.yr_wk_num,
        MAX(i.cal_dt)                        AS last_dt_of_week
    FROM inventory i
    JOIN calendar c ON i.cal_dt = c.cal_dt
    GROUP BY i.store_key, i.prod_key, c.yr_wk_num
""")
end_of_week_inv.createOrReplaceTempView("eow_dates")

eow_snapshot = spark.sql("""
    SELECT
        i.store_key,
        i.prod_key,
        c.yr_wk_num,
        i.inventory_on_hand_qty              AS stock_on_hand_qty,
        i.inventory_on_order_qty             AS ordered_stock_qty
    FROM inventory i
    JOIN calendar c  ON i.cal_dt = c.cal_dt
    JOIN eow_dates e ON i.store_key = e.store_key
                     AND i.prod_key  = e.prod_key
                     AND i.cal_dt    = e.last_dt_of_week
                     AND c.yr_wk_num = e.yr_wk_num
""")
eow_snapshot.createOrReplaceTempView("eow_snapshot")

# Step 3 — weekly inventory aggregates
weekly_inv = spark.sql("""
    SELECT
        i.store_key,
        i.prod_key,
        c.yr_wk_num,
        SUM(CAST(i.out_of_stock_flg AS INT))  AS no_stock_instances,
        SUM(CAST(i.low_stock_flg    AS INT))  AS low_stock_instances,
        SUM(CAST(i.out_of_stock_flg AS INT) +
            CAST(i.low_stock_flg    AS INT))  AS total_low_stock_impact,
        COUNT(*)                               AS days_tracked
    FROM inventory i
    JOIN calendar c ON i.cal_dt = c.cal_dt
    GROUP BY i.store_key, i.prod_key, c.yr_wk_num
""")
weekly_inv.createOrReplaceTempView("weekly_inv")

# Step 4 — combine everything into final weekly fact
weekly_fact_df = spark.sql("""
    SELECT
        ws.year_wk_num,
        ws.store_key,
        ws.prod_key,

        -- sales metrics
        ws.total_sales_qty,
        ws.total_sales_amt,
        ws.avg_sales_price,
        ws.total_cost_amt,

        -- inventory end-of-week
        e.stock_on_hand_qty,
        e.ordered_stock_qty,

        -- stock health metrics
        wi.no_stock_instances,
        wi.low_stock_instances,
        wi.total_low_stock_impact,
        ROUND(1 - (wi.no_stock_instances / NULLIF(wi.days_tracked, 0)), 4)
                                               AS pct_in_stock,

        -- supply weeks remaining
        ROUND(e.stock_on_hand_qty / NULLIF(ws.total_sales_qty, 0), 2)
                                               AS weeks_of_supply

    FROM weekly_sales    ws
    LEFT JOIN eow_snapshot e  ON ws.store_key  = e.store_key
                              AND ws.prod_key   = e.prod_key
                              AND ws.year_wk_num = e.yr_wk_num
    LEFT JOIN weekly_inv  wi ON ws.store_key  = wi.store_key
                              AND ws.prod_key   = wi.prod_key
                              AND ws.year_wk_num = wi.yr_wk_num
""")

print(f"  weekly fact rows: {weekly_fact_df.count():,}")

# ════════════════════════════════════════════
# WRITE PARQUET TO S3
# ════════════════════════════════════════════
print("✓ Writing Parquet files to S3...")

output = params["output_path"]

weekly_fact_df.write.mode("overwrite") \
    .partitionBy("year_wk_num") \
    .parquet(f"{output}/weekly_fact/")

store_dim_df.write.mode("overwrite") \
    .partitionBy("cntry_cd") \
    .parquet(f"{output}/store_dim/")

product_dim_df.write.mode("overwrite") \
    .partitionBy("category_name") \
    .parquet(f"{output}/product_dim/")

calendar_dim_df.write.mode("overwrite") \
    .partitionBy("year_num") \
    .parquet(f"{output}/calendar_dim/")

print("✅ Transformation complete!")
spark.stop()