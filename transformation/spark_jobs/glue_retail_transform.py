import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType,
    DoubleType, BooleanType
)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "store_path",
    "product_path",
    "calendar_path",
    "sales_path",
    "inventory_path",
    "output_path"
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.sparkContext.setLogLevel("ERROR")

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
    StructField("prod_key",         IntegerType(), True),
    StructField("prod_name",        StringType(),  True),
    StructField("vol",              DoubleType(),  True),
    StructField("wgt",              DoubleType(),  True),
    StructField("brand_name",       StringType(),  True),
    StructField("status_code",      IntegerType(), True),
    StructField("status_code_name", StringType(),  True),
    StructField("category_key",     IntegerType(), True),
    StructField("category_name",    StringType(),  True),
    StructField("subcategory_key",  IntegerType(), True),
    StructField("subcategory_name", StringType(),  True),
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
    StructField("cal_dt",                 DateType(),    True),
    StructField("store_key",              IntegerType(), True),
    StructField("prod_key",               IntegerType(), True),
    StructField("inventory_on_hand_qty",  DoubleType(),  True),
    StructField("inventory_on_order_qty", DoubleType(),  True),
    StructField("out_of_stock_flg",       BooleanType(), True),
    StructField("waste_qty",              DoubleType(),  True),
    StructField("promotion_flg",          BooleanType(), True),
    StructField("low_stock_flg",          BooleanType(), True),
])

store_df = spark.read.option("header", True).schema(store_schema).csv(args["store_path"])
product_df = spark.read.option("header", True).schema(product_schema).csv(args["product_path"])
calendar_df = spark.read.option("header", True).schema(calendar_schema).csv(args["calendar_path"])
sales_df = spark.read.option("header", True).schema(sales_schema).csv(args["sales_path"])
inventory_df = spark.read.option("header", True).schema(inventory_schema).csv(args["inventory_path"])

store_dim_df = store_df.withColumnRenamed("prov_state_desc","prov_name").withColumnRenamed("prov_state_cd","prov_code").drop("frnchs_flg","store_size")
product_dim_df = product_df
calendar_dim_df = calendar_df.withColumnRenamed("yr_num","year_num").withColumnRenamed("wk_num","week_num").withColumnRenamed("mnth_num","month_num").withColumnRenamed("yr_wk_num","year_wk_num").withColumnRenamed("yr_mnth_num","year_month_num").drop("day_of_wk_desc")

sales_df.createOrReplaceTempView("sales")
inventory_df.createOrReplaceTempView("inventory")
calendar_df.createOrReplaceTempView("calendar")

spark.sql("""
    SELECT s.store_key, s.prod_key, c.yr_wk_num AS year_wk_num,
        SUM(s.sales_qty) AS total_sales_qty,
        SUM(s.sales_amt) AS total_sales_amt,
        SUM(s.cost_amt) AS total_cost_amt,
        SUM(s.sales_amt) / NULLIF(SUM(s.sales_qty), 0) AS avg_sales_price
    FROM sales s JOIN calendar c ON s.trans_dt = c.cal_dt
    GROUP BY s.store_key, s.prod_key, c.yr_wk_num
""").createOrReplaceTempView("weekly_sales")

spark.sql("""
    SELECT i.store_key, i.prod_key, c.yr_wk_num, MAX(i.cal_dt) AS last_dt_of_week
    FROM inventory i JOIN calendar c ON i.cal_dt = c.cal_dt
    GROUP BY i.store_key, i.prod_key, c.yr_wk_num
""").createOrReplaceTempView("eow_dates")

spark.sql("""
    SELECT i.store_key, i.prod_key, c.yr_wk_num,
        i.inventory_on_hand_qty AS stock_on_hand_qty,
        i.inventory_on_order_qty AS ordered_stock_qty
    FROM inventory i
    JOIN calendar c ON i.cal_dt = c.cal_dt
    JOIN eow_dates e ON i.store_key = e.store_key
        AND i.prod_key = e.prod_key
        AND i.cal_dt = e.last_dt_of_week
        AND c.yr_wk_num = e.yr_wk_num
""").createOrReplaceTempView("eow_snapshot")

spark.sql("""
    SELECT i.store_key, i.prod_key, c.yr_wk_num,
        SUM(CAST(i.out_of_stock_flg AS INT)) AS no_stock_instances,
        SUM(CAST(i.low_stock_flg AS INT)) AS low_stock_instances,
        SUM(CAST(i.out_of_stock_flg AS INT) + CAST(i.low_stock_flg AS INT)) AS total_low_stock_impact,
        COUNT(*) AS days_tracked
    FROM inventory i JOIN calendar c ON i.cal_dt = c.cal_dt
    GROUP BY i.store_key, i.prod_key, c.yr_wk_num
""").createOrReplaceTempView("weekly_inv")

weekly_fact_df = spark.sql("""
    SELECT ws.year_wk_num, ws.store_key, ws.prod_key,
        ws.total_sales_qty, ws.total_sales_amt, ws.avg_sales_price, ws.total_cost_amt,
        e.stock_on_hand_qty, e.ordered_stock_qty,
        wi.no_stock_instances, wi.low_stock_instances, wi.total_low_stock_impact,
        ROUND(1 - (wi.no_stock_instances / NULLIF(wi.days_tracked, 0)), 4) AS pct_in_stock,
        ROUND(e.stock_on_hand_qty / NULLIF(ws.total_sales_qty, 0), 2) AS weeks_of_supply
    FROM weekly_sales ws
    LEFT JOIN eow_snapshot e ON ws.store_key = e.store_key AND ws.prod_key = e.prod_key AND ws.year_wk_num = e.yr_wk_num
    LEFT JOIN weekly_inv wi ON ws.store_key = wi.store_key AND ws.prod_key = wi.prod_key AND ws.year_wk_num = wi.yr_wk_num
""")

output = args["output_path"]

weekly_fact_df.write.mode("overwrite").partitionBy("year_wk_num").parquet(output + "/weekly_fact/")
store_dim_df.write.mode("overwrite").parquet(output + "/store_dim/")
product_dim_df.write.mode("overwrite").parquet(output + "/product_dim/")
calendar_dim_df.write.mode("overwrite").parquet(output + "/calendar_dim/")

job.commit()


