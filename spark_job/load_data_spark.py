from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import json

# Create Spark session in YARN mode
spark = SparkSession.builder \
    .appName("ETL Sales Data") \
    .master("yarn") \
    .getOrCreate()
# if you are using on pc you can use "local[*]"
# Define schema (optional but recommended)
schema = StructType([
    StructField("OrderId", StringType(), True),
    StructField("OrderItemId", StringType(), True),
    StructField("QuantityOrdered", IntegerType(), True),
    StructField("ItemPrice", DoubleType(), True),
    StructField("PromotionDiscount", StringType(), True),
    StructField("batch_id", IntegerType(), True)
])

# Extract
df_a = spark.read.option("header", True).schema(schema).csv("/path/sales_region_a.csv")
df_b = spark.read.option("header", True).schema(schema).csv("/path/sales_region_b.csv")

# Transform
df_a = df_a.withColumn("region", expr("'A'"))
df_b = df_b.withColumn("region", expr("'B'"))

df_combined = df_a.unionByName(df_b)

# Define UDF to extract discount amount from JSON string
def extract_discount(discount_str):
    try:
        discount_dict = json.loads(discount_str)
        return float(discount_dict["Amount"])
    except:
        return 0.0

extract_discount_udf = udf(extract_discount, DoubleType())

df_transformed = (
 df_combined
 .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
 .withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))
 .withColumn("PromotionDiscount", extract_discount_udf(col("PromotionDiscount")))
 .withColumn("net_sale", col("total_sales") - col("PromotionDiscount"))
 .dropDuplicates(["OrderId"])
 .filter(col("net_sale") > 0) # Exclude orders with non-positive net sales
)

# For backup and archive purpose we should create a parquet file
# Outbound to Parquet
df_transformed.write.mode("overwrite").format("parquet").save("output/sales_data_parquet")

# Load to a database (e.g., MySQL)
db_properties = {
    "user": "admin",
    "password": "pass***",
    "driver": "com.mysql.cj.jdbc.Driver"
}

db_url = "jdbc:mysql://your_database_url:3306/SQLlite"

# Assuming we get data on a daily basis
df_transformed.write.mode("append").jdbc(url=db_url, table="sales_data", properties=db_properties)

spark.stop()
