from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Create Spark session in YARN mode
spark = SparkSession.builder \
 .appName("ETL Sales Data") \
 .master("yarn") \
 .getOrCreate()

# Define schema (optional but recommended)
schema = StructType([
 StructField("id", IntegerType(), True),
 StructField("date", StringType(), True),
 StructField("product_id", IntegerType(), True),
 StructField("quantity", IntegerType(), True),
 StructField("price", DoubleType(), True)
])

# Extract
df_a = spark.read.option("header", True).schema(schema).csv("/path/sales_region_a.csv")
df_b = spark.read.option("header", True).schema(schema).csv("/path/sales_region_b.csv")


# Transform
df_a = df_a.withColumn("region", expr("'A'"))
df_b = df_b.withColumn("region", expr("'B'"))

df_combined = df_a.unionByName(df_b)

# i am assuming "id" is a primary key, So dropping duplicate accordingly

df_transformed = (
 df_combined
 .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
 .withColumn("total_sales", col("quantity") * col("price"))
 .dropDuplicates(["id"])
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
db_url = "jdbc:mysql://your_database_url:3306/your_database_name"

# i am assuming we getting data daily basis
df_transformed.write.mode("append").jdbc(url=db_url, table="sales_data", properties=db_properties)

spark.stop()
