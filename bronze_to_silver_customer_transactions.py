import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, DateType, TimestampType

# -------- Args --------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_S3_PATH", "TARGET_S3_PATH"])
SOURCE = args["SOURCE_S3_PATH"]
TARGET = args["TARGET_S3_PATH"]

# -------- Spark --------
spark = SparkSession.builder.appName("bronze_to_silver_customer_transactions").getOrCreate()

# -------- Read Bronze (CSV) --------
df = (
    spark.read
         .option("header", "true")
         .option("escape", '"')
         .csv(SOURCE)
)

# -------- Basic Cleaning / Typing --------
# Trim string columns
for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c)))

# Cast numeric and date/time columns
df = (
    df.withColumn("Premium_Amount", F.col("Premium_Amount").cast(DoubleType()))
      .withColumn("Claim_Amount", F.col("Claim_Amount").cast(DoubleType()))
      .withColumn("Transaction_Date", F.to_date("Transaction_Date", "yyyy-MM-dd"))
      .withColumn("Last_Updated", F.to_timestamp("Last_Updated", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# Optional standardization of Product_Type values (example)
df = df.withColumn(
    "Product_Type",
    F.when(F.col("Product_Type") == F.lit("MutualFund"), F.lit("Mutual Fund"))
     .otherwise(F.col("Product_Type"))
)

# Deduplicate (business key example)
# Keep latest by Last_Updated for same Policy_ID + Transaction_Date
w = (
    F.window(F.col("Last_Updated"), "36500 days")  # not actually grouping; we’ll use row_number over sort
)
from pyspark.sql.window import Window
win = Window.partitionBy("Policy_ID", "Transaction_Date").orderBy(F.col("Last_Updated").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1).drop("rn")

# Add partitions from Transaction_Date
df = (
    df.withColumn("year", F.year("Transaction_Date"))
      .withColumn("month", F.date_format("Transaction_Date", "MM"))
      .withColumn("day", F.date_format("Transaction_Date", "dd"))
)

# Reorder columns (optional, for readability)
final_cols = [
    "Policy_ID","Customer_ID","Product_Type","Premium_Amount","Payment_Status",
    "Transaction_Date","Claim_Amount","Claim_Status","Agent_ID","Last_Updated",
    "Region","Source_System","year","month","day"
]
df = df.select(*final_cols)

# -------- Write Silver (Parquet, partitioned) --------
(
    df.write
      .mode("overwrite")  # use "append" once you enable bookmarks/incremental
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(TARGET)
)

spark.stop()
