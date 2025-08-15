import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, DateType, TimestampType

# -------- Args --------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "SILVER_S3_PATH", "GOLD_CURATED_PATH", "GOLD_METRICS_PATH"
])
SILVER = args["SILVER_S3_PATH"]
GOLD_CURATED = args["GOLD_CURATED_PATH"]
GOLD_METRICS = args["GOLD_METRICS_PATH"]

spark = SparkSession.builder.appName("silver_to_gold_transformations").getOrCreate()

# -------- Read Silver --------
df = spark.read.parquet(SILVER)

# -------- Light Quality Filters --------
# (Keep rows with essential keys and non-negative amounts)
df = df.filter(F.col("Policy_ID").isNotNull())
df = df.filter((F.col("Premium_Amount") >= 0) & (F.col("Claim_Amount") >= 0))

# -------- Derived Columns --------
df = (
    df
    .withColumn("Has_Claim", F.when(F.col("Claim_Amount") > 0, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("Payment_Flag", F.when(F.col("Payment_Status") == "Paid", F.lit(1)).otherwise(F.lit(0)))
    .withColumn("Premium_to_Claim_Ratio",
                F.when(F.col("Claim_Amount") > 0, F.col("Premium_Amount")/F.col("Claim_Amount"))
                 .otherwise(F.lit(None).cast(DoubleType())))
)

# -------- Curated Transactions (Gold 1) --------
curated_cols = [
    "Policy_ID","Customer_ID","Product_Type","Premium_Amount","Payment_Status",
    "Transaction_Date","Claim_Amount","Claim_Status","Agent_ID","Last_Updated",
    "Region","Source_System",
    "Has_Claim","Payment_Flag","Premium_to_Claim_Ratio",
    # keep partitions from Silver
    "year","month","day"
]
curated_df = df.select(*curated_cols)

# Write partitioned Parquet
(
    curated_df.write
        .mode("overwrite")  # first run; switch to "append" for incremental
        .format("parquet")
        .partitionBy("year","month","day")
        .save(GOLD_CURATED)
)

# -------- Daily Product KPIs (Gold 2) --------
metrics_df = (
    df.groupBy("year","month","day","Product_Type")
      .agg(
          F.countDistinct("Policy_ID").alias("policies"),
          F.count("*").alias("transactions"),
          F.sum("Premium_Amount").alias("total_premium"),
          F.sum("Claim_Amount").alias("total_claim"),
          F.avg("Premium_Amount").alias("avg_premium"),
          F.sum("Payment_Flag").alias("paid_count"),
          F.sum("Has_Claim").alias("claim_count")
      )
      .withColumn("paid_rate", F.col("paid_count") / F.col("transactions"))
      .withColumn("claim_rate", F.col("claim_count") / F.col("transactions"))
      .withColumn("created_ts", F.current_timestamp())
)

(
    metrics_df.write
        .mode("overwrite")  # first run; switch to "append" for incremental
        .format("parquet")
        .partitionBy("year","month","day")
        .save(GOLD_METRICS)
)

spark.stop()
