"""Bronze: incremental raw ingestion. Everything stays a string; we only add lineage columns.
Local stand-in for Databricks Auto Loader: Structured Streaming file source + checkpoint,
run with availableNow so each run only picks up files that have not been processed yet."""
from pyspark.sql import functions as F, types as T

SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in
    ["order_id","customer_id","order_date","city","product","category","quantity",
     "unit_price","discount_pct","payment_mode","order_status"]])

def run_bronze(spark, landing, bronze, checkpoint):
    stream = (spark.readStream.format("csv").option("header", True).schema(SCHEMA).load(landing)
              .withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("source_file", F.input_file_name()))
    q = (stream.writeStream.format("delta").option("checkpointLocation", checkpoint)
         .outputMode("append").trigger(availableNow=True).start(bronze))
    q.awaitTermination()
    return spark.read.format("delta").load(bronze).count()
