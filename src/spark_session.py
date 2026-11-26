from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark(app="ecom-medallion"):
    b = (SparkSession.builder.appName(app).master("local[1]")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").config("spark.databricks.delta.snapshotPartitions", "2").config("spark.sql.adaptive.enabled","false")
         .config("spark.driver.memory", "1g"))
    return configure_spark_with_delta_pip(b).getOrCreate()
