"""Gold: business-ready aggregates + revenue reconciliation."""
from pyspark.sql import functions as F

def run_gold(spark, silver_valid, gold_dir):
    s = spark.read.format("delta").load(silver_valid)
    delivered = s.filter(F.col("order_status") == "Delivered")   # revenue = delivered orders only
    out = {
      "city_sales": delivered.groupBy("city").agg(F.count("*").alias("orders"),
                      F.round(F.sum("order_amount"), 2).alias("revenue"),
                      F.round(F.avg("order_amount"), 2).alias("avg_order_value")).orderBy(F.desc("revenue")),
      "category_sales": delivered.groupBy("category").agg(F.sum("quantity").alias("units"),
                      F.round(F.sum("order_amount"), 2).alias("revenue")).orderBy(F.desc("revenue")),
      "product_revenue": delivered.groupBy("category", "product").agg(F.sum("quantity").alias("units"),
                      F.round(F.sum("order_amount"), 2).alias("revenue")).orderBy(F.desc("revenue")),
      "payment_mode_analysis": s.groupBy("payment_mode").agg(F.count("*").alias("orders"),
                      F.round(F.sum("order_amount"), 2).alias("gross_amount"),
                      F.round(F.avg("order_amount"), 2).alias("avg_order_value")).orderBy(F.desc("orders")),
      "delivered_orders": delivered.select("order_id", "customer_id", "order_date", "city", "product",
                      "category", "quantity", "order_amount", "payment_mode"),
    }
    for name, df in out.items():
        df.write.format("delta").mode("overwrite").save(f"{gold_dir}/{name}")
    return reconcile(spark, s, delivered, gold_dir)

def reconcile(spark, valid, delivered, gold_dir):
    """Every gold revenue table must add up to the same delivered revenue in silver."""
    base = round(delivered.agg(F.sum("order_amount")).first()[0], 2)
    res = {"silver_delivered_revenue": base}
    for n in ["city_sales", "category_sales", "product_revenue"]:
        tot = round(spark.read.format("delta").load(f"{gold_dir}/{n}").agg(F.sum("revenue")).first()[0], 2)
        res[n] = tot; res[n + "_ok"] = abs(tot - base) < 0.05
    allv = round(valid.agg(F.sum("order_amount")).first()[0], 2)   # payment table covers ALL valid orders, any status
    pay = round(spark.read.format("delta").load(f"{gold_dir}/payment_mode_analysis").agg(F.sum("gross_amount")).first()[0], 2)
    res["silver_all_valid_amount"] = allv; res["payment_mode_analysis"] = pay; res["payment_mode_analysis_ok"] = abs(pay - allv) < 0.05
    return res
