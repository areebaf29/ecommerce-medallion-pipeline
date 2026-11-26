"""Silver: clean, standardise, type-cast, validate with six rules, split valid / rejected."""
from pyspark.sql import functions as F, Window

PAYMENT_MODES = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Net Banking"]
STATUSES = ["Delivered", "Shipped", "Cancelled", "Returned"]

def clean(df):
    t = lambda c: F.initcap(F.trim(F.col(c)))
    df = (df.withColumn("city", F.when(F.trim("city") == "", None).otherwise(t("city")))
            .withColumn("product", F.trim("product")).withColumn("category", t("category"))
            .withColumn("payment_mode", F.trim("payment_mode")).withColumn("order_status", t("order_status"))
            .withColumn("order_date", F.to_date(F.trim("order_date"), "yyyy-MM-dd"))  # bad dates -> null
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("discount_pct", F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0)))
            .withColumn("payment_mode", F.when(F.upper("payment_mode") == "UPI", "UPI").otherwise(t("payment_mode")))
            .withColumn("payment_mode", F.when(F.col("payment_mode") == "Cash On Delivery", "Cash on Delivery").otherwise(F.col("payment_mode"))))
    return df.withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2))

def validate(df):
    """Six business rules. Each rule is a boolean column; rejection reasons are collected."""
    rules = {
        "R1_missing_city":        F.col("city").isNull(),
        "R2_invalid_quantity":    F.col("quantity").isNull() | (F.col("quantity") <= 0),
        "R3_invalid_unit_price":  F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
        "R4_invalid_discount":    (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100),
        "R5_invalid_order_date":  F.col("order_date").isNull(),
        "R6_unknown_payment_mode": F.coalesce(~F.col("payment_mode").isin(PAYMENT_MODES), F.lit(True)),
    }
    w = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").asc())
    df = df.withColumn("_rn", F.row_number().over(w))
    reasons = [F.when(cond, F.lit(name)) for name, cond in rules.items()]
    reasons.append(F.when(F.col("_rn") > 1, F.lit("DUP_duplicate_order_id")))
    df = df.withColumn("rejection_reasons", F.concat_ws("; ", *reasons)).drop("_rn")
    return df

def run_silver(spark, bronze, silver_valid, silver_rejected):
    b = spark.read.format("delta").load(bronze)
    v = validate(clean(b))
    ok = v.filter(F.col("rejection_reasons") == "").drop("rejection_reasons")
    bad = v.filter(F.col("rejection_reasons") != "")
    ok.write.format("delta").mode("overwrite").save(silver_valid)
    bad.write.format("delta").mode("overwrite").save(silver_rejected)
    return ok.count(), bad.count()
