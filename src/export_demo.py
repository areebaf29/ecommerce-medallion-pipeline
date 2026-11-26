"""Export gold tables + rejection stats from the Delta lakehouse to JSON for the static demo site."""
import json
from pathlib import Path
from pyspark.sql import functions as F
from spark_session import get_spark
ROOT = Path(__file__).resolve().parent.parent; L = ROOT / "lakehouse"
spark = get_spark(); spark.sparkContext.setLogLevel("ERROR")
rd = lambda p: spark.read.format("delta").load(str(L / p))
rows = lambda df: [r.asDict() for r in df.collect()]
out = {n: rows(rd(f"gold/{n}")) for n in ["city_sales", "category_sales", "product_revenue", "payment_mode_analysis"]}
for k in out:
    for r in out[k]:
        for c, v in r.items():
            if hasattr(v, "item"): r[c] = v.item()
rej = rd("silver/orders_rejected").select(F.explode(F.split("rejection_reasons", "; ")).alias("reason")).groupBy("reason").count().orderBy(F.desc("count"))
out["rejection_reasons"] = rows(rej)
out["sample_rejected"] = rows(rd("silver/orders_rejected").select("order_id", "city", "quantity", "unit_price", "payment_mode", "rejection_reasons").limit(12))
out["counts"] = {"bronze_rows": rd("bronze/orders").count(), "silver_valid": rd("silver/orders_valid").count(),
                 "silver_rejected": rd("silver/orders_rejected").count(), "delivered_orders": rd("gold/delivered_orders").count()}
out["reconciliation"] = json.load(open(ROOT / "docs/run2.json"))["reconciliation"]
(ROOT / "docs").mkdir(exist_ok=True)
(ROOT / "docs/data.json").write_text(json.dumps(out, default=str, indent=1))
print(out["counts"]); print(out["rejection_reasons"])
