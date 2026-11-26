import json, shutil, sys
from pathlib import Path
from spark_session import get_spark
from bronze import run_bronze
from silver import run_silver
from gold import run_gold

ROOT = Path(__file__).resolve().parent.parent
LAND, LAKE = str(ROOT / "data/landing"), ROOT / "lakehouse"

def main(reset=False):
    if reset and LAKE.exists(): shutil.rmtree(LAKE)
    spark = get_spark(); spark.sparkContext.setLogLevel("ERROR")
    b = run_bronze(spark, LAND, str(LAKE / "bronze/orders"), str(LAKE / "_checkpoints/bronze"))
    s_ok, s_bad = run_silver(spark, str(LAKE / "bronze/orders"), str(LAKE / "silver/orders_valid"), str(LAKE / "silver/orders_rejected"))
    rec = run_gold(spark, str(LAKE / "silver/orders_valid"), str(LAKE / "gold"))
    summary = {"bronze_rows": b, "silver_valid": s_ok, "silver_rejected": s_bad, "reconciliation": rec}
    print(json.dumps(summary, indent=2)); return summary

if __name__ == "__main__":
    main(reset="--reset" in sys.argv)
