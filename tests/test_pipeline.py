"""Real Spark runs: each of the 6 rules, null/malformed values, partitioning,
idempotent re-runs, cross-batch duplicates and gold reconciliation."""
import sys, pathlib, csv
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
import pytest
from pyspark.sql import functions as F
from spark_session import get_spark
from bronze import run_bronze
from silver import run_silver
from gold import run_gold

H = ["order_id","customer_id","order_date","city","product","category","quantity","unit_price","discount_pct","payment_mode","order_status"]
def row(oid, **kw):
    r = dict(order_id=oid, customer_id="C1", order_date="2026-01-05", city="Pune", product="Desk Lamp", category="Home",
             quantity="2", unit_price="900", discount_pct="10", payment_mode="UPI", order_status="Delivered"); r.update(kw); return r
def write(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, H); w.writeheader(); w.writerows(rows)

@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests"); s.sparkContext.setLogLevel("ERROR"); yield s; s.stop()

def lake(tmp_path):
    L = tmp_path; (L / "land").mkdir(exist_ok=True)
    return dict(land=str(L/"land"), b=str(L/"b"), ck=str(L/"ck"), sv=str(L/"sv"), sr=str(L/"sr"), g=str(L/"g"))
def go(spark, p):
    run_bronze(spark, p["land"], p["b"], p["ck"]); ok, bad = run_silver(spark, p["b"], p["sv"], p["sr"])
    return ok, bad

def test_each_rule_rejects_with_its_reason_and_good_rows_pass(spark, tmp_path):
    p = lake(tmp_path)
    bad = {"R1_missing_city": row("A1", city=""), "R2_invalid_quantity": row("A2", quantity="-1"),
           "R3_invalid_unit_price": row("A3", unit_price="0"), "R4_invalid_discount": row("A4", discount_pct="150"),
           "R5_invalid_order_date": row("A5", order_date="31/02/2026"), "R6_unknown_payment_mode": row("A6", payment_mode="Barter")}
    write(pathlib.Path(p["land"]) / "b1.csv", [row("OK1"), row("OK2", city=" mumbai ")] + list(bad.values()))
    ok, nbad = go(spark, p)
    assert (ok, nbad) == (2, 6)
    rej = {r.order_id: r.rejection_reasons for r in spark.read.format("delta").load(p["sr"]).collect()}
    for (reason, r) in bad.items(): assert rej[r["order_id"]] == reason
    good = {r.order_id: r for r in spark.read.format("delta").load(p["sv"]).collect()}
    assert good["OK2"].city == "Mumbai" and good["OK1"].order_amount == 1620.0

def test_null_and_malformed_values_are_rejected_not_crashing(spark, tmp_path):
    p = lake(tmp_path)
    write(pathlib.Path(p["land"]) / "b1.csv", [row("M1", quantity="two"), row("M2", unit_price=""), row("M3", order_date=""),
                                               row("M4", discount_pct=""), row("M5", payment_mode="")])
    ok, bad = go(spark, p)
    ids_ok = {r.order_id for r in spark.read.format("delta").load(p["sv"]).collect()}
    assert ids_ok == {"M4"}          # blank discount defaults to 0 and is valid
    assert bad == 4

def test_partition_is_complete_and_disjoint(spark, tmp_path):
    p = lake(tmp_path)
    write(pathlib.Path(p["land"]) / "b1.csv", [row("P1"), row("P2", quantity="0"), row("P3"), row("P3")])
    ok, bad = go(spark, p)
    assert ok + bad == spark.read.format("delta").load(p["b"]).count() == 4
    a = {r.order_id for r in spark.read.format("delta").load(p["sv"]).collect()}
    b = spark.read.format("delta").load(p["sr"])
    assert "P1" in a and "P3" in a and b.filter("rejection_reasons like '%DUP%'").count() == 1

def test_rerun_without_new_files_is_idempotent_and_new_file_is_incremental(spark, tmp_path):
    p = lake(tmp_path)
    write(pathlib.Path(p["land"]) / "b1.csv", [row("I1"), row("I2")])
    go(spark, p); n1 = spark.read.format("delta").load(p["b"]).count()
    go(spark, p); assert spark.read.format("delta").load(p["b"]).count() == n1 == 2   # no re-ingest
    write(pathlib.Path(p["land"]) / "b2.csv", [row("I3")])
    ok, bad = go(spark, p)
    assert spark.read.format("delta").load(p["b"]).count() == 3 and ok == 3 and bad == 0

def test_cross_batch_duplicate_keeps_earliest_ingested(spark, tmp_path):
    p = lake(tmp_path)
    write(pathlib.Path(p["land"]) / "b1.csv", [row("D1", city="Pune")])
    go(spark, p)
    write(pathlib.Path(p["land"]) / "b2.csv", [row("D1", city="Delhi")])
    ok, bad = go(spark, p)
    kept = spark.read.format("delta").load(p["sv"]).collect()
    assert ok == 1 and bad == 1 and kept[0].city == "Pune"

def test_gold_tables_reconcile_and_count_only_delivered(spark, tmp_path):
    p = lake(tmp_path)
    write(pathlib.Path(p["land"]) / "b1.csv", [row("G1"), row("G2", city="Delhi", product="Gel Pen Set", category="Stationery", unit_price="100", quantity="3", discount_pct="0"),
                                               row("G3", order_status="Cancelled"), row("G4", quantity="-1")])
    go(spark, p); rec = run_gold(spark, p["sv"], p["g"])
    assert rec["silver_delivered_revenue"] == 1620.0 + 300.0
    assert rec["city_sales_ok"] and rec["category_sales_ok"] and rec["product_revenue_ok"]
    assert rec["payment_mode_analysis_ok"] and rec["silver_all_valid_amount"] == 1620.0 + 300.0 + 1620.0  # incl. cancelled G3
    assert spark.read.format("delta").load(p["g"] + "/delivered_orders").count() == 2
