#!/usr/bin/env python3
"""
SPARK-55848 correctness benchmark — reproduces the Table I row-count results.

Demonstrates the silent data-inflation bug in Apache Spark's Storage-Partitioned
Join (SPJ) under partial clustering, and its correction in SPARK-55848.

Run the SAME script on two Spark versions:
  * a PRE-FIX release  (e.g. pyspark==4.0.0, or 3.5.0-3.5.8 / 4.0.0-4.0.2)
        -> SPJ silently inflates post-join deduplication by (split_degree - 1)
  * a POST-FIX release (e.g. pyspark==4.0.3, or 3.5.9 / 4.1.2 / 4.2.0)
        -> SPJ output matches the shuffle-join baseline (correct)

For each split degree N in {2, 5, 6, 10}, three post-join dedup patterns are
run with SPJ ON (the affected path) and SPJ OFF (correct baseline).

Usage:
    pip install pyspark==4.0.0          # or 4.0.3 for the post-fix run
    python table1_correctness_benchmark.py [warehouse_dir] [ivy_dir]

Requires Java 17 and network access (Iceberg runtime pulled from Maven Central).
Runs locally (local[4]); the row-count result is deterministic and needs no cluster.
"""
import sys, os, shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

WH  = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else os.path.abspath("./_spj_warehouse")
IVY = os.path.abspath(sys.argv[2]) if len(sys.argv) > 2 else os.path.abspath("./_spj_ivy")
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"

BUCKET_COUNT, HOT_KEY, NUM_CUSTOMERS = 8, 1, 500
NORMAL_ROWS, ROWS_PER_FILE = 5_000, 50
SPLIT_DEGREES = [2, 5, 6, 10]

if os.path.isdir(WH):
    shutil.rmtree(WH)  # fresh Iceberg tables each run

spark = (SparkSession.builder
    .appName("spark55848-table1")
    .master("local[4]")
    .config("spark.jars.packages", ICEBERG_PKG)
    .config("spark.jars.ivy", IVY)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WH)
    .config("spark.sql.catalog.local.read.split.target-size", "1")
    .config("spark.sql.catalog.local.read.split.open-file-cost", "1")
    .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
    .config("spark.sql.adaptive.enabled", "false")           # no AQE rewrite
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")    # force SortMergeJoin
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
ver = spark.version
print(f"\nSpark version: {ver}\n")

def spj_on():
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")

def spj_off():
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "false")

def spj_active(df):
    """True if no Exchange precedes the Join in the executed plan (SPJ engaged)."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    for line in plan.split("\n"):
        if "Exchange" in line: return False
        if "Join" in line: return True
    return True

TP = "TBLPROPERTIES ('read.split.target-size'='1','read.split.open-file-cost'='1')"
spark.sql("CREATE DATABASE IF NOT EXISTS local.bench")
spark.sql("DROP TABLE IF EXISTS local.bench.customers")
spark.sql(f"CREATE TABLE local.bench.customers (customer_id BIGINT, name STRING) "
          f"USING iceberg PARTITIONED BY (bucket({BUCKET_COUNT}, customer_id)) {TP}")
# customers 1..500
(spark.range(NUM_CUSTOMERS).withColumn("customer_id", (F.col("id") + 1).cast("long"))
     .withColumn("name", F.concat(F.lit("c_"), F.col("id"))).drop("id")
     .writeTo("local.bench.customers").append())

rows = []
for deg in SPLIT_DEGREES:
    tbl = f"local.bench.orders_{deg}"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    spark.sql(f"CREATE TABLE {tbl} (order_id BIGINT, customer_id BIGINT, amount DOUBLE, seq_id BIGINT) "
              f"USING iceberg PARTITIONED BY (bucket({BUCKET_COUNT}, customer_id)) {TP}")
    # background orders for customers 2..500 (hot key EXCLUDED -> split degree == deg exactly)
    (spark.range(NORMAL_ROWS).withColumn("order_id", F.col("id"))
        .withColumn("customer_id", (F.lit(2) + (F.col("id") % (NUM_CUSTOMERS - 1))).cast("long"))
        .withColumn("amount", F.lit(50.0)).withColumn("seq_id", F.col("id")).drop("id")
        .writeTo(tbl).append())
    # hot key written in `deg` separate files -> `deg` parallel scan splits
    for fi in range(deg):
        off = NORMAL_ROWS + fi * ROWS_PER_FILE
        (spark.range(ROWS_PER_FILE).withColumn("order_id", (F.col("id") + off).cast("long"))
            .withColumn("customer_id", F.lit(HOT_KEY).cast("long"))
            .withColumn("amount", F.lit(500.0)).withColumn("seq_id", (F.col("id") + off).cast("long")).drop("id")
            .writeTo(tbl).append())

    measured_deg = (spark.table(tbl).filter(F.col("customer_id") == HOT_KEY)
                    .select(F.input_file_name()).distinct().count())
    orders = spark.table(tbl); cust = spark.table("local.bench.customers")

    out = {"split_degree": measured_deg}
    for cond, fn in [("baseline", spj_off), ("spj", spj_on)]:
        fn()
        joined = orders.join(cust, "customer_id")   # inner join on the bucketed key
        p1 = joined.dropDuplicates(["customer_id"]).count()
        w  = Window.partitionBy("customer_id").orderBy(F.col("seq_id").asc())
        p2 = joined.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).count()
        p3 = joined.select("customer_id").distinct().count()
        out[cond] = {"p1": p1, "p2": p2, "p3": p3}
        if cond == "spj":
            out["spj_active"] = spj_active(joined)
    rows.append(out)
    print(f"split_degree={out['split_degree']:>2} | baseline={out['baseline']['p1']} | "
          f"SPJ P1/P2/P3={out['spj']['p1']}/{out['spj']['p2']}/{out['spj']['p3']} | "
          f"SPJ_active={out['spj_active']}")

print(f"\n================ RESULTS (Spark {ver}) ================")
print(f"{'split_degree':>12}{'baseline':>10}{'SPJ P1':>8}{'SPJ P2':>8}{'SPJ P3':>8}{'inflation':>11}")
for r in rows:
    inf = r['spj']['p1'] - r['baseline']['p1']
    print(f"{r['split_degree']:>12}{r['baseline']['p1']:>10}{r['spj']['p1']:>8}"
          f"{r['spj']['p2']:>8}{r['spj']['p3']:>8}{('+' + str(inf)):>11}")
print("\nPre-fix  (<3.5.9 / <4.0.3): inflation = split_degree - 1  (silent BUG)")
print("Post-fix (>=3.5.9 / >=4.0.3): inflation = 0, SPJ output matches baseline (FIXED)")
spark.stop()
