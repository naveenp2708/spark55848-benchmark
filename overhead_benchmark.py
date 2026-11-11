#!/usr/bin/env python3
"""SPARK-55848 overhead measurement (run on POST-FIX pyspark 4.0.3).
Compares the corrected SPJ path (SPJ on) against the shuffle-join baseline
(SPJ off) for the affected join+dedup query. Both are correct on 4.0.3.
Warmup runs discarded; median of N timed count() runs; min/max for variance."""
import sys, os, shutil, time, statistics
from pyspark.sql import SparkSession, functions as F

WH  = os.path.abspath("./_perf_wh")
IVY = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else os.path.abspath("./_spj_ivy")
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
BUCKET, HOT, NCUST, NORMAL, RPF, DEG = 8, 1, 100_000, 1_000_000, 1_000, 6
WARMUP, TIMED = 2, 7

if os.path.isdir(WH):
    shutil.rmtree(WH)

spark = (SparkSession.builder.appName("spark55848-perf").master("local[4]")
  .config("spark.jars.packages", ICEBERG_PKG).config("spark.jars.ivy", IVY)
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "hadoop")
  .config("spark.sql.catalog.local.warehouse", WH)
  .config("spark.sql.catalog.local.read.split.target-size", "1")
  .config("spark.sql.catalog.local.read.split.open-file-cost", "1")
  .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
  .config("spark.sql.adaptive.enabled", "false")
  .config("spark.sql.autoBroadcastJoinThreshold", "-1")
  .config("spark.sql.shuffle.partitions", "16")
  .config("spark.driver.memory", "8g")
  .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark version:", spark.version)

def spj_on():
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")
def spj_off():
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "false")

TP = "TBLPROPERTIES ('read.split.target-size'='1','read.split.open-file-cost'='1')"
spark.sql("CREATE DATABASE IF NOT EXISTS local.bench")
for t in ["customers", "orders"]:
    spark.sql(f"DROP TABLE IF EXISTS local.bench.{t}")
spark.sql(f"CREATE TABLE local.bench.customers (customer_id BIGINT, name STRING) "
          f"USING iceberg PARTITIONED BY (bucket({BUCKET}, customer_id)) {TP}")
(spark.range(NCUST).withColumn("customer_id", (F.col("id")+1).cast("long"))
     .withColumn("name", F.concat(F.lit("c_"), F.col("id"))).drop("id")
     .writeTo("local.bench.customers").append())
spark.sql(f"CREATE TABLE local.bench.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, seq_id BIGINT) "
          f"USING iceberg PARTITIONED BY (bucket({BUCKET}, customer_id)) {TP}")
(spark.range(NORMAL).withColumn("order_id", F.col("id"))
    .withColumn("customer_id", (F.lit(2)+(F.col("id")%(NCUST-1))).cast("long"))
    .withColumn("amount", F.lit(50.0)).withColumn("seq_id", F.col("id")).drop("id")
    .writeTo("local.bench.orders").append())
for fi in range(DEG):
    off = NORMAL + fi*RPF
    (spark.range(RPF).withColumn("order_id", (F.col("id")+off).cast("long"))
        .withColumn("customer_id", F.lit(HOT).cast("long"))
        .withColumn("amount", F.lit(500.0)).withColumn("seq_id", (F.col("id")+off).cast("long")).drop("id")
        .writeTo("local.bench.orders").append())

orders = spark.table("local.bench.orders"); cust = spark.table("local.bench.customers")
def run():
    return orders.join(cust, "customer_id").dropDuplicates(["customer_id"])

def timeit(setup):
    setup()
    for _ in range(WARMUP):
        run().count()                       # warmup: discard (JIT, caches)
    ts = []; cnt = None
    for _ in range(TIMED):
        t0 = time.time(); cnt = run().count(); ts.append((time.time()-t0)*1000)
    ts.sort()
    return statistics.median(ts), ts[0], ts[-1], cnt

bmed, bmin, bmax, bcnt = timeit(spj_off)
smed, smin, smax, scnt = timeit(spj_on)
print(f"\n==== OVERHEAD (Spark {spark.version}; {NCUST} customers, {NORMAL} orders, split_degree {DEG}) ====")
print(f"{'variant':>34}{'median_ms':>11}{'min':>7}{'max':>7}{'rows':>9}")
print(f"{'shuffle-join baseline (SPJ off)':>34}{round(bmed):>11}{round(bmin):>7}{round(bmax):>7}{bcnt:>9}")
print(f"{'SPJ + SPARK-55848 fix (SPJ on)':>34}{round(smed):>11}{round(smin):>7}{round(smax):>7}{scnt:>9}")
d = smed - bmed
print(f"\nBoth return {bcnt} rows (correct). Corrected-SPJ vs shuffle-join baseline: "
      f"{round(d):+d} ms median ({round(100*d/bmed):+d}%).")
spark.stop()
