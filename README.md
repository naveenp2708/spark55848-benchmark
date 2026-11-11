# SPARK-55848 — Correctness Benchmark

Reproduces the row-count results (Table I) for **SPARK-55848**: a silent
data-inflation bug in Apache Spark's Storage-Partitioned Join (SPJ) under
partial clustering, and its correction.

Post-join deduplication over a partially-clustered SPJ join silently returns
**inflated** results on pre-fix Spark, and **correct** results after the fix.

The post-join failure was first reported by GitHub user **paryoja** in
apache/spark issue **#54378** (2026-02-19). This repository accompanies the
JIRA the author filed for the fix, **SPARK-55848**.

## What it tests

Three post-join deduplication patterns, over `orders.join(customers, "customer_id")`:

- **P1** — `dropDuplicates("customer_id")`
- **P2** — `row_number() OVER (PARTITION BY customer_id ORDER BY seq_id)` filtered to `rn = 1`
- **P3** — `SELECT DISTINCT customer_id`

Each is run at four split degrees (2, 5, 6, 10), with SPJ **on** (affected path)
and SPJ **off** (correct shuffle-join baseline).

## Requirements

- Java 17
- Python 3
- Network access (the Iceberg runtime is pulled from Maven Central on first run)

## Run

**Pre-fix (buggy)** — any defective release before the fix (e.g. 4.0.0 or 4.0.2):

```bash
python3 -m venv venv-prefix
./venv-prefix/bin/pip install pyspark==4.0.2
./venv-prefix/bin/python table1_correctness_benchmark.py
```

**Post-fix (correct):**

```bash
python3 -m venv venv-postfix
./venv-postfix/bin/pip install pyspark==4.0.3
./venv-postfix/bin/python table1_correctness_benchmark.py
```

## Expected output

**Pre-fix (defective release):**

| split_degree | baseline | SPJ P1 / P2 / P3 | inflation |
|---|---|---|---|
| 2 | 500 | 501 / 501 / 501 | +1 |
| 5 | 500 | 504 / 504 / 504 | +4 |
| 6 | 500 | 505 / 505 / 505 | +5 |
| 10 | 500 | 509 / 509 / 509 | +9 |

**Post-fix (Spark 4.0.3):** SPJ P1/P2/P3 = **500** at every split degree (inflation 0).

Inflation on the pre-fix path equals `split_degree - 1`, identical across all
three dedup patterns. The baseline (SPJ off) is always correct (500). The defect
is identical across every pre-fix release that contains partial clustering
(3.4.x, 3.5.0–3.5.8, 4.0.0–4.0.2, 4.1.x), so the inflation reproduces the same way
on any of them.

## Overhead benchmark (optional)

`overhead_benchmark.py` measures the wall-clock cost of the corrective Exchange
on the post-fix release: it times the affected join-then-dedup query with SPJ on
(fixed) vs the shuffle-join baseline, median of 7 runs after 2 warm-ups.

```bash
./venv-postfix/bin/python overhead_benchmark.py
```

Expected (Spark 4.0.3; 100k customers, 1M orders, split degree 6): the corrected
SPJ path (~152 ms) is faster than the shuffle-join baseline (~225 ms), both
returning the correct 100,000 rows — the corrective Exchange does not negate
SPJ's benefit. (Absolute numbers vary by machine; the relative result holds.)

## How it works

- Iceberg tables bucketed `bucket(8, customer_id)` on both sides (required for SPJ).
- The hot-key customer (`customer_id = 1`) is written in `N` separate Iceberg
  files and excluded from the background orders, so the split degree equals `N`
  exactly. `read.split.target-size=1` forces each file into its own scan split.
- With partial clustering enabled, the hot key's rows spread across `N` tasks.
  Pre-fix, the join output claims it satisfies `ClusteredDistribution`, so no
  Exchange is inserted before dedup and each task deduplicates only its local
  subset → `N` surviving rows for the hot key instead of 1. SPARK-55848 adds the
  `isPartiallyClustered` flag so the corrective Exchange is inserted.
- Runs locally (`local[4]`); the row-count result is deterministic — no cluster needed.

## Affected / fixed versions

- **Affected (pre-fix):** 3.4.x, 3.5.0–3.5.8, 4.0.0–4.0.2, 4.1.x
- **Fixed by the `isPartiallyClustered` flag (this work):** 3.5.9, 4.0.3, 4.1.2
- **master / 4.2.0:** the same defect is closed by an independent structural
  refactor, PR #54330 (SPARK-55535) (Peter Toth's `KeyedPartitioning` refactor), **not** by
  this flag; the author's regression tests for SPARK-55848 (#54714) are included
  in 4.2.0.

## References

- JIRA (fix): https://issues.apache.org/jira/browse/SPARK-55848
- Original report: paryoja, apache/spark issue #54378 — https://github.com/apache/spark/issues/54378
- Master/4.2.0 refactor: PR #54330 (SPARK-55535/SPARK-55092) — https://github.com/apache/spark/pull/54330
