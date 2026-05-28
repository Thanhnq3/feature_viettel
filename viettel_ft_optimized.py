"""
viettel_ft_optimized.py
=======================
Month-by-month optimized pipeline for large datasets (1B+ rows).

Strategy
--------
Two independent phases, controlled by the --mode parameter:

  Phase 1 — Staging  (--mode staging | all)
      Processes the raw invoice table ONE MONTH AT A TIME using the
      partition_month column for efficient partition pruning.
      For each month derived from run_dates, builds and persists:

          build_staging       → STG_DAILY_TABLE       (partitioned by report_date)
          build_buyer_staging → STG_BUYER_TABLE        (partitioned by month_start)
          features_from_raw   → STG_RAW_MONTHLY_TABLE  (partitioned by report_date)

      Re-running a month overwrites only that month's partitions.

  Phase 2 — Final features  (--mode features | all)
      Reads the three staging tables (no raw scan) with a narrow date filter
      covering the last 12 months, computes final features per run_date, and
      writes one partition to FT_TABLE.

Why this is faster at 1B rows
------------------------------
  Original script scans the full raw table 3× per run_date.
  This script scans only 1 month of raw data per staging iteration (Phase 1),
  then Phase 2 reads from pre-aggregated staging tables which are orders of
  magnitude smaller.

New tables vs. original script
-------------------------------
  Original persisted: stg_daily, monthly_intermediate, ft_final.
  This script ADDS:
      STG_BUYER_TABLE        — build_buyer_staging output (was recomputed from raw)
      STG_RAW_MONTHLY_TABLE  — features_from_raw output   (was recomputed from raw)

Notebook usage
--------------
    import datetime
    run_dates = [datetime.date(2025, 1, 1), datetime.date(2025, 2, 1)]
    run_pipeline(spark, run_dates)          # mode="all" by default

    # Phase 1 only (build / refresh staging):
    run_pipeline(spark, run_dates, mode="staging")

    # Phase 2 only (assumes Phase 1 already done for all required history):
    run_pipeline(spark, run_dates, mode="features")

CLI usage
---------
    spark-submit viettel_ft_optimized.py \\
        --run-dates 2025-01-01,2025-02-01,2025-03-01 \\
        --mode all
"""

import datetime
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import processing functions and constants from the existing script.
# SparkSession.getOrCreate() at module level in that file is harmless —
# it returns the already-running session in notebook / spark-submit context.
from viettel_ft_pyspark_2_4_4 import (
    build_staging,
    build_buyer_staging,
    features_from_raw,
    build_final_features,
    f_last_day_of_previous_month,
    f_first_day_of_previous_month,
    COL_PARTITION,
)


# =============================================================================
# TABLE CONFIG — override via run_pipeline() kwargs or CLI args
# =============================================================================

INVOICE_TABLE         = "your_schema.viettel_invoice"
STG_DAILY_TABLE       = "your_schema.viettel_stg_daily"
STG_BUYER_TABLE       = "your_schema.viettel_stg_buyer_monthly"
STG_RAW_MONTHLY_TABLE = "your_schema.viettel_stg_raw_monthly"
FT_TABLE              = "your_schema.viettel_ft_seller"


# =============================================================================
# WRITE HELPER
# =============================================================================

def _write_partition(spark, df, table, partition_cols):
    """
    2-step safe write for partitioned tables. No full-table overwrite.

    Step 1 — Drop existing partitions matching the values in df via
              ALTER TABLE DROP IF EXISTS PARTITION (Spark 2.4.4+, Hive + DataSource).
    Step 2 — Append df into the cleared slots with write.mode("append").

    First call (table does not exist): creates the table via saveAsTable+partitionBy.
    Table existence is detected by a limit(0) read — works on any catalog.
    Column order in df does not matter — partitionBy() identifies partition
    columns by name, not position.
    """
    # Table existence: read attempt works on any catalog.
    try:
        spark.table(table).limit(0).count()
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        df.write.mode("append").partitionBy(*partition_cols).saveAsTable(table)
        # Refresh so the next spark.table() in the same session sees the new table.
        spark.catalog.refreshTable(table)
        return

    # Step 1: Drop the partitions that the new data will replace.
    partition_values = df.select(*partition_cols).distinct().collect()
    for row in partition_values:
        part_spec = ", ".join(
            "{}='{}'".format(col, row[col]) for col in partition_cols
        )
        spark.sql(
            "ALTER TABLE {} DROP IF EXISTS PARTITION ({})".format(table, part_spec)
        )

    # Step 2: Append into the cleared partition slots.
    df.write.mode("append").partitionBy(*partition_cols).saveAsTable(table)
    # Refresh after each write so subsequent spark.table() calls see the latest data
    # instead of a stale cached plan. Without this, Spark 2.4's SessionCatalog can
    # serve an invalidated entry that causes "Table or view not found" on the next read.
    spark.catalog.refreshTable(table)


# =============================================================================
# UTILITIES
# =============================================================================

def _data_month_str(run_date):
    """
    Return the YYYYMM string for the data month that run_date targets.
    run_date builds features for last_day_of_previous_month(run_date),
    so the raw data month is that previous month.

    Example: run_date=2025-02-01  →  "202501"
    """
    return f_last_day_of_previous_month(run_date).strftime("%Y%m")


# =============================================================================
# PHASE 1: STAGING — one month at a time from raw
# =============================================================================

def run_staging_phase(spark, run_dates,
                      invoice_table=INVOICE_TABLE,
                      stg_daily_table=STG_DAILY_TABLE,
                      stg_buyer_table=STG_BUYER_TABLE,
                      stg_raw_monthly_table=STG_RAW_MONTHLY_TABLE):
    """
    For each run_date, filter raw data to the corresponding month and build
    three staging tables.

    run_dates must be sorted chronologically so that the staging tables
    accumulate in date order.

    Raw table scans: 1 per run_date (covering ~1 month of data via partition
    pruning on COL_PARTITION), compared with 3 full-table scans in the
    original script per run_date.
    """
    print("=" * 60)
    print("PHASE 1 — STAGING  ({} months)".format(len(run_dates)))
    print("=" * 60)

    df_raw_full = spark.table(invoice_table)

    for i, run_date in enumerate(run_dates, 1):
        pm            = _data_month_str(run_date)
        set_last_date = f_last_day_of_previous_month(run_date)

        print("\n[Phase 1 | {}/{}] run_date={}  partition_month={}".format(
            i, len(run_dates), run_date, pm))

        # Partition pruning: read only the target month.
        # Relies on COL_PARTITION ("partition_month") being a physical partition
        # column in the raw table so Spark skips all other month directories.
        df_raw_month = df_raw_full.filter(F.col(COL_PARTITION) == pm)

        # ------------------------------------------------------------------
        # 1a. Daily seller aggregation
        # ------------------------------------------------------------------
        print("  [1a] Daily staging ...")
        df_stg = build_staging(df_raw_month)
        _write_partition(spark, df_stg, stg_daily_table, ["report_date"])
        print("  [1a] Written -> {} (partition report_date IN {})".format(
            stg_daily_table, pm))

        # ------------------------------------------------------------------
        # 1b. Monthly buyer-seller pair aggregation
        # ------------------------------------------------------------------
        print("  [1b] Buyer staging ...")
        df_buyer = build_buyer_staging(df_raw_month)
        _write_partition(spark, df_buyer, stg_buyer_table, ["month_start"])
        print("  [1b] Written -> {} (partition month_start ~ {})".format(
            stg_buyer_table, pm))

        # ------------------------------------------------------------------
        # 1c. Raw monthly features (invoice-grain features that cannot be
        #     derived from the daily staging table)
        # ------------------------------------------------------------------
        print("  [1c] Raw monthly features ...")
        df_raw_monthly = features_from_raw(df_raw_month)
        _write_partition(spark, df_raw_monthly, stg_raw_monthly_table, ["report_date"])
        print("  [1c] Written -> {} (partition report_date={})".format(
            stg_raw_monthly_table, set_last_date))

        # features_from_raw internally caches df_raw_proc and df_buyer_ranked.
        # Clear them here so they don't accumulate across loop iterations.
        spark.catalog.clearCache()
        print("  Cache cleared.")

    print("\nPhase 1 complete.")


# =============================================================================
# PHASE 2: FINAL FEATURES — reads from staging, no raw scan
# =============================================================================

def run_features_phase(spark, run_dates,
                       stg_daily_table=STG_DAILY_TABLE,
                       stg_buyer_table=STG_BUYER_TABLE,
                       stg_raw_monthly_table=STG_RAW_MONTHLY_TABLE,
                       ft_table=FT_TABLE):
    """
    For each run_date, read the necessary window of staging data and compute
    final features. Writes one report_date partition to ft_table per run_date.

    Window logic (mirrors build_final_features internals):
        set_last_date = last_day_of_previous_month(run_date)
        window_start  = first_day_of_month 12 months before set_last_date
        stg_daily_table   : report_date in [window_start, set_last_date]
        stg_buyer_table   : month_start  in [window_start, set_last_date]
        stg_raw_monthly_table : report_date == set_last_date  (single month)

    No raw table is read in this phase.
    """
    print("=" * 60)
    print("PHASE 2 — FINAL FEATURES  ({} run_dates)".format(len(run_dates)))
    print("=" * 60)

    for i, run_date in enumerate(run_dates, 1):
        set_last_date = f_last_day_of_previous_month(run_date)
        window_start  = f_first_day_of_previous_month(set_last_date, nbr_of_mth=11)

        print("\n[Phase 2 | {}/{}] run_date={}  window={} → {}".format(
            i, len(run_dates), run_date, window_start, set_last_date))

        # ------------------------------------------------------------------
        # Read pre-filtered windows from staging tables.
        # Partition pruning on report_date / month_start means Spark reads
        # only the relevant month directories, not the full staging history.
        # ------------------------------------------------------------------
        df_stg_window = (
            spark.table(stg_daily_table)
            .filter(F.col("report_date").between(
                F.lit(str(window_start)).cast("date"),
                F.lit(str(set_last_date)).cast("date"),
            ))
        )

        df_buyer_window = (
            spark.table(stg_buyer_table)
            .filter(F.col("month_start").between(
                F.lit(str(window_start)).cast("date"),
                F.lit(str(set_last_date)).cast("date"),
            ))
        )

        # Raw monthly features: only the single target month is needed here.
        # build_final_features already filters df_raw_monthly to set_last_date
        # internally, but we pre-filter to avoid passing unnecessary rows.
        df_raw_monthly_window = (
            spark.table(stg_raw_monthly_table)
            .filter(F.col("report_date") == F.lit(str(set_last_date)).cast("date"))
        )

        # ------------------------------------------------------------------
        # Compute final features
        # ------------------------------------------------------------------
        print("  Computing final features ...")
        df_ft, _ = build_final_features(
            spark,
            df_stg_window,
            df_buyer_window,
            df_raw_monthly_window,
            run_date,
        )

        # ------------------------------------------------------------------
        # Write one report_date partition to ft_table
        # ------------------------------------------------------------------
        print("  Writing -> {} (report_date={}) ...".format(ft_table, set_last_date))
        _write_partition(spark, df_ft, ft_table, ["report_date"])
        print("  Done.")

        # build_final_features caches df_daily internally; clear before next iteration.
        spark.catalog.clearCache()
        print("  Cache cleared.")

    print("\nPhase 2 complete.")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_pipeline(spark, run_dates,
                 invoice_table=INVOICE_TABLE,
                 stg_daily_table=STG_DAILY_TABLE,
                 stg_buyer_table=STG_BUYER_TABLE,
                 stg_raw_monthly_table=STG_RAW_MONTHLY_TABLE,
                 ft_table=FT_TABLE,
                 mode="all"):
    """
    Orchestrate the two-phase optimized pipeline.

    Parameters
    ----------
    spark                 : active SparkSession
    run_dates             : list of datetime.date
                            Each date targets the month ending on
                            last_day_of_previous_month(run_date).
                            Will be sorted chronologically automatically.
    invoice_table         : source raw invoice table
    stg_daily_table       : daily seller aggregation (Phase 1 output / Phase 2 input)
    stg_buyer_table       : buyer-seller monthly staging (Phase 1 output / Phase 2 input)
    stg_raw_monthly_table : raw monthly features (Phase 1 output / Phase 2 input)
    ft_table              : final feature output (Phase 2 writes here)
    mode                  : "staging"  — Phase 1 only
                            "features" — Phase 2 only (Phase 1 must have run first)
                            "all"      — Phase 1 then Phase 2
    """
    run_dates = sorted(run_dates)

    print("Pipeline start | mode={} | {} run_date(s): {}".format(
        mode, len(run_dates), run_dates))

    if mode in ("staging", "all"):
        run_staging_phase(
            spark, run_dates,
            invoice_table=invoice_table,
            stg_daily_table=stg_daily_table,
            stg_buyer_table=stg_buyer_table,
            stg_raw_monthly_table=stg_raw_monthly_table,
        )

    if mode in ("features", "all"):
        run_features_phase(
            spark, run_dates,
            stg_daily_table=stg_daily_table,
            stg_buyer_table=stg_buyer_table,
            stg_raw_monthly_table=stg_raw_monthly_table,
            ft_table=ft_table,
        )

    print("\nPipeline complete.")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Viettel FT Optimized — month-by-month pipeline"
    )
    parser.add_argument("--invoice-table",          default=INVOICE_TABLE,
                        help="Source raw invoice table")
    parser.add_argument("--stg-daily-table",        default=STG_DAILY_TABLE,
                        help="Daily seller staging table")
    parser.add_argument("--stg-buyer-table",        default=STG_BUYER_TABLE,
                        help="Monthly buyer-seller staging table")
    parser.add_argument("--stg-raw-monthly-table",  default=STG_RAW_MONTHLY_TABLE,
                        help="Raw monthly features staging table")
    parser.add_argument("--ft-table",               default=FT_TABLE,
                        help="Final feature output table")
    parser.add_argument("--run-dates", required=True,
                        help="Comma-separated run dates YYYY-MM-DD,...")
    parser.add_argument("--mode", default="all",
                        choices=["staging", "features", "all"],
                        help="staging=Phase1 only | features=Phase2 only | all=both")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("viettel_ft_optimized").getOrCreate()

    run_dates = sorted(
        datetime.datetime.strptime(d.strip(), "%Y-%m-%d").date()
        for d in args.run_dates.split(",")
    )

    run_pipeline(
        spark, run_dates,
        invoice_table=args.invoice_table,
        stg_daily_table=args.stg_daily_table,
        stg_buyer_table=args.stg_buyer_table,
        stg_raw_monthly_table=args.stg_raw_monthly_table,
        ft_table=args.ft_table,
        mode=args.mode,
    )
