"""
Viettel Invoice Feature Engineering - SQL Version
==================================================
Same logic as viettel_ft_pyspark.py but uses Spark SQL queries.
Designed to run in Databricks or any Spark SQL environment.

Pipeline:
    Step 1: Create staging view (daily seller-level)
    Step 2: Create monthly aggregation view
    Step 3: Build final multi-period features

Usage:
    - Set TABLE_INVOICE to your actual invoice table path
    - Run the full script or copy individual SQL blocks to your environment
"""

from pyspark.sql import SparkSession
import datetime
from dateutil.relativedelta import relativedelta

spark = SparkSession.builder.appName("viettel_ft_sql").getOrCreate()


# =============================================================================
# CONFIG - adapt to your environment
# =============================================================================
TABLE_INVOICE = "your_schema.viettel_invoice"
TABLE_STG_OUTPUT = "your_schema.viettel_stg_daily"
TABLE_FT_OUTPUT = "your_schema.viettel_ft_seller"

# Date calculation
def get_period_dates(run_date=None):
    if run_date is None:
        run_date = datetime.date.today()
    last_date = run_date.replace(day=1) - datetime.timedelta(days=1)
    return {
        "run_date": str(run_date),
        "last_date": str(last_date),
        "l1m": str(last_date.replace(day=1)),
        "l3m": str(last_date.replace(day=1) - relativedelta(months=2)),
        "l6m": str(last_date.replace(day=1) - relativedelta(months=5)),
        "l12m": str(last_date.replace(day=1) - relativedelta(months=11)),
    }


# =============================================================================
# STEP 1: STAGING - Daily seller-level aggregation
# =============================================================================

SQL_STAGING = """
CREATE OR REPLACE TEMPORARY VIEW vt_stg_daily AS
WITH base AS (
    SELECT
        iv1.col1   AS invoice_id,
        TRIM(CAST(iv1.col21 AS STRING)) AS mst_seller,
        TRIM(CAST(iv1.col29 AS STRING)) AS mst_buyer,
        COALESCE(TO_TIMESTAMP(iv1.col23), TO_TIMESTAMP(iv1.col4)) AS invoice_ts,
        CAST(iv1.col9 AS INT)  AS invoice_status,
        CAST(iv1.col10 AS INT) AS adj_type,
        CAST(iv1.col11 AS INT) AS adj_status,
        -- Adjusted total sales (logic from dev_ft_viettel_3)
        CASE
            WHEN CAST(iv1.col10 AS INT) = 9
                THEN COALESCE(CAST(iv1.col55 AS DOUBLE), 0) + COALESCE(CAST(iv2.col55 AS DOUBLE), 0)
            WHEN CAST(iv1.col10 AS INT) = 3
                THEN COALESCE(CAST(iv2.col55 AS DOUBLE), 0)
            ELSE COALESCE(CAST(iv1.col55 AS DOUBLE), 0)
        END AS total_sales,
        CAST(iv1.col47 AS DOUBLE) AS tax_amt,
        CAST(iv1.col48 AS DOUBLE) AS discount_amt,
        CAST(iv1.col52 AS DOUBLE) AS total_with_vat,
        CAST(iv1.col53 AS DOUBLE) AS total_without_vat,
        iv1.col79 AS payment_method,
        iv1.col30 AS transport,
        HOUR(COALESCE(TO_TIMESTAMP(iv1.col23), TO_TIMESTAMP(iv1.col4))) AS invoice_hour,
        iv1.col129 AS partition_time
    FROM {table_invoice} iv1
    LEFT JOIN {table_invoice} iv2
        ON iv1.col129 = iv2.col129
        AND iv1.col72 = iv2.col1
    WHERE CAST(iv1.col9 AS INT) = 1
),
with_flags AS (
    SELECT *,
        TO_DATE(invoice_ts) AS report_date,
        CASE WHEN invoice_hour >= 22 OR invoice_hour < 6 THEN 1 ELSE 0 END AS is_night,
        CASE WHEN invoice_hour >= 0 AND invoice_hour < 4 THEN 1 ELSE 0 END AS is_core
    FROM base
    WHERE mst_seller IS NOT NULL
      AND invoice_ts IS NOT NULL
)
SELECT
    mst_seller,
    report_date,
    -- Sales metrics
    SUM(total_sales)                          AS daily_total_sales,
    COUNT(*)                                  AS daily_invoice_count,
    COUNT(DISTINCT mst_buyer)                 AS daily_buyer_count,
    -- Financial
    SUM(tax_amt)                              AS daily_total_tax,
    SUM(discount_amt)                         AS daily_total_discount,
    SUM(total_without_vat)                    AS daily_total_without_vat,
    SUM(total_with_vat)                       AS daily_total_with_vat,
    -- Night / core
    SUM(is_night)                             AS daily_night_invoice_count,
    SUM(is_core)                              AS daily_core_invoice_count,
    -- Diversity
    COUNT(DISTINCT payment_method)            AS daily_distinct_payment_methods,
    COUNT(DISTINCT transport)                 AS daily_distinct_transport,
    -- Invoice value distribution
    MAX(total_sales)                          AS daily_max_invoice_value,
    MIN(total_sales)                          AS daily_min_invoice_value,
    AVG(total_sales)                          AS daily_avg_invoice_value,
    STDDEV(total_sales)                       AS daily_std_invoice_value,
    -- Derived ratios
    CASE WHEN COUNT(*) > 0
         THEN CAST(SUM(is_night) AS DOUBLE) / COUNT(*)
         ELSE 0.0 END                        AS daily_night_ratio,
    CASE WHEN COUNT(*) > 0
         THEN CAST(SUM(is_core) AS DOUBLE) / COUNT(*)
         ELSE 0.0 END                        AS daily_core_ratio,
    CASE WHEN SUM(total_sales) > 0
         THEN COALESCE(SUM(discount_amt), 0) / SUM(total_sales)
         ELSE 0.0 END                        AS daily_discount_ratio,
    CASE WHEN COUNT(DISTINCT mst_buyer) > 0
         THEN SUM(total_sales) / COUNT(DISTINCT mst_buyer)
         ELSE 0.0 END                        AS daily_sales_per_buyer,
    CASE WHEN COUNT(*) > 0
         THEN SUM(total_sales) / COUNT(*)
         ELSE 0.0 END                        AS daily_sales_per_invoice
FROM with_flags
GROUP BY mst_seller, report_date
"""


# =============================================================================
# STEP 2: MONTHLY AGGREGATION VIEW
# =============================================================================

SQL_MONTHLY = """
CREATE OR REPLACE TEMPORARY VIEW vt_monthly AS
SELECT
    mst_seller,
    LAST_DAY(report_date) AS month_end,
    -- Monthly totals
    SUM(daily_total_sales)          AS monthly_total_sales,
    SUM(daily_invoice_count)        AS monthly_invoice_count,
    SUM(daily_buyer_count)          AS monthly_total_buyer_touches,
    AVG(daily_total_sales)          AS monthly_avg_daily_sales,
    AVG(daily_invoice_count)        AS monthly_avg_daily_invoices,
    AVG(daily_buyer_count)          AS monthly_avg_daily_buyers,
    -- Night metrics (monthly avg)
    AVG(daily_night_ratio)          AS monthly_avg_night_ratio,
    AVG(daily_core_ratio)           AS monthly_avg_core_ratio,
    SUM(daily_night_invoice_count)  AS monthly_night_invoices,
    -- Activity
    COUNT(DISTINCT report_date)     AS monthly_active_days,
    -- Discount
    SUM(daily_total_discount)       AS monthly_total_discount,
    AVG(daily_discount_ratio)       AS monthly_avg_discount_ratio
FROM vt_stg_daily
GROUP BY mst_seller, LAST_DAY(report_date)
"""


# =============================================================================
# STEP 3: BUYER AGGREGATION VIEW
# =============================================================================

SQL_BUYER_AGG = """
CREATE OR REPLACE TEMPORARY VIEW vt_buyer_agg AS
WITH buyer_monthly AS (
    SELECT
        TRIM(CAST(iv1.col21 AS STRING)) AS mst_seller,
        TRIM(CAST(iv1.col29 AS STRING)) AS mst_buyer,
        DATE_TRUNC('month', COALESCE(TO_TIMESTAMP(iv1.col23), TO_TIMESTAMP(iv1.col4))) AS month_start,
        SUM(
            CASE
                WHEN CAST(iv1.col10 AS INT) = 9
                    THEN COALESCE(CAST(iv1.col55 AS DOUBLE), 0) + COALESCE(CAST(iv2.col55 AS DOUBLE), 0)
                WHEN CAST(iv1.col10 AS INT) = 3
                    THEN COALESCE(CAST(iv2.col55 AS DOUBLE), 0)
                ELSE COALESCE(CAST(iv1.col55 AS DOUBLE), 0)
            END
        ) AS buyer_monthly_sales,
        COUNT(*) AS buyer_monthly_invoice_count
    FROM {table_invoice} iv1
    LEFT JOIN {table_invoice} iv2
        ON iv1.col129 = iv2.col129 AND iv1.col72 = iv2.col1
    WHERE CAST(iv1.col9 AS INT) = 1
      AND TRIM(CAST(iv1.col21 AS STRING)) IS NOT NULL
      AND TRIM(CAST(iv1.col29 AS STRING)) IS NOT NULL
    GROUP BY 1, 2, 3
),
buyer_total AS (
    SELECT
        mst_seller, mst_buyer,
        SUM(buyer_monthly_sales) AS total_buyer_sales
    FROM buyer_monthly
    WHERE month_start >= DATE('{l12m}')
      AND month_start <= DATE('{last_date}')
    GROUP BY mst_seller, mst_buyer
),
buyer_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY mst_seller ORDER BY total_buyer_sales DESC) AS buyer_rank
    FROM buyer_total
),
seller_total AS (
    SELECT mst_seller,
        SUM(total_buyer_sales) AS seller_total_sales_l12m,
        COUNT(DISTINCT mst_buyer) AS distinct_buyers_l12m
    FROM buyer_total
    GROUP BY mst_seller
),
top3 AS (
    SELECT mst_seller, SUM(total_buyer_sales) AS top3_buyer_sales_l12m
    FROM buyer_ranked WHERE buyer_rank <= 3
    GROUP BY mst_seller
),
top5 AS (
    SELECT mst_seller, SUM(total_buyer_sales) AS top5_buyer_sales_l12m
    FROM buyer_ranked WHERE buyer_rank <= 5
    GROUP BY mst_seller
)
SELECT
    s.mst_seller,
    s.seller_total_sales_l12m,
    s.distinct_buyers_l12m,
    t3.top3_buyer_sales_l12m,
    t5.top5_buyer_sales_l12m,
    CASE WHEN s.seller_total_sales_l12m > 0
         THEN t3.top3_buyer_sales_l12m / s.seller_total_sales_l12m
         ELSE 0.0 END AS top3_buyer_concentration_l12m,
    CASE WHEN s.seller_total_sales_l12m > 0
         THEN t5.top5_buyer_sales_l12m / s.seller_total_sales_l12m
         ELSE 0.0 END AS top5_buyer_concentration_l12m
FROM seller_total s
LEFT JOIN top3 t3 ON s.mst_seller = t3.mst_seller
LEFT JOIN top5 t5 ON s.mst_seller = t5.mst_seller
"""


# =============================================================================
# STEP 4: FINAL FEATURES - Multi-period aggregation
# =============================================================================

def generate_period_agg(col, funcs, periods_dict, source="vt_stg_daily", date_col="report_date"):
    """Generate SQL aggregation expressions for multiple periods."""
    lines = []
    for func in funcs:
        for period_name, period_date in periods_dict.items():
            alias = f"{col}_{func}_{period_name}"
            sql_func = func.upper()
            if func == "std":
                sql_func = "STDDEV"
            elif func == "med":
                sql_func = "PERCENTILE"
            elif func == "countDistinct":
                lines.append(
                    f"    COUNT(DISTINCT CASE WHEN {date_col} >= DATE('{period_date}') THEN {col} END) AS {alias}"
                )
                continue

            if func == "med":
                lines.append(
                    f"    PERCENTILE({col}, 0.5) FILTER (WHERE {date_col} >= DATE('{period_date}')) AS {alias}"
                )
            else:
                lines.append(
                    f"    {sql_func}(CASE WHEN {date_col} >= DATE('{period_date}') THEN {col} END) AS {alias}"
                )
    return ",\n".join(lines)


SQL_FINAL_FEATURES = """
CREATE OR REPLACE TEMPORARY VIEW vt_features AS
WITH
-- Gap calculation
daily_with_gap AS (
    SELECT *,
        DATEDIFF(report_date, LAG(report_date) OVER (PARTITION BY mst_seller ORDER BY report_date)) AS days_gap
    FROM vt_stg_daily
    WHERE report_date >= DATE('{l12m}')
      AND report_date <= DATE('{last_date}')
),
-- GROUP 1: Daily sales features (multi-period)
ft_sales AS (
    SELECT mst_seller,
{sales_agg}
    FROM daily_with_gap
    GROUP BY mst_seller
),
-- GROUP 2: Night sale features
ft_night AS (
    SELECT mst_seller,
{night_agg}
    FROM daily_with_gap
    GROUP BY mst_seller
),
-- GROUP 3: Discount features
ft_discount AS (
    SELECT mst_seller,
{discount_agg}
    FROM daily_with_gap
    GROUP BY mst_seller
),
-- GROUP 4: Invoice value distribution
ft_value AS (
    SELECT mst_seller,
{value_agg}
    FROM daily_with_gap
    GROUP BY mst_seller
),
-- GROUP 5: Activity pattern features
ft_activity AS (
    SELECT mst_seller,
{activity_agg},
{active_days_agg}
    FROM daily_with_gap
    GROUP BY mst_seller
),
-- GROUP 6: Monthly -> multi-period
ft_monthly AS (
    SELECT mst_seller,
{monthly_agg}
    FROM vt_monthly
    WHERE month_end >= DATE('{l12m}')
      AND month_end <= DATE('{last_date}')
    GROUP BY mst_seller
),
-- GROUP 7: Night flagging
ft_night_flag AS (
    SELECT mst_seller,
        SUM(CASE WHEN monthly_avg_night_ratio >= 0.30 AND monthly_avg_daily_invoices >= 100 THEN 1 ELSE 0 END)
            AS night_flag_qualified_months_l12m,
        AVG(monthly_avg_night_ratio) AS avg_night_ratio_l12m,
        MAX(monthly_avg_night_ratio) AS max_night_ratio_l12m,
        AVG(monthly_avg_core_ratio)  AS avg_core_ratio_l12m,
        CASE WHEN SUM(CASE WHEN monthly_avg_night_ratio >= 0.30 AND monthly_avg_daily_invoices >= 100 THEN 1 ELSE 0 END) >= 2
             THEN TRUE ELSE FALSE END AS is_night_flagged
    FROM vt_monthly
    WHERE month_end >= DATE('{l12m}')
      AND month_end <= DATE('{last_date}')
    GROUP BY mst_seller
),
-- GROUP 8: Top-5 sales days concentration
ft_top_days AS (
    SELECT mst_seller,
        SUM(daily_total_sales) AS top5_days_sales_l12m
    FROM (
        SELECT mst_seller, daily_total_sales,
            ROW_NUMBER() OVER (PARTITION BY mst_seller ORDER BY daily_total_sales DESC) AS rn
        FROM vt_stg_daily
        WHERE report_date >= DATE('{l12m}')
          AND report_date <= DATE('{last_date}')
    )
    WHERE rn <= 5
    GROUP BY mst_seller
)
-- FINAL JOIN
SELECT
    DATE('{last_date}') AS report_date,
    s.mst_seller,
    -- Sales features
    s.*,
    -- Night features
    n.*,
    -- Discount features
    d.*,
    -- Value distribution
    v.*,
    -- Activity features
    a.*,
    -- Monthly features
    m.*,
    -- Buyer concentration
    b.seller_total_sales_l12m,
    b.distinct_buyers_l12m,
    b.top3_buyer_sales_l12m,
    b.top5_buyer_sales_l12m,
    b.top3_buyer_concentration_l12m,
    b.top5_buyer_concentration_l12m,
    -- Night flagging
    nf.night_flag_qualified_months_l12m,
    nf.avg_night_ratio_l12m,
    nf.max_night_ratio_l12m,
    nf.avg_core_ratio_l12m,
    nf.is_night_flagged,
    -- Top days concentration
    td.top5_days_sales_l12m,
    CASE WHEN b.seller_total_sales_l12m > 0
         THEN td.top5_days_sales_l12m / b.seller_total_sales_l12m
         ELSE 0.0 END AS top5_days_concentration_l12m
FROM ft_sales s
LEFT JOIN ft_night n ON s.mst_seller = n.mst_seller
LEFT JOIN ft_discount d ON s.mst_seller = d.mst_seller
LEFT JOIN ft_value v ON s.mst_seller = v.mst_seller
LEFT JOIN ft_activity a ON s.mst_seller = a.mst_seller
LEFT JOIN ft_monthly m ON s.mst_seller = m.mst_seller
LEFT JOIN vt_buyer_agg b ON s.mst_seller = b.mst_seller
LEFT JOIN ft_night_flag nf ON s.mst_seller = nf.mst_seller
LEFT JOIN ft_top_days td ON s.mst_seller = td.mst_seller
"""


# =============================================================================
# EXECUTION
# =============================================================================

def run_pipeline(run_date=None, invoice_table=None, output_stg=None, output_ft=None):
    """Execute the full SQL pipeline."""
    if invoice_table is None:
        invoice_table = TABLE_INVOICE
    if output_stg is None:
        output_stg = TABLE_STG_OUTPUT
    if output_ft is None:
        output_ft = TABLE_FT_OUTPUT

    if run_date is None:
        run_date = datetime.date.today()

    dates = get_period_dates(run_date)
    print(f"Run date: {run_date}")
    print(f"Periods: {dates}")

    # --- Step 1: Staging ---
    print("Step 1: Building staging view...")
    sql_stg = SQL_STAGING.format(table_invoice=invoice_table)
    spark.sql(sql_stg)
    print("  Staging view created: vt_stg_daily")

    # Persist staging if needed
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_stg} AS SELECT * FROM vt_stg_daily WHERE 1=0
    """)
    spark.sql(f"""
        INSERT OVERWRITE TABLE {output_stg} SELECT * FROM vt_stg_daily
    """)
    print(f"  Staging written to {output_stg}")

    # --- Step 2: Monthly view ---
    print("Step 2: Building monthly view...")
    spark.sql(SQL_MONTHLY)
    print("  Monthly view created: vt_monthly")

    # --- Step 3: Buyer aggregation ---
    print("Step 3: Building buyer aggregation...")
    sql_buyer = SQL_BUYER_AGG.format(
        table_invoice=invoice_table,
        l12m=dates["l12m"],
        last_date=dates["last_date"],
    )
    spark.sql(sql_buyer)
    print("  Buyer agg view created: vt_buyer_agg")

    # --- Step 4: Final features ---
    print("Step 4: Building final features...")

    period_dates = {
        "l1m": dates["l1m"],
        "l3m": dates["l3m"],
        "l6m": dates["l6m"],
        "l12m": dates["l12m"],
    }

    # Generate aggregation SQL fragments
    sales_agg = generate_period_agg(
        "daily_total_sales", ["sum", "avg", "min", "max", "std"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_invoice_count", ["sum", "avg", "min", "max"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_buyer_count", ["sum", "avg", "min", "max"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_sales_per_buyer", ["avg", "max", "std"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_sales_per_invoice", ["avg", "max", "std"], period_dates
    )

    night_agg = generate_period_agg(
        "daily_night_ratio", ["avg", "max", "std"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_core_ratio", ["avg", "max", "std"], period_dates
    )

    discount_agg = generate_period_agg(
        "daily_discount_ratio", ["sum", "avg", "max"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_total_discount", ["sum", "avg", "max"], period_dates
    )

    value_agg = generate_period_agg(
        "daily_avg_invoice_value", ["avg", "max", "min", "std"], period_dates
    ) + ",\n" + generate_period_agg(
        "daily_max_invoice_value", ["avg", "max", "min", "std"], period_dates
    )

    activity_agg = generate_period_agg(
        "days_gap", ["avg", "max", "min", "std"], period_dates
    )

    active_days_parts = []
    for period_name, period_date in period_dates.items():
        active_days_parts.append(
            f"    COUNT(DISTINCT CASE WHEN report_date >= DATE('{period_date}') THEN report_date END) AS active_days_{period_name}"
        )
    active_days_agg = ",\n".join(active_days_parts)

    monthly_agg = generate_period_agg(
        "monthly_total_sales", ["avg", "min", "max", "std"], period_dates, date_col="month_end"
    ) + ",\n" + generate_period_agg(
        "monthly_invoice_count", ["avg", "min", "max", "std"], period_dates, date_col="month_end"
    ) + ",\n" + generate_period_agg(
        "monthly_total_buyer_touches", ["avg", "min", "max"], period_dates, date_col="month_end"
    )

    sql_ft = SQL_FINAL_FEATURES.format(
        l12m=dates["l12m"],
        last_date=dates["last_date"],
        sales_agg=sales_agg,
        night_agg=night_agg,
        discount_agg=discount_agg,
        value_agg=value_agg,
        activity_agg=activity_agg,
        active_days_agg=active_days_agg,
        monthly_agg=monthly_agg,
    )

    spark.sql(sql_ft)
    print("  Features view created: vt_features")

    # Write final features
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_ft} AS SELECT * FROM vt_features WHERE 1=0
    """)
    spark.sql(f"""
        INSERT OVERWRITE TABLE {output_ft} SELECT * FROM vt_features
    """)
    print(f"  Features written to {output_ft}")
    print("Done.")


# =============================================================================
# STANDALONE SQL EXPORT - Pure SQL for non-Spark environments
# =============================================================================

def export_pure_sql(run_date=None, invoice_table=None, output_file="viettel_ft_pure.sql"):
    """
    Export the entire pipeline as a single pure SQL script that can be run
    directly in any SQL environment (Databricks SQL, Hive, Trino, etc.)
    """
    if invoice_table is None:
        invoice_table = TABLE_INVOICE
    if run_date is None:
        run_date = datetime.date.today()

    dates = get_period_dates(run_date)
    period_dates = {
        "l1m": dates["l1m"],
        "l3m": dates["l3m"],
        "l6m": dates["l6m"],
        "l12m": dates["l12m"],
    }

    # Build all agg fragments
    sales_agg = generate_period_agg("daily_total_sales", ["sum", "avg", "min", "max", "std"], period_dates)
    sales_agg += ",\n" + generate_period_agg("daily_invoice_count", ["sum", "avg", "min", "max"], period_dates)
    sales_agg += ",\n" + generate_period_agg("daily_buyer_count", ["sum", "avg", "min", "max"], period_dates)
    sales_agg += ",\n" + generate_period_agg("daily_sales_per_buyer", ["avg", "max", "std"], period_dates)
    sales_agg += ",\n" + generate_period_agg("daily_sales_per_invoice", ["avg", "max", "std"], period_dates)

    night_agg = generate_period_agg("daily_night_ratio", ["avg", "max", "std"], period_dates)
    night_agg += ",\n" + generate_period_agg("daily_core_ratio", ["avg", "max", "std"], period_dates)

    discount_agg = generate_period_agg("daily_discount_ratio", ["sum", "avg", "max"], period_dates)
    discount_agg += ",\n" + generate_period_agg("daily_total_discount", ["sum", "avg", "max"], period_dates)

    value_agg = generate_period_agg("daily_avg_invoice_value", ["avg", "max", "min", "std"], period_dates)
    value_agg += ",\n" + generate_period_agg("daily_max_invoice_value", ["avg", "max", "min", "std"], period_dates)

    activity_agg = generate_period_agg("days_gap", ["avg", "max", "min", "std"], period_dates)

    active_days_parts = []
    for period_name, period_date in period_dates.items():
        active_days_parts.append(
            f"    COUNT(DISTINCT CASE WHEN report_date >= DATE('{period_date}') THEN report_date END) AS active_days_{period_name}"
        )
    active_days_agg = ",\n".join(active_days_parts)

    monthly_agg = generate_period_agg("monthly_total_sales", ["avg", "min", "max", "std"], period_dates, date_col="month_end")
    monthly_agg += ",\n" + generate_period_agg("monthly_invoice_count", ["avg", "min", "max", "std"], period_dates, date_col="month_end")
    monthly_agg += ",\n" + generate_period_agg("monthly_total_buyer_touches", ["avg", "min", "max"], period_dates, date_col="month_end")

    sql_stg = SQL_STAGING.format(table_invoice=invoice_table)
    sql_buyer = SQL_BUYER_AGG.format(
        table_invoice=invoice_table,
        l12m=dates["l12m"],
        last_date=dates["last_date"],
    )
    sql_ft = SQL_FINAL_FEATURES.format(
        l12m=dates["l12m"],
        last_date=dates["last_date"],
        sales_agg=sales_agg,
        night_agg=night_agg,
        discount_agg=discount_agg,
        value_agg=value_agg,
        activity_agg=activity_agg,
        active_days_agg=active_days_agg,
        monthly_agg=monthly_agg,
    )

    full_sql = f"""-- ============================================================
-- Viettel Invoice Feature Engineering - Pure SQL
-- Generated: {datetime.datetime.now().isoformat()}
-- Run date: {run_date}
-- Periods: l1m={dates['l1m']}, l3m={dates['l3m']}, l6m={dates['l6m']}, l12m={dates['l12m']}
-- ============================================================

-- STEP 1: Staging (daily seller-level aggregation)
{sql_stg};

-- STEP 2: Monthly aggregation
{SQL_MONTHLY};

-- STEP 3: Buyer aggregation & concentration
{sql_buyer};

-- STEP 4: Final multi-period features
{sql_ft};

-- Output: SELECT * FROM vt_features;
"""
    with open(output_file, "w") as fh:
        fh.write(full_sql)
    print(f"Pure SQL exported to: {output_file}")
    return full_sql


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Viettel Feature Engineering - SQL Version")
    parser.add_argument("--invoice-table", default=TABLE_INVOICE, help="Input invoice table")
    parser.add_argument("--stg-table", default=TABLE_STG_OUTPUT, help="Staging output table")
    parser.add_argument("--ft-table", default=TABLE_FT_OUTPUT, help="Feature output table")
    parser.add_argument("--run-date", default=None, help="Run date YYYY-MM-DD")
    parser.add_argument("--export-sql", action="store_true", help="Export pure SQL file instead of running")
    parser.add_argument("--sql-output", default="viettel_ft_pure.sql", help="Pure SQL output filename")
    args = parser.parse_args()

    run_date = None
    if args.run_date:
        run_date = datetime.datetime.strptime(args.run_date, "%Y-%m-%d").date()

    if args.export_sql:
        export_pure_sql(
            run_date=run_date,
            invoice_table=args.invoice_table,
            output_file=args.sql_output,
        )
    else:
        run_pipeline(
            run_date=run_date,
            invoice_table=args.invoice_table,
            output_stg=args.stg_table,
            output_ft=args.ft_table,
        )
