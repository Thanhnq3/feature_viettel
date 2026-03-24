"""
Viettel Invoice Feature Engineering - PySpark Version
=====================================================
Combines dev_ft_viettel_1, dev_ft_viettel_2, dev_ft_viettel_3 into a single
structured pipeline following the staging -> final feature pattern.

Pipeline:
    Step 1 (Staging): Raw invoice data -> Daily seller-level aggregation
    Step 2 (Final):   Daily -> Monthly -> Multi-period features (l1m, l3m, l6m, l12m)

Input:  Viettel invoice table (col1..col129)
Output: Feature table keyed by seller MST (col21) + report_date

Column Reference (from viettel_data.xlsx):
    col1:   invoice_id (PK)
    col4:   record creation timestamp
    col9:   invoice status (1=issued)
    col10:  adjustment type (1=original, 3=replacement, 5=info_adj, 7=cancelled, 9=monetary_adj)
    col11:  adjustment status (0=none, 1=info_adj, 2=monetary_adj, 3=replaced)
    col21:  seller MST (tax code)
    col23:  invoice issue timestamp
    col29:  buyer MST
    col30:  transport method
    col33:  buyer address
    col34:  buyer email
    col35:  buyer phone
    col47:  total tax amount
    col48:  discount amount
    col49:  settlement discount
    col52:  total amount with VAT (VND)
    col53:  total amount without VAT
    col55:  total amount with VAT (foreign currency)
    col65:  currency
    col72:  original invoice number (for adj/replacement invoices)
    col79:  actual payment method
    col94:  item list (JSON)
    col129: partition (YYYYMM)
"""

import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

spark = SparkSession.builder.appName("viettel_feature_engineering").getOrCreate()


# =============================================================================
# CONFIG
# =============================================================================

# --- Paths (adapt to your environment) ---
INVOICE_TABLE = "your_schema.viettel_invoice"       # raw invoice table
STG_TABLE = "your_schema.viettel_stg_daily"          # staging output
FT_TABLE = "your_schema.viettel_ft_seller"           # final feature output

# --- Column aliases for readability ---
COL_INVOICE_ID = "col1"
COL_RECORD_TS = "col4"
COL_INVOICE_STATUS = "col9"
COL_ADJ_TYPE = "col10"
COL_ADJ_STATUS = "col11"
COL_SELLER_MST = "col21"
COL_INVOICE_ISSUE_TS = "col23"
COL_BUYER_MST = "col29"
COL_TRANSPORT = "col30"
COL_BUYER_NAME = "col31"
COL_BUYER_ADDRESS = "col33"
COL_BUYER_EMAIL = "col34"
COL_BUYER_PHONE = "col35"
COL_TAX_AMT = "col47"
COL_DISCOUNT = "col48"
COL_SETTLEMENT_DISCOUNT = "col49"
COL_TOTAL_WITH_VAT = "col52"
COL_TOTAL_WITHOUT_VAT = "col53"
COL_TOTAL_VAT_FRN = "col55"
COL_CURRENCY = "col65"
COL_ORIG_INVOICE = "col72"
COL_PAYMENT_METHOD = "col79"
COL_ITEM_JSON = "col94"
COL_PARTITION = "col129"

# Night sale thresholds (from dev_ft_viettel_1)
NIGHT_START = 22
NIGHT_END = 6
CORE_START = 0
CORE_END = 4


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def f_last_day_of_previous_month(data_date):
    return data_date.replace(day=1) - datetime.timedelta(days=1)

def f_first_day_of_previous_month(data_date, nbr_of_mth: int):
    return data_date.replace(day=1) - relativedelta(months=nbr_of_mth)


def compute_adjusted_sales(df: DataFrame) -> DataFrame:
    """
    Compute total_sales handling adjustment and replacement invoices.
    Logic from dev_ft_viettel_3:
        - col10=9 (monetary adjustment): total = iv1.col55 + iv2.col55
        - col10=3 (replacement): total = iv2.col55 (original)
        - else: total = iv1.col55
    Where iv2 is the original invoice joined via col72 -> col1.
    """
    iv1 = df.alias("iv1")
    iv2 = df.alias("iv2")

    joined = iv1.join(
        iv2.select(
            F.col(COL_INVOICE_ID).alias("_orig_id"),
            F.col(COL_TOTAL_VAT_FRN).alias("_orig_total"),
            F.col(COL_PARTITION).alias("_orig_part"),
        ),
        on=(
            (F.col(f"iv1.{COL_ORIG_INVOICE}") == F.col("_orig_id"))
            & (F.col(f"iv1.{COL_PARTITION}") == F.col("_orig_part"))
        ),
        how="left",
    )

    joined = joined.withColumn(
        "total_sales",
        F.when(
            F.col(f"iv1.{COL_ADJ_TYPE}") == 9,
            F.coalesce(F.col(f"iv1.{COL_TOTAL_VAT_FRN}").cast("double"), F.lit(0.0))
            + F.coalesce(F.col("_orig_total").cast("double"), F.lit(0.0)),
        )
        .when(
            F.col(f"iv1.{COL_ADJ_TYPE}") == 3,
            F.coalesce(F.col("_orig_total").cast("double"), F.lit(0.0)),
        )
        .otherwise(
            F.coalesce(F.col(f"iv1.{COL_TOTAL_VAT_FRN}").cast("double"), F.lit(0.0))
        ),
    )
    return joined


# =============================================================================
# STEP 1: STAGING - Daily seller-level aggregation
# =============================================================================

def build_staging(df_raw: DataFrame) -> DataFrame:
    """
    From raw invoice data, produce daily seller-level metrics.
    Filters: only issued invoices (col9 = 1).

    Output columns:
        mst_seller, report_date,
        daily_total_sales, daily_invoice_count, daily_buyer_count,
        daily_total_tax, daily_total_discount,
        daily_total_without_vat, daily_total_with_vat,
        daily_night_invoice_count, daily_core_invoice_count,
        daily_distinct_payment_methods, daily_distinct_transport,
        daily_item_count, daily_max_invoice_value, daily_min_invoice_value,
        daily_avg_invoice_value
    """
    # Filter issued invoices only
    df = df_raw.filter(F.col(COL_INVOICE_STATUS).cast("int") == 1)

    # Compute adjusted sales
    df = compute_adjusted_sales(df)

    # Derive columns
    df = (
        df
        .withColumn("mst_seller", F.trim(F.col(f"iv1.{COL_SELLER_MST}").cast("string")))
        .withColumn("mst_buyer", F.trim(F.col(f"iv1.{COL_BUYER_MST}").cast("string")))
        .withColumn(
            "invoice_ts",
            F.coalesce(
                F.to_timestamp(F.col(f"iv1.{COL_INVOICE_ISSUE_TS}")),
                F.to_timestamp(F.col(f"iv1.{COL_RECORD_TS}")),
            ),
        )
        .withColumn("report_date", F.to_date("invoice_ts"))
        .withColumn("hour", F.hour("invoice_ts"))
        .withColumn(
            "is_night",
            (F.col("hour") >= F.lit(NIGHT_START)) | (F.col("hour") < F.lit(NIGHT_END)),
        )
        .withColumn(
            "is_core",
            (F.col("hour") >= F.lit(CORE_START)) & (F.col("hour") < F.lit(CORE_END)),
        )
        .withColumn("tax_amt", F.col(f"iv1.{COL_TAX_AMT}").cast("double"))
        .withColumn("discount_amt", F.col(f"iv1.{COL_DISCOUNT}").cast("double"))
        .withColumn("total_without_vat", F.col(f"iv1.{COL_TOTAL_WITHOUT_VAT}").cast("double"))
        .withColumn("total_with_vat", F.col(f"iv1.{COL_TOTAL_WITH_VAT}").cast("double"))
        .withColumn("payment_method", F.col(f"iv1.{COL_PAYMENT_METHOD}"))
        .withColumn("transport", F.col(f"iv1.{COL_TRANSPORT}"))
    )

    df = df.filter(F.col("mst_seller").isNotNull() & F.col("report_date").isNotNull())

    # Daily aggregation per seller
    df_daily = (
        df
        .groupBy("mst_seller", "report_date")
        .agg(
            # Sales
            F.sum("total_sales").alias("daily_total_sales"),
            F.count(F.lit(1)).alias("daily_invoice_count"),
            F.countDistinct("mst_buyer").alias("daily_buyer_count"),
            # Financial
            F.sum("tax_amt").alias("daily_total_tax"),
            F.sum("discount_amt").alias("daily_total_discount"),
            F.sum("total_without_vat").alias("daily_total_without_vat"),
            F.sum("total_with_vat").alias("daily_total_with_vat"),
            # Night / core
            F.sum(F.when(F.col("is_night"), F.lit(1)).otherwise(F.lit(0))).alias("daily_night_invoice_count"),
            F.sum(F.when(F.col("is_core"), F.lit(1)).otherwise(F.lit(0))).alias("daily_core_invoice_count"),
            # Diversity
            F.countDistinct("payment_method").alias("daily_distinct_payment_methods"),
            F.countDistinct("transport").alias("daily_distinct_transport"),
            # Invoice value distribution
            F.max("total_sales").alias("daily_max_invoice_value"),
            F.min("total_sales").alias("daily_min_invoice_value"),
            F.avg("total_sales").alias("daily_avg_invoice_value"),
            F.stddev("total_sales").alias("daily_std_invoice_value"),
        )
    )

    # Add derived ratios
    df_daily = df_daily.withColumn(
        "daily_night_ratio",
        F.when(F.col("daily_invoice_count") > 0,
               F.col("daily_night_invoice_count") / F.col("daily_invoice_count"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "daily_core_ratio",
        F.when(F.col("daily_invoice_count") > 0,
               F.col("daily_core_invoice_count") / F.col("daily_invoice_count"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "daily_discount_ratio",
        F.when(F.col("daily_total_sales") > 0,
               F.coalesce(F.col("daily_total_discount"), F.lit(0.0)) / F.col("daily_total_sales"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "daily_sales_per_buyer",
        F.when(F.col("daily_buyer_count") > 0,
               F.col("daily_total_sales") / F.col("daily_buyer_count"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "daily_sales_per_invoice",
        F.when(F.col("daily_invoice_count") > 0,
               F.col("daily_total_sales") / F.col("daily_invoice_count"))
        .otherwise(F.lit(0.0)),
    )

    return df_daily


# =============================================================================
# STEP 2: BUYER-LEVEL STAGING (for buyer-specific features per seller)
# =============================================================================

def build_buyer_staging(df_raw: DataFrame) -> DataFrame:
    """
    Daily buyer-seller pair aggregation for buyer concentration features.
    """
    df = df_raw.filter(F.col(COL_INVOICE_STATUS).cast("int") == 1)
    df = compute_adjusted_sales(df)

    df = (
        df
        .withColumn("mst_seller", F.trim(F.col(f"iv1.{COL_SELLER_MST}").cast("string")))
        .withColumn("mst_buyer", F.trim(F.col(f"iv1.{COL_BUYER_MST}").cast("string")))
        .withColumn(
            "invoice_ts",
            F.coalesce(
                F.to_timestamp(F.col(f"iv1.{COL_INVOICE_ISSUE_TS}")),
                F.to_timestamp(F.col(f"iv1.{COL_RECORD_TS}")),
            ),
        )
        .withColumn("report_date", F.to_date("invoice_ts"))
        .withColumn("month_start", F.date_trunc("month", F.col("report_date")))
    )

    # Monthly buyer-seller aggregation
    df_buyer_monthly = (
        df
        .filter(F.col("mst_seller").isNotNull() & F.col("mst_buyer").isNotNull())
        .groupBy("mst_seller", "mst_buyer", "month_start")
        .agg(
            F.sum("total_sales").alias("buyer_monthly_sales"),
            F.count(F.lit(1)).alias("buyer_monthly_invoice_count"),
        )
    )
    return df_buyer_monthly


# =============================================================================
# STEP 3: FINAL FEATURES - Multi-period aggregation
# =============================================================================

def build_agg(df, group_by, columns, functions, conditions):
    """
    Generic aggregation builder following tcb_ft_final pattern.
    conditions: list of [filter_expr, suffix_str]

    Spark 2.4.4 compatible: percentile functions (med, pct25, pct75) use
    F.expr("percentile_approx(...)") via SQL, which requires pre-materialized
    temp columns since F.when() Column objects can't be embedded in SQL strings.
    """
    PERCENTILE_MAP = {"med": 0.5, "pct25": 0.25, "pct75": 0.75}
    needs_percentile = any(f in PERCENTILE_MAP for f in functions)

    # Pre-add temp columns for percentile computation (SQL expr needs column names)
    tmp_col_names = []
    if needs_percentile:
        for col_name in columns:
            for cond_expr, suffix in conditions:
                tmp_name = f"__tmp_{col_name}{suffix}"
                df = df.withColumn(tmp_name, F.when(cond_expr, F.col(col_name)).cast("double"))
                tmp_col_names.append(tmp_name)

    agg_list = []
    for col_name in columns:
        for func_name in functions:
            for cond_expr, suffix in conditions:
                ft_name = f"{col_name}_{func_name}{suffix}"
                if func_name in PERCENTILE_MAP:
                    tmp_name = f"__tmp_{col_name}{suffix}"
                    pct = PERCENTILE_MAP[func_name]
                    agg_list.append(
                        F.expr(f"percentile_approx(`{tmp_name}`, {pct})").alias(ft_name)
                    )
                else:
                    expr = F.when(cond_expr, F.col(col_name))
                    if func_name == "sum":
                        agg_list.append(F.sum(expr).alias(ft_name))
                    elif func_name == "avg":
                        agg_list.append(F.avg(expr).alias(ft_name))
                    elif func_name == "min":
                        agg_list.append(F.min(expr).alias(ft_name))
                    elif func_name == "max":
                        agg_list.append(F.max(expr).alias(ft_name))
                    elif func_name == "std":
                        agg_list.append(F.stddev(expr).alias(ft_name))
                    elif func_name == "kurt":
                        agg_list.append(F.kurtosis(expr).alias(ft_name))
                    elif func_name == "skew":
                        agg_list.append(F.skewness(expr).alias(ft_name))
                    elif func_name == "count":
                        agg_list.append(F.count(expr).alias(ft_name))
                    elif func_name == "countDistinct":
                        agg_list.append(F.countDistinct(expr).alias(ft_name))

    result = df.groupBy(*group_by).agg(*agg_list)

    # Drop temp columns (they disappear after groupBy, but be explicit)
    for tmp_name in tmp_col_names:
        if tmp_name in result.columns:
            result = result.drop(tmp_name)

    return result


def build_final_features(df_daily: DataFrame, df_buyer_monthly: DataFrame, run_date) -> DataFrame:
    """
    Build multi-period seller features from daily staging data.
    Following tcb_ft_final pattern: daily -> monthly -> multi-period.
    """
    set_last_date = f_last_day_of_previous_month(run_date)
    periods = {
        "l1m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=0),
        "l3m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=2),
        "l6m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=5),
        "l12m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=11),
    }

    # Filter to last 12 months
    df_daily = (
        df_daily
        .filter(F.col("report_date") >= periods["l12m"])
        .filter(F.col("report_date") <= set_last_date)
        .repartition("mst_seller")
        .cache()
    )

    time_conditions = [
        [F.col("report_date") >= periods[k], f"_{k}"] for k in periods
    ]

    # =========================================================================
    # GROUP 1: Daily -> Direct multi-period aggregation (sales, invoices, buyers)
    # =========================================================================
    sales_cols = [
        "daily_total_sales",
        "daily_invoice_count",
        "daily_buyer_count",
        "daily_sales_per_buyer",
        "daily_sales_per_invoice",
    ]
    sales_funcs = ["sum", "avg", "min", "max", "std"]

    df_ft_sales = build_agg(
        df_daily,
        group_by=[F.col("mst_seller")],
        columns=sales_cols,
        functions=sales_funcs,
        conditions=time_conditions,
    )

    # =========================================================================
    # GROUP 2: Night sale features
    # =========================================================================
    night_cols = ["daily_night_ratio", "daily_core_ratio"]
    night_funcs = ["avg", "max", "std"]

    df_ft_night = build_agg(
        df_daily,
        group_by=[F.col("mst_seller")],
        columns=night_cols,
        functions=night_funcs,
        conditions=time_conditions,
    )

    # =========================================================================
    # GROUP 3: Discount features
    # =========================================================================
    discount_cols = ["daily_discount_ratio", "daily_total_discount"]
    discount_funcs = ["sum", "avg", "max"]

    df_ft_discount = build_agg(
        df_daily,
        group_by=[F.col("mst_seller")],
        columns=discount_cols,
        functions=discount_funcs,
        conditions=time_conditions,
    )

    # =========================================================================
    # GROUP 4: Invoice value distribution features
    # =========================================================================
    value_cols = ["daily_avg_invoice_value", "daily_max_invoice_value"]
    value_funcs = ["avg", "max", "min", "std", "med", "skew"]

    df_ft_value = build_agg(
        df_daily,
        group_by=[F.col("mst_seller")],
        columns=value_cols,
        functions=value_funcs,
        conditions=time_conditions,
    )

    # =========================================================================
    # GROUP 5: Activity pattern features (gaps, active days)
    # =========================================================================
    w_seller = Window.partitionBy("mst_seller").orderBy("report_date")
    df_with_gap = df_daily.withColumn(
        "prev_date", F.lag("report_date").over(w_seller)
    ).withColumn(
        "days_gap", F.datediff(F.col("report_date"), F.col("prev_date"))
    )

    activity_cols = ["days_gap"]
    activity_funcs = ["avg", "max", "min", "std"]

    df_ft_activity = build_agg(
        df_with_gap,
        group_by=[F.col("mst_seller")],
        columns=activity_cols,
        functions=activity_funcs,
        conditions=time_conditions,
    )

    # Active days count per period
    active_days_aggs = []
    for cond_expr, suffix in time_conditions:
        ft_name = f"active_days{suffix}"
        active_days_aggs.append(
            F.countDistinct(F.when(cond_expr, F.col("report_date"))).alias(ft_name)
        )
    df_ft_active_days = df_daily.groupBy(F.col("mst_seller")).agg(*active_days_aggs)

    # =========================================================================
    # GROUP 6: Monthly aggregation -> multi-period (following tcb pattern)
    # =========================================================================
    group_by_monthly = [
        F.col("mst_seller"),
        F.last_day(F.col("report_date")).alias("month_end"),
    ]
    monthly_cols = [
        "daily_total_sales",
        "daily_invoice_count",
        "daily_buyer_count",
    ]
    monthly_funcs = ["sum", "avg"]

    df_monthly = build_agg(
        df_daily,
        group_by=group_by_monthly,
        columns=monthly_cols,
        functions=monthly_funcs,
        conditions=[[F.lit(True), "_mly"]],
    )

    # Rename month_end -> report_date for period filtering
    df_monthly = df_monthly.withColumnRenamed("month_end", "report_date")

    # Monthly -> multi-period
    monthly_agg_cols = [
        "daily_total_sales_sum_mly",
        "daily_invoice_count_sum_mly",
        "daily_buyer_count_sum_mly",
    ]
    monthly_agg_funcs = ["avg", "min", "max", "std"]

    time_conditions_monthly = [
        [F.col("report_date") >= periods[k], f"_{k}"] for k in periods
    ]

    df_ft_monthly = build_agg(
        df_monthly,
        group_by=[F.col("mst_seller")],
        columns=monthly_agg_cols,
        functions=monthly_agg_funcs,
        conditions=time_conditions_monthly,
    )

    # =========================================================================
    # GROUP 7: Top-N buyer concentration (from dev_ft_viettel_1 & 2)
    # =========================================================================
    df_buyer_monthly_filtered = (
        df_buyer_monthly
        .filter(F.col("month_start") >= periods["l12m"])
        .filter(F.col("month_start") <= set_last_date)
    )

    # Per seller: total sales, top-3 buyer sales, top-5 buyer sales
    w_buyer = Window.partitionBy("mst_seller").orderBy(F.col("total_buyer_sales").desc())

    df_buyer_total = (
        df_buyer_monthly_filtered
        .groupBy("mst_seller", "mst_buyer")
        .agg(F.sum("buyer_monthly_sales").alias("total_buyer_sales"))
    )

    df_buyer_ranked = df_buyer_total.withColumn("buyer_rank", F.row_number().over(w_buyer))

    df_seller_total = df_buyer_total.groupBy("mst_seller").agg(
        F.sum("total_buyer_sales").alias("seller_total_sales_l12m"),
        F.countDistinct("mst_buyer").alias("distinct_buyers_l12m"),
    )

    df_top3 = (
        df_buyer_ranked
        .filter(F.col("buyer_rank") <= 3)
        .groupBy("mst_seller")
        .agg(F.sum("total_buyer_sales").alias("top3_buyer_sales_l12m"))
    )

    df_top5 = (
        df_buyer_ranked
        .filter(F.col("buyer_rank") <= 5)
        .groupBy("mst_seller")
        .agg(F.sum("total_buyer_sales").alias("top5_buyer_sales_l12m"))
    )

    df_ft_concentration = (
        df_seller_total
        .join(df_top3, on="mst_seller", how="left")
        .join(df_top5, on="mst_seller", how="left")
        .withColumn(
            "top3_buyer_concentration_l12m",
            F.when(F.col("seller_total_sales_l12m") > 0,
                   F.col("top3_buyer_sales_l12m") / F.col("seller_total_sales_l12m"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "top5_buyer_concentration_l12m",
            F.when(F.col("seller_total_sales_l12m") > 0,
                   F.col("top5_buyer_sales_l12m") / F.col("seller_total_sales_l12m"))
            .otherwise(F.lit(0.0)),
        )
    )

    # =========================================================================
    # GROUP 8: Payment & transport diversity (from dev_ft_viettel_3)
    # =========================================================================
    diversity_cols = ["daily_distinct_payment_methods", "daily_distinct_transport"]
    diversity_funcs = ["avg", "max"]

    df_ft_diversity = build_agg(
        df_daily,
        group_by=[F.col("mst_seller")],
        columns=diversity_cols,
        functions=diversity_funcs,
        conditions=time_conditions,
    )

    # =========================================================================
    # GROUP 9: Night sale flagging (from dev_ft_viettel_1)
    # =========================================================================
    # Monthly night metrics
    df_monthly_night = (
        df_daily
        .withColumn("month_start", F.date_trunc("month", F.col("report_date")))
        .groupBy("mst_seller", "month_start")
        .agg(
            F.avg("daily_invoice_count").alias("avg_invoices_per_day"),
            F.avg("daily_night_ratio").alias("avg_night_ratio"),
            F.avg("daily_core_ratio").alias("avg_core_ratio"),
            F.countDistinct("report_date").alias("active_days"),
            F.sum("daily_invoice_count").alias("total_invoices_month"),
            F.sum("daily_night_invoice_count").alias("total_night_invoices_month"),
        )
        .withColumn("qualified_night_month",
                     (F.col("avg_invoices_per_day") >= 100) & (F.col("avg_night_ratio") >= 0.30))
    )

    df_ft_night_flag = (
        df_monthly_night
        .groupBy("mst_seller")
        .agg(
            F.sum(F.when(F.col("qualified_night_month"), F.lit(1)).otherwise(F.lit(0)))
            .cast("int").alias("night_flag_qualified_months_l12m"),
            F.avg("avg_night_ratio").alias("avg_night_ratio_l12m"),
            F.max("avg_night_ratio").alias("max_night_ratio_l12m"),
            F.avg("avg_core_ratio").alias("avg_core_ratio_l12m"),
        )
        .withColumn(
            "is_night_flagged",
            F.col("night_flag_qualified_months_l12m") >= 2,
        )
    )

    # =========================================================================
    # GROUP 10: Top-N sales day features (from dev_ft_viettel_2)
    # =========================================================================
    w_top_days = Window.partitionBy("mst_seller").orderBy(F.col("daily_total_sales").desc())
    df_ranked_days = df_daily.withColumn("day_rank", F.row_number().over(w_top_days))

    df_top5_days = (
        df_ranked_days
        .filter(F.col("day_rank") <= 5)
        .groupBy("mst_seller")
        .agg(F.sum("daily_total_sales").alias("top5_days_sales_l12m"))
    )

    df_top5_day_concentration = df_top5_days.join(
        df_seller_total.select("mst_seller", "seller_total_sales_l12m"),
        on="mst_seller",
        how="left",
    ).withColumn(
        "top5_days_concentration_l12m",
        F.when(F.col("seller_total_sales_l12m") > 0,
               F.col("top5_days_sales_l12m") / F.col("seller_total_sales_l12m"))
        .otherwise(F.lit(0.0)),
    ).select("mst_seller", "top5_days_sales_l12m", "top5_days_concentration_l12m")

    # =========================================================================
    # FINAL JOIN
    # =========================================================================
    df_ft = (
        df_ft_sales
        .join(df_ft_night, on="mst_seller", how="left")
        .join(df_ft_discount, on="mst_seller", how="left")
        .join(df_ft_value, on="mst_seller", how="left")
        .join(df_ft_activity, on="mst_seller", how="left")
        .join(df_ft_active_days, on="mst_seller", how="left")
        .join(df_ft_monthly, on="mst_seller", how="left")
        .join(df_ft_concentration, on="mst_seller", how="left")
        .join(df_ft_diversity, on="mst_seller", how="left")
        .join(df_ft_night_flag, on="mst_seller", how="left")
        .join(df_top5_day_concentration, on="mst_seller", how="left")
    )

    # Add report_date
    df_ft = df_ft.select(
        F.lit(str(set_last_date)).cast("date").alias("report_date"),
        "*",
    )

    return df_ft


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Viettel Feature Engineering - PySpark")
    parser.add_argument("--invoice-table", default=INVOICE_TABLE, help="Input invoice table")
    parser.add_argument("--stg-table", default=STG_TABLE, help="Staging output table")
    parser.add_argument("--ft-table", default=FT_TABLE, help="Feature output table")
    parser.add_argument("--run-date", default=None, help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument("--mode", default="all", choices=["staging", "features", "all"],
                        help="Run staging only, features only, or both")
    args = parser.parse_args()

    if args.run_date:
        run_date = datetime.datetime.strptime(args.run_date, "%Y-%m-%d").date()
    else:
        run_date = datetime.date.today()

    print(f"Run date: {run_date}")
    print(f"Invoice table: {args.invoice_table}")

    # Read raw data
    df_raw = spark.table(args.invoice_table)

    if args.mode in ("staging", "all"):
        print("Building staging data...")
        df_stg = build_staging(df_raw)
        df_stg.write.mode("overwrite").saveAsTable(args.stg_table)
        print(f"Staging written to {args.stg_table}")

    if args.mode in ("features", "all"):
        print("Building features...")
        if args.mode == "features":
            # Read from staging table
            df_stg = spark.table(args.stg_table)
        df_buyer = build_buyer_staging(df_raw)
        df_ft = build_final_features(df_stg, df_buyer, run_date)
        df_ft.write.mode("overwrite").saveAsTable(args.ft_table)
        print(f"Features written to {args.ft_table}")

    print("Done.")
