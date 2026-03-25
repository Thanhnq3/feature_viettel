"""
Viettel Invoice Feature Engineering - PySpark 2.4.4 Compatible
==============================================================
Rewritten for full Spark 2.4.4 compatibility.
- NO F.percentile / F.percentile_approx (not in PySpark 2.4 API)
- NO JVM calls to org.apache.spark.sql.functions.percentile_approx
- Percentiles computed via Spark SQL temp views using the built-in
  SQL function percentile_approx (registered in Spark's FunctionRegistry
  since Spark 2.0, works without Hive).

Pipeline:
    Step 1 (Staging):  Raw invoice data -> Daily seller-level aggregation
    Step 2 (Buyer):    Raw -> Monthly buyer-seller pair aggregation
    Step 3 (Features):
        A) Direct:   daily -> multi-period (l1m, l3m, l6m, l12m)
        B) Monthly:  daily -> monthly df -> multi-period
        C) Specialized: buyer concentration, night flags, top-N days

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

spark = SparkSession.builder.appName("viettel_feature_engineering").getOrCreate()


# =============================================================================
# CONFIG
# =============================================================================

INVOICE_TABLE = "your_schema.viettel_invoice"
STG_TABLE = "your_schema.viettel_stg_daily"
MONTHLY_TABLE = "your_schema.viettel_monthly"
FT_TABLE = "your_schema.viettel_ft_seller"

COL_RECORD_TS = "col4"
COL_INVOICE_STATUS = "col9"
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
COL_CURRENCY = "col65"
COL_PAYMENT_METHOD = "col79"
COL_ITEM_JSON = "col94"
COL_FINAL_SALE = "total_amount_with_vat_final"
COL_PARTITION = "col129"

NIGHT_START = 22
NIGHT_END = 6
CORE_START = 0
CORE_END = 4


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def f_last_day_of_previous_month(data_date):
    return data_date.replace(day=1) - datetime.timedelta(days=1)


def f_first_day_of_previous_month(data_date, nbr_of_mth):
    return data_date.replace(day=1) - relativedelta(months=nbr_of_mth)


# =============================================================================
# AGGREGATION HELPERS (Spark 2.4.4 compatible)
# =============================================================================

def build_agg(spark, df, group_by, columns, functions, conditions):
    """
    Generic aggregation builder. Spark 2.4.4 compatible.

    For percentile functions (med, pct25, pct75), uses Spark SQL via temp views
    because percentile_approx is only available as a SQL function in Spark 2.4.

    Args:
        spark:      SparkSession
        df:         input DataFrame
        group_by:   list of column name STRINGS (not Column objects)
        columns:    list of column name strings to aggregate
        functions:  list of function names (sum, avg, min, max, std, kurt,
                    skew, count, countDistinct, med, pct25, pct75)
        conditions: list of [filter_Column_expr, suffix_str]
    Returns:
        DataFrame with group_by columns + aggregated feature columns
    """
    PERCENTILE_MAP = {"med": 0.5, "pct25": 0.25, "pct75": 0.75}
    regular_funcs = [f for f in functions if f not in PERCENTILE_MAP]
    pct_funcs = [f for f in functions if f in PERCENTILE_MAP]

    df_regular = None
    df_pct = None

    # --- Regular aggregations via PySpark DataFrame API ---
    if regular_funcs:
        agg_list = []
        for col_name in columns:
            for func_name in regular_funcs:
                for cond_expr, suffix in conditions:
                    ft_name = "{}_{}{}".format(col_name, func_name, suffix)
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

        gb_cols = [F.col(c) for c in group_by]
        df_regular = df.groupBy(*gb_cols).agg(*agg_list)

    # --- Percentile aggregations via Spark SQL ---
    if pct_funcs:
        # Pre-add conditional columns (SQL can't embed F.when Column objects)
        df_tmp = df
        tmp_names = []
        for col_name in columns:
            for cond_expr, suffix in conditions:
                tmp_name = "__p_{}_{}".format(col_name, suffix.lstrip("_"))
                df_tmp = df_tmp.withColumn(
                    tmp_name,
                    F.when(cond_expr, F.col(col_name)).cast("double"),
                )
                tmp_names.append(tmp_name)

        view_name = "__pct_agg_view"
        df_tmp.createOrReplaceTempView(view_name)

        gb_sql = ", ".join(["`{}`".format(c) for c in group_by])
        pct_exprs = []
        for col_name in columns:
            for func_name in pct_funcs:
                pct_val = PERCENTILE_MAP[func_name]
                for _, suffix in conditions:
                    ft_name = "{}_{}{}".format(col_name, func_name, suffix)
                    tmp_name = "__p_{}_{}".format(col_name, suffix.lstrip("_"))
                    pct_exprs.append(
                        "percentile_approx(`{}`, {}) as `{}`".format(
                            tmp_name, pct_val, ft_name
                        )
                    )

        sql = "SELECT {}, {} FROM {} GROUP BY {}".format(
            gb_sql, ", ".join(pct_exprs), view_name, gb_sql
        )
        df_pct = spark.sql(sql)

    # --- Combine results ---
    if df_regular is not None and df_pct is not None:
        return df_regular.join(df_pct, on=group_by, how="inner")
    elif df_regular is not None:
        return df_regular
    else:
        return df_pct


# =============================================================================
# STEP 1: STAGING - Daily seller-level aggregation
# =============================================================================

def build_staging(df_raw):
    """
    From raw invoice data, produce daily seller-level metrics.
    Filters: only issued invoices (col9 = 1).
    Uses pre-computed total_amount_with_vat_final as total_sales.
    """
    df = df_raw.filter(F.col(COL_INVOICE_STATUS).cast("int") == 1)

    df = (
        df
        .withColumn("mst_seller", F.trim(F.col(COL_SELLER_MST).cast("string")))
        .withColumn("mst_buyer", F.trim(F.col(COL_BUYER_MST).cast("string")))
        .withColumn(
            "invoice_ts",
            F.coalesce(
                F.to_timestamp(F.col(COL_INVOICE_ISSUE_TS)),
                F.to_timestamp(F.col(COL_RECORD_TS)),
            ),
        )
        .withColumn("report_date", F.to_date("invoice_ts"))
        .withColumn("hour", F.hour("invoice_ts"))
        .withColumn("total_sales", F.col(COL_FINAL_SALE).cast("double"))
        .withColumn(
            "is_night",
            (F.col("hour") >= F.lit(NIGHT_START)) | (F.col("hour") < F.lit(NIGHT_END)),
        )
        .withColumn(
            "is_core",
            (F.col("hour") >= F.lit(CORE_START)) & (F.col("hour") < F.lit(CORE_END)),
        )
        .withColumn("tax_amt", F.col(COL_TAX_AMT).cast("double"))
        .withColumn("discount_amt", F.col(COL_DISCOUNT).cast("double"))
        .withColumn("total_without_vat", F.col(COL_TOTAL_WITHOUT_VAT).cast("double"))
        .withColumn("total_with_vat", F.col(COL_TOTAL_WITH_VAT).cast("double"))
        .withColumn("payment_method", F.col(COL_PAYMENT_METHOD))
        .withColumn("transport", F.col(COL_TRANSPORT))
    )

    df = df.filter(F.col("mst_seller").isNotNull() & F.col("report_date").isNotNull())

    df_daily = (
        df
        .groupBy("mst_seller", "report_date")
        .agg(
            F.sum("total_sales").alias("daily_total_sales"),
            F.count(F.lit(1)).alias("daily_invoice_count"),
            F.countDistinct("mst_buyer").alias("daily_buyer_count"),
            F.sum("tax_amt").alias("daily_total_tax"),
            F.sum("discount_amt").alias("daily_total_discount"),
            F.sum("total_without_vat").alias("daily_total_without_vat"),
            F.sum("total_with_vat").alias("daily_total_with_vat"),
            F.sum(F.when(F.col("is_night"), F.lit(1)).otherwise(F.lit(0))).alias("daily_night_invoice_count"),
            F.sum(F.when(F.col("is_core"), F.lit(1)).otherwise(F.lit(0))).alias("daily_core_invoice_count"),
            F.countDistinct("payment_method").alias("daily_distinct_payment_methods"),
            F.countDistinct("transport").alias("daily_distinct_transport"),
            F.max("total_sales").alias("daily_max_invoice_value"),
            F.min("total_sales").alias("daily_min_invoice_value"),
            F.avg("total_sales").alias("daily_avg_invoice_value"),
            F.stddev("total_sales").alias("daily_std_invoice_value"),
        )
    )

    df_daily = (
        df_daily
        .withColumn(
            "daily_night_ratio",
            F.when(F.col("daily_invoice_count") > 0,
                   F.col("daily_night_invoice_count") / F.col("daily_invoice_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_core_ratio",
            F.when(F.col("daily_invoice_count") > 0,
                   F.col("daily_core_invoice_count") / F.col("daily_invoice_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_discount_ratio",
            F.when(F.col("daily_total_sales") > 0,
                   F.coalesce(F.col("daily_total_discount"), F.lit(0.0)) / F.col("daily_total_sales"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_sales_per_buyer",
            F.when(F.col("daily_buyer_count") > 0,
                   F.col("daily_total_sales") / F.col("daily_buyer_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_sales_per_invoice",
            F.when(F.col("daily_invoice_count") > 0,
                   F.col("daily_total_sales") / F.col("daily_invoice_count"))
            .otherwise(F.lit(0.0)),
        )
    )

    return df_daily


# =============================================================================
# STEP 2: BUYER-LEVEL STAGING
# =============================================================================

def build_buyer_staging(df_raw):
    """Monthly buyer-seller pair aggregation for buyer concentration features."""
    df = df_raw.filter(F.col(COL_INVOICE_STATUS).cast("int") == 1)

    df = (
        df
        .withColumn("mst_seller", F.trim(F.col(COL_SELLER_MST).cast("string")))
        .withColumn("mst_buyer", F.trim(F.col(COL_BUYER_MST).cast("string")))
        .withColumn("total_sales", F.col(COL_FINAL_SALE).cast("double"))
        .withColumn(
            "invoice_ts",
            F.coalesce(
                F.to_timestamp(F.col(COL_INVOICE_ISSUE_TS)),
                F.to_timestamp(F.col(COL_RECORD_TS)),
            ),
        )
        .withColumn("report_date", F.to_date("invoice_ts"))
        .withColumn("month_start", F.date_trunc("month", F.col("report_date")))
    )

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
# STEP 3A: DIRECT FEATURES (daily -> multi-period, no monthly intermediate)
# =============================================================================

def build_direct_daily_features(spark, df_daily, periods):
    """
    Feature groups that aggregate directly from daily to multi-period.
    Groups: sales, night ratios, discount, value distribution, activity, diversity.

    Returns: DataFrame keyed by mst_seller with all direct features.
    """
    time_conditions = [
        [F.col("report_date") >= periods[k], "_{}".format(k)] for k in periods
    ]

    # GROUP 1: Sales, invoices, buyers
    sales_cols = [
        "daily_total_sales",
        "daily_invoice_count",
        "daily_buyer_count",
        "daily_sales_per_buyer",
        "daily_sales_per_invoice",
    ]
    df_ft_sales = build_agg(
        spark, df_daily,
        group_by=["mst_seller"],
        columns=sales_cols,
        functions=["sum", "avg", "min", "max", "std"],
        conditions=time_conditions,
    )

    # GROUP 2: Night sale ratios
    df_ft_night = build_agg(
        spark, df_daily,
        group_by=["mst_seller"],
        columns=["daily_night_ratio", "daily_core_ratio"],
        functions=["avg", "max", "std"],
        conditions=time_conditions,
    )

    # GROUP 3: Discount
    df_ft_discount = build_agg(
        spark, df_daily,
        group_by=["mst_seller"],
        columns=["daily_discount_ratio", "daily_total_discount"],
        functions=["sum", "avg", "max"],
        conditions=time_conditions,
    )

    # GROUP 4: Invoice value distribution (includes median via SQL)
    df_ft_value = build_agg(
        spark, df_daily,
        group_by=["mst_seller"],
        columns=["daily_avg_invoice_value", "daily_max_invoice_value"],
        functions=["avg", "max", "min", "std", "med", "skew"],
        conditions=time_conditions,
    )

    # GROUP 5: Activity pattern (gaps between active days)
    w_seller = Window.partitionBy("mst_seller").orderBy("report_date")
    df_with_gap = (
        df_daily
        .withColumn("prev_date", F.lag("report_date").over(w_seller))
        .withColumn("days_gap", F.datediff(F.col("report_date"), F.col("prev_date")))
    )

    df_ft_activity = build_agg(
        spark, df_with_gap,
        group_by=["mst_seller"],
        columns=["days_gap"],
        functions=["avg", "max", "min", "std"],
        conditions=time_conditions,
    )

    # Active days count per period
    active_days_aggs = []
    for cond_expr, suffix in time_conditions:
        ft_name = "active_days{}".format(suffix)
        active_days_aggs.append(
            F.countDistinct(F.when(cond_expr, F.col("report_date"))).alias(ft_name)
        )
    df_ft_active_days = df_daily.groupBy("mst_seller").agg(*active_days_aggs)

    # GROUP 8: Payment & transport diversity
    df_ft_diversity = build_agg(
        spark, df_daily,
        group_by=["mst_seller"],
        columns=["daily_distinct_payment_methods", "daily_distinct_transport"],
        functions=["avg", "max"],
        conditions=time_conditions,
    )

    # JOIN all direct features
    df_direct = (
        df_ft_sales
        .join(df_ft_night, on="mst_seller", how="left")
        .join(df_ft_discount, on="mst_seller", how="left")
        .join(df_ft_value, on="mst_seller", how="left")
        .join(df_ft_activity, on="mst_seller", how="left")
        .join(df_ft_active_days, on="mst_seller", how="left")
        .join(df_ft_diversity, on="mst_seller", how="left")
    )

    return df_direct


# =============================================================================
# STEP 3B: MONTHLY AGGREGATION (daily -> monthly intermediate df)
# =============================================================================

def build_daily_to_monthly(df_daily):
    """
    Aggregate daily staging data to monthly level per seller.
    This is the intermediate step before multi-period aggregation.

    Returns: DataFrame with mst_seller, report_date (month_end),
             and monthly-level metrics.
    """
    df_monthly = (
        df_daily
        .withColumn("month_end", F.last_day(F.col("report_date")))
        .groupBy("mst_seller", "month_end")
        .agg(
            # Monthly totals from daily
            F.sum("daily_total_sales").alias("monthly_total_sales"),
            F.sum("daily_invoice_count").alias("monthly_invoice_count"),
            F.sum("daily_buyer_count").alias("monthly_buyer_count"),
            F.avg("daily_total_sales").alias("monthly_avg_daily_sales"),
            F.avg("daily_invoice_count").alias("monthly_avg_daily_invoices"),
            F.avg("daily_buyer_count").alias("monthly_avg_daily_buyers"),
            # Night metrics for night-flag computation
            F.avg("daily_invoice_count").alias("avg_invoices_per_day"),
            F.avg("daily_night_ratio").alias("avg_night_ratio"),
            F.avg("daily_core_ratio").alias("avg_core_ratio"),
            F.countDistinct("report_date").alias("active_days"),
            F.sum("daily_night_invoice_count").alias("total_night_invoices_month"),
        )
        .withColumnRenamed("month_end", "report_date")
    )

    return df_monthly


# =============================================================================
# STEP 3C: MONTHLY -> MULTI-PERIOD FEATURES
# =============================================================================

def build_monthly_to_multiperiod(spark, df_monthly, periods):
    """
    Aggregate monthly intermediate df to multi-period features.

    Returns: DataFrame keyed by mst_seller with monthly->multi-period features.
    """
    time_conditions = [
        [F.col("report_date") >= periods[k], "_{}".format(k)] for k in periods
    ]

    monthly_agg_cols = [
        "monthly_total_sales",
        "monthly_invoice_count",
        "monthly_buyer_count",
    ]

    df_ft_monthly = build_agg(
        spark, df_monthly,
        group_by=["mst_seller"],
        columns=monthly_agg_cols,
        functions=["avg", "min", "max", "std"],
        conditions=time_conditions,
    )

    return df_ft_monthly


# =============================================================================
# STEP 3D: NIGHT FLAG (uses monthly intermediate)
# =============================================================================

def build_night_flag(df_monthly):
    """
    Night sale flagging from monthly intermediate data.
    A month is 'qualified' if avg_invoices_per_day >= 100 and avg_night_ratio >= 0.30.
    A seller is flagged if >= 2 qualified months.
    """
    df_monthly_flagged = df_monthly.withColumn(
        "qualified_night_month",
        (F.col("avg_invoices_per_day") >= 100) & (F.col("avg_night_ratio") >= 0.30),
    )

    df_ft_night_flag = (
        df_monthly_flagged
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

    return df_ft_night_flag


# =============================================================================
# STEP 3E: BUYER CONCENTRATION
# =============================================================================

def build_buyer_concentration(df_buyer_monthly, periods, set_last_date):
    """
    Top-N buyer concentration features (from dev_ft_viettel_1 & 2).
    Returns: df_ft_concentration, df_seller_total (needed by top-days).
    """
    df_buyer_monthly_filtered = (
        df_buyer_monthly
        .filter(F.col("month_start") >= periods["l12m"])
        .filter(F.col("month_start") <= set_last_date)
    )

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

    return df_ft_concentration, df_seller_total


# =============================================================================
# STEP 3F: TOP-N SALES DAY FEATURES
# =============================================================================

def build_top_days(df_daily, df_seller_total):
    """Top-N sales day features (from dev_ft_viettel_2)."""
    w_top_days = Window.partitionBy("mst_seller").orderBy(F.col("daily_total_sales").desc())
    df_ranked_days = df_daily.withColumn("day_rank", F.row_number().over(w_top_days))

    df_top5_days = (
        df_ranked_days
        .filter(F.col("day_rank") <= 5)
        .groupBy("mst_seller")
        .agg(F.sum("daily_total_sales").alias("top5_days_sales_l12m"))
    )

    df_top5_day_concentration = (
        df_top5_days
        .join(
            df_seller_total.select("mst_seller", "seller_total_sales_l12m"),
            on="mst_seller",
            how="left",
        )
        .withColumn(
            "top5_days_concentration_l12m",
            F.when(F.col("seller_total_sales_l12m") > 0,
                   F.col("top5_days_sales_l12m") / F.col("seller_total_sales_l12m"))
            .otherwise(F.lit(0.0)),
        )
        .select("mst_seller", "top5_days_sales_l12m", "top5_days_concentration_l12m")
    )

    return df_top5_day_concentration


# =============================================================================
# STEP 4: ORCHESTRATOR - Build all final features
# =============================================================================

def build_final_features(spark, df_daily, df_buyer_monthly, run_date):
    """
    Orchestrate all feature groups and join into final output.

    Returns: (df_ft, df_monthly)
        df_ft:      final feature DataFrame keyed by mst_seller + report_date
        df_monthly: intermediate monthly DataFrame (for inspection / storage)
    """
    set_last_date = f_last_day_of_previous_month(run_date)
    periods = {
        "l1m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=0),
        "l3m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=2),
        "l6m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=5),
        "l12m": f_first_day_of_previous_month(set_last_date, nbr_of_mth=11),
    }

    # Filter daily to last 12 months
    df_daily = (
        df_daily
        .filter(F.col("report_date") >= periods["l12m"])
        .filter(F.col("report_date") <= set_last_date)
        .repartition("mst_seller")
        .cache()
    )

    # --- A) Direct: daily -> multi-period ---
    df_direct = build_direct_daily_features(spark, df_daily, periods)

    # --- B) Monthly intermediate ---
    df_monthly = build_daily_to_monthly(df_daily)

    # --- B.1) Monthly -> multi-period ---
    df_ft_monthly = build_monthly_to_multiperiod(spark, df_monthly, periods)

    # --- B.2) Night flag (from monthly) ---
    df_ft_night_flag = build_night_flag(df_monthly)

    # --- C) Buyer concentration ---
    df_ft_concentration, df_seller_total = build_buyer_concentration(
        df_buyer_monthly, periods, set_last_date
    )

    # --- D) Top-N days ---
    df_ft_top_days = build_top_days(df_daily, df_seller_total)

    # --- FINAL JOIN ---
    df_ft = (
        df_direct
        .join(df_ft_monthly, on="mst_seller", how="left")
        .join(df_ft_night_flag, on="mst_seller", how="left")
        .join(df_ft_concentration, on="mst_seller", how="left")
        .join(df_ft_top_days, on="mst_seller", how="left")
    )

    # Add report_date
    df_ft = df_ft.select(
        F.lit(str(set_last_date)).cast("date").alias("report_date"),
        "*",
    )

    return df_ft, df_monthly


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Viettel Feature Engineering - PySpark 2.4.4")
    parser.add_argument("--invoice-table", default=INVOICE_TABLE, help="Input invoice table")
    parser.add_argument("--stg-table", default=STG_TABLE, help="Staging output table")
    parser.add_argument("--monthly-table", default=MONTHLY_TABLE, help="Monthly intermediate table")
    parser.add_argument("--ft-table", default=FT_TABLE, help="Feature output table")
    parser.add_argument("--run-date", default=None, help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--mode", default="all",
        choices=["staging", "features", "all"],
        help="Run staging only, features only, or both",
    )
    args = parser.parse_args()

    if args.run_date:
        run_date = datetime.datetime.strptime(args.run_date, "%Y-%m-%d").date()
    else:
        run_date = datetime.date.today()

    print("Run date: {}".format(run_date))
    print("Invoice table: {}".format(args.invoice_table))

    df_raw = spark.table(args.invoice_table)

    if args.mode in ("staging", "all"):
        print("Building staging data...")
        df_stg = build_staging(df_raw)
        df_stg.write.mode("overwrite").saveAsTable(args.stg_table)
        print("Staging written to {}".format(args.stg_table))

    if args.mode in ("features", "all"):
        print("Building features...")
        if args.mode == "features":
            df_stg = spark.table(args.stg_table)
        df_buyer = build_buyer_staging(df_raw)

        df_ft, df_monthly = build_final_features(spark, df_stg, df_buyer, run_date)

        # Save monthly intermediate
        df_monthly.write.mode("overwrite").saveAsTable(args.monthly_table)
        print("Monthly intermediate written to {}".format(args.monthly_table))

        # Save final features
        df_ft.write.mode("overwrite").saveAsTable(args.ft_table)
        print("Features written to {}".format(args.ft_table))

    print("Done.")
