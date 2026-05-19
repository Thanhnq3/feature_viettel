# %% [Cell 1 — Config]
# %pyspark

# -- Paths (adapt to your environment) --
# INVOICE_TABLE = "kpidh_db.t_adpm_sinvoice_hddt_invoice_merge_by_month"  # raw invoice table
# INVOICE_TABLE = "kpidh_db.tcb_draft_7"
INVOICE_TABLE  = "kpidh_db.tcb_draft_6_2023_2026"
STG_TABLE      = "kpidh_db.thannq6_viettel_stg_daily"    # staging output
MONTHLY_TABLE  = "kpidh_db.thannq6_viettel_stg_monthly"  # monthly data
FT_TABLE       = "kpidh_db.thannq6_viettel_ft_seller"    # final feature output

# -- Column aliases for readability --
COL_INVOICE_ID          = "id"
COL_RECORD_TS           = "created_date"
COL_INVOICE_STATUS      = "invoice_status"
COL_ADJ_TYPE            = "adjustment_type"
COL_ADJ_STATUS          = "adjusted_status"
COL_SELLER_MST          = "tenant_tax_code_encrypted"
COL_INVOICE_ISSUE_TS    = "issue_date"
COL_BUYER_MST           = "buyer_tax_code_encrypted"
COL_TRANSPORT           = "delivery_vehicle"
COL_BUYER_NAME          = "buyer_name"
COL_BUYER_ADDRESS       = "buyer_address"
COL_BUYER_EMAIL         = "buyer_email_address"
COL_BUYER_PHONE         = "buyer_phone_number"
COL_TAX_AMT             = "total_vat_amount"
COL_DISCOUNT            = "discount_amount"
COL_SETTLEMENT_DISCOUNT = "settlement_discount_amount"
COL_TOTAL_WITH_VAT      = "total_amount_with_vat"
COL_TOTAL_WITHOUT_VAT   = "total_amount_without_vat"
COL_TOTAL_VAT_FRN       = "total_amount_with_vat_frn"
COL_CURRENCY            = "currency_code"
COL_ORIG_INVOICE        = "original_invoice_no"
COL_PAYMENT_METHOD      = "payment_type"
COL_ITEM_JSON           = "list_product"
COL_PARTITION           = "partition_month"
COL_PARTITION_TIMESTAMP = "signed_date_proc"
COL_ERROR_CODE          = "error_code"
COL_BIZ_TYPE            = "business_type"
COL_FINAL_SALE          = "total_amount_with_vat_final_fx_trans"
COL_TRUSTED_PARTNERS    = ""

# Night sale thresholds (from dev_ft_viettel_1)
NIGHT_START = 22
NIGHT_END   = 6
CORE_START  = 0
CORE_END    = 4


# %% [Cell 2 — Imports]
# %pyspark

import datetime
from functools import reduce
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.column import Column, _to_java_column

spark = SparkSession.builder.appName("viettel_feature_engineering").getOrCreate()
spark.version


# %% [Cell 3 — Helper functions]
# %pyspark

def f_last_day_of_previous_month(data_date):
    return data_date.replace(day=1) - datetime.timedelta(days=1)


def f_first_day_of_previous_month(data_date, nbr_of_mth):
    return data_date.replace(day=1) - relativedelta(months=nbr_of_mth)


# %% [Cell 4 — Aggregation helpers]
# %pyspark

# data exploration

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


# %% [Cell 5 — build_staging]
# %pyspark

# =============================================================================
# STEP 1: STAGING - Daily seller-level aggregation
# =============================================================================

def build_staging(df_raw):
    """
    From raw invoice data, produce daily seller-level metrics.
    Filters: only issued invoices (col9 = 1).
    Uses pre-computed total_amount_with_vat_final as total_sales.
    """
    # define report_date (invoice signed), hour of invoice, if night or core invoice hour,
    # rename other columns: seller and buyer taxcode, invoice signed date,
    # payment method, tax amount, discount amount and payment method
    df = df_raw.filter(
        (F.col(COL_INVOICE_STATUS).cast("int") == 1)
        & ~(F.col(COL_PARTITION_TIMESTAMP).isNull())
        & (F.col(COL_ERROR_CODE).like("%_CODE_APPROVED%"))
        & (F.col(COL_ADJ_TYPE).isin('1', '3', '7', '9'))
    )

    df = (
        df
        .withColumn("mst_seller", F.trim(F.col(COL_SELLER_MST).cast("string")))
        .withColumn("mst_buyer",  F.trim(F.col(COL_BUYER_MST).cast("string")))
        .withColumn(
            "invoice_ts",
            F.to_timestamp(F.col(COL_PARTITION_TIMESTAMP)),
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
        .withColumn("tax_amt",          F.col(COL_TAX_AMT).cast("double"))
        .withColumn("discount_amt",     F.col(COL_DISCOUNT).cast("double"))
        .withColumn("total_sales",      F.col(COL_FINAL_SALE).cast("double"))
        .withColumn("payment_method",   F.col(COL_PAYMENT_METHOD))
        .withColumn("transport",        F.col(COL_TRANSPORT))
        .withColumn(
            "tax_pct",
            F.round(
                F.col(COL_TAX_AMT) / (F.col("total_sales") + F.col("discount_amt") + F.col("tax_amt")) * 100,
                2,
            ),
        )
        .withColumn(
            "tax_pct_grp",
            F.when(F.col("tax_pct") == 10.00, "10%")
            .when(F.col("tax_pct") == 8.00,  "8%")
            .when(F.col("tax_pct") == 5.00,  "5%")
            .when(F.col("tax_pct") == 0.00,  "0%")
            .otherwise("others"),
        )
    )

    # filter valid invoice with condition: not null key mapping columns  #thuthb5may
    df = df.filter(F.col("mst_seller").isNotNull() & F.col("report_date").isNotNull())

    # create daily aggregate table
    df_daily = (
        df
        .groupBy("mst_seller", "report_date")
        .agg(
            F.sum("total_sales").alias("daily_total_sales"),
            F.count(F.lit(1)).alias("daily_invoice_count"),
            F.countDistinct("mst_buyer").alias("daily_buyer_count"),
            F.sum("tax_amt").alias("daily_total_tax"),
            F.sum("discount_amt").alias("daily_total_discount"),
            F.sum(F.when(F.col("is_night"), F.lit(1)).otherwise(F.lit(0))).alias("daily_night_invoice_count"),
            F.sum(F.when(F.col("is_core"),  F.lit(1)).otherwise(F.lit(0))).alias("daily_core_invoice_count"),
            F.countDistinct("payment_method").alias("daily_distinct_payment_methods"),
            F.countDistinct("transport").alias("daily_distinct_transport"),
            F.max("total_sales").alias("daily_max_invoice_value"),
            F.min("total_sales").alias("daily_min_invoice_value"),
            F.avg("total_sales").alias("daily_avg_invoice_value"),
            F.stddev("total_sales").alias("daily_std_invoice_value"),
            F.sum(
                F.when(
                    F.coalesce(F.col("total_amount_with_vat_adj"), F.col("total_amount_without_vat_adj")).isNotNull(),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            ).alias("daily_invoice_adjusted_count"),
            F.sum(
                F.coalesce(F.col("total_amount_with_vat_adj"), F.col("total_amount_without_vat_adj"), F.lit(0.0)),
            ).alias("daily_invoice_adjusted_amount"),
            F.sum(F.when(F.col("tax_pct_grp") == "10%", 1).otherwise(0)).alias("daily_invoice_count_tax10"),
            F.sum(F.when(F.col("tax_pct_grp") == "8%",  1).otherwise(0)).alias("daily_invoice_count_tax8"),
            F.sum(F.when(F.col("tax_pct_grp") == "5%",  1).otherwise(0)).alias("daily_invoice_count_tax5"),
            F.sum(F.when(F.col("tax_pct_grp") == "0%",  1).otherwise(0)).alias("daily_invoice_count_tax0"),
            F.sum(F.when(F.col("tax_pct_grp") == "others", 1).otherwise(0)).alias("daily_invoice_count_taxOthers"),
            F.sum(F.when(F.col("tax_pct_grp") == "10%", F.col("total_sales")).otherwise(0)).alias("daily_total_sales_tax10"),
            F.sum(F.when(F.col("tax_pct_grp") == "8%",  F.col("total_sales")).otherwise(0)).alias("daily_total_sales_tax8"),
            F.sum(F.when(F.col("tax_pct_grp") == "5%",  F.col("total_sales")).otherwise(0)).alias("daily_total_sales_tax5"),
            F.sum(F.when(F.col("tax_pct_grp") == "0%",  F.col("total_sales")).otherwise(0)).alias("daily_total_sales_tax0"),
            F.sum(F.when(F.col("tax_pct_grp") == "others", F.col("total_sales")).otherwise(0)).alias("daily_total_sales_taxOthers"),
        )
    )

    # calculate ratio features by daily
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
            "daily_discount_ratio",  # add another for monthly
            F.when(F.col("daily_total_sales") > 0,
                   F.coalesce(F.col("daily_total_discount"), F.lit(0.0)) / F.col("daily_total_sales"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_sales_per_buyer",  # add another for monthly
            F.when(F.col("daily_buyer_count") > 0,
                   F.col("daily_total_sales") / F.col("daily_buyer_count"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "daily_sales_per_invoice",  # add another for monthly
            F.when(F.col("daily_invoice_count") > 0,
                   F.col("daily_total_sales") / F.col("daily_invoice_count"))
            .otherwise(F.lit(0.0)),
        )
    )

    return df_daily


# %% [Cell 6 — build_buyer_staging]
# %pyspark

# =============================================================================
# STEP 2: BUYER-LEVEL STAGING
# =============================================================================

def build_buyer_staging(df_raw):
    """Monthly buyer-seller pair aggregation for buyer concentration features."""
    # df = df_raw.filter(F.col(COL_INVOICE_STATUS).cast("int") == 1) #already proc
    # & ~(F.col(COL_PARTITION_TIMESTAMP).isNull())
    # & (F.col(COL_ERROR_CODE).like("%_CODE_APPROVED%"))
    # & (F.col(COL_ADJ_TYPE).isin('1', '3', '7', '9'))

    # process buyer seller taxcode, invoice signed date (report_date), month of report date, total sales
    df = (
        df_raw
        .withColumn("mst_seller", F.trim(F.col(COL_SELLER_MST).cast("string")))
        .withColumn("mst_buyer",  F.trim(F.col(COL_BUYER_MST).cast("string")))
        .withColumn(
            "invoice_ts",
            F.to_timestamp(F.col(COL_PARTITION_TIMESTAMP)),
        )
        .withColumn("report_date",  F.to_date("invoice_ts"))
        .withColumn("month_start",  F.date_trunc("month", F.col("report_date")))
        .withColumn("total_sales",  F.col(COL_FINAL_SALE).cast("double"))
    )

    # calculate sum total sales and number of invoice in a month between seller and buyer
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


# %% [Cell 7 — build_direct_daily_features]
# %pyspark

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
        "daily_invoice_adjusted_count",
        "daily_invoice_adjusted_amount",
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
        .join(df_ft_night,       on="mst_seller", how="left")
        .join(df_ft_discount,    on="mst_seller", how="left")
        .join(df_ft_value,       on="mst_seller", how="left")
        .join(df_ft_activity,    on="mst_seller", how="left")
        .join(df_ft_active_days, on="mst_seller", how="left")
        .join(df_ft_diversity,   on="mst_seller", how="left")
    )

    return df_direct


# %% [Cell 8 — build_daily_to_monthly]
# %pyspark

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
            F.sum("daily_buyer_count").alias("monthly_buyer_count"),  # tbt add another ft as monthly
            F.avg("daily_total_sales").alias("monthly_avg_daily_sales"),
            F.avg("daily_invoice_count").alias("monthly_avg_daily_invoices"),
            F.avg("daily_buyer_count").alias("monthly_avg_daily_buyers"),
            # Night metrics for night-flag computation
            F.avg("daily_invoice_count").alias("avg_invoices_per_day"),
            F.avg("daily_night_ratio").alias("avg_night_ratio"),
            F.avg("daily_core_ratio").alias("avg_core_ratio"),
            F.avg("daily_night_invoice_count").alias("avg_night_invoice_count"),
            F.avg("daily_core_invoice_count").alias("avg_core_invoice_count"),
            F.countDistinct("report_date").alias("active_days"),
            F.sum("daily_night_invoice_count").alias("monthly_total_night_invoices_count"),
            F.sum("daily_core_invoice_count").alias("monthly_total_core_invoices_count"),
            F.sum("daily_invoice_adjusted_count").alias("monthly_total_invoice_adjusted_count"),
            F.sum("daily_invoice_adjusted_amount").alias("monthly_total_invoice_adjusted_amount"),
            F.sum("daily_total_tax").alias("monthly_total_tax"),                              # tbt invoice tax
            F.sum("daily_invoice_count_tax10").alias("monthly_invoice_count_tax10"),          # tbt invoice tax
            F.sum("daily_invoice_count_tax8").alias("monthly_invoice_count_tax8"),            # tbt invoice tax
            F.sum("daily_invoice_count_tax5").alias("monthly_invoice_count_tax5"),            # tbt invoice tax
            F.sum("daily_invoice_count_tax0").alias("monthly_invoice_count_tax0"),            # tbt invoice tax
            F.sum("daily_invoice_count_taxOthers").alias("monthly_invoice_count_taxOthers"),  # tbt invoice tax
            F.sum("daily_total_sales_tax10").alias("monthly_total_sales_tax10"),              # tbt invoice tax
            F.sum("daily_total_sales_tax8").alias("monthly_total_sales_tax8"),                # tbt invoice tax
            F.sum("daily_total_sales_tax5").alias("monthly_total_sales_tax5"),                # tbt invoice tax
            F.sum("daily_total_sales_tax0").alias("monthly_total_sales_tax0"),                # tbt invoice tax
            F.sum("daily_total_sales_taxOthers").alias("monthly_total_sales_taxOthers"),      # tbt invoice tax
        )
        .withColumnRenamed("month_end", "report_date")
    )

    # Top-5/10 days by month: computed from df_daily directly
    w_top_days_by_month = Window.partitionBy("mst_seller", "month_end").orderBy(F.col("daily_total_sales").desc())
    df_ranked_days_by_month = (
        df_daily
        .withColumn("month_end", F.last_day(F.col("report_date")))
        .withColumn("day_rank", F.row_number().over(w_top_days_by_month))
    )

    df_top5_days_by_month = (
        df_ranked_days_by_month
        .filter(F.col("day_rank") <= 5)
        .groupBy("mst_seller", "month_end")
        .agg(F.sum("daily_total_sales").alias("top5_days_by_month_sales"))
    )

    df_top10_days_by_month = (
        df_ranked_days_by_month
        .filter(F.col("day_rank") <= 10)
        .groupBy("mst_seller", "month_end")
        .agg(F.sum("daily_total_sales").alias("top10_days_by_month_sales"))
    )

    # Build concentration: base=top5_days, join monthly total + top10
    df_top5_day_by_month_concentration = (
        df_top5_days_by_month
        .join(
            df_monthly.withColumn("month_end", F.col("report_date")).select("mst_seller", "monthly_total_sales", "month_end"),
            on=["mst_seller", "month_end"],
            how="left",
        )
        .join(
            df_top10_days_by_month.select("mst_seller", "top10_days_by_month_sales", "month_end"),
            on=["mst_seller", "month_end"],
            how="left",
        )
        .withColumn(
            "top5_days_by_month_concentration",
            F.when(F.col("monthly_total_sales") > 0,
                   F.col("top5_days_by_month_sales") / F.col("monthly_total_sales"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "top10_days_by_month_concentration",
            F.when(F.col("monthly_total_sales") > 0,
                   F.col("top10_days_by_month_sales") / F.col("monthly_total_sales"))
            .otherwise(F.lit(0.0)),
        )
        .select(
            "mst_seller",
            "top5_days_by_month_sales",
            "top5_days_by_month_concentration",
            "top10_days_by_month_sales",
            "top10_days_by_month_concentration",
            "month_end",
        )
        .withColumnRenamed("month_end", "report_date")
    )

    df_monthly_2 = (
        df_monthly
        .join(df_top5_day_by_month_concentration, on=["report_date", "mst_seller"], how="left")
    )

    # -------------------------------------------------------------------------
    # POST-PROCESSING: month-over-month change features
    # (originally in Cell 15 notebook block)
    # -------------------------------------------------------------------------
    w_sale_perc = Window.partitionBy("mst_seller").orderBy("report_date")
    df_monthly_2 = (
        df_monthly_2
        .withColumn("monthly_total_sales_last_mth",
                    F.lag("monthly_total_sales").over(w_sale_perc))
        .withColumn(
            "sales_amt_perc_change",
            F.when(
                F.col("monthly_total_sales_last_mth") > 0,
                ((F.col("monthly_total_sales") - F.col("monthly_total_sales_last_mth")) * 100)
                / F.col("monthly_total_sales_last_mth"),
            ).otherwise(None),
        )
        .withColumn("monthly_invoice_count_last_mth",
                    F.lag("monthly_invoice_count").over(w_sale_perc))
        .withColumn(
            "invoice_cnt_perc_change_self",
            F.when(
                F.col("monthly_invoice_count_last_mth") > 0,
                ((F.col("monthly_invoice_count") - F.col("monthly_invoice_count_last_mth")) * 100)
                / F.col("monthly_invoice_count_last_mth"),
            ).otherwise(None),
        )
    )

    # df_monthly_2 = df_monthly_2.withColumnRenamed("month_end", "report_date")
    return df_monthly_2
    # return df_monthly, df_top5_day_by_month_concentration


# %% [Cell 9 — build_monthly_to_multiperiod]
# %pyspark

# =============================================================================
# STEP 3C: MONTHLY -> MULTI-PERIOD FEATURES
# =============================================================================

def build_monthly_to_multiperiod(spark, df_monthly, periods):
    """
    Aggregate monthly intermediate df to multi-period features.

    Dynamically covers ALL metric columns in df_monthly — including the
    Cell-15 derived columns (sales_amt_perc_change, invoice_cnt_perc_change_self,
    top-day concentrations, etc.) — so no hardcoded column list is needed.
    This replaces the standalone agg_monthly_table function.

    Excluded from aggregation:
        - key/structural columns: mst_seller, report_date, prev_report_date
        - lag-reference columns: *_last_mth (the raw lag values are intermediate
          inputs to the change columns; aggregating them separately adds noise)

    Returns: (df_ft_monthly, df_ft_month_gap)
        df_ft_monthly:   DataFrame keyed by mst_seller, one row per seller,
                         with sum/avg/min/max/std for every metric over each period.
        df_ft_month_gap: DataFrame with months_gap sum features per period.
    """
    time_conditions = [
        [F.col("report_date") >= periods[k], "_{}".format(k)] for k in periods
    ]

    # Dynamically build column list — every column except structural/reference ones
    _exclude = {
        "mst_seller", "report_date", "prev_report_date",
        "monthly_total_sales_last_mth", "monthly_invoice_count_last_mth",
    }
    monthly_agg_cols = [c for c in df_monthly.columns if c not in _exclude]

    df_ft_monthly = build_agg(
        spark, df_monthly,
        group_by=["mst_seller"],
        columns=monthly_agg_cols,
        functions=["sum", "avg", "min", "max", "std"],
        conditions=time_conditions,
    )

    # Monthly gap features: months between consecutive active months
    w_seller = Window.partitionBy("mst_seller").orderBy("report_date")
    df_with_gap_month = (
        df_monthly
        .withColumn("prev_month", F.lag("report_date").over(w_seller))
        .withColumn("months_gap", F.months_between(F.col("report_date"), F.col("prev_month")))
    )
    time_conditions_gap = [[F.col("report_date") >= periods[k], "_{}".format(k)] for k in periods]
    df_ft_month_gap = build_agg(
        spark, df_with_gap_month,
        group_by=["mst_seller"],
        columns=["months_gap"],
        functions=["sum"],
        conditions=time_conditions_gap,
    )

    return df_ft_monthly, df_ft_month_gap


# %% [Cell 10 — build_night_flag]
# %pyspark
# # def build_night_flag(df_monthly)

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


# %% [Cell 11 — build_buyer_concentration]
# %pyspark

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


# %% [Cell 12 — build_top_days]
# %pyspark

# =============================================================================
# STEP 3F: TOP-N SALES DAY FEATURES
# =============================================================================

def build_top_days(df_daily, df_seller_total):
    """Top-N sales day features (from dev_ft_viettel_1)."""
    w_top_days = Window.partitionBy("mst_seller").orderBy(F.col("daily_total_sales").desc())
    df_ranked_days = df_daily.withColumn("day_rank", F.row_number().over(w_top_days))

    df_top5_days = (
        df_ranked_days
        .filter(F.col("day_rank") <= 5)
        .groupBy("mst_seller")
        .agg(F.sum("daily_total_sales").alias("top5_days_sales_l12m"))
    )

    df_top10_days = (
        df_ranked_days
        .filter(F.col("day_rank") <= 10)
        .groupBy("mst_seller")
        .agg(F.sum("daily_total_sales").alias("top10_days_sales_l12m"))
    )

    df_top5_day_concentration = (
        df_top5_days
        .join(
            df_seller_total.select("mst_seller", "seller_total_sales_l12m"),
            on="mst_seller",
            how="left",
        )
        .join(
            df_top10_days.select("mst_seller", "top10_days_sales_l12m"),
            on="mst_seller",
            how="left",
        )
        .withColumn(
            "top5_days_concentration_l12m",
            F.when(F.col("seller_total_sales_l12m") > 0,
                   F.col("top5_days_sales_l12m") / F.col("seller_total_sales_l12m"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "top10_days_concentration_l12m",
            F.when(F.col("seller_total_sales_l12m") > 0,
                   F.col("top10_days_sales_l12m") / F.col("seller_total_sales_l12m"))
            .otherwise(F.lit(0.0)),
        )
        .select(
            "mst_seller",
            "top5_days_sales_l12m",
            "top5_days_concentration_l12m",
            "top10_days_sales_l12m",
            "top10_days_concentration_l12m",
        )
    )

    return df_top5_day_concentration


# %% [Cell 13 — features_from_raw]
# %pyspark

# =============================================================================
# STEP 3G: FEATURES FROM RAW (raw -> monthly aggregation)
# Aggregates directly from raw invoice data to monthly features per seller.
# Returns one row per (mst_seller, report_date=month_end); caller filters to
# the target month before joining into the final feature table.
#
# Path A — raw -> monthly (direct):
#   Taxcode-buyer totals; highest-invoice value;
#   most/least-frequent buyer; top-1/2/3 buyer concentration.
# Path B — raw -> daily -> monthly:
#   No-sales-day gap metrics (all invoices and taxcode-buyer invoices).
# =============================================================================

def features_from_raw(df_raw):
    # -------------------------------------------------------------------------
    # PRE-PROCESS: shared base DataFrame, cached because all five paths below
    # read from it. COL_PARTITION_TIMESTAMP ("signed_date_proc") is the
    # canonical signed/issued date used across raw-level feature functions.
    # -------------------------------------------------------------------------
    df_raw_proc = (
        df_raw
        .withColumn("mst_seller",      F.trim(F.col(COL_SELLER_MST).cast("string")))
        .withColumn("mst_buyer",       F.trim(F.col(COL_BUYER_MST).cast("string")))
        .withColumn("invoice_ts",      F.to_timestamp(F.col(COL_PARTITION_TIMESTAMP)))
        .withColumn("report_date",     F.to_date("invoice_ts"))
        .withColumn("month_end",       F.last_day(F.col("report_date")))
        .withColumn("total_sales",     F.col(COL_FINAL_SALE).cast("double"))
        .withColumn("discount_amount", F.col(COL_DISCOUNT).cast("double"))
        .filter(F.col("mst_seller").isNotNull())
        .cache()   # shared across paths A-1, A-2, A-3, B-1, B-2 below
    )

    # -------------------------------------------------------------------------
    # PATH A-1: three taxcode-buyer monthly totals in ONE scan.
    # -------------------------------------------------------------------------
    df_monthly_taxcode = (
        df_raw_proc
        .filter(F.col("mst_buyer").isNotNull())
        .groupBy("mst_seller", "month_end")
        .agg(
            F.sum("discount_amount").alias("monthly_total_discount_buyer_taxcode"),
            F.countDistinct("mst_buyer").alias("monthly_distinct_buyer_count"),
            F.sum("total_sales").alias("monthly_total_sales_buyer_taxcode"),
        )
        .withColumnRenamed("month_end", "report_date")
    )

    # -------------------------------------------------------------------------
    # PATH A-2: highest single-invoice value per seller per month.
    # -------------------------------------------------------------------------
    df_monthly_highest_inv = (
        df_raw_proc
        .filter(F.col("total_sales") > 0)
        .groupBy("mst_seller", "month_end")
        .agg(F.max("total_sales").alias("monthly_sales_amt_highest_inv"))
        .withColumnRenamed("month_end", "report_date")
    )

    # -------------------------------------------------------------------------
    # PATH A-3: buyer-frequency and top-buyer sales features.
    # -------------------------------------------------------------------------
    df_buyer_agg = (
        df_raw_proc
        .filter(F.col("mst_buyer").isNotNull() & (F.col("total_sales") > 0))
        .groupBy("mst_seller", "month_end", "mst_buyer")
        .agg(
            F.count("total_sales").alias("inv_cnt"),
            F.sum("total_sales").alias("sales_amt"),
            F.sum("discount_amount").alias("disc_amt_sum"),
        )
    )

    w_freq_most  = Window.partitionBy("mst_seller", "month_end").orderBy(F.col("inv_cnt").desc())
    w_freq_least = Window.partitionBy("mst_seller", "month_end").orderBy(F.col("inv_cnt").asc())
    w_sales      = Window.partitionBy("mst_seller", "month_end").orderBy(F.col("sales_amt").desc())

    df_buyer_ranked = (
        df_buyer_agg
        .withColumn("rank_most",  F.row_number().over(w_freq_most))
        .withColumn("rank_least", F.row_number().over(w_freq_least))
        .withColumn("rank_sales", F.row_number().over(w_sales))
        .cache()
    )

    df_most_freq = (
        df_buyer_ranked
        .filter(F.col("rank_most") == 1)
        .select(
            "mst_seller",
            F.col("month_end").alias("report_date"),
            F.col("inv_cnt").alias("inv_cnt_freq_buyer_taxcode"),
            F.col("sales_amt").alias("sales_amt_most_freq_buyer_taxcode"),
            (F.col("sales_amt") / F.col("inv_cnt")).alias("sales_amt_most_freq_buyer_taxcode_per_trans"),
            F.col("disc_amt_sum").alias("disc_amt_most_freq_buyer_taxcode"),
        )
    )

    df_least_freq = (
        df_buyer_ranked
        .filter(F.col("rank_least") == 1)
        .select(
            "mst_seller",
            F.col("month_end").alias("report_date"),
            F.col("inv_cnt").alias("inv_cnt_least_freq_buyer_taxcode"),
            F.col("sales_amt").alias("sales_amt_least_freq_buyer_taxcode"),
            (F.col("sales_amt") / F.col("inv_cnt")).alias("sales_amt_least_freq_buyer_taxcode_per_trans"),
        )
    )

    df_top_buyers = (
        df_buyer_ranked
        .groupBy("mst_seller", "month_end")
        .agg(
            F.sum("sales_amt").alias("monthly_sales_amt"),
            F.sum(F.when(F.col("rank_sales") == 1, F.col("sales_amt"))).alias("sales_amt_top1_big_buyer_taxcode"),
            F.sum(F.when(F.col("rank_sales") <= 2, F.col("sales_amt"))).alias("sales_amt_top2_big_buyer_taxcode"),
            F.sum(F.when(F.col("rank_sales") <= 3, F.col("sales_amt"))).alias("sales_amt_top3_big_buyer_taxcode"),
        )
        .withColumn(
            "sales_amt_top3_big_buyer_taxcode_vs_sales_amt",
            F.when(
                F.col("monthly_sales_amt") > 0,
                F.col("sales_amt_top3_big_buyer_taxcode") / F.col("monthly_sales_amt"),
            ).otherwise(F.lit(None)),
        )
        .withColumnRenamed("month_end", "report_date")
    )

    # -------------------------------------------------------------------------
    # PATH B: raw -> daily aggregate -> monthly gap sum.
    # -------------------------------------------------------------------------
    w_count_day = Window.partitionBy("mst_seller").orderBy("report_date")

    df_day_cnt_no_sales = (
        df_raw_proc
        .groupBy("mst_seller", "month_end", "report_date")
        .agg(F.sum("total_sales").alias("daily_sales"))
        .withColumn("days_gap", F.datediff("report_date", F.lag("report_date").over(w_count_day)))
        .groupBy("mst_seller", "month_end")
        .agg(F.sum("days_gap").alias("day_cnt_no_sales"))
        .withColumnRenamed("month_end", "report_date")
    )

    df_day_cnt_no_sales_per_cus = (
        df_raw_proc
        .filter(F.col("mst_buyer").isNotNull())
        .groupBy("mst_seller", "month_end", "report_date")
        .agg(F.sum("total_sales").alias("daily_sales"))
        .withColumn("days_gap", F.datediff("report_date", F.lag("report_date").over(w_count_day)))
        .groupBy("mst_seller", "month_end")
        .agg(F.sum("days_gap").alias("day_cnt_no_sales_per_cus_taxcode"))
        .withColumnRenamed("month_end", "report_date")
    )

    # -------------------------------------------------------------------------
    # ASSEMBLY
    # -------------------------------------------------------------------------
    df_monthly_proc = (
        df_monthly_taxcode
        .join(df_monthly_highest_inv,      on=["mst_seller", "report_date"], how="left")
        .join(df_most_freq,                on=["mst_seller", "report_date"], how="left")
        .join(df_least_freq,               on=["mst_seller", "report_date"], how="left")
        .join(df_top_buyers,               on=["mst_seller", "report_date"], how="left")
        .join(df_day_cnt_no_sales,         on=["mst_seller", "report_date"], how="left")
        .join(df_day_cnt_no_sales_per_cus, on=["mst_seller", "report_date"], how="left")
    )

    # -------------------------------------------------------------------------
    # POST-PROCESSING: ratio and gap features on the raw monthly frame
    # (originally in Cell 15 notebook block)
    # -------------------------------------------------------------------------
    df_monthly_proc = df_monthly_proc.withColumn(
        "Sales_amt_per_buyer_taxcode",
        F.when(
            F.col("monthly_distinct_buyer_count") > 0,
            F.col("monthly_total_sales_buyer_taxcode") / F.col("monthly_distinct_buyer_count"),
        ).otherwise(None),
    )

    w_count_month = Window.partitionBy("mst_seller").orderBy("report_date")
    df_monthly_proc = (
        df_monthly_proc
        .withColumn("prev_report_date", F.lag("report_date").over(w_count_month))
        .withColumn("mth_cnt_no_rev",   F.months_between(F.col("report_date"), F.col("prev_report_date")))
    )

    return df_monthly_proc


# %% [Cell 14 — build_final_features]
# %pyspark

# add df_raw

# =============================================================================
# STEP 4: ORCHESTRATOR - Build all final features
# =============================================================================

def build_final_features(spark, df_daily, df_buyer_monthly, df_raw_monthly, run_date):
    """
    Orchestrate all feature groups and join into final output.

    Args:
        spark:            SparkSession
        df_daily:         output of build_staging() — daily seller-level metrics
        df_buyer_monthly: output of build_buyer_staging() — monthly buyer-seller pairs
        df_raw_monthly:   output of features_from_raw() — monthly raw features (all months);
                          pass in pre-built and cached so multi-date loops reuse it
        run_date:         Python date; features are built for the month ending on
                          f_last_day_of_previous_month(run_date)

    Returns: (df_ft, df_monthly)
        df_ft:      final feature DataFrame keyed by mst_seller + report_date
        df_monthly: intermediate monthly DataFrame (for inspection / storage)
    """
    set_last_date = f_last_day_of_previous_month(run_date)
    periods = {
        "l1m":  f_first_day_of_previous_month(set_last_date, nbr_of_mth=0),
        "l3m":  f_first_day_of_previous_month(set_last_date, nbr_of_mth=2),
        "l6m":  f_first_day_of_previous_month(set_last_date, nbr_of_mth=5),
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
    df_ft_monthly, df_ft_month_gap = build_monthly_to_multiperiod(spark, df_monthly, periods)

    # --- B.2) Night flag (from monthly) ---
    # df_ft_night_flag = build_night_flag(df_monthly)

    # --- C) Buyer concentration ---
    df_ft_concentration, df_seller_total = build_buyer_concentration(
        df_buyer_monthly, periods, set_last_date
    )

    # --- D) Top-N days ---
    df_ft_top_days = build_top_days(df_daily, df_seller_total)

    # --- E) Raw monthly features (Step 3G) ---
    # df_raw_monthly contains one row per (mst_seller, report_date=month_end) for
    # every month in the raw data.  Filter to set_last_date (the target month_end
    # for this run) so we join exactly one month's worth of raw-level features.
    # report_date is dropped before the join because build_final_features adds its
    # own report_date column after the join (see select below).
    df_ft_raw_monthly = (
        df_raw_monthly
        .filter(F.col("report_date") == F.lit(str(set_last_date)).cast("date"))
        .drop("report_date")
    )

    # --- FINAL JOIN ---
    df_ft = (
        df_direct
        .join(df_ft_monthly,       on="mst_seller", how="left")
        .join(df_ft_concentration, on="mst_seller", how="left")
        .join(df_ft_top_days,      on="mst_seller", how="left")
        .join(df_ft_raw_monthly,   on="mst_seller", how="left")
        .join(df_ft_month_gap,     on="mst_seller", how="left")
        # .join(df_ft_night_flag,  on="mst_seller", how="left")
    )

    # Add report_date
    df_ft = df_ft.select(
        F.lit(str(set_last_date)).cast("date").alias("report_date"),
        "*",
    )

    return df_ft, df_monthly


# %% [Cell 15 — Post-processing: stg monthly -> final]
# %pyspark
# NOTE: the Cell 15 notebook code has been integrated directly into the pipeline functions:
#   - df_monthly_proc ratio/gap features  →  end of features_from_raw()
#   - df_monthly month-over-month change  →  end of build_daily_to_monthly()


# %% [Cell 16 — agg_monthly_table]
# %pyspark
# NOTE: agg_monthly_table has been removed. Its aggregation logic (sum/avg/max/min over
# 3m/6m/12m periods) is now fully covered by build_monthly_to_multiperiod(), which
# dynamically discovers all monthly columns (including Cell-15-derived features) and
# aggregates them using build_agg(). No caller should reference agg_monthly_table.


# %% [Cell 17 — Main execution]
# %pyspark

# ============================================================
# Configure run parameters — edit these before running
# ============================================================
run_dates = [
    datetime.date(2025, 1, 1),
    # datetime.date(2025, 2, 1),  # add more months as needed
]

# ============================================================
# Phase 1a — Daily staging  (raw → STG_TABLE)
# ============================================================
print("Reading invoice table: {}".format(INVOICE_TABLE))
df_raw = spark.table(INVOICE_TABLE)

print("Building daily staging...")
df_stg = build_staging(df_raw)
df_stg.write.mode("overwrite").saveAsTable(STG_TABLE)
print("Staging written to {}".format(STG_TABLE))

# ============================================================
# Phase 1b — Buyer-seller monthly staging
# ============================================================
print("Building buyer staging...")
df_buyer = build_buyer_staging(df_raw)

# ============================================================
# Phase 1c — Raw monthly features (built once, reused for all run_dates)
# ============================================================
print("Building raw monthly features...")
df_raw_monthly = features_from_raw(df_raw).cache()

# ============================================================
# Phase 2 — Final features (one iteration per run_date)
# ============================================================
ft_parts      = []
monthly_parts = []

for run_date in sorted(run_dates):
    print("Computing features for run_date={}...".format(run_date))
    df_ft, df_monthly = build_final_features(
        spark, df_stg, df_buyer, df_raw_monthly, run_date
    )
    ft_parts.append(df_ft)
    monthly_parts.append(df_monthly)

df_ft_all      = reduce(lambda a, b: a.union(b), ft_parts)
df_monthly_all = reduce(lambda a, b: a.union(b), monthly_parts)

# ============================================================
# Write outputs
# ============================================================
df_monthly_all.write.mode("overwrite").saveAsTable(MONTHLY_TABLE)
print("Monthly intermediate written to {}".format(MONTHLY_TABLE))

df_ft_all.write.mode("overwrite").saveAsTable(FT_TABLE)
print("Features written to {}".format(FT_TABLE))

print("Done.")
