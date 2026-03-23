from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, lower, coalesce, lit
from pyspark.sql import Window as W
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
import re
import itertools
from typing import List, Tuple

%md
#### Helper function

# --------- Normalize helpers (UDFs) ---------
@F.udf(T.ArrayType(T.StringType()))
def normalize_phone_candidates_udf(s: str) -> List[str]:
    if s is None:
        return []
    tokens = re.split(r"\D+", str(s))
    out = []
    for t in tokens:
        if not t:
            continue
        if t.startswith("84") and len(t) >= 11:
            t = "0" + t[2:]
        t = re.sub(r"^0+", "0", t)
        if 9 <= len(t) <= 12:
            out.append(t)
    # unique, keep order
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

@F.udf(T.ArrayType(T.StringType()))
def normalize_emails_udf(s: str) -> List[str]:
    if s is None:
        return []
    parts = re.split(r"[\s,;|/]+", str(s).lower())
    email_re = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")
    out = []
    for p in parts:
        p = p.strip()
        if p and email_re.match(p):
            out.append(p)
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

@F.udf(T.ArrayType(T.StringType()))
def normalize_id_doc_udf(s: str) -> List[str]:
    if s is None:
        return []
    parts = re.split(r"[\s,;|/]+", str(s).upper())
    out = []
    for p in parts:
        p = re.sub(r"[^A-Z0-9]", "", p)
        if p:
            out.append(p)
    seen, uniq = set(), []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

# Union nhiều mảng (array_union chỉ nhận 2 đối số)
def array_union_chain(*cols):
    col = cols[0]
    for c in cols[1:]:
        col = F.array_union(col, c)
    return col

# Tạo mọi tổ hợp 2 phần tử từ danh sách MST (trả array<struct<a,b>>)
pair_struct = T.StructType([
    T.StructField("a", T.StringType(), False),
    T.StructField("b", T.StringType(), False),
])

@F.udf(T.ArrayType(pair_struct))
def combinations_pairs_udf(msts: List[str]) -> List[Tuple[str, str]]:
    if not msts:
        return []
    # chuẩn hoá unique + sort để ổn định
    ms = sorted(set([str(x) for x in msts]))
    out = []
    for a, b in itertools.combinations(ms, 2):
        # cặp vô hướng: (min, max)
        x = min(a, b)
        y = max(a, b)
        out.append({"a": x, "b": y})
    return out

# UDF join mảng chuỗi bằng dấu phẩy
@F.udf(T.StringType())
def join_with_comma_udf(arr: List[str]) -> str:
    if not arr:
        return None
    return ", ".join([str(x) for x in arr])

# UDF format detail: from array<struct<rel:string, vals:array<string>>>
detail_struct = T.ArrayType(
    T.StructType([
        T.StructField("rel", T.StringType()),
        T.StructField("vals", T.ArrayType(T.StringType()))
    ])
)

@F.udf(T.StringType())
def format_detail_udf(items):
    if not items:
        return None
    segs = []
    for it in items:
        rel = it["rel"]
        vals = it["vals"] or []
        segs.append("{" + f"{rel}: {', '.join(vals)}" + "}")
    return ", ".join(segs)
	
%md
##### find_related_customers_spark

def find_related_customers_spark(
    df: DataFrame,
    mst_col: str = "col18",
    rep_id_col: str = "col12",
    email_cols: Tuple[str, ...] = ("col21", "col211", "col212"),
    phone_cols: Tuple[str, ...] = ("col27", "col52", "col22"),
) -> DataFrame:
    data = (
        df.withColumn(mst_col, F.col(mst_col).cast("string"))
          .withColumn(mst_col, F.trim(F.col(mst_col)))
    )

    # ----- Build normalized arrays -----
    # Phones
    phone_arrays = []
    for c in phone_cols:
        phone_arrays.append(
            normalize_phone_candidates_udf(F.col(c).cast("string"))
            if c in data.columns else F.array()
        )
    phones_all = array_union_chain(*[F.coalesce(p, F.array()) for p in phone_arrays])

    # Emails (gộp chung thành 1 chỉ tiêu "Trùng email")
    email_arrays = []
    for c in email_cols:
        email_arrays.append(
            normalize_emails_udf(F.col(c).cast("string"))
            if c in data.columns else F.array()
        )
    emails_all = array_union_chain(*[F.coalesce(e, F.array()) for e in email_arrays])

    # Rep IDs
    rep_ids = normalize_id_doc_udf(F.col(rep_id_col).cast("string")) if rep_id_col in data.columns else F.array()

    data = (
        data.withColumn("_phones_all", F.array_sort(F.array_distinct(phones_all)))
            .withColumn("_emails_all", F.array_sort(F.array_distinct(emails_all)))
            .withColumn("_rep_ids",    F.array_sort(F.array_distinct(rep_ids)))
    )

    # ----- Helper: from (mst, array) -> per value pairs (a,b,val,label) -----
    def build_pairs_with_vals(list_col: str, label: str) -> DataFrame:
        exploded = (
            data.select(F.col(mst_col).alias("mst"), F.col(list_col).alias("arr"))
                .withColumn("val", F.explode_outer("arr"))
                .filter(F.col("val").isNotNull() & (F.col("val") != ""))
        )
        # group by val -> collect msts (distinct), then generate pairs
        msts_by_val = (
            exploded.groupBy("val")
                    .agg(F.collect_set("mst").alias("msts"))
                    .withColumn("pairs", combinations_pairs_udf(F.col("msts")))
                    .select("val", F.explode_outer("pairs").alias("p"))
                    .select("val", F.col("p.a").alias("a"), F.col("p.b").alias("b"))
                    .withColumn("rel", F.lit(label))
        )
        return msts_by_val

    pairs_phone = build_pairs_with_vals("_phones_all", "Trùng SĐT")
    pairs_email = build_pairs_with_vals("_emails_all", "Trùng email")
    pairs_iddoc = build_pairs_with_vals("_rep_ids",   "Trùng Số giấy tờ người đại diện")

    all_pairs = pairs_phone.unionByName(pairs_email, allowMissingColumns=True).unionByName(pairs_iddoc, allowMissingColumns=True)

    # ----- Aggregate: (a,b,rel) -> vals; then (a,b) -> relationship + detail -----
    per_rel = (
        all_pairs.groupBy("a","b","rel")
                 .agg(F.array_sort(F.array_distinct(F.collect_list("val"))).alias("vals"))
    )

    # mối quan hệ: join ", " trên danh sách rel duy nhất
    rel_summary = (
        per_rel.groupBy("a","b")
               .agg(F.array_sort(F.array_distinct(F.collect_list("rel"))).alias("rels"))
               .withColumn("moi_quan_he", join_with_comma_udf(F.col("rels")))
               .select("a","b","moi_quan_he")
    )

    # chi tiết: {rel: v1, v2}, ...
    detail_summary = (
        per_rel.groupBy("a","b")
               .agg(F.collect_list(F.struct(F.col("rel").alias("rel"), F.col("vals").alias("vals"))).alias("items"))
               .withColumn("chi_tiet_gia_tri_trung", format_detail_udf(F.col("items")))
               .select("a","b","chi_tiet_gia_tri_trung")
    )

    out = (
        rel_summary.join(detail_summary, on=["a","b"], how="left")
                   .select(
                       F.col("a").alias("Mã số thuế đơn vị"),
                       F.col("b").alias("Mã số thuế bên liên quan"),
                       F.col("moi_quan_he").alias("mối quan hệ"),
                       F.col("chi_tiet_gia_tri_trung")
                   )
                   .orderBy("Mã số thuế đơn vị","Mã số thuế bên liên quan")
    )
    return out
	
%md
##### cust_revenue_concentration_80pct_spark

def cust_revenue_concentration_xx_pct_spark(
    df: DataFrame,
    date_col: str = "invoice_date",
    amount_col: str = "amount",
    id_col: str = "ID_KH",
    last_n_months: int = 12,
    threshold: float = 0.8,
    drop_na_amount: bool = True,
    positive_only_for_calc: bool = False,
    group_by_customer: bool = True
) -> DataFrame:


    # 1. Chuẩn hoá dữ liệu

    df0 = (
        df.withColumn(date_col, F.to_timestamp(F.col(date_col)))
          .filter(F.col(date_col).isNotNull())
          .withColumn(amount_col, F.col(amount_col).cast("double"))
    )

    if drop_na_amount:
        df0 = df0.filter(F.col(amount_col).isNotNull())
    else:
        df0 = df0.fillna({amount_col: 0.0})

    # day & month
    df0 = (
        df0.withColumn("day", F.to_date(F.col(date_col)))
           .withColumn("month_start", F.date_trunc("month", F.col("day")))
           .withColumn("month", F.date_format(F.col("month_start"), "yyyy-MM"))
    )

    # 2. Lấy N tháng gần nhất

    months_df = (
        df0.select("month_start")
           .distinct()
           .orderBy(F.col("month_start").desc())
           .limit(last_n_months)
    )

    df0 = df0.join(months_df, on="month_start", how="inner")

    # 3. Tổng hợp doanh thu theo NGÀY

    group_daily_keys = ["day"]
    if group_by_customer and id_col in df0.columns:
        group_daily_keys = [id_col, "day"]

    daily = (
        df0.groupBy(*group_daily_keys, "month_start", "month")
           .agg(F.sum(amount_col).alias("daily_revenue"))
    )


    # 4. Tổng ròng tháng

    group_month_keys = ["month_start", "month"]
    if group_by_customer and id_col in daily.columns:
        group_month_keys = [id_col, "month_start", "month"]

    net_month_total = (
        daily.groupBy(*group_month_keys)
             .agg(F.sum("daily_revenue").alias("net_month_total"))
    )


    # 5. Chọn tập ngày dùng để tính 80%

    if positive_only_for_calc:
        calc_df = daily.filter(F.col("daily_revenue") > 0)
    else:
        calc_df = daily

    # Tổng mẫu số để tính %
    month_total = (
        calc_df.groupBy(*group_month_keys)
               .agg(F.sum("daily_revenue").alias("month_total"))
    )


    # 6. Xếp hạng ngày theo doanh thu giảm dần

    w = W.partitionBy(*group_month_keys).orderBy(F.col("daily_revenue").desc())

    ranked = (
    calc_df
    .withColumn("day_rank", F.row_number().over(w))
    .withColumn(
        "cum_revenue",
        F.sum("daily_revenue").over(
            w.rowsBetween(W.unboundedPreceding, W.currentRow)
            )
        )
    )

    ranked = ranked.withColumn(
        "cum_pct",
        F.when(
            F.sum("daily_revenue").over(W.partitionBy(*group_month_keys)) > 0,
            F.col("cum_revenue") /
            F.sum("daily_revenue").over(W.partitionBy(*group_month_keys))
        ).otherwise(F.lit(0.0))
    )

    # 7. Xác định số ngày cần để đạt threshold

    cutoff = (
        ranked.filter(F.col("cum_pct") >= F.lit(threshold))
              .groupBy(*group_month_keys)
              .agg(F.min("day_rank").alias("days_needed_for_threshold"))
    )

    # 8. Lấy danh sách ngày được chọn

    selected_days = (
    ranked.join(cutoff, on=group_month_keys, how="inner")
          .filter(F.col("day_rank") <= F.col("days_needed_for_threshold"))
          .withColumn("day_of_month", F.dayofmonth(F.col("day")))
          .groupBy(*group_month_keys, "days_needed_for_threshold")
          .agg(
              F.collect_list("day_of_month").alias("selected_days"),
              F.max("cum_pct").alias("covered_pct") 
          )
    )

    # 9. Số ngày có phát sinh trong tháng

    days_in_data = (
        daily.groupBy(*group_month_keys)
             .agg(F.countDistinct("day").alias("total_days_in_month_in_data"))
    )

    # 10. Kết quả cuối

    result = (
    net_month_total
        .join(month_total, on=group_month_keys, how="left")
        .join(selected_days, on=group_month_keys, how="left")
        .join(days_in_data, on=group_month_keys, how="left")
        .withColumn(
            "share_of_days",
            F.when(
                F.col("total_days_in_month_in_data") > 0,
                F.col("days_needed_for_threshold") / F.col("total_days_in_month_in_data")
            ).otherwise(F.lit(0.0))
        )
)

    # 11. KQ sx

    sort_cols = ["month_start"]
    if group_by_customer and id_col in result.columns:
        sort_cols = [id_col] + sort_cols

    cols = [
        "month",
        "month_start",
        "net_month_total",
        "month_total",
        "days_needed_for_threshold",
        "covered_pct",
        "selected_days",
        "total_days_in_month_in_data",
        "share_of_days"
    ]
    if group_by_customer and id_col in result.columns:
        cols = [id_col] + cols

    return result.select(*cols).orderBy(*sort_cols)

%md
##### revenue_links_xx_month_spark
def revenue_links_xx_month_spark(
    df_rev: DataFrame,
    related_pairs: DataFrame,
    ts_col: str = "col4",
    seller_col: str = "col21",
    buyer_col: str = "col29",
    amount_col: str = "col52",
    months: int = 12,
    now_ts: str = None,      # "2026-02-28T23:59:59" 
    include_self: bool = False,
    monthly_breakdown: bool = False
) -> dict:
    df = (df_rev
          .withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
          .withColumn(seller_col, F.trim(F.col(seller_col).cast("string")))
          .withColumn(buyer_col,  F.trim(F.col(buyer_col).cast("string")))
          .withColumn(amount_col, F.col(amount_col).cast("double"))
         )

    # Filter valid rows
    df = df.filter(F.col(ts_col).isNotNull() & F.col(seller_col).isNotNull() & F.col(buyer_col).isNotNull())
    if not include_self:
        df = df.filter(F.col(seller_col) != F.col(buyer_col))

    # Time window: last N months
    now_expr = F.current_timestamp() if now_ts is None else F.to_timestamp(F.lit(now_ts))
    since_expr = F.add_months(now_expr, -months)
    df = df.filter(F.col(ts_col) >= since_expr)

    # Aggregate pair-level
    pair_agg = (
        df.groupBy(seller_col, buyer_col)
          .agg(
              F.sum(F.coalesce(F.col(amount_col), F.lit(0.0))).alias("revenue_12m"),
              F.count(F.lit(1)).alias("invoice_count_12m")
          )
          .withColumnRenamed(seller_col, "issuer_mst")
          .withColumnRenamed(buyer_col,  "buyer_mst")
    )

    # Canonical pair (a,b) = (least, greatest)
    pair_agg = pair_agg.withColumn("a", F.least(F.col("issuer_mst"), F.col("buyer_mst"))) \
                       .withColumn("b", F.greatest(F.col("issuer_mst"), F.col("buyer_mst")))

    # Prepare related pairs (already canonical)
    rel = (related_pairs
           .select(
               F.col("Mã số thuế đơn vị").alias("A"),
               F.col("Mã số thuế bên liên quan").alias("B"),
               F.col("mối quan hệ").alias("relationship"),
               F.col("chi_tiet_gia_tri_trung").alias("relationship_detail")
           )
           .withColumn("a", F.least(F.col("A"), F.col("B")))
           .withColumn("b", F.greatest(F.col("A"), F.col("B")))
           .select("a","b","relationship","relationship_detail")
           .dropDuplicates(["a","b"])
    )

    # Join tag
    pair_level_12m = (
        pair_agg.join(rel, on=["a","b"], how="left")
                .withColumn("is_related", F.col("relationship").isNotNull())
                .select(
                    "issuer_mst","buyer_mst","revenue_12m","invoice_count_12m",
                    "is_related",
                    F.col("relationship").alias("mối quan hệ"),
                    F.col("relationship_detail").alias("chi_tiet_gia_tri_trung")
                )
                .orderBy("issuer_mst","buyer_mst")
    )

    # Seller overview
    seller_overview_12m = (
        pair_level_12m
        .withColumn("revenue_rel",   F.when(F.col("is_related"), F.col("revenue_12m")).otherwise(F.lit(0.0)))
        .withColumn("revenue_unrel", F.when(~F.col("is_related"), F.col("revenue_12m")).otherwise(F.lit(0.0)))
        .groupBy("issuer_mst")
        .agg(
            F.sum("revenue_12m").alias("revenue_12m"),
            F.sum("invoice_count_12m").alias("invoice_count_12m"),
            F.sum("revenue_rel").alias("revenue_to_related_12m"),
            F.sum("revenue_unrel").alias("revenue_to_unrelated_12m"),
            F.countDistinct("buyer_mst").alias("n_counterparties"),
            F.sum(F.when(F.col("is_related"), F.lit(1)).otherwise(F.lit(0))).alias("n_related_counterparties"),
        )
        .orderBy("issuer_mst")
    )

    out = {
        "pair_level_12m": pair_level_12m,
        "seller_overview_12m": seller_overview_12m
    }

    if monthly_breakdown:
        by_month = (
            df.withColumn("month_start", F.date_trunc("month", F.col(ts_col)))
              .groupBy(seller_col, buyer_col, "month_start")
              .agg(
                  F.sum(F.coalesce(F.col(amount_col), F.lit(0.0))).alias("revenue"),
                  F.count(F.lit(1)).alias("invoice_count")
              )
              .withColumnRenamed(seller_col, "issuer_mst")
              .withColumnRenamed(buyer_col,  "buyer_mst")
              .withColumn("a", F.least(F.col("issuer_mst"), F.col("buyer_mst")))
              .withColumn("b", F.greatest(F.col("issuer_mst"), F.col("buyer_mst")))
              .join(rel, on=["a","b"], how="left")
              .withColumn("is_related", F.col("relationship").isNotNull())
              .select(
                  "issuer_mst","buyer_mst","month_start","revenue","invoice_count",
                  "is_related",
                  F.col("relationship").alias("mối quan hệ"),
                  F.col("relationship_detail").alias("chi_tiet_gia_tri_trung")
              )
              .orderBy("issuer_mst","buyer_mst","month_start")
        )
        out["pair_by_month"] = by_month

    return out
    
%md
##### flag_night_sale_xx_month_spark
def flag_night_sale_xx_month_spark(
    df_rev: DataFrame,
    ts_col: str = "col4",
    seller_col: str = "col21",
    months: int = 12,
    now_ts: str = None,
    use_calendar_days: bool = False,     # False: avg theo ngày có GD; True: theo ngày lịch
    night_start: int = 22,
    night_end: int = 6,                  # [22, 6)
    core_start: int = 0,                 # Rui ro hon
    core_end: int = 4,                  # Rui ro hon
    min_avg_invoices_per_day: float = 100.0,
    min_avg_night_ratio: float = 0.30
) -> Tuple[DataFrame, DataFrame]:

    # Chuẩn hoá cơ bản
    df = (df_rev
          .withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
          .withColumn(seller_col, F.trim(F.col(seller_col).cast("string")))
         ).filter(F.col(ts_col).isNotNull() & F.col(seller_col).isNotNull())

    # Lọc 12M
    now_expr = F.current_timestamp() if now_ts is None else F.to_timestamp(F.lit(now_ts))
    since_expr = F.add_months(now_expr, -months)
    df = df.filter(F.col(ts_col) >= since_expr)

    if df.limit(1).count() == 0:
        empty_schema_eval = T.StructType([
            T.StructField("issuer_mst", T.StringType()),
            T.StructField("qualified_months_count_12m", T.IntegerType()),
            T.StructField("is_flagged", T.BooleanType()),
            T.StructField("qualified_months", T.ArrayType(T.StringType())),
            T.StructField("notes", T.StringType()),
        ])
        empty_schema_monthly = T.StructType([
            T.StructField(seller_col, T.StringType()),
            T.StructField("month_start", T.TimestampType()),
            T.StructField("month", T.StringType()),
            T.StructField("avg_invoices_per_day_month", T.DoubleType()),
            T.StructField("avg_night_ratio_month", T.DoubleType()),
            T.StructField("avg_core_ratio_month", T.DoubleType()),
            T.StructField("active_days", T.IntegerType()),
            T.StructField("total_invoices_month", T.LongType()),
            T.StructField("total_night_invoices_month", T.LongType()),
            T.StructField("meets_volume", T.BooleanType()),
            T.StructField("meets_night_ratio", T.BooleanType()),
            T.StructField("qualified_month", T.BooleanType()),
        ])
        spark = df_rev.sql_ctx.sparkSession
        return spark.createDataFrame([], empty_schema_eval), spark.createDataFrame([], empty_schema_monthly)

    # Derive hour, day, month_start
    df = (df
          .withColumn("hour", F.hour(F.col(ts_col)))
          .withColumn("day", F.to_date(F.col(ts_col)))
          .withColumn("month_start", F.date_trunc("month", F.col(ts_col)))
          .withColumn("month", F.date_format(F.col("month_start"), "yyyy-MM"))
    )

    # is_night [22:00, 06:00) qua nửa đêm: if hour >= 22 or hour < 6
    df = df.withColumn("is_night", (F.col("hour") >= F.lit(night_start)) | (F.col("hour") < F.lit(night_end)))
    # is_core [00:00, 04:00)
    df = df.withColumn("is_core", (F.col("hour") >= F.lit(core_start)) & (F.col("hour") < F.lit(core_end)))

    # Daily aggregation per issuer
    daily = (
        df.groupBy(seller_col, "day")
          .agg(
              F.count(F.lit(1)).alias("total_invoices_day"),
              F.sum(F.when(F.col("is_night"), F.lit(1)).otherwise(F.lit(0))).alias("night_invoices_day"),
              F.sum(F.when(F.col("is_core"), F.lit(1)).otherwise(F.lit(0))).alias("core_invoices_day"),
              F.first("month_start").alias("month_start"),
              F.first("month").alias("month"),
          )
          .withColumn("daily_night_ratio",
                      F.col("night_invoices_day")/F.col("total_invoices_day"))
          .withColumn("daily_core_ratio",
                      F.col("core_invoices_day")/F.col("total_invoices_day"))
    )

    if not use_calendar_days:
        monthly = (
            daily.groupBy(seller_col, "month_start", "month")
                 .agg(
                     F.avg("total_invoices_day").alias("avg_invoices_per_day_month"),
                     F.avg("daily_night_ratio").alias("avg_night_ratio_month"),
                     F.avg("daily_core_ratio").alias("avg_core_ratio_month"),
                     F.countDistinct("day").alias("active_days"),
                     F.sum("total_invoices_day").alias("total_invoices_month"),
                     F.sum("night_invoices_day").alias("total_night_invoices_month"),
                 )
        )
    else:
        # số ngày lịch trong tháng = dayofmonth(last_day(month_start))
        days_in_month = (
            daily.select("month_start").distinct()
                 .withColumn("days_in_month", F.dayofmonth(F.last_day(F.col("month_start"))))
        )
        base = (
            daily.groupBy(seller_col, "month_start", "month")
                 .agg(
                     F.sum("total_invoices_day").alias("total_invoices_month"),
                     F.sum("night_invoices_day").alias("total_night_invoices_month"),
                     F.countDistinct("day").alias("active_days"),
                     F.avg("daily_night_ratio").alias("avg_night_ratio_month"),
                     F.avg("daily_core_ratio").alias("avg_core_ratio_month"),
                 )
                 .join(days_in_month, on="month_start", how="left")
        )
        monthly = base.withColumn(
            "avg_invoices_per_day_month",
            F.col("total_invoices_month")/F.col("days_in_month").cast("double")
        ).drop("days_in_month")

    monthly = (monthly
               .withColumn("meets_volume", F.col("avg_invoices_per_day_month") >= F.lit(min_avg_invoices_per_day))
               .withColumn("meets_night_ratio", F.col("avg_night_ratio_month") >= F.lit(min_avg_night_ratio))
               .withColumn("qualified_month", F.col("meets_volume") & F.col("meets_night_ratio"))
    )

    # Count qualified months & list months per issuer
    issuer_eval = (
        monthly.groupBy(seller_col)
               .agg(
                   F.sum(F.when(F.col("qualified_month"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("qualified_months_count_12m"),
                   F.array_sort(F.array_distinct(F.collect_list(F.when(F.col("qualified_month"), F.col("month")).otherwise(F.lit(None))))).alias("qualified_months")
               )
               .withColumnRenamed(seller_col, "issuer_mst")
               .withColumn("qualified_months", F.expr("filter(qualified_months, x -> x is not null)"))
               .withColumn("is_flagged", F.col("qualified_months_count_12m") >= F.lit(2))
               .withColumn("notes", F.concat_ws(
                   " ", 
                   F.col("qualified_months_count_12m").cast("string"),
                   F.lit("tháng đạt: TB>="), F.lit(int(min_avg_invoices_per_day)).cast("string"),
                   F.lit("HĐ/ngày & TB>="), F.lit(int(min_avg_night_ratio*100)).cast("string"),
                   F.lit("% HĐ ban đêm")
               ))
               .orderBy(F.desc("is_flagged"), F.desc("qualified_months_count_12m"), "issuer_mst")
    )

    monthly_detail = (monthly
                      .withColumnRenamed(seller_col, "issuer_mst")
                      .orderBy("issuer_mst","month_start"))

    return issuer_eval, monthly_detail