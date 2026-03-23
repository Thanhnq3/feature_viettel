import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "databricks-feature-engineering", "-q"])

"""
Description:
    1. Job name: Create data for CASA&TD monthly balance

    2. Output:
        ft_corp_a_balance

    3. Input:
        stg_corp_atxca_balance_daily
        stg_corp_asvtd_balance_daily
        corp_atxca_txn_agg_daily
"""
import os
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import config_function as cf
import config_path as cp
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


def f_get_daily_data(beg_date, end_date):
    df_atxca = (
        spark.table(
            f"{path_rmd_stg}.{path_stg_corp_atxca}"
        )
        .where(
            (f.col("ds_partition_date") >= beg_date)
            & (f.col("ds_partition_date") <= end_date)
        )
        .drop('ds_etl_timestamp', 'ds_partition_date', 'processing_dt')
    )

    df_asvtd = (
        spark.read.table(
            f"{path_rmd_stg}.{path_stg_corp_asvtd}"
        )
        .where(
            (f.col("ds_partition_date") >= beg_date)
            & (f.col("ds_partition_date") <= end_date)
        )
        .drop('ds_etl_timestamp', 'ds_partition_date', 'processing_dt')
    )

    # Calculate daily transaction amount in
    df_txn_daily = (
        spark.read.table(
            f"{path_rmd_stg}.{path_stg_corp_atxca_txn}"
        )
        .where(
            (f.col("ds_partition_date") >= beg_date)
            & (f.col("ds_partition_date") <= end_date)
        )
        .groupBy("customer_code", "ds_partition_date")
        .agg(
            f.sum(
                f.when(f.col("cash_flow_lv1") == "IN", f.col("txn_amt"))
            ).alias("trans_in_amt")
        )
        .withColumnRenamed("ds_partition_date", "report_date")
        .withColumnRenamed("customer_code", "cusid")
    )

    tmp_col = [
        "cusid",
        "report_date",
        "atxca_bal",
        "asvtd_bal",
        "trans_in_amt"
    ]

    df_joined = (
        df_atxca
        .join(df_asvtd, on=["cusid", "report_date"], how="full")
        .join(df_txn_daily, on=["cusid", "report_date"], how="full")
        .select(*tmp_col)
    )

    # Add some columns
    tmp_add_cols = [
        (f.coalesce(f.col("atxca_bal"), f.lit(0)) + f.coalesce(f.col("asvtd_bal"), f.lit(0))).alias("a_bal_catd"),
    ]
    df_final = df_joined.select(df_joined.columns + tmp_add_cols)

    return df_final


# Functions for Build aggregation rules
def f_ft_build_agg_simple(column: str, function: str, filter_cond: str, suffix_cond: str):
    """
    Function for create common aggregaton for group by
    """
    # Define feature name
    ft_name = column + '_' + function + suffix_cond
    # define list of aggregation
    if function == 'sum':
        agg = f.sum(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'avg':
        agg = f.avg(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'min':
        agg = f.min(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'max':
        agg = f.max(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'std':
        agg = f.std(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'pct5':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.05).alias(ft_name)
    elif function == 'pct10':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.10).alias(ft_name)
    elif function == 'pct25':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.25).alias(ft_name)
    elif function == 'med':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.50).alias(ft_name)
    elif function == 'pct75':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.75).alias(ft_name)
    elif function == 'pct90':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.90).alias(ft_name)
    elif function == 'pct95':
        agg = f.percentile(f.when(filter_cond, f.col(column)), 0.95).alias(ft_name)
    elif function == 'kurt':
        agg = f.kurtosis(f.when(filter_cond, f.col(column))).alias(ft_name)
    elif function == 'skew':
        agg = f.skewness(f.when(filter_cond, f.col(column))).alias(ft_name)

    return agg


def f_ft_build_agg_regr(column_y: str, column_x: str, function: str, filter_cond: str, suffix_cond: str):
    """
    Function for create simple regression feature
    """
    ft_name = column_y + '_' + function + suffix_cond
    if function == 'regr_slope':
        agg = f.regr_slope(
            f.when(filter_cond, f.col(column_y)),
            f.when(filter_cond, f.col(column_x))
        ).alias(ft_name)
    elif function == 'regr_intercept':
        agg = f.regr_intercept(
            f.when(filter_cond, f.col(column_y)),
            f.when(filter_cond, f.col(column_x))
        ).alias(ft_name)
    elif function == 'regr_r2':
        agg = f.regr_r2(
            f.when(filter_cond, f.col(column_y)),
            f.when(filter_cond, f.col(column_x))
        ).alias(ft_name)

    return agg


def fe_agg_from_daily(df_daily, group_by, list_condition, list_cols, list_funs):
    # 1. Build agg:
    list_agg = []
    for col in list_cols:
        for func in list_funs:
            for cond in list_condition:
                tmp_agg = f_ft_build_agg_simple(column=col, function=func, filter_cond=cond[0], suffix_cond=cond[1])
                list_agg = list_agg + [tmp_agg]
    # 2. Build df:
    df = (
        df_daily
        .groupBy(group_by)
        .agg(*list_agg)
    )

    return df


def fe_agg_from_daily_regr(df_daily, group_by, list_condition, col_x, list_cols, list_funs):
    # 1. Build agg:
    list_agg = []
    for col in list_cols:
        for func in list_funs:
            for cond in list_condition:
                tmp_agg = f_ft_build_agg_regr(column_y=col, column_x=col_x, function=func, filter_cond=cond[0], suffix_cond=cond[1])
                list_agg = list_agg + [tmp_agg]
    # 2. Build df:
    df = (
        df_daily
        .groupBy(group_by)
        .agg(*list_agg)
    )

    return df


def fe_process_ft(run_date):
    # 0. Define date
    set_last_date = cf.f_last_day_of_previous_month(data_date=run_date)
    set_start_dates = {
        'l1w': cf.f_first_day_of_previous_week(data_date=set_last_date, nbr_of_week=0),
        'l2w': cf.f_first_day_of_previous_week(data_date=set_last_date, nbr_of_week=1),
        'l1m': cf.f_first_day_of_previous_month(data_date=set_last_date, nbr_of_mth=0),
        'l3m': cf.f_first_day_of_previous_month(data_date=set_last_date, nbr_of_mth=2),
        'l6m': cf.f_first_day_of_previous_month(data_date=set_last_date, nbr_of_mth=5),
        'l12m': cf.f_first_day_of_previous_month(data_date=set_last_date, nbr_of_mth=11),
    }
    # 1. Daily input
    df_daily = f_get_daily_data(
        beg_date=set_start_dates['l12m'],
        end_date=set_last_date
    )
    df_daily = df_daily.withColumn('date_id', f.date_diff(f.col('report_date'), f.lit('2020-01-01')))
    df_daily = df_daily.repartition('cusid').cache()
    # 2.1 GR 1: Feature monthly
    # 2.1.1 Monthly_agg
    list_col_1 = [
        'atxca_bal',
        'asvtd_bal',
    ]
    list_time_1 = [[f.lit(True), 'mly']]
    list_func_1 = ['avg']
    group_by_1 = [f.col('cusid'), f.last_day(f.col('report_date')).alias('report_date')]
    df_monthly = fe_agg_from_daily(
        df_daily=df_daily,
        group_by=group_by_1,
        list_condition=list_time_1,
        list_cols=list_col_1,
        list_funs=list_func_1
    )

    df_monthly = df_monthly.withColumn('a_bal_catd_avgmly', (f.coalesce(f.col("atxca_bal_avgmly"), f.lit(0)) + f.coalesce(f.col("asvtd_bal_avgmly"), f.lit(0))))

    df_monthly_txn = (df_daily
        .withColumn("month", f.last_day("report_date"))
        .groupBy("cusid", "month")
        .agg(
            f.sum("trans_in_amt").alias("trans_in_amt_monthly")
        )
    )

    df_monthly = (df_monthly
        .join(df_monthly_txn, (df_monthly.cusid == df_monthly_txn.cusid) & (df_monthly.report_date == df_monthly_txn.month), "left")
        .drop(df_monthly_txn.cusid)
        .drop(df_monthly_txn.month)
    )

    # 2.1.2 Fe
    list_col_2 = [
        'a_bal_catd_avgmly',
    ]
    list_time_2 = [
        [f.col('report_date') >= set_start_dates['l1m'], '_l1m'],
        [f.col('report_date') >= set_start_dates['l3m'], '_l3m'],
        [f.col('report_date') >= set_start_dates['l6m'], '_l6m'],
        [f.col('report_date') >= set_start_dates['l12m'], '_l12m'],
    ]
    list_func_2 = ['sum', 'min', 'max', 'std']
    group_by_2 = [f.col('cusid')]

    coalesce_features = [
        'a_bal_catd_avgmly_min_l1m', 'a_bal_catd_avgmly_min_l3m', 'a_bal_catd_avgmly_min_l6m', 'a_bal_catd_avgmly_min_l12m',
    ]

    df_ft_1 = fe_agg_from_daily(
        df_daily=df_monthly,
        group_by=group_by_2,
        list_condition=list_time_2,
        list_cols=list_col_2,
        list_funs=list_func_2
    )
    df_ft_1 = (df_ft_1
        .withColumn('a_bal_catd_avgmly_avg_l1m', f.col('a_bal_catd_avgmly_sum_l1m') / f.lit(1))
        .withColumn('a_bal_catd_avgmly_avg_l3m', f.col('a_bal_catd_avgmly_sum_l3m') / f.lit(3))
        .withColumn('a_bal_catd_avgmly_avg_l6m', f.col('a_bal_catd_avgmly_sum_l6m') / f.lit(6))
        .withColumn('a_bal_catd_avgmly_avg_l12m', f.col('a_bal_catd_avgmly_sum_l12m') / f.lit(12))
    )
    df_ft_1 = df_ft_1.na.fill(0, subset=coalesce_features)

    # 2.2 Feature from daily
    list_col_3 = [
        'a_bal_catd'
    ]
    list_time_3 = [
        [f.col('report_date') >= set_start_dates['l1w'], '_l1w'],
        [f.col('report_date') >= set_start_dates['l2w'], '_l2w'],
        [f.col('report_date') >= set_start_dates['l1m'], '_l1m'],
        [f.col('report_date') >= set_start_dates['l3m'], '_l3m'],
        [f.col('report_date') >= set_start_dates['l6m'], '_l6m'],
        [f.col('report_date') >= set_start_dates['l12m'], '_l12m'],
    ]
    list_func_3 = ['avg', 'min', 'max']
    group_by_3 = [f.col('cusid')]
    df_ft_2 = fe_agg_from_daily(
        df_daily=df_daily,
        group_by=group_by_3,
        list_condition=list_time_3,
        list_cols=list_col_3,
        list_funs=list_func_3
    )
    # Feature from daily 2
    list_col_4 = [
        'a_bal_catd'
    ]
    list_time_4 = [
        [f.col('report_date') >= set_start_dates['l1m'], '_l1m'],
        [f.col('report_date') >= set_start_dates['l3m'], '_l3m'],
        [f.col('report_date') >= set_start_dates['l6m'], '_l6m'],
        [f.col('report_date') >= set_start_dates['l12m'], '_l12m'],
    ]
    list_func_4 = ['std', 'pct10', 'pct25', 'med', 'pct75', 'pct90', 'kurt', 'skew']
    group_by_4 = [f.col('cusid')]
    df_ft_3 = fe_agg_from_daily(
        df_daily=df_daily,
        group_by=group_by_4,
        list_condition=list_time_4,
        list_cols=list_col_4,
        list_funs=list_func_4
    )
    list_func_5 = ['regr_slope']
    df_ft_4 = fe_agg_from_daily_regr(
        df_daily=df_daily,
        group_by=group_by_4,
        list_condition=list_time_4,
        list_cols=list_col_4,
        col_x="date_id",
        list_funs=list_func_5
    )

    df_monthly = df_monthly.withColumn("a_rto_amt_in_vs_catd",
        f.when(
            f.col("a_bal_catd_avgmly") == 0, f.lit(0)
        ).otherwise(
            f.coalesce(f.col("trans_in_amt_monthly"), f.lit(0)) / f.col("a_bal_catd_avgmly")
        )
    )

    list_cols_ratio = ["a_rto_amt_in_vs_catd"]
    list_time_ratio = [
        [f.col("report_date") >= set_start_dates["l1m"], "_l1m"],
        [f.col("report_date") >= set_start_dates["l3m"], "_l3m"],
        [f.col("report_date") >= set_start_dates["l6m"], "_l6m"],
        [f.col("report_date") >= set_start_dates["l12m"], "_l12m"],
    ]
    list_func_ratio = ["sum", "min", "max"]

    df_ft_ratio = fe_agg_from_daily(
        df_daily=df_monthly,
        group_by=[f.col("cusid")],
        list_condition=list_time_ratio,
        list_cols=list_cols_ratio,
        list_funs=list_func_ratio
    )
    df_ft_ratio = (df_ft_ratio
        .withColumn('a_rto_amt_in_vs_catd_avg_l1m', f.col('a_rto_amt_in_vs_catd_sum_l1m') / f.lit(1))
        .withColumn('a_rto_amt_in_vs_catd_avg_l3m', f.col('a_rto_amt_in_vs_catd_sum_l3m') / f.lit(3))
        .withColumn('a_rto_amt_in_vs_catd_avg_l6m', f.col('a_rto_amt_in_vs_catd_sum_l6m') / f.lit(6))
        .withColumn('a_rto_amt_in_vs_catd_avg_l12m', f.col('a_rto_amt_in_vs_catd_sum_l12m') / f.lit(12))
    )
    ratio_coalesce_features = [
        'a_rto_amt_in_vs_catd_avg_l1m', 'a_rto_amt_in_vs_catd_avg_l3m', 'a_rto_amt_in_vs_catd_avg_l6m', 'a_rto_amt_in_vs_catd_avg_l12m',
        'a_rto_amt_in_vs_catd_min_l1m', 'a_rto_amt_in_vs_catd_min_l3m', 'a_rto_amt_in_vs_catd_min_l6m', 'a_rto_amt_in_vs_catd_min_l12m',
        'a_rto_amt_in_vs_catd_max_l1m', 'a_rto_amt_in_vs_catd_max_l3m', 'a_rto_amt_in_vs_catd_max_l6m', 'a_rto_amt_in_vs_catd_max_l12m',
    ]

    df_ft_ratio = df_ft_ratio.na.fill(0, subset=ratio_coalesce_features)

    ### final join
    df_ft = (
        df_ft_1
        .join(df_ft_2, on="cusid")
        .join(df_ft_3, on="cusid")
        .join(df_ft_4, on="cusid")
        .join(df_ft_ratio, on="cusid")
        .drop("customer_code")
    )
    df_ft = df_ft.select([f.lit(set_last_date).alias('report_date')] + df_ft.columns)
    return df_ft


if __name__ == "__main__":
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    path_curated_domain = dbutils.widgets.get("path_curated_domain")
    path_rmd_stg = dbutils.widgets.get("path_rmd_stg")
    path_stg_corp_atxca = dbutils.widgets.get("path_stg_corp_atxca")
    path_stg_corp_asvtd = dbutils.widgets.get("path_stg_corp_asvtd")
    path_stg_corp_atxca_txn = dbutils.widgets.get("path_stg_corp_atxca_txn")
    set_output_schema = dbutils.widgets.get("set_output_schema")
    set_output_table = dbutils.widgets.get("set_output_table")
    set_run_date = dbutils.widgets.get("set_run_date")

    if set_run_date != "None":
        set_run_date = datetime.datetime.strptime(set_run_date, "%Y-%m-%d").date()
    else:
        set_run_date = datetime.date.today()

    last_eom_date = cf.f_last_day_of_previous_month(data_date=set_run_date)

    # no need pre-check

    # process data
    df = fe_process_ft(run_date=set_run_date)

    # write to table
    cf.f_ft_insert(
        df=df,
        schema=set_output_schema,
        table=set_output_table,
        partition="report_date"
    )

    print("done")