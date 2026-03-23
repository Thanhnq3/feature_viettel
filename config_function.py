# config_function.py

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(" ").getOrCreate()

# FUNCTIONS FOR DATE ----------------------------------------------------
import datetime
from dateutil.relativedelta import relativedelta
def f_last_day_of_previous_month(data_date):
    prev_month = data_date.replace(day=1) - datetime.timedelta(days=1)
    return prev_month

def f_first_day_of_previous_week(data_date, nbr_of_week: int):
    """
    Get first day, previous week
    """
    first_date = data_date.replace(day=1) - relativedelta(weeks=nbr_of_week)
    return first_date

def f_first_day_of_previous_month(data_date, nbr_of_mth: int):
    """
    Get first day, previous month
    """
    first_date = data_date.replace(day=1) - relativedelta(months=nbr_of_mth)
    return first_date

# FUNCTIONS FOR TRANSFORM DATA ----------------------------------------------------
# Function fill cus and timekey
def fe_fill_data(df, cols_cross_join = [], fill=0):
    """
    Funtion will cross join cols_cross_join and create new data frame.
    Then Left join df to new df and fill null
    """
    for col in cols_cross_join:
        if col == cols_cross_join[0]:
            df_full = df.select(col).distinct()
        else:
            df_full = df_full.crossJoin(
                df.select(col).distinct()
            )
            # in case khong co du lieu ngay cn, code nay cung khong fill data them (khong sinh them dong du lieu vao ngay nghi)
    df_full = df_full.cache()
    df_full = df_full.join(df, on = cols_cross_join, how = "left").na.fill(fill).cache()
    return df_full


# Function to process all customer ana a timekey
def fe_fill_data_cus(df, df_cus, col_date, fill=0):
    """
    Funtion will cross join cols_cross_join and create new data frame.
    Then Left join df to new df and fill null
    """
    df_full = df_cus.crossJoin(
        df.select(col_date).distinct()
    )
    df_full = df_full.cache()
    df_full = df_full.join(df, on = df_full.columns, how = "left").na.fill(fill).cache()
    return df_full


# Function change unit:
def fe_unit_change(df, list_cols, unit):
    for col in list_cols:
        df = df.withColumn(col, f.col(col).try_cast('double') * f.lit(unit))
    return df



# 2. FUNCTIONS FOR INSERT DATA ------------------------------------------------------------------------------
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
# 2.1 Normal Insert
def f_data_insert(
    df,
    schema,
    table,
    partition,
    cusid = "cusid",
    description="",
):
    # Define some column for manage table:
    key = [
        f.current_timestamp().alias("ds_etl_timestamp"),
        f.col(partition).cast("date").alias("ds_partition_date"),
        f.col(partition).cast("timestamp").alias("processing_dt"),
    ]
    df = df.select(key + df.columns)

    # Check and create table
    table_name = schema + "." + table
    primary_keys=[cusid] + ["processing_dt"]
    print(primary_keys)
    if not spark.catalog.tableExists(table_name):
        fe.create_table(
            name=table_name,
            primary_keys=primary_keys,
            schema=df.schema,
            description=description,
            partition_columns=partition,
        )
    print("Insert data into feature table: " + table_name)
    # Insert data:
    fe.write_table(name=table_name, df=df, mode="merge")
    print("------")

# 2.2 Feature table insert using feature engineering client
def f_ft_insert(
    df,
    schema,
    table,
    partition,
    cusid = "cusid",
    description="",
):
    # Define some column for manage table:
    key = [
        f.current_timestamp().alias("ds_etl_timestamp"),
        (f.lit(None).cast("string")).alias("ds_etl_job_id"),
        (f.lit(None).cast("string")).alias("ds_source_system"),
        (f.lit(None).cast("string")).alias("ds_key"),
        f.col(partition).cast("date").alias("ds_partition_date"),
        f.col(partition).cast("timestamp").alias("processing_dt"),
    ]
    df = df.select(key + df.columns)

    # Check and create table
    table_name = schema + "." + table
    primary_keys=[cusid] + ["processing_dt"]
    print(primary_keys)
    if not spark.catalog.tableExists(table_name):
        fe.create_table(
            name=table_name,
            primary_keys=primary_keys,
            schema=df.schema,
            description=description,
            partition_columns=partition,
            timestamp_keys="processing_dt"
        )
    print("Insert data into feature table: " + table_name)
    # Insert data:
    fe.write_table(name=table_name, df=df, mode="merge")
    print("------")