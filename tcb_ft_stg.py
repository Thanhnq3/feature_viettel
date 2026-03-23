import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "databricks-feature-engineering", "-q"])

"""
Description:
    1. Job name: Create data for saving account balance daily

    2. Output:
        stg_corp_asvcd_balance_daily

    3. Input:
        tcb_data_prod_apse_1_curated.v_domain_data.td_product_holding_gra_hist
        tcb_data_prod_apse_1_curated.v_domain_data.au_bal
"""
import os
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as f, Window
import config_function as cf
import config_path as cp

spark = SparkSession.builder.getOrCreate()


def fe_corp_asvcd_balance_daily(beg_date, end_date, unit=(1 / 1.0e6)):
    beg_date_str = beg_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Query
    query = f"""
        WITH 
            data_date AS (
                SELECT
                    '{beg_date}' AS beg_date,
                    '{end_date}' AS end_date
            )
            , cdr_full as 
            (
                select * from 
                (
                    select cdr2.date_code latest, 
                        cdr.date_code,  
                        row_number () OVER (PARTITION BY cdr.date_code ORDER BY cdr2.date_code DESC) rn 
                    from {path_curated_domain}.calendar cdr
                    join (
                        select * from {path_curated_domain}.calendar cdr
                        where cdr.date_code between (SELECT date_add(month,-1,date_trunc("month",beg_date)) FROM data_date) and (SELECT end_date FROM data_date)
                        and cdr.off_date = false
                        ) cdr2 
                        on cdr2.date_code <= cdr.date_code
                    where cdr.date_code between (SELECT beg_date FROM data_date) and (SELECT end_date FROM data_date)
                )
                where rn=1
            )
            ,cd AS (
                SELECT 
                    unq_id_src_stm, 
                    cast(ds_partition_date as date) as ds_partition_date, 
                    cst_code
                FROM {path_curated_domain}.td_product_holding_gra_hist
                WHERE 1=1
                    AND ds_rec_st = 1
                    AND ar_bsn_line != "RETAIL"
                    AND pd_lvl_2_nm IN ("Valuable papers issued to customers", "CORP_Valuable papers issued to customers")
                    AND cast(ds_partition_date as date) >= (SELECT date_add(month,-1,date_trunc("month",beg_date)) FROM data_date)
                    AND cast(ds_partition_date as date) <= (SELECT end_date FROM data_date)
            )
            , au AS (
                SELECT
                    unq_id_src_stm, ppn_dt, SUM(net_amt_lcy) AS net_amt_lcy
                FROM {path_curated_domain}.au_bal
                WHERE 1=1  
                    AND au_tp_code IN ("INDUE_PRIN_BAL", "INDUE_PRIN_BAL_DEBIT", "PST_DUE_PRIN")
                    AND ppn_dt >= (SELECT date_add(month,-1,date_trunc("month",beg_date)) FROM data_date)
                    AND ppn_dt <= (SELECT end_date FROM data_date)
                GROUP BY unq_id_src_stm, ppn_dt
                HAVING SUM(net_amt_lcy) > 0
            )
        SELECT
            cd.cst_code as cusid
            , cast(cdr_full.date_code as date) AS report_date
            , CAST(SUM(au.net_amt_lcy) * {unit} AS DOUBLE) AS asvcd_bal
        FROM cdr_full
            LEFT JOIN
            cd
            ON cdr_full.latest = cd.ds_partition_date
            JOIN 
            au
            ON cd.unq_id_src_stm = au.unq_id_src_stm 
            AND cdr_full.latest = au.ppn_dt
        GROUP BY cd.cst_code, cdr_full.date_code
    """
    # Get data
    df_daily = spark.sql(query)

    return df_daily


def pre_check(run_date):
    # Table path
    pre_check_condition_1 = {
        'td_product_holding_gra_hist': f"{path_curated_domain}.td_product_holding_gra_hist"                   
    } 

    pre_check_condition_2 = {        
        'au_bal': f"{path_curated_domain}.au_bal"             
    }   

    not_met_conditions = []

    # Check data source 1
    for table_name, table_path in pre_check_condition_1.items():
        query = f"""
            select ds_partition_date from {table_path}
            where cast(ds_partition_date as date) > date('{run_date}')
            and cast(ds_partition_date as date) <= dateadd(day,10,date('{run_date}'))
            limit 3
        """
        df = spark.sql(query)
        if df.count() == 0:
            not_met_conditions.append(f"{table_name}")

    # Check data source 2
    for table_name, table_path in pre_check_condition_2.items():
        query = f"""
            select ppn_dt FROM {table_path}
            where ppn_dt > DATE('{run_date}')
            and ppn_dt <= dateadd(day,10,date('{run_date}'))
            limit 3
        """
        df = spark.sql(query)
        if df.count() == 0:
            not_met_conditions.append(f"{table_name}")

    # Conclusion
    if not_met_conditions:
        print(f"Data availability condition(s) not met: {', '.join(not_met_conditions)}. Stopping execution")
        return False
    else:
        print(f"All pre-checks passed")
        return True


if __name__ == "__main__":
    ## CONFIG PATH DATA
    path_curated_domain = dbutils.widgets.get("path_curated_domain")
    set_output_schema = dbutils.widgets.get("set_output_schema")
    set_output_table = dbutils.widgets.get("set_output_table")
    set_start_date = dbutils.widgets.get("set_start_date")
    set_last_date = dbutils.widgets.get("set_last_date")

    # path_curated_domain = "curated_priv.domain_data"
    # set_output_schema = "user_workbench_rmd_priv.rmd_ra_crm"
    # set_output_table = "chihtk3_stg_corp_atxca_balance_daily"
    # set_start_date = "None"
    # set_last_date = "None"

    if set_start_date != "None":
        set_start_date = datetime.datetime.strptime(set_start_date, "%Y-%m-%d").date()
        set_last_date = datetime.datetime.strptime(set_last_date, "%Y-%m-%d").date()
    else:
        set_run_date = datetime.date.today()
        # Start date
        set_start_date = cf.f_first_day_of_previous_month(data_date=set_run_date, nbr_of_mth=1)
        # End Date
        set_last_date = cf.f_last_day_of_previous_month(data_date = set_run_date)  

    print(f"Start date: {set_start_date}")
    print(f"End date: {set_last_date}")

    # precheck    
    if pre_check(set_start_date) and pre_check(set_last_date):
        print("Continue processing...")
    else:
        raise Exception("Pre-check failed. Stopping execution.")

    # process data
    df = fe_corp_asvcd_balance_daily(beg_date=set_start_date, end_date=set_last_date)

    # write to table
    cf.f_data_insert(
        df=df, schema=set_output_schema, table=set_output_table, partition="report_date"
    )