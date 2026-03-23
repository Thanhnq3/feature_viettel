# config_path.py

path_curated_domain = "curated_priv.domain_data"
path_rmd_fs = "user_workbench_rmd_priv.division_rmd"
path_rmd_stg = "user_workbench_rmd_priv.rmd_ra_crm"
path_stg_corp_atxca = "chihtk3_stg_corp_atxca_balance_daily" 
path_stg_corp_asvtd = "chihtk3_stg_corp_asvtd_balance_daily"
path_stg_corp_asvcd = "chihtk3_stg_corp_asvcd_balance_daily"
path_ft_corp_atxca = "chihtk3_ft_corp_atxca_balance"
path_ft_corp_asv = "chihtk3_ft_corp_asv_balance"
path_ft_corp_a = "chihtk3_ft_corp_a_balance"
#set_start_date = "2024-12-31"
#set_last_date = "2025-10-31"
# set_run_date = "2025-11-01"
set_start_date = None
set_last_date = None
set_run_date = None

# staging table must have prefix chihtk3_ due to corresponding table in user_workbench_rmd_priv.rmd_ra_crm has Khanhnq7 as owner and 
# can not be modified as he retired without changing table manage / owner permission