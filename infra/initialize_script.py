# Databricks notebook source
dbutils.widgets.text('master_id', 'NA', '')
dbutils.widgets.text('job_name', 'NA', '')
dbutils.widgets.text('wfl_start_dt', 'NA', '')
dbutils.widgets.text('wfl_end_dt', 'NA', '')
dbutils.widgets.text('target_layer_list', 'NA', '')

master_id = dbutils.widgets.get('master_id')
job_name = dbutils.widgets.get('job_name')
wfl_start_dt = dbutils.widgets.get('wfl_start_dt')
wfl_end_dt = dbutils.widgets.get('wfl_end_dt')
target_layer_list = dbutils.widgets.get('target_layer_list')

# COMMAND ----------

from pyspark.sql.functions import *

catalog_name = 'ecommerce'

#Convert the parameter values to comma separated values.
target_layer_list = ', '.join(f'"{x.strip()}"' for x in target_layer_list.split(',') if x.strip())

print(target_layer_list)

# COMMAND ----------

#Check the Job status for the last execution
last_exec = spark.sql(f"select exec_status, data_load_end_dt as end_dt from {catalog_name}.audit.cntrl_batch_exec where job_name = '{job_name}' order by wf_end_datetime desc limit 1")

if last_exec.count() == 0:
    print("No Previous run found. Continuing with current iteration...")
else:
    if last_exec.collect()[0]['exec_status'] == "Failed":
        raise Exception(f"The Last execution of the Job: '{job_name}' failed. Please check the logs for more details.")

# COMMAND ----------

data_cutt_off_date = spark.sql("select date_add(SECOND, -1, to_date(current_timestamp())) as dt").collect()[0][0]

#Update cntrl_stm_mapping table
update_query = f"""
          UPDATE {catalog_name}.audit.cntrl_stm_mapping
          SET last_load_start_dt = (
              CASE 
                WHEN '{wfl_start_dt}' != 'NA' THEN to_timestamp('{wfl_start_dt}')
                ELSE last_load_end_dt
              END
          ),
          last_load_end_dt = (
              CASE
                WHEN '{wfl_end_dt}' != 'NA' THEN to_timestamp('{wfl_end_dt}')
                ELSE '{data_cutt_off_date}'
              END
          ),
          is_processed_flag = False,
          dw_mdfctn_dt = current_timestamp(),
          dw_load_id = '{master_id}'
          WHERE 
          target_layer_name in ({target_layer_list})
          and active_flag is TRUE
          and is_delete_flag is FALSE
          """

print(update_query)
spark.sql(update_query)

# COMMAND ----------

data_cutt_off_date = spark.sql("select date_add(SECOND, -1, to_date(current_timestamp())) as dt").collect()[0][0]

display(data_cutt_off_date)