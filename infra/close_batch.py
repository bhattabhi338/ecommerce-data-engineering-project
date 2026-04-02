# Databricks notebook source
dbutils.widgets.text("master_id", "NA", "")
dbutils.widgets.text("job_exec_id", "NA", "")
dbutils.widgets.text("master_batch", "NA", "")
dbutils.widgets.text("target_layer_list", "NA", "")


master_id = dbutils.widgets.get("master_id")
job_exec_id = dbutils.widgets.get("job_exec_id")
master_batch = dbutils.widgets.get("master_batch")
target_layer_list =  dbutils.widgets.get("target_layer_list")

# COMMAND ----------

from pyspark.sql.functions import *

catalog_name = 'ecommerce'

#Convert the parameter values to comma separated values.
target_layer_list = ', '.join(f'"{x.strip()}"' for x in target_layer_list.split(',') if x.strip())

print(target_layer_list)

# COMMAND ----------

#Check if the run is triggered by Master batch or Sub workflow
if master_batch == "Y":
    #Check if there are any un-processed tables after the batch run.
    fail_count = spark.sql(f"select target_table_name from {catalog_name}.audit.cntrl_stm_mapping where is_processed_flag is false and active_flag is True and is_delete_flag is False")

else:
    fail_count = spark.sql(f"select target_table_name from {catalog_name}.audit.cntrl_stm_mapping where is_processed_flag is False and active_flag is True and is_delete_flag is False and target_layer_name in ({target_layer_list})")


#Raise exception if there are any unprocessed tables.
if fail_count.count() > 0:
    unprocessed_tables = ", ".join(item['target_table_name'] for item in fail_count.collect())

    #Update the batch with failed status
    spark.sql(f"""
    UPDATE {catalog_name}.audit.cntrl_batch_exec
    SET
    exec_status = "Failed",
    wf_end_datetime = current_timestamp(),
    status_message = "Listed tables remained unprocessed: '{unprocessed_tables}'"
    WHERE master_job_id = '{master_id}'
    and job_exec_id = '{job_exec_id}'
    """)

    print("Batch Failed")

    #Raise an exception
    raise Exception(f"The following tables are unprocessed: {unprocessed_tables}")

else:
    #Get data load dates from cntrl_stm_mapping table
    date_df = spark.sql(f"""
    SELECT 
        MAX(last_load_start_dt) AS load_start, 
        MAX(last_load_end_dt) AS load_end 
    FROM {catalog_name}.audit.cntrl_stm_mapping 
    WHERE is_processed_flag = True 
      AND active_flag = True 
      AND is_delete_flag = False
    """).collect()[0]

    load_start = date_df['load_start']
    load_end = date_df['load_end']

    #Update the Successful batch status
    spark.sql(f"""
    UPDATE {catalog_name}.audit.cntrl_batch_exec
    SET
    exec_status = "Success",
    wf_end_datetime = current_timestamp(),
    status_message = "Workflow completed successfully.",
    data_load_start_dt = '{load_start}',
    data_load_end_dt = '{load_end}'
    WHERE master_job_id = '{master_id}'
    and job_exec_id = '{job_exec_id}'
    """)

    print("Batch Success")