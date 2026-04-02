# Databricks notebook source
dbutils.widgets.text("master_id", "NA", "")
dbutils.widgets.text("job_name", "NA", "")

master_id = dbutils.widgets.get("master_id")
job_name = dbutils.widgets.get("job_name")

# COMMAND ----------

from pyspark.sql.functions import *
catalog_name = 'ecommerce'

# COMMAND ----------

fail_count = spark.sql(f"select count(*) as cnt from {catalog_name}.audit.cntrl_stm_mapping where active_flag is True and is_delete_flag is False and is_processed_flag is False").collect()[0]['cnt']

if fail_count > 0:
    status_message = "There are unprocessed tables in the cntrl_stm_mapping table."
else:
    status_message = "There are failed jobs in the batch, Please check the logs."

print(status_message)

# COMMAND ----------

spark.sql(f"""
UPDATE {catalog_name}.audit.cntrl_batch_exec
SET
    exec_status = 'Failed',
    wf_end_datetime = current_timestamp(),
    status_message = '{status_message}'
WHERE master_job_id = '{master_id}'
and job_name = '{job_name}'
""")

raise Exception(f"{status_message}")
