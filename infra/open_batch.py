# Databricks notebook source
dbutils.widgets.text("master_id", "NA", "")
dbutils.widgets.text("job_exec_id", "NA", "")
dbutils.widgets.text("job_name", "NA", "")
dbutils.widgets.text("wfl_start_dt", "NA", "")

master_id = dbutils.widgets.get("master_id")
job_exec_id = dbutils.widgets.get("job_exec_id")
job_name = dbutils.widgets.get("job_name")
wfl_start_dt = dbutils.widgets.get("wfl_start_dt")

# COMMAND ----------

from pyspark.sql.functions import *

catalog_name = 'ecommerce'

if wfl_start_dt == "NA":
  wfl_start_dt = spark.sql('select current_timestamp() as dt').collect()[0][0]


# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog_name}.audit.cntrl_batch_exec(
  master_job_id,
  job_exec_id,
  job_name,
  exec_status,
  wf_start_datetime,
  wf_end_datetime,
  status_message,
  data_load_start_dt,
  data_load_end_dt
)
VALUES (
  '{master_id}',
  '{job_exec_id}',
  '{job_name}',
  'Started',
  '{wfl_start_dt}',
  null,
  'In Progress',
  null,
  null
)
""")

print("Batch Opened Successfully.")
