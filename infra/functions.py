# Databricks notebook source
from pyspark.sql.functions import *
from datetime import *

catalog_name = 'ecommerce'

# COMMAND ----------

#Open a batch in the batch_exec_dtl table
def open_batch(master_id, job_exec_id, job_exec_dtl_id, task_name, component_name):
    spark.sql(f"""INSERT INTO {catalog_name}.audit.cntrl_batch_exec_dtl(master_job_id, job_exec_id, job_exec_dtl_id, task_name, component_name, exec_status, status_message, insert_count, update_count, start_datetime, end_datetime) VALUES(
    '{master_id}',
    '{job_exec_id}',
    '{job_exec_dtl_id}',
    '{task_name}',
    '{component_name}',
    'Started',
    'In Progress',
    0,
    0,
    current_timestamp(),
    null)
    """)

#Close the batch in the batch_exec_dtl table
def close_batch(insert_count, update_count, task_name, job_exec_dtl_id):
    spark.sql(f"""UPDATE {catalog_name}.audit.cntrl_batch_exec_dtl
    SET exec_status = 'Success',
    status_message = 'Success',
    insert_count = {insert_count},
    update_count = {update_count},
    end_datetime = current_timestamp()
    where task_name = '{task_name}'
    and job_exec_dtl_id = '{job_exec_dtl_id}'
    """)

#Fail the batch in the batch_exec_dtl table
def fail_batch(task_name, job_exec_dtl_id, fail_message):
    spark.sql(f"""UPDATE {catalog_name}.audit.cntrl_batch_exec_dtl
    SET exec_status = 'Failed',
    status_message = '{fail_message}',
    insert_count = 0,
    update_count = 0,
    end_datetime = current_timestamp()
    where task_name = '{task_name}'
    and job_exec_dtl_id = '{job_exec_dtl_id}'
    """)