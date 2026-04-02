# Databricks notebook source
dbutils.widgets.text('master_id', 'NA', '')
dbutils.widgets.text('job_exec_id', 'NA', '')
dbutils.widgets.text('job_exec_dtl_id', 'NA', '')
dbutils.widgets.text('skip_notebook', '0', '')
dbutils.widgets.text('file_path', '/Volumes/ecommerce/file_storage_schema/storage_volume/', '')

master_id = dbutils.widgets.get('master_id')
job_exec_id = dbutils.widgets.get('job_exec_id')
job_exec_dtl_id = dbutils.widgets.get('job_exec_dtl_id')
skip_notebook = dbutils.widgets.get('skip_notebook')
file_path = dbutils.widgets.get('file_path')

# COMMAND ----------

if skip_notebook == '1':
    dbutils.notebook.exit("Skipping the notebook...")

# COMMAND ----------

# MAGIC %run ../infra//functions

# COMMAND ----------

#Global variables
target_layer = 'landing'
catalog_name = 'ecommerce'
component_name = "C_source_landing"

# COMMAND ----------

#List all tables to be processed from cntrl_stm_mapping table
tables_df = spark.sql(f"""select
b.table_id as bronze_table_id,
b.target_table_name as bronze_table_name,
a.*
from {catalog_name}.audit.cntrl_stm_mapping a
join {catalog_name}.audit.cntrl_stm_mapping b
on a.target_table_name = b.source_table_name
where a.target_layer_name = '{target_layer}'
and a.is_delete_flag is false 
and a.active_flag is True 
and a.is_processed_flag is false""")


#Handle when all tables are already processed.
if tables_df.isEmpty():
  dbutils.notebook.exit("All the tables are already processed. Exiting the notebook...")


# COMMAND ----------

# DBTITLE 1,Cell 6
#Loop through each table and start processing
for row in tables_df.collect():
  try:

    file_name = row.source_table_name
    last_load_start = row.last_load_start_dt
    last_load_end = row.last_load_end_dt
    bronze_table_id = row.bronze_table_id
    task_name = f"T_source_landing_{file_name}"

    #Open a batch in the batch_exec_dtl table
    open_batch(master_id, job_exec_id, job_exec_dtl_id, task_name, component_name)

  
    last_load_end_date = spark.sql(f"select to_date('{last_load_end}') as date_type").collect()[0][0]
    destination = f'{file_path}landing_files/{file_name}/{last_load_end_date}'

    #Read the csv and use CDC column to capture Delta
    delta_table = (
        spark.read.format('csv')
        .option('header', True)
        .load(f'{file_path}source_files/{file_name}/{file_name}.csv')
        .filter(
            (
                col('create_dt').between(last_load_start, last_load_end)
            ) |
            (
                col('mdfctn_dt').between(last_load_start, last_load_end)
            )
        )
    )

    #If delta is empty, skip the table processing.
    if delta_table.isEmpty():
      print(f"There is no new data for the file: {file_name}, therefore skipping...")

    else:
      print(f"Processing the file: {file_name}")

      #Write as a parquet file
      delta_table.write.mode('overwrite').parquet(destination)

      #Update landing_file_list table
      spark.sql(f"""update {catalog_name}.audit.landing_file_list 
                set file_path = '{destination}',
                delta_load_dt = '{last_load_end}',
                file_create_dt = current_timestamp(),
                is_processed_flag = False,
                dw_load_id = {master_id}
                where table_id = '{bronze_table_id}'
                """)
      print(f"------------------------'landing_file_list' table updated successfully for the file: {file_name}")
    
    #Close the batch in the batch_exec_dtl table
    spark.sql(f"""UPDATE {catalog_name}.audit.cntrl_batch_exec_dtl
              SET exec_status = "Success",
              status_message = "Success",
              end_datetime = current_timestamp()
              where task_name = '{task_name}'
              and job_exec_dtl_id = {job_exec_dtl_id}
              """)
  
    #Update cntrl_stm_mapping and mark the table as processed
    spark.sql(f"""UPDATE {catalog_name}.audit.cntrl_stm_mapping 
                  SET is_processed_flag = True, 
                      dw_mdfctn_dt = current_timestamp() 
                  WHERE source_table_name = '{file_name}' and target_layer_name = '{target_layer}'""")
    print(f"------------------------'Cntrl_stm_mapping' table updated successfully for table: {file_name}")


  except Exception as ex:
    fail_message = str(ex).replace("'", "''")
    fail_batch(task_name, job_exec_dtl_id, fail_message)
    raise ex

# COMMAND ----------

#Incase any table remain unprocessed, raise a runtime error.
processed_count = spark.sql(f"""select count(*) from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and is_processed_flag is false""").collect()[0][0]

if processed_count > 0:
    df = spark.sql(f"select source_table_name from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and is_processed_flag is false").collect()
                   
    df = ','.join([row.source_table_name for row in df])

    raise RuntimeError(f"Following are the unprocessed tables: {df}")
