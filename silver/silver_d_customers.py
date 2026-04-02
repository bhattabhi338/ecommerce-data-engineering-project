# Databricks notebook source
dbutils.widgets.text('master_id', 'NA', '')
dbutils.widgets.text('job_exec_id', 'NA', '')
dbutils.widgets.text('job_exec_dtl_id', 'NA', '')
dbutils.widgets.text('skip_notebook', '0', '')

master_id = dbutils.widgets.get('master_id')
job_exec_id = dbutils.widgets.get('job_exec_id')
job_exec_dtl_id = dbutils.widgets.get('job_exec_dtl_id')
skip_notebook = dbutils.widgets.get('skip_notebook')

# COMMAND ----------

if skip_notebook == "1":
    dbutils.notebook.exit("Skipping the Notebook...")

# COMMAND ----------

# MAGIC %run ../infra//functions

# COMMAND ----------

#Global variables
catalog_name = 'ecommerce'
source_layer = 'bronze'
target_layer = 'silver'
source_table = target_table = 'd_customers'
component_name = 'C_bronze_silver'

# COMMAND ----------

table_df = spark.sql(f"select * from {catalog_name}.audit.cntrl_stm_mapping where target_table_name = '{target_table}' and target_layer_name = '{target_layer}'")

display(table_df)

if table_df.isEmpty():
    dbutils.notebook.exit("There are no tables to process. Exiting the notebook...")

# COMMAND ----------

is_processed = table_df.collect()[0]['is_processed_flag']
is_active = table_df.collect()[0]['active_flag']

if(is_processed): 
    dbutils.notebook.exit(f"{catalog_name}.{target_layer}.{target_table} is already processed. Exiting the notebook.")
elif(is_active is False): 
    dbutils.notebook.exit(f"{catalog_name}.{target_layer}.{target_table} table is inactive. Exiting the notebook.")

# COMMAND ----------

try:
    #Fetch last load date time
    last_load_start = table_df.collect()[0]['last_load_start_dt']
    last_load_end = table_df.collect()[0]['last_load_end_dt']
    task_name = f'C_bronze_silver_{target_table}'

    #Open a batch in the batch_exec_dtl table
    open_batch(master_id, job_exec_id, job_exec_dtl_id, task_name, component_name)

    #Fetch required column lists
    pk_list = spark.sql(f"select primary_key from {catalog_name}.audit.primary_key_list where table_name = '{target_table}'").collect()

    primary_keys = [item.primary_key for item in pk_list]
    audit_columns = ['dw_create_dt', 'dw_mdfctn_dt', 'dw_load_id', 'dw_update_id', 'dw_eff_start_dt', 'dw_eff_end_dt']
    surrogate_keys = ['customer_sk']
    all_columns = spark.sql(f"select * from {catalog_name}.{target_layer}.{target_table} where 1=2").columns
    insert_columns = [item for item in all_columns if item not in audit_columns+surrogate_keys]

    #Create dynamic joins and insert conditions
    join_condition = ' and '.join("s."+ item + " = " + "t." + item for item in primary_keys)

    insert_condition = ', '.join("s." + item for item in insert_columns)

    insert_cols = ", ".join(item for item in all_columns if item not in surrogate_keys)

    #Fetch the delta table
    delta_df = spark.sql(f"select * from {catalog_name}.{source_layer}.{source_table} where (create_dt between '{last_load_start}' and '{last_load_end}') and (mdfctn_dt between '{last_load_start}' and '{last_load_end}')")

    if delta_df.isEmpty():
        insert_count = target_count = source_count = update_count = 0
        print(f"There is no Delta for {target_table} in the bronze table.")
    
    else:
        source_count = delta_df.count()

        delta_df.createOrReplaceTempView('temp_source')

        current_time = spark.sql("select current_timestamp()").collect()[0][0]

        #SCD TYPE 2 Implementation

        #Expire the latest record if match found
        expire_records = f"""
        MERGE INTO {catalog_name}.{target_layer}.{target_table} t
        USING temp_source s
        on {join_condition} AND t.dw_eff_end_dt is null
        WHEN MATCHED AND (t.mdfctn_dt <> s.mdfctn_dt)
        THEN UPDATE SET
        t.dw_eff_end_dt = '{last_load_end}',
        t.dw_mdfctn_dt = '{current_time}',
        t.dw_update_id = '{master_id}'
        """
        spark.sql(expire_records)

        #Fetch latest records to be inserted in our silver table
        get_insert_records = spark.sql(f"""select * from temp_source s where not exists (select 1 from {catalog_name}.{target_layer}.{target_table} t where {join_condition} and t.dw_eff_end_dt is null)""")

        #Insert the records to our silver table
        get_insert_records.createOrReplaceTempView('temp_insert')

        current_time = spark.sql("select current_timestamp()").collect()[0][0]

        insert_query = f"""
        INSERT INTO {catalog_name}.{target_layer}.{target_table} ({insert_cols})
            select {insert_condition}, '{current_time}', '{current_time}', '{master_id}', null, '{last_load_end}', null from temp_insert s

        """
        spark.sql(insert_query)

        target_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{target_table} where date(dw_mdfctn_dt) = date('{current_time}')").collect()[0]['cnt']

        insert_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{target_table} where dw_load_id = '{master_id}' ").collect()[0]['cnt']
            
        update_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{target_table} where dw_update_id = '{master_id}' ").collect()[0]['cnt']

    #Close the batch in the batch_exec_dtl
    close_batch(insert_count, update_count, task_name, job_exec_dtl_id)


    #Update cntrl_stm_mapping table
    update_stm = f"""update {catalog_name}.audit.cntrl_stm_mapping set 
    is_processed_flag = True,
    source_count = '{source_count}',
    target_count = '{target_count}',
    dw_mdfctn_dt = current_timestamp(),
    dw_load_id = '{master_id}'
    where target_table_name =  '{target_table}' and
    target_layer_name = '{target_layer}'"""

    spark.sql(update_stm)
    print(f"---'Cntrl_stm_mapping' table updated successfully for table: {target_table}")

except Exception as ex:
    fail_message = str(ex).replace("'", "''")
    fail_batch(task_name, job_exec_dtl_id, fail_message)
    raise ex

# COMMAND ----------

#Incase table remained unprocessed, throw a runtime error
is_unprocessed = spark.sql(f"select is_processed_flag from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and target_table_name = '{target_table}'").collect()[0][0]

if is_unprocessed is False:
    raise RuntimeError(f"'{catalog_name}.{target_layer}.{target_table}' table is unprocessed.") 