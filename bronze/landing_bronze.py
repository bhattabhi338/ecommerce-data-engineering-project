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

if skip_notebook == '1':
    dbutils.notebook.exit("Exiting the notebook...")

# COMMAND ----------

# MAGIC %run ../infra//functions

# COMMAND ----------

#Global variable
target_layer = 'bronze'
catalog_name = 'ecommerce'
component_name = 'C_landing_bronze'

# COMMAND ----------

#Pull all the tables to be processed
tables_df = spark.sql(f"""select * from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and is_processed_flag is false and active_flag is true""")

display(tables_df)

if tables_df.isEmpty():
    dbutils.notebook.exit("There are no tables to be processed. Exiting Notebook...")


# COMMAND ----------

for row in tables_df.collect():
    try:
        table_id = row.table_id
        table_name = row.target_table_name
        last_load_start = row.last_load_start_dt
        last_load_end = row.last_load_end_dt
        task_name = f'C_landing_bronze_{table_name}'

        #Open a batch in the batch_exec_dtl table
        open_batch(master_id, job_exec_id, job_exec_dtl_id, task_name, component_name)

        # Get list of primary keys
        get_pk = spark.sql(
            f"""select primary_key from {catalog_name}.audit.primary_key_list where table_name = '{table_name}'"""
        ).collect()
        pk_list = [item.primary_key for item in get_pk]

        # Declare audit columns list
        audit_columns = [
            'dw_create_dt',
            'dw_mdfctn_dt',
            'dw_load_id',
            'dw_update_id'
        ]

        # Fetch all the table columns
        all_col_list = spark.sql(
            f"""select * from {catalog_name}.{target_layer}.{table_name} where 1=2"""
        ).columns

        # Create a list of columns to be updated by excluding primary key and audit columns
        update_columns = [
            item for item in all_col_list if item not in pk_list + audit_columns
        ]

        # Create a list of columns to be inserted excluding audit columns
        insert_columns = [
            item for item in all_col_list if item not in audit_columns
        ]

        # Create dynamic join, update and insert conditions
        join_condition = " and ".join(
            "t." + item + "=" + "s." + item for item in pk_list
        )
        update_condition = ", ".join(
            "t." + item + "=" + "s." + item for item in update_columns
        )
        insert_condition = ",".join(
            "s." + item for item in insert_columns
        )
        insert_cols = ", ".join(
            item for item in insert_columns + audit_columns
        )

        # Fetch the landing file location from the landing_file_list table
        file_df = spark.sql(
            f"""select * from {catalog_name}.audit.landing_file_list where table_id = '{table_id}' and delta_load_dt = '{last_load_end}' and is_processed_flag is false"""
        ).collect()

        if not file_df:
            insert_count = target_count = source_count = update_count = 0
            print(f"There is no Delta for {table_name} in the landing_file_list table.")
            
        else:
            print(f"Processing data for the table {table_name}.")
            
            file_path = file_df[0].file_path

            # Read the landing file as a spark dataframe
            spark_df = spark.read.format('parquet').load(file_path)

            source_count = spark_df.count()

            # Write the landing_file
            spark_df.createOrReplaceTempView('temp_source')

            current_time = spark.sql(
                "select current_timestamp() as dt"
            ).collect()[0][0]

            merge_query = f"""MERGE INTO {catalog_name}.{target_layer}.{table_name} t
                    USING temp_source s
                    on {join_condition}
                    WHEN MATCHED THEN UPDATE SET 
                        {update_condition},
                        t.dw_mdfctn_dt = '{current_time}',
                        t.dw_update_id = '{master_id}'
                    WHEN NOT MATCHED THEN INSERT({insert_cols}) VALUES(
                        {insert_condition},
                        '{current_time}',
                        '{current_time}',
                        '{master_id}',
                        null
                        )
                    """
            spark.sql(merge_query)
            target_count = spark.sql(
                f"""select count(*) as cnt from {catalog_name}.{target_layer}.{table_name} where date(dw_mdfctn_dt) = date('{current_time}')"""
            ).collect()[0]['cnt']

            insert_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{table_name} where dw_load_id = '{master_id}' ").collect()[0]['cnt']
            
            update_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{table_name} where dw_update_id = '{master_id}' ").collect()[0]['cnt']

            print(f"{table_name} - Insert_Count: {insert_count}, Update_Count: {update_count}")

        print("trying the close the batch")
        #Close the batch in the batch_exec_dtl table
        close_batch(insert_count, update_count, task_name, job_exec_dtl_id)

        print("batch is closed")

        # Update cntrl_stm_mapping table
        update_stm = f"""update {catalog_name}.audit.cntrl_stm_mapping set 
        is_processed_flag = True,
        source_count = '{source_count}',
        target_count = '{target_count}',
        dw_mdfctn_dt = current_timestamp()
        where target_table_name =  '{table_name}'
        and target_layer_name = '{target_layer}'
        """

        # Update landing_file_list table
        spark.sql(f"""
                  update {catalog_name}.audit.landing_file_list
                  set is_processed_flag = True
                  where table_name = '{table_name}'
                  """)
        print(f"---'landing_file_list' table updated successfully for table: {table_name}")

        spark.sql(update_stm)
        print(f"---'Cntrl_stm_mapping' table updated successfully for table: {table_name}")

    except Exception as ex:
        fail_message = str(ex).replace("'", "''")
        fail_batch(task_name, job_exec_dtl_id, fail_message)
        raise ex

# COMMAND ----------

#Incase a table remained unprocessed throw a runtime error:
unprocessed_table = spark.sql(f"select count(*) as cnt from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and is_processed_flag = False").collect()[0]['cnt']

if unprocessed_table > 0:
    table_list = spark.sql(f"select target_table_name from {catalog_name}.audit.cntrl_stm_mapping where target_layer_name = '{target_layer}' and is_processed_flag = False").collect()

    raise RuntimeError(f"Following are the Un-Processed tables: f{[item.target_table_name for item in table_list]}")