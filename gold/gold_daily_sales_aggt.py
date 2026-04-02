# Databricks notebook source
dbutils.widgets.text('master_id', 'NA', '')
dbutils.widgets.text('job_exec_id', 'NA', '')
dbutils.widgets.text('job_exec_dtl_id', 'NA', '')
dbutils.widgets.text('skip_notebook', '0', '')
dbutils.widgets.text("l_year", '0')
dbutils.widgets.text("l_month", '0')
dbutils.widgets.text("l_day", '0')

master_id = dbutils.widgets.get('master_id')
job_exec_id = dbutils.widgets.get('job_exec_id')
job_exec_dtl_id = dbutils.widgets.get('job_exec_dtl_id')
skip_notebook = dbutils.widgets.get('skip_notebook')
l_year = dbutils.widgets.get("l_year")
l_month = dbutils.widgets.get("l_month")
l_day = dbutils.widgets.get("l_day")

# COMMAND ----------

if skip_notebook == "1":
    dbutils.notebook.exit("Skipping the Notebook...")

# COMMAND ----------

# MAGIC %run ../infra//functions

# COMMAND ----------

#Global variables
catalog_name = 'ecommerce'
target_layer = 'gold'
target_table = 'daily_sales_aggt'
component_name = 'C_silver_gold'

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

last_load_start = None
last_load_end = None
try:
    if l_year == "0" or l_month == "0" or l_day == "0":
        last_load_start = table_df.collect()[0]['last_load_start_dt']
        last_load_end = table_df.collect()[0]['last_load_end_dt']

    else:
        y = int(l_year)
        m = int(l_month)
        d = int(l_day)
        last_load_start = datetime(y, m, d) - timedelta(seconds=1)
        last_load_end = datetime(y, m, d) + timedelta(days=1) - timedelta(seconds=1)
    
    print(last_load_start)
    print(last_load_end)

except Exception as ex:
    print(ex)

# COMMAND ----------

try:
    task_name = f'C_silver_gold_{target_table}'

    #Open a batch in the batch_exec_dtl table
    open_batch(master_id, job_exec_id, job_exec_dtl_id, task_name, component_name)

    audit_columns = ['dw_create_dt', 'dw_mdfctn_dt', 'dw_load_id', 'dw_update_id']
    all_columns = spark.sql(f"select * from {catalog_name}.{target_layer}.{target_table} where 1=2").columns
    insert_columns = [item for item in all_columns if item not in audit_columns]

    #Create dynamic conditions
    insert_condition = ', '.join("s." + item for item in insert_columns)

    insert_cols = ", ".join(item for item in all_columns)

    #Fetch the affected order_date(s)
    get_impacted_dates = spark.sql(f"""
    select distinct(o.order_date) as o_dates
    from {catalog_name}.silver.f_order_items oi
    inner join {catalog_name}.silver.f_orders o on oi.order_id = o.order_id
    where (date(oi.create_dt) between '{last_load_start}' and '{last_load_end}') OR (date(oi.mdfctn_dt) between '{last_load_start}' and '{last_load_end}') or (date(o.mdfctn_dt) between '{last_load_start}' and '{last_load_end}')
    """).collect()

    impacted_dates_list = [str(row.o_dates) for row in get_impacted_dates]

    if not impacted_dates_list:
        source_count = target_count = insert_count = 0

        print("There is no new data to process. Closing the batch...")
    
    else:

        impacted_dates = ",".join("'" + item + "'" for item in impacted_dates_list)

        # Compute the affected partitions
        delta_df = spark.sql(f"""
        select
        o.order_date as order_date,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as total_customers,
        round(sum((oi.quantity * oi.unit_price) - oi.discount), 2) as total_revenue,
        sum(quantity) as total_items_sold,
        round(SUM((oi.quantity * oi.unit_price) - oi.discount) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
        from {catalog_name}.silver.f_order_items oi
        inner join {catalog_name}.silver.f_orders o on oi.order_id = o.order_id
        where o.order_date in ({impacted_dates})
        group by o.order_date
    """)
        
        source_count = delta_df.count()

        delta_df.createOrReplaceTempView('temp_source')

        #Delete the existing affected parition
        spark.sql(f"DELETE FROM {catalog_name}.{target_layer}.{target_table} where order_date in ({impacted_dates})")


        #Insert the new partitions
        current_time = spark.sql("select current_timestamp()").collect()[0][0]

        spark.sql(f"""
            INSERT INTO {catalog_name}.{target_layer}.{target_table} ({insert_cols})
            select
            {insert_condition},
            '{current_time}',
            '{current_time}',
            '{master_id}',
            null
            from temp_source s"""
        )

        target_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{target_table} where date(dw_mdfctn_dt) = date('{current_time}')").collect()[0]['cnt']

        insert_count = spark.sql(f"select count(*) as cnt from {catalog_name}.{target_layer}.{target_table} where dw_load_id = '{master_id}' ").collect()[0]['cnt']

    #Close the batch in the batch_exec_dtl
    close_batch(insert_count, 0, task_name, job_exec_dtl_id)

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