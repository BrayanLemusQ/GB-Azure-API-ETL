# Databricks notebook source
print('Notebook execution Started !')
try:
    dbutils.widgets.text("table", "","")
    table = dbutils.widgets.get("table")

    dbutils.widgets.text("lastFileDatetime", "","")
    last_datetime = dbutils.widgets.get("lastFileDatetime")
    print(f'table parameter received succesfully:\t{table}\t{last_datetime}')
except Exception as error:
    msg_exception = (f'Execution error receiving parameters. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

#dbutils.fs.mounts() # List existing mount points

# COMMAND ----------

# dbutils.fs.unmount("/mnt/") unmount mount Points

# COMMAND ----------

try:
    existing_mountPoints = [mountpoint.mountPoint for mountpoint in dbutils.fs.mounts()]
    if "/mnt/" not in existing_mountPoints :
        mount_config = {"fs.azure.account.key.dfgbteststorageaccount.blob.core.windows.net":"N6GAjHrA35c4mcDA1GNjPymi2cRnl6uvFaQmrLdcPiXANgdTF4aI/uTmR8GGJ1AFv2CahJ5PxZ86+AStKaZPGQ=="}
        dbutils.fs.mount(
            source = "wasbs://datacontainer@dfgbteststorageaccount.blob.core.windows.net",
            mount_point = "/mnt/",
            extra_configs = mount_config 
        )
        print('Mount Point created')
    else:
        print('MountPoint already exists')
except Exception as error:
    msg_exception = (f'Execution error creating mount point. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_jobs = StructType([
    StructField('id', IntegerType(), False),
    StructField('job', StringType(), False)
])
schema_departments = StructType([
    StructField('id', IntegerType(), False),
    StructField('department', StringType(), False)
])
schema_employees = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('datetime', StringType(), False),
    StructField('department_id', IntegerType(), False),
    StructField('job_id', IntegerType(), False)
])

tables = ('jobs', 'departments', 'hired_employees')
data_schemas_csv = {
    'jobs' : schema_jobs,
    'departments' : schema_departments,
    'hired_employees' : schema_employees
}
data_schemas_tables = {
    'jobs' : 'id INT, job STRING',
    'departments' : 'id INT, department STRING',
    'hired_employees' : 'id INT, name STRING, datetime STRING, department_id INT, job_id INT'
}
if table not in tables:
    msg_exception = (f'Execution error file with data not allowed. ERROR:\t{str(table)} not in {str(tables)}')
    raise Exception(msg_exception)

# COMMAND ----------

tb_schema = 'silver'
spark.sql(f"create schema if not exists {tb_schema}")
spark.sql(f"use {tb_schema}")
df_tables = spark.sql("show tables")
df_tables.createOrReplaceTempView("df_tables")
df_existing_tables = spark.sql(f"select * from df_tables where upper(tableName) = upper('{table}') and upper(database) = upper('{tb_schema}')")
#df_existing_tables.take(20)

# COMMAND ----------

try:
    source = f"/mnt/bronze/{table}{last_datetime}.csv"
    dest = f"/mnt/silver/{table}"
    if df_existing_tables.count() == 0:
        spark.sql(f"""create table if not exists {tb_schema}.{table} ({data_schemas_tables[table]}) 
                  using delta LOCATION '{dest}'""")
        print(f'Table {table} created')
    else:
        print(f"Table {table} already exists")
except Exception as error:
    msg_exception = (f'Execution error verifying table existance in schema silver. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

try:
    df_new_data = spark.read.format('csv').schema(data_schemas_csv[table]).load(source)
    df_old_data = spark.sql(f'SELECT * FROM silver.{table}')
    diferencias = set(df_new_data.schema.fieldNames()) - set(df_old_data.schema.fieldNames())
    if len(diferencias) != 0:
        raise Exception(f'Execution error reading data different schema. Expected: {df_old_data.schema}\tFound: {df_new_data.schema}')
    df_new_data.createOrReplaceTempView(f'{table}_staging')
    print('Reading data succesfully!')
except Exception as error:
    msg_exception = (f'Execution error reading data {table}. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

def setUpdateQueryString(keys, update_schema, update_table):
    update_fields = spark.sql(f"SELECT * FROM {update_table}_staging LIMIT(1)").columns
    
    str_update_fields = ', '.join([f'dest.{field} = source.{field}' for field in update_fields])
    str_keys = ' AND '.join([f'dest.{key} = source.{key}' for key in keys])
    str_fields = ', '.join([f'{field}' for field in update_fields])
    str_values = ', '.join([f'source.{field}'for field in update_fields])

    update_statement = f"""MERGE INTO {update_schema}.{update_table} AS dest
                        USING {update_table}_staging AS source
                        ON {str_keys}
                        WHEN MATCHED THEN 
                            UPDATE SET {str_update_fields}
                        WHEN NOT MATCHED THEN 
                            INSERT ( {str_fields} ) VALUES ( {str_values} )
                        """
    return update_statement

#setUpdateQueryString(['id'], 'silver', 'jobs')

# COMMAND ----------

try:
    update_merge = setUpdateQueryString(['id'], 'silver', table)
    print(f'Table {table} data updating...')
    spark.sql(update_merge)
    print(f'Table {table} updated!')
except Exception as error:
    msg_exception = (f'Execution error updating table {table}. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

#spark.sql(f'SELECT * FROM silver.jobs').filter('id >= 184').take(5)
