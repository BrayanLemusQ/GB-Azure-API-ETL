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
if table not in tables:
    msg_exception = (f'Execution error file with data not stablished. ERROR:\t{str(table)} not in {str(tables)}')
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
        spark.sql(f"create table if not exists {tb_schema}.{table} using delta location '{dest}'")
        print(f'Table {table} created')
    else:
        print(f"Table {table} already exists")
except Exception as error:
    msg_exception = (f'Execution error loading data to schema bronze. ERROR:\t{str(error)}')
    raise Exception(msg_exception)
