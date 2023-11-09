# Databricks notebook source
print('Notebook execution Started !')
try:
    dbutils.widgets.text("year", "","")
    year = dbutils.widgets.get("year")
    print(f'year parameter received succesfully:\t{year}')
except Exception as error:
    msg_exception = (f'Execution error receiving parameters. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

try:
    spark.sql('USE SILVER')
    df_silver_tables = spark.sql('SHOW TABLES')
    df_silver_tables.createOrReplaceTempView('silver_tables')
    df_default_tables = spark.createDataFrame([(row,) for row in ['jobs', 'hired_employees', 'departments']], ['tableName'])

    df_tables_result = df_default_tables.join(df_silver_tables, on='tableName', how='inner')
    if df_tables_result.count() != 3:
        raise Exception(f'Execution error verifying table existance in schema silver missing table.')
    
except Exception as error:
    msg_exception = (f'Execution error verifying table existance in schema silver. ERROR:\t{str(error)}')
    raise Exception(msg_exception)

# COMMAND ----------

df_total_hired = spark.sql(f"""
                           with employees_avg as (
                               select avg(employees_count) as average
                               from (
                                   select count(*) employees_count from silver.hired_employees
                                   where year(datetime) = {year}
                                   group by department_id
                               )
                           )
                           select he.department_id, dep.department, count(*) as hired
                           from silver.hired_employees he left join silver.departments dep on he.department_id = dep.id 
                           where year(he.datetime) = {year}
                           group by he.department_id, dep.department
                           having count(*) > (select average from employees_avg)
                           order by hired desc
                           """)
#df_total_hired.take(5)

# COMMAND ----------

table = f'TopHiringCompanies_{year}'
dest = f'/mnt/gold/{table}'
schema = "gold"
try:
    df_total_hired.write.format("delta").mode("overwrite").save(dest)
    spark.sql(f"create schema if not exists {schema}")
    spark.sql(f"use {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} using delta location '{dest}'")
    spark.sql(f"REFRESH TABLE {schema}.{table}")
except Exception as error:
    msg_exception = (f'Execution error writing table {table} in gold layer. ERROR:\t{str(error)}')
    raise Exception(msg_exception)
