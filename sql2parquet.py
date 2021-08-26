import sys
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType


def get_spark(jdbc_driver_path):
    return SparkSession.builder.master("local[10]")\
            .config("spark.driver.extraClassPath", jdbc_driver_path)\
            .config("spark.debug.maxToStringFields", 8000)\
            .getOrCreate()


def get_sql_dataframe(host, database_name, table_name, order_by, upperBound, partition_num):
    return spark.read.format("jdbc")\
        .option("url", "jdbc:sqlserver://{host}:1433;databasename={database_name};IntegratedSecurity=true".format(host=host, database_name=database_name))\
        .option("dbtable", "(SELECT ROW_NUMBER() OVER (ORDER BY {order_by}) AS row_num, * FROM {table_name}) as tmp".format(order_by=order_by, table_name=table_name))\
        .option("partitionColumn", "row_num")\
        .option("lowerBound", 0)\
        .option("upperBound", upperBound)\
        .option("numPartitions", partition_num)\
        .load()\
        .drop('row_num')


def get_count(host, database_name, table_name):
    return spark.read.format("jdbc")\
        .option("url", "jdbc:sqlserver://{host}:1433;databasename={database_name};IntegratedSecurity=true".format(host=host, database_name=database_name))\
        .option("dbtable", "(SELECT COUNT(*) as row_count FROM {table_name}) as tmp".format(table_name=table_name))\
        .load()\
        .first()['row_count']


def export(sql_df, output_folder, is_raw):
    for colName in sql_df.columns:
        if is_raw:
            sql_df = sql_df.withColumn(
                colName, col(colName).cast(StringType()))
        sql_df = sql_df.withColumnRenamed(
            colName, colName.lower().replace(' ', '_'))

    sql_df.printSchema()

    sql_df.write.mode("overwrite").parquet(output_folder)


if __name__ == "__main__":
    # Database Configuration
    host = 'NLC1PRODCI01'
    database_name = 'CustomerIntelligence'
    table_name = '[raw].[BigQuery_Salesforce]'
    order_by = 'EtlInsertTime'
    is_raw = False
    jdbc_driver_path = 'C:\sqljdbc\sqljdbc_6.4\enu\mssql-jdbc-6.4.0.jre8.jar'

    # Output
    output_folder = 'D:/Parquets/BigQuery_Salesforce'
    partition_num = 10000

    spark = get_spark(jdbc_driver_path)

    total_count = get_count(host, database_name, table_name)

    print (str(total_count) + " rows will be exported.")

    print ("get sql start..........")
    sql_df = get_sql_dataframe(host, database_name, table_name, order_by, total_count, partition_num)
    print ("get sql end..........")
    
    print ("export start..........")
    export(sql_df, output_folder, is_raw)
    print ("export end..........")