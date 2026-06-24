import dlt
from pyspark.sql.functions import lit

#Streaming Table Using Autoloader
@dlt.table(
    name = "autovol_table"
)
def autovol_table():
    return (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/default/raw")
    )

#Materialized Table

@dlt.table(
    name = "autovol_table_enr"
)
def autovol_table_stg():
    df = (
        spark.read.table("autovol_table")
    )

    return (
        df.withColumn("flag" , lit("Yes"))
    )