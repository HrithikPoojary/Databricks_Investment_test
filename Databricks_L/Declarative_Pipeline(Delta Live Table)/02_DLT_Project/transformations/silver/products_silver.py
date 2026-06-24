import dlt
from pyspark.sql.functions import current_timestamp , col 

@dlt.view(
    name = "products_silver_view"
)
def products_bronze():
    df_prod = spark.readStream.table("products_bronze")

    df_prod = df_prod.withColumn("process_date" , current_timestamp())

    return df_prod


dlt.create_streaming_table("products_silver")

dlt.create_auto_cdc_flow(
    source = 'products_silver_view',
    target = 'products_silver',
    keys = ['product_id'],
    sequence_by = col("process_date"),
    stored_as_scd_type = 1
)