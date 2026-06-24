import dlt
from pyspark.sql.functions import col , current_timestamp , regexp_replace 


@dlt.view(
    name = "stores_silver_view"
)
def stores_silver_view():
    df_store = spark.readStream.table("stores_bronze")

    df_store = df_store.withColumns(
        {
            "store_name" : regexp_replace(col("store_name"),"_",""),
            "process_date" : current_timestamp()
        }
    )
    return df_store

dlt.create_streaming_table("stores_silver")

dlt.create_auto_cdc_flow(
    source = 'stores_silver_view',
    target = 'stores_silver',
    keys = ['store_id'],
    sequence_by = col("process_date"),
    stored_as_scd_type = 1
)