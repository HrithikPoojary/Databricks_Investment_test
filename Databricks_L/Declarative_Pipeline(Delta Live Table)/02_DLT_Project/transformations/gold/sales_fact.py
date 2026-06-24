import dlt 
from pyspark.sql.functions import col


@dlt.view(
    name = "sales_gold_view"
)
def sales_glod_view():
    return (
        spark.readStream.table("sales_silver_view")
    )

dlt.create_streaming_table("sales_fact")

dlt.create_auto_cdc_flow(
    target = "sales_fact",
    source = "sales_gold_view",
    keys = ['sales_id'],
    sequence_by = col('process_date'),
    stored_as_scd_type = 1
)