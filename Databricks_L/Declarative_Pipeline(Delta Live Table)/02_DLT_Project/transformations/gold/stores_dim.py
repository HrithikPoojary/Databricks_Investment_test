import dlt
from pyspark.sql.functions import col 

@dlt.view(
    name = 'stores_gold_view'
)
def stores_gold_view():
    return (
        spark.readStream.table("stores_silver_view")
    )

dlt.create_streaming_table("stores_dim")

dlt.create_auto_cdc_flow(
    target = 'stores_dim',
    source = 'stores_gold_view',
    keys = ['store_id'],
    sequence_by = col("process_date"),
    except_column_list = ['process_date'],
    stored_as_scd_type = 2
)