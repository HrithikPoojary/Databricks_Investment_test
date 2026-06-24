import dlt
from pyspark.sql.functions import col 

@dlt.view(
    name = "customers_gold_view"
)
def customers_gold_view():
    return(
        spark.readStream.table("customers_silver_view")
    )

dlt.create_streaming_table("customers_dim")

dlt.create_auto_cdc_flow(
    target = 'customers_dim',
    source = 'customers_gold_view',
    keys = ['customer_id'],
    sequence_by = col("process_date"),
    except_column_list = ['process_date'],
    stored_as_scd_type = 2
)