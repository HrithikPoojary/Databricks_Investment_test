import dlt 
from pyspark.sql.functions import col 

# Source -> Streating Staging
@dlt.table(
    name = "food_src_stg"
)
def food_src_stg():
    return (
        spark.readStream.table("ashlamba.bronze.food_src")
    )


dlt.create_streaming_table(
    name = "food_type1_trg"
)

dlt.create_auto_cdc_flow(
    target = "food_type1_trg",
    source = "food_src_stg",
    keys = ['product_id'],
    sequence_by = col("process_date"),
    stored_as_scd_type = 1
)

dlt.create_streaming_table(
    name = "food_type2_trg"
)

dlt.create_auto_cdc_flow(
    target = "food_type2_trg",
    source = "food_src_stg",
    keys = ['product_id'],
    sequence_by = col("process_date"),
    except_column_list = ["process_date"] # this will skip the modification on this column and not included in the target table to avoid duplicates (old and new records because column is populated as current_timestamp)
    stored_as_scd_type = 2
)