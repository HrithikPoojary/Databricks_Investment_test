import dlt

@dlt.view(
    name = "products_gold_view"
)
def products_gold_view():
    return(
        spark.readStream.table("products_silver_view")
    )

dlt.create_streaming_table("products_dim")

dlt.create_auto_cdc_flow(
    target = 'products_dim',
    source = 'products_gold_view',
    keys = ['product_id'],
    sequence_by = 'process_date',
    except_column_list = ['process_date'],
    stored_as_scd_type = 2
)