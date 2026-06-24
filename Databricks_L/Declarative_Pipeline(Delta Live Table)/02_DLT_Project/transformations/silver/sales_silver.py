import dlt
from pyspark.sql.functions import col,round, current_timestamp 


#STREAMING VIEW
@dlt.view(
    name = "sales_silver_view"
)
def sales_silver_view():
    
    df_sales = spark.readStream.table("sales_bronze")

    df_sales = df_sales.withColumns(
        {
            "price_per_sale" : round(col("total_amount")/col("quantity"),2),
            "process_date" : current_timestamp()
        }
    )

    return df_sales

#STREAMING SILVER TABLE
dlt.create_streaming_table("sales_silver")

dlt.create_auto_cdc_flow(
    source = 'sales_silver_view',
    target = 'sales_silver',
    keys = ['sales_id'],
    sequence_by = col('process_date'),
    stored_as_scd_type = 1
)





