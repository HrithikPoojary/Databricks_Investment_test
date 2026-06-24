import dlt
from pyspark.sql.functions import upper , col , split , current_timestamp 

@dlt.view(
    name = "customers_silver_view"
)
def customers_silver_view():
    df_custo = spark.readStream.table("customers_bronze")

    df_custo = df_custo.withColumns(
        {
            "name" : upper("name"),
            "domain" : split(col("email"), "@")[1],
            "process_date" : current_timestamp()
        }
    )

    return df_custo

dlt.create_streaming_table("customers_silver")

dlt.create_auto_cdc_flow(
    target = 'customers_silver',
    source = "customers_silver_view",
    keys = ['customer_id'],
    sequence_by = col('process_date'),
    stored_as_scd_type = 1
)
