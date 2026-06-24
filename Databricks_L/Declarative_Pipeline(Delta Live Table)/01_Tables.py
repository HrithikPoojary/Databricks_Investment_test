import dlt

#Streaming Table
@dlt.table(
    name = "demo_streaming_table"
)
def demo_streaming_table():
    df = spark.readStream.table("ashlamba.silver.sales_enr")
    return df

#Materialized View
@dlt.table(
    name = "demo_mat_view"
)
def demo_mat_view():
    df = spark.read.table("ashlamba.silver.sales_enr")
    return df

#Temporary View(Batch)
@dlt.view(
    name = 'demo_temp_batch_view'
)
def demo_temp_batch_view():
    df = spark.read.table("ashlamba.silver.sales_enr")
    return df 

#Temporary View(Stream)
@dlt.view(
    name = "demo_temp_stream_view"
)
def demo_temp_stream_view():
    df = spark.readStream.table("ashlamba.silver.sales_enr")
    return df



