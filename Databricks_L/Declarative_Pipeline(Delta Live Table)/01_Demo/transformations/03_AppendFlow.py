import dlt

#Creating Empty Streaming Table
dlt.create_streaming_table(
    "append_table"
    #,schema= 'id int, name string'
    )

#Flow1

@dlt.append_flow(
    target = "append_table"
)
def flow1():
    return (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/default/raw/Flow1/")
    )

#Flow2
@dlt.append_flow(
    target = "append_table"
)
def flow2():
    return(
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/job_volume/Flow2/")
    )