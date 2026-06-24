import dlt
from pyspark.sql.functions import col

@dlt.table(
    name = "sales_stg"
)
def sales_stg():
    df = (
        spark.readStream.option("skipChangeCommits",True)
                .table("ashlamba.silver.sales_enr")
    )
    return df 

@dlt.table(
    name = "sales_enr"
)
def sales_enr():
    df = spark.read.table("sales_stg")
    return (
        df.withColumn(
            "priceAfterDiscount" , col("total_amount") - col("discount")
        )
    )

@dlt.table(
    name = "sales_cur"
)
def sales_cur():
    return(
        spark.read.table("sales_enr")
    )


