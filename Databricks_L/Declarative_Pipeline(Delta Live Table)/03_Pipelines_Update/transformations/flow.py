from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,count


@dp.table(
    name = "sales_bronze"
)
def sales_bronze()->DataFrame:
    return(
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/bronze_volume/sales/")
    )


@dp.temparory_view(
    name = 'sales_silver'
)
def sales_silver()->DataFrame:
    return(
            spark.read.table("sales_bronze")
                        .withColumn("sales_per_price" , col("total_amount")/ col("quantity"))
    )

@dp.materialized_view(
    name = "sales_gold"
)
def sales_gold()->DataFrame:
    return (
        spark.read.table("sales_silver")
                    .groupBy(col("customer_id")).agg(count(col("product_id")).alias("total_prodcuct"))
    )









