import dlt
from pyspark.sql.types import StructType , StructField , IntegerType , StringType ,DoubleType , TimestampType


table_name = spark.conf.get("table_name")

expectations = {
    "product_id_cannot_null" : "product_id is not null",
    "category_cannot_null" : "category is not null"
}

schema = StructType(
    [
        StructField("product_id" , IntegerType()),
        StructField("product_name" , StringType()),
        StructField("category", StringType()),
        StructField("price" , DoubleType()),
        StructField("process_date" , TimestampType())
    ]
)

@dlt.table(
    name = "product_data_quality",
    schema = schema
)
@dlt.expect_all_or_drop(expectations) #expect_all_or_warn(default),expect_all_or_fail
def product_data_quality():
    return (
        spark.read.table(f"ashlamba.silver.{table_name}")
    )

