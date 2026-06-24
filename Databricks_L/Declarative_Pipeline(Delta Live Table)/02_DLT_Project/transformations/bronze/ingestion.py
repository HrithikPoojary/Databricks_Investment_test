import dlt

#CREATE STREAMING TABLE SALES
@dlt.table(
    name = 'sales_bronze'
)
def sales_bronze():
    return(
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/bronze_volume/sales/")
    )

#CREATE STREAMING TABLE STORES
@dlt.table(
    name = "stores_bronze"
)
def stores_bronze():
    return (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/bronze_volume/stores/")
    )

#CREATE STREAMING TABLE PRODUCTS
@dlt.table(
    name = "products_bronze"
)
def products_bronze():
    return (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/bronze_volume/products/")
    )

#CREATE STREAMING TABLE CUSTOMERS
@dlt.table(
    name = "customers_bronze"
)
def customers_bronze():
    return (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format" , "csv")
                        .load("/Volumes/ashlamba/bronze/bronze_volume/customers/")
    )













