from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# Schemas para los archivos CSV
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("country", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True)
])

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category_id", IntegerType(), True)
])

order_item_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True)
])

category_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("category_name", StringType(), True)
])
