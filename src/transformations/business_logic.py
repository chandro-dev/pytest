from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date

def join_data(customers: DataFrame, orders: DataFrame, products: DataFrame, order_items: DataFrame, categories: DataFrame) -> DataFrame:
    # Join orders with customers
    df = orders.join(customers, "customer_id", "inner")
    
    # Join order_items with orders
    df = df.join(order_items, "order_id", "inner")
    
    # Join products with order_items
    df = df.join(products, "product_id", "inner")
    
    # Join categories with products
    df = df.join(categories, "category_id", "inner")
    
    # Add current date as partition column
    return df.withColumn("date", current_date())
