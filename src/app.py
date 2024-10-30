from pyspark.sql import SparkSession
from output.output import load_csv, write_parquet
from transformations.business_logic import join_data
from resources.schemas.schema import customer_schema, order_schema, product_schema, order_item_schema, category_schema

def main():
    # Inicializar SparkSession
    spark = SparkSession.builder \
                .appName("FastAxI_PySpark_CSV") \
                .getOrCreate()
    
    # Definir rutas de los archivos CSV
    customers_path = "src/resources/data/customers.csv"
    orders_path = "src/resources/data/orders.csv"
    products_path = "src/resources/data/products.csv"
    order_items_path = "src/resources/data/order_items.csv"
    
    categories_path = "src/resources/data/categories.csv"
    
    # Cargar datos CSV
    customers = load_csv(spark, customers_path, customer_schema)
    orders = load_csv(spark, orders_path, order_schema)
    products = load_csv(spark, products_path, product_schema)
    order_items = load_csv(spark, order_items_path, order_item_schema)
    categories = load_csv(spark, categories_path, category_schema)
    
    # Realizar transformaciones y joins
    final_df = join_data(customers, orders, products, order_items, categories)
    
    # Definir ruta de salida
    output_path = "resources/salida"
    #/data/master/abtq/data/t_abtq/pepe
    
    # Escribir el DataFrame en formato Parquet con particionamiento por fecha
    write_parquet(final_df, output_path)
    
    # Finalizar SparkSession
    spark.stop()

if __name__ == "__main__":
    main()


