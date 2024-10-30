from pyspark.sql import SparkSession

def load_csv(spark, path, schema):
    return spark.read.format("csv").option("header", "true").schema(schema).load(path)

def write_parquet(df, output_path):
    df.write.mode("overwrite").partitionBy("date").parquet(output_path)
