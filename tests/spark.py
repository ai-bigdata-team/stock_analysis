from pyspark.sql import SparkSession 
import os
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\DELL\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .getOrCreate()
df = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])
df.show()
spark.stop()
