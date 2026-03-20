import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\SuktijVerma\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[1]") \
    .config("spark.driver.host","127.0.0.1") \
    .config("spark.driver.bindAddress","127.0.0.1") \
    .getOrCreate()

data = [("Chair",150),("Table",300)]
df = spark.createDataFrame(data,["Product","Price"])

df.show()
spark.stop()