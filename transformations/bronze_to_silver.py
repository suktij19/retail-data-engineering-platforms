from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.functions import col

# Load env
load_dotenv()

account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
account_key = os.getenv("AZURE_STORAGE_KEY")

if not account_name or not account_key:
    raise ValueError("Azure credentials missing. Check .env")

# Spark session with Azure support
spark = SparkSession.builder \
    .appName("Bronze to Silver Transformations") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Azure config
spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key
)

# Paths
file_path = f"abfss://bronze@{account_name}.dfs.core.windows.net/sales/retail_sales.csv"
output_path = f"abfss://silver@{account_name}.dfs.core.windows.net/sales_clean/"

print("Reading from:", file_path)

# Read
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Initial Row Count:", df.count())

# Cleaning
df = df.dropDuplicates()

# safer than dropna()
df = df.filter(col("Quantity").isNotNull() & col("UnitPrice").isNotNull())

df = df.withColumn("Quantity", col("Quantity").cast("int"))
df = df.withColumn("UnitPrice", col("UnitPrice").cast("double"))

df = df.filter(col("Quantity") > 0)

df = df.withColumn("total_amount", col("Quantity") * col("UnitPrice"))

print("Final Row Count:", df.count())

# Write
df.write.mode("overwrite").parquet(output_path)

print("Data successfully written to Silver layer")