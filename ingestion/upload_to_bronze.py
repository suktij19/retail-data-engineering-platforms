from azure.storage.filedatalake import DataLakeServiceClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="../.env")

# Credentials (from .env)
account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
account_key = os.getenv("AZURE_STORAGE_KEY")
file_system_name = os.getenv("AZURE_CONTAINER")  # bronze

# File details
directory_name = "sales"
file_name = "retail_sales.csv"

# Connect
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

file_system_client = service_client.get_file_system_client(file_system=file_system_name)

# Create directory if not exists
directory_client = file_system_client.get_directory_client(directory_name)
try:
    directory_client.create_directory()
except Exception:
    pass

# Create file
file_client = directory_client.get_file_client(file_name)

# Read local file
with open("../data/bronze/retail_sales.csv", "rb") as f:
    data = f.read()

# Upload
file_client.upload_data(data, overwrite=True)

print("File uploaded to Bronze layer successfully")