from azure.storage.filedatalake import DataLakeServiceClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="../.env")

# Get credentials from .env
account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
account_key = os.getenv("AZURE_STORAGE_KEY")

# Connect to ADLS
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

print("Connected to Azure Data Lake successfully")