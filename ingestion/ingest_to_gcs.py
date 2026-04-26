import pandas as pd
from google.cloud import storage
import io
from datetime import datetime

RAW_DATA_URL = "https://raw.githubusercontent.com/safacanmetin/skyline-medallion/refs/heads/main/xxx.csv"
BUCKET_NAME = "skyline-landing-zone-marmaris"
#KEY_PATH = "gcp-keys.json" //local json files are not secure anymore

def ingest():
    # 1. fetch data from API
    print("Data is being fetched from source...")
    df = pd.read_csv(RAW_DATA_URL)
    
    # 2. add metadata
    df['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 3. GCP connection
    #client = storage.Client.from_service_account_json(KEY_PATH)
    client = storage.Client(project='project-xxx')
    bucket = client.get_bucket(BUCKET_NAME)
    
    # 4. load as Parquet, faster in cloud envs
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    
    blob = bucket.blob(f"bronze/bank_data_{datetime.now().strftime('%Y%m%d')}.parquet")
    blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")
    
    print(f"Success! Data {BUCKET_NAME} is loaded to your bucket into 'bronze' folder.")

if __name__ == "__main__":
    ingest()