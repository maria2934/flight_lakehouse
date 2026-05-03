import polars as pl
from deltalake import write_deltalake, DeltaTable
import os
import time
from datetime import datetime

RAW_DATA_DIR = "data/raw/by_day"
BUCKET = os.getenv("S3_BUCKET", "s3://lakehouse")
BRONZE_TABLE_PATH = f"{BUCKET}/bronze/flights"

storage_options = {
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "region": "us-east-1",
    "allow_http": "true"
}

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def load_bronze():
    time.sleep(5) # Ждем готовности S3 бакетов
    log("НАЧАЛО ЗАГРУЗКИ BRONZE В S3")
    csv_files = sorted([f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")])
    
    for file in csv_files:
        df = pl.read_csv(os.path.join(RAW_DATA_DIR, file), infer_schema_length=10000, null_values=[''])
        try:
            DeltaTable(BRONZE_TABLE_PATH, storage_options=storage_options)
            mode = "append"
        except:
            mode = "overwrite"

        write_deltalake(
            BRONZE_TABLE_PATH,
            df.to_arrow(),
            mode=mode,
            storage_options=storage_options,
            schema_mode="merge"
        )
        log(f"Записан {file} в S3")
    log("ЗАГРУЗКА В S3 ЗАВЕРШЕНА")

if __name__ == "__main__":
    load_bronze()
