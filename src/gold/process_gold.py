import polars as pl
from deltalake import DeltaTable, write_deltalake
import os
from datetime import datetime

BUCKET = os.getenv("S3_BUCKET", "s3://lakehouse")
SILVER_PATH = f"{BUCKET}/silver/flights"
GOLD_DIR = f"{BUCKET}/gold"

storage_options = {
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "region": "us-east-1",
    "allow_http": "true"
}

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def process_gold():
    log("ОБРАБОТКА GOLD (Lazy + VACUUM + Schema Overwrite)")
    
    # Lazy чтение из Silver
    q = pl.scan_delta(SILVER_PATH, storage_options=storage_options)
    
    # Выбираем признаки и целевую переменную
    features = q.select([
        "year", "month", "day_of_week", "season", "hour",
        "IATA_Code_Marketing_Airline", "Origin", "Dest", "route",
        "Distance", "DepDelayMinutes", "ArrDelayMinutes"
    ]).with_columns([
        (pl.col("ArrDelayMinutes") > 15).cast(pl.Int8).alias("is_delayed_15")
    ]).collect()
    
    # ТРЕБОВАНИЕ: Запись с возможностью изменения схемы (schema_mode="overwrite")
    write_deltalake(
        f"{GOLD_DIR}/features", 
        features.to_arrow(), 
        mode="overwrite", 
        schema_mode="overwrite", 
        storage_options=storage_options
    )

    # ТРЕБОВАНИЕ: VACUUM (удаление старых файлов)
    dt = DeltaTable(f"{GOLD_DIR}/features", storage_options=storage_options)
    dt.vacuum(retention_hours=168, enforce_retention_duration=False, dry_run=False)
    
    log(f"GOLD ЗАВЕРШЕН. Feature Table версия: {dt.version()}")

if __name__ == "__main__":
    process_gold()
