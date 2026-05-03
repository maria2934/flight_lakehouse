import polars as pl
from deltalake import DeltaTable, write_deltalake
import os
from datetime import datetime

BUCKET = os.getenv("S3_BUCKET", "s3://lakehouse")
BRONZE_PATH = f"{BUCKET}/bronze/flights"
SILVER_PATH = f"{BUCKET}/silver/flights"

storage_options = {
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "region": "us-east-1",
    "allow_http": "true"
}

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def process_silver():
    log("ОБРАБОТКА SILVER (Lazy API + Z-ORDER)")
    
    # 1. Lazy-чтение через scan_delta (Selection/Projection Pushdown)
    # Выбираем колонки на этапе сканирования
    needed_cols = ["FlightDate", "CRSDepTime", "DepDelayMinutes", "ArrDelayMinutes", "Cancelled", "Diverted", "DepDel15", "Origin", "Dest", "Flight_Number_Marketing_Airline", "Distance", "IATA_Code_Marketing_Airline"]
    
    q = pl.scan_delta(BRONZE_PATH, storage_options=storage_options)
    
    # 2. Цепочка трансформаций (Lazy)
    q = q.select(needed_cols).filter((pl.col("Cancelled") == 0) & (pl.col("Diverted") == 0))
    
    # Типизация и признаки
    q = q.with_columns([
        pl.col("FlightDate").str.strptime(pl.Date, format="%Y-%m-%d"),
        pl.col("CRSDepTime").cast(pl.Utf8).str.pad_start(4, "0").str.replace(r"(\d{2})(\d{2})", r"$1:$2"),
        pl.col("DepDelayMinutes").cast(pl.Float64).fill_null(0.0),
        pl.col("ArrDelayMinutes").cast(pl.Float64).fill_null(0.0)
    ]).with_columns([
        pl.col("FlightDate").dt.year().alias("year"),
        pl.col("FlightDate").dt.month().alias("month"),
        pl.col("FlightDate").dt.weekday().alias("day_of_week")
    ]).with_columns([
        pl.col("CRSDepTime").str.strptime(pl.Time, format="%H:%M").dt.hour().alias("hour"),
        (pl.col("Origin") + "_" + pl.col("Dest")).alias("route"),
        pl.when(pl.col("month").is_in([12, 1, 2])).then(pl.lit("winter"))
          .when(pl.col("month").is_in([3, 4, 5])).then(pl.lit("spring"))
          .when(pl.col("month").is_in([6, 7, 8])).then(pl.lit("summer"))
          .otherwise(pl.lit("fall")).alias("season")
    ])

    # 3. Collect — выполняем все разом
    log("Выполнение Lazy-запроса (collect)...")
    df = q.collect()
    
    # Удаление дублей
    df = df.with_columns([(pl.col("Origin") + "_" + pl.col("Dest") + "_" + pl.col("FlightDate").dt.strftime("%Y-%m-%d") + "_" + pl.col("Flight_Number_Marketing_Airline").cast(pl.Utf8)).alias("flight_id")])
    df = df.unique(subset=["flight_id"])

    # 4. Запись (Merge/Overwrite)
    unique_parts = df.select(["year", "month"]).unique().to_dicts()
    predicate = " OR ".join([f"(year = {p['year']} AND month = {p['month']})" for p in unique_parts])

    write_deltalake(SILVER_PATH, df.to_arrow(), mode="overwrite", predicate=predicate, partition_by=["year", "month"], storage_options=storage_options, schema_mode="merge")
    
    # 5. Delta-оптимизация: Z-ORDER (для ускорения поиска по маршрутам)
    log("Оптимизация таблицы (Z-ORDER по route)...")
    dt = DeltaTable(SILVER_PATH, storage_options=storage_options)
    dt.optimize.z_order(["route"])
    
    log(f"SILVER ЗАВЕРШЕН. Версия: {dt.version()}")

if __name__ == "__main__":
    process_silver()
