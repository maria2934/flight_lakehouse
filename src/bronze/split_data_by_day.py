import polars as pl
import os
from datetime import datetime

# --- Пути ---
RAW_CSV_PATH = "data/raw/flight_data.csv"
OUTPUT_DIR = "data/raw/by_day"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

log("Читаю исходный CSV...")

# Читаем CSV
df = pl.read_csv(RAW_CSV_PATH)

# Проверим первую строку
log(f"Размер данных: {df.shape[0]} строк, {df.shape[1]} колонок")
log(f"Первая дата: {df['FlightDate'][0]}")

# Убедимся, что FlightDate — строка, и преобразуем в дату
df = df.with_columns(
    pl.col("FlightDate").str.strptime(pl.Date, format="%Y-%m-%d")
)

# Добавим колонку с днём
df = df.with_columns(
    pl.col("FlightDate").dt.strftime("%Y-%m-%d").alias("DAY")
)

# Получим уникальные дни
days = df["DAY"].unique().sort()
log(f"Найдено дней: {len(days)} → {days.to_list()}")

# Сохраняем по дням
for day in days:
    df_day = df.filter(pl.col("DAY") == day).drop(["DAY"])
    filename = f"flights_{day}.csv"
    output_path = os.path.join(OUTPUT_DIR, filename)
    df_day.write_csv(output_path)
    log(f"Сохранено: {filename} → {len(df_day)} строк")

log(" Разделение по дням завершено!")
