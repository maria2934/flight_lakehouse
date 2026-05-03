import subprocess
import sys
from datetime import datetime

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_step(command, desc):
    log(f"Начало шага: {desc}")
    command = command.replace("python ", "python3 ")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        log(f"ОШИБКА на шаге: {desc}")
        sys.exit(1)
    log(f"УСПЕШНО: {desc}\n")

if __name__ == "__main__":
    log("ЗАПУСК ПОЛНОГО ПАЙПЛАЙНА")
    print("-" * 40)

    run_step("python src/bronze/split_data_by_day.py", "Разделение данных по дням")
    run_step("python src/bronze/load_bronze.py", "Загрузка в Bronze")
    run_step("python src/silver/process_silver.py", "Обработка Silver")
    run_step("python src/gold/process_gold.py", "Обработка Gold")
    run_step("python src/ml/train.py", "Обучение ML-моделей")

    print("-" * 40)
    log("ПАЙПЛАЙН ЗАВЕРШЕН УСПЕШНО")
    log("Отчет: reports/gold_summary.md")
    log("ML-эксперименты: папка mlruns/")
