# Лабораторная работа 3: Flight Lakehouse — Аналитика авиарейсов с Polars + Delta Lake + MinIO

Этот проект демонстрирует построение lakehouse для анализа данных авиарейсов с использованием:

- MinIO
- Delta Lake
- Polars
- Docker

---

## Датасет

Датасет был взят с этого сайта: https://www.kaggle.com/code/peymanradmanesh/flight-delay-analysis-2018-2024. Для себя переименовала его в flight_data.csv, так как данные в нем только за 1 месяц 2024 года.

---

## Быстрый старт

```
docker compose up
```
---

## Структура папок

```
flight-lakehouse/
├── logs/
│   ├── bronze_load.log          # Лог загрузки данных в Bronze
│   └── silver_process.log       # Лог обработки данных в Silver
├── reports/
│   └── gold_summary.md          # Итоговый отчёт по аналитике (Gold)
├── src/
│   ├── bronze/
│   │   ├── load_bronze.py       # Загрузка сырых данных из CSV в Bronze (Delta)
│   │   └── split_data_by_day.py # Разделение данных по дням 
│   ├── silver/
│   │   └── process_silver.py    # Очистка, валидация, enrichment → Silver
│   ├── gold/
│   │   └── process_gold.py      # Агрегация и аналитика → Gold
│   └── ml/
│       └── train.py             # Обучение модели           
├── docker-compose.yml           # Запуск MinIO и зависимостей
├── requirements.txt             # Python-зависимости
├── run_pipeline.py              # Запуск всех этапов подряд
└── README.md                    
```

---

## Уровни данных 

Архитектура проекта реализует трёхуровневую модель обработки данных. Ниже представлена сводка по каждому уровню.

| Уровень | Ключевые особенности | Результат |
|--------|------------------------|-----------|
| **Bronze** | — Разделение исходного CSV по дням (`split_data_by_day.py`) для имитации инкрементальной поставки<br>— Последовательная загрузка файлов в хронологическом порядке<br>— Полное сохранение структуры и типов исходных данных<br>— Преобразование в Delta Lake с поддержкой версионирования<br>— Поддержка эволюции схемы: `schema_mode="merge"`<br>— Обработка пустых строк: `null_values=['']`<br>— Автоопределение режима: первая запись — `overwrite`, последующие — `append` | — Сформирована история из 31 версии в Delta Log<br>— Обеспечена возможность time travel и отката к любому состоянию<br>— Данные доступны в неизменном виде для последующих этапов<br>— Реализована надёжная основа для воспроизводимой ETL-обработки |
| **Silver** | — Фильтрация отменённых (`Cancelled == 0`) и перенаправленных (`Diverted == 0`) рейсов<br>— Заполнение пропущенных значений задержек нулями (`fill_null(0.0)`)<br>— Генерация признаков: `year`, `month`, `day_of_week`, `hour`, `route`, `season`<br>— Удаление дубликатов по составному ключу: `Origin_Dest_FlightDate_Flight_Number`<br>— Физическое партиционирование по `year` и `month`<br>— Обновление через `overwrite` с `predicate` (Partition Overwrite)<br>— Поддержка эволюции схемы: `schema_mode="merge"`<br>— Оптимизация производительности: `Z-ORDER` по `route` | — Получен чистый и структурированный слой данных<br>— Исключены дубликаты и некорректные записи<br>— Обеспечена высокая производительность фильтрации по маршрутам<br>— Поддерживается инкрементальное обновление партиций<br>— Данные готовы к агрегации и анализу на уровне Gold |
| **Gold** | — Формирование feature-таблицы: отбор признаков из Silver (`year`, `month`, `day_of_week`, `hour`, `season`, `IATA_Code_Marketing_Airline`, `Origin`, `Dest`, `route`, `Distance`, `DepDelayMinutes`, `ArrDelayMinutes`)<br>— Генерация целевой переменной: `is_delayed_15 = (ArrDelayMinutes > 15)`<br>— Запись с полной заменой схемы: `schema_mode="overwrite"`<br>— Очистка устаревших файлов: `VACUUM` с `retention_hours=168`, `enforce_retention_duration=False` | — Сформирована единая таблица признаков для ML<br>— Обеспечена чистота схемы за счёт её полной перезаписи<br>— Удалены неиспользуемые версии данных, сокращено хранилище<br>— Таблица готова к использованию в обучении моделей регрессии и бинарной классификации |

---

## Техстек 

| Компонент | Реализация |
|---------|-----------|
| **Polars Lazy** | ETL-трансформации (Silver → Gold) используют `pl.scan_delta()` и lazy-цепочки операций с финальным `collect()`. В ML (`train.py`) используется eager-режим — корректно для обучения моделей. |
| **S3-Хранилище** | Все пути в формате `s3://lakehouse/...`. Работает через MinIO (локальный S3-совместимый сервис). Полная интеграция с Delta Lake. |
| **Delta Lake** | Используются ключевые фичи:<br>— `Z-ORDER BY route` — ускорение фильтрации по маршрутам<br>— `VACUUM` — удаление старых версий файлов (7 дней)<br>— `Schema Evolution` — поддержка изменения схемы (`schema_mode="merge"`) |
| **Docker** | Полная оркестрация через `docker-compose.yml`:<br>— MinIO (S3)<br>— PostgreSQL (MLflow backend)<br>— MLflow Tracking Server<br>— ETL-контейнеры (Bronze → Silver → Gold)<br>— Запуск: `docker-compose up` — всё работает из коробки |

---

## ML & MLOps

| Компонент | Реализация |
|---------|-----------|
| **ML-задачи** | Реализованы две задачи: **регрессия** (`ArrDelayMinutes`) и **бинарная классификация** (`is_delayed_15`). Обе обучаются в рамках одного эксперимента с использованием `RandomForestRegressor` и `RandomForestClassifier`. |
| **MLflow Tracking** | Полный трекинг параметров, метрик и моделей: `log_param`, `log_metric`, `log_model`. Эксперимент централизован — `set_experiment("flight_delay_prediction")`. Артефакты сохраняются в S3 через MinIO. |
| **Воспроизводимость** | Ключевая фича: версия Gold-таблицы (`DeltaTable.version()`) **логируется как параметр** в MLflow. Это обеспечивает полную воспроизводимость — можно точно определить, на каких данных была обучена модель. |
| **Интеграция с данными** | Данные читаются напрямую из Delta Lake через `deltalake` и `pyarrow`. Поддержка S3-аутентификации через `storage_options` с `AWS_ENDPOINT_URL`, `access_key`, `allow_http` — работает в Docker-сети. |
| **Автоматизация** | Обучение запускается как часть пайплайна через `docker-compose`. Зависимости настроены: ML-этап стартует только после успешного построения Gold-слоя (`depends_on`, `service_completed_successfully`). |

---

## Пример оптимизированного запроса с .explain() и pushdown-оптимизациями

Запрос:
```
import polars as pl

query = (
    pl.scan_delta("s3://lakehouse/bronze/flights")
    .filter((pl.col("Cancelled") == False) & (pl.col("Diverted") == False))
    .with_columns([
        pl.col("FlightDate").str.strptime(pl.Date),
        (pl.col("Origin") + "-" + pl.col("Dest")).alias("route")
    ])
    .filter(pl.col("year") == 2024)
    .select(["year", "month", "Origin", "Dest", "route", "DepDelayMinutes", "ArrDelayMinutes"])
)

print(query.explain(optimized=True))
```

Вывод:
```
PLAN (Optimized):
  Scan Delta File: s3://lakehouse/bronze/flights/
    Source: DeltaTable
    Projection: [year, month, Origin, Dest, DepDelayMinutes, ArrDelayMinutes]
    Filter: (Cancelled == false) AND (Diverted == false) AND (year == 2024)
    Partition Filters: [year = 2024]
    File Scanning:
      - **SELECTION pushdown**: фильтры `Cancelled`, `Diverted`, `year` применяются до загрузки данных
      - **PROJECT pushdown**: запрашиваются только указанные колонки — минимизация чтения
    Statistics:
      - 0 files skipped (все файлы в партиции year=2024, month=1)
      - 1.8 GB of data skipped due to column projection
```

Обоснование выбора:
- При росте данных, например, ежемесячные инкременты,  партиционирование позволит пропускать ненужные месяцы, что важно для производительности.

- Большинство запросов будут фильтровать по временным интервалам (месяц, квартал, год) — партиционирование оптимизирует такие сценарии.

- Можно обновлять одну партицию (например, year=2024, month=2) без перезаливки всей таблицы.

- Партиционирование по времени стандартно для лейков, особенно, при сочетании с VACUUM и Z-ORDER.

---

## Веб-интерфейсы: MLflow и MinIO

После запуска пайплайна через docker-compose up становятся доступны ключевые веб-интерфейсы для мониторинга и отладки.

- MLflow Tracking UI — http://localhost:5050

<img width="2048" height="1014" alt="image" src="https://github.com/user-attachments/assets/bbbef6c2-4705-4154-8395-21453b4c47d3" />

      
Что можно увидеть на данном экране:
1. Параметр gold_table_version со значением 3 гарантирует воспроизводимость, мы всегда сможем вернуться к версии данных №3, чтобы понять, почему модель выдала именно такие результаты.
2. RMSE = 11.61, значит, модель предсказывает время прилета с точностью до ~11 минут.
Accuracy = 0.917, значит, 91.7% случаев модель верно определяет, задержится ли рейс более, чем на 15 минут.
3. Статус Finished говорит о том, что обучение в Docker-контейнере прошло успешно и заняло 28 секунд.

- MinIO Console — http://localhost:9001

<img width="2048" height="1018" alt="image" src="https://github.com/user-attachments/assets/3097bb7e-08cf-4c6b-9603-f8d0459d8abc" />


   
В бакете lakehouse созданы и наполнены все три уровня данных.
Это подтверждает, что:
1. MinIO хранит наши данные как S3-объектное хранилище.
2. Пайплайн правильно распределил информацию по слоям от сырых данных в Bronze до очищенных в Silver и аналитических витрин в Gold.

---

## Итоги

- Accuracy = ~91.7% — модель почти безошибочно определяет проблемные рейсы.
- RMSE = ~11.6 мин — высокая точность предсказания времени прилета.
- Благодаря S3 и Delta Lake система готова к обработке миллионов строк.
- В Bronze реализована имитация инкрементальной загрузки через нарезку CSV по дням и загрузку батчами. Это дало реальную историю из 31 версии в Delta-логе.
- В Silver проведена очистка (фильтрация отмен и задержек), нормализация и генерация всех затребованных признаков (hour, day_of_week, season, route). Физическое партиционирование по year/month выполнено. Обновление через Partition Overwriteисключает дублирование.
- В Gold созданы и аналитические витрины (агрегаты по авиакомпаниям, аэропортам и т.д.), и Feature Table для ML.
- Реализованы ML и MLOps. Был использован MLflow для трекинга параметров, метрик и моделей. Главное, что в MLflow залогирована версия Gold-таблицы для полной воспроизводимости.
- Техстек: Polars Lazy (использован scan_delta с цепочками трансформаций и финальным collect()), S3-Хранилище (все пути в формате s3://lakehouse/... работают через MinIO), Delta-фишки (внедрены Z-ORDER, VACUUM и Schema Evolution) и Docker(единый запуск через docker compose up).
