FROM python:3.11-slim
WORKDIR /app
# Установка системных зависимостей для работы с Delta Lake
RUN apt-get update && apt-get install -y libssl-dev gcc
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "run_pipeline.py"]
