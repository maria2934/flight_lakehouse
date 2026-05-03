import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error, accuracy_score
import polars as pl
from deltalake import DeltaTable
import pandas as pd
import os

BUCKET = os.getenv("S3_BUCKET", "s3://lakehouse")
FEATURE_TABLE_PATH = f"{BUCKET}/gold/features"
EXPERIMENT_NAME = "flight_delay_prediction"

storage_options = {
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "region": "us-east-1",
    "allow_http": "true"
}

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
mlflow.set_experiment(EXPERIMENT_NAME)

def train_ml():
    dt = DeltaTable(FEATURE_TABLE_PATH, storage_options=storage_options)
    current_version = dt.version() # Получаем версию таблицы
    
    df = pl.from_arrow(dt.to_pyarrow_dataset().to_table())

    X_pd = df.select(["month", "day_of_week", "hour", "Distance", "DepDelayMinutes", "season"]).to_pandas()
    X = pd.get_dummies(X_pd, columns=["season"], drop_first=True)
    y_reg, y_clf = df["ArrDelayMinutes"].to_numpy(), df["is_delayed_15"].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(X, y_reg, test_size=0.2, random_state=42)
    _, _, y_train_c, y_test_c = train_test_split(X, y_clf, test_size=0.2, random_state=42)

    with mlflow.start_run():
        # ТРЕБОВАНИЕ: Логируем версию таблицы
        mlflow.log_param("gold_table_version", current_version)
        
        reg = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42).fit(X_train, y_train)
        clf = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42).fit(X_train, y_train_c)

        mlflow.log_metric("rmse", root_mean_squared_error(y_test, reg.predict(X_test)))
        mlflow.log_metric("accuracy", accuracy_score(y_test_c, clf.predict(X_test)))
        mlflow.sklearn.log_model(reg, "model")

    print(f" Модель обучена на версии таблицы: {current_version}")

if __name__ == "__main__":
    train_ml()
