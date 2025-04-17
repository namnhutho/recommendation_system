import pandas as pd
from io import BytesIO
from pyspark.sql.types import *
from config.minio_config import get_minio_client

    # Khởi tạo MinIO client và đọc dữ liệu một lần khi server khởi động
client = get_minio_client()
bucket_name = "eventsdataset"
csv_file_name = "events.csv"
response = client.get_object(bucket_name, csv_file_name)
df_pandas = pd.read_csv(BytesIO(response.read()), parse_dates=["event_time"])

def compute_scores(df, recent_days=7, top_n=10):
    # Tính điểm phổ biến toàn bộ thời gian
    popularity_score = df.groupby("product_id")["event_type"].value_counts().unstack(fill_value=0)
    popularity_score["score"] = (
        popularity_score.get("purchase", 0) * 3 +
        popularity_score.get("cart", 0) * 2 +
        popularity_score.get("view", 0) * 1
    )

    # Dữ liệu gần đây
    latest_date = df["event_time"].max()
    recent_df = df[df["event_time"] >= (latest_date - pd.Timedelta(days=recent_days))]
    trend_score = recent_df.groupby("product_id")["event_type"].value_counts().unstack(fill_value=0)
    trend_score["trend_score"] = (
        trend_score.get("purchase", 0) * 3 +
        trend_score.get("cart", 0) * 2 +
        trend_score.get("view", 0) * 1
    )

    top_popular = popularity_score.sort_values("score", ascending=False).reset_index().head(top_n)
    top_trending = trend_score.sort_values("trend_score", ascending=False).reset_index().head(top_n)

    return top_popular[["product_id", "score"]], top_trending[["product_id", "trend_score"]]
