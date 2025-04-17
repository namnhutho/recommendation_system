import random
import pandas as pd
from io import BytesIO
from config.minio_config import get_minio_client
# Hàm đọc dữ liệu metadata mới nhất từ MinIO
def get_latest_metadata():
    client = get_minio_client()
    obj = client.get_object("eventsdataset", "events.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df

def get_popular_items(df_metadata: pd.DataFrame, top_k=50):
    popular_products = (
        df_metadata[df_metadata['event_type'] == 'purchase']
        .groupby('product_id')
        .size()
        .reset_index(name='score')
    )
    popular_products['source'] = 'popular'
    return popular_products

# def get_trending_items(df_metadata: pd.DataFrame, top_k=50):
#     trending_weights = {
#         'view': 1,
#         'cart': 2,
#         'purchase': 3
#     }

#     df_metadata['weight'] = df_metadata['event_type'].map(trending_weights)
#     trend_scores = (
#         df_metadata.groupby('product_id')['weight']
#         .sum()
#         .reset_index(name='score')
#         .sort_values(by='score', ascending=False)
#         .head(top_k)
#     )
#     trend_scores['source'] = 'trending'
#     return trend_scores
from datetime import datetime, timedelta, timezone

def get_trending_items(df_metadata: pd.DataFrame, top_k=50, days=3650):
    # Chuyển đổi cột thời gian
    df_metadata['event_time'] = pd.to_datetime(df_metadata['event_time'])

    # Giới hạn thời gian (ví dụ: 7 ngày gần nhất, có UTC)
    time_threshold = datetime.now(timezone.utc) - timedelta(days=days)
    df_recent = df_metadata[df_metadata['event_time'] >= time_threshold]

    # Tính điểm trending với trọng số
    trending_weights = {
        'view': 1,
        'cart': 2,
        'purchase': 3
    }
    df_recent['weight'] = df_recent['event_type'].map(trending_weights)

    trend_scores = (
        df_recent.groupby('product_id')['weight']
        .sum()
        .reset_index(name='score')
        .sort_values(by='score', ascending=False)
        .head(top_k)
    )
    trend_scores['source'] = 'trending'
    return trend_scores

def get_trending_and_popular(df_metadata, top_k=10):
    trending_df = get_trending_items(df_metadata, top_k * 2)
    popular_df = get_popular_items(df_metadata, top_k * 2)

    combined_df = pd.concat([trending_df, popular_df])
    combined_df = combined_df.groupby('product_id').agg({
        'score': 'sum',
        'source': lambda x: 'both' if len(set(x)) > 1 else list(x)[0]
    }).reset_index()

    top_combined = combined_df.sort_values(by='score', ascending=False).head(top_k)
    return top_combined

def get_diversified_trending_popular(df_metadata, top_k_each=5):
    trending_df = get_trending_items(df_metadata, top_k=top_k_each)  # Top 5 trending

    full_popular_df = get_popular_items(df_metadata, top_k=50)       # Lấy 50 popular đầu tiên
    popular_sample = full_popular_df.sample(n=top_k_each, random_state=random.randint(1, 9999))  # Random 5

    combined = pd.concat([trending_df, popular_sample], ignore_index=True).drop_duplicates(subset='product_id')
    return combined