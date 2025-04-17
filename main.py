# main.py
from data.load_data import load_data_from_minio
from model.cosine_similarity import compute_cosine_similarity
from preprocessing.prepare_data import preprocess_data
from model.als_model import train_als_model
from recommend.hybrid_recommend import map_recommendations, build_hybrid
from pyspark.sql.functions import col, isnan, max,row_number, dense_rank, countDistinct
from pyspark.sql.window import Window
from recommend.recommendation_model import load_weights_from_minio, get_hybrid_recommendations
from data.upload_to_minio import upload_df_to_minio, upload_df_to_minio_as_parquet
from config.minio_config import get_minio_client
from recommend.trend_popular import compute_scores, df_pandas
from data.collect_data import collect_data
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import threading
import schedule
import time
import pandas as pd
import io
from routes.kafka_routes import kafka_bp
from routes.recommend_routes import recommend_bp
from routes.anonymous_routes import anonymous_bp

app = Flask(__name__)
app.register_blueprint(kafka_bp)
app.register_blueprint(recommend_bp)
app.register_blueprint(anonymous_bp)

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# @app.route('/send-kafka', methods=['POST'])
# def send_to_kafka():
#     data = request.get_json()
#     if not data:
#         return jsonify({'error': 'No JSON provided'}), 400

#     producer.send('json-topic', value=data)
#     producer.flush()
#     return jsonify({'status': 'Message sent to Kafka ✅'}), 200

def run_collect_data():
    collect_data()

def run_training_job():
    print("🛠️ Bắt đầu huấn luyện mô hình CF + CBF + Hybrid...")
    try:
        spark, df_spark = load_data_from_minio()
        cosine_df = compute_cosine_similarity(df_spark)

        (train_data, test_data), user_to_index, index_to_user, product_to_index, index_to_product = preprocess_data(df_spark)
        print("Số user và product", len(index_to_user), len(index_to_product))
        cvModel, rmse = train_als_model(train_data, test_data)
        print(f"✅ Mô hình CF huấn luyện xong với RMSE: {rmse}")

        top_k = 10
        recommendations = cvModel.bestModel.recommendForAllUsers(top_k)
        final_recommendations = map_recommendations(recommendations, index_to_user, index_to_product, spark)
        cf_recommendations_spark = spark.createDataFrame(final_recommendations)
        hybrid_df = build_hybrid(cf_recommendations_spark, cosine_df, spark)
        # Chọn chỉ các cột user_id, product_id và final_score
        hybrid_df = hybrid_df.select("user_id", "product_id", "final_score")
        hybrid_df = hybrid_df.dropDuplicates(["product_id", "final_score"])

        # Tạo cửa sổ nhóm theo user_id và sắp xếp theo final_score giảm dần
        window_spec = Window.partitionBy("user_id").orderBy(col("final_score").desc())

        # Tính thứ hạng dựa trên final_score
        hybrid_df_with_rank = hybrid_df.withColumn("rank", row_number().over(window_spec))

        # Loại bỏ các product_id trùng lặp trong từng nhóm user_id
        # Chỉ giữ lại product_id đầu tiên xuất hiện (với thứ tự rank)
        window_spec_unique = Window.partitionBy("user_id", "product_id").orderBy(col("rank"))
        hybrid_df_unique = hybrid_df_with_rank.withColumn("dense_rank", dense_rank().over(window_spec_unique))

        # Lọc giữ lại các sản phẩm với dense_rank = 1 (không trùng product_id)
        hybrid_df_filtered = hybrid_df_unique.filter(col("dense_rank") == 1)

        # Lấy top 10 sản phẩm cho mỗi user_id
        window_spec_top10 = Window.partitionBy("user_id").orderBy(col("final_score").desc())
        top10_per_user = hybrid_df_filtered.withColumn("rank", row_number().over(window_spec_top10)) \
                                        .filter(col("rank") <= 10) \
                                        .drop("rank", "dense_rank")  # Xóa cột không cần thiết
        # Hiển thị kết quả
        print("ĐÃ LẤY CHỈ USER_ID, PRODUCT_ID VÀ FINAL_SCORE")
        print(top10_per_user)
        t=time.time()
        df_pandas = top10_per_user.toPandas()
        print(time.time()-t)
        print("Dữ liệu từ pandas")
        print(df_pandas)
        # ✅ Upload lên MinIO
        upload_df_to_minio(df_pandas, bucket_name='eventsdataset', object_name='recommendations_testtop10v2.csv')
        print("🎉 Huấn luyện mô hình hoàn tất.")
    except Exception as e:
        print(f"❌ Lỗi khi huấn luyện mô hình: {e}")
        
def schedule_training():
    # Huấn luyện mỗi ngày lúc 01:00 sáng
    schedule.every().day.at("08:39").do(run_training_job)

    # Lặp vô hạn để chạy lịch
    while True:
        schedule.run_pending()
        time.sleep(60)

def load_csv_from_minio(bucket: str, object_name: str):
    try:
        minio_client = get_minio_client()
        response = minio_client.get_object(bucket, object_name)
        data = response.read()
        return pd.read_csv(io.BytesIO(data))
    except Exception as e:
        print(f"❌ Lỗi khi load {object_name} từ MinIO: {e}")
        return None

# curl http://127.0.0.1:5000/recommend-weighted/1515915625519645266
# Tải mô hình trọng số từ MinIO khi khởi động server
# df_metadata = load_csv_from_minio("eventsdataset", "events.csv")  # Dataset gốc
# df_weight = load_weights_from_minio()
# if df_weight:
#     print("True")
# else:
#     print("False")
# @app.route('/recommend-weighted/<int:user_id>', methods=['GET'])
# def recommend_weighted(user_id):
#     try:
#         top_k = 10
#         recommendations_df = get_hybrid_recommendations(user_id, df_weight, k=top_k)

#         if hasattr(recommendations_df, "toPandas"):
#             recommendations_df = recommendations_df.toPandas()
#         if not recommendations_df.empty:
#             # Lấy metadata từ dataset gốc
#             metadata_df = df_metadata[['product_id', 'category_code', 'brand', 'price']].drop_duplicates()
#             merged_df = recommendations_df.merge(metadata_df, on='product_id', how='left')
#             recommendations_list = merged_df.to_dict(orient='records')
#         else:
#             recommendations_list = []
#         return jsonify({
#             'user_id': user_id,
#             'recommendations': recommendations_list
#         })

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# curl "http://localhost:5000/trending-products?top_n=5&recent_days=14"
# curl http://localhost:5000/trending-products
# curl http://localhost:5000/popular-products
@app.route("/popular-products", methods=["GET"])
def get_popular_products():
    popular, _ = compute_scores(df_pandas)
    return jsonify(popular.to_dict(orient="records"))

@app.route("/trending-products", methods=["GET"])
def get_trending_products():
    _, trending = compute_scores(df_pandas)
    return jsonify(trending.to_dict(orient="records"))
    
if __name__ == '__main__':
    # Tạo thread để chạy collect_data song song
    t = threading.Thread(target=run_collect_data, daemon=True)
    t.start()
    # Thread 2: lên lịch huấn luyện định kỳ
    t2 = threading.Thread(target=schedule_training, daemon=True)
    t2.start()
    # Khởi động Flask server
    print("🚀 Flask đang chạy tại http://localhost:5000")
    app.run(host='0.0.0.0', port=5000)
    
    
    