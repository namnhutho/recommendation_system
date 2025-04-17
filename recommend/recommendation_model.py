from minio import Minio
import pandas as pd
from pyspark.sql import SparkSession
from io import BytesIO
from config.minio_config import get_minio_client

def load_weights_from_minio():
    """
    Load trọng số từ file CSV trong MinIO và trả về Spark DataFrame.
    Không nhận tham số, phục vụ cho việc gợi ý/dự đoán.
    
    Returns:
        df_spark (DataFrame): Spark DataFrame chứa trọng số.
    """
    client = get_minio_client()
    bucket_name = "eventsdataset"
    # file_name = "recommendations_v4.csv"
    file_name = "recommendations_testtop10v2.csv"
    # Lấy dữ liệu từ MinIO
    response = client.get_object(bucket_name, file_name)
    csv_data = response.read()

    # Đọc thành Pandas DataFrame
    df_pandas = pd.read_csv(BytesIO(csv_data))

    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Load Weights") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # Tắt log cảnh báo
    spark.sparkContext.setLogLevel("ERROR")

    # Chuyển về Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)

    return df_spark

from pyspark.sql.functions import col, isnan, max
def get_hybrid_recommendations(user_id, hybrid_df, k=10):
    user_recommendations = (
        hybrid_df
        .filter(col("user_id") == user_id)
        .filter(~isnan("final_score"))  # Remove NaN values
        .filter(col("final_score").isNotNull()) 
        .groupBy("product_id").agg(max("final_score").alias("final_score")) 
        .orderBy(col("final_score").desc()) 
        .limit(k) 
        .toPandas()  
    )
    return user_recommendations