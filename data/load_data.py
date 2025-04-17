# load_data.py - auto generated
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from config.minio_config import get_minio_client

def load_data_from_minio():
    client = get_minio_client()
    bucket_name = "eventsdataset"
    csv_file_name = 'events.csv'
    response = client.get_object(bucket_name, csv_file_name)
    df_pandas = pd.read_csv(BytesIO(response.read()))

    # Fill nulls and convert types
    df_pandas['product_id'] = df_pandas['product_id'].fillna(0).astype('int64')
    df_pandas['category_id'] = df_pandas['category_id'].fillna(0).astype('int64')
    df_pandas['price'] = df_pandas['price'].fillna(0).astype('float64')
    df_pandas['user_id'] = df_pandas['user_id'].fillna(0).astype('int64')
    df_pandas.fillna({'category_code': '', 'brand': '', 'user_session': ''}, inplace=True)

    spark = SparkSession.builder \
        .appName("MinIO CSV") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "100")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])

    df_spark = spark.createDataFrame(df_pandas, schema=schema)
    return spark, df_spark
