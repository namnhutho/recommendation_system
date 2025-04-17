# prepare_data.py - auto generated
from pyspark.sql.functions import col, when, monotonically_increasing_id

def preprocess_data(df_spark):
    df = df_spark.limit(20000).dropna().dropDuplicates()
    df = df.withColumn("rating", when(col("event_type") == "purchase", 0.5)
                                .when(col("event_type") == "cart", 0.3)
                                .when(col("event_type") == "view", 0.1)
                                .otherwise(0))

    user_ids = df.select("user_id").distinct().withColumn("user_index", monotonically_increasing_id())
    product_ids = df.select("product_id").distinct().withColumn("product_index", monotonically_increasing_id())

    user_to_index = {row["user_id"]: row["user_index"] for row in user_ids.collect()}
    index_to_user = {v: k for k, v in user_to_index.items()}
    product_to_index = {row["product_id"]: row["product_index"] for row in product_ids.collect()}
    index_to_product = {v: k for k, v in product_to_index.items()}

    df = df.join(user_ids, "user_id").join(product_ids, "product_id")
    ratings = df.select("user_index", "product_index", "rating")\
                .withColumn("user_index", col("user_index").cast("integer"))\
                .withColumn("product_index", col("product_index").cast("integer"))\
                .withColumn("rating", col("rating").cast("float"))

    return ratings.randomSplit([0.8, 0.2]), user_to_index, index_to_user, product_to_index, index_to_product
