# hybrid_recommend.py - auto generated
from pyspark.sql.functions import col, explode, struct, collect_list, expr,slice

def map_recommendations(recommendations, index_to_user, index_to_product, spark):
    recommendations = recommendations.withColumn("recommendations", explode(col("recommendations")))
    recommendations = recommendations.select(
        col("user_index"),
        col("recommendations.product_index").alias("product_index"),
        col("recommendations.rating").alias("predicted_rating")
    ).toPandas()

    recommendations["user_id"] = recommendations["user_index"].map(index_to_user)
    recommendations["product_id"] = recommendations["product_index"].map(index_to_product)
    return recommendations[["user_id", "product_id", "predicted_rating"]]

def build_hybrid(cf_recommendations_spark, cbf_df, spark):
    # Tính điểm dựa trên nội dung (CBF)
    cbf_recommendations = (
        cbf_df
        .groupBy("i")
        .agg(collect_list(struct("j", "cosine_similarity")).alias("recommendations"))
        .withColumn("recommendations", slice(col("recommendations"), 1, 10))  # lấy top 10
        .withColumn("recommendation", explode(col("recommendations")))
        .select(
            col("i").alias("product_id"),
            col("recommendation.j").alias("recommended_product"),
            (col("recommendation.cosine_similarity") * 0.3).alias("cbf_score")
        )
    )

    # Kết hợp CF và CBF
    hybrid = (
        cf_recommendations_spark
        .join(cbf_recommendations, on=["product_id"], how="left")
        .fillna({"cbf_score": 0})
        .withColumn("final_score", col("predicted_rating") * 0.7 + col("cbf_score"))
    )

    return hybrid.orderBy(col("final_score").desc())

# def build_hybrid(cf_recommendations_spark, cbf_df, spark):
#     cbf_recommendations = cbf_df.groupBy("i").agg(collect_list(struct("j", "cosine_similarity")).alias("recommendations"))
#     cbf_recommendations = cbf_recommendations.withColumn("recommendations", expr("slice(recommendations, 1, 10)"))
#     cbf_recommendations = cbf_recommendations.selectExpr("i as product_id", "explode(recommendations) as recs")
#     cbf_recommendations = cbf_recommendations.selectExpr("product_id", "recs.j as recommended_product", "recs.cosine_similarity as cbf_score")
#     cbf_recommendations = cbf_recommendations.withColumn("cbf_score", col("cbf_score") * 0.3)

#     hybrid = cf_recommendations_spark.join(cbf_recommendations, on=["product_id"], how="left").fillna(0, subset=["cbf_score"])
#     hybrid = hybrid.withColumn("final_score", col("predicted_rating") * 0.7 + col("cbf_score"))
#     return hybrid.orderBy(col("final_score").desc())

# import time
# from pyspark.sql.functions import col, collect_list, struct, expr

# def build_hybrid(cf_recommendations_spark, cbf_df, spark):
#     start_time = time.time()
#     print("🔧 Bắt đầu xây dựng mô hình Hybrid...")

#     print("📦 Bước 1: Gom nhóm và lấy top 10 sản phẩm từ CBF...")
#     cbf_recommendations = cbf_df.groupBy("i").agg(collect_list(struct("j", "cosine_similarity")).alias("recommendations"))
#     print(f"  ➕ Tổng sản phẩm có CBF: {cbf_recommendations.count()}")

#     cbf_recommendations = cbf_recommendations.withColumn("recommendations", expr("slice(recommendations, 1, 10)"))
#     cbf_recommendations = cbf_recommendations.selectExpr("i as product_id", "explode(recommendations) as recs")
#     cbf_recommendations = cbf_recommendations.selectExpr("product_id", "recs.j as recommended_product", "recs.cosine_similarity as cbf_score")
#     cbf_recommendations = cbf_recommendations.withColumn("cbf_score", col("cbf_score") * 0.3)
#     print(f"  ✅ Số dòng sau explode (CBF recommendations): {cbf_recommendations.count()}")

#     print("🔗 Bước 2: Join CF với CBF...")
#     hybrid = cf_recommendations_spark.join(cbf_recommendations, on=["product_id"], how="left")
#     print(f"  🔄 Số dòng sau join: {hybrid.count()}")

#     print("🧮 Bước 3: Tính điểm hybrid tổng hợp (70% CF + 30% CBF)...")
#     hybrid = hybrid.fillna(0, subset=["cbf_score"])
#     hybrid = hybrid.withColumn("final_score", col("predicted_rating") * 0.7 + col("cbf_score"))

#     hybrid = hybrid.orderBy(col("final_score").desc())
#     print(f"✅ Hoàn tất hybrid. Tổng số dòng: {hybrid.count()}")
    
#     elapsed = time.time() - start_time
#     print(f"⏱️ Tổng thời gian xử lý hybrid: {elapsed:.2f} giây")

#     return hybrid


