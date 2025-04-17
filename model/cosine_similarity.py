# cosine_similarity.py - auto generated
from pyspark.sql.functions import col, lower, split, concat_ws, udf
from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF, Normalizer
from pyspark.sql.types import DoubleType

def compute_cosine_similarity(df_spark):
    df = df_spark.limit(20000).dropna(subset=["category_code", "brand"]).dropDuplicates()
    cbf_df = df.select("product_id", "category_code", "brand")
    cbf_df = cbf_df.withColumn("category_code", split(lower(col("category_code")), "\\."))
    cbf_df = cbf_df.withColumn("brand", lower(col("brand")))
    cbf_df = cbf_df.withColumn("combined_features", concat_ws(" ", col("brand"), col("category_code")))
    cbf_df = cbf_df.withColumn("combined_features", split(col("combined_features"), " "))
    
    remover = StopWordsRemover(inputCol="combined_features", outputCol="filtered")
    cbf_df = remover.transform(cbf_df)

    hashingTF = HashingTF(inputCol="filtered", outputCol="tf")
    tf = hashingTF.transform(cbf_df)
    idf = IDF(inputCol="tf", outputCol="tfidf").fit(tf)
    tfidf = idf.transform(tf)

    normalizer = Normalizer(inputCol="tfidf", outputCol="norm")
    data = normalizer.transform(tfidf)

    dot_udf = udf(lambda x, y: float(x.dot(y)), DoubleType())
    cos_similarity = data.alias("i").join(data.alias("j"), col("i.product_id") < col("j.product_id"))\
                         .select(
                             col("i.product_id").alias("i"),
                             col("j.product_id").alias("j"),
                             dot_udf("i.norm", "j.norm").alias("cosine_similarity")
                         )\
                         .filter(col("cosine_similarity") > 0.1)
    return cos_similarity
