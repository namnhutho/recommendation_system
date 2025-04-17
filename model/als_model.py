# als_model.py - auto generated
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

def train_als_model(train_data, test_data):
    als = ALS(userCol="user_index",
              itemCol="product_index",
              ratingCol="rating",
              coldStartStrategy="drop")
    paramGrid = ParamGridBuilder()\
        .addGrid(als.rank, [10])\
        .addGrid(als.regParam, [0.01])\
        .build()
    
    crossval = CrossValidator(estimator=als,
                              estimatorParamMaps=paramGrid,
                              evaluator=RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse"),
                              numFolds=3)
    cvModel = crossval.fit(train_data)
    predictions = cvModel.transform(test_data)
    rmse = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse").evaluate(predictions)
    
    return cvModel, rmse
