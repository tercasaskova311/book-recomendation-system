from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("ALS-CollaborativeFiltering") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
    .getOrCreate()

def load_ratings(path="ratings.csv"):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.select(
        col("User-ID").alias("user_id").cast("integer"),
        col("ISBN").alias("isbn"),
        col("Book-Rating").alias("rating").cast("float")
    )
    return df


def index_users_items(df):
    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx").fit(df)
    item_indexer = StringIndexer(inputCol="isbn", outputCol="item_idx").fit(df)
    
    df_indexed = user_indexer.transform(df)
    df_indexed = item_indexer.transform(df_indexed)
    
    return df_indexed, user_indexer, item_indexer


def train_als(df_indexed, rank=20, maxIter=10, regParam=0.1, implicitPrefs=True):
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        rank=rank,
        maxIter=maxIter,
        regParam=regParam,
        implicitPrefs=implicitPrefs,
        coldStartStrategy="drop"  # avoid NaNs in evaluation
    )
    model = als.fit(df_indexed)
    return model


def evaluate_als(model, df_indexed):
    predictions = model.transform(df_indexed)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"ALS RMSE: {rmse:.4f}")


def main_als():
    df = load_ratings("ratings.csv")
    df_indexed, user_idx, item_idx = index_users_items(df)
    
    model = train_als(df_indexed)
    evaluate_als(model, df_indexed)
    
    model.userFactors.write.format("delta").mode("overwrite").save("delta/user_factors")
    model.itemFactors.write.format("delta").mode("overwrite").save("delta/item_factors")

spark.stop()

