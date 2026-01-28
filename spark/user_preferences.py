import os
import sys
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
import traceback

load_dotenv()

# Project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from common.spark_session import get_spark_session
from config import (
    JDBC_URL, JDBC_PROPERTIES,
    DELTA_ALS_MODEL,
    DELTA_USER_FACTORS,
    DELTA_ITEM_FACTORS,
    DELTA_USER_RECS
)

ALS_RANK = 50
ALS_MAX_ITER = 10
ALS_REG = 0.1
MIN_RATING = 1
TOP_N = 100

def load_ratings(spark):
    return (
        spark.read
        .jdbc(JDBC_URL, "ratings", properties=JDBC_PROPERTIES)
        .groupBy("user_id", "isbn")
        .agg(F.avg("rating").alias("rating"))
        .filter(F.col("rating") >= MIN_RATING)
    )

def index_users_items(df):
    user_indexer = StringIndexer(
        inputCol="user_id",
        outputCol="user_idx",
        handleInvalid="skip"
    )

    item_indexer = StringIndexer(
        inputCol="isbn",
        outputCol="item_idx",
        handleInvalid="skip"
    )

    pipeline = Pipeline(stages=[user_indexer, item_indexer])
    model = pipeline.fit(df)
    indexed = model.transform(df)

    data = (
        indexed
        .select(
            F.col("user_idx").cast("int"),
            F.col("item_idx").cast("int"),
            F.col("rating").cast("float")
        )
        .dropna()
    )

    user_map = indexed.select("user_id", "user_idx").dropDuplicates()
    item_map = indexed.select("isbn", "item_idx").dropDuplicates()

    return data, user_map, item_map, model

def train_als(data, spark):
    import shutil

    checkpoint_dir = "/tmp/spark-checkpoints"
    # Clean old checkpoints
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
    os.makedirs(checkpoint_dir, exist_ok=True)

    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG,
        implicitPrefs=False,
        coldStartStrategy="drop"
    )

    return als.fit(data)

def generate_als_recommendations(model, user_map, item_map, top_n=TOP_N):

    recs = model.recommendForAllUsers(top_n)

    recs_flat = (
        recs
        .withColumn("rec", F.explode("recommendations"))
        .select(
            F.col("user_idx"),
            F.col("rec.item_idx").alias("item_idx"),
            F.col("rec.rating").alias("als_score")
        )
    )

    return (
        recs_flat
        .join(user_map, "user_idx")
        .join(item_map, "item_idx")
        .select("user_id", "isbn", "als_score")
    )

def save_outputs(model, als_recs):

    # Save ALS model
    model.write().overwrite().save(DELTA_ALS_MODEL)

    # Save latent factors
    model.userFactors.write.format("delta").mode("overwrite").save(DELTA_USER_FACTORS)
    model.itemFactors.write.format("delta").mode("overwrite").save(DELTA_ITEM_FACTORS)

    # Save recommendations (THIS is what hybrid uses)
    als_recs.write.format("delta").mode("overwrite").save(DELTA_USER_RECS)

    print("ALS model, factors, and recommendations saved")

def main():
    spark = get_spark_session(
        app_name="ALS-Training",
        enable_delta=True,
        extra_conf={"spark.sql.shuffle.partitions": "100"}
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        ratings = load_ratings(spark)
        data, user_map, item_map, indexer_model = index_users_items(ratings)

        als_model = train_als(data, spark)

        als_recs = generate_als_recommendations(
            als_model,
            user_map,
            item_map
        )

        save_outputs(als_model, als_recs)

        print(f"Generated {als_recs.count():,} ALS recommendations")

    except Exception as e:
        print("❌ ERROR")
        traceback.print_exc()

    finally:
        spark.stop()
        print("Spark stopped")

if __name__ == "__main__":
    main()
