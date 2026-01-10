#ALS - use rating training
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from dotenv import load_dotenv
load_dotenv()
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from common.spark_session import get_spark_session

# ============ ALS HYPERPARAMETERS ============
ALS_RANK = 50          # Number of latent factors (complexity of patterns)
ALS_MAX_ITER = 10      # Training iterations
ALS_REG = 0.1          # Regularization (prevent overfitting)
MIN_RATING = 1         # Filter out implicit 0 ratings
N_RECOMMENDATIONS = 100  # Top-N recommendations per user

# ============ PATHS ============
ALS_MODEL_PATH = "models/als_model"
ALS_INDEXERS_PATH = "models/als_indexers"
DELTA_USER_RECS = "delta/collaborative_recommendations"
DELTA_USER_FACTORS = "delta/user_factors"
DELTA_ITEM_FACTORS = "delta/item_factors"
# ======== LOAD DATA =====================================

def load_ratings_from_postgres(spark):    
    df = spark.read \
        .jdbc(
            url=JDBC_URL,
            table="ratings",
            properties=JDBC_PROPERTIES
        )
    
    ratings = (
        df.groupBy("user_id", "isbn")
        .agg(F.avg("rating").alias("rating"))  # Average if multiple ratings
        .filter(F.col("rating") >= MIN_RATING)  # Filter low ratings
    )
    
    return ratings

# =========== INDEX USERS & ITEMS =======================

def index_users_items(df):
    
    #from str to int(needed for matrix factorization...)
    #isbn: "0195153448"   → 0
    #user_id: "23904" => 0

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

    pipeline = Pipeline(stages=[user_indexer, item_indexer]) #chain transformation
    fitted = pipeline.fit(ratings) #learns mapping from training data

    #applies learned mapping to the data...
    #Adds two new columns: `user_idx`, `item_idx`
    indexed = fitted.transform(ratings).select(
    "user_id", "isbn", "user_idx", "item_idx", "rating"
    )

    #create reverse mapping tables (for later, when we need real isbn and user id back....)
    user_map = indexed.select("user_id", "user_idx").dropDuplicates()
    items_map = indexed.select("isbn", "item_idx").dropDuplicates()

    # select only var needed for als
    data = (
        indexed
        .select(
            F.col("user_idx").cast("int").alias("user_idx"),
            F.col("item_idx").cast("int").alias("item_idx"),
            F.col("rating").cast("float").alias("rating"),
        )
        .dropna(subset=["user_idx", "item_idx", "rating"])
        .filter(F.col("rating") >= 0)
        .filter(F.col("user_idx") >= 0)
        .filter(F.col("item_idx") >= 0)
    )
    
    if data.rdd.isEmpty():
        raise RuntimeError("❌ No training data after filtering!")
    
    return 

# =========== TRAIN ALS MODEL ==========================

def train_als(data, spark):
    #Set checkpoint directory (required for iterative algorithms)
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    
    als = ALS(
        userCol="user_idx",           # Column with user indices
        itemCol="item_idx",            # Column with item (book) indices
        ratingCol="rating",            # Column with ratings
        rank=rank,                     # Latent factors dimension (e.g., 20)
        maxIter=maxIter,               # Training iterations (e.g., 10)
        regParam=regParam,             # L2 regularization (e.g., 0.1)
        implicitPrefs=False,           # False = explicit ratings (1-10 scale)
        coldStartStrategy="drop",      # Drop predictions for unknown users/items
        nonnegative=False              # Allow negative latent factors
    )
    
    model = als.fit(train_df)    
    return model

#====== SIMILARITES SAVE ============================
def save_features(model, fitted):

    #trained ALS MODEL => can be load for retraining or gen recs....
    #ALS model = metadata, itemfactors, userfactors
    #load it= ALSModel.load("models/als")
    model.writes().overwrite().save(ALS_MODEL_PATH)

    #fitted INDEXERS pipeline...
    #fitted stringindexer pipeline = mapping user_id => user_idx..... + isbn -
    #PipelineModel.load("models/als_indexers")
    fitted.write().overwrite().save(ALS_INDEXERS_PATH)

    #=== EMBEDDINGS ============
    #latent vectors = dense vectors - capture preferences - core of ALS algo

    #latent factors - users: dim depends on rank(setup)
    model.userFactors.write\
        .format("delta")\
        .mode("overwrite")\
        .save(DELTA_USER_FACTORS)
    
    #latent factors - books
    model.itemFactors.write\
        .format("delta") \
        .mode("overwrite") \
        .save(DELTA_ITEM_FACTORS)

def generate_and_save_recommendations(model, users_map, items_map):
    
    #every user = top N recs books by ALS!
    #return df: user_idx , recommendations [(item_idx, rating), (....)]
    raw_user_recs = model.recommendForAllUsers(N_RECOMMENDATIONS)
    
    #converts nested structure to flat table
    #before
    """
        user_idx | recommendations
        ---------|----------------
        0        | [(5, 8.3), (12, 8.1), (7, 7.9)]
        1        | [(3, 9.2), (7, 8.9)]
    """

    #after
    #user_idx | rec
    """
        ---------|------------------
        0        | (item_idx=5, rating=8.3)
        0        | (item_idx=12, rating=8.1)
    """

    user_recs = (
        raw_user_recs
        .withColumn("rec", F.explode("recommendations"))
        .select(
            F.col("user_idx"),
            F.col("rec.item_idx").alias("item_idx"),
            F.col("rec.rating").alias("als_score"),
        )
        .join(users_map, "user_idx")  # Map back to original user_id
        .join(items_map, "item_idx")   # Map back to original ISBN
        .select("user_id", "isbn", "als_score")
    )

    """
    **After joins:**
        user_id | isbn         | als_score
        --------|--------------|----------
        276725  | 0195153448   | 8.3
        276725  | 0002005018   | 8.1
        276725  | 0060973129   | 7.9
        276726  | 0747532699   | 9.2
    """
    
    # Save to Delta Lake
    user_recs.write \
        .format("delta") \
        .mode("overwrite") \
        .save(DELTA_USER_RECS) #to: delta/collaborative_recommendations/

# =========== MAIN PIPELINE =============================

def main():
    
    spark = get_spark_session(
        app_name="ALS-Training",
        enable_delta=True,
        extra_conf={
            "spark.sql.shuffle.partitions": "50",
            "spark.default.parallelism": "50"
        }
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 1. Load data
        ratings = load_ratings_from_postgres(spark)
        
        # 2. Index users and items
        data, fitted, users_map, items_map = index_and_prepare_data(ratings)
        
        # 3. Split into train/test (80/20)
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
        
        # 4. Train model
        model = train_als(train_data, spark)
        
        # 5. Evaluate on test set
        metrics = evaluate_als(model, test_data)
        
        # 6. Save everything
        save_model_artifacts(model, fitted)
        generate_and_save_recommendations(model, users_map, items_map)

        print(" COMPLETE!")

        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n Spark session stopped")


if __name__ == "__main__":
    main()