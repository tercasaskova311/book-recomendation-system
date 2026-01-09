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

# ======== LOAD DATA =====================================

def load_ratings(spark, path):    
    df = spark.read.csv(path, header=True, inferSchema=True)    
    df = df.select(
        col("User-ID").alias("user_id").cast("integer"),
        col("ISBN").alias("isbn"),
        col("Book-Rating").alias("rating").cast("float")
    )
    
    df = df.filter(
        (col("user_id").isNotNull()) &
        (col("isbn").isNotNull()) &
        (col("rating").isNotNull()) &
        (col("rating") >= 0)  
    )
    return df

# =========== INDEX USERS & ITEMS =======================

def index_users_items(df):
    
    user_indexer = StringIndexer(
        inputCol="user_id",
        outputCol="user_idx",
        handleInvalid="skip"  # Skip unknown users
    ).fit(df)
    
    item_indexer = StringIndexer(
        inputCol="isbn",
        outputCol="item_idx",
        handleInvalid="skip"  # Skip unknown items
    ).fit(df)
    
    df_indexed = user_indexer.transform(df)
    df_indexed = item_indexer.transform(df_indexed)
    
    n_users = df_indexed.select("user_idx").distinct().count()
    n_items = df_indexed.select("item_idx").distinct().count()
        
    return df_indexed, user_indexer, item_indexer

# =========== TRAIN ALS MODEL ==========================

def train_als(train_df, rank=20, maxIter=10, regParam=0.1):
    
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        rank=rank,
        maxIter=maxIter,
        regParam=regParam,
        implicitPrefs=False,  
        coldStartStrategy="drop",  # Avoid NaNs for unknown users/items
        nonnegative=False  # Allow negative factors
    )
    
    model = als.fit(train_df)    
    return model

# =========== EVALUATE MODEL ============================

def evaluate_als(model, test_df):
    
    predictions = model.transform(test_df)    
    predictions = predictions.filter(col("prediction").isNotNull())
    
    # RMSE (Root Mean Squared Error)
    rmse_evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = rmse_evaluator.evaluate(predictions)
    
    # MAE (Mean Absolute Error)
    mae_evaluator = RegressionEvaluator(
        metricName="mae",
        labelCol="rating",
        predictionCol="prediction"
    )
    mae = mae_evaluator.evaluate(predictions)
    
    # R² (Coefficient of Determination)
    r2_evaluator = RegressionEvaluator(
        metricName="r2",
        labelCol="rating",
        predictionCol="prediction"
    )
    r2 = r2_evaluator.evaluate(predictions)
    
    print(f"✅ Evaluation Results:")
    print(f"   RMSE: {rmse:.4f}")
    print(f"   MAE:  {mae:.4f}")
    print(f"   R²:   {r2:.4f}")
    
    return {"rmse": rmse, "mae": mae, "r2": r2}

# =========== SAVE ARTIFACTS ============================

def save_model_artifacts(model, user_indexer, item_indexer, base_path="models"):
    
    os.makedirs(base_path, exist_ok=True)
    os.makedirs("delta", exist_ok=True)
    
    # 1. Save full model (for inference)
    model_path = f"{base_path}/als_model"
    model.write().overwrite().save(model_path)
    
    # 2. Save user factors to Delta Lake
    user_factors_path = "delta/user_factors"
    model.userFactors.write \
        .format("delta") \
        .mode("overwrite") \
        .save(user_factors_path)
    
    # 3. Save item factors to Delta Lake
    item_factors_path = "delta/item_factors"
    model.itemFactors.write \
        .format("delta") \
        .mode("overwrite") \
        .save(item_factors_path)
    
    # 4. Save indexers (CRITICAL for making predictions later!)
    user_indexer_path = f"{base_path}/user_indexer"
    user_indexer.write().overwrite().save(user_indexer_path)
    
    item_indexer_path = f"{base_path}/item_indexer"
    item_indexer.write().overwrite().save(item_indexer_path)

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
        df = load_ratings(spark,"data/Ratings.csv")
        
        # 2. Index users and items
        df_indexed, user_indexer, item_indexer = index_users_items(df)
        
        # 3. Split into train/test (80/20)
        print("\n✂️  Splitting data (80% train, 20% test)...")
        train_df, test_df = df_indexed.randomSplit([0.8, 0.2], seed=42)
        print(f"   Train: {train_df.count():,} ratings")
        print(f"   Test:  {test_df.count():,} ratings")
        
        # 4. Train model
        model = train_als(
            train_df,
            rank=20,      # Number of latent factors
            maxIter=10,   # Number of iterations
            regParam=0.1  # Regularization to prevent overfitting
        )
        
        # 5. Evaluate on test set
        metrics = evaluate_als(model, test_df)
        
        # 6. Save everything
        save_model_artifacts(model, user_indexer, item_indexer)
        
        print(" COMPLETE!")
        print(f"\n Final Metrics:")
        print(f"   RMSE: {metrics['rmse']:.4f}")
        print(f"   MAE:  {metrics['mae']:.4f}")
        print(f"   R²:   {metrics['r2']:.4f}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n Spark session stopped")


if __name__ == "__main__":
    main()