"""
 Hybrid Recommendations
- Input:  user_factors, item_factors, book_similarities:                                               
    1. For each user: Hybrid score: α*collab + (1-α)*content              
    - α = 0.7 (favor collaborative filtering)            
    2. Rank top-100 books per user by hybrid score             
    3. Filter out already-rated books                          
    4. Apply diversity boost (avoid recommending same author)  

- Output: recommendations_cache table                          
- Pre-computed recommendations ready for instant serving 
"""
import os
import sys
import traceback
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from common.spark_session import get_spark_session
from config import (
    JDBC_URL, JDBC_PROPERTIES, ALPHA, N_RECOMMENDATIONS,
    MIN_CONTENT_SIMILARITY, DELTA_SIMILARITIES,
    DELTA_FINAL_RECS, DELTA_USER_RECS, DELTA_ALS_MODEL
)

#========= USER HISTORY: filter out already rated books + find content based candidates... =============

def read_user_rating(spark):
    return (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="ratings",
            properties=JDBC_PROPERTIES
        )
        .filter(F.col("rating") >= 7)  # Only consider highly-rated books (7+)
        .select(
            F.col("user_id"),
            F.col("rating"),
            F.col("isbn")
        )
        .dropDuplicates()
    )

#========LOAD DATA FROM DELTA =============

def load_sim (spark):
    similarities = (
        spark.read.format("delta")
        .load(DELTA_SIMILARITIES)
        .select(
            F.col("isbn_a"),
            F.col("isbn_b"),
            F.col("similarity_score").cast(DoubleType())
        )
        .filter(F.col("similarity_score") >= MIN_CONTENT_SIMILARITY)
        .dropna()
    )
    sims_reverse = similarities.select(
        F.col("isbn_b").alias("isbn_a"),
        F.col("isbn_a").alias("isbn_b"),
        F.col("similarity_score")
    )

    return similarities.union(sims_reverse)

def load_als_score(spark):

    return (
        spark.read.format("delta")
        .load(DELTA_USER_RECS)
        .select(
            F.col("user_id"),
            F.col("isbn"),
            F.col("als_score").cast(DoubleType())
        )
        .dropna(subset=["user_id", "isbn", "als_score"])
    )

#======= FINAL SCORE ==================================
def compute_sim_scores(ratings, content_sims):
    """
    Compute content-based scores per user by finding max similarity
    to any rated book.
    """
    content_scores = (
        ratings
        .join(
            content_sims,
            ratings.isbn == content_sims.isbn_a,
            how="inner"
        )
        .groupBy(
            ratings.user_id,
            content_sims.isbn_b
        )
        .agg(
            F.max("similarity_score").alias("content_score")
        )
        .withColumnRenamed("isbn_b", "isbn")
    )

    return content_scores

# ===================== COMPUTE HYBRID SCORE =====================
def compute_hybrid_scores(als_recs, content_scores, alpha=ALPHA):
    """
    Combine ALS and content scores into a hybrid score.
    Handle cold-start by filling missing scores with median.
    """
    # Compute medians for cold-start
    median_als = als_recs.approxQuantile("als_score", [0.5], 0.01)[0]
    median_content = content_scores.approxQuantile("content_score", [0.5], 0.01)[0]

    hybrid_df = als_recs.alias("als").join(
        content_scores.alias("cont"),
        on=["user_id", "isbn"],
        how="outer"
    ).select(
        F.coalesce(F.col("als.user_id"), F.col("cont.user_id")).alias("user_id"),
        F.coalesce(F.col("als.isbn"), F.col("cont.isbn")).alias("isbn"),
        (
            alpha * F.coalesce(F.col("als.als_score"), F.lit(median_als)) +
            (1 - alpha) * F.coalesce(F.col("cont.content_score"), F.lit(median_content))
        ).alias("hybrid_score"),
        F.coalesce(F.col("als.als_score"), F.lit(median_als)).alias("als_score"),
        F.coalesce(F.col("cont.content_score"), F.lit(median_content)).alias("content_score")
    )

    return hybrid_df


def filter_already_rated(hybrid_df, user_history):

    return hybrid_df.alias("h").join(
        user_history.alias("hist"),
        (F.col("h.user_id") == F.col("hist.user_id")) &
        (F.col("h.isbn") == F.col("hist.isbn")),
        "left_anti"
    )


# ============ STEP 7: Select Top-N Per User ============
def select_top_n_per_user(hybrid_df, top_n=N_RECOMMENDATIONS):

    window = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))
    return (
        hybrid_df
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
        .withColumn("generated_at", F.current_timestamp())
        .select("user_id", "isbn", "hybrid_score", "als_score", "content_score", "rank", "generated_at")
    )


# ============ : Save to PostgreSQL Cache ============
def save_to_postgres(df):
    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table="recommendations_cache",
            mode="overwrite",
            properties=JDBC_PROPERTIES
        )
    )


# ============ MAIN PIPELINE ============
def main():

    spark = get_spark_session(
        app_name="BookRecommendation-Hybrid",
        enable_delta=True,
        extra_conf={
            "spark.sql.shuffle.partitions": "50"
        }
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:        
        user_history = read_user_rating(spark)
        als_recs = load_als_score(spark)
        content_sims = load_sim(spark)

        content_scores = compute_sim_scores(user_history, content_sims)

        hybrid = compute_hybrid_scores(als_recs, content_scores, alpha=ALPHA)

        filtered = filter_already_rated(hybrid, user_history)

        final_recs = select_top_n_per_user(filtered, top_n=N_RECOMMENDATIONS)

        save_to_postgres(final_recs)

        print(" HYBRID RECOMMENDATION PIPELINE COMPLETE!")

        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        return 1
        
    finally:
        spark.stop()
        print("\ Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())



