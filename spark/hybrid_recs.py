#
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
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import traceback


# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from common.spark_session import get_spark_session
from config import JDBC_URL, JDBC_PROPERTIES

ALPHA = 0.7
NUM_RECS = 100 #top n per user
MIN_CONTENT_SIMILARITY = 0.1 #filter weak content similarities..
DELTA_ALS_RECS = "delta/collaborative_recommendations"
DELTA_CONTENT_SIMS = "delta/content_similarities"
DELTA_FINAL_RECS = "delta/final_recommendations"

#========= USER HISTORY: filter out already rated books + find content based candidates... =============

def read_user_history(spark):
    
    history = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="ratings",
            properties=JDBC_PROPERTIES
        )
        .select(
            F.col("user_id"),
            F.col("isbn").alias("rated_isbn"),
            F.col("rating")
        )
        .filter(F.col("rating") >= 7)  # Only consider highly-rated books (7+)
        .select("user_id", "rated_isbn")
        .dropDuplicates()
    )
    
    return history

#========LOAD DATA FROM DELTA =============

def load_content_sim (spark):
    content_sim = (
        spark.read
            .format("delta")
            .load(DELTA_CONTENT_SIMS)
            .select(
                F.col("isbn_a"),
                F.col("isbn_b"),
                F.col("similarity_score").cast(DoubleType())
            )
            .filter(F.col("similarity_score") >= MIN_CONTENT_SIMILARITY)
            .dropna()
        )
        sims_reverse = sim.select(
            F.col("isbn_b").alias("isbn_a"),
            F.col("isbn_a").alias("isbn_b"),
            F.col("similarity_score")
        )

        all_sims = sims.union(sims_reverse)

        return all_sims

def load_ALS_score(spark):

    als_recs = (
        spark.read
            .format("delta")
            .load(DELTA_ALS_RECS)
            .select(
                F.col("user_id"),
                F.col("isbn"),
                F.col("als_sore").cast(DoubleType())
            )
            .dropna(subset=["user_id", "isbn", "als_score"])
        )
        return als_recs

#======= FINAL SCORE ==================================

def hybrid_score(user_history, als_recs):
    #take user rated book(from history)
    #find similar book(contatn sim)
    #agg - max similarity across all users rated books...

    content_scores = (
        user_history.alias("h")
        .join(
            content_sim.alias("s"),
            F.col("h.rated_isbn") == F.col("s.isbn_a"),
            "inner"
        )
        .groupBy(
            F.col("h.user_id").alias("user_id"),
            F.col("s.isbn_b").alias("isbn")  # Candidate book
        )
        .agg(
            F.max("s.similarity_score").alias("content_score")  # Max similarity
        )
    )
    
    return content_scores


def filter_already_rated(hybrid_df, user_history):

    # Anti-join: keep only books NOT in user's history
    filtered = (
        hybrid_df.alias("h")
        .join(
            user_history.alias("hist"),
            (F.col("h.user_id") == F.col("hist.user_id")) &
            (F.col("h.isbn") == F.col("hist.rated_isbn")),
            "left_anti"  # Anti-join: keep rows with no match
        )
    )
    
    before_count = hybrid_df.count()
    after_count = filtered.count()
    
    return filtered


# ============ STEP 7: Select Top-N Per User ============
def select_top_n_per_user(hybrid_df, top_n=N_RECOMMENDATIONS):
    
    window = Window.partitionBy("user_id").orderBy(F.desc("hybrid_score"))
    
    top_recs = (
        hybrid_df
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
        .withColumn("generated_at", F.current_timestamp())
        .select(
            "user_id",
            "isbn",
            "hybrid_score",
            "als_score",
            "content_score",
            "rank",
            "generated_at"
        )
    )
    
    return top_recs


# ============ SAVE TO DELTA ============
def save_final_recommendations(df):
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(DELTA_FINAL_RECS)
    
    print(" final recs aved to DELTA")


# ============ OPTIONAL: Save to PostgreSQL Cache ============
def save_to_postgres_cache(df):
    """
    Optional: Write top-100 recommendations back to PostgreSQL
    for even faster API serving (vs reading from Delta).
    """
    
    # Prepare for PostgreSQL
    pg_recs = df.select(
        "user_id",
        "isbn",
        F.col("hybrid_score").alias("score"),
        "rank",
        "generated_at"
    )
    
    # Write to PostgreSQL
    pg_recs.write \
        .jdbc(
            url=JDBC_URL,
            table="recommendations_cache",
            mode="overwrite",
            properties=JDBC_PROPERTIES
        )
    
    print("   ✓ Saved to PostgreSQL!")


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
        # 1. Load data
        user_history = read_user_history(spark)
        als_recs = read_als_recommendations(spark)
        content_sims = read_content_similarities(spark)
        
        # 2. Compute content-based scores
        content_scores = compute_content_scores(user_history, content_sims)
        
        # 3. Combine ALS + content
        hybrid = compute_hybrid_scores(als_recs, content_scores, alpha=HYBRID_ALPHA)
        
        # 4. Filter already-rated books
        filtered = filter_already_rated(hybrid, user_history)
        
        # 5. Select top-N per user
        final_recs = select_top_n_per_user(filtered, top_n=N_RECOMMENDATIONS)
        
        # 6. Save results
        save_final_recommendations(final_recs)
        
        # Optional: Save to PostgreSQL for faster serving
        # save_to_postgres_cache(final_recs)

        print("✅ HYBRID RECOMMENDATION PIPELINE COMPLETE!")

        print(f"   Alpha (collaborative weight): {HYBRID_ALPHA}")
        print(f"   Recommendations per user: {N_RECOMMENDATIONS}")
        print(f"   Total recommendations: {final_recs.count():,}")
        print(f"   Unique users: {final_recs.select('user_id').distinct().count():,}")
        
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



