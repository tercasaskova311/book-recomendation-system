"""
Complete Content-Based Recommendation Pipeline
1. similarities from description:
-Load books from a database
-Convert textual descriptions into numeric embeddings (vectors)
-Cache embeddings to avoid recomputation
-Later (in another step), combine embeddings with other features (categories, metadata)
-Compute similarities using LSH
-Save everything to Delta Lake (efficient, scalable storage)
2. Get other features + Apply weights (70% description, 20% categories, 10% metadata)
3. Compute pairwise similarity using LSH
4. Save features and similarities to Delta Lake
"""
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from pyspark.sql.types import DoubleType

import os
import sys
import re
import numpy as np
from dotenv import load_dotenv
import traceback
from sentence_transformers import SentenceTransformer
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, size, lower, array_join, lit, array, length,
    regexp_replace, trim, explode, split, min, max, udf
)
from pyspark.sql.types import (
    ArrayType, StringType, FloatType, DoubleType, 
    StructType, StructField
)
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, OneHotEncoder, VectorAssembler,
    BucketedRandomProjectionLSH
)
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml import Pipeline
load_dotenv()

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import (
    JDBC_URL, DELTA_DESCRIPTION_EMBEDDINGS, DELTA_SIMILARITIES, DELTA_SIM_FEATURES,JDBC_PROPERTIES,
    MIN_DESCRIPTION_LENGTH,CATEGORY_TOP_N,POSTGRES_JDBC_JAR,
    SIMILARITY_THRESHOLD, TOP_K_SIMILAR, MIN_DESCRIPTION_LENGTH, CATEGORY_TOP_N
)
import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from common.spark_session import get_spark_session

# =========== DATA LOADING ============================================
def load_books(spark): 
    df = spark.read \
        .jdbc(
            url=JDBC_URL,
            table="books",
            properties=JDBC_PROPERTIES
        )
    return (
        df
        .filter(col("description").isNotNull())
        .filter(length(col("description")) >= MIN_DESCRIPTION_LENGTH)
        .select("isbn", "description", "categories", "page_count", "language")  
    )

def load_existing_embeddings(spark):
    if not spark._jsparkSession.catalog().tableExists("delta.`{}`".format(DELTA_DESCRIPTION_EMBEDDINGS)):
        return None

    return spark.read.format("delta").load(DELTA_DESCRIPTION_EMBEDDINGS)

def get_descriptions_to_embed(_, books_df):
    return books_df

# ========= DESCRIPTIONS ===============================================

def embed_descriptions(spark, df):
    model = SentenceTransformer("all-MiniLM-L6-v2")

    pdf = df.toPandas()

    embeddings = model.encode(
        pdf["description"].tolist(),
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=True
    )

    pdf["embedding"] = embeddings.tolist()

    spark_df = spark.createDataFrame(pdf[["isbn", "embedding"]])

    to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())

    return spark_df.withColumn(
        "description_embedding",
        to_vector(col("embedding"))
    ).drop("embedding")

def save_embeddings(spark, df):
    if df is None:
        return

    df.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_DESCRIPTION_EMBEDDINGS)

def load_all_embeddings(spark):
    return spark.read.format("delta").load(DELTA_DESCRIPTION_EMBEDDINGS) \
        .select("isbn", "description_embedding")


# ======== CATEGORY ENCODING ============================================
def sanitize_category_name(category):
    safe_name = (category
        .replace(" ", "_")
        .replace("-", "_")
        .replace("&", "and")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace("'", "")
    )
    return re.sub(r'[^a-z0-9_]', '', safe_name)

def process_categories(df):

    df_cat = df.withColumn("categories_clean", when((col("categories").isNotNull()) & (size(col("categories")) > 0), col("categories")).otherwise(array(lit("Uncategorized"))))    
    df_cat = df_cat.withColumn("categories_str", lower(array_join(col("categories_clean"), ",")))
    
    #Find top N categories by counting    
    top_categories_df = df_cat \
        .withColumn("category", explode(split(col("categories_str"), ","))) \
        .withColumn("category", trim(col("category"))) \
        .filter(
            (col("category") != "") & 
            (col("category") != "uncategorized")
        ) \
        .groupBy("category") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(CATEGORY_TOP_N)
    
    top_categories_raw = [row["category"] for row in top_categories_df.collect()]
    top_categories = [
        (cat, sanitize_category_name(cat))
        for cat in top_categories_raw
    ]

    # Create all columns in one pass (more efficient than loop)
    for original_cat, safe_name in top_categories:
        df_cat = df_cat.withColumn(
            f"cat_{safe_name}", 
            when(col("categories_str").contains(original_cat), 1.0).otherwise(0.0)
        )
    
    # Extract column names (no need to sanitize again)
    cat_cols = [f"cat_{safe}" for _, safe in top_categories]

    return df_cat.select("isbn", *cat_cols), cat_cols

# ======== NORMALIZE PAGE COUNT ============================================
def normalize_page_count(df):  
    df_pages = df.filter(
        (col("page_count").isNotNull()) & 
        (col("page_count") > 0) & 
        (col("page_count") < 5000)
    ).withColumn("page_count", col("page_count").cast("double"))

    stats = df_pages.agg(
        min("page_count").alias("min_val"),
        max("page_count").alias("max_val")
    ).collect()[0]

    min_val = stats["min_val"]
    max_val = stats["max_val"]


    df_normalized = df_pages.withColumn(
        "page_count_normalized",
        (col("page_count") - min_val) / (max_val - min_val)
    )

    return df_normalized.select("isbn", "page_count_normalized")
   
# ========== ENCODE LANGUAGE =============================================

def encode_language(df):
    
    df_lang = df.filter(col("language").isNotNull())
    
    indexer = StringIndexer(inputCol="language", outputCol="language_index")
    
    encoder = OneHotEncoder(
        inputCols=["language_index"],
        outputCols=["language_encoded"],
        dropLast=False
    )

    pipeline = Pipeline(stages=[
        StringIndexer(inputCol="language", outputCol="language_index"), 
        OneHotEncoder(
            inputCols=["language_index"],
            outputCols=["language_encoded"],
            dropLast=False
        )  
    ]) 

    model = pipeline.fit(df_lang) #fit stringindexer = what lang exist + fit onehotencoder: how many cat?
    df_encoded = model.transform(df_lang) #add lang index column => convert indices to one-hot vector
    
    return df_encoded.select("isbn", "language_encoded") #select only language vector col

# =================== COMBINE EVERYTHING =========================================

def combine_features(df, all_embeddings, cat_df, page_df, lang_df, tfidf_weight=0.7, category_weight=0.2, metadata_weight=0.1):

    df_combined = df \
        .join(all_embeddings, "isbn", "inner") \
        .join(cat_df, "isbn", "inner") \
        .join(page_df, "isbn", "inner") \
        .join(lang_df, "isbn", "inner")
    
    cat_cols = [c for c in cat_df.columns if c.startswith("cat_")]    
    n_categories = len(cat_cols)  

    # TF-IDF or embedding size
    sample_vec = df_combined.select("description_embedding").first()
    n_tfidf = sample_vec["description_embedding"].size

    # Metadata size: page_count (1) + language_encoded
    n_language = df_combined.select("language_encoded").first()["language_encoded"].size
    n_metadata = 1 + n_language

    assembler = VectorAssembler(
        inputCols=cat_cols + ["description_embedding", "page_count_normalized", "language_encoded"],
        outputCol="features_unweighted",
        handleInvalid="skip"  # Skip rows with invalid values
    )

    df_assembled = assembler.transform(df_combined) 
    

    def create_weighted_features(v):
        if v is None:
            return None
        arr = np.array(v.toArray())
        arr[0:n_categories] *= category_weight
        arr[n_categories:n_categories+n_tfidf] *= tfidf_weight
        arr[n_categories+n_tfidf:] *= metadata_weight
        return Vectors.dense(arr.tolist())

    weight_udf = udf(create_weighted_features, VectorUDT())
    df_weighted = df_assembled.withColumn("features", weight_udf("features_unweighted"))

    return df_weighted.select("isbn", "features")
            
# ========== COSINE SIMILARITY COMPUTATION ===========================

def compute_pairwise_similarity_lsh(spark, df, top_k=50): #using LSH (Locality-Sensitive Hashing) bucketedRandomProjectionLSH works well for cosine similarity
    
    lsh = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol="hashes",
        bucketLength=2.0,  # Tune this: smaller = more accurate but slower
        numHashTables=3     # More tables = more accurate but slower
    )
    
    model = lsh.fit(df)    
    
    #Use approxSimilarityJoin instead of collect + loop
    # This runs in parallel across the cluster!
    similarities = model.approxSimilarityJoin(
        df, 
        df, 
        threshold=2.0,  # Euclidean distance threshold
        distCol="distance"
    )
    
    # Filter out self-joins and convert to similarity score
    similarities = similarities \
        .filter(col("datasetA.isbn") < col("datasetB.isbn")) \
        .withColumn(
            "similarity_score",
            (1 - (col("distance") ** 2 / 2)).cast(DoubleType())
        ) \
        .select(
            col("datasetA.isbn").alias("isbn_a"),
            col("datasetB.isbn").alias("isbn_b"),
            col("similarity_score")
        ) \
        .filter(col("similarity_score") >= SIMILARITY_THRESHOLD)
    
    print(f"   Filtering to top-{top_k} per book...")
    
    # Keep top-K per book (both directions)
    window_a = Window.partitionBy("isbn_a").orderBy(desc("similarity_score"))
    window_b = Window.partitionBy("isbn_b").orderBy(desc("similarity_score"))
    
    top_a = similarities \
        .withColumn("rank", row_number().over(window_a)) \
        .filter(col("rank") <= top_k) \
        .drop("rank")
    
    top_b = similarities \
        .select(
            col("isbn_b").alias("isbn_a"),
            col("isbn_a").alias("isbn_b"),
            col("similarity_score")
        ) \
        .withColumn("rank", row_number().over(window_b)) \
        .filter(col("rank") <= top_k) \
        .drop("rank")
    
    # Union both directions
    final_similarities = top_a.union(top_b).distinct()
    
    count = final_similarities.count()
    print(f"   ✅ Computed {count:,} similarity pairs")
    
    return final_similarities

# =========== SAVE TO POSTGRESQL ==========================================

def save_features(df):
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(DELTA_SIM_FEATURES)
    print(" Features saved to Delta Lake")

# ========== SAVE RESULTS ============================================

def save_similarities(df, output_path=DELTA_SIMILARITIES):
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(DELTA_SIMILARITIES)
    
    print("Similarities saved to Delta Lake")

# ========== MAIN PIPELINE ===========================================
def main():   
    spark = get_spark_session(
        app_name="BookFeaturesAndSimilarity",
        enable_delta=True,
        extra_conf={
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "200"
        }
    )
    
    spark.sparkContext.setLogLevel("WARN")
        
    try:
        books_df = load_books(spark)
        existing_embeddings = load_existing_embeddings(spark)

        if existing_embeddings is not None:
            books_to_embed = books_df.join(
                existing_embeddings, on="isbn", how="left_anti"
            )  # keep only books not in embeddings
        else:
            books_to_embed = books_df

        new_embeddings = embed_descriptions(spark, books_to_embed)
        save_embeddings(spark, new_embeddings)

        all_embeddings = load_all_embeddings(spark)

        cat_df, cat_cols = process_categories(books_df)
        page_df = normalize_page_count(books_df)
        lang_df = encode_language(books_df)
        
        df_final = combine_features(
        books_df, all_embeddings, cat_df, page_df, lang_df,
        tfidf_weight=0.7, category_weight=0.2, metadata_weight=0.1
        )
        save_features(df_final)

        similarities = compute_pairwise_similarity_lsh(spark, df_final, top_k=TOP_K_SIMILAR)
        save_similarities(similarities)
       
        print("SIMILARITY COMPUTATION COMPLETE!")

        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n Spark session stopped")

if __name__ == "__main__":  
    main()
