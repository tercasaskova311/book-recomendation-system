"""
Complete Content-Based Recommendation Pipeline
1. Extract features from books (TF-IDF, categories, metadata)
2. Apply weights (70% description, 20% categories, 10% metadata)
3. Compute pairwise similarity using LSH
4. Save features and similarities to Delta Lake
"""

import os
import sys
import re
import numpy as np
from dotenv import load_dotenv
import traceback

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
    JDBC_URL, 
    JDBC_PROPERTIES,
    TF_IDF_NUM_FEATURES,
    MIN_DESCRIPTION_LENGTH,
    CATEGORY_TOP_N,
    POSTGRES_JDBC_JAR  
)
from common.spark_session import get_spark_session

# =========== DATA LOADING ============================================

def load_books (spark): 
    df = spark.read \
        .jdbc(
            url=JDBC_URL,
            table="books",
            properties=JDBC_PROPERTIES
        )
    return df

# ========= DESCRIPTIONS ===============================================

def process_descriptions(df):   
    df_with_desc = df.filter(
        (col("description").isNotNull()) & 
        (length(col("description")) >= MIN_DESCRIPTION_LENGTH)
    )   

    df_clean = df_with_desc.withColumn(
        "description_clean",
        lower(regexp_replace(col("description"), "[^a-zA-Z\\s]", ""))
    )
    
    tokenizer = Tokenizer(inputCol="description_clean", outputCol="words")
    
    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="words_filtered")
    
    hashing_tf = HashingTF(
        inputCol="words_filtered",
        outputCol="raw_features",
        numFeatures=TF_IDF_NUM_FEATURES
    )
    
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    
    pipeline = Pipeline(stages=[
        tokenizer,
        stop_words_remover,
        hashing_tf,
        idf
    ])
    
    model = pipeline.fit(df_clean)
    df_tfidf = model.transform(df_clean)
    
    return df_tfidf.select("isbn", "tfidf_features")

# ======== CATEGORY ENCODING ============================================
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
    
    top_categories = [] 
    for row in top_categories_df.collect():
        top_categories.append(row.category)    
    
    for category in top_categories:  # ← Loop through Python list, not Spark column!
        safe_name = (category
            .replace(" ", "_")
            .replace("-", "_")
            .replace("&", "and")
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
            .replace("'", "")
        )
        safe_name = re.sub(r'[^a-z0-9_]', '', safe_name)
        df_cat = df_cat.withColumn(f"cat_{safe_name}", when(col("categories_str").contains(category), 1.0).otherwise(0.0))

    # Step 5: Create list of column names
    cat_cols = []
    for category in top_categories:
        safe_name = (category
            .replace(" ", "_")
            .replace("-", "_")
            .replace("&", "and")
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
            .replace("'", "")
        )
        safe_name = re.sub(r'[^a-z0-9_]', '', safe_name)
        cat_cols.append(f"cat_{safe_name}")
    
    return df_cat.select("isbn", *cat_cols)

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

    pipeline = Pipeline(stages=[indexer, encoder]) #instruction

    model = pipeline.fit(df_lang) #fit stringindexer = what lang exist + fit onehotencoder: how many cat?
    df_encoded = model.transform(df_lang) #add lang index column => convert indices to one-hot vector
    
    return df_encoded.select("isbn", "language_encoded") #select only language vector col

# =================== COMBINE EVERYTHING =========================================

def combine_features(df, tfidf_df, cat_df, page_df, lang_df, tfidf_weight=0.7, category_weight=0.2, metadata_weight=0.1):

    df_combined = df \
        .join(tfidf_df, "isbn", "left") \
        .join(cat_df, "isbn", "left") \
        .join(page_df, "isbn", "left") \
        .join(lang_df, "isbn", "left")
    
    cat_cols = [c for c in cat_df.columns if c.startswith("cat_")]    
    n_categories = len(cat_cols)  

    sample_tfidf = tfidf_df.select("tfidf_features").first()
    if sample_tfidf is None:
        raise ValueError("tfidf_df is empty")
    n_tfidf = sample_tfidf["tfidf_features"].size

    
    sample_lang = lang_df.select("language_encoded").first()
    if sample_lang is None:
        raise ValueError("lang_df is empty")
    n_language = sample_lang["language_encoded"].size

    n_page = 1  
    n_metadata = n_page + n_language  

    assembler = VectorAssembler(
        inputCols=cat_cols + ["tfidf_features", "page_count_normalized", "language_encoded"],
        outputCol="features_unweighted",
        handleInvalid="skip"  # Skip rows with invalid values
    )

    df_assembled = assembler.transform(df_combined) 
    

    def create_weighted_features(features_vector):

        if features_vector is None:
            return None
            
        arr = np.array(features_vector.toArray())  
        cat_start = 0
        cat_end = n_categories
            
        tfidf_start = cat_end
        tfidf_end = tfidf_start + n_tfidf
            
        meta_start = tfidf_end
        meta_end = meta_start + n_metadata
            
        arr[cat_start:cat_end] *= category_weight     
        arr[tfidf_start:tfidf_end] *= tfidf_weight    
        arr[meta_start:meta_end] *= metadata_weight   
            
        return Vectors.dense(arr.tolist())
        
    weight_udf = udf(create_weighted_features, VectorUDT())
        
    df_weighted = df_assembled.withColumn(
        "features",
        weight_udf("features_unweighted")
    )
        
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
    similarities = []
    books = df.select("isbn", "features").collect()
    total_books = len(books)
    
    for i, book in enumerate(books):
        if i % 100 == 0:
            print(f"   Processing book {i}/{total_books}...")
        
        isbn = book['isbn']
        features = book['features']
        
        neighbors = model.approxNearestNeighbors(
            df, 
            features, 
            top_k + 1,  # +1 because it includes itself
            distCol="distance"
        )
        
        # LSH returns Euclidean distance; convert to cosine similarity = similarity ≈ 1 - (distance² / 2)
        neighbor_data = neighbors.select("isbn", "distance") \
            .filter(col("isbn") != isbn) \
            .limit(top_k) \
            .collect()
        
        similar_books = [
            {"isbn": n['isbn'], "similarity": float(1 - (n['distance']**2 / 2))}
            for n in neighbor_data
        ]
        
        similarities.append((isbn, similar_books))
    
    # Create DataFrame
    schema = StructType([
        StructField("isbn", StringType(), False),
        StructField("similar_books", ArrayType(
            StructType([
                StructField("isbn", StringType(), False),
                StructField("similarity", FloatType(), False)
            ])
        ), False)
    ])
    
    result_df = spark.createDataFrame(similarities, schema)    
    return result_df

# =========== SAVE TO POSTGRESQL ==========================================

def save_features(df):
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save("/Users/terezasaskova/Desktop/book-recomendation-system/delta/book_features")

    print(" Features saved to Delta Lake")

# ========== SAVE RESULTS ============================================

def save_similarities(df, output_path="delta/book_similarities"):
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/Users/terezasaskova/Desktop/book-recomendation-system/delta/similarities")
    
    print("Similarities saved to Delta Lake")

# ========== MAIN PIPELINE ===========================================

def main():   
    spark = get_spark_session(
        app_name="BookFeaturesAndSimilarity",
        enable_delta=True,
        extra_conf={
            "spark.sql.shuffle.partitions": "50"
        }
    )
        
    try:
        df = load_books(spark)
        tfidf_df = process_descriptions(df)
        cat_df = process_categories(df)
        page_df = normalize_page_count(df)
        lang_df = encode_language(df)
        
        df_final = combine_features(df, tfidf_df, cat_df, page_df, lang_df)
        save_features(df_final)
            
        similarities = compute_pairwise_similarity_lsh(spark, df_final, top_k=50)        
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
