"""
content similarity feature extraction (Transforms book data into ML-ready features for content-based recommendations)
n.pages, description, category, language => into one vector space
"""


import os
import sys
import re
import traceback
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    when, 
    size, 
    lower, 
    array_join, 
    lit,           
    array,        
    length,
    regexp_replace,
    trim,
    explode,       
    split,
    min,
    max,
    substring      
)
from pyspark.sql.types import ArrayType, StringType, FloatType, DoubleType
from pyspark.ml.feature import (
    Tokenizer, 
    StopWordsRemover, 
    HashingTF, 
    IDF,
    MinMaxScaler,
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)
from pyspark.ml import Pipeline

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import (
    JDBC_URL, 
    JDBC_PROPERTIES,
    TF_IDF_NUM_FEATURES,
    MIN_DESCRIPTION_LENGTH,
    CATEGORY_TOP_N,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER,
    POSTGRES_JDBC_JAR
)
from delta import configure_spark_with_delta_pip

# ============= SPARK SESSION ===============================================
def create_spark_session(): #with postgresdriver ... 
    builder = (
        SparkSession.builder 
        .appName("BookRecommendation-conntent-similarity") 
        .config("spark.jars", POSTGRES_JDBC_JAR) 
        .config("spark.driver.extraClassPath", POSTGRES_JDBC_JAR) \
        .config("spark.executor.extraClassPath", POSTGRES_JDBC_JAR)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)  
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

# =========== DATA LOADING ============================================

def load_books_from_postgres(spark): #Loading books from PostgreSQL
    df = spark.read \
        .jdbc(
            url=JDBC_URL,
            table="books",
            properties=JDBC_PROPERTIES
        )
    
    print(f" Loaded {df.count()} books")    
    return df

# ========= DESCRIPTIONS ===============================================

def process_descriptions(df): #book descriptions into TF-IDF vectors - Filter books with descriptions - Clean text - Tokenize - stop words - Compute TF-IDF    
    df_with_desc = df.filter(
        (col("description").isNotNull()) & 
        (length(col("description")) >= MIN_DESCRIPTION_LENGTH)
    )    
    df_clean = df_with_desc.withColumn(
        "description_clean",
        lower(regexp_replace(col("description"), "[^a-zA-Z\\s]", ""))
    )
    
    tokenizer = Tokenizer(inputCol="description_clean", outputCol="words")
    
    stop_words_remover = StopWordsRemover(
        inputCol="words",
        outputCol="words_filtered"
    )
    
    hashing_tf = HashingTF(
        inputCol="words_filtered",
        outputCol="raw_features",
        numFeatures=TF_IDF_NUM_FEATURES
    )
    
    idf = IDF(
        inputCol="raw_features",
        outputCol="tfidf_features"
    )
    
    pipeline = Pipeline(stages=[
        tokenizer,
        stop_words_remover,
        hashing_tf,
        idf
    ])
    
    model = pipeline.fit(df_clean)
    df_tfidf = model.transform(df_clean)
    
    print("TF-IDF processing complete")
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
    
    indexer = StringIndexer(
        inputCol="language",
        outputCol="language_index"
    )
    
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

def combine_features(df, tfidf_df, cat_df, page_df, lang_df):
    
    df_combined = df \
        .join(tfidf_df, "isbn", "left") \
        .join(cat_df, "isbn", "left") \
        .join(page_df, "isbn", "left") \
        .join(lang_df, "isbn", "left")
    
    cat_cols = [c for c in cat_df.columns if c.startswith("cat_")]

    # Assemble all features into one vector
    assembler = VectorAssembler(
        inputCols=cat_cols + ["tfidf_features", "page_count_normalized", "language_encoded"],
        outputCol="features",
        handleInvalid="skip"  # Skip rows with invalid values
    )
    
    df_final = assembler.transform(df_combined)    
    return df_final.select("isbn", "features")

# =========== SAVE TO POSTGRESQL ==========================================

def save_to_delta(df):
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save("/Users/terezasaskova/Desktop/book-recomendation-system/delta/book_features")


    print(" Features saved to Delta Lake")

# ================= MAIN ===========================================

def main():    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce verbose logging
    
    try:
        df = load_books_from_postgres(spark)

        tfidf_df = process_descriptions(df)
        cat_df = process_categories(df)
        page_df = normalize_page_count(df)
        lang_df = encode_language(df)
        
        df_final = combine_features(df, tfidf_df, cat_df, page_df, lang_df)
        
        save_to_delta(df_final)
        
        print("COMPLETE!")
    
    except Exception as e:
        print(f"❌ Error: {e}")       
        traceback.print_exc()         

    finally:
        spark.stop()

if __name__ == "__main__":
    main()


#spark.read.format("delta").load("/delta/book_features").show(5)

