#Transforms book data into ML-ready features for content-based recommendations
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array_join, when, lower, regexp_replace, length, size, trim, explore, split
from pyspark.sql.types import ArrayType, StringType, FloatType, IntegerType, DoubleType
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
import os
from dotenv import load_dotenv
load_dotenv()
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# CONFIG 
import (
    JDBC_URL, 
    JDBC_PROPERTIES,
    TF_IDF_NUM_FEATURES,
    MIN_DESCRIPTION_LENGTH,
    CATEGORY_TOP_N,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    POSTGRES_JDBC_JAR
)

# ============= SPARK SESSION ===============================================
def create_spark_session(): #with postgresdriver ... 
    return SparkSession.builder \
        .appName("BookRecommendation-conntent-similarity") \
        .config("spark.jars", POSTGRES_JDBC_JAR) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \ 
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .getOrCreate()

#rn i have 4g driver memory & 4g for each executor - this should be enought
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

# ========= FEATURE 1: TF-IDF ON DESCRIPTIONS ===============================================

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

# ======== FEATURE 2: CATEGORY ENCODING ============================================
def process_categories(df): # One-hot encode book categories    
    df_cat = df.withColumn(
        "categories_clean",
        when(
            (col("categories").isNotNull()) & (size(col("categories")) > 0),
            col("categories")
        ).otherwise(array(lit("Uncategorized")))  # Default for empty
    )
    
    df_cat = df_cat.withColumn(
        "categories_str",
        lower(array_join(col("categories_clean"), ","))
    )
    # Step 5: Create binary features
    # Clean category names for column names
    for category in categories:
        safe_name = (category
            .replace(" ", "_")
            .replace("-", "_")
            .replace("&", "and")
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
            .replace("'", "")
        )
        
        safe_name = regexp_replace(safe_name, "[^a-z0-9_]", "")
        
        df_cat = df_cat.withColumn(
            f"cat_{safe_name}",
            when(col("categories_str").contains(category), 1.0).otherwise(0.0)
        )
    
    cat_cols = [
        f"cat_{category.replace(' ', '_').replace('-', '_').replace('&', 'and').replace(',', '').replace('(', '').replace(')', '').replace('\', '')}"
        for category in categories
    ]
    
    # Clean column names (in case there are still issues)
    cat_cols = [regexp_replace(col, "[^a-z0-9_]", "") for col in cat_cols]
    
    assembler = VectorAssembler(
        inputCols=cat_cols,
        outputCol="category_features",
        handleInvalid="keep"  # Keep rows with null values
    )
    
    df_cat_final = assembler.transform(df_cat)    
    return df_cat_final.select("isbn", "category_features")

# ======== FEATURE 3: NORMALIZE PAGE COUNT ============================================

def normalize_page_count(df):    
    df_pages = df.filter(
        (col("page_count").isNotNull()) & 
        (col("page_count") > 0) & 
        (col("page_count") < 5000)
    ).withColumn("page_count", col("page_count").cast("double"))
    
    assembler = VectorAssembler(
        inputCols=["page_count"],
        outputCol="page_count_vec"
    )
    
    df_pages = assembler.transform(df_pages)
    
    scaler = MinMaxScaler(
        inputCol="page_count_vec",
        outputCol="page_count_normalized"
    )
    
    scaler_model = scaler.fit(df_pages)
    df_pages_scaled = scaler_model.transform(df_pages)    
    return df_pages_scaled.select("isbn", "page_count_normalized")

# ========== FEATURE 5: ENCODE LANGUAGE =============================================

def encode_language(df):
    
    df_lang = df.filter(col("language").isNotNull())
    
    # converts strings to indices
    indexer = StringIndexer(
        inputCol="language",
        outputCol="language_index"
    )
    
    # One-hot encoder
    encoder = OneHotEncoder(
        inputCols=["language_index"],
        outputCols=["language_encoded"]
    )
    
    pipeline = Pipeline(stages=[indexer, encoder])
    model = pipeline.fit(df_lang)
    df_lang_encoded = model.transform(df_lang)    
    return df_lang_encoded.select("isbn", "language_encoded")

# =================== COMBINE EVERYTHING =========================================

def combine_features(df, tfidf_df, cat_df, page_df, year_df, lang_df):
    """Combine all features into single vector per book"""    
    df_combined = df.select("isbn") \
        .join(tfidf_df, "isbn", "left") \
        .join(cat_df, "isbn", "left") \
        .join(page_df, "isbn", "left") \
        .join(lang_df, "isbn", "left")
    
    # Fill nulls with zeros (for books missing some features)
    # This is handled by VectorAssembler's handleInvalid parameter
    
    # Assemble all features into one vector
    assembler = VectorAssembler(
        inputCols=[
            "tfidf_features",      # Description similarity (most important)
            "category_features",   # Genre similarity
            "page_count_normalized",
            "language_encoded"
        ],
        outputCol="features",
        handleInvalid="skip"  # Skip rows with invalid values
    )
    
    df_final = assembler.transform(df_combined)
    
    print(f"Combined features for {df_final.count()} books")
    
    return df_final.select("isbn", "features")

# =========== SAVE TO POSTGRESQL ==========================================

def save_to_postgres(df, spark):
    """Save feature vectors to book_features table"""    
    # Convert vector to array for PostgreSQL
    vector_to_array = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
    df_save = df.withColumn("feature_array", vector_to_array(col("features")))
    
    # Save to PostgreSQL
    df_save.select("isbn", "feature_array") \
        .write \
        .jdbc(
            url=JDBC_URL,
            table="book_features",
            mode="overwrite",  # Replace existing features
            properties=JDBC_PROPERTIES
        )
    
    print(" Features saved to book_features table")

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
        df_final = combine_features(df, tfidf_df, cat_df, page_df, year_df, lang_df)
        save_to_postgres(df_final, spark)
        
        print("COMPLETE!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()




#spark processing part 1.

"""
Input:  books table (descriptions, categories, metadata)     
Process:                                                     
1. TF-IDF on book descriptions (Spark MLlib)             
 - Tokenization → Stop words removal → TF-IDF vectors    
- Primary signal for content similarity                 
2. One-hot encode categories (Fiction, Mystery, etc.)       
3. Normalize page_count (min-max scaling)                   
4. Normalize published_year                                 
5. Encode language (categorical)                            
Output: book_features table                                   
- Combined feature vector per book                          
- Used for content-based similarity   
"""
#from postgres load db => table books  - table conntent here: 
#for the similarity content wise I use - describtion - idk if sentence transformer is the best?? - since the description is usually a bit vague...
# I also use category for sure
#also author
#language
