import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from common.spark_session import get_spark_session
from config import JDBC_URL, JDBC_PROPERTIES, DELTA_SIMILARITIES, DELTA_FINAL_RECS

# ============ SPARK SESSION ============
@st.cache_resource
def get_spark():
    return get_spark_session(
        app_name="StreamlitApp",
        enable_delta=True
    )

# ============ LOAD DATA ============
@st.cache_data
def load_books(_spark):
    """Load books from PostgreSQL (not Delta Lake)"""
    try:
        books_df = _spark.read \
            .jdbc(
                url=JDBC_URL,
                table="books",
                properties=JDBC_PROPERTIES
            ) \
            .select("isbn", "title", "description", "authors", 
                   "categories", "page_count", "language")
        
        return books_df.toPandas()
    except Exception as e:
        st.error(f"Error loading books: {e}")
        return pd.DataFrame()

@st.cache_data
def load_user_recommendations(_spark, user_id):
    """Load user recommendations from Delta Lake"""
    try:
        recs_df = _spark.read \
            .format("delta") \
            .load(DELTA_FINAL_RECS) \
            .filter(F.col("user_id") == user_id) \
            .orderBy(F.col("rank"))
        
        return recs_df.toPandas()
    except Exception as e:
        st.error(f"Error loading recommendations: {e}")
        return pd.DataFrame()

@st.cache_data
def load_similar_books(_spark, isbn):
    """Load similar books from Delta Lake"""
    try:
        sims = _spark.read.format("delta").load(DELTA_SIMILARITIES)
        
        similar = sims.filter(
            (F.col("isbn_a") == isbn) | (F.col("isbn_b") == isbn)
        ).withColumn(
            "similar_isbn",
            F.when(F.col("isbn_a") == isbn, F.col("isbn_b")).otherwise(F.col("isbn_a"))
        ).select("similar_isbn", "similarity_score") \
         .orderBy(F.desc("similarity_score")) \
         .limit(10)
        
        return similar.toPandas()
    except Exception as e:
        st.error(f"Error loading similar books: {e}")
        return pd.DataFrame()

@st.cache_data
def load_statistics(_spark):
    """Load dataset statistics"""
    try:
        books_count = _spark.read.jdbc(
            JDBC_URL, "books", properties=JDBC_PROPERTIES
        ).count()
        
        ratings_df = _spark.read.jdbc(
            JDBC_URL, "ratings", properties=JDBC_PROPERTIES
        )
        ratings_count = ratings_df.count()
        avg_rating = ratings_df.agg(F.avg("rating")).first()[0]
        
        users_count = _spark.read.jdbc(
            JDBC_URL, "users", properties=JDBC_PROPERTIES
        ).count()
        
        return {
            "books": books_count,
            "ratings": ratings_count,
            "avg_rating": round(avg_rating, 2) if avg_rating else 0,
            "users": users_count
        }
    except Exception as e:
        st.error(f"Error loading statistics: {e}")
        return {"books": 0, "ratings": 0, "avg_rating": 0, "users": 0}

# ============ MAIN APP ============
def main():
    st.set_page_config(
        page_title="Book Recommendations",
        page_icon="📚",
        layout="wide"
    )
    
    st.title("📚 Book Recommendation System")
    st.markdown("---")
    
    # Initialize Spark
    try:
        spark = get_spark()
    except Exception as e:
        st.error(f"Failed to initialize Spark: {e}")
        return
    
    # Sidebar - Choose mode
    mode = st.sidebar.radio(
        "Choose Mode:",
        ["🎯 Personalized Recommendations", "🔍 Find Similar Books", "📊 Explore Data"]
    )
    
    # ======== MODE 1: Personalized Recommendations ========
    if mode == "🎯 Personalized Recommendations":
        st.header("Get Personalized Recommendations")
        
        user_id = st.number_input(
            "Enter User ID:",
            min_value=1,
            value=276725,
            step=1
        )
        
        if st.button("Get Recommendations"):
            with st.spinner("Loading recommendations..."):
                recs_df = load_user_recommendations(spark, user_id)
                
                if recs_df.empty:
                    st.warning(f"No recommendations found for user {user_id}")
                else:
                    st.success(f"Found {len(recs_df)} recommendations!")
                    
                    # Display top 10
                    st.subheader("Top 10 Recommendations")
                    for idx, row in recs_df.head(10).iterrows():
                        with st.expander(f"#{row['rank']} - {row['isbn']} (Score: {row['hybrid_score']:.2f})"):
                            col1, col2 = st.columns([1, 2])
                            with col1:
                                st.metric("Hybrid Score", f"{row['hybrid_score']:.2f}")
                                st.metric("ALS Score", f"{row['als_score']:.2f}")
                                st.metric("Content Score", f"{row['content_score']:.2f}")
                            with col2:
                                st.write(f"**ISBN:** {row['isbn']}")
                                st.write(f"**Rank:** {row['rank']}")
    
    # ======== MODE 2: Similar Books ========
    elif mode == "🔍 Find Similar Books":
        st.header("Find Similar Books")
        
        search_type = st.radio("Search by:", ["Book Title", "ISBN"])
        
        if search_type == "Book Title":
            books_df = load_books(spark)
            
            if not books_df.empty:
                search_query = st.text_input("Enter book title:")
                
                if search_query:
                    matches = books_df[
                        books_df['title'].str.contains(search_query, case=False, na=False)
                    ].head(10)
                    
                    if not matches.empty:
                        selected_book = st.selectbox(
                            "Select a book:",
                            matches['isbn'].tolist(),
                            format_func=lambda x: matches[matches['isbn']==x]['title'].values[0]
                        )
                        
                        if st.button("Find Similar Books"):
                            similar_df = load_similar_books(spark, selected_book)
                            
                            if not similar_df.empty:
                                st.subheader(f"Books similar to: {matches[matches['isbn']==selected_book]['title'].values[0]}")
                                st.dataframe(similar_df)
                            else:
                                st.warning("No similar books found")
                    else:
                        st.warning("No books found. Try another search.")
        
        else:  # ISBN search
            isbn = st.text_input("Enter ISBN:")
            
            if st.button("Find Similar Books") and isbn:
                with st.spinner("Finding similar books..."):
                    similar_df = load_similar_books(spark, isbn)
                    
                    if similar_df.empty:
                        st.warning(f"No similar books found for ISBN: {isbn}")
                    else:
                        st.success(f"Found {len(similar_df)} similar books!")
                        st.dataframe(similar_df)
    
    # ======== MODE 3: Explore Data ========
    elif mode == "📊 Explore Data":
        st.header("Explore Dataset")
        
        with st.spinner("Loading statistics..."):
            stats = load_statistics(spark)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Books", f"{stats['books']:,}")
        
        with col2:
            st.metric("Total Users", f"{stats['users']:,}")
        
        with col3:
            st.metric("Total Ratings", f"{stats['ratings']:,}")
        
        with col4:
            st.metric("Avg Rating", f"{stats['avg_rating']}")
        
        st.subheader("Sample Books")
        books_df = load_books(spark)
        if not books_df.empty:
            st.dataframe(books_df.head(20))


if __name__ == "__main__":
    main()