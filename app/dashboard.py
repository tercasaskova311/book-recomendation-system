import streamlit as st
import pandas as pd
import psycopg2
import sys
import os

# Project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import JDBC_URL, JDBC_PROPERTIES, DB_CONFIG

# ========= DB HELPER =========
def query_db(query, params=None):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# ========= LOAD USER RECOMMENDATIONS =========
@st.cache_data
def load_user_recommendations(user_id):
    query = """
        SELECT r.rank, r.isbn, r.hybrid_score, r.als_score, r.content_score,
               b.title, b.authors, b.categories
        FROM recommendations_cache r
        JOIN books b USING (isbn)
        WHERE r.user_id = %s
        ORDER BY r.rank
        LIMIT 20
    """
    return query_db(query, (user_id,))

# ========= LOAD POPULAR BOOKS =========
@st.cache_data
def load_popular_books(top_n=10):
    query = """
        SELECT b.title, b.authors, b.categories, COUNT(r.rating) AS num_ratings, AVG(r.rating) AS avg_rating
        FROM ratings r
        JOIN books b USING(isbn)
        GROUP BY b.isbn, b.title, b.authors, b.categories
        ORDER BY num_ratings DESC
        LIMIT %s
    """
    return query_db(query, (top_n,))

# ========= GET USERS =========
@st.cache_data
def get_all_users_with_recs():
    query = "SELECT DISTINCT user_id FROM recommendations_cache"
    result = query_db(query)
    return result['user_id'].tolist() if not result.empty else []

@st.cache_data
def get_cold_start_users(limit=20):
    query = """
        SELECT user_id
        FROM users
        WHERE user_id NOT IN (SELECT DISTINCT user_id FROM recommendations_cache)
        LIMIT %s
    """
    return query_db(query, (limit,))

# ========= MAIN DASHBOARD =========
def main():
    st.set_page_config("Book Recommendation System", "📚", layout="wide")
    st.title("📚 Book Recommendation System")

    tab1, tab2, tab3 = st.tabs(["User Recommendations", "Popular Books", "Cold Start Users"])

    # --------- Tab 1: User Recommendations ---------
    with tab1:
        users_with_recs = get_all_users_with_recs()
        if not users_with_recs:
            st.warning("No recommendations available yet. Run the pipeline first.")
        else:
            user_id = st.selectbox("Select User ID", users_with_recs, index=0)
            if st.button("Get Recommendations", key="user_recs_btn"):
                with st.spinner("Loading recommendations..."):
                    df = load_user_recommendations(user_id)
                if df.empty:
                    st.warning("No recommendations found")
                else:
                    for _, row in df.iterrows():
                        with st.expander(f"#{row['rank']} – {row['title']}"):
                            st.write(f"**Authors:** {row['authors']}")
                            st.write(f"**Categories:** {row['categories']}")
                            st.metric("Hybrid score", round(row["hybrid_score"], 3))

    # --------- Tab 2: Most Popular Books ---------
    with tab2:
        popular_books = load_popular_books()
        st.table(popular_books)

    # --------- Tab 3: Cold Start Users ---------
    with tab3:
        cold_users = get_cold_start_users()
        st.write("Users without recommendations yet:")
        st.table(cold_users)

if __name__ == "__main__":
    main()
