import streamlit as st 
import pandas as pd 
import psycopg2 
import sys 
import os 
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
sys.path.insert(0, project_root) 
from config import (
    JDBC_URL, JDBC_PROPERTIES,DB_CONFIG,
    DELTA_SIMILARITIES, DELTA_FINAL_RECS)

# ========= DB HELPER =========
def query_db(query, params=None):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# ========= LOAD RECOMMENDATIONS =========
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

# ========= MAIN =========
def main():
    st.set_page_config("Book Recommendation System", "📚", layout="wide")
    st.title("📚 Book Recommendation System")

    user_id = st.number_input("User ID", min_value=1, value=276725)

    if st.button("Get recommendations"):
        with st.spinner("Loading recommendations..."):
            df = load_user_recommendations(user_id)

        if df.empty:
            st.warning("No recommendations found")
            return

        for _, row in df.iterrows():
            with st.expander(f"#{row['rank']} – {row['title']}"):
                st.write(f"**Authors:** {row['authors']}")
                st.write(f"**Categories:** {row['categories']}")
                st.metric("Hybrid score", round(row["hybrid_score"], 3))

if __name__ == "__main__":
    main()
