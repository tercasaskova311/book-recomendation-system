import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import json
from tqdm import tqdm
import os
from dotenv import load_dotenv 
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)load_dotenv()
from config import (
    DB_HOST, DB_PORT, DB_NAME,
    DB_USER, DB_PASSWORD)


def get_connection():
    config = DB_CONFIG.copy()
    return psycopg2.connect(**config)

#============== CONNECTION ========================================================


def test_connection():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print(" Database connection successful!")
        return True
    except Exception as e:
        print(f" Connection failed: {e}")
        return False

# ============ # CREATE DB & SQL ================================================

def create_database():
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database='postgres'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (DB_CONFIG['database'],)
    )
    
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
        print(f" Created database: {DB_CONFIG['database']}")
    else:
        print(f" Database exists: {DB_CONFIG['database']}")
    
    cursor.close()
    conn.close()

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()    
    with open('ingestion/database/schema.sql', 'r') as f:
        sql_script = f.read()
    
    cursor.execute(sql_script)
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Tables created from schema.sql!")

# ============== LOAD DATA ==============================================

def load_books(csv_path):
    
    df = pd.read_csv(csv_path)
    conn = get_connection()
    cursor = conn.cursor()
    records = []
    for _, row in df.iterrows():
        
        authors = json.loads(row['authors']) if pd.notna(row['authors']) else []
        categories = json.loads(row['categories']) if pd.notna(row['categories']) else []
        
        records.append((
            row['isbn'],
            row.get('kaggle_title'),
            row.get('kaggle_author'),
            int(row['kaggle_year']) if pd.notna(row['kaggle_year']) else None,
            row.get('kaggle_publisher'),
            row['title'],
            row['description'] if pd.notna(row['description']) else None,
            authors,       # ← Parsed from JSON string to Python list
            categories,    # ← Parsed from JSON string to Python list
            row['publisher'] if pd.notna(row['publisher']) else None,
            row['published_date'] if pd.notna(row['published_date']) else None,
            int(row['page_count']) if pd.notna(row['page_count']) else None,
            row['language'],
            row['enriched'],
            row['data_source']
        ))
    
    query = """
        INSERT INTO books (
            isbn, kaggle_title, kaggle_author, kaggle_year, kaggle_publisher,
            title, description, authors, categories, publisher, published_date,
            page_count, language, enriched, data_source
        ) VALUES %s
        ON CONFLICT (isbn) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description
    """
    
    execute_values(cursor, query, records, page_size=100)
    conn.commit()
    
    print(f" Loaded {len(records)} books")
    
    cursor.close()
    conn.close()

def load_users(csv_path):    
    df = pd.read_csv(csv_path)
    conn = get_connection()
    cursor = conn.cursor()
    
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['User-ID']),
            row['Location'] if pd.notna(row['Location']) else None,
            int(row['Age']) if pd.notna(row['Age']) and str(row['Age']).strip() else None
        ))
    
    query = """
        INSERT INTO users (user_id, location, age)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING
    """
    
    execute_values(cursor, query, records, page_size=1000)
    conn.commit()
    
    print(f" Loaded {len(records)} users")
    
    cursor.close()
    conn.close()

def load_ratings(csv_path, enriched_books_csv=None):
    
    df = pd.read_csv(csv_path)    
    if enriched_books_csv:
        enriched_df = pd.read_csv(enriched_books_csv)
        valid_isbns = set(enriched_df['isbn'])
        df = df[df['ISBN'].isin(valid_isbns)]
        print(f"   Filtered to {len(df)} ratings for enriched books")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    records = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Preparing"):
        records.append((
            int(row['User-ID']),
            row['ISBN'],
            int(row['Book-Rating'])
        ))
    
    query = """
        INSERT INTO ratings (user_id, isbn, rating)
        VALUES %s
        ON CONFLICT (user_id, isbn) DO NOTHING
    """

    print("   Inserting into database...")
    execute_values(cursor, query, records, page_size=1000)
    conn.commit()
    
    print(f" Loaded {len(records)} ratings")
    
    cursor.close()
    conn.close()


#============== QUERY DATA ===================

def get_table_counts():
    conn = get_connection()
    cursor = conn.cursor()
    print("\n Table Statistics:")
    
    for table in ['books', 'users', 'ratings']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count:,} rows")
    
    cursor.close()
    conn.close()

def sample_books():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT isbn, title, array_length(authors, 1) as num_authors,
               array_length(categories, 1) as num_categories,
               length(description) as desc_length
        FROM books
        WHERE enriched = TRUE
        LIMIT 5
    """)
    
    print("\n Sample Books:")
    for row in cursor.fetchall():
        print(f"   ISBN: {row[0]}")
        print(f"   Title: {row[1]}")
        print(f"   Authors: {row[2]}, Categories: {row[3]}, Desc: {row[4]} chars")
        print()
    
    cursor.close()
    conn.close()


# ============ MAIN PIPELINE ================================================

def main():
    create_database()

    if not test_connection():
        print("Cannot connect to database. Check your .env file.")
        return
    
    create_tables()
    load_books('data/books_enriched.csv')    
    load_users('data/Users.csv')    
    load_ratings('data/Ratings.csv', enriched_books_csv='data/books_enriched.csv')    
    get_table_counts()
    sample_books()
    print("\n Pipeline complete!")

if __name__ == "__main__":
    main()