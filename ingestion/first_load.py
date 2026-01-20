import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from tqdm import tqdm
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv()

from config import DB_CONFIG

def get_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(**DB_CONFIG)

# ============== CONNECTION TEST ==========

def test_connection():
    """Test database connection"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

# ============== DATABASE SETUP ==========

def create_database():
    """Create database if it doesn't exist"""
    try:
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
            print(f"✅ Created database: {DB_CONFIG['database']}")
        else:
            print(f"✅ Database exists: {DB_CONFIG['database']}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database creation failed: {e}")
        return False

def create_tables():
    """Create tables from schema.sql"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        schema_path = 'ingestion/database/schema.sql'
        if not os.path.exists(schema_path):
            print(f"❌ Schema file not found: {schema_path}")
            return False
        
        with open(schema_path, 'r') as f:
            sql_script = f.read()
        
        # Execute the entire script at once (PostgreSQL can handle multiple statements)
        cursor.execute(sql_script)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print("✅ Tables created from schema.sql")
        return True
        
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============== DATA LOADING ==========

def clean_isbn(isbn):
    """Clean ISBN - remove hyphens, spaces, keep only valid characters"""
    if pd.isna(isbn):
        return None
    isbn_str = str(isbn).strip()
    # Remove hyphens and spaces
    isbn_clean = isbn_str.replace('-', '').replace(' ', '')
    # Keep only alphanumeric and X (for ISBN-10 check digit)
    isbn_clean = ''.join(c for c in isbn_clean if c.isalnum() or c == 'X')
    # Valid ISBNs are 10 or 13 characters
    if len(isbn_clean) not in [10, 13]:
        return None
    return isbn_clean

def load_books_from_kaggle(csv_path):
    """
    Load books from Kaggle CSV into PostgreSQL
    - Only loads books with valid ISBNs
    - Marks all as unenriched (enriched=FALSE)
    - Google enrichment will happen separately
    """
    print(f"\n📚 Loading books from {csv_path}...")
    
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return 0
        
        # Read CSV with error handling
        df = pd.read_csv(
            csv_path,
            encoding='latin-1',
            on_bad_lines='skip'
        )
        
        print(f"   Read {len(df):,} rows from CSV")
        
        # Clean ISBNs
        df['isbn_clean'] = df['ISBN'].apply(clean_isbn)
        
        # Filter to valid ISBNs only
        df_valid = df[df['isbn_clean'].notna()].copy()
        
        print(f"   Valid ISBNs: {len(df_valid):,}/{len(df):,} ({len(df_valid)/len(df)*100:.1f}%)")
        
        # Remove duplicates (keep first occurrence)
        df_valid = df_valid.drop_duplicates(subset=['isbn_clean'], keep='first')
        
        print(f"   After deduplication: {len(df_valid):,} unique books")
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Process in batches to avoid memory issues
        batch_size = 1000
        total_inserted = 0
        
        query = """
            INSERT INTO books (
                isbn,
                kaggle_title, kaggle_author, kaggle_year, kaggle_publisher,
                title, description, authors, categories, publisher, published_date,
                page_count, language, enriched, data_source
            ) VALUES %s
            ON CONFLICT (isbn) DO NOTHING
        """
        
        for i in tqdm(range(0, len(df_valid), batch_size), desc="Inserting batches"):
            batch = df_valid.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch.iterrows():
                try:
                    year = int(row['Year-Of-Publication'])
                except (ValueError, TypeError):
                    year = None
                
                records.append((
                    row['isbn_clean'],
                    row.get('Book-Title'),
                    row.get('Book-Author'),
                    year,
                    row.get('Publisher'),
                    row.get('Book-Title'),
                    None,
                    None,
                    None,
                    row.get('Publisher'),
                    None,
                    None,
                    'en',
                    False,
                    'kaggle_initial'
                ))
            
            try:
                execute_values(cursor, query, records, page_size=1000)
                conn.commit()
                total_inserted += cursor.rowcount
            except Exception as e:
                print(f"\n❌ Error inserting batch {i//batch_size}: {e}")
                conn.rollback()
        
        print(f"\n✅ Loaded {total_inserted:,} books into database")
        
        cursor.close()
        conn.close()
        
        return total_inserted
        
    except Exception as e:
        print(f"❌ Book loading failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def load_users(csv_path):
    """Load users from Kaggle CSV"""
    print(f"\n👥 Loading users from {csv_path}...")
    
    try:
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return 0
        
        df = pd.read_csv(
            csv_path,
            encoding='latin-1',
            on_bad_lines='skip'
        )
        
        print(f"   Read {len(df):,} users from CSV")
        
        conn = get_connection()
        cursor = conn.cursor()
        
        batch_size = 1000
        total_inserted = 0
        
        query = """
            INSERT INTO users (user_id, location, age)
            VALUES %s
            ON CONFLICT (user_id) DO NOTHING
        """
        
        for i in tqdm(range(0, len(df), batch_size), desc="Inserting batches"):
            batch = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch.iterrows():
                # Clean age data
                age = None
                if pd.notna(row['Age']):
                    try:
                        age_val = int(row['Age'])
                        # Filter unrealistic ages
                        if 5 <= age_val <= 120:
                            age = age_val
                    except (ValueError, TypeError):
                        pass
                
                records.append((
                    int(row['User-ID']),
                    row['Location'] if pd.notna(row['Location']) else None,
                    age
                ))
            
            try:
                execute_values(cursor, query, records, page_size=1000)
                conn.commit()
                total_inserted += cursor.rowcount
            except Exception as e:
                print(f"\n❌ Error inserting batch {i//batch_size}: {e}")
                conn.rollback()
        
        print(f"\n✅ Loaded {total_inserted:,} users into database")
        
        cursor.close()
        conn.close()
        
        return total_inserted
        
    except Exception as e:
        print(f"❌ User loading failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def load_ratings(csv_path):
    """
    Load ratings from Kaggle CSV
    - Only loads ratings for books that exist in database
    """
    print(f"\n⭐ Loading ratings from {csv_path}...")
    
    try:
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return 0
        
        # First, get all valid ISBNs from database
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT isbn FROM books")
        valid_isbns = {row[0] for row in cursor.fetchall()}
        print(f"   Found {len(valid_isbns):,} valid ISBNs in database")
        
        cursor.close()
        conn.close()
        
        # Read ratings CSV
        df = pd.read_csv(
            csv_path,
            encoding='latin-1',
            on_bad_lines='skip'
        )
        
        print(f"   Read {len(df):,} ratings from CSV")
        
        # Clean ISBNs in ratings
        df['isbn_clean'] = df['ISBN'].apply(clean_isbn)
        
        # Filter to ratings for books we have
        df_valid = df[df['isbn_clean'].isin(valid_isbns)].copy()
        
        print(f"   Ratings for valid books: {len(df_valid):,}/{len(df):,} ({len(df_valid)/len(df)*100:.1f}%)")
        
        # Filter to valid ratings (0-10)
        df_valid = df_valid[
            (df_valid['Book-Rating'] >= 0) & 
            (df_valid['Book-Rating'] <= 10)
        ]
        
        conn = get_connection()
        cursor = conn.cursor()
        
        batch_size = 1000
        total_inserted = 0
        
        query = """
            INSERT INTO ratings (user_id, isbn, rating)
            VALUES %s
            ON CONFLICT (user_id, isbn) DO UPDATE SET rating = EXCLUDED.rating
        """
        
        for i in tqdm(range(0, len(df_valid), batch_size), desc="Inserting batches"):
            batch = df_valid.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch.iterrows():
                records.append((
                    int(row['User-ID']),
                    row['isbn_clean'],
                    int(row['Book-Rating'])
                ))
            
            try:
                execute_values(cursor, query, records, page_size=1000)
                conn.commit()
                total_inserted += cursor.rowcount
            except Exception as e:
                print(f"\n❌ Error inserting batch {i//batch_size}: {e}")
                conn.rollback()
        
        print(f"\n✅ Loaded {total_inserted:,} ratings into database")
        
        cursor.close()
        conn.close()
        
        return total_inserted
        
    except Exception as e:
        print(f"❌ Rating loading failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

# ============== STATISTICS ==========

def get_table_counts():
    """Get row counts for all tables"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print("\n📊 TABLE STATISTICS:")
        print("=" * 60)
        
        for table in ['books', 'users', 'ratings']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   {table:20} {count:>15,} rows")
        
        # Book enrichment stats
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN enriched = TRUE THEN 1 ELSE 0 END) as enriched,
                SUM(CASE WHEN enriched = FALSE THEN 1 ELSE 0 END) as pending
            FROM books
        """)
        
        stats = cursor.fetchone()
        print(f"\n   Books enriched:      {stats[0]:>15,}")
        print(f"   Books pending:       {stats[1]:>15,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error getting statistics: {e}")

def sample_books():
    """Show sample books"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT isbn, kaggle_title, kaggle_author, enriched, data_source
            FROM books
            ORDER BY created_at DESC
            LIMIT 5
        """)
        
        print("\n📚 SAMPLE BOOKS:")
        print("=" * 60)
        for row in cursor.fetchall():
            print(f"   ISBN: {row[0]}")
            print(f"   Title: {row[1]}")
            print(f"   Author: {row[2]}")
            print(f"   Enriched: {row[3]}")
            print(f"   Source: {row[4]}")
            print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error getting sample books: {e}")

# ============== MAIN PIPELINE ==========

def main():
    """
    Main data loading pipeline:
    1. Create database and tables
    2. Load Kaggle data (books, users, ratings)
    3. All books marked as unenriched
    4. Enrichment happens separately via enriching_books.py
    """
    print("\n" + "=" * 60)
    print("📚 BOOK RECOMMENDATION SYSTEM - INITIAL DATA LOAD")
    print("=" * 60)
    
    # Step 1: Database setup
    print("\n🔧 Step 1: Database Setup")
    
    if not create_database():
        print("❌ Database creation failed. Exiting.")
        return 1
    
    if not test_connection():
        print("❌ Cannot connect to database. Check your .env file.")
        return 1
    
    if not create_tables():
        print("❌ Table creation failed. Exiting.")
        return 1
    
    # Step 2: Load data
    print("\n📥 Step 2: Loading Kaggle Data")
    
    books_loaded = load_books_from_kaggle('data/Books.csv')
    if books_loaded == 0:
        print("⚠️  Warning: No books loaded")
    
    users_loaded = load_users('data/Users.csv')
    if users_loaded == 0:
        print("⚠️  Warning: No users loaded")
    
    ratings_loaded = load_ratings('data/Ratings.csv')
    if ratings_loaded == 0:
        print("⚠️  Warning: No ratings loaded")
    
    # Step 3: Show statistics
    print("\n📊 Step 3: Final Statistics")
    get_table_counts()
    sample_books()
    
    # Step 4: Next steps
    print("\n" + "=" * 60)
    print("✅ INITIAL DATA LOAD COMPLETE!")
    print("=" * 60)
    print("\n📋 NEXT STEPS:")
    print("   1. Run enrichment: python ingestion/enriching_books.py")
    print("   2. Or enable Airflow DAG 'enrich_new_books'")
    print("   3. Monitor enrichment progress in database")
    print("\n💡 TIP: Run 'python ingestion/google_books.py diagnose' to check ISBN quality")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())