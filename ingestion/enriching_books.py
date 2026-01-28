import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from ingestion.google_books import GoogleBooksClient
from config import DB_CONFIG

# ========== CONFIGURATION ==========
DAILY_LIMIT = int(os.getenv('ENRICHMENT_BATCH_SIZE', 100))

# ========== DATABASE OPERATIONS ==========

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_unenriched_books(limit=DAILY_LIMIT):
    """
    Fetch books from database that haven't been enriched yet
    Returns: List of (isbn, kaggle_title, kaggle_author, kaggle_year, kaggle_publisher)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT isbn, kaggle_title, kaggle_author, kaggle_year, kaggle_publisher
        FROM books
        WHERE enriched = FALSE
        AND isbn IS NOT NULL
        AND LENGTH(REGEXP_REPLACE(isbn, '[^0-9X]', '', 'g')) IN (10, 13)  -- Valid ISBNs only
        ORDER BY created_at DESC
        LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    books = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    print(f" Found {len(books)} unenriched books with valid ISBNs")
    return books

def update_book_with_google_data(isbn, google_data):
    """
    Update a single book with Google Books API data
    
    Args:
        isbn: Book ISBN
        google_data: Dict from GoogleBooksClient (or None if not found)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if google_data:
            # Successfully enriched
            cursor.execute("""
                UPDATE books SET
                    title = %s,
                    description = %s,
                    authors = %s,
                    categories = %s,
                    publisher = COALESCE(%s, kaggle_publisher),
                    page_count = %s,
                    language = %s,
                    enriched = TRUE,
                    data_source = 'google_books'
                WHERE isbn = %s
            """, (
                google_data['title'],
                google_data['description'],
                google_data['authors'],
                google_data['categories'],
                google_data['publisher'],
                google_data['page_count'],
                google_data['language'],
                isbn
            ))
            print(f"Enriched: {google_data['title'][:50]}")
        else:
            # Not found in Google Books - mark as enriched anyway to avoid retrying
            # Use Kaggle data as fallback
            cursor.execute("""
                UPDATE books SET
                    title = COALESCE(title, kaggle_title),
                    authors = COALESCE(authors, ARRAY[kaggle_author]),
                    enriched = TRUE,
                    data_source = 'kaggle_only'
                WHERE isbn = %s
            """, (isbn,))
            print(f"⚠️  Not found, using Kaggle data: {isbn}")
        
        conn.commit()
        
    except Exception as e:
        print(f" Database error for ISBN {isbn}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_enrichment_stats():
    """Get enrichment statistics from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN enriched = TRUE THEN 1 ELSE 0 END) as enriched,
            SUM(CASE WHEN data_source = 'google_books' THEN 1 ELSE 0 END) as from_google,
            SUM(CASE WHEN data_source = 'kaggle_only' THEN 1 ELSE 0 END) as kaggle_only
        FROM books
    """)
    
    stats = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return {
        'total': stats[0],
        'enriched': stats[1],
        'from_google': stats[2],
        'kaggle_only': stats[3],
        'pending': stats[0] - stats[1]
    }

# ========== ENRICHMENT PIPELINE ==========

def enrich_books_batch(limit=DAILY_LIMIT):
    """
    Main enrichment pipeline:
    1. Get unenriched books from database
    2. Call Google Books API for each (with fallback to title search)
    3. Update database with results
    """
    
    # Print initial stats
    print("\n" + "=" * 60)
    print("ENRICHMENT STATISTICS (BEFORE)")
    print("=" * 60)
    stats = get_enrichment_stats()
    for key, value in stats.items():
        print(f"   {key}: {value:,}")
    
    # Step 1: Get books to enrich
    books = get_unenriched_books(limit)
    
    if not books:
        print("\n✅ No books to enrich - all done!")
        return {
            'processed': 0,
            'enriched': 0,
            'not_found': 0
        }
    
    # Step 2: Initialize Google Books API client
    client = GoogleBooksClient()
    
    if not client.test_connection():
        print(" Google Books API connection test failed!")
        raise ConnectionError("Cannot connect to Google Books API")
    
    print(f" Google Books API connected")
    
    # Step 3: Process each book
    enriched_count = 0
    not_found_count = 0
    
    for isbn, title, author, year, publisher in tqdm(books, desc="Enriching books"):
        # Fetch from Google Books with fallback to title search
        google_data = client.search_by_isbn(
            isbn,
            retry_with_title=True,  # Enable fallback
            title=title,
            author=author
        )
        
        # Update database
        update_book_with_google_data(isbn, google_data)
        
        # Track stats
        if google_data:
            enriched_count += 1
        else:
            not_found_count += 1
    
    # Step 4: Print final stats
    print("  COMPLETE")
    print(f"   Processed: {len(books)}")
    print(f"   Successfully enriched: {enriched_count}")
    print(f"   Not found (using Kaggle): {not_found_count}")
    
    # Print overall stats
    print("\n DATABASE STATISTICS (AFTER)")
    stats = get_enrichment_stats()
    for key, value in stats.items():
        print(f"   {key}: {value:,}")
    
    return {
        'processed': len(books),
        'enriched': enriched_count,
        'not_found': not_found_count
    }

# ========== MAIN ENTRY POINT ==========

def main():
    try:
        results = enrich_books_batch(limit=DAILY_LIMIT)
        print(f"\n Enrichment succeeded: {results}")
        return 0
    except Exception as e:
        print(f"\n Enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())