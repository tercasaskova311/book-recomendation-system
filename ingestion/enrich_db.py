import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm
import os
import sys
import logging
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from ingestion.fetching_data import GoogleBooksClient
from config import DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== CONFIGURATION ==========
DAILY_LIMIT = int(os.getenv('ENRICHMENT_BATCH_SIZE', 100))  #API quota

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
        ORDER BY created_at DESC
        LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    books = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    logger.info(f"Found {len(books)} unenriched books in database")
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
                google_data['page_count'],
                google_data['language'],
                isbn
            ))
            logger.debug(f"Enriched: {google_data['title']}")
        else:
            # Not found in Google Books - mark as enriched anyway
            cursor.execute("""
                UPDATE books SET
                    enriched = TRUE,
                    data_source = 'kaggle_only'
                WHERE isbn = %s
            """, (isbn,))
            logger.debug(f" Not found: {isbn}")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Database error for ISBN {isbn}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ========== ENRICHMENT PIPELINE ==========

def enrich_books_batch(limit=DAILY_LIMIT):
    """
    Main enrichment pipeline:
    1. Get unenriched books from database
    2. Call Google Books API for each
    3. Update database with results
    """
    logger.info("=" * 60)
    logger.info("BOOK ENRICHMENT PIPELINE STARTED")
    logger.info("=" * 60)
    
    # Step 1: Get books to enrich
    books = get_unenriched_books(limit)
    
    if not books:
        logger.info(" No books to enrich found - all done!")
        return {
            'processed': 0,
            'enriched': 0,
            'not_found': 0
        }
    
    # Step 2: Initialize Google Books API client
    client = GoogleBooksClient(rate_limit_delay=0.1)
    
    if not client.test_connection():
        logger.error(" Google Books API connection test failed!")
        raise ConnectionError("Cannot connect to Google Books API")
    
    logger.info(f"Google Books API connected")
    
    # Step 3: Process each book
    enriched_count = 0
    not_found_count = 0
    
    for isbn, title, author, year, publisher in tqdm(books, desc="Enriching books"):
        # Fetch from Google Books
        google_data = client.search_by_isbn(isbn)
        
        # Update database
        update_book_with_google_data(isbn, google_data)
        
        # Track stats
        if google_data:
            enriched_count += 1
        else:
            not_found_count += 1
    
    # Step 4: Print summary
    logger.info(" COMPLETE")
    logger.info(f"Total processed: {len(books)}")
    logger.info(f"Successfully enriched: {enriched_count} ({enriched_count/len(books)*100:.1f}%)")
    logger.info(f"Not found: {not_found_count} ({not_found_count/len(books)*100:.1f}%)")

# ========== MAIN ENTRY POINT ==========

def main():
    try:
        results = enrich_books_batch(limit=DAILY_LIMIT)
        logger.info(f"\n succeeded: {results}")
        return 0
    except Exception as e:
        logger.error(f"\nfailed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())