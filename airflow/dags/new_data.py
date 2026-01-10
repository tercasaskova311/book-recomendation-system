#Runs independently at 1 AM daily

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys 
from fetching_data import GoogleBooksClient
import psycopg2

#========== 100 NEW BOOKS PER DAY =======================
def enrich_books_from_api(): #Fetch up to 100 unenriched books and enrich them using Google Books API.

    sys.path.insert(0, '/opt/project')
    # Database connection
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'book_recommendations'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD', '')
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT isbn, kaggle_title, kaggle_author 
        FROM books 
        WHERE enriched = FALSE 
        AND data_source = 'kaggle_only'
        LIMIT 100
    """)
    
    books_to_enrich = cursor.fetchall()
    
    if not books_to_enrich:
        print(" No books need enrichment")
        cursor.close()
        conn.close()
        return 0
    
    print(f" Enriching {len(books_to_enrich)} books from Google Books API...")
    
    # Initialize Google Books client
    client = GoogleBooksClient(rate_limit_delay=0.1)
    enriched_count = 0
    
    for isbn, title, author in books_to_enrich:
        google_data = client.search_by_isbn(isbn)
        
        if google_data:
            cursor.execute("""
                UPDATE books 
                SET 
                    title = %s,
                    description = %s,
                    authors = %s,
                    categories = %s,
                    publisher = %s,
                    published_date = %s,
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
                google_data['published_date'],
                google_data['page_count'],
                google_data['language'],
                isbn
            ))
            enriched_count += 1
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f" Successfully enriched {enriched_count}/{len(books_to_enrich)} books")
    
    return enriched_count

#========= DAG DEFINITION =======================
default_args = {
    'owner' : 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'new_books',
    default_args=default_args,
    description='adding new books from google books api',
    schedule_interval='0 1 * * *',  # 1:00 AM daily
    start_date=days_ago(1),
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only one pipeline instance at a time
    tags=['ingestion', 'google-api', 'books'],
) as dag:
    enrich_books_task = PythonOperator(
        task_id= 'enrich_books',
        python_callable= enrich_books_from_api,
    )