import pandas as pd
import json
from tqdm import tqdm
from src.fetching_data import GoogleBooksClient

# ========== CONFIGURATION ==========
INPUT_FILE = 'data/Books.csv'
OUTPUT_FILE = 'data/books_enriched.csv'
MAX_BOOKS = 1000  # Change to None for all books
# ===================================

df = pd.read_csv(INPUT_FILE)
print(f" Loaded {len(df)} unique books")

# Limit for testing
if MAX_BOOKS:
    df = df.head(MAX_BOOKS)
    print(f"Test mode: Processing {MAX_BOOKS} books")

client = GoogleBooksClient(rate_limit_delay=0.1)
if not client.test_connection():
    print("Connection failed!")
    exit(1)
print("Connected!\n")

enriched = []
success = 0

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Enriching"):
    isbn = row['ISBN']
    
    google_data = client.search_by_isbn(isbn)
    
    book = {
        'isbn': isbn,
        'kaggle_title': row['Book-Title'],
        'kaggle_author': row['Book-Author'],
        'kaggle_year': row['Year-Of-Publication'],
        'kaggle_publisher': row['Publisher']
    }
    
    if google_data:
        # Add Google Books data
        book.update({
            'title': google_data['title'],
            'subtitle': google_data['subtitle'],
            'description': google_data['description'],
            'authors': json.dumps(google_data['authors']),
            'categories': json.dumps(google_data['categories']),
            'publisher': google_data['publisher'],
            'published_date': google_data['published_date'],
            'page_count': google_data['page_count'],
            'language': google_data['language'],
            'enriched': 'Yes',
            'data_source': 'google_books'
        })
        success += 1
    else:
        # Not found - use Kaggle data only
        book.update({
            'title': row['Book-Title'],
            'description': None,
            'authors': json.dumps([row['Book-Author']]),
            'categories': None,
            'enriched': 'No',
            'data_source': 'kaggle_only'
        })
    
    enriched.append(book)

# Save results
result_df = pd.DataFrame(enriched)
result_df.to_csv(OUTPUT_FILE, index=False)

# Print comprehensive summary
print(" ENRICHMENT COMPLETE!")
print(f" Total books processed: {len(result_df)}")
print(f" Successfully enriched: {success} ({success/len(result_df)*100:.1f}%)")
print(f" Not found: {len(result_df) - success} ({(len(result_df)-success)/len(result_df)*100:.1f}%)")

# API statistics
print("API STATISTICS")
stats = client.get_stats()
for key, value in stats.items():
    print(f"   {key}: {value}")

# Data quality statistics
enriched_books = result_df[result_df['enriched'] == 'Yes']
if len(enriched_books) > 0:
    with_desc = enriched_books['description'].notna().sum()
    with_categories = enriched_books['categories'].notna().sum()
    with_ratings = enriched_books['average_rating'].notna().sum()
    print(f"   Books with descriptions: {with_desc} ({with_desc/len(enriched_books)*100:.1f}%)")
    print(f"   Books with categories: {with_categories} ({with_categories/len(enriched_books)*100:.1f}%)")
    print(f"   Books with ratings: {with_ratings} ({with_ratings/len(enriched_books)*100:.1f}%)")
