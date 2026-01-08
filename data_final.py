import pandas as pd
import json
from tqdm import tqdm
from fetching_data import GoogleBooksClient

# ========== CONFIGURATION ==========
INPUT_FILE = 'data/Books.csv'
OUTPUT_FILE = 'data/books_enriched.csv'
MAX_BOOKS = 100  # Change to None for all books
# ===================================
print(f"\n Loading: {INPUT_FILE}")
df = pd.read_csv(INPUT_FILE)
print(f"✅ Loaded {len(df)} unique books")

# Limit for testing
if MAX_BOOKS:
    df = df.head(MAX_BOOKS)
    print(f"Test mode: Processing {MAX_BOOKS} books")

# Initialize Google Books client
print(f"\n🔌 Connecting to Google Books API...")
client = GoogleBooksClient(rate_limit_delay=0.1)

if not client.test_connection():
    print("❌ Connection failed!")
    exit(1)

print("✅ Connected!\n")

# Enrich books
enriched = []
success = 0

print(f"🚀 Starting enrichment of {len(df)} books...")
print(f"⏱️  Estimated time: {len(df) * 0.1 / 60:.1f} minutes\n")

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Enriching"):
    isbn = row['ISBN']
    
    # Get Google Books data
    google_data = client.search_by_isbn(isbn)
    
    # Build record
    book = {
        # Original Kaggle data
        'isbn': isbn,
        'kaggle_title': row['Book-Title'],
        'kaggle_author': row['Book-Author'],
        'kaggle_year': row['Year-Of-Publication'],
        'kaggle_publisher': row['Publisher'],
        'kaggle_image': row['Image-URL-L'],
    }
    
    if google_data:
        # Add Google Books data
        book.update({
            'google_id': google_data['google_id'],
            'title': google_data['title'],
            'subtitle': google_data['subtitle'],
            'description': google_data['description'],
            'authors': json.dumps(google_data['authors']),
            'categories': json.dumps(google_data['categories']),
            'publisher': google_data['publisher'],
            'published_date': google_data['published_date'],
            'page_count': google_data['page_count'],
            'language': google_data['language'],
            'average_rating': google_data['average_rating'],
            'ratings_count': google_data['ratings_count'],
            'cover_url': google_data['cover_url'],
            'preview_link': google_data['preview_link'],
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
print("\n" + "="*70)
print("✅ ENRICHMENT COMPLETE!")
print("="*70)
print(f"📊 Total books processed: {len(result_df)}")
print(f"✅ Successfully enriched: {success} ({success/len(result_df)*100:.1f}%)")
print(f"❌ Not found: {len(result_df) - success} ({(len(result_df)-success)/len(result_df)*100:.1f}%)")
print(f"💾 Saved to: {OUTPUT_FILE}")

# API statistics
print("\n" + "="*70)
print("📡 API STATISTICS")
print("="*70)
stats = client.get_stats()
for key, value in stats.items():
    print(f"   {key}: {value}")

# Data quality statistics
enriched_books = result_df[result_df['enriched'] == 'Yes']
if len(enriched_books) > 0:
    with_desc = enriched_books['description'].notna().sum()
    with_categories = enriched_books['categories'].notna().sum()
    with_ratings = enriched_books['average_rating'].notna().sum()
    
    print("\n" + "="*70)
    print("📈 DATA QUALITY")
    print("="*70)
    print(f"   Books with descriptions: {with_desc} ({with_desc/len(enriched_books)*100:.1f}%)")
    print(f"   Books with categories: {with_categories} ({with_categories/len(enriched_books)*100:.1f}%)")
    print(f"   Books with ratings: {with_ratings} ({with_ratings/len(enriched_books)*100:.1f}%)")

# Show sample enriched books
print("\n" + "="*70)
print("📚 SAMPLE ENRICHED BOOKS")
print("="*70)

samples = result_df[result_df['enriched'] == 'Yes'].head(3)
for i, book in samples.iterrows():
    print(f"\n{i+1}. {book['title']}")
    
    if book['authors']:
        authors = json.loads(book['authors'])
        print(f"   👤 Authors: {', '.join(authors)}")
    
    if book['categories']:
        categories = json.loads(book['categories'])
        print(f"   🏷️  Categories: {', '.join(categories)}")
    
    if book['average_rating']:
        print(f"   ⭐ Rating: {book['average_rating']} ({book['ratings_count']} ratings)")
    
    if book['page_count']:
        print(f"   📄 Pages: {book['page_count']}")
    
    if book['description']:
        desc_preview = book['description'][:150] + "..." if len(book['description']) > 150 else book['description']
        print(f"   📝 Description: {desc_preview}")

print("\n" + "="*70)
print("🎉 Done! Check your output file.")
print("="*70)