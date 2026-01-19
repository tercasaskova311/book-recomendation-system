### Data Ingestion Layer

#### `ingestion/fetching_data.py`
- **Purpose**: Fetch book metadata from Google Books API
- **Rate Limit**: 1,000 requests/day (free tier)
- **Enriches**: Title, description, authors, categories, page count, language
- **Output**: Structured book data for PostgreSQL

#### `ingestion/load_data.py`
- **Purpose**: Initialize PostgreSQL database and load Kaggle datasets
- **Creates**: `books`, `users`, `ratings` tables
- **Filters**: Only loads ratings for enriched books

#### `ingestion/ingestion.py`
- **Purpose**: Daily enrichment of unenriched books
- **Schedule**: Runs via Airflow DAG (`new_data.py`)
- **Limit**: 100 books/day (respects API quota)

---

## Architecture

### High-Level Flow

```
┌─────────────────┐
│  Kaggle CSVs    │
│ (270K books)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│ Google Books    │────▶│   PostgreSQL     │
│  API Enrichment │     │  (Source Tables) │
└─────────────────┘     └────────-─────────┘

```

---

## Data Pipeline

### 1. Data Ingestion

#### `fetching_data.py`
**Purpose**: Fetch book metadata from Google Books API

**Key Components**:
- `GoogleBooksClient`: API client with rate limiting (0.1s delay)
- ISBN-based search with automatic retries
- Extracts: title, description, authors, categories, publisher, page count, language

**Example**:
```python
client = GoogleBooksClient(api_key="YOUR_KEY")
book_data = client.search_by_isbn("9780547928227")  # The Hobbit
# Returns: {title, description, authors[], categories[], ...}
```

**Rate Limits**: 1,000 requests/day (Google Books API free tier)

---

#### `load_data.py`
**Purpose**: Initialize PostgreSQL database and load Kaggle datasets

**Workflow**:
1. **Create Database**: Sets up `book_recommendations` database
2. **Create Schema**: Executes `schema.sql` to create tables:
   - `books`: Book metadata (enriched + original Kaggle data)
   - `users`: User profiles (location, age)
   - `ratings`: User-book ratings (1-10 scale)
   - Pre-computed tables: `book_features`, `user_factors`, `item_factors`, etc.

3. **Load Data**:
   - Books: 270K from Kaggle → PostgreSQL
   - Users: 278K user profiles
   - Ratings: 1M user ratings (filtered to enriched books only)

**Database Schema**:
```sql
books (
  isbn VARCHAR(13) PRIMARY KEY,
  title, description, authors[], categories[],
  publisher, published_date, page_count, language,
  enriched BOOLEAN, data_source VARCHAR(20)
)

ratings (
  rating_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users,
  isbn VARCHAR(13) REFERENCES books,
  rating INTEGER CHECK (1-10)
)
```

---

#### `data_final.py` (Enrichment Script)
**Purpose**: Combine Kaggle metadata with Google Books API data

**Process**:
1. Load `Books.csv` (270K books with basic metadata)
2. For each book:
   - Search by ISBN in Google Books API
   - Extract rich metadata (description, categories, etc.)
   - Merge with Kaggle data
3. Save to `books_enriched.csv`

**Enrichment Stats** (from 1K sample):
- Successfully enriched: ~60-70% of books
- Books with descriptions: ~50%
- Books with categories: ~40%

**Output**:
```csv
isbn,title,description,authors,categories,publisher,...
9780547928227,"The Hobbit","In a hole...",["J.R.R. Tolkien"],["Fantasy"],...
```
