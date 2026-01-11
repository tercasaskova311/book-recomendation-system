## fetching_data.py
- fetching data from google books api

## load_data.py
- creating sql schema and est postgres db
- loading kaggle csv files into postgres

## ingestion.py
- combining 1k books from kaggle books.csv with google books api metadata 
- enriching a pure metadata without any content related info by description, genre, pages, language, ...

# Book Recommendation System Documentation

A production-grade hybrid recommendation system combining collaborative filtering and content-based filtering to deliver personalized book recommendations.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Data Pipeline](#data-pipeline)
4. [ML Components](#ml-components)
5. [Serving Layer](#serving-layer)
6. [Development Guide](#development-guide)

---

## System Overview

### What It Does
- **Processes**: 270K books, 1M ratings from Kaggle dataset
- **Enriches**: Book metadata using Google Books API (descriptions, categories, language)
- **Recommends**: Personalized book suggestions combining user preferences and content similarity
- **Serves**: Real-time recommendations through Streamlit dashboard

### Tech Stack
- **Data Processing**: Apache Spark 3.5.5, Delta Lake 3.3.0
- **Orchestration**: Apache Airflow 2.9.0
- **Database**: PostgreSQL 13
- **ML**: PySpark MLlib (ALS, TF-IDF)
- **UI**: Streamlit
- **Infrastructure**: Docker Compose

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
└─────────────────┘     └────────┬─────────┘
                                 │
                                 ▼
                        ┌────────────────────┐
                        │   Spark Pipeline   │
                        │  - Content Feats   │
                        │  - User Prefs (ALS)│
                        │  - Hybrid Scores   │
                        └────────┬───────────┘
                                 │
                                 ▼
                        ┌────────────────────┐
                        │    Delta Lake      │
                        │  - Features        │
                        │  - Similarities    │
                        │  - Recommendations │
                        └────────┬───────────┘
                                 │
                                 ▼
                        ┌────────────────────┐
                        │  Streamlit UI      │
                        │  (User-facing)     │
                        └────────────────────┘
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
