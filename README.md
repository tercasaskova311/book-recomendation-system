# book-recomendation-system
Book recommendation system based by the content and reviews of other users.

A production-grade hybrid book recommendation system combining collaborative filtering (user behavior) and content-based filtering (book metadata) to deliver personalized book recommendations. The system processes 270K books and 1M ratings from Kaggle, enriched with Google Books API data, using offline batch processing for model training and online serving for fast user queries.


---

## Orchestration

### Airflow DAGs

#### `new_data.py`
**Purpose**: Daily enrichment of unenriched books

**Schedule**: 1:00 AM daily

**Workflow**:
```python
1. Query PostgreSQL for unenriched books (enriched=FALSE)
2. LIMIT 100 (Google API quota)
3. For each book:
   - Fetch metadata from Google Books API
   - UPDATE books SET description=..., enriched=TRUE
4. Track success rate
```

**Why 100/day**: Google Books API free tier = 1,000 requests/day (save quota)

---

#### `recommendation.py`
**Purpose**: Trigger Spark ML pipeline when new data arrives

**Schedule**: 2:00 AM daily

**Workflow**:
```python
1. Check data threshold:
   - new_ratings > 1,000 OR
   - new_books > 100
   
2. IF threshold met:
   - Run content_similarity.py (Spark)
   - Run user_preferences.py (Spark)
   - Run hybrid_recs.py (Spark)
   
3. ELSE:
   - Skip (no retraining needed)
```

**Dependency Graph**:
```
check_threshold
       ↓
   ┌───┴───┐
   ↓       ↓
content  collaborative
   ↓       ↓
   └───┬───┘
       ↓
  hybrid_recs
```

---

## Development Guide

### Setup

1. **Clone & Configure**:
   ```bash
   git clone <repo>
   cd book-recommendation-system
   cp .env.development.example .env.development
   # Edit .env.development with your configs
   ```

2. **Build & Start**:
   ```bash
   make init  # Builds images + starts all services
   ```

3. **Access Services**:
   - Airflow UI: http://localhost:8081
   - Spark UI: http://localhost:8080
   - Streamlit: http://localhost:8501
   - PostgreSQL: localhost:5060

### Makefile Commands

```bash
make up              # Start all services
make down            # Stop all services
make restart         # Restart everything

make spark-logs      # View Spark logs
make airflow-logs    # View Airflow logs
make dashboard-logs  # View Streamlit logs

make status          # Check container status
```

### File Structure

```
book-recommendation-system/
├── airflow/
│   ├── dags/
│   │   ├── new_data.py           # Daily enrichment
│   │   └── recommendation.py     # ML pipeline trigger
│   └── config/
│       └── pipeline_config.py    # Thresholds, hyperparameters
├── spark/
│   ├── content_similarity.py     # TF-IDF + LSH
│   ├── user_preferences.py       # ALS training
│   └── hybrid_recs.py            # Score combination
├── src/
│   ├── database/
│   │   └── schema.sql            # PostgreSQL schema
│   └── ingestion/
│       └── load_data.py          # Initial data load
├── app/
│   └── dashboard.py              # Streamlit UI
├── common/
│   └── spark_session.py          # Shared Spark config
├── delta/                        # Delta Lake storage
│   ├── book_features/
│   ├── similarities/
│   ├── collaborative_recommendations/
│   └── final_recommendations/
├── docker/
│   ├── airflow/
│   ├── spark/
│   └── dashboard/
├── config.py                     # Global config
├── fetching_data.py              # Google Books API client
├── data_final.py                 # Initial enrichment script
└── docker-compose.yml            # Service orchestration
```

---

## Performance

### Data Volume
- **Books**: 270K (78 enriched with full features)
- **Users**: 278K
- **Ratings**: 1M
- **Recommendations**: 100 per user × 278K users = 27.8M pre-computed

### Processing Times (Estimated)
- Content features: ~5 min
- ALS training: ~10 min
- Similarity computation: ~15 min (LSH)
- Hybrid recommendations: ~5 min
- **Total pipeline**: ~35 minutes

### Storage
- Delta Lake: ~500 MB (features + similarities)
- PostgreSQL: ~200 MB (source data)

---

## Future Enhancements

1. **Real-time Feedback Loop**:
   - User rates book → Immediate re-ranking
   - Online learning (incremental ALS updates)

2. **Advanced Features**:
   - Book cover image embeddings (CNN)
   - Reading level/difficulty scoring
   - Author similarity networks

3. **Scalability**:
   - Move to AWS EMR / Databricks for Spark
   - PostgreSQL → AWS RDS
   - Add Redis cache for hot recommendations

4. **A/B Testing**:
   - Test different α values (collaborative vs. content weight)
   - Evaluate diversity vs. accuracy trade-off

---

## Troubleshooting

### Common Issues

**Spark job fails**:
```bash
# Check Spark logs
make spark-logs

# Common fix: Increase memory
# Edit docker-compose.yml:
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
```

**PostgreSQL connection errors**:
```bash
# Verify DB is running
docker ps | grep postgres

# Test connection
docker exec -it book-recs-postgres psql -U terezasaskova -d book_recommendations
```

**Airflow DAG not running**:
```bash
# Check scheduler logs
make airflow-logs

# Restart Airflow
make airflow-down && make airflow-up
```

---

## Credits

- **Dataset**: Kaggle Book-Crossing Dataset
- **Enrichment**: Google Books API
- **Framework**: Apache Spark, Airflow, Delta Lake
- **Author**: [Your Name]
┌─────────────────────────────────────────────────────────────────────────┐
│                           SYSTEM ARCHITECTURE                           │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ 1. DATA INGESTION LAYER (One-time + Scheduled Updates)                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Initial Load:                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────────┐            │
│  │ Kaggle CSVs  │───>│ Google Books │───>│   PostgreSQL    │            │
│  │ - Books      │    │     API      │    │   - books       │            │
│  │ - Ratings    │    │  Enrichment  │    │   - ratings     │            │
│  │ - Users      │    │              │    │   - users       │            │
│  └──────────────┘    └──────────────┘    └─────────────────┘            │
│       270K books          API fetch           Relational DB             │
│       1M ratings       (description,                                    │
│       278K users        categories)                                     │
│                                                                         │
│  Ongoing Updates (Daily Python Script):                                 │
│  ┌──────────────┐                       ┌─────────────────┐             │
│  │ New Books    │─────────────────────>│   PostgreSQL    │              │
│  │ (Google API) │                       │   INSERT books  │             │
│  └──────────────┘                       └─────────────────┘             │
│                                                                         │
│  ┌──────────────┐                       ┌─────────────────┐             │
│  │ User Ratings │─────────────────────>│   PostgreSQL    │              │
│  │ (via FastAPI)│                       │  INSERT ratings │             │
│  └──────────────┘                       └─────────────────┘             │
│                                                ↓                        │
│                                    Check new data volume                │
│                                    If > threshold → trigger             │
│                                         Spark retraining                │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. STORAGE LAYER (PostgreSQL)                                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Source Tables:                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ books (isbn PK)                                             │        │
│  │  - title, authors[], publisher, published_year              │        │
│  │  - description, categories[], page_count, language          │        │
│  │  - cover_url, preview_link, data_source                     │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ ratings (rating_id PK, user_id FK, isbn FK)                 │        │
│  │  - rating (1-10), timestamp                                 │        │
│  │  - Indexed: user_id, isbn, rating                           │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ users (user_id PK)                                          │        │
│  │  - location, age, created_at                                │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  Pre-computed Tables (Updated by Spark):                                │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ book_features (isbn PK)                                     │        │
│  │  - tfidf_vector[], category_vector[]                        │        │
│  │  - page_count_norm, year_norm                               │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ user_factors (user_id PK)                                   │        │
│  │  - factors[] (latent embeddings from ALS)                   │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ item_factors (isbn PK)                                      │        │
│  │  - factors[] (latent embeddings from ALS)                   │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ book_similarities (isbn_a, isbn_b) composite PK             │        │
│  │  - similarity_score (cosine similarity)                     │        │
│  │  - Stores top-50 similar books per book                     │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐        │
│  │ recommendations_cache (user_id, isbn) composite PK          │        │
│  │  - score (hybrid: α*collaborative + (1-α)*content)          │        │
│  │  - rank (1 = top recommendation)                            │        │
│  │  - generated_at timestamp                                   │        │
│  │  - Stores top-100 recommendations per user                  │        │
│  └─────────────────────────────────────────────────────────────┘        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. BATCH PROCESSING LAYER (PySpark - Scheduled Nightly)                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Orchestration: Apache Airflow (optional) or Cron                       │
│  Trigger: Daily at 2:00 AM OR when new_ratings > 1000                   │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ Spark Job 1: Feature Engineering                              │     │
│  ├───────────────────────────────────────────────────────────────┤     │
│  │ Input:  books table (descriptions, categories, metadata)      │     │
│  │ Process:                                                       │     │
│  │   1. TF-IDF on book descriptions (Spark MLlib)                │     │
│  │      - Tokenization → Stop words removal → TF-IDF vectors     │     │
│  │      - Primary signal for content similarity                  │     │
│  │   2. One-hot encode categories (Fiction, Mystery, etc.)       │     │
│  │   3. Normalize page_count (min-max scaling)                   │     │
│  │   4. Normalize published_year                                 │     │
│  │   5. Encode language (categorical)                            │     │
│  │ Output: book_features table                                   │     │
│  │   - Combined feature vector per book                          │     │
│  │   - Used for content-based similarity                         │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                ↓                                         │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ Spark Job 2: Collaborative Filtering (ALS)                    │     │
│  ├───────────────────────────────────────────────────────────────┤     │
│  │ Input:  ratings table (user_id, isbn, rating triplets)        │     │
│  │ Process:                                                       │     │
│  │   1. Matrix Factorization using ALS (Alternating Least Sq.)   │     │
│  │      - Learns latent factors for users and items              │     │
│  │      - Handles implicit feedback (rating > 0)                 │     │
│  │   2. Hyperparameter tuning (rank, regParam, iterations)       │     │
│  │      - Cross-validation on held-out test set                  │     │
│  │   3. Model evaluation (RMSE, MAE, precision@K)                │     │
│  │ Output:                                                        │     │
│  │   - user_factors table (user embeddings)                      │     │
│  │   - item_factors table (book embeddings)                      │     │
│  │   - Model metrics logged for monitoring                       │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                ↓                                         │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ Spark Job 3: Content-Based Similarity                         │     │
│  ├───────────────────────────────────────────────────────────────┤     │
│  │ Input:  book_features table                                   │     │
│  │ Process:                                                       │     │
│  │   1. Compute pairwise cosine similarity between books         │     │
│  │      - Focus on TF-IDF vectors (description similarity)       │     │
│  │      - Weight: 70% description, 20% categories, 10% metadata  │     │
│  │   2. For each book, keep top-50 most similar books            │     │
│  │      - Filters out low similarity scores (< 0.1 threshold)    │     │
│  │ Output: book_similarities table                               │     │
│  │   - isbn_a → isbn_b with similarity_score                     │     │
│  │   - Indexed for fast lookup                                   │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                ↓                                         │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ Spark Job 4: Hybrid Recommendations                           │     │
│  ├───────────────────────────────────────────────────────────────┤     │
│  │ Input:  user_factors, item_factors, book_similarities         │     │
│  │ Process:                                                       │     │
│  │   1. For each user:                                           │     │
│  │      a) Collaborative score: dot(user_factor, item_factor)    │     │
│  │         - Predicts rating for unrated books                   │     │
│  │      b) Content score: avg similarity to user's top books     │     │
│  │         - If user liked "Harry Potter", boost similar fantasy │     │
│  │      c) Hybrid score: α*collab + (1-α)*content                │     │
│  │         - α = 0.7 (favor collaborative filtering)             │     │
│  │   2. Rank top-100 books per user by hybrid score              │     │
│  │   3. Filter out already-rated books                           │     │
│  │   4. Apply diversity boost (avoid recommending same author)   │     │
│  │ Output: recommendations_cache table                           │     │
│  │   - Pre-computed recommendations ready for instant serving    │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Performance: Processes 1M ratings in ~10-20 minutes on local Spark     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. SERVING LAYER (FastAPI + PostgreSQL)                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  FastAPI Endpoints (Response time: <100ms)                              │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ GET /recommendations/{user_id}                                │     │
│  │ ─────────────────────────────────────────────────────────────  │     │
│  │ Returns: Top-N personalized recommendations                   │     │
│  │ Logic:                                                         │     │
│  │   SELECT isbn, score, rank                                    │     │
│  │   FROM recommendations_cache                                  │     │
│  │   WHERE user_id = {user_id}                                   │     │
│  │   ORDER BY rank                                               │     │
│  │   LIMIT 10;                                                   │     │
│  │                                                               │     │
│  │   JOIN with books table for metadata                          │     │
│  │ Use Case: "Show me my personalized recommendations"           │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ GET /similar_books/{isbn}                                     │     │
│  │ ──────────────────────────────────────────────────────────────│     │
│  │ Returns: Books similar to given ISBN                          │     │
│  │ Logic:                                                         │     │
│  │   SELECT b.isbn_b, b.similarity_score, books.*               │     │
│  │   FROM book_similarities b                                    │     │
│  │   JOIN books ON b.isbn_b = books.isbn                        │     │
│  │   WHERE b.isbn_a = {isbn}                                    │     │
│  │   ORDER BY similarity_score DESC                             │     │
│  │   LIMIT 10;                                                   │     │
│  │                                                               │     │
│  │ Use Case: "I just read 'The Hobbit', show me similar books"  │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ POST /rate_book                                               │     │
│  │ ──────────────────────────────────────────────────────────────│     │
│  │ Body: {user_id, isbn, rating}                                │     │
│  │ Logic:                                                         │     │
│  │   INSERT INTO ratings (user_id, isbn, rating, timestamp)     │     │
│  │   VALUES (...);                                               │     │
│  │                                                               │     │
│  │   // Immediate response with similar books                    │     │
│  │   SELECT similar books from book_similarities                 │     │
│  │                                                               │     │
│  │   // Trigger retraining check (async)                         │     │
│  │   if new_ratings_count > 1000:                                │     │
│  │       schedule_spark_job()                                    │     │
│  │                                                               │     │
│  │ Use Case: User rates a book, get instant feedback             │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ GET /search_books?query={title}                              │     │
│  │ ──────────────────────────────────────────────────────────────│     │
│  │ Returns: Books matching search query                          │     │
│  │ Logic:                                                         │     │
│  │   SELECT * FROM books                                         │     │
│  │   WHERE title ILIKE '%{query}%'                              │     │
│  │      OR array_to_string(authors, ' ') ILIKE '%{query}%'      │     │
│  │   LIMIT 20;                                                   │     │
│  │                                                               │     │
│  │ Use Case: Search for books to rate or explore                │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Key Design: NO ML computation in serving layer!                        │
│             All recommendations pre-computed by Spark                   │
│             FastAPI only does fast SQL queries                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 5. USER INTERFACE (Streamlit)                                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Features:                                                               │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ 1. User Dashboard                                             │     │
│  │    - Login/select user_id                                     │     │
│  │    - View personalized recommendations                        │     │
│  │    - Display: book cover, title, author, rating, categories   │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ 2. Book Search & Details                                      │     │
│  │    - Search bar for books                                     │     │
│  │    - Click → show full details (description, metadata)        │     │
│  │    - "Find Similar Books" button                              │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ 3. Rating Interface                                           │     │
│  │    - Star rating widget (1-10)                                │     │
│  │    - Submit → POST to FastAPI                                 │     │
│  │    - Instant feedback with similar books                      │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────┐     │
│  │ 4. Recommendation Explanation (optional)                      │     │
│  │    - Show why book was recommended:                           │     │
│  │      "Based on your love for Harry Potter and high ratings    │     │
│  │       for fantasy books"                                      │     │
│  │    - Display hybrid score breakdown                           │     │
│  └───────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  Technology: Streamlit + requests to FastAPI backend                    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘