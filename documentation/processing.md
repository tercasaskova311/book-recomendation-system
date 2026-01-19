### Batch Processing Layer (Spark)

#### `spark/content_similarity.py`
**Pipeline:**
1. **Text Embeddings**: Uses `sentence-transformers` (all-MiniLM-L6-v2) to convert descriptions → 384-dim vectors
2. **Category Encoding**: One-hot encodes top-5 categories
3. **Metadata Normalization**: Normalizes page count, language
4. **Weighted Features**: 70% description, 20% categories, 10% metadata
5. **LSH Similarity**: Computes top-50 similar books per book using Locality-Sensitive Hashing

**Output**: `delta/similarities`, `delta/sim_features`

#### `spark/user_preferences.py`
**Algorithm**: Alternating Least Squares (ALS) - Matrix Factorization
- **Input**: User-item rating matrix (1M ratings)
- **Hyperparameters**: rank=50, iterations=10, regParam=0.1
- **Output**: 
  - `delta/user_factors`: User latent embeddings (50-dim)
  - `delta/item_factors`: Book latent embeddings (50-dim)
  - `delta/user_recs`: Top-100 books per user (ALS scores)

#### `spark/hybrid_recs.py`
**Combination Strategy:**
```python
hybrid_score = 0.7 * als_score + 0.3 * content_score
```
- **ALS Score**: Predicted rating from collaborative filtering
- **Content Score**: Max similarity to user's liked books (rating ≥ 7)
- **Filters**: Removes already-rated books
- **Output**: `delta/final_recommendations` - Top-100 per user

### Orchestration Layer (Airflow)

#### DAG: `new_data.py`
- **Schedule**: Hourly (configurable)
- **Task**: Enrich 100 unenriched books from Google Books API
- **Respects**: API rate limits

#### DAG: `recommendation.py`
- **Schedule**: Every 20 minutes (configurable)
- **Tasks**:
  1. `generate_content_features` → Content similarity computation
  2. `train_als_model` → Collaborative filtering
  3. `generate_hybrid_recs` → Combine scores
- **Dependency**: Tasks run sequentially



## Details

### Content-Based Filtering

**Feature Engineering:**
```python
# 1. Description Embeddings (384-dim)
model = SentenceTransformer('all-MiniLM-L6-v2')
embeddings = model.encode(descriptions)

# 2. Category Encoding (5-dim one-hot)
categories = ['Fiction', 'Biography', 'Mystery', 'Science', 'History']

# 3. Metadata (2-dim)
page_count_normalized = (pages - min) / (max - min)
language_encoded = one_hot(language)

# 4. Weighted Combination
final_vector = 0.7 * embedding + 0.2 * categories + 0.1 * metadata
```

**Similarity Computation:**
- Uses **LSH** (Locality-Sensitive Hashing) for scalable cosine similarity
- Buckets similar vectors together → avoids O(n²) comparisons
- Keeps top-50 similar books per book (similarity ≥ 0.1)

### Collaborative Filtering (ALS)

**Matrix Factorization:**
```
R ≈ U × V^T

Where:
- R: User-item rating matrix (sparse)
- U: User factors (278K × 50)
- V: Item factors (270K × 50)
```

**Training:**
- **Alternating Least Squares** optimizes U and V iteratively
- **Cold Start**: Drops users/items with no ratings (strategy='drop')
- **Implicit Feedback**: Uses explicit ratings (1-10 scale)

### Hybrid Approach

**Score Combination:**
```python
# For each user-book pair:
als_score = dot(user_factor, item_factor)  # Predicted rating
content_score = max(similarity(book, user_liked_books))  # Max similarity

hybrid_score = 0.7 * als_score + 0.3 * content_score
```

**Why 70/30 split?**
- Collaborative filtering (ALS) is stronger when sufficient rating data exists
- Content similarity handles cold-start and adds diversity
- α=0.7 empirically balances accuracy vs. serendipity

---
