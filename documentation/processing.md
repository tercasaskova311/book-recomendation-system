## content_similarity.py
Input:  books table (descriptions, categories, metadata)                                                         
1. TF-IDF on book descriptions (Spark MLlib)             
 - Tokenization → Stop words removal → TF-IDF vectors    
- Primary signal for content similarity                 
2. One-hot encode categories (Fiction, Mystery, etc.)       
3. Normalize page_count (min-max scaling)                   
4. Normalize published_year                                 
5. Encode language (categorical)                            
Output: book_features table                                   
- Combined feature vector per book                          
- Used for content-based similarity   



## user_preerences.py
Collaborative Filtering 
- input: user x rating matrix
    - User-ID
    - ISBN
    -rating: Book-Rating 
- goal => learn latent embeddings for users and items...
    - U = user factors matrix
    - V = itamfactors matrix
- output = can recs books to users by dot products  or embeddings


## hybrid_recs.py
- combing about scores together
- using alpha 0.7 fro content and 0.3 for ratings
- providing personalized recs for users


---

### 2. Batch Processing (Spark)

All Spark jobs run nightly via Airflow orchestration.

#### `content_similarity.py`
**Purpose**: Generate content-based features and compute book similarities

**Input**: `books` table from PostgreSQL

**Pipeline**:

1. **Text Processing (TF-IDF)**:
   ```python
   # Description → TF-IDF vectors
   Tokenizer → StopWordsRemover → HashingTF → IDF
   # Output: 2000-dimensional sparse vectors
   ```

2. **Category Encoding**:
   ```python
   # Top 5 categories (Fiction, Biography, etc.)
   # One-hot encoding: [0, 1, 0, 0, 1] → Fiction + Medical
   ```

3. **Metadata Normalization**:
   ```python
   # Page count: (x - min) / (max - min)  → [0, 1]
   # Language: One-hot encoding (en, de, es, fr, ...)
   ```

4. **Feature Combination** (Weighted):
   ```python
   Final Vector = 0.7 * TF-IDF + 0.2 * Categories + 0.1 * Metadata
   # TF-IDF dominates (descriptions most important)
   ```

5. **Similarity Computation** (LSH):
   ```python
   # Locality-Sensitive Hashing for scalable cosine similarity
   # For each book → Find top-50 most similar books
   # Filter: similarity_score >= 0.1
   ```

**Output** (Delta Lake):
- `delta/book_features`: Feature vectors per book
- `delta/similarities`: Top-50 similar books per ISBN

**Example**:
```
ISBN: 9780547928227 (The Hobbit)
Similar Books:
  - 0345339681 (Lord of the Rings) → 0.89 similarity
  - 0345325419 (Fellowship of the Ring) → 0.87
  - ...
```

---

#### `user_preferences.py`
**Purpose**: Learn user preferences via collaborative filtering (ALS)

**Input**: `ratings` table (user_id, isbn, rating)

**Algorithm**: Alternating Least Squares (ALS)
- Matrix factorization: R ≈ U × V^T
- U: User factors (latent user preferences)
- V: Item factors (latent book characteristics)

**Hyperparameters**:
```python
ALS_RANK = 50          # Latent factors dimension
ALS_MAX_ITER = 10      # Training iterations
ALS_REG = 0.1          # L2 regularization
MIN_RATING = 1         # Filter implicit zeros
```

**Process**:
1. **Index Users & Items**:
   ```python
   # String → Integer mapping (required for matrix factorization)
   user_id "276725" → user_idx 0
   isbn "0195153448" → item_idx 0
   ```

2. **Train ALS Model**:
   ```python
   # 80/20 train-test split
   model = ALS(rank=50, maxIter=10, regParam=0.1)
   model.fit(train_data)
   ```

3. **Generate Recommendations**:
   ```python
   # For each user → Top 100 books by predicted rating
   # Dot product: user_factors · item_factors^T
   ```

**Output** (Delta Lake):
- `delta/user_factors`: 50-dim embeddings per user
- `delta/item_factors`: 50-dim embeddings per book
- `delta/collaborative_recommendations`: Top-100 books per user (ALS scores)

**Example**:
```
User 276725:
  - ISBN 0195153448 → ALS score 8.3
  - ISBN 0002005018 → ALS score 8.1
  - ...
```

---

#### `hybrid_recs.py`
**Purpose**: Combine collaborative + content signals into final recommendations

**Input**:
- `delta/collaborative_recommendations` (ALS scores)
- `delta/similarities` (content similarities)
- `ratings` table (user history)

**Algorithm**:
```python
# For each user:
# 1. Get ALS recommendations (collaborative signal)
als_score = dot(user_factors, item_factors)

# 2. Get content-based score
# Find books similar to user's highly-rated books (rating >= 7)
content_score = max_similarity(user_liked_books, candidate_book)

# 3. Combine with weighted average
hybrid_score = 0.7 * als_score + 0.3 * content_score
```

**Process**:
1. Load user history (books rated >= 7)
2. For each user:
   - Get ALS recommendations
   - Compute content scores (similarity to user's liked books)
   - Combine scores: `α=0.7` (favor collaborative filtering)
3. Filter out already-rated books
4. Rank top-100 per user

**Output** (Delta Lake):
- `delta/final_recommendations`:
  ```
  user_id | isbn | hybrid_score | als_score | content_score | rank
  276725  | 0195153448 | 8.1 | 8.3 | 7.5 | 1
  276725  | 0002005018 | 7.9 | 8.1 | 7.3 | 2
  ```

**Cold Start Handling**:
- **New users** (no ratings): Use content-based only (popular books in favorite genres)
- **New books** (no ratings): Use content similarity to existing books
- **Warm users**: Full hybrid approach

---

