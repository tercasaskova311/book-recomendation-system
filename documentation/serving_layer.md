## serving

## dashboard.py
- using simple streamlit syntax to create UI for personalized recommendation 
- showcasing recommendation based by user_id and isbn

- serving layer work on this base: 
- recommendations are saved in delta lake
- batch processing run every day(1k limit for google books api)
- if similarity score not yet in delta lake = cold start => avg value, max values... 

## Serving Layer

### `dashboard.py`
**Purpose**: Streamlit UI for exploring recommendations

**Features**:

1. **Personalized Recommendations**:
   ```python
   # User enters user_id → Get top-10 recommendations
   SELECT isbn, hybrid_score, rank
   FROM delta/final_recommendations
   WHERE user_id = ?
   ORDER BY rank LIMIT 10
   ```

2. **Similar Books Search**:
   ```python
   # User searches book title → Find similar books
   SELECT similar_isbn, similarity_score
   FROM delta/similarities
   WHERE isbn = ?
   ORDER BY similarity_score DESC LIMIT 10
   ```

3. **Data Exploration**:
   - Total books, ratings, users
   - Sample books with metadata
   - Category distribution

**Tech**:
- Reads directly from Delta Lake (fast!)
- Uses PySpark for data loading
- Caches results with `@st.cache_data`

**Example UI Flow**:
```
┌─────────────────────────────────────┐
│  📚 Book Recommendation System      │
├─────────────────────────────────────┤
│                                     │
│  Mode: [🎯 Personalized Recs]      │
│                                     │
│  Enter User ID: [276725]    [Get]  │
│                                     │
│  Your Top 10 Recommendations:       │
│  ┌───────────────────────────────┐ │
│  │ #1 Classical Mythology        │ │
│  │    Score: 8.1 (ALS: 8.3)     │ │
│  │    ISBN: 0195153448           │ │
│  └───────────────────────────────┘ │
│  ...                                │
└─────────────────────────────────────┘
```
