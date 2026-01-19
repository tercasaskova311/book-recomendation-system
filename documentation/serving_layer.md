### Serving Layer

#### `app/dashboard.py`
**Features:**
1. **Personalized Recommendations**
   - Input: User ID
   - Output: Top-10 recommendations with scores
   
2. **Similar Books Search**
   - Input: Book title or ISBN
   - Output: Top-10 similar books with similarity scores
   
3. **Data Explorer**
   - Dataset statistics
   - Sample books with metadata

**Data Source**: Reads directly from Delta Lake (fast, no API calls)


