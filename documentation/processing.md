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

