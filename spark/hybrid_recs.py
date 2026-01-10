#
"""
 Hybrid Recommendations
- Input:  user_factors, item_factors, book_similarities:                                               
    1. For each user: Hybrid score: α*collab + (1-α)*content              
    - α = 0.7 (favor collaborative filtering)            
    2. Rank top-100 books per user by hybrid score             
    3. Filter out already-rated books                          
    4. Apply diversity boost (avoid recommending same author)  

- Output: recommendations_cache table                          
- Pre-computed recommendations ready for instant serving 
"""


def load_content_sim (spark, df):

    return df

def load_ALS_score(df):
    df.read(
        .format("delta")
        .model("read")
        .pah("/Users/terezasaskova/Desktop/book-recomendation-system/delta/similarities")
    )
    return df

def hybrid_score(df, df):
    
