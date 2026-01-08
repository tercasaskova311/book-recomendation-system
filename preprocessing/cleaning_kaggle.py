import pandas as pd 

books = pd.read_csv('data/Books.csv')
ratings = pd.read_csv('data/Ratings.csv')
users = pd.read_csv('data/Users.csv')
books_ratings = pd.merge(books, ratings, on = 'ISBN', how = 'outer')
books_ratings_users = pd.merge(
    books_ratings,
    users,
    on='User-ID',
    how='outer'          
)

books = books['ISBN'].dropna()

book2 = pd.read_csv('data/books_enriched.csv')
books_cat_type = book2['categories']
books_cat_type.to_csv('categories.csv')

print(books_cat_type)


