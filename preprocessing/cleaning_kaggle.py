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

print(books.head())
print(ratings.head())
print(users.head())


