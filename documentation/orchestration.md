#Orchestration: Apache Airflow (optional) or Cron = Trigger: Daily at 2:00 AM OR when new_ratings > 1000 
#okay so I have these scripts:
#ingestion/load_data.py = enriching kaggle data by google_books_api (100 per day)- I should schedule that as well
#spark:
"""
- spark/content_similarity.py = fetching features from description, language, pages = computing similarity of vectors by lsh
- spark/user_preferences.py = getting ratings of users from kaggle => computing ALS score - each user top 100 books => idk yet how would this work potentionally in real life scenario - for now I have only static data from kaggle - there is many users tho - I am thinking tho that for a serving layer - I would create a possibility to rate a book(by isbn) and this would be add to the postgres rating csv and depends and add to the system?? idk this is kinda advnace..
- spark/hybrid_recs.py = getting the two recs score together - creating a recommendation based on both user rating of some other book and existing books
"""

#streamlit: app/dashboard.py - simple UI for the scrits - will be adjusted based on the othe scripts







# This creates:
#         check_data
#         /         \
# content_features  collaborative_filtering
#         \         /
#      hybrid_recs
#           |
#       validate