# airflow/config/pipeline_config.yaml

# When to trigger retraining
new_ratings_threshold: 1000
new_books_threshold: 100

# Model parameters (used by Spark scripts)
als:
  rank: 50
  max_iter: 10
  reg_param: 0.1

content:
  tfidf_features: 2000
  top_k_similar: 50

hybrid:
  alpha: 0.7  # 70% collaborative, 30% content