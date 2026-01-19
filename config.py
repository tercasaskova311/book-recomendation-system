import os
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5060')
DB_NAME = os.getenv('DB_NAME', 'book_recommendations')
DB_USER = os.getenv('DB_USER', 'terezasaskova')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Database connection dictionary (for psycopg2)
DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'database': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD
}

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}" #adress of the db...
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# =============== SPARK =============================================

# Memory settings (adjust based on your RAM)
SPARK_DRIVER_MEMORY = "4g"      # Coordinates the job
SPARK_EXECUTOR_MEMORY = "4g"    # Does the processing
SPARK_EXECUTOR_CORES = 2        # CPU cores per executor

# Local mode settings
SPARK_MASTER = "local[*]"       # Use all available cores
# or
# SPARK_MASTER = "local[4]"     # Use exactly 4 cores

POSTGRES_JDBC_JAR = "/Users/terezasaskova/spark-jars/postgresql-42.7.1.jar"
#java driver file...


# ============================================================
# MODEL PARAMETERS
# ============================================================

ALS_RANK = 50
ALS_MAX_ITER = 10
ALS_REG_PARAM = 0.01

SIMILARITY_THRESHOLD = 0.1
TOP_K_SIMILAR = 50

MIN_DESCRIPTION_LENGTH = 50
CATEGORY_TOP_N = 5

ALPHA = 0.7
MIN_CONTENT_SIMILARITY = 0.1 #filter weak content similarities..
N_RECOMMENDATIONS = 100
# ============================================================
# FILE PATHS
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

KAGGLE_BOOKS = os.path.join(DATA_DIR, 'Books.csv')
KAGGLE_RATINGS = os.path.join(DATA_DIR, 'Ratings.csv')
KAGGLE_USERS = os.path.join(DATA_DIR, 'Users.csv')
ENRICHED_BOOKS = os.path.join(DATA_DIR, 'books_enriched.csv')

SCHEMA_SQL = os.path.join(PROJECT_ROOT, 'src/database/schema.sql')


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DELTA_BASE = os.path.join(PROJECT_ROOT, 'delta')

DELTA_USER_RECS = os.path.join(DELTA_BASE, "user_recs")
DELTA_USER_FACTORS = os.path.join(DELTA_BASE, "user_factors")
DELTA_ITEM_FACTORS = os.path.join(DELTA_BASE, "item_factors")
DELTA_ALS_MODEL = os.path.join(DELTA_BASE, "als_model")

DELTA_SIMILARITIES = os.path.join(DELTA_BASE, "similarities")
DELTA_SIM_FEATURES = os.path.join(DELTA_BASE, "sim_features")
DELTA_DESCRIPTION_EMBEDDINGS = os.path.join(DELTA_BASE, "descr_emb")

DELTA_FINAL_RECS = os.path.join(DELTA_BASE, "final_recommendations")

