# 📚 Book Recommendation System

Recommendation system built on spark, airflow and postgres, combining **collaborative filtering** (user ratings) and **content-based filtering** (book descriptions and metadata) to deliver scalable book recommendations.

## Overview

**What it does:**
- Process in batches books with ratings from Kaggle dataset with **270K books** and **1M+ ratings** 
- Enriches book metadata using **Google Books API** (descriptions, categories, language)
- Generates **personalized recommendations** using hybrid ML models
- Serves results through **Streamlit dashboard**

**Tech:**
- **Data Processing**: Apache Spark 3.5.0 with Delta Lake
- **ML**: ALS (Collaborative Filtering) + Sentence Transformers (Content-Based)
- **Orchestration**: Apache Airflow 2.9.0
- **Database**: PostgreSQL 15
- **UI**: Streamlit
- **Infrastructure**: Docker 

## Data Sources
- **Kaggle Book-Crossing Dataset**: [Link](https://www.kaggle.com/datasets/ruchi798/bookcrossing-dataset)
  - 270K books, 1M ratings, 278K users
- **Google Books API**: [Documentation](https://developers.google.com/books)
  - Free tier: 1,000 requests/day
  - Provides: descriptions, categories, metadata

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kaggle CSV              ──┐                                    │
│                            ├──> Google Books API Enrichment     │
│  User Ratings      ────────┘         ↓                          │
│                              ┌───────────────┐                  │
│                              │  PostgreSQL   │                  │
│                              │  - books      │                  │
│                              │  - ratings    │                  │
│                              │  - users      │                  │
│                              └───────┬───────┘                  │
└──────────────────────────────────────┼──────────────────────────┘
                                       ↓
┌─────────────────────────────────────────────────────────────────┐
│                    BATCH PROCESSING (Spark)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐    ┌──────────────────────┐           │
│  │ Content Similarity   │    │   User Preferences   │           │
│  │ - Sentence Embeddings│    │   - ALS Model        │           │
│  │ - Category Encoding  │    │   - User Factors     │           │
│  │ - LSH Similarity     │    │   - Item Factors     │           │
│  └──────────┬───────────┘    └──────────┬───────────┘           │
│             │                           │                       │
│             └───────────┬───────────────┘                       │
│                         ↓                                       │
│              ┌──────────────────── ─┐                           │
│              │  Hybrid Combiner     │                           │
│              │ α*ALS + (1-α)*Content│                           │
│              └──────────┬────────── ┘                           │
│                         ↓                                       │
│                  ┌─────────────┐                                │
│                  │ Delta Lake  │                                │
│                  │ - Features  │                                │
│                  │ - Similarities                               │
│                  │ - Final Recs│                                │
│                  └──────┬──────┘                                │
└─────────────────────────┼───────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│                       SERVING LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│              ┌──────────────────────┐                           │
│              │  Streamlit Dashboard │                           │
│              │  - User Recs         │                           │
│              │  - Similar Books     │                           │
│              │  - Data Explorer     │                           │
│              └──────────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
```

---

##  Start

### 1. Clone & Configure
```bash
git clone <repository-url>

# Copy environment template
cp .env.development.example .env.development

#Create Google API KEY and enable Google Books

# Edit with your settings (PostgreSQL credentials, Google API key)
nano .env.development
```

### 2. Initialize System
```bash
# Build all Docker images and start services
make init

# This will:
# - Build Spark, Airflow, Dashboard images
# - Start PostgreSQL
# - Initialize Airflow database
# - Start all services
#- load kaggle data into DB
```

### 4. Output

```bash

# Access Airflow UI: http://localhost:8080 (admin/admin) => Enable and trigger the 'new_data' & 'recommendation' DAG
# Dashboard UI: http://localhost:8501 => Enter a User ID to get personalized recommendations

```

---

### Repo structure

```
book-recommendation-system/
├── airflow/
│   ├── dags/
│   │   ├── new_data.py           # Daily enrichment DAG
│   │   └── recommendation.py     # ML pipeline DAG
│   └── config/
│       └── pipeline_config.yaml  # Schedule configuration
├── spark/
│   ├── content_similarity.py     # Content-based features
│   ├── user_preferences.py       # ALS training
│   └── hybrid_recs.py            # Score combination
├── ingestion/
│   ├── fetching_data.py          # Google Books API client
│   ├── load_data.py              # Initial data load
│   ├── ingestion.py              # Daily enrichment
│   └── database/
│       └── schema.sql            # PostgreSQL schema
├── app/
│   └── dashboard.py              # Streamlit UI
├── common/
│   └── spark_session.py          # Shared Spark config
├── delta/                        # Delta Lake storage
│   ├── similarities/
│   ├── user_factors/
│   ├── item_factors/
│   └── final_recommendations/
├── docker/
│   ├── postgres/
│   ├── spark/
│   ├── airflow/
│   └── dashboard/
├── config.py                     # Global configuration
├── Makefile                      # Automation commands
└── README.md
```
---


