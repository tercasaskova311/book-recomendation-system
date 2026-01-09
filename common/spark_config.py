"""
Unified Spark Session Builder for Book Recommendation System
  - PostgreSQL JDBC connection
  - Memory configuration
  - Master selection (local vs cluster)
  - Environment-based overrides
"""
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession


def _in_spark_submit() -> bool:
    """
    Detect if running under spark-submit
    Returns True if spark-submit arguments detected in environment
    """
    pyspark_args = (os.getenv("PYSPARK_SUBMIT_ARGS") or "") + " "
    spark_args = (os.getenv("SPARK_SUBMIT_ARGS") or "")
    return "--master" in (pyspark_args + spark_args) or "spark-submit" in (pyspark_args + spark_args)


def get_spark_session(
    app_name: str,
    postgres_jar_path: Optional[str] = None,
    extra_conf: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create or get existing Spark session with PostgreSQL support
    
    Args:
        app_name: Name of the Spark application
        postgres_jar_path: Path to PostgreSQL JDBC JAR (if None, uses POSTGRES_JDBC_JAR env var)
        extra_conf: Additional Spark configurations
        
    Returns:
        Configured SparkSession
        
    Environment Variables:
        FORCE_MASTER: Override master URL (e.g., "local[4]", "spark://host:7077")
        SPARK_DRIVER_MEMORY: Driver memory (default: 4g)
        SPARK_EXECUTOR_MEMORY: Executor memory (default: 4g)
        POSTGRES_JDBC_JAR: Path to PostgreSQL JDBC JAR
        SPARK_JARS: Additional JARs (comma-separated)
        
    Examples:
        # Local development
        spark = get_spark_session("FeatureEngineering")
        
        # With custom memory
        spark = get_spark_session(
            "FeatureEngineering",
            extra_conf={"spark.driver.memory": "8g"}
        )
        
        # In production (with spark-submit)
        # spark-submit --master yarn --deploy-mode cluster feature_engineering.py
        # (master will be set by spark-submit, not by this function)
    """
    
    # Get configuration from environment
    force_master = (os.getenv("FORCE_MASTER") or "").strip()
    driver_memory = (os.getenv("SPARK_DRIVER_MEMORY") or "4g").strip()
    executor_memory = (os.getenv("SPARK_EXECUTOR_MEMORY") or "4g").strip()
    
    # PostgreSQL JAR
    if postgres_jar_path is None:
        postgres_jar_path = os.getenv("POSTGRES_JDBC_JAR")
    
    if postgres_jar_path:
        postgres_jar_path = os.path.abspath(os.path.expanduser(postgres_jar_path))
        
        # Verify JAR exists
        if not os.path.exists(postgres_jar_path):
            raise FileNotFoundError(
                f"PostgreSQL JDBC JAR not found: {postgres_jar_path}\n"
                f"Download with:\n"
                f"  mkdir -p ~/spark-jars\n"
                f"  curl -o ~/spark-jars/postgresql-42.7.1.jar "
                f"https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"
            )
    
    # Additional JARs from environment
    extra_jars = (os.getenv("SPARK_JARS") or "").strip()
    
    # Build session
    builder = SparkSession.builder.appName(app_name)
    
    # --- Master Selection ---
    if force_master:
        # Explicit override via environment
        builder = builder.master(force_master)
    elif not _in_spark_submit():
        # Running locally (not via spark-submit)
        builder = builder.master("local[*]")
    # else: in spark-submit, let submit command decide master
    
    # --- JARs Configuration ---
    all_jars = []
    if postgres_jar_path:
        all_jars.append(postgres_jar_path)
    if extra_jars:
        all_jars.extend([j.strip() for j in extra_jars.split(",") if j.strip()])
    
    if all_jars:
        jars_str = ",".join(all_jars)
        builder = (builder
            .config("spark.jars", jars_str)
            .config("spark.driver.extraClassPath", jars_str)
            .config("spark.executor.extraClassPath", jars_str)
        )
    
    # --- Memory Configuration ---
    builder = (builder
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
    )
    
    # --- Network & Timeout ---
    builder = (builder
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "600s")
        .config("spark.rpc.askTimeout", "600s")
    )
    
    # --- SQL & Session ---
    builder = (builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
    )
    
    # --- Streaming (if needed later) ---
    builder = builder.config("spark.streaming.stopGracefullyOnShutdown", "true")
    
    # --- User Overrides ---
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, str(value))
    
    # Create or get existing session
    spark = builder.getOrCreate()
    
    # Set log level
    log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def stop_spark_session(spark: SparkSession):
    """
    Gracefully stop Spark session
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()