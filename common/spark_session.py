# common/spark_session.py
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from config import POSTGRES_JDBC_JAR, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY
from delta import configure_spark_with_delta_pip


def _in_spark_submit() -> bool:
    return "PYSPARK_SUBMIT_ARGS" in os.environ

def get_spark_session(
    app_name: Optional[str] = None,
    extra_conf: Optional[Dict[str, Any]] = None,
    enable_delta: bool = False  # ← ADD THIS PARAMETER
) -> SparkSession:
    
    builder = SparkSession.builder.appName(app_name or "Book recs")
    
    # Set master (only if not using spark-submit)
    if not _in_spark_submit():
        builder = builder.master("local[*]")
    
    # Memory configuration
    builder = builder \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)

    # Network & timeouts
    builder = builder \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "600s") \
        .config("spark.rpc.askTimeout", "600s")
    
    # SQL optimizations
    builder = builder \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200")
    
    # Streaming (for future use)
    builder = builder.config("spark.streaming.stopGracefullyOnShutdown", "true")
    
    # PostgreSQL JAR configuration
    if POSTGRES_JDBC_JAR and os.path.exists(POSTGRES_JDBC_JAR):
        builder = builder \
            .config("spark.jars", POSTGRES_JDBC_JAR) \
            .config("spark.driver.extraClassPath", POSTGRES_JDBC_JAR) \
            .config("spark.executor.extraClassPath", POSTGRES_JDBC_JAR)
        print(f" PostgreSQL JDBC JAR loaded: {POSTGRES_JDBC_JAR}")
    else:
        print(f"Warning: PostgreSQL JDBC JAR not found at: {POSTGRES_JDBC_JAR}")
    
    # Apply extra configurations
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, str(value))
    
    # Enable Delta Lake if requested
    if enable_delta:
        try:
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            print("Delta Lake enabled")
        except ImportError:
            print("Delta Lake not installed. Install with: pip install delta-spark")
            print("Continuing without Delta Lake support...")
            spark = builder.getOrCreate()
    else:
        # Create session normally
        spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def create_spark() -> SparkSession:
    """Quick spark session with env defaults"""
    return get_spark_session()