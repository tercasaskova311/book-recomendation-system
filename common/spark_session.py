# common/spark_session.py
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from config import POSTGRES_JDBC_JAR, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY
from delta import configure_spark_with_delta_pip


def get_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    extra_conf: Optional[Dict[str, Any]] = None,
    enable_delta: bool = False 
) -> SparkSession:
    
    builder = SparkSession.builder.appName(app_name or "Book recs")

    # Always use local mode for single-machine setup
    builder = builder.master(master or "local[*]")
    
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
    
    # PostgreSQL JAR configuration - try multiple paths
    jar_paths = [
        POSTGRES_JDBC_JAR,
        "/opt/spark/jars/postgresql-42.6.0.jar",
        "/opt/project/jars/postgresql-42.6.0.jar"
    ]
    
    jar_loaded = False
    for jar_path in jar_paths:
        if jar_path and os.path.exists(jar_path):
            builder = builder \
                .config("spark.jars", jar_path) \
                .config("spark.driver.extraClassPath", jar_path) \
                .config("spark.executor.extraClassPath", jar_path)
            print(f"✓ PostgreSQL JDBC JAR loaded: {jar_path}")
            jar_loaded = True
            break
    
    if not jar_loaded:
        print(f"⚠️  Warning: PostgreSQL JDBC JAR not found in any location")
    
    # Apply extra configurations
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, str(value))
    
    # Enable Delta Lake if requested
    if enable_delta:
        builder = builder \
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            ) \
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        print("✓ Delta Lake enabled")
    else:
        spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Print session info
    print(f"✓ Spark session created: {spark.sparkContext.master}")
    
    return spark

def create_spark() -> SparkSession:
    """Quick spark session with env defaults"""
    return get_spark_session()