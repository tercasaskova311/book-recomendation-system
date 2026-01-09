# spark_session.py
from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
from config import SparkConfig
import os

def _in_spark_submit() -> bool: #Check if running inside spark-submit
    return "PYSPARK_SUBMIT_ARGS" in os.environ

#========= SPARK SESSION =======================
#app_name: Application name (overrides config)
def get_spark_session(
    app_name: Optional[str] = None,
    config: Optional[SparkConfig] = None,
    extra_conf: Optional[Dict[str, Any]] = None
) -> SparkSession:
    
    # Load config from environment if not provided
    if config is None:
        config = SparkConfig.from_env()
    
    # Override app name if provided
    if app_name:
        config.app_name = app_name
    
    # Start building session
    builder = SparkSession.builder.appName(config.app_name)
    
    # Set master (only if not using spark-submit)
    if not _in_spark_submit():
        builder = builder.master(config.master)
    
    # Memory configuration
    builder = builder \
        .config("spark.driver.memory", config.driver_memory) \
        .config("spark.executor.memory", config.executor_memory) \
        .config("spark.executor.cores", str(config.executor_cores))
    
    # Network & timeouts
    builder = builder \
        .config("spark.executor.heartbeatInterval", config.heartbeat_interval) \
        .config("spark.network.timeout", config.network_timeout) \
        .config("spark.rpc.askTimeout", config.rpc_timeout)
    
    # SQL optimizations
    builder = builder \
        .config("spark.sql.session.timeZone", config.timezone) \
        .config("spark.sql.adaptive.enabled", str(config.adaptive_enabled)) \
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
    
    # Streaming (for future use)
    builder = builder.config("spark.streaming.stopGracefullyOnShutdown", "true")
    
    # PostgreSQL JAR configuration
    if config.postgres_jar:
        builder = builder \
            .config("spark.jars", config.postgres_jar) \
            .config("spark.driver.extraClassPath", config.postgres_jar) \
            .config("spark.executor.extraClassPath", config.postgres_jar)
    
    # Apply extra configurations
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, str(value))
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(config.log_level)
    
    return spark

def create_spark() -> SparkSession: #Quick spark session with env defaults
    return get_spark_session()