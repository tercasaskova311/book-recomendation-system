# config.py
from dataclasses import dataclass
from typing import Optional
import os
from pathlib import Path
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

@dataclass
class SparkConfig:
    
    app_name: str = "MySparkApp"    
    master: str = "local[*]"    
    driver_memory: str = "2g"
    executor_memory: str = "4g"
    executor_cores: int = 2

    postgres_jar: Optional[str] = None
    
    # Timeouts
    network_timeout: str = "600s"
    heartbeat_interval: str = "60s"
    rpc_timeout: str = "600s"
    
    # SQL
    shuffle_partitions: int = 200
    timezone: str = "UTC"
    adaptive_enabled: bool = True
    
    # Logging
    log_level: str = "WARN"
    
    @classmethod
    def from_env(cls) -> "SparkConfig":
                
        # Get PostgreSQL JAR path and expand it
        postgres_jar = os.getenv("POSTGRES_JDBC_JAR")
        if postgres_jar:
            postgres_jar = str(Path(postgres_jar).expanduser().resolve())
        
        return cls(
            master=os.getenv("SPARK_MASTER", "local[*]"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            postgres_jar=postgres_jar,
            network_timeout=os.getenv("SPARK_NETWORK_TIMEOUT", "600s"),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")),
            log_level=os.getenv("SPARK_LOG_LEVEL", "WARN")
        )