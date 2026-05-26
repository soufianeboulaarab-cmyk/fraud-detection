"""
Configuration et utilitaires pour la pipeline Spark Streaming
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """Configuration Kafka"""
    brokers: str = os.getenv("KAFKA_BROKERS", "localhost:9092")
    topic_input: str = "transactions-raw"
    topic_enriched: str = "transactions-enriched"
    topic_predictions: str = "fraud-predictions"
    consumer_group: str = "fraud-detection-pipeline"
    max_rate_per_partition: int = 10000
    
    def __post_init__(self):
        """Valider configuration"""
        if not self.brokers:
            raise ValueError("KAFKA_BROKERS non configuré")


@dataclass
class PostgresConfig:
    """Configuration PostgreSQL"""
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "fraud_db")
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    @property
    def jdbc_url(self) -> str:
        """Retourne URL JDBC"""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass
class SparkConfig:
    """Configuration Spark"""
    app_name: str = "FraudDetectionStreaming"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints")
    
    # Spark Streaming
    batch_duration_ms: int = 1000  # 1 seconde
    
    # Performance
    shuffle_partitions: int = 200
    default_parallelism: int = 8


@dataclass
class PipelineConfig:
    """Configuration complète pipeline"""
    kafka: KafkaConfig = None
    postgres: PostgresConfig = None
    spark: SparkConfig = None
    
    # Features
    window_10min: bool = True
    window_1hour: bool = True
    window_24h: bool = True
    
    # Outputs
    write_to_kafka: bool = True
    write_to_postgres: bool = False
    
    # Monitoring
    monitor_throughput: bool = True
    monitor_lag: bool = True
    metrics_interval_sec: int = 60
    
    def __post_init__(self):
        """Initialiser configurations par défaut"""
        if self.kafka is None:
            self.kafka = KafkaConfig()
        if self.postgres is None:
            self.postgres = PostgresConfig()
        if self.spark is None:
            self.spark = SparkConfig()


def get_config() -> PipelineConfig:
    """Charge configuration depuis environment"""
    return PipelineConfig()


# Tables PostgreSQL
SCHEMA_TRANSACTIONS_ENRICHED = """
CREATE TABLE IF NOT EXISTS transactions_enriched (
    transaction_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    amount FLOAT,
    currency VARCHAR(3),
    merchant_id VARCHAR(255),
    merchant_country VARCHAR(2),
    usd_amount FLOAT,
    timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP,
    card_present INT,
    online INT,
    mcc_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_id ON transactions_enriched(customer_id);
CREATE INDEX idx_timestamp ON transactions_enriched(timestamp);
"""

SCHEMA_FRAUD_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS fraud_predictions (
    prediction_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    window_type VARCHAR(10),
    txn_count INT,
    total_amount FLOAT,
    avg_amount FLOAT,
    distinct_countries INT,
    risk_score FLOAT,
    is_fraud BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_predictions ON fraud_predictions(customer_id);
CREATE INDEX idx_risk_score ON fraud_predictions(risk_score);
"""

SCHEMA_KAFKA_LAG = """
CREATE TABLE IF NOT EXISTS kafka_lag_monitoring (
    id SERIAL PRIMARY KEY,
    partition INT,
    consumer_lag BIGINT,
    offset BIGINT,
    log_end_offset BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lag_timestamp ON kafka_lag_monitoring(timestamp);
"""

SCHEMA_PIPELINE_METRICS = """
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metrics_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    messages_processed BIGINT,
    throughput_msg_per_sec FLOAT,
    latency_ms FLOAT,
    errors_count INT,
    checkpoint_duration_ms INT
);

CREATE INDEX idx_metrics_timestamp ON pipeline_metrics(metrics_timestamp);
"""
