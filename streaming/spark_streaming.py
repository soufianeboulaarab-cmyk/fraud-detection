"""
Spark Structured Streaming Pipeline
Processus transactions Kafka en temps réel avec features engineering
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, count, sum as spark_sum,
    countDistinct, current_timestamp, unix_timestamp, when, lit,
    avg, stddev, max as spark_max, min as spark_min, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration environment
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092")
KAFKA_TOPIC_INPUT = "transactions-raw"
KAFKA_TOPIC_ENRICHED = "transactions-enriched"
KAFKA_TOPIC_PREDICTIONS = "fraud-predictions"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

CHECKPOINT_DIR = "/tmp/spark_checkpoints"

# Schémas JSON
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_country", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("card_present", LongType()),
    StructField("online", LongType()),
    StructField("mcc_code", StringType())
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("age", LongType()),
    StructField("country", StringType()),
    StructField("account_age_days", LongType()),
    StructField("credit_limit", DoubleType())
])


class SparkStreamingPipeline:
    """Pipeline Spark Streaming pour détection fraude temps réel"""
    
    def __init__(self):
        """Initialise la session Spark"""
        self.spark = self._create_spark_session()
        self.metrics = {
            "messages_processed": 0,
            "throughput_msg_per_sec": 0,
            "kafka_lag": 0,
            "pipeline_errors": 0
        }
    
    def _create_spark_session(self) -> SparkSession:
        """Crée une session Spark configurée"""
        return (SparkSession.builder
            .appName("FraudDetectionStreaming")
            .master("local[*]")  # Mode local pour dev, "spark://spark-master:7077" en prod
            .config("spark.streaming.kafka.maxRatePerPartition", "10000")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.7.1")
            .getOrCreate())
    
    def read_kafka_stream(self) -> DataFrame:
        """Lit transactions depuis Kafka"""
        logger.info(f"Lecture depuis Kafka: {KAFKA_BROKERS}/{KAFKA_TOPIC_INPUT}")
        
        df = (self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("subscribe", KAFKA_TOPIC_INPUT)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())
        
        # Parse JSON
        df = df.select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
        ).select("data.*")
        
        return df
    
    def load_customer_data(self) -> DataFrame:
        """Charge données clients (lookup table)"""
        logger.info(f"Chargement données clients depuis {POSTGRES_HOST}")
        
        # En production, charger depuis PostgreSQL
        # Pour dev, retourner vide (pour tests)
        return self.spark.createDataFrame([], CUSTOMER_SCHEMA)
    
    def enrich_transactions(self, df: DataFrame, customers_df: DataFrame) -> DataFrame:
        """Enrichit les transactions avec données clients"""
        logger.info("Enrichissement des transactions")
        
        # Jointure avec données clients
        if customers_df.count() > 0:
            enriched = df.join(
                customers_df,
                df.customer_id == customers_df.customer_id,
                "left"
            )
        else:
            enriched = df.select("*")
        
        # Ajouter timestamp processing et conversion devise
        enriched = enriched.withColumn(
            "processing_timestamp",
            current_timestamp()
        ).withColumn(
            "usd_amount",
            when(col("currency") == "USD", col("amount"))
            .when(col("currency") == "EUR", col("amount") * 1.10)
            .when(col("currency") == "GBP", col("amount") * 1.27)
            .otherwise(col("amount"))
        )
        
        return enriched
    
    def create_real_time_features(self, df: DataFrame) -> DataFrame:
        """Crée features temps réel avec fenêtres temporelles"""
        logger.info("Création des features temps réel")
        
        # Fenêtres temporelles
        features_10min = (df
            .groupBy(
                window(col("timestamp"), "10 minutes", "1 minute"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count_10min"),
                spark_sum("usd_amount").alias("total_amount_10min"),
                avg("usd_amount").alias("avg_amount_10min"),
                countDistinct("merchant_country").alias("distinct_countries_10min")
            )
            .withColumn("window_type", lit("10min"))
        )
        
        features_1hour = (df
            .groupBy(
                window(col("timestamp"), "1 hour", "5 minutes"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count_1hour"),
                spark_sum("usd_amount").alias("total_amount_1hour"),
                stddev("usd_amount").alias("stddev_amount_1hour"),
                spark_max("usd_amount").alias("max_amount_1hour")
            )
            .withColumn("window_type", lit("1hour"))
        )
        
        features_24h = (df
            .groupBy(
                window(col("timestamp"), "24 hours", "1 hour"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count_24h"),
                spark_sum("usd_amount").alias("total_amount_24h"),
                countDistinct("merchant_country").alias("distinct_countries_24h"),
                countDistinct("merchant_id").alias("distinct_merchants_24h")
            )
            .withColumn("window_type", lit("24h"))
        )
        
        # Combiner et ajouter scores risque
        features = features_10min.select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("window_type"),
            col("txn_count_10min"),
            col("total_amount_10min"),
            col("avg_amount_10min"),
            col("distinct_countries_10min")
        )
        
        # Calculer score risque simple
        features = features.withColumn(
            "risk_score",
            when(col("txn_count_10min") > 5, 0.7)
            .when(col("distinct_countries_10min") > 2, 0.6)
            .when(col("avg_amount_10min") > 5000, 0.5)
            .otherwise(0.1)
        )
        
        return features
    
    def write_to_kafka(self, df: DataFrame, topic: str, checkpoint_id: str) -> None:
        """Écrit les données vers Kafka"""
        logger.info(f"Écriture vers Kafka topic: {topic}")
        
        query = (df
            .select(
                col("customer_id").cast("string").alias("key"),
                to_json(struct("*")).alias("value")
            )
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", topic)
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_id}")
            .option("failOnDataLoss", "false")
            .start())
        
        return query
    
    def write_to_postgres(self, df: DataFrame, table: str, checkpoint_id: str) -> None:
        """Écrit les données vers PostgreSQL"""
        logger.info(f"Écriture vers PostgreSQL table: {table}")
        
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        
        query = (df
            .writeStream
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint_id}")
            .start())
        
        return query
    
    def monitor_performance(self, df: DataFrame) -> None:
        """Monitore throughput et lag Kafka"""
        logger.info("Monitoring performance")
        
        # Compter messages par seconde
        count_query = (df
            .withColumn("processing_time", unix_timestamp(current_timestamp()))
            .groupBy(
                window(col("processing_time"), "60 seconds")
            )
            .agg(count("*").alias("messages_per_minute"))
            .writeStream
            .format("console")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/monitoring")
            .start())
        
        return count_query
    
    def run(self):
        """Lance la pipeline complète"""
        logger.info("=" * 60)
        logger.info("Démarrage pipeline Spark Streaming")
        logger.info("=" * 60)
        
        try:
            # 1. Lire Kafka
            df_raw = self.read_kafka_stream()
            
            # 2. Charger données clients
            customers_df = self.load_customer_data()
            
            # 3. Enrichir transactions
            df_enriched = self.enrich_transactions(df_raw, customers_df)
            
            # 4. Créer features temps réel
            df_features = self.create_real_time_features(df_enriched)
            
            # 5. Écrire résultats
            query_enriched = self.write_to_kafka(
                df_enriched, 
                KAFKA_TOPIC_ENRICHED,
                "enriched"
            )
            
            query_predictions = self.write_to_kafka(
                df_features,
                KAFKA_TOPIC_PREDICTIONS,
                "predictions"
            )
            
            # 6. Monitoring
            query_monitoring = self.monitor_performance(df_enriched)
            
            logger.info("Pipeline lancée. Awaiting termination...")
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            logger.error(f"Erreur pipeline: {str(e)}", exc_info=True)
            self.metrics["pipeline_errors"] += 1
            raise
        finally:
            self.spark.stop()
            logger.info("Session Spark fermée")


def main():
    """Fonction principale"""
    pipeline = SparkStreamingPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
