import os
import sys
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, count, sum, 
    collect_set, to_json, struct, current_timestamp, 
    date_format, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'fraud_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# Définition du schéma des transactions
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("merchant_id", StringType()),
    StructField("country", StringType()),
    StructField("device_type", StringType())
])

def create_spark_session():
    """Crée une session Spark avec les configurations nécessaires"""
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .master(os.getenv('SPARK_MASTER', 'local[*]')) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def create_kafka_topics_if_not_exist(spark):
    """Crée les topics Kafka s'ils n'existent pas"""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    
    topics = [
        NewTopic(name='transactions-raw', num_partitions=3, replication_factor=3),
        NewTopic(name='transactions-enriched', num_partitions=3, replication_factor=3),
        NewTopic(name='transactions-predictions', num_partitions=3, replication_factor=3)
    ]
    
    try:
        fs = admin_client.create_topics(new_topics=topics, validate_only=False)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic '{topic}' créé avec succès")
            except TopicAlreadyExistsError:
                logger.info(f"Topic '{topic}' existe déjà")
    except Exception as e:
        logger.warning(f"Erreur lors de la création des topics: {e}")
    finally:
        admin_client.close()

def read_kafka_stream(spark):
    """Lit le flux Kafka des transactions brutes"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", "transactions-raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", "spark-streaming-group") \
        .load()
    
    # Parse le JSON
    df = df.select(
        col("value").cast(StringType()),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )
    
    df = df.select(
        from_json(col("value"), transaction_schema).alias("data"),
        col("kafka_timestamp"),
        col("partition"),
        col("offset")
    ).select("data.*", "kafka_timestamp", "partition", "offset")
    
    return df

def read_customers_table(spark):
    """Lit la table des clients depuis PostgreSQL"""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "customers") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    return df

def create_features(df):
    """Crée les features temps réel avec fenêtres glissantes"""
    
    # Fenêtre 10 minutes : nombre transactions par client
    features_10min = df.withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "10 minutes"),
            col("customer_id")
        ) \
        .agg(
            count("transaction_id").alias("tx_count_10min"),
            sum("amount").alias("total_amount_10min")
        ) \
        .select(
            col("window.start").alias("window_start_10min"),
            col("customer_id"),
            col("tx_count_10min"),
            col("total_amount_10min")
        )
    
    # Fenêtre 1 heure : somme des montants par client
    features_1h = df.withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 hour"),
            col("customer_id")
        ) \
        .agg(
            count("transaction_id").alias("tx_count_1h"),
            sum("amount").alias("total_amount_1h")
        ) \
        .select(
            col("window.start").alias("window_start_1h"),
            col("customer_id"),
            col("tx_count_1h"),
            col("total_amount_1h")
        )
    
    # Fenêtre 24 heures : pays différents par client
    features_24h = df.withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "24 hours"),
            col("customer_id")
        ) \
        .agg(
            collect_set("country").alias("countries_24h"),
            count("transaction_id").alias("tx_count_24h")
        ) \
        .select(
            col("window.start").alias("window_start_24h"),
            col("customer_id"),
            col("countries_24h"),
            col("tx_count_24h")
        )
    
    return features_10min, features_1h, features_24h

def enrich_transactions(df, customers_df):
    """Enrichit les transactions avec les données clients"""
    enriched = df.join(
        customers_df,
        df.customer_id == customers_df.customer_id,
        "left"
    ).select(
        df["*"],
        customers_df["risk_level"],
        customers_df["account_age_days"]
    )
    
    return enriched

def write_to_postgres(df, table_name, mode="append"):
    """Écrit les résultats dans PostgreSQL"""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    def write_batch(batch_df, batch_id):
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info(f"Batch {batch_id} écrit dans {table_name}")
        except Exception as e:
            logger.error(f"Erreur écriture PostgreSQL: {e}")
    
    return write_batch

def write_to_kafka(df, topic_name):
    """Écrit les résultats vers Kafka"""
    def write_batch(batch_df, batch_id):
        try:
            batch_df.select(
                col("*"),
                to_json(struct("*")).alias("value")
            ).select("value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", topic_name) \
            .option("checkpointLocation", f"/tmp/checkpoint-{topic_name}-{batch_id}") \
            .save()
            logger.info(f"Batch {batch_id} écrit vers topic {topic_name}")
        except Exception as e:
            logger.error(f"Erreur écriture Kafka: {e}")
    
    return write_batch

def log_metrics(df, stream_name):
    """Log les métriques de la pipeline"""
    def log_batch(batch_df, batch_id):
        count = batch_df.count()
        logger.info(f"[{stream_name}] Batch {batch_id}: {count} lignes")
    return log_batch

def main():
    """Fonction principale"""
    logger.info("=== Démarrage de la pipeline Spark Streaming ===")
    
    # Initialiser Spark
    spark = create_spark_session()
    logger.info(f"Session Spark créée: {spark.version}")
    
    # Créer les topics Kafka
    logger.info("Création des topics Kafka...")
    create_kafka_topics_if_not_exist(spark)
    
    # Lire le flux Kafka
    logger.info("Lecture du flux Kafka...")
    transactions_df = read_kafka_stream(spark)
    
    # Lire la table des clients
    logger.info("Lecture de la table clients...")
    try:
        customers_df = read_customers_table(spark)
        
        # Enrichir les transactions
        logger.info("Enrichissement des transactions...")
        enriched_df = enrich_transactions(transactions_df, customers_df)
        
        # Créer les features
        logger.info("Création des features...")
        features_10min, features_1h, features_24h = create_features(enriched_df)
        
        # Écrire les résultats enrichis vers Kafka
        query_enriched = enriched_df \
            .select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", "transactions-enriched") \
            .option("checkpointLocation", "/tmp/checkpoint-enriched") \
            .option("failOnDataLoss", "false") \
            .start()
        
        logger.info("Query enriched lancée")
        
        # Écrire les features 10 min
        query_features_10min = features_10min \
            .select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", "transactions-predictions") \
            .option("checkpointLocation", "/tmp/checkpoint-features-10min") \
            .option("failOnDataLoss", "false") \
            .foreachBatch(log_metrics(features_10min, "Features-10min")) \
            .start()
        
        logger.info("Query features 10min lancée")
        
        # Attendre les queries
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
