"""
Tests unitaires pipeline Spark Streaming avec données mock
"""

import os
import json
import pytest
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, count, sum as spark_sum,
    countDistinct, current_timestamp, unix_timestamp, when, lit, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schémas
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


@pytest.fixture(scope="session")
def spark():
    """Crée une session Spark pour les tests"""
    session = (SparkSession.builder
        .appName("FraudDetectionTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate())
    
    yield session
    session.stop()


class MockDataGenerator:
    """Génère des données mock réalistes"""
    
    COUNTRIES = ["FR", "US", "UK", "DE", "ES", "IT", "RU", "CN", "NG", "BR"]
    MERCHANTS = ["MERCHANT_" + str(i).zfill(5) for i in range(1, 51)]
    MCC_CODES = ["5411", "5412", "5813", "5814", "5542", "5733", "5411"]
    CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
    
    @classmethod
    def generate_transaction(
        cls,
        customer_id: str,
        amount: float = 100.0,
        timestamp: datetime = None,
        is_fraud: bool = False
    ) -> Dict:
        """Génère une transaction mock"""
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Transactions frauduleuses : montants plus élevés, pays différents
        if is_fraud:
            amount = amount * 5.0
            merchant_country = "NG"  # Nigeria pour fraude
            card_present = 0
        else:
            merchant_country = cls.COUNTRIES[hash(customer_id) % len(cls.COUNTRIES)]
            card_present = 1
        
        return {
            "transaction_id": f"TXN_{customer_id}_{int(timestamp.timestamp())}",
            "customer_id": customer_id,
            "amount": amount,
            "currency": cls.CURRENCIES[hash(customer_id) % len(cls.CURRENCIES)],
            "merchant_id": cls.MERCHANTS[hash(customer_id) % len(cls.MERCHANTS)],
            "merchant_country": merchant_country,
            "timestamp": timestamp.isoformat() + "Z",
            "card_present": card_present,
            "online": 1 - card_present,
            "mcc_code": cls.MCC_CODES[hash(customer_id) % len(cls.MCC_CODES)]
        }
    
    @classmethod
    def generate_customer(cls, customer_id: str) -> Dict:
        """Génère un client mock"""
        return {
            "customer_id": customer_id,
            "age": 25 + (hash(customer_id) % 50),
            "country": cls.COUNTRIES[hash(customer_id) % len(cls.COUNTRIES)],
            "account_age_days": 100 + (hash(customer_id) % 3000),
            "credit_limit": 5000.0 + (hash(customer_id) % 20000)
        }
    
    @classmethod
    def generate_transactions_batch(
        cls,
        num_customers: int = 10,
        transactions_per_customer: int = 5,
        fraud_rate: float = 0.1
    ) -> List[Dict]:
        """Génère un batch de transactions"""
        transactions = []
        base_time = datetime.utcnow()
        
        for c_id in range(num_customers):
            customer_id = f"CUST_{str(c_id).zfill(5)}"
            
            for t_id in range(transactions_per_customer):
                is_fraud = (hash(f"{customer_id}_{t_id}") % 100) < (fraud_rate * 100)
                timestamp = base_time + timedelta(minutes=t_id)
                
                txn = cls.generate_transaction(
                    customer_id=customer_id,
                    amount=100.0 + (t_id * 50),
                    timestamp=timestamp,
                    is_fraud=is_fraud
                )
                transactions.append(txn)
        
        return transactions


class TestStreamingPipeline:
    """Tests unitaires du pipeline"""
    
    def test_transaction_schema_validation(self, spark):
        """Test validation du schéma transactions"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=5,
            transactions_per_customer=2
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        assert df.count() == 10, "Devrait avoir 10 transactions"
        assert df.columns == [
            "transaction_id", "customer_id", "amount", "currency",
            "merchant_id", "merchant_country", "timestamp", "card_present", "online", "mcc_code"
        ]
        
        logger.info("✓ Schema validation passed")
    
    def test_transaction_enrichment(self, spark):
        """Test enrichissement transactions"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=5,
            transactions_per_customer=2
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        # Enrichissement : ajout timestamp et conversion devise
        enriched = df.withColumn(
            "processing_timestamp",
            current_timestamp()
        ).withColumn(
            "usd_amount",
            when(col("currency") == "USD", col("amount"))
            .when(col("currency") == "EUR", col("amount") * 1.10)
            .when(col("currency") == "GBP", col("amount") * 1.27)
            .otherwise(col("amount"))
        )
        
        assert enriched.count() == 10
        assert "processing_timestamp" in enriched.columns
        assert "usd_amount" in enriched.columns
        
        # Vérifier les conversions
        sample = enriched.first()
        assert sample["usd_amount"] is not None
        
        logger.info("✓ Transaction enrichment passed")
    
    def test_10min_window_features(self, spark):
        """Test création features fenêtre 10 min"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=3,
            transactions_per_customer=5
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        # Convertir timestamp string en timestamp
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("timestamp", to_timestamp("timestamp"))
        
        # Créer features 10 min
        features = (df
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count"),
                spark_sum("amount").alias("total_amount"),
                countDistinct("merchant_country").alias("distinct_countries")
            )
        )
        
        assert features.count() > 0, "Devrait avoir au moins une fenêtre"
        
        # Vérifier colonnes
        assert "txn_count" in features.columns
        assert "total_amount" in features.columns
        
        logger.info(f"✓ 10min window features passed ({features.count()} windows)")
    
    def test_1hour_window_features(self, spark):
        """Test création features fenêtre 1 heure"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=3,
            transactions_per_customer=10
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("timestamp", to_timestamp("timestamp"))
        
        # Créer features 1h
        features = (df
            .groupBy(
                window(col("timestamp"), "1 hour"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count"),
                spark_sum("amount").alias("total_amount")
            )
        )
        
        assert features.count() > 0
        logger.info(f"✓ 1hour window features passed ({features.count()} windows)")
    
    def test_24h_window_features(self, spark):
        """Test création features fenêtre 24h"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=3,
            transactions_per_customer=10
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("timestamp", to_timestamp("timestamp"))
        
        # Créer features 24h
        features = (df
            .groupBy(
                window(col("timestamp"), "24 hours"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count"),
                countDistinct("merchant_country").alias("distinct_countries")
            )
        )
        
        assert features.count() > 0
        logger.info(f"✓ 24h window features passed ({features.count()} windows)")
    
    def test_fraud_detection_features(self, spark):
        """Test calcul score risque"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=5,
            transactions_per_customer=5,
            fraud_rate=0.2
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("timestamp", to_timestamp("timestamp"))
        
        # Calcul score risque simple
        features = (df
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("customer_id")
            )
            .agg(
                count("*").alias("txn_count"),
                spark_sum("amount").alias("total_amount")
            )
            .withColumn(
                "risk_score",
                when(col("txn_count") > 3, 0.8)
                .when(col("total_amount") > 500, 0.7)
                .otherwise(0.1)
            )
        )
        
        # Vérifier qu'on a des scores risque différents
        scores = features.select("risk_score").distinct().collect()
        assert len(scores) > 1, "Devrait avoir plusieurs risk scores"
        
        logger.info(f"✓ Fraud detection features passed ({len(scores)} risk levels)")
    
    def test_kafka_message_format(self, spark):
        """Test format messages pour Kafka"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=2,
            transactions_per_customer=2
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        # Format Kafka
        kafka_df = df.select(
            col("customer_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        )
        
        assert kafka_df.count() == 4
        
        # Vérifier format JSON
        sample = kafka_df.first()
        assert sample["key"] is not None
        assert sample["value"] is not None
        assert isinstance(sample["value"], str)
        
        # Parser le JSON
        json_obj = json.loads(sample["value"])
        assert "customer_id" in json_obj
        assert "amount" in json_obj
        
        logger.info("✓ Kafka message format passed")
    
    def test_multiple_currencies_conversion(self, spark):
        """Test conversion multiples devises"""
        
        # Créer transactions avec différentes devises
        data = [
            {
                "transaction_id": "TXN_1", "customer_id": "CUST_1",
                "amount": 100.0, "currency": "USD",
                "merchant_id": "M1", "merchant_country": "US",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "card_present": 1, "online": 0, "mcc_code": "5411"
            },
            {
                "transaction_id": "TXN_2", "customer_id": "CUST_1",
                "amount": 100.0, "currency": "EUR",
                "merchant_id": "M2", "merchant_country": "FR",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "card_present": 1, "online": 0, "mcc_code": "5411"
            },
            {
                "transaction_id": "TXN_3", "customer_id": "CUST_1",
                "amount": 100.0, "currency": "GBP",
                "merchant_id": "M3", "merchant_country": "UK",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "card_present": 1, "online": 0, "mcc_code": "5411"
            }
        ]
        
        df = spark.createDataFrame(data, TRANSACTION_SCHEMA)
        
        # Conversion
        converted = df.withColumn(
            "usd_amount",
            when(col("currency") == "USD", col("amount"))
            .when(col("currency") == "EUR", col("amount") * 1.10)
            .when(col("currency") == "GBP", col("amount") * 1.27)
            .otherwise(col("amount"))
        )
        
        results = converted.select("currency", "amount", "usd_amount").collect()
        
        # Vérifier conversions
        for row in results:
            if row["currency"] == "USD":
                assert row["usd_amount"] == 100.0
            elif row["currency"] == "EUR":
                assert abs(row["usd_amount"] - 110.0) < 0.01
            elif row["currency"] == "GBP":
                assert abs(row["usd_amount"] - 127.0) < 0.01
        
        logger.info("✓ Currency conversion passed")
    
    def test_data_quality_checks(self, spark):
        """Test contrôles qualité données"""
        transactions = MockDataGenerator.generate_transactions_batch(
            num_customers=5,
            transactions_per_customer=5
        )
        
        df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
        
        # Vérifications
        # 1. Pas de null customer_id
        nulls = df.filter(col("customer_id").isNull()).count()
        assert nulls == 0, "Ne devrait pas avoir de customer_id null"
        
        # 2. Montants positifs
        negative = df.filter(col("amount") <= 0).count()
        assert negative == 0, "Tous les montants doivent être positifs"
        
        # 3. Timestamps valides
        invalid_ts = df.filter(col("timestamp").isNull()).count()
        assert invalid_ts == 0, "Ne devrait pas avoir de timestamp null"
        
        logger.info("✓ Data quality checks passed")


class TestMockDataGenerator:
    """Tests pour le générateur de données mock"""
    
    def test_transaction_generation(self):
        """Test génération transaction"""
        txn = MockDataGenerator.generate_transaction("CUST_001")
        
        assert txn["customer_id"] == "CUST_001"
        assert txn["amount"] > 0
        assert txn["currency"] in MockDataGenerator.CURRENCIES
        assert txn["merchant_country"] in MockDataGenerator.COUNTRIES
        
        logger.info("✓ Transaction generation passed")
    
    def test_fraud_transaction_generation(self):
        """Test génération transaction frauduleuse"""
        normal = MockDataGenerator.generate_transaction("CUST_002", amount=100, is_fraud=False)
        fraud = MockDataGenerator.generate_transaction("CUST_002", amount=100, is_fraud=True)
        
        # Transactions frauduleuses ont montants plus élevés
        assert fraud["amount"] > normal["amount"]
        assert fraud["merchant_country"] == "NG"
        assert fraud["card_present"] == 0
        
        logger.info("✓ Fraud transaction generation passed")
    
    def test_batch_generation(self):
        """Test génération batch"""
        batch = MockDataGenerator.generate_transactions_batch(
            num_customers=10,
            transactions_per_customer=5,
            fraud_rate=0.1
        )
        
        assert len(batch) == 50
        
        # Vérifier diversité
        customers = set(txn["customer_id"] for txn in batch)
        assert len(customers) == 10
        
        logger.info("✓ Batch generation passed")


if __name__ == "__main__":
    # Lancer les tests avec pytest
    pytest.main([__file__, "-v", "-s"])
