import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKERS = 'localhost:9092'
TOPIC = 'transactions-raw'

def create_mock_transaction():
    """Génère une transaction mock aléatoire"""
    countries = ['FR', 'DE', 'IT', 'ES', 'BE', 'NL', 'CH', 'AT', 'PL', 'GB']
    device_types = ['mobile', 'desktop', 'tablet', 'api']
    
    return {
        "transaction_id": f"TRX-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 5000)}",
        "amount": round(random.uniform(5.0, 10000.0), 2),
        "timestamp": int(time.time() * 1000),
        "merchant_id": f"MER-{random.randint(1, 1000)}",
        "country": random.choice(countries),
        "device_type": random.choice(device_types)
    }

def send_transactions(num_transactions=100, batch_size=10, delay_between_batches=5):
    """Envoie des transactions mock vers Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    
    logger.info(f"=== Début de l'envoi de {num_transactions} transactions ===")
    logger.info(f"Broker: {KAFKA_BROKERS}")
    logger.info(f"Topic: {TOPIC}")
    
    try:
        sent_count = 0
        for batch_num in range(0, num_transactions, batch_size):
            batch_size_actual = min(batch_size, num_transactions - batch_num)
            
            logger.info(f"\n--- Batch {batch_num // batch_size + 1} ---")
            
            for i in range(batch_size_actual):
                transaction = create_mock_transaction()
                
                future = producer.send(TOPIC, value=transaction)
                record_metadata = future.get(timeout=10)
                
                sent_count += 1
                logger.info(f"[{sent_count}/{num_transactions}] "
                          f"TXN ID: {transaction['transaction_id']}, "
                          f"Amount: {transaction['amount']:.2f}€, "
                          f"Country: {transaction['country']}, "
                          f"Partition: {record_metadata.partition}")
            
            if batch_num + batch_size_actual < num_transactions:
                logger.info(f"Attente {delay_between_batches}s avant le prochain batch...")
                time.sleep(delay_between_batches)
        
        producer.flush()
        logger.info(f"\n=== {sent_count} transactions envoyées avec succès ===")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi: {e}", exc_info=True)
    finally:
        producer.close()

def test_kafka_connection():
    """Teste la connexion à Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKERS],
            request_timeout_ms=5000
        )
        logger.info(f"✓ Connexion à Kafka établie: {KAFKA_BROKERS}")
        producer.close()
        return True
    except Exception as e:
        logger.error(f"✗ Impossible de se connecter à Kafka: {e}")
        return False

if __name__ == "__main__":
    # Test connexion
    if not test_kafka_connection():
        logger.error("Impossible de continuer sans connexion à Kafka")
        exit(1)
    
    # Envoyer transactions
    send_transactions(
        num_transactions=100,
        batch_size=10,
        delay_between_batches=2
    )
