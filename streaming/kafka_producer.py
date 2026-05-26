"""
Générateur de données transactions pour Kafka
Envoie des transactions mock vers le topic 'transactions-raw'
"""

import json
import logging
import random
import time
import argparse
from datetime import datetime, timedelta
from typing import Generator, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionGenerator:
    """Génère des transactions réalistes"""
    
    COUNTRIES = ["FR", "US", "UK", "DE", "ES", "IT", "RU", "CN", "NG", "BR", "JP", "CA"]
    MERCHANTS = [f"MERCHANT_{str(i).zfill(5)}" for i in range(1, 101)]
    MCC_CODES = ["5411", "5412", "5813", "5814", "5542", "5733", "3000", "6211", "7994"]
    CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
    
    def __init__(self, num_customers: int = 100):
        """Initialise le générateur"""
        self.num_customers = num_customers
        self.transaction_counter = 0
    
    def generate(
        self,
        fraud_rate: float = 0.05,
        high_value_rate: float = 0.10
    ) -> Generator[Dict, None, None]:
        """
        Génère un flux infini de transactions
        
        Args:
            fraud_rate: % de transactions frauduleuses (0.05 = 5%)
            high_value_rate: % de transactions à haut montant
        
        Yields:
            Dict : transaction
        """
        
        while True:
            customer_id = f"CUST_{random.randint(1, self.num_customers):06d}"
            self.transaction_counter += 1
            
            # Déterminer type transaction
            is_fraud = random.random() < fraud_rate
            is_high_value = random.random() < high_value_rate
            is_online = random.random() < 0.7
            
            # Générer montant
            if is_fraud:
                # Fraudes : montants plus élevés, pays suspects
                amount = random.uniform(2000, 50000)
                merchant_country = random.choice(["NG", "RU", "CN"])
            elif is_high_value:
                # Montants élevés
                amount = random.uniform(1000, 5000)
                merchant_country = random.choice(self.COUNTRIES)
            else:
                # Montants normaux
                amount = random.uniform(10, 500)
                merchant_country = random.choice(self.COUNTRIES)
            
            transaction = {
                "transaction_id": f"TXN_{customer_id}_{self.transaction_counter:010d}",
                "customer_id": customer_id,
                "amount": round(amount, 2),
                "currency": random.choice(self.CURRENCIES),
                "merchant_id": random.choice(self.MERCHANTS),
                "merchant_country": merchant_country,
                "timestamp": (datetime.utcnow() + timedelta(seconds=random.randint(-30, 0))).isoformat() + "Z",
                "card_present": 0 if is_online else 1,
                "online": 1 if is_online else 0,
                "mcc_code": random.choice(self.MCC_CODES),
                "is_fraud_synthetic": is_fraud  # Pour validation tests
            }
            
            yield transaction


class KafkaTransactionProducer:
    """Envoie transactions vers Kafka"""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions-raw"
    ):
        """Initialise le producteur"""
        self.topic = topic
        self.messages_sent = 0
        self.errors = 0
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=30000
            )
            logger.info(f"✓ Connecté à Kafka: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"✗ Erreur connexion Kafka: {e}")
            raise
    
    def send_transaction(self, transaction: Dict) -> bool:
        """
        Envoie une transaction vers Kafka
        
        Args:
            transaction: Dict avec les données
        
        Returns:
            bool: True si envoyé, False sinon
        """
        try:
            # Utiliser customer_id comme key (partition)
            key = transaction["customer_id"].encode("utf-8")
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            
            # Attendre confirmation
            record_metadata = future.get(timeout=10)
            self.messages_sent += 1
            
            return True
            
        except KafkaError as e:
            logger.error(f"✗ Erreur envoi Kafka: {e}")
            self.errors += 1
            return False
    
    def send_batch(
        self,
        transactions: list,
        wait_for_all: bool = True
    ) -> int:
        """
        Envoie un batch de transactions
        
        Args:
            transactions: Liste de transactions
            wait_for_all: Attendre toutes les confirmations
        
        Returns:
            int: Nombre envoyé avec succès
        """
        sent_count = 0
        
        for transaction in transactions:
            try:
                key = transaction["customer_id"].encode("utf-8")
                self.producer.send(
                    self.topic,
                    key=key,
                    value=transaction
                )
                sent_count += 1
            except Exception as e:
                logger.error(f"✗ Erreur batch: {e}")
                self.errors += 1
        
        if wait_for_all:
            self.producer.flush(timeout=30)
        
        self.messages_sent += sent_count
        return sent_count
    
    def get_stats(self) -> Dict:
        """Retourne statistiques envois"""
        return {
            "messages_sent": self.messages_sent,
            "errors": self.errors,
            "success_rate": (
                (self.messages_sent / (self.messages_sent + self.errors) * 100)
                if (self.messages_sent + self.errors) > 0
                else 0
            )
        }
    
    def close(self):
        """Ferme le producteur"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producteur Kafka fermé")


def run_stream(
    bootstrap_servers: str = "localhost:9092",
    num_customers: int = 100,
    fraud_rate: float = 0.05,
    msg_per_sec: int = 100,
    duration_sec: Optional[int] = None
):
    """
    Lance un flux continu de transactions
    
    Args:
        bootstrap_servers: Adresse Kafka
        num_customers: Nombre de clients uniques
        fraud_rate: Taux de fraude synthétique
        msg_per_sec: Messages par seconde à générer
        duration_sec: Durée en secondes (None = infini)
    """
    
    generator = TransactionGenerator(num_customers)
    producer = KafkaTransactionProducer(bootstrap_servers)
    
    logger.info(f"Démarrage streaming: {msg_per_sec} msg/sec, {num_customers} clients")
    logger.info(f"Taux fraude synthétique: {fraud_rate*100}%")
    
    try:
        start_time = time.time()
        transaction_gen = generator.generate(fraud_rate=fraud_rate)
        batch_size = max(1, msg_per_sec // 10)  # Batcher pour efficacité
        batch_delay = batch_size / msg_per_sec
        
        while True:
            # Check timeout
            if duration_sec and (time.time() - start_time) > duration_sec:
                logger.info(f"Arrêt après {duration_sec}s")
                break
            
            # Générer et envoyer batch
            batch = [next(transaction_gen) for _ in range(batch_size)]
            producer.send_batch(batch, wait_for_all=False)
            
            # Stats périodiques
            if producer.messages_sent % (msg_per_sec * 10) == 0:
                stats = producer.get_stats()
                elapsed = time.time() - start_time
                actual_rate = stats["messages_sent"] / elapsed
                logger.info(
                    f"Envoyés: {stats['messages_sent']} "
                    f"| Erreurs: {stats['errors']} "
                    f"| Rate: {actual_rate:.0f} msg/sec"
                )
            
            # Throttle
            time.sleep(batch_delay)
    
    except KeyboardInterrupt:
        logger.info("\n⏹ Arrêt utilisateur")
    except Exception as e:
        logger.error(f"✗ Erreur: {e}", exc_info=True)
    finally:
        stats = producer.get_stats()
        logger.info("=" * 60)
        logger.info(f"FINAL STATS")
        logger.info(f"  Messages envoyés: {stats['messages_sent']}")
        logger.info(f"  Erreurs: {stats['errors']}")
        logger.info(f"  Taux succès: {stats['success_rate']:.1f}%")
        logger.info("=" * 60)
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générateur données Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Adresse Kafka (défaut: localhost:9092)"
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=100,
        help="Nombre de clients uniques (défaut: 100)"
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.05,
        help="Taux fraude (défaut: 0.05 = 5%)"
    )
    parser.add_argument(
        "--msg-per-sec",
        type=int,
        default=100,
        help="Messages par seconde (défaut: 100)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Durée en secondes (défaut: infini)"
    )
    
    args = parser.parse_args()
    
    run_stream(
        bootstrap_servers=args.bootstrap_servers,
        num_customers=args.num_customers,
        fraud_rate=args.fraud_rate,
        msg_per_sec=args.msg_per_sec,
        duration_sec=args.duration
    )
