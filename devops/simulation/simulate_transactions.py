#!/usr/bin/env python3
"""
Script de simulation de transactions bancaires
Envoie des transactions dans Kafka topic: transactions-raw
Simule le flux décrit dans le cahier des charges
"""

import json
import random
import time
import uuid
from datetime import datetime

# Configuration Kafka
KAFKA_BOOTSTRAP = "kafka-1:9092"
TOPIC = "transactions-raw"

# Données de simulation
CLIENTS = ["C001", "C002", "C003", "C004", "C005"]
CURRENCIES = ["EUR", "USD", "MAD", "GBP"]
COUNTRIES = ["FR", "MA", "DE", "ES", "US", "CN", "RU"]
MERCHANT_CATS = ["retail", "travel", "casino", "food", "tech", "luxury"]

def generate_transaction():
    """Génère une transaction aléatoire"""
    is_suspicious = random.random() < 0.15  # 15% de transactions suspectes

    if is_suspicious:
        # Transaction suspecte : montant élevé, pays étranger, casino
        amount = round(random.uniform(2000, 9999), 2)
        country = random.choice(["RU", "CN", "NG"])
        merchant = "casino"
    else:
        # Transaction normale
        amount = round(random.uniform(5, 500), 2)
        country = random.choice(["FR", "MA", "DE", "ES"])
        merchant = random.choice(["retail", "food", "tech"])

    return {
        "tx_id": str(uuid.uuid4()),
        "client_id": random.choice(CLIENTS),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "country": country,
        "merchant_cat": merchant,
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        "timestamp": datetime.utcnow().isoformat(),
        "is_suspicious_hint": is_suspicious
    }

def main():
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Installation de kafka-python...")
        import subprocess
        subprocess.run(["pip", "install", "kafka-python"], check=True)
        from kafka import KafkaProducer

    print(f"Connexion a Kafka: {KAFKA_BOOTSTRAP}")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Envoi de transactions vers topic: {TOPIC}")
    print("Appuie sur Ctrl+C pour arreter\n")

    count = 0
    while True:
        tx = generate_transaction()
        producer.send(TOPIC, value=tx)
        count += 1

        status = "SUSPECT" if tx["is_suspicious_hint"] else "normal"
        print(f"[{count}] {status.upper():8} | client={tx['client_id']} | {tx['amount']:8.2f} {tx['currency']} | {tx['country']} | {tx['merchant_cat']}")

        # Vitesse variable : parfois rapide, parfois lente
        time.sleep(random.uniform(0.2, 1.5))

if __name__ == "__main__":
    main()
