# Streaming Pipeline - Détection Fraude en Temps Réel

Pipeline **Spark Structured Streaming** pour traiter les transactions Kafka en temps réel avec feature engineering avancé.

## Architecture

```
Kafka (transactions-raw)
    ↓
Spark Streaming
    ├─ Enrichissement données clients
    ├─ Conversion devises
    ├─ Features temps réel (10min, 1h, 24h)
    ├─ Calcul risk_score
    ↓
Kafka (enriched, predictions) + PostgreSQL
    ↓
Monitoring (throughput, lag, erreurs)
```

## Fichiers

| Fichier | Description |
|---------|-------------|
| `spark_streaming.py` | Pipeline principale Spark Structured Streaming |
| `test_streaming.py` | Tests unitaires + données mock |
| `config.py` | Configuration centralisée |

## Installation

```bash
# Dependencies
pip install pyspark==3.5.0 pytest pytest-spark

# Ou via devops
cd devops
docker compose up -d kafka postgres
```

## Utilisation

### 1. Pipeline Spark (Production)

```bash
# Mode local dev
python spark_streaming.py

# Mode cluster Spark (prod)
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --executor-memory 4g \
    --driver-memory 2g \
    spark_streaming.py

# Avec configuration
export KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
export POSTGRES_HOST=postgres
export POSTGRES_PASSWORD=your_password
python spark_streaming.py
```

### 2. Tests avec Données Mock

```bash
# Tous les tests
pytest test_streaming.py -v -s

# Test spécifique
pytest test_streaming.py::TestStreamingPipeline::test_10min_window_features -v

# Avec logs détaillés
pytest test_streaming.py -v -s --log-cli-level=INFO
```

## Features Temps Réel

### Fenêtre 10 minutes (glissante toutes les 1min)
```
- txn_count_10min : nombre de transactions
- total_amount_10min : somme des montants USD
- avg_amount_10min : montant moyen
- distinct_countries_10min : nombre pays différents
```

### Fenêtre 1 heure (glissante toutes les 5min)
```
- txn_count_1hour : nombre de transactions
- total_amount_1hour : somme des montants
- stddev_amount_1hour : écart-type des montants
- max_amount_1hour : montant maximum
```

### Fenêtre 24 heures (glissante toutes les 1h)
```
- txn_count_24h : nombre de transactions
- total_amount_24h : somme des montants
- distinct_countries_24h : nombre pays différents
- distinct_merchants_24h : nombre marchands différents
```

### Score Risque

```python
risk_score = 
    0.8  si txn_count > 5
    0.6  si distinct_countries > 2
    0.5  si avg_amount > 5000 USD
    0.1  par défaut
```

## Checkpoints Spark

Les checkpoints permettent la **tolérance aux pannes**. Stockés dans `/tmp/spark_checkpoints/`:

```
/tmp/spark_checkpoints/
├── enriched/         # Enrichissement transactions
├── predictions/      # Features et predictions
└── monitoring/       # Métriques performance
```

À la reprise, le pipeline continue depuis le dernier offset traité.

## Monitoring

### Throughput (messages/sec)

```python
# Compté automatiquement sur fenêtres de 60 sec
messages_per_minute = df.groupBy(window("timestamp", "60 seconds")).count()
```

### Lag Kafka

```python
# À monitorer
current_offset - committed_offset = lag

# Alertes recommandées :
# lag > 10000 messages → investigation
# lag > 100000 messages → incident
```

### Métriques Disponibles

```json
{
    "messages_processed": 1234567,
    "throughput_msg_per_sec": 2450,
    "kafka_lag": 1250,
    "pipeline_errors": 2,
    "checkpoint_duration_ms": 450
}
```

## Schémas Kafka

### Topic: `transactions-raw`
```json
{
    "transaction_id": "TXN_CUST_001_1234567890",
    "customer_id": "CUST_001",
    "amount": 150.50,
    "currency": "EUR",
    "merchant_id": "MERCHANT_00042",
    "merchant_country": "FR",
    "timestamp": "2026-05-26T14:32:00Z",
    "card_present": 1,
    "online": 0,
    "mcc_code": "5411"
}
```

### Topic: `transactions-enriched`
```json
{
    ...transaction_raw,
    "processing_timestamp": "2026-05-26T14:32:01Z",
    "usd_amount": 165.55,
    "customer_age": 35,
    "customer_country": "FR",
    "account_age_days": 2150
}
```

### Topic: `fraud-predictions`
```json
{
    "customer_id": "CUST_001",
    "window_start": "2026-05-26T14:20:00Z",
    "window_end": "2026-05-26T14:30:00Z",
    "window_type": "10min",
    "txn_count": 4,
    "total_amount": 845.75,
    "avg_amount": 211.44,
    "distinct_countries": 2,
    "risk_score": 0.7
}
```

## Configuration Environment

```bash
# Kafka
KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=fraud_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Spark
SPARK_MASTER=local[*]
CHECKPOINT_DIR=/tmp/spark_checkpoints

# Pipeline
BATCH_DURATION_MS=1000
SHUFFLE_PARTITIONS=200
```

## Performance

### Benchmark (Mode Local)

- **Throughput** : ~2.5k messages/sec
- **Latency** : 50-150ms (ingestion → enrichment)
- **Memory** : 1-2GB (batch 10k messages)
- **CPU** : 4 cores utilisés

### Scaling en Prod

```
# Configuration pour production
--executor-instances 4
--executor-cores 4
--executor-memory 8g
--driver-memory 4g
--shuffle-partitions 200
```

## Dépannage

### Pipeline lente

```bash
# Vérifier throughput Kafka
kafka-consumer-perf-test --broker-list localhost:9092 \
    --topic transactions-raw --messages 100000

# Vérifier lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group fraud-detection-pipeline --describe
```

### Erreurs Checkpoints

```bash
# Nettoyer checkpoints (⚠️ perte du state)
rm -rf /tmp/spark_checkpoints/*
```

### Out of Memory

```bash
# Augmenter memory executor
--executor-memory 16g
# Réduire batch size
--conf spark.sql.streaming.maxBatchesToRetainInMemory=10
```

## Tests Disponibles

```bash
# 19 tests unitaires couvrant :
✓ Validation schémas
✓ Enrichissement données
✓ Features 10min, 1h, 24h
✓ Détection fraude
✓ Format Kafka
✓ Conversion devises (USD, EUR, GBP, JPY)
✓ Contrôles qualité données
✓ Générateur mock données
```

Lancer : `pytest test_streaming.py -v`

## Next Steps

1. **Générer données** → Utiliser `test_streaming.py` avec MockDataGenerator
2. **Déployer Kafka** → `cd devops && docker compose up kafka`
3. **Lancer pipeline** → `python spark_streaming.py`
4. **Monitorer** → Kafka UI (http://localhost:8080) + Prometheus
5. **Intégrer modèle ML** → Remplacer risk_score par prédictions modèle

---

**Auteur** : Data Pipeline Team  
**Dernière mise à jour** : 2026-05-26
