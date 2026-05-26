#!/bin/bash

# Quick reference pour les commandes principales

cat << 'EOF'

╔════════════════════════════════════════════════════════════════╗
║    Fraud Detection Streaming Pipeline - QUICK REFERENCE       ║
╚════════════════════════════════════════════════════════════════╝


1️⃣  SETUP INITIAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Installer les dépendances
  make install

  # Vérifier la configuration
  python validate_config.py

  # Créer fichier de configuration
  cp .env.example .env  # puis éditer .env


2️⃣  INITIALISATION INFRASTRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Initialiser Kafka + PostgreSQL
  make init-all

  # Ou séparément :
  make init-kafka           # Créer les topics
  make init-postgres        # Créer les tables


3️⃣  LANCER LA PIPELINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Terminal 1 : Pipeline Spark Streaming
  make stream

  # Terminal 2 : Générateur de données
  make produce              # 100 msg/sec, 5% fraude
  make produce-high         # 500 msg/sec, 10% fraude


4️⃣  TESTS & VALIDATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Tests unitaires avec données mock
  make test                 # Tous les tests
  make test-unit            # Tests unitaires
  make test-stream          # Tests spécifiques streaming

  # Démo interactive
  python demo.py


5️⃣  MONITORING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Via Docker (depuis devops/)
  make docker-run

  # Accès aux interfaces
  🌐 Kafka UI               http://localhost:8080
  🌐 Spark Master UI        http://localhost:8081
  🌐 MLflow                 http://localhost:5000
  🌐 Prometheus             http://localhost:9090
  🌐 Grafana                http://localhost:3000
  🌐 PostgreSQL             localhost:5432

  # PostgreSQL queries
  psql postgresql://postgres@localhost/fraud_db

  # Voir transactions enrichies
  SELECT COUNT(*) FROM transactions_enriched;

  # Voir prédictions fraude
  SELECT * FROM fraud_predictions WHERE risk_score > 0.5;

  # Lag Kafka
  SELECT * FROM kafka_lag_monitoring ORDER BY timestamp DESC LIMIT 10;


6️⃣  TROUBLESHOOTING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Nettoyer les checkpoints (attention: perte du state)
  make clean

  # Logs détaillés Spark
  tail -f /tmp/spark_streaming.log

  # Vérifier topics Kafka
  kafka-topics.sh --bootstrap-server localhost:9092 --list

  # Consumer lag
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group fraud-detection-pipeline --describe

  # Docker logs
  docker compose -f ../devops/docker-compose.yml logs -f spark


7️⃣  ARRÊT PROPRE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # Arrêter la pipeline
  Ctrl+C (dans le terminal Spark)

  # Arrêter la génération de données
  Ctrl+C (dans le terminal producer)

  # Arrêter les services
  make docker-stop


📚 DOCUMENTATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  README.md              Documentation complète
  config.py              Configuration centralisée
  spark_streaming.py     Pipeline Spark Streaming
  test_streaming.py      Tests unitaires + mock data
  kafka_producer.py      Générateur Kafka


🏗️  ARCHITECTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Kafka (transactions-raw)
    ↓
  Spark Streaming
    ├─ Enrichissement
    ├─ Features temps réel (10min, 1h, 24h)
    ├─ Risk scoring
    ↓
  Kafka (enriched, predictions) + PostgreSQL
    ↓
  Monitoring (Prometheus, Grafana)


✨ FEATURES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ Spark Structured Streaming (microbatches)
  ✓ Fenêtres temporelles (10min, 1h, 24h)
  ✓ Feature engineering en temps réel
  ✓ Gestion checkpoints (fault-tolerance)
  ✓ Ecriture Kafka + PostgreSQL
  ✓ Monitoring throughput et lag Kafka
  ✓ Tests unitaires complets
  ✓ Données mock réalistes


EOF
