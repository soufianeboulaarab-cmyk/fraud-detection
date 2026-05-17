#!/bin/bash

# Script de création des 5 topics Kafka du projet

echo "Attente du démarrage de Kafka..."
sleep 20

KAFKA_BIN="kafka-topics.sh"
BOOTSTRAP="--bootstrap-server kafka-1:9092"

echo "📦 Création des topics..."

# 1. Transactions brutes (données brutes de la banque)
$KAFKA_BIN $BOOTSTRAP --create --if-not-exists \
  --topic transactions-raw \
  --partitions 10 \
  --replication-factor 3

# 2. Transactions enrichies (après ajout infos clients)
$KAFKA_BIN $BOOTSTRAP --create --if-not-exists \
  --topic transactions-enriched \
  --partitions 10 \
  --replication-factor 3

# 3. Labels de fraude (retour des opérateurs humains)
$KAFKA_BIN $BOOTSTRAP --create --if-not-exists \
  --topic fraud-labels \
  --partitions 3 \
  --replication-factor 3

# 4. Prédictions du modèle ML (scores 0.0 → 1.0)
$KAFKA_BIN $BOOTSTRAP --create --if-not-exists \
  --topic predictions \
  --partitions 10 \
  --replication-factor 3

# 5. Alertes pour les opérateurs
$KAFKA_BIN $BOOTSTRAP --create --if-not-exists \
  --topic alerts \
  --partitions 3 \
  --replication-factor 3

echo ""
echo "✅ Topics créés avec succès !"
echo "📋 Liste des topics :"
$KAFKA_BIN $BOOTSTRAP --list