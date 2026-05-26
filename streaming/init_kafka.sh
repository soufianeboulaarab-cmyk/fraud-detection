#!/bin/bash

# Script d'initialisation Kafka
# Crée les topics nécessaires pour la pipeline

set -e

KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
BOOTSTRAP_SERVERS="--bootstrap-server ${KAFKA_BROKERS}"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Initialisation Topics Kafka${NC}"
echo -e "${YELLOW}========================================${NC}"
echo "Brokers: $KAFKA_BROKERS"
echo ""

# Configuration topics
TOPICS=(
    "transactions-raw:6:3"      # topic:partitions:replication-factor
    "transactions-enriched:6:3"
    "fraud-predictions:6:3"
    "models-updates:3:3"
    "monitoring-metrics:3:3"
)

# Créer les topics
for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    
    echo -n "Création topic '${topic}'... "
    
    kafka-topics.sh \
        ${BOOTSTRAP_SERVERS} \
        --create \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${replication}" \
        --config retention.ms=604800000 \
        --config segment.ms=86400000 \
        --if-not-exists
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
    fi
done

echo ""
echo -e "${YELLOW}Listing Topics:${NC}"
kafka-topics.sh ${BOOTSTRAP_SERVERS} --list

echo ""
echo -e "${YELLOW}Détails Topics:${NC}"
for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic _ _ <<< "$topic_config"
    echo ""
    echo -e "${YELLOW}Topic: ${topic}${NC}"
    kafka-topics.sh ${BOOTSTRAP_SERVERS} --describe --topic "${topic}"
done

echo ""
echo -e "${GREEN}✓ Initialisation Kafka terminée${NC}"
