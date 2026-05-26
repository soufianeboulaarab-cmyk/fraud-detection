#!/bin/bash

# Script de démarrage complet de la pipeline
# Initialise Kafka, PostgreSQL et lance la pipeline Spark

set -e

# Configuration
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  FRAUD DETECTION STREAMING PIPELINE    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Aide
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat <<EOF
Usage: ./startup.sh [COMMAND] [OPTIONS]

Commands:
  init-all       Initialiser Kafka + PostgreSQL (défaut)
  init-kafka     Seulement initialiser Kafka
  init-postgres  Seulement initialiser PostgreSQL
  stream         Lancer la pipeline Spark Streaming
  produce        Générer données Kafka
  test           Lancer les tests unitaires
  help           Afficher cette aide

Options:
  --kafka-brokers BROKERS    (défaut: localhost:9092)
  --postgres-host HOST       (défaut: localhost)
  --postgres-password PWD    (défaut: postgres)
  --num-messages N           Nombre de messages à générer
  --fraud-rate RATE          Taux fraude (défaut: 0.05)
  --msg-per-sec N            Messages par seconde (défaut: 100)

Examples:
  ./startup.sh init-all
  ./startup.sh stream
  ./startup.sh produce --msg-per-sec 500 --fraud-rate 0.1
  ./startup.sh test

EOF
    exit 0
fi

COMMAND="${1:-init-all}"
shift 2>/dev/null || true

# Parser les options
MSG_PER_SEC=100
FRAUD_RATE=0.05
DURATION=3600  # 1 heure par défaut

while [[ $# -gt 0 ]]; do
    case $1 in
        --kafka-brokers)
            KAFKA_BROKERS="$2"
            shift 2
            ;;
        --postgres-host)
            POSTGRES_HOST="$2"
            shift 2
            ;;
        --postgres-password)
            POSTGRES_PASSWORD="$2"
            shift 2
            ;;
        --msg-per-sec)
            MSG_PER_SEC="$2"
            shift 2
            ;;
        --fraud-rate)
            FRAUD_RATE="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        *)
            echo "Option inconnue: $1"
            exit 1
            ;;
    esac
done

# Fonctions
init_kafka() {
    echo -e "${YELLOW}Initialisation Kafka topics...${NC}"
    KAFKA_BROKERS="$KAFKA_BROKERS" bash "$(dirname "$0")/init_kafka.sh"
}

init_postgres() {
    echo -e "${YELLOW}Initialisation PostgreSQL...${NC}"
    export POSTGRES_HOST POSTGRES_PORT POSTGRES_PASSWORD
    bash "$(dirname "$0")/init_postgres.sh"
}

run_stream() {
    echo -e "${YELLOW}Démarrage pipeline Spark Streaming...${NC}"
    export KAFKA_BROKERS POSTGRES_HOST
    python "$(dirname "$0")/spark_streaming.py"
}

run_produce() {
    echo -e "${YELLOW}Génération données Kafka...${NC}"
    python "$(dirname "$0")/kafka_producer.py" \
        --bootstrap-servers "$KAFKA_BROKERS" \
        --msg-per-sec "$MSG_PER_SEC" \
        --fraud-rate "$FRAUD_RATE" \
        --duration "$DURATION"
}

run_tests() {
    echo -e "${YELLOW}Lancement tests unitaires...${NC}"
    pytest "$(dirname "$0")/test_streaming.py" -v -s
}

# Exécuter la commande
case $COMMAND in
    init-all)
        init_kafka
        init_postgres
        ;;
    init-kafka)
        init_kafka
        ;;
    init-postgres)
        init_postgres
        ;;
    stream)
        run_stream
        ;;
    produce)
        run_produce
        ;;
    test)
        run_tests
        ;;
    *)
        echo -e "${RED}Commande inconnue: $COMMAND${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}✓ Opération terminée${NC}"
