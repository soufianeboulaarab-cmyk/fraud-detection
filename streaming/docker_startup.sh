#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEVOPS_DIR="$SCRIPT_DIR/../devops"

echo "╔════════════════════════════════════════╗"
echo "║   🚀 Fraud Detection Streaming         ║"
echo "║         Démarrage Docker               ║"
echo "╚════════════════════════════════════════╝"
echo ""

# Vérifier Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker n'est pas installé"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose n'est pas installé"
    exit 1
fi

echo "✅ Docker trouvé"
echo ""

# Aller au répertoire devops
cd "$DEVOPS_DIR"

# Construire les images
echo "🔨 Construction des images Docker..."
docker-compose build --no-cache

# Démarrer les services
echo ""
echo "🐳 Démarrage des services..."
docker-compose up -d

# Attendre que les services se lancent
echo ""
echo "⏳ Attente 30 secondes pour l'initialisation..."
sleep 30

# Vérifier l'état
echo ""
echo "📊 État des services:"
docker-compose ps

echo ""
echo "✅ Services démarrés!"
echo ""
echo "URLs disponibles:"
echo "  • Kafka UI:      http://localhost:8080"
echo "  • Spark Master:  http://localhost:8081"
echo "  • Prometheus:    http://localhost:9090"
echo "  • Grafana:       http://localhost:3000 (admin/GrafanaPass123)"
echo ""
echo "Prochaine étape:"
echo "  cd $(dirname "$SCRIPT_DIR") && make test"
echo ""
