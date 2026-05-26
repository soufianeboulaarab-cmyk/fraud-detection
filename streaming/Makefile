.PHONY: help up down ps logs logs-streaming test build clean

help:
	@echo "╔════════════════════════════════════════╗"
	@echo "║   Fraud Detection Streaming Pipeline   ║"
	@echo "║         🐳 Docker Compose 🐳          ║"
	@echo "╚════════════════════════════════════════╝"
	@echo ""
	@echo "Commandes:"
	@echo "  make up              - Démarrer tous les services"
	@echo "  make down            - Arrêter tous les services"
	@echo "  make ps              - État des services"
	@echo "  make logs            - Logs en direct"
	@echo "  make logs-streaming  - Logs Spark Streaming"
	@echo "  make test            - Lancer test avec données mock"
	@echo "  make build           - Construire les images"
	@echo "  make clean           - Supprimer les volumes"
	@echo ""
	@echo "URLs:"
	@echo "  Kafka UI:      http://localhost:8080"
	@echo "  Spark Master:  http://localhost:8081"
	@echo "  Prometheus:    http://localhost:9090"
	@echo "  Grafana:       http://localhost:3000"
	@echo ""

up: build
	@echo "🚀 Démarrage des services..."
	@cd /home/pc/fraud-detection/devops && docker-compose up -d
	@echo "⏳ Attente 30 secondes pour l'initialisation..."
	@sleep 30
	@echo "✅ Services prêts!"
	@make ps

down:
	@echo "🛑 Arrêt des services..."
	@cd /home/pc/fraud-detection/devops && docker-compose down
	@echo "✅ Services arrêtés"

build:
	@echo "🔨 Construction des images Docker..."
	@cd /home/pc/fraud-detection/devops && docker-compose build --no-cache

ps:
	@cd /home/pc/fraud-detection/devops && docker-compose ps

logs:
	@cd /home/pc/fraud-detection/devops && docker-compose logs -f

logs-streaming:
	@cd /home/pc/fraud-detection/devops && docker-compose logs -f spark-streaming-pipeline

test:
	@echo "🧪 Envoi de 100 transactions mock..."
	@cd /home/pc/fraud-detection/streaming && python test_mock_producer.py

clean:
	@echo "🧹 Nettoyage complet..."
	@cd /home/pc/fraud-detection/devops && docker-compose down -v
	@echo "✅ Nettoyé"
