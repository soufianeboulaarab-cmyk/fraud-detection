#!/bin/bash

# Script d'initialisation PostgreSQL
# Crée les tables et schémas pour la pipeline

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-fraud_db}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Initialisation PostgreSQL${NC}"
echo -e "${YELLOW}========================================${NC}"
echo "Host: $POSTGRES_HOST:$POSTGRES_PORT"
echo "Database: $POSTGRES_DB"
echo ""

# SQL pour créer les tables
SQL_INIT=$(cat <<'EOF'
-- Table transactions enrichies
CREATE TABLE IF NOT EXISTS transactions_enriched (
    transaction_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    amount FLOAT NOT NULL,
    currency VARCHAR(3),
    merchant_id VARCHAR(255),
    merchant_country VARCHAR(2),
    usd_amount FLOAT,
    timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP,
    card_present INT,
    online INT,
    mcc_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_txn_customer_id ON transactions_enriched(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions_enriched(timestamp);
CREATE INDEX IF NOT EXISTS idx_txn_merchant_country ON transactions_enriched(merchant_country);

-- Table prédictions fraude
CREATE TABLE IF NOT EXISTS fraud_predictions (
    prediction_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    window_type VARCHAR(10),
    txn_count INT,
    total_amount FLOAT,
    avg_amount FLOAT,
    distinct_countries INT,
    distinct_merchants INT,
    risk_score FLOAT,
    is_fraud BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pred_customer_id ON fraud_predictions(customer_id);
CREATE INDEX IF NOT EXISTS idx_pred_risk_score ON fraud_predictions(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_pred_is_fraud ON fraud_predictions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_pred_created_at ON fraud_predictions(created_at DESC);

-- Table monitoring Kafka lag
CREATE TABLE IF NOT EXISTS kafka_lag_monitoring (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255),
    partition INT,
    consumer_group VARCHAR(255),
    consumer_lag BIGINT,
    offset BIGINT,
    log_end_offset BIGINT,
    lag_percentage FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lag_topic_partition ON kafka_lag_monitoring(topic, partition);
CREATE INDEX IF NOT EXISTS idx_lag_timestamp ON kafka_lag_monitoring(timestamp DESC);

-- Table métriques pipeline
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metrics_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    messages_processed BIGINT,
    throughput_msg_per_sec FLOAT,
    latency_p50_ms FLOAT,
    latency_p95_ms FLOAT,
    latency_p99_ms FLOAT,
    errors_count INT,
    checkpoint_duration_ms INT,
    memory_usage_mb FLOAT,
    checkpoint_name VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON pipeline_metrics(metrics_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_checkpoint ON pipeline_metrics(checkpoint_name);

-- Table alertes
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255),
    transaction_id VARCHAR(255),
    risk_score FLOAT,
    reason VARCHAR(500),
    manual_review BOOLEAN DEFAULT FALSE,
    action_taken VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_customer_id ON fraud_alerts(customer_id);
CREATE INDEX IF NOT EXISTS idx_alerts_risk_score ON fraud_alerts(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_manual_review ON fraud_alerts(manual_review);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON fraud_alerts(created_at DESC);

-- Table modèles ML
CREATE TABLE IF NOT EXISTS model_versions (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    model_path VARCHAR(500),
    metrics JSONB,
    accuracy FLOAT,
    precision FLOAT,
    recall FLOAT,
    f1_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deployed_at TIMESTAMP,
    is_active BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_model_name_version ON model_versions(model_name, version);
CREATE INDEX IF NOT EXISTS idx_model_active ON model_versions(is_active);

-- View pour analytics
CREATE OR REPLACE VIEW daily_fraud_summary AS
SELECT
    DATE(created_at) as date,
    COUNT(*) as total_transactions,
    COUNT(CASE WHEN is_fraud = TRUE THEN 1 END) as fraud_count,
    ROUND(100.0 * COUNT(CASE WHEN is_fraud = TRUE THEN 1 END) / COUNT(*), 2) as fraud_rate,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(AVG(CASE WHEN is_fraud = FALSE THEN total_amount END), 2) as avg_amount_legitimate,
    ROUND(AVG(CASE WHEN is_fraud = TRUE THEN total_amount END), 2) as avg_amount_fraud
FROM fraud_predictions
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- Autoriser le producteur à écrire
GRANT INSERT ON transactions_enriched TO postgres;
GRANT INSERT ON fraud_predictions TO postgres;
GRANT INSERT ON kafka_lag_monitoring TO postgres;
GRANT INSERT ON pipeline_metrics TO postgres;
GRANT INSERT ON fraud_alerts TO postgres;

-- Autoriser les lectures
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;
GRANT SELECT ON daily_fraud_summary TO postgres;

EOF
)

echo -e "${YELLOW}Exécution SQL...${NC}"

# Exécuter SQL
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    <<< "$SQL_INIT" 2>&1 | grep -v "already exists\|NOTICE" || true

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi

echo ""
echo -e "${YELLOW}Verification tables:${NC}"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "\dt+"

echo ""
echo -e "${YELLOW}Verification views:${NC}"
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "\dv"

echo ""
echo -e "${GREEN}✓ Initialisation PostgreSQL terminée${NC}"
