-- Schéma analytique — adapté depuis devops/postgres/init.sql
-- Lecture seule pour analytics (P4)

CREATE TABLE IF NOT EXISTS transactions (
    tx_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       VARCHAR(50) NOT NULL,
    amount          DECIMAL(12, 2) NOT NULL,
    currency        VARCHAR(3) NOT NULL DEFAULT 'EUR',
    country         VARCHAR(2),
    merchant_cat    VARCHAR(50),
    ip_address      VARCHAR(45),
    timestamp       TIMESTAMPTZ DEFAULT NOW(),
    score           FLOAT,
    decision        VARCHAR(10),
    is_fraud        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    client_id           VARCHAR(50) PRIMARY KEY,
    credit_limit        DECIMAL(12, 2) DEFAULT 5000.00,
    account_age_days    INT DEFAULT 0,
    fraud_history       INT DEFAULT 0,
    country_declared    VARCHAR(2),
    risk_score          FLOAT DEFAULT 0.0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_labels (
    id          SERIAL PRIMARY KEY,
    tx_id       UUID REFERENCES transactions(tx_id),
    labeled_by  VARCHAR(100),
    labeled_at  TIMESTAMPTZ DEFAULT NOW(),
    is_fraud    BOOLEAN NOT NULL
);

-- Index recommandés pour les requêtes analytiques
CREATE INDEX IF NOT EXISTS idx_tx_timestamp  ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_tx_is_fraud   ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_tx_client     ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_tx_decision   ON transactions(decision);
