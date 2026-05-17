-- ─────────────────────────────────────────────────────────
-- Schéma de la base de données fraud_db
-- ─────────────────────────────────────────────────────────

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

-- Table clients (pour enrichissement Spark)
CREATE TABLE IF NOT EXISTS customers (
    client_id           VARCHAR(50) PRIMARY KEY,
    credit_limit        DECIMAL(12, 2) DEFAULT 5000.00,
    account_age_days    INT DEFAULT 0,
    fraud_history       INT DEFAULT 0,
    country_declared    VARCHAR(2),
    risk_score          FLOAT DEFAULT 0.0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Table fraud_labels (boucle de feedback humain)
CREATE TABLE IF NOT EXISTS fraud_labels (
    id          SERIAL PRIMARY KEY,
    tx_id       UUID REFERENCES transactions(tx_id),
    labeled_by  VARCHAR(100),
    labeled_at  TIMESTAMPTZ DEFAULT NOW(),
    is_fraud    BOOLEAN NOT NULL
);

-- Index pour accélérer les requêtes analytiques
CREATE INDEX IF NOT EXISTS idx_tx_client    ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_tx_decision  ON transactions(decision);
CREATE INDEX IF NOT EXISTS idx_tx_score     ON transactions(score);

-- Données de test (quelques clients fictifs)
INSERT INTO customers (client_id, credit_limit, account_age_days, fraud_history, country_declared)
VALUES
    ('C001', 10000.00, 1200, 0, 'FR'),
    ('C002', 5000.00,  365,  1, 'MA'),
    ('C003', 25000.00, 2500, 0, 'DE'),
    ('C004', 3000.00,  90,   2, 'FR'),
    ('C005', 8000.00,  730,  0, 'ES')
ON CONFLICT DO NOTHING;

-- Message de confirmation
DO $$ BEGIN
    RAISE NOTICE 'Base de données fraud_db initialisée avec succès';
END $$;
