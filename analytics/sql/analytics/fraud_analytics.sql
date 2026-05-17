-- ============================================================================
-- FRAUD ANALYTICS SQL
-- Auteur : Data Scientist P4
-- Destinataire : DevOps P3 (PostgreSQL / ClickHouse)
-- Table principale : transactions
-- ============================================================================

-- ============================================================================
-- REQUÊTE 1 : Taux de fraude par pays
-- Usage : Dashboard Grafana - carte géographique
-- Refresh : toutes les heures
-- ============================================================================
SELECT
    country,
    COUNT(*)                                          AS total_transactions,
    SUM(is_fraud)                                     AS total_fraudes,
    ROUND(100.0 * SUM(is_fraud) / COUNT(*), 3)        AS fraud_rate_pct,
    ROUND(AVG(amt), 2)                                AS avg_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt END), 2) AS total_fraud_amount
FROM transactions
WHERE trans_time >= NOW() - INTERVAL '30 days'
GROUP BY country
HAVING COUNT(*) >= 100   -- exclure les pays avec trop peu de données
ORDER BY fraud_rate_pct DESC;


-- ============================================================================
-- REQUÊTE 2 : Montant total sauvé par jour (fraudes détectées)
-- Usage : Dashboard Grafana - time series
-- Refresh : quotidien
-- ============================================================================
SELECT
    DATE(trans_time)                                          AS jour,
    COUNT(*)                                                  AS nb_transactions,
    SUM(is_fraud)                                             AS nb_fraudes_detectees,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END), 2) AS montant_sauve_usd,
    ROUND(100.0 * SUM(is_fraud) / COUNT(*), 3)                AS fraud_rate_pct,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amt END), 2)        AS avg_fraud_amount
FROM transactions
WHERE trans_time >= NOW() - INTERVAL '90 days'
GROUP BY DATE(trans_time)
ORDER BY jour DESC;


-- ============================================================================
-- REQUÊTE 3 : Top 10 commerçants avec le plus de fraudes
-- Usage : Dashboard Grafana - bar chart + alerte
-- Refresh : toutes les heures
-- ============================================================================
SELECT
    merchant,
    category,
    COUNT(*)                                                   AS total_transactions,
    SUM(is_fraud)                                              AS nb_fraudes,
    ROUND(100.0 * SUM(is_fraud) / COUNT(*), 2)                 AS fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END), 2)  AS montant_fraude_total,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amt END), 2)         AS avg_fraud_amount
FROM transactions
WHERE trans_time >= NOW() - INTERVAL '30 days'
GROUP BY merchant, category
HAVING SUM(is_fraud) >= 5   -- seuil minimum pour éviter le bruit
ORDER BY nb_fraudes DESC
LIMIT 10;


-- ============================================================================
-- REQUÊTE 4 : Performance du modèle (matrice de confusion, précision, rappel)
-- Usage : Dashboard monitoring modèle
-- Prérequis : table model_predictions (cc_num, trans_id, score, predicted_label,
--             actual_label, predicted_at)
-- Refresh : toutes les heures
-- ============================================================================
WITH confusion AS (
    SELECT
        DATE_TRUNC('hour', predicted_at)                    AS heure,
        SUM(CASE WHEN predicted_label = 1 AND actual_label = 1 THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN predicted_label = 1 AND actual_label = 0 THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN predicted_label = 0 AND actual_label = 1 THEN 1 ELSE 0 END) AS fn,
        SUM(CASE WHEN predicted_label = 0 AND actual_label = 0 THEN 1 ELSE 0 END) AS tn
    FROM model_predictions
    WHERE predicted_at >= NOW() - INTERVAL '24 hours'
    GROUP BY DATE_TRUNC('hour', predicted_at)
)
SELECT
    heure,
    tp, fp, fn, tn,
    ROUND(tp::NUMERIC / NULLIF(tp + fp, 0) * 100, 2)  AS precision_pct,
    ROUND(tp::NUMERIC / NULLIF(tp + fn, 0) * 100, 2)  AS recall_pct,
    ROUND(
        2.0 * tp / NULLIF(2 * tp + fp + fn, 0) * 100, 2
    )                                                  AS f1_score_pct,
    ROUND(
        (tp + tn)::NUMERIC / NULLIF(tp + fp + fn + tn, 0) * 100, 2
    )                                                  AS accuracy_pct
FROM confusion
ORDER BY heure DESC;


-- ============================================================================
-- REQUÊTE 5 : Distribution des scores de prédiction par seuil
-- Usage : Calibration du seuil de décision (recommandé : 0.35)
-- Prérequis : table model_predictions avec colonne `score` (float 0-1)
-- ============================================================================
WITH score_buckets AS (
    SELECT
        ROUND(score::NUMERIC, 1)                                    AS score_bucket,
        COUNT(*)                                                     AS nb_predictions,
        SUM(actual_label)                                            AS nb_fraudes_reelles,
        ROUND(100.0 * SUM(actual_label) / COUNT(*), 2)              AS fraud_rate_in_bucket
    FROM model_predictions
    WHERE predicted_at >= NOW() - INTERVAL '7 days'
    GROUP BY ROUND(score::NUMERIC, 1)
),
cumulative AS (
    SELECT
        score_bucket,
        nb_predictions,
        nb_fraudes_reelles,
        fraud_rate_in_bucket,
        SUM(nb_fraudes_reelles) OVER (ORDER BY score_bucket DESC)   AS cumul_fraudes_capturees,
        SUM(nb_predictions)     OVER (ORDER BY score_bucket DESC)   AS cumul_alertes
    FROM score_buckets
)
SELECT
    score_bucket                                                     AS seuil,
    nb_predictions,
    fraud_rate_in_bucket,
    cumul_fraudes_capturees,
    cumul_alertes,
    ROUND(
        100.0 * cumul_fraudes_capturees /
        NULLIF(SUM(nb_fraudes_reelles) OVER (), 0), 2
    )                                                                AS recall_cumul_pct,
    ROUND(
        100.0 * cumul_fraudes_capturees /
        NULLIF(cumul_alertes, 0), 2
    )                                                                AS precision_cumul_pct
FROM cumulative
ORDER BY seuil;

-- ============================================================================
-- NOTE P3 : Index recommandés pour les performances
-- CREATE INDEX idx_trans_time ON transactions(trans_time);
-- CREATE INDEX idx_is_fraud ON transactions(is_fraud);
-- CREATE INDEX idx_merchant ON transactions(merchant);
-- CREATE INDEX idx_cc_num ON transactions(cc_num);
-- ============================================================================
