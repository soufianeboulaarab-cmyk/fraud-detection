# Grafana Dashboard Specs
**Auteur :** Data Scientist P4 | **Destinataire :** DevOps P3

---

## Dashboard 1 : `fraud_overview.json` — Vue Opérationnelle

**Refresh :** 1 minute | **Période par défaut :** Last 24h

### Panels

| # | Titre | Type | Requête SQL | Position |
|---|-------|------|-------------|----------|
| 1 | Taux de fraude en temps réel | Stat (grand) | `SELECT AVG(is_fraud)*100 FROM transactions WHERE trans_time > NOW()-'1h'` | Top-left |
| 2 | Montant sauvé aujourd'hui | Stat | Requête 2 (fraud_analytics.sql) filtrée sur today | Top-center |
| 3 | Fraudes détectées (24h) | Stat | `SELECT COUNT(*) FROM transactions WHERE is_fraud=1 AND trans_time > NOW()-'24h'` | Top-right |
| 4 | Taux de fraude par heure | Time series | `SELECT DATE_TRUNC('hour', trans_time), AVG(is_fraud)*100 FROM transactions GROUP BY 1 ORDER BY 1` | Row 2 full-width |
| 5 | Top 10 commerçants fraudés | Bar chart horizontal | Requête 3 (fraud_analytics.sql) | Row 3 left |
| 6 | Fraudes par catégorie | Pie chart | `SELECT category, SUM(is_fraud) FROM transactions WHERE trans_time > NOW()-'24h' GROUP BY category` | Row 3 right |
| 7 | Carte géographique fraudes | Geomap | Requête 1 (fraud_analytics.sql) | Row 4 full-width |

### Alertes à configurer
```
Alerte 1 : fraud_rate_15min > 5%
  → Sévérité : Critical
  → Notification : Slack #fraud-alerts + email équipe

Alerte 2 : nb_fraudes_1h > 200
  → Sévérité : Warning
  → Notification : Slack #fraud-alerts

Alerte 3 : montant_fraude_1h > $15,000
  → Sévérité : Critical
  → Notification : Slack #fraud-alerts + PagerDuty
```

---

## Dashboard 2 : `model_performance.json` — Monitoring Modèle

**Refresh :** 5 minutes | **Période par défaut :** Last 6h

### Panels

| # | Titre | Type | Requête SQL | Position |
|---|-------|------|-------------|----------|
| 1 | Précision (Precision) | Gauge (0-100%) | Requête 4 - colonne precision_pct | Top-left |
| 2 | Rappel (Recall) | Gauge (0-100%) | Requête 4 - colonne recall_pct | Top-center |
| 3 | F1-Score | Gauge (0-100%) | Requête 4 - colonne f1_score_pct | Top-right |
| 4 | Précision & Rappel dans le temps | Time series (2 lignes) | Requête 4 complète | Row 2 full-width |
| 5 | Distribution des scores | Bar chart | Requête 5 - colonnes seuil + nb_predictions | Row 3 left |
| 6 | Precision/Recall curve | XY Chart | Requête 5 - precision_cumul vs recall_cumul | Row 3 right |
| 7 | Latence inférence P50/P95/P99 | Time series | `SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) FROM model_predictions GROUP BY DATE_TRUNC('5min', predicted_at)` | Row 4 full-width |

### Seuils visuels à configurer
```
Precision gauge :
  - Vert  : > 85%
  - Orange: 75-85%
  - Rouge : < 75%

Recall gauge :
  - Vert  : > 80%
  - Orange: 70-80%
  - Rouge : < 70%

Latence P99 :
  - Vert  : < 50ms
  - Orange: 50-100ms
  - Rouge : > 100ms
```

### Alertes à configurer
```
Alerte 4 : precision_pct < 75% pendant 15min
  → Sévérité : Critical → déclencher re-training pipeline

Alerte 5 : recall_pct < 70% pendant 15min
  → Sévérité : Critical → déclencher re-training pipeline

Alerte 6 : latence_p99 > 100ms
  → Sévérité : Warning → vérifier charge Kafka/Redis
```

---

## Variables de template Grafana

```yaml
variables:
  - name: country
    type: query
    query: "SELECT DISTINCT country FROM transactions ORDER BY country"
    multi: true
    include_all: true

  - name: category
    type: query
    query: "SELECT DISTINCT category FROM transactions ORDER BY category"
    multi: true
    include_all: true

  - name: time_window
    type: interval
    options: [5m, 15m, 1h, 6h, 24h]
    default: 1h
```

---

## Datasource recommandée
- **Type :** PostgreSQL ou ClickHouse (meilleures perfs pour time series)
- **Connection :** `fraud-db:5432` (voir docker-compose.yml)
- **Read-only user :** `grafana_reader` avec SELECT uniquement sur `transactions` et `model_predictions`
