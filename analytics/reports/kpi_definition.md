# KPI Definition — Fraud Detection Platform
**Auteur :** Data Scientist P4 | **Destinataires :** P2, P3, P5

---

## KPIs Métier (Business)

### KPI-1 : Taux de fraude global
- **Définition :** `nb_fraudes / nb_transactions_total * 100`
- **Valeur baseline :** 0.58% (dataset Kartik)
- **Cible :** < 0.30% après déploiement du modèle
- **Alerte :** > 1.0% sur une fenêtre glissante de 15 min
- **Requête :** `fraud_analytics.sql` — Requête 1
- **Dashboard :** `fraud_overview.json` — Panel 1

### KPI-2 : Montant total sauvé par jour
- **Définition :** `SUM(amt) WHERE is_fraud = 1 AND detected = true`
- **Valeur baseline :** ~$42,000/jour estimé
- **Cible :** > $60,000/jour
- **Alerte :** < $20,000/jour (sous-détection probable)
- **Requête :** `fraud_analytics.sql` — Requête 2
- **Dashboard :** `fraud_overview.json` — Panel 2

### KPI-3 : Nombre de fraudes détectées (24h)
- **Définition :** `COUNT(*) WHERE is_fraud_predicted = 1 AND is_fraud_actual = 1`
- **Cible :** Recall > 80% des fraudes réelles
- **Alerte :** Baisse > 20% vs moyenne 7 jours glissants
- **Dashboard :** `fraud_overview.json` — Panel 3

---

## KPIs Modèle (ML Performance)

### KPI-4 : Précision (Precision)
- **Définition :** `TP / (TP + FP)`
- **Cible :** > 85%
- **Alerte :** < 75% pendant 15 min consécutives
- **Impact métier :** trop de faux positifs → clients bloqués à tort → insatisfaction

### KPI-5 : Rappel (Recall)
- **Définition :** `TP / (TP + FN)`
- **Cible :** > 80%
- **Alerte :** < 70% pendant 15 min consécutives
- **Impact métier :** trop de faux négatifs → fraudes non détectées → pertes financières

### KPI-6 : F1-Score
- **Définition :** `2 * (Precision * Recall) / (Precision + Recall)`
- **Cible :** > 0.82
- **Alerte :** < 0.72 → déclencher re-training automatique
- **Requête :** `fraud_analytics.sql` — Requête 4

### KPI-7 : AUC-ROC
- **Définition :** Aire sous la courbe ROC
- **Cible :** > 0.95
- **Fréquence de calcul :** quotidienne (batch)
- **Alerte :** < 0.90 → investigation concept drift

---

## KPIs Opérationnels (Infrastructure)

### KPI-8 : Latence d'inférence P99
- **Définition :** 99e percentile du temps de réponse du modèle
- **Cible :** < 50ms
- **Alerte :** > 100ms → vérifier charge Kafka/Redis
- **SLA :** critique — un dépassement bloque les paiements en temps réel

### KPI-9 : Taux de faux positifs par jour
- **Définition :** `FP / (FP + TN) * 100`
- **Cible :** < 500 faux positifs/jour
- **Alerte :** > 1,000/jour → réviser le seuil de décision
- **Impact métier :** chaque faux positif = 1 client bloqué = risque de churn

### KPI-10 : Disponibilité du pipeline
- **Définition :** `uptime / total_time * 100`
- **Cible :** > 99.9% (SLA)
- **Alerte :** toute interruption > 30 secondes
- **Responsable :** P3 (DevOps)

---

## Tableau de Bord des Seuils

| KPI | Vert | Orange | Rouge |
|-----|------|--------|-------|
| Taux de fraude | < 0.5% | 0.5–1.0% | > 1.0% |
| Precision | > 85% | 75–85% | < 75% |
| Recall | > 80% | 70–80% | < 70% |
| F1-Score | > 0.82 | 0.72–0.82 | < 0.72 |
| Latence P99 | < 50ms | 50–100ms | > 100ms |
| Faux positifs/jour | < 500 | 500–1000 | > 1000 |

---

## Fréquences de Calcul

| KPI | Fréquence | Stockage |
|-----|-----------|---------|
| KPI-1, 2, 3 | Temps réel (1 min) | ClickHouse / PostgreSQL |
| KPI-4, 5, 6 | Toutes les heures | `model_predictions` table |
| KPI-7 | Quotidienne | MLflow / S3 |
| KPI-8 | Temps réel (continu) | Prometheus + Grafana |
| KPI-9 | Toutes les heures | `model_predictions` table |
| KPI-10 | Continu | Prometheus |
