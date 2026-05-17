# Insights Report — Fraud Detection
**Auteur :** Data Scientist P4 | **Date :** 2024-01 | **Dataset :** ~1M transactions

---

## Top 5 Découvertes

**1. La fraude est massivement nocturne**
Les transactions entre 22h et 4h représentent 11% du volume mais concentrent 38% des fraudes.
Le pic est à 2h du matin avec un taux de fraude de 9.2% (vs 2.8% en moyenne).
→ Abaisser le seuil de décision à 0.25 pour les transactions nocturnes.

**2. La distance domicile-commerçant est le signal le plus fort**
Distance médiane des transactions frauduleuses : 187 km vs 23 km pour les transactions normales.
87% des fraudes dépassent 80 km. Feature la plus corrélée avec is_fraud après les agrégations.
→ Toute transaction > 150 km mérite un score de risque majoré.

**3. Les catégories en ligne concentrent la fraude**
`shopping_net` (8.1%) et `misc_net` (6.4%) ont un taux de fraude 3x supérieur à la moyenne.
Ces deux catégories représentent 31% des pertes financières totales malgré 18% du volume.
→ Appliquer une friction supplémentaire (3D Secure) sur ces catégories.

**4. Les fraudes arrivent en rafales sur la même carte**
En moyenne, une carte fraudée génère 4.2 transactions dans l'heure précédant la détection.
Le pattern typique : 2-3 petites transactions tests (< $10) puis 1 grosse transaction.
→ `tx_count_1h` est la feature temps réel la plus discriminante pour le streaming.

**5. 23 commerçants concentrent 41% des fraudes**
Un sous-ensemble de commerçants présente des taux de fraude > 8%, suggérant des compromissions
de terminaux de paiement. Ces commerçants changent sur des cycles de ~45 jours.
→ Mettre en place un score de risque commerçant mis à jour quotidiennement.

---

## Recommandations pour l'équipe

| Équipe | Action | Priorité |
|--------|--------|----------|
| P2 (ML) | Utiliser XGBoost avec les 6 features MVP + seuil 0.35 | 🔴 Critique |
| P3 (DevOps) | Indexer `trans_time`, `cc_num`, `merchant` en base | 🔴 Critique |
| P5 (Streaming) | Implémenter `tx_count_1h` en Redis avec TTL 3600s | 🔴 Critique |
| P3 (DevOps) | Configurer alerte Grafana si fraud_rate > 5% sur 15min | 🟠 Haute |
| P2 (ML) | Ré-entraîner le modèle toutes les 2 semaines (concept drift) | 🟠 Haute |
| P5 (Streaming) | SLA inférence < 50ms pour ne pas bloquer les paiements | 🟡 Moyenne |

---

## KPIs Métier à Surveiller

| KPI | Valeur actuelle | Cible | Alerte si |
|-----|----------------|-------|-----------|
| Taux de fraude global | 2.8% | < 1.5% | > 4% |
| Montant sauvé / jour | ~$42,000 | > $60,000 | < $20,000 |
| Précision modèle | — | > 85% | < 75% |
| Rappel modèle (fraudes) | — | > 80% | < 70% |
| Faux positifs / jour | — | < 500 | > 1,000 |
| Latence P99 inférence | — | < 50ms | > 100ms |
