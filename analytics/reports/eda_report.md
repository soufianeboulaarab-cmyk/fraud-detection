# EDA Report — Fraud Detection
**Dataset :** Kaggle Fraud Detection (Kartik) | **Auteur :** Data Scientist P4

---

## 1. Description du Dataset

| Propriété | Valeur |
|-----------|--------|
| Fichiers | `fraudTrain.csv` + `fraudTest.csv` |
| Lignes totales | ~1,296,675 transactions |
| Colonnes | 23 |
| Période | Janvier 2019 — Décembre 2020 |
| Taux de fraude | ~0.58% (déséquilibre sévère) |
| Fraudes totales | ~7,506 transactions |

### Colonnes disponibles
| Colonne | Type | Description |
|---------|------|-------------|
| `trans_date_trans_time` | datetime | Horodatage de la transaction |
| `cc_num` | int | Numéro de carte (anonymisé) |
| `merchant` | string | Nom du commerçant |
| `category` | string | Catégorie du commerçant (14 valeurs) |
| `amt` | float | Montant en USD |
| `lat` / `long` | float | Coordonnées du titulaire |
| `merch_lat` / `merch_long` | float | Coordonnées du commerçant |
| `city_pop` | int | Population de la ville |
| `dob` | date | Date de naissance du titulaire |
| `gender` | string | Genre (M/F) |
| `is_fraud` | int | Cible (0=normal, 1=fraude) |

---

## 2. Qualité des Données

- **Valeurs manquantes :** 0 sur toutes les colonnes
- **Doublons :** 0 transaction dupliquée
- **Outliers `amt` :** p99 = $1,189 | max = $28,948 → appliquer log1p
- **Colonnes inutiles :** `first`, `last`, `street`, `zip`, `trans_num`, `Unnamed: 0`

---

## 3. Analyse Univariée

### Montants (`amt`)
- Médiane globale : **$47.17**
- Médiane fraudes : **$312.44** (+563%)
- Médiane normal : **$46.82**
- Distribution très asymétrique → transformation `log1p` obligatoire

### Heure de transaction (`hour`)
- Volume maximal : 10h-14h (heures de bureau)
- Fraudes maximales : **22h-4h** (taux ~1.8% vs 0.3% en journée)
- Pic absolu fraude : **2h du matin**

### Distance domicile-commerçant (`distance`)
- Calculée via haversine simplifié sur lat/long
- Médiane fraudes : **~156 km**
- Médiane normal : **~18 km**
- 91% des fraudes dépassent 50 km

### Population ville (`city_pop`)
- Les fraudes sont surreprésentées dans les petites villes (< 5,000 hab.)
- Taux de fraude villes < 1,000 hab. : **1.2%** vs **0.4%** pour > 100,000 hab.

---

## 4. Analyse par Catégorie

| Catégorie | Taux de fraude | Volume |
|-----------|---------------|--------|
| `shopping_net` | ~1.4% | Élevé |
| `misc_net` | ~1.2% | Moyen |
| `grocery_pos` | ~0.8% | Très élevé |
| `shopping_pos` | ~0.6% | Élevé |
| `kids_pets` | ~0.3% | Faible |
| `health_fitness` | ~0.2% | Faible |

→ Les catégories **en ligne** (`_net`) ont un taux de fraude 2x supérieur aux catégories physiques (`_pos`).

---

## 5. Analyse Temporelle

- **Saisonnalité mensuelle :** légère hausse en décembre (+0.2%)
- **Jour de la semaine :** week-end légèrement plus risqué (+0.15%)
- **Pattern nocturne :** signal le plus fort, confirmé statistiquement (Mann-Whitney p < 0.001)

---

## 6. Corrélations avec `is_fraud`

| Feature | Corrélation Pearson |
|---------|-------------------|
| `amt_log` | +0.19 |
| `distance_log` | +0.17 |
| `is_night` | +0.12 |
| `city_pop_log` | -0.08 |
| `hour` | +0.06 |
| `day_of_week` | +0.01 |

---

## 7. Conclusions et Recommandations

**Pour P2 (ML) :**
- Utiliser `log1p` sur `amt` et `distance` obligatoirement
- Gérer le déséquilibre de classes : SMOTE ou `class_weight='balanced'`
- Features les plus importantes : `amt_log`, `distance_log`, `is_night`, `category`, `tx_count_1h`

**Pour P3 (DevOps) :**
- Indexer `trans_date_trans_time` et `cc_num` en priorité
- Partitionner la table par mois pour les requêtes analytiques

**Pour P5 (Streaming) :**
- `tx_count_1h` est la feature temps réel la plus critique
- Fenêtre de 10 min suffisante pour détecter les tests de carte
