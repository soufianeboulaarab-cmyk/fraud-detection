# Fraud Detection Platform — Infrastructure DevOps

Plateforme de détection de fraude bancaire en temps réel.
Infrastructure complète déployée via Docker Compose, sans coût cloud.

---

## Démarrage rapide

```bash
git clone https://github.com/soufianeboulaarab-cmyk/fraud-detection.git
cd fraud-detection/devops
cp .env.example .env
# Remplir les mots de passe dans .env
docker compose up -d
docker compose ps
```

---

## Services disponibles

| Service | URL | Description |
|---|---|---|
| Kafka UI | http://localhost:8080 | Visualiser les topics et messages en temps réel |
| Spark Master | http://localhost:8081 | État du cluster Spark (1 master + 2 workers) |
| MLflow | http://localhost:5000 | Suivi et versioning des modèles ML |
| MinIO Console | http://localhost:9001 | Stockage fichiers S3-compatible (login dans .env) |
| Grafana | http://localhost:3000 | Dashboards monitoring (login dans .env) |
| Prometheus | http://localhost:9090 | Collecte de métriques système |
| PostgreSQL | localhost:5432 | Base de données principale (credentials dans .env) |

---

## Architecture

```
SOURCES (API / Simulation)
         │
         ▼
    Apache Kafka              → 3 brokers, 5 topics, haute disponibilité
         │
         ▼
 Spark Structured Streaming   → 1 master + 2 workers
         │
         ▼
     Modèle ML                → MLflow + MinIO
         │
         ▼
     PostgreSQL               → 3 tables : transactions, customers, fraud_labels
         │
         ▼
  Prometheus + Grafana        → Monitoring temps réel
```

---

## Ce que chaque membre doit savoir

---

### Personne 1 — Fullstack / API

**Technologie à utiliser :** `kafka-python`

Tu envoies tes transactions dans le topic **`transactions-raw`** sur `localhost:9092`.

Les 5 topics Kafka disponibles :

| Topic | Rôle | Partitions |
|---|---|---|
| transactions-raw | Tu envoies tes transactions ici | 10 |
| transactions-enriched | Spark écrit ici après enrichissement | 10 |
| predictions | Les scores ML sont publiés ici | 10 |
| fraud-labels | Feedback humain (fraudes confirmées) | 3 |
| alerts | Alertes pour les opérateurs | 3 |

Vérifie que tes messages arrivent sur http://localhost:8080

---

### Personne 2 — Machine Learning

**Technologies à utiliser :** `mlflow`, `psycopg2`, `boto3`

- Sauvegarde tes modèles sur **MLflow** → http://localhost:5000
- Récupère tes données d'entraînement depuis **PostgreSQL** → base `fraud_db`
- Tes artifacts (modèles sérialisés) sont stockés automatiquement dans **MinIO**
- Les tables disponibles : `transactions`, `customers`, `fraud_labels`

---

### Personne 4 — Data Analyst

**Technologie à utiliser :** `psycopg2` ou tout client SQL (DBeaver, TablePlus, pgAdmin)

Connexion PostgreSQL :

```
Host     : localhost
Port     : 5432
Database : fraud_db
User     : fraud_user
Password : voir fichier .env
```

Tables disponibles :

| Table | Contenu |
|---|---|
| transactions | Toutes les transactions avec score et décision |
| customers | Profil et historique de chaque client |
| fraud_labels | Fraudes confirmées par les opérateurs |

---

### Personne 5 — Data Pipeline Streaming

**Technologies à utiliser :** `PySpark`, `spark-sql-kafka`, `psycopg2`

- Spark Master URL : `spark://localhost:7077`
- Lis depuis Kafka : `localhost:9092` → topic `transactions-raw`
- Enrichis avec les données clients depuis PostgreSQL : `localhost:5432`
- Écris les résultats dans PostgreSQL → table `transactions`
- Consulte l'état du cluster sur http://localhost:8081

---

## Topics Kafka — Schéma d'un message

Chaque message dans `transactions-raw` a cette structure :

```json
{
  "tx_id": "uuid",
  "client_id": "C001",
  "amount": 250.00,
  "currency": "EUR",
  "country": "FR",
  "merchant_cat": "retail",
  "ip_address": "192.168.1.1",
  "timestamp": "2026-05-17T10:00:00"
}
```

---

## Schéma de la base de données

```
transactions
├── tx_id        UUID (clé primaire)
├── client_id    VARCHAR
├── amount       DECIMAL
├── currency     VARCHAR
├── country      VARCHAR
├── merchant_cat VARCHAR
├── ip_address   VARCHAR
├── timestamp    TIMESTAMPTZ
├── score        FLOAT (0.0 à 1.0, rempli par le modèle ML)
├── decision     VARCHAR (REJETER / VÉRIFIER / ACCEPTER)
└── is_fraud     BOOLEAN (confirmé par opérateur)

customers
├── client_id        VARCHAR (clé primaire)
├── credit_limit     DECIMAL
├── account_age_days INT
├── fraud_history    INT
├── country_declared VARCHAR
└── risk_score       FLOAT

fraud_labels
├── id          SERIAL
├── tx_id       UUID (référence transactions)
├── labeled_by  VARCHAR
├── labeled_at  TIMESTAMPTZ
└── is_fraud    BOOLEAN
```

---

## Simulation de transactions

Pour tester l'infrastructure sans attendre l'API de la Personne 1 :

```bash
cd devops
docker compose run --rm simulator
```

Visualise les messages en temps réel sur http://localhost:8080
→ fraud-cluster → Topics → transactions-raw → Messages

---

## Commandes utiles

```bash
# État de tous les services
docker compose ps

# Logs d'un service spécifique
docker compose logs kafka-1
docker compose logs spark-master
docker compose logs postgres

# Redémarrer un service
docker compose restart <nom_service>

# Arrêter toute l'infrastructure
docker compose down

# Arrêter et supprimer les volumes (reset complet)
docker compose down -v

# Lister les topics Kafka
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list

# Vérifier les tables PostgreSQL
docker exec postgres psql -U fraud_user -d fraud_db -c "\dt"

# Vérifier les données dans PostgreSQL
docker exec postgres psql -U fraud_user -d fraud_db -c "SELECT COUNT(*) FROM transactions;"
```

---

## Variables d'environnement (.env)

Copie `.env.example` en `.env` et remplis les valeurs :

```
POSTGRES_USER=fraud_user
POSTGRES_PASSWORD=ton_mot_de_passe
POSTGRES_DB=fraud_db
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=ton_mot_de_passe
GRAFANA_PASSWORD=ton_mot_de_passe
```
---

## Structure du repo

```
fraud-detection/
├── devops/                    ← Infrastructure complète (Personne 3)
│   ├── docker-compose.yml     ← Tous les services
│   ├── .env.example           ← Template des variables
│   ├── kafka/                 ← Configuration Kafka
│   ├── postgres/              ← Schéma SQL
│   ├── mlflow/                ← Dockerfile MLflow custom
│   ├── monitoring/
│   │   ├── prometheus/        ← Métriques + alertes
│   │   └── grafana/           ← Dashboards
│   └── simulation/            ← Script de simulation transactions
├── api/                       ← API REST (Personne 1)
├── ml/                        ← Modèle ML (Personne 2)
├── analytics/                 ← Analyses SQL (Personne 4)
└── streaming/                 ← Pipeline Spark (Personne 5)
```

---

## CI/CD

Un pipeline GitHub Actions valide automatiquement l'infrastructure à chaque push sur toutes les  branches :
- Validation syntaxe docker-compose
- Démarrage Kafka + création des topics
- Vérification PostgreSQL + tables
- Démarrage Spark + vérification UI

---
