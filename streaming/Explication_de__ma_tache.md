**Résumé de la tâche**
- **But**: Déployer une pipeline de streaming pour ingérer des transactions depuis Kafka, enrichir avec la table clients, calculer des features temps réel (fenêtres 10min, 1h, 24h), écrire les résultats vers Kafka ou une base SQL, gérer checkpoints Spark, et superviser le débit et le lag.

**Tâches concrètes**
- **Déployer brokers Kafka**: lancer un cluster Kafka (local/dev via `devops/docker-compose.yml`).
- **Créer topics**: `transactions-raw`, `transactions-enriched`, `transactions-predictions` (script: `devops/kafka/create-topics.sh`).
- **Lire flux Kafka**: Spark Structured Streaming lit via `format("kafka")`.
- **Joindre clients**: joindre le flux avec la table `clients` (source SQL/Postgres ou broadcast join selon la taille).
- **Créer features temps réel**: rolling/windowed aggregations (10min, 1h, 24h) et features dérivées.
- **Écrire résultats**: réécrire vers Kafka ou persister dans une base SQL (Postgres).
- **Checkpoints Spark**: configurer `checkpointLocation` pour tolérance aux pannes.
- **Mesures & monitoring**: throughput (msg/s), lag Kafka, erreurs pipeline (log + alertes Prometheus/Grafana).

**Comment ça marche (flux global)**
- 1) Les producteurs envoient des messages transaction sur `transactions-raw`.
- 2) Spark lit le flux avec `spark.readStream.format("kafka")...` et décode la clé/valeur.
- 3) Les événements sont nettoyés et enrichis (ex: jointure avec clients chargés depuis Postgres).
- 4) On créé des features temps réel:
  - fenêtre 10min: `count` des transactions (feature: `cnt_10m`)
  - fenêtre 1h: `sum` des montants (feature: `sum_1h`)
  - fenêtre 24h: `approx_count_distinct` ou `collect_set` des pays (feature: `countries_24h`)
- 5) Les résultats sont écrits en sortie (Kafka topic `transactions-enriched` ou table SQL `predictions`).
- 6) Checkpoints assurent reprise; métriques exposées à Prometheus et dashboards Grafana surveillent latence, lag et erreurs.

**Points d'implémentation importants**
- Utiliser Structured Streaming avec `writeStream` et `outputMode` adapté (`append`, `update` selon opérations).
- Pour les agrégations sur fenêtres event-time: définir colonne `event_time`, `watermark` et `groupBy(window(event_time, "10 minutes"), key)`.
- Pour joindre table clients: si petite, charger en broadcast via `spark.read.jdbc(...)` puis `broadcast()`; sinon utiliser une jointure d'état ou lookup service.
- Checkpoint: définir un dossier accessible (`hdfs://` ou volume Docker) et `option("checkpointLocation", "/path/checkpoint")`.
- Mesures: instrumenter counts via `StreamingQueryListener`, exporter métriques Spark et Kafka (consumer group lag) vers Prometheus.

**Commandes courantes (exécution Docker)**
```
# Démarrer tout (Kafka, ZK, Postgres, Prometheus, Grafana)
docker compose -f devops/docker-compose.yml up -d

# Créer topics Kafka (à partir du conteneur ou local)
./devops/kafka/create-topics.sh

# Lancer la pipeline Spark (si image / container prévu)
docker compose -f devops/docker-compose.yml exec spark-worker bash -c "python3 /opt/app/spark_streaming.py"

# Lancer tests unitaires locaux (si vous êtes dans un conteneur Python)
pytest streaming/test_streaming.py
```

**Fichiers clés et rôle de chaque fichier (dossier `streaming/`)**
- **`00_START_HERE.md`**: guide d'introduction rapide pour démarrer le module streaming.
- **`config.py`**: configuration centralisée (URLs Kafka, topics, Postgres DSN, chemins checkpoint, paramètres Spark).
- **`demo.py`**: exemple d'exécution simple ou démonstration ad-hoc.
- **`docker_startup.sh`**: script d'entrée pour containers Docker (prépare l'environnement puis lance la pipeline).
- **`INDEX.md`**: index documentaire du dossier streaming.
- **`init_kafka.sh`**: script d'initialisation Kafka (attente des services, création topics si nécessaire).
- **`init_postgres.sh`**: script de préparation/initialisation de la base Postgres (schémas, inserts de test pour `clients`).
- **`kafka_producer.py`**: producteur de test (génère des transactions et les pousse sur `transactions-raw`).
- **`Makefile`**: tâches utilitaires (build, run, test, clean).
- **`monitoring.py`**: métriques et export Prometheus (throughput, erreurs, latence), et routines de vérification du lag Kafka.
- **`PROJECT_SUMMARY.md`**: résumé du projet et objectifs pour le module streaming.
- **`QUICK_START.md`**: pas-à-pas pour démarrer rapidement la pipeline en local/dev.
- **`README.md`**: documentation générale et liens vers guides détaillés.
- **`requirements.txt`**: dépendances Python pour exécution et tests (pyspark, confluent-kafka, pytest, prometheus-client, psycopg2-binary...).
- **`spark_streaming_pipeline.py`**: implémentation principale de la pipeline Structured Streaming (lecture Kafka, transformations, agrégations fenêtrées, écritures, checkpoints).
- **`spark_streaming.py`**: wrapper / job launcher qui configure SparkSession puis appelle `spark_streaming_pipeline.py`.
- **`startup.sh`**: script pour lancer la stack locale (combinaison d'`init_*.sh` et `docker_startup.sh`).
- **`test_mock_producer.py`**: test qui simule un producteur et envoie des messages mock pour tests d'intégration.
- **`test_streaming.py`**: tests unitaires/ d'intégration pour la logique de transformation et les agrégations (utilise données mock et fixtures Spark).
- **`validate_config.py`**: vérifications pré-exécution des variables de configuration et connexions (Kafka, Postgres).

**Autres fichiers utiles dans le dépôt**
- **`devops/docker-compose.yml`**: configuration Docker Compose pour Kafka, Zookeeper, Spark, Postgres, Prometheus et Grafana.
- **`devops/kafka/create-topics.sh`**: script pour créer automatiquement les topics nécessaires.
- **`devops/postgres/init.sql`**: schéma et données d'exemple pour la table `clients` (si présent dans `devops/postgres/`).

**Tests & validation**
- `test_streaming.py` doit contenir des fixtures Spark (SparkSession en mode local) et des exemples d'événements pour valider les agrégations et la jointure clients.
- `test_mock_producer.py` et `kafka_producer.py` servent à injecter données dans Kafka pour tester bout-à-bout.

**Surveillance et observabilité**
- Exposer métriques via `prometheus-client` et scrapper Prometheus.
- Dashboards Grafana pour visualiser throughput, erreurs, lag consumer group, temps de traitement micro-batch.

**Conseils d'exploitation / bonnes pratiques**
- Toujours configurer `watermark` pour éviter état infini sur les fenêtres.
- Externaliser le `checkpointLocation` dans un volume persistant monté dans Docker.
- Partitionner les topics selon la clé de partition pertinente (ex: `client_id`) pour scalabilité.
- Faire des tests unitaires rapides sur transformations avant de tester le job intégré.

Si tu veux, j'ajoute des sections supplémentaires: instructions détaillées pour exécuter la stack Docker, exemple concret d'agrégation Spark (code prêt à coller) ou j'exécute les tests dans un conteneur Docker maintenant.