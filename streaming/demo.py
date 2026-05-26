"""
Script de démonstration de la pipeline
Génère des données, lance les tests et affiche les résultats
"""

import subprocess
import sys
import os
import time
from pathlib import Path

# Couleurs
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

def print_header(text: str):
    """Affiche un titre"""
    print(f"\n{BLUE}{'='*60}{NC}")
    print(f"{BLUE}{text:^60}{NC}")
    print(f"{BLUE}{'='*60}{NC}\n")

def run_command(cmd: str, description: str = ""):
    """Exécute une commande"""
    if description:
        print(f"{YELLOW}▶ {description}...{NC}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"{RED}✗ Erreur{NC}")
            if result.stderr:
                print(result.stderr)
            return False
        
        if result.stdout:
            print(result.stdout)
        print(f"{GREEN}✓ Succès{NC}")
        return True
    
    except Exception as e:
        print(f"{RED}✗ Exception: {e}{NC}")
        return False

def demo():
    """Lance la démonstration"""
    
    print_header("FRAUD DETECTION STREAMING PIPELINE - DEMO")
    
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 1. Setup
    print_header("1. SETUP")
    
    print(f"{YELLOW}Vérification Python et dépendances...{NC}")
    if not run_command("python -m pytest --version", "pytest"):
        print(f"{YELLOW}Installation pytest...{NC}")
        run_command("pip install pytest pytest-spark", "pip install")
    
    # 2. Tests unitaires
    print_header("2. TESTS UNITAIRES")
    
    print(f"{YELLOW}Lancement tests...{NC}")
    result = subprocess.run(
        ["python", "-m", "pytest", "test_streaming.py", "-v", "--tb=short"],
        capture_output=False
    )
    
    if result.returncode != 0:
        print(f"{RED}✗ Tests échoués{NC}")
        return False
    
    print(f"{GREEN}✓ Tous les tests passés{NC}")
    
    # 3. Données mock
    print_header("3. GÉNÉRATION DONNÉES MOCK")
    
    print(f"{YELLOW}Créer 100 transactions mock...{NC}")
    
    from test_streaming import MockDataGenerator, TransactionGenerator
    
    generator = MockDataGenerator()
    transactions = generator.generate_transactions_batch(
        num_customers=10,
        transactions_per_customer=10,
        fraud_rate=0.1
    )
    
    print(f"{GREEN}✓ {len(transactions)} transactions générées{NC}")
    
    # Stats
    fraud_count = sum(1 for t in transactions if t.get("is_fraud_synthetic"))
    print(f"  - Total: {len(transactions)}")
    print(f"  - Frauduleuses: {fraud_count} ({fraud_count/len(transactions)*100:.1f}%)")
    print(f"  - Clients: {len(set(t['customer_id'] for t in transactions))}")
    
    # 4. Statistiques
    print_header("4. STATISTIQUES TRANSACTIONS")
    
    amounts = [t["amount"] for t in transactions]
    countries = set(t["merchant_country"] for t in transactions)
    currencies = set(t["currency"] for t in transactions)
    
    print(f"Montants (USD):")
    print(f"  - Min: ${min(amounts):.2f}")
    print(f"  - Max: ${max(amounts):.2f}")
    print(f"  - Moy: ${sum(amounts)/len(amounts):.2f}")
    
    print(f"\nDiversité:")
    print(f"  - Pays: {len(countries)} ({', '.join(sorted(countries))})")
    print(f"  - Devises: {', '.join(sorted(currencies))}")
    print(f"  - Marchands: {len(set(t['merchant_id'] for t in transactions))}")
    
    # 5. Exemples transactions
    print_header("5. EXEMPLES TRANSACTIONS")
    
    # Frauduleuse
    fraud_txn = next(t for t in transactions if t.get("is_fraud_synthetic"))
    print(f"{RED}Frauduleuse:{NC}")
    print(f"  Customer: {fraud_txn['customer_id']}")
    print(f"  Amount: ${fraud_txn['amount']:.2f} {fraud_txn['currency']}")
    print(f"  Country: {fraud_txn['merchant_country']}")
    print(f"  Card Present: {fraud_txn['card_present']}")
    
    # Normale
    normal_txn = next(t for t in transactions if not t.get("is_fraud_synthetic"))
    print(f"\n{GREEN}Normale:{NC}")
    print(f"  Customer: {normal_txn['customer_id']}")
    print(f"  Amount: ${normal_txn['amount']:.2f} {normal_txn['currency']}")
    print(f"  Country: {normal_txn['merchant_country']}")
    print(f"  Card Present: {normal_txn['card_present']}")
    
    # 6. Configuration
    print_header("6. CONFIGURATION PIPELINE")
    
    from config import get_config
    
    config = get_config()
    print(f"Kafka:")
    print(f"  - Brokers: {config.kafka.brokers}")
    print(f"  - Topics: {config.kafka.topic_input}, {config.kafka.topic_enriched}, {config.kafka.topic_predictions}")
    
    print(f"\nPostgreSQL:")
    print(f"  - Host: {config.postgres.host}:{config.postgres.port}")
    print(f"  - Database: {config.postgres.database}")
    
    print(f"\nSpark:")
    print(f"  - Master: {config.spark.master}")
    print(f"  - Checkpoint Dir: {config.spark.checkpoint_dir}")
    
    print(f"\nFeatures activées:")
    print(f"  - 10 min window: {config.window_10min}")
    print(f"  - 1 hour window: {config.window_1hour}")
    print(f"  - 24 hour window: {config.window_24h}")
    
    # 7. Summary
    print_header("RÉSUMÉ")
    
    print(f"""
{GREEN}✓ Setup complété{NC}
{GREEN}✓ Tests passés{NC}
{GREEN}✓ Données mock générées{NC}

{YELLOW}Prochaines étapes:{NC}

1. Initialiser Kafka & PostgreSQL:
   make init-all

2. Lancer la pipeline Spark:
   make stream

3. En parallèle, générer des données:
   make produce

4. Monitorer les résultats:
   - Kafka UI: http://localhost:8080
   - Spark UI: http://localhost:8081
   - PostgreSQL: psql postgresql://postgres@localhost/fraud_db

{YELLOW}Fichiers clés:{NC}
  - spark_streaming.py     : Pipeline Spark Streaming
  - test_streaming.py      : Tests et données mock
  - kafka_producer.py      : Générateur données Kafka
  - config.py              : Configuration
  - README.md              : Documentation complète
    """)
    
    return True


if __name__ == "__main__":
    try:
        success = demo()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}⏹ Arrêté par l'utilisateur{NC}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{RED}✗ Erreur: {e}{NC}")
        sys.exit(1)
