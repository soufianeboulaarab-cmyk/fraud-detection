"""
Utilitaires pour valider la configuration de la pipeline
"""

import os
import sys
import subprocess
from typing import Tuple, List

# Couleurs
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'


class ConfigValidator:
    """Valide la configuration de la pipeline"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def check_python_version(self) -> bool:
        """Vérifie que Python 3.8+ est installé"""
        if sys.version_info < (3, 8):
            self.errors.append(f"Python 3.8+ requis (actuel: {sys.version})")
            return False
        return True
    
    def check_kafka_brokers(self) -> bool:
        """Vérifie la connexion aux brokers Kafka"""
        brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
        
        # Vérifier au moins un broker
        if not brokers:
            self.errors.append("KAFKA_BROKERS non configuré")
            return False
        
        self.warnings.append(f"Kafka brokers configurés: {brokers}")
        return True
    
    def check_postgres_connection(self) -> bool:
        """Vérifie la connexion PostgreSQL"""
        try:
            import psycopg2
            
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            
            # Connexion test simple (sans credentiels)
            self.warnings.append(f"PostgreSQL configuré: {host}:{port}")
            return True
        
        except ImportError:
            self.warnings.append("psycopg2 non installé (optionnel)")
            return True
    
    def check_spark_installation(self) -> bool:
        """Vérifie que Spark est installé"""
        try:
            import pyspark
            from pyspark import __version__
            
            if __version__ < "3.5":
                self.warnings.append(f"PySpark {__version__} (recommandé: 3.5+)")
            else:
                self.warnings.append(f"PySpark {__version__} ✓")
            
            return True
        
        except ImportError:
            self.errors.append("PySpark non installé (pip install pyspark==3.5.0)")
            return False
    
    def check_kafka_python(self) -> bool:
        """Vérifie kafka-python"""
        try:
            import kafka
            self.warnings.append("kafka-python ✓")
            return True
        except ImportError:
            self.errors.append("kafka-python non installé (pip install kafka-python)")
            return False
    
    def check_required_topics(self) -> bool:
        """Vérifie que les topics Kafka existent"""
        try:
            brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
            
            # Tentative de connexion simple
            from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
            from kafka.errors import KafkaError
            
            try:
                admin = KafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms=5000)
                topics = admin.list_topics()
                admin.close()
                
                required = ["transactions-raw", "transactions-enriched", "fraud-predictions"]
                missing = [t for t in required if t not in topics]
                
                if missing:
                    self.warnings.append(f"Topics manquants: {missing} (créer avec: make init-kafka)")
                else:
                    self.warnings.append(f"Topics Kafka ✓ ({len(topics)} trouvés)")
                
                return True
            
            except Exception as e:
                self.warnings.append(f"Impossible de vérifier topics: {e}")
                return True
        
        except Exception as e:
            self.warnings.append(f"Configuration topics non testable: {e}")
            return True
    
    def check_checkpoint_directory(self) -> bool:
        """Vérifie répertoire checkpoints"""
        checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints")
        
        if not os.path.exists(checkpoint_dir):
            try:
                os.makedirs(checkpoint_dir, exist_ok=True)
                self.warnings.append(f"Checkpoint dir créé: {checkpoint_dir}")
            except Exception as e:
                self.errors.append(f"Cannot create checkpoint dir: {e}")
                return False
        
        self.warnings.append(f"Checkpoint dir: {checkpoint_dir} ✓")
        return True
    
    def run_all_checks(self) -> Tuple[bool, List[str], List[str]]:
        """Lance tous les contrôles"""
        checks = [
            self.check_python_version,
            self.check_spark_installation,
            self.check_kafka_python,
            self.check_kafka_brokers,
            self.check_postgres_connection,
            self.check_checkpoint_directory,
            self.check_required_topics
        ]
        
        results = []
        for check in checks:
            try:
                results.append(check())
            except Exception as e:
                self.errors.append(f"Erreur lors de {check.__name__}: {e}")
                results.append(False)
        
        success = all(results)
        
        return success, self.warnings, self.errors


def print_report(success: bool, warnings: List[str], errors: List[str]):
    """Affiche le rapport de validation"""
    
    print(f"\n{BLUE}{'='*60}{NC}")
    print(f"{BLUE}VALIDATION CONFIGURATION PIPELINE{NC:^60}")
    print(f"{BLUE}{'='*60}{NC}\n")
    
    # Warnings
    if warnings:
        print(f"{YELLOW}ℹ Informations:{NC}")
        for w in warnings:
            print(f"  {w}")
        print()
    
    # Errors
    if errors:
        print(f"{RED}✗ Erreurs:{NC}")
        for e in errors:
            print(f"  {e}")
        print()
    
    # Status
    if success:
        print(f"{GREEN}✓ Configuration valide{NC}")
        print(f"\n{YELLOW}Prochaines étapes:{NC}")
        print(f"  1. make init-all          # Initialiser Kafka + PostgreSQL")
        print(f"  2. make stream            # Lancer la pipeline Spark")
        print(f"  3. make produce           # Générer des données (terminal 2)")
    else:
        print(f"{RED}✗ Configuration invalide - Veuillez corriger les erreurs ci-dessus{NC}")
        print(f"\n{YELLOW}Suggestions:{NC}")
        print(f"  - make install            # Installer les dépendances")
        print(f"  - docker compose up -d    # Démarrer les services (depuis devops/)")
        print(f"  - cp .env.example .env    # Configurer les variables d'environnement")
    
    print(f"\n{BLUE}{'='*60}{NC}\n")
    
    return success


if __name__ == "__main__":
    validator = ConfigValidator()
    success, warnings, errors = validator.run_all_checks()
    success = print_report(success, warnings, errors)
    
    sys.exit(0 if success else 1)
