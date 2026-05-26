"""
Utilitaires de monitoring pour la pipeline
"""

import time
import logging
from datetime import datetime
from typing import Dict, Optional
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """Collecte des métriques pipeline"""
    
    def __init__(self, window_size: int = 1000):
        """
        Initialise le collecteur de métriques
        
        Args:
            window_size: Nombre de points à garder en mémoire
        """
        self.window_size = window_size
        
        # Métriques
        self.messages_processed = 0
        self.messages_errored = 0
        self.processing_times = deque(maxlen=window_size)
        self.latencies = deque(maxlen=window_size)
        
        # Temps
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.last_report_count = 0
    
    def record_message(self, processing_time_ms: float, latency_ms: float):
        """Enregistre le traitement d'un message"""
        self.messages_processed += 1
        self.processing_times.append(processing_time_ms)
        self.latencies.append(latency_ms)
    
    def record_error(self):
        """Enregistre une erreur"""
        self.messages_errored += 1
    
    def get_throughput(self) -> float:
        """Retourne throughput en messages/sec"""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.messages_processed / elapsed
        return 0.0
    
    def get_average_latency(self) -> float:
        """Retourne latence moyenne en ms"""
        if len(self.latencies) > 0:
            return sum(self.latencies) / len(self.latencies)
        return 0.0
    
    def get_p95_latency(self) -> float:
        """Retourne latence P95 en ms"""
        if len(self.latencies) < 2:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[idx]
    
    def get_p99_latency(self) -> float:
        """Retourne latence P99 en ms"""
        if len(self.latencies) < 2:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[idx]
    
    def get_error_rate(self) -> float:
        """Retourne taux d'erreur en %"""
        total = self.messages_processed + self.messages_errored
        if total > 0:
            return (self.messages_errored / total) * 100
        return 0.0
    
    def get_snapshot(self) -> Dict:
        """Retourne snapshot des métriques"""
        return {
            "messages_processed": self.messages_processed,
            "messages_errored": self.messages_errored,
            "throughput_msg_per_sec": self.get_throughput(),
            "avg_latency_ms": self.get_average_latency(),
            "p95_latency_ms": self.get_p95_latency(),
            "p99_latency_ms": self.get_p99_latency(),
            "error_rate_percent": self.get_error_rate(),
            "uptime_sec": time.time() - self.start_time
        }
    
    def report(self):
        """Log un rapport des métriques"""
        snapshot = self.get_snapshot()
        
        logger.info(
            f"METRICS | "
            f"Processed: {snapshot['messages_processed']} | "
            f"Throughput: {snapshot['throughput_msg_per_sec']:.0f} msg/sec | "
            f"AvgLat: {snapshot['avg_latency_ms']:.1f}ms | "
            f"P95Lat: {snapshot['p95_latency_ms']:.1f}ms | "
            f"Errors: {snapshot['messages_errored']} ({snapshot['error_rate_percent']:.2f}%) | "
            f"Uptime: {snapshot['uptime_sec']:.0f}s"
        )
        
        self.last_report_time = time.time()
        self.last_report_count = self.messages_processed
    
    def should_report(self, interval_sec: int = 60) -> bool:
        """Vérifie si on doit faire un rapport"""
        return (time.time() - self.last_report_time) > interval_sec


class KafkaLagMonitor:
    """Monitore le lag Kafka"""
    
    def __init__(self):
        """Initialise le monitoring lag"""
        self.lag_history = defaultdict(deque)
    
    def record_lag(self, partition: int, lag: int):
        """Enregistre le lag pour une partition"""
        self.lag_history[partition].append({
            'timestamp': datetime.utcnow(),
            'lag': lag
        })
    
    def get_current_lag(self, partition: int) -> Optional[int]:
        """Retourne le lag actuel pour une partition"""
        history = self.lag_history[partition]
        if history:
            return history[-1]['lag']
        return None
    
    def get_total_lag(self) -> int:
        """Retourne le lag total"""
        total = 0
        for partition_data in self.lag_history.values():
            if partition_data:
                total += partition_data[-1]['lag']
        return total
    
    def is_lagging(self, threshold: int = 10000) -> bool:
        """Vérifie si le lag dépasse le seuil"""
        return self.get_total_lag() > threshold
    
    def get_status(self) -> Dict:
        """Retourne statut du lag"""
        total_lag = self.get_total_lag()
        
        return {
            "total_lag": total_lag,
            "is_lagging": self.is_lagging(),
            "partitions": {
                p: self.get_current_lag(p)
                for p in self.lag_history.keys()
            }
        }


class AlertManager:
    """Gère les alertes de la pipeline"""
    
    ALERT_LEVELS = {
        "INFO": 1,
        "WARNING": 2,
        "ERROR": 3,
        "CRITICAL": 4
    }
    
    def __init__(self):
        """Initialise le gestionnaire d'alertes"""
        self.alerts = []
    
    def create_alert(
        self,
        level: str,
        message: str,
        context: Optional[Dict] = None
    ):
        """Crée une alerte"""
        alert = {
            "timestamp": datetime.utcnow(),
            "level": level,
            "message": message,
            "context": context or {}
        }
        self.alerts.append(alert)
        
        # Log selon le niveau
        if level == "CRITICAL":
            logger.critical(f"ALERT: {message} | {context}")
        elif level == "ERROR":
            logger.error(f"ALERT: {message} | {context}")
        elif level == "WARNING":
            logger.warning(f"ALERT: {message} | {context}")
        else:
            logger.info(f"ALERT: {message}")
    
    def alert_high_lag(self, lag: int, threshold: int = 10000):
        """Alerte si lag élevé"""
        self.create_alert(
            "WARNING",
            f"Kafka lag élevé détecté",
            {"current_lag": lag, "threshold": threshold}
        )
    
    def alert_high_error_rate(self, error_rate: float, threshold: float = 5.0):
        """Alerte si taux d'erreur élevé"""
        self.create_alert(
            "ERROR",
            f"Taux d'erreur élevé détecté",
            {"error_rate_percent": error_rate, "threshold": threshold}
        )
    
    def alert_low_throughput(self, throughput: float, threshold: float = 100.0):
        """Alerte si throughput faible"""
        self.create_alert(
            "WARNING",
            f"Throughput faible détecté",
            {"throughput": throughput, "threshold": threshold}
        )
    
    def alert_high_latency(self, latency: float, threshold: float = 1000.0):
        """Alerte si latence élevée"""
        self.create_alert(
            "WARNING",
            f"Latence élevée détectée",
            {"latency_ms": latency, "threshold": threshold}
        )
    
    def get_recent_alerts(self, minutes: int = 5):
        """Retourne alertes récentes"""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        return [a for a in self.alerts if a['timestamp'] > cutoff]


if __name__ == "__main__":
    # Test
    metrics = PipelineMetrics()
    
    # Simuler traitement
    for i in range(100):
        metrics.record_message(processing_time_ms=5.5, latency_ms=10 + (i % 50))
    
    metrics.report()
