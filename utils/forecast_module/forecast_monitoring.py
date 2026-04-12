"""
Sistema de Monitoramento de Performance em Tempo Real
====================================================

Monitora a performance dos modelos de forecasting em tempo real,
alertando sobre degradação de qualidade e fornecendo métricas de acompanhamento.

Características:
- Monitoramento em tempo real
- Alertas automáticos
- Métricas de performance
- Dashboard de monitoramento
- Histórico de performance
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import sqlite3
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

@dataclass
class PerformanceMetrics:
    """Métricas de performance de um modelo."""
    model_name: str
    category: str
    horizon: int
    mape: float
    rmse: float
    mae: float
    confidence: float
    timestamp: datetime
    data_points: int
    forecast_accuracy: float = None

@dataclass
class Alert:
    """Alerta de monitoramento."""
    alert_id: str
    alert_type: str  # "performance_degradation", "model_failure", "data_quality"
    severity: str    # "low", "medium", "high", "critical"
    message: str
    timestamp: datetime
    category: str = None
    model_name: str = None
    resolved: bool = False
    resolution_notes: str = None

class ForecastMonitor:
    """
    Monitor de performance de forecasting em tempo real.
    """
    
    def __init__(self, db_path: str = "forecast_monitoring.db"):
        self.db_path = db_path
        self._init_database()
        
        # Thresholds de alerta
        self.alert_thresholds = {
            'mape_critical': 50.0,
            'mape_high': 30.0,
            'mape_medium': 20.0,
            'confidence_low': 0.5,
            'data_points_min': 10
        }
    
    def _init_database(self):
        """Inicializa banco de dados para monitoramento."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabela de métricas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                category TEXT NOT NULL,
                horizon INTEGER NOT NULL,
                mape REAL NOT NULL,
                rmse REAL NOT NULL,
                mae REAL NOT NULL,
                confidence REAL NOT NULL,
                timestamp DATETIME NOT NULL,
                data_points INTEGER NOT NULL,
                forecast_accuracy REAL
            )
        ''')
        
        # Tabela de alertas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                category TEXT,
                model_name TEXT,
                resolved BOOLEAN DEFAULT FALSE,
                resolution_notes TEXT
            )
        ''')
        
        # Tabela de configurações de monitoramento
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def record_performance(self, metrics: PerformanceMetrics) -> bool:
        """Registra métricas de performance."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO performance_metrics 
                (model_name, category, horizon, mape, rmse, mae, confidence, 
                 timestamp, data_points, forecast_accuracy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metrics.model_name, metrics.category, metrics.horizon,
                metrics.mape, metrics.rmse, metrics.mae, metrics.confidence,
                metrics.timestamp, metrics.data_points, metrics.forecast_accuracy
            ))
            
            conn.commit()
            conn.close()
            
            # Verificar alertas
            self._check_alerts(metrics)
            
            return True
        except Exception as e:
            print(f"Erro ao registrar performance: {e}")
            return False
    
    def _check_alerts(self, metrics: PerformanceMetrics):
        """Verifica se métricas geram alertas."""
        alerts = []
        
        # Alerta de MAPE crítico
        if metrics.mape > self.alert_thresholds['mape_critical']:
            alerts.append(Alert(
                alert_id=f"mape_critical_{metrics.model_name}_{metrics.category}_{metrics.timestamp}",
                alert_type="performance_degradation",
                severity="critical",
                message=f"MAPE crítico: {metrics.mape:.1f}% para {metrics.model_name} em {metrics.category}",
                timestamp=datetime.now(),
                category=metrics.category,
                model_name=metrics.model_name
            ))
        
        # Alerta de MAPE alto
        elif metrics.mape > self.alert_thresholds['mape_high']:
            alerts.append(Alert(
                alert_id=f"mape_high_{metrics.model_name}_{metrics.category}_{metrics.timestamp}",
                alert_type="performance_degradation",
                severity="high",
                message=f"MAPE alto: {metrics.mape:.1f}% para {metrics.model_name} em {metrics.category}",
                timestamp=datetime.now(),
                category=metrics.category,
                model_name=metrics.model_name
            ))
        
        # Alerta de confiança baixa
        if metrics.confidence < self.alert_thresholds['confidence_low']:
            alerts.append(Alert(
                alert_id=f"confidence_low_{metrics.model_name}_{metrics.category}_{metrics.timestamp}",
                alert_type="performance_degradation",
                severity="medium",
                message=f"Confiança baixa: {metrics.confidence:.2f} para {metrics.model_name} em {metrics.category}",
                timestamp=datetime.now(),
                category=metrics.category,
                model_name=metrics.model_name
            ))
        
        # Alerta de dados insuficientes
        if metrics.data_points < self.alert_thresholds['data_points_min']:
            alerts.append(Alert(
                alert_id=f"data_insufficient_{metrics.model_name}_{metrics.category}_{metrics.timestamp}",
                alert_type="data_quality",
                severity="medium",
                message=f"Dados insuficientes: {metrics.data_points} pontos para {metrics.model_name} em {metrics.category}",
                timestamp=datetime.now(),
                category=metrics.category,
                model_name=metrics.model_name
            ))
        
        # Salvar alertas
        for alert in alerts:
            self._save_alert(alert)
    
    def _save_alert(self, alert: Alert):
        """Salva alerta no banco de dados."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO alerts 
                (alert_id, alert_type, severity, message, timestamp, category, model_name, resolved, resolution_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                alert.alert_id, alert.alert_type, alert.severity, alert.message,
                alert.timestamp, alert.category, alert.model_name, alert.resolved, alert.resolution_notes
            ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Erro ao salvar alerta: {e}")
    
    def get_recent_performance(self, hours: int = 24) -> pd.DataFrame:
        """Obtém performance recente."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            query = '''
                SELECT * FROM performance_metrics 
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
            '''
            
            df = pd.read_sql_query(query, conn, params=(cutoff_time,))
            conn.close()
            
            return df
        except Exception as e:
            print(f"Erro ao obter performance: {e}")
            return pd.DataFrame()
    
    def get_active_alerts(self) -> List[Alert]:
        """Obtém alertas ativos."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM alerts 
                WHERE resolved = FALSE
                ORDER BY timestamp DESC
            ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            alerts = []
            for row in rows:
                alerts.append(Alert(
                    alert_id=row[0],
                    alert_type=row[1],
                    severity=row[2],
                    message=row[3],
                    timestamp=datetime.fromisoformat(row[4]),
                    category=row[5],
                    model_name=row[6],
                    resolved=bool(row[7]),
                    resolution_notes=row[8]
                ))
            
            return alerts
        except Exception as e:
            print(f"Erro ao obter alertas: {e}")
            return []
    
    def get_performance_summary(self, days: int = 7) -> Dict[str, Any]:
        """Obtém resumo de performance."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            cutoff_time = datetime.now() - timedelta(days=days)
            
            # Métricas gerais
            query = '''
                SELECT 
                    model_name,
                    AVG(mape) as avg_mape,
                    AVG(rmse) as avg_rmse,
                    AVG(mae) as avg_mae,
                    AVG(confidence) as avg_confidence,
                    COUNT(*) as total_predictions
                FROM performance_metrics 
                WHERE timestamp >= ?
                GROUP BY model_name
                ORDER BY avg_mape ASC
            '''
            
            df = pd.read_sql_query(query, conn, params=(cutoff_time,))
            
            # Alertas por severidade
            alert_query = '''
                SELECT severity, COUNT(*) as count
                FROM alerts 
                WHERE timestamp >= ? AND resolved = FALSE
                GROUP BY severity
            '''
            
            alert_df = pd.read_sql_query(alert_query, conn, params=(cutoff_time,))
            
            conn.close()
            
            return {
                'model_performance': df.to_dict('records'),
                'alerts_by_severity': alert_df.to_dict('records'),
                'total_predictions': df['total_predictions'].sum(),
                'avg_mape': df['avg_mape'].mean(),
                'best_model': df.loc[df['avg_mape'].idxmin(), 'model_name'] if not df.empty else None
            }
        except Exception as e:
            print(f"Erro ao obter resumo: {e}")
            return {}
    
    def resolve_alert(self, alert_id: str, resolution_notes: str = None):
        """Resolve um alerta."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE alerts 
                SET resolved = TRUE, resolution_notes = ?
                WHERE alert_id = ?
            ''', (resolution_notes, alert_id))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Erro ao resolver alerta: {e}")
    
    def update_thresholds(self, new_thresholds: Dict[str, float]):
        """Atualiza thresholds de alerta."""
        self.alert_thresholds.update(new_thresholds)
        
        # Salvar no banco
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for key, value in new_thresholds.items():
                cursor.execute('''
                    INSERT OR REPLACE INTO monitoring_config (key, value, updated_at)
                    VALUES (?, ?, ?)
                ''', (key, str(value), datetime.now()))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Erro ao atualizar thresholds: {e}")
    
    def get_model_trends(self, model_name: str, days: int = 30) -> Dict[str, Any]:
        """Obtém tendências de performance de um modelo."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            cutoff_time = datetime.now() - timedelta(days=days)
            
            query = '''
                SELECT 
                    DATE(timestamp) as date,
                    AVG(mape) as avg_mape,
                    AVG(confidence) as avg_confidence,
                    COUNT(*) as predictions
                FROM performance_metrics 
                WHERE model_name = ? AND timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date
            '''
            
            df = pd.read_sql_query(query, conn, params=(model_name, cutoff_time))
            conn.close()
            
            if df.empty:
                return {'trend': 'no_data', 'message': 'Sem dados suficientes'}
            
            # Calcular tendência
            mape_trend = np.polyfit(range(len(df)), df['avg_mape'], 1)[0]
            confidence_trend = np.polyfit(range(len(df)), df['avg_confidence'], 1)[0]
            
            trend_direction = "improving" if mape_trend < 0 else "degrading" if mape_trend > 0 else "stable"
            
            return {
                'trend': trend_direction,
                'mape_trend': mape_trend,
                'confidence_trend': confidence_trend,
                'current_mape': df['avg_mape'].iloc[-1],
                'current_confidence': df['avg_confidence'].iloc[-1],
                'data_points': len(df)
            }
        except Exception as e:
            print(f"Erro ao obter tendências: {e}")
            return {'trend': 'error', 'message': str(e)}

class ForecastDashboard:
    """
    Dashboard de monitoramento de forecasting.
    """
    
    def __init__(self, monitor: ForecastMonitor):
        self.monitor = monitor
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """Gera dados para dashboard de monitoramento."""
        # Performance recente
        recent_performance = self.monitor.get_recent_performance(hours=24)
        
        # Alertas ativos
        active_alerts = self.monitor.get_active_alerts()
        
        # Resumo de performance
        performance_summary = self.monitor.get_performance_summary(days=7)
        
        # Tendências por modelo
        model_trends = {}
        if not recent_performance.empty:
            unique_models = recent_performance['model_name'].unique()
            for model in unique_models:
                model_trends[model] = self.monitor.get_model_trends(model, days=7)
        
        return {
            'recent_performance': recent_performance.to_dict('records'),
            'active_alerts': [alert.__dict__ for alert in active_alerts],
            'performance_summary': performance_summary,
            'model_trends': model_trends,
            'last_updated': datetime.now().isoformat()
        }
    
    def get_health_score(self) -> float:
        """Calcula score de saúde do sistema (0-100)."""
        try:
            performance_summary = self.monitor.get_performance_summary(days=7)
            active_alerts = self.monitor.get_active_alerts()
            
            # Score baseado na performance
            avg_mape = performance_summary.get('avg_mape', 50)
            mape_score = max(0, 100 - (avg_mape * 2))  # 50% MAPE = 0 pontos
            
            # Penalizar por alertas
            alert_penalty = 0
            for alert in active_alerts:
                if alert.severity == 'critical':
                    alert_penalty += 20
                elif alert.severity == 'high':
                    alert_penalty += 10
                elif alert.severity == 'medium':
                    alert_penalty += 5
                else:
                    alert_penalty += 2
            
            health_score = max(0, mape_score - alert_penalty)
            return min(100, health_score)
        except Exception as e:
            print(f"Erro ao calcular health score: {e}")
            return 0.0

# Funções de conveniência
def create_monitor(db_path: str = "forecast_monitoring.db") -> ForecastMonitor:
    """Cria instância do monitor."""
    return ForecastMonitor(db_path)

def record_model_performance(monitor: ForecastMonitor, model_name: str, category: str,
                           horizon: int, mape: float, rmse: float, mae: float,
                           confidence: float, data_points: int) -> bool:
    """Registra performance de um modelo."""
    metrics = PerformanceMetrics(
        model_name=model_name,
        category=category,
        horizon=horizon,
        mape=mape,
        rmse=rmse,
        mae=mae,
        confidence=confidence,
        timestamp=datetime.now(),
        data_points=data_points
    )
    return monitor.record_performance(metrics)

