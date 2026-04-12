"""
Sistema de Métricas e Validação de Forecast
=========================================

Implementa cálculo robusto de métricas de qualidade de forecast
sem heurísticas fixas, apenas backtesting real.

Métricas implementadas:
- MAPE (Mean Absolute Percentage Error)
- RMSE (Root Mean Squared Error) 
- MAE (Mean Absolute Error)
- R² (Coefficient of Determination)
- Coverage (% de valores dentro do intervalo de confiança)
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ForecastMetrics:
    """Container para métricas de forecast."""
    mape: float
    rmse: float
    mae: float
    r_squared: float
    coverage: float
    reliability_text: str
    sample_size: int
    # Novo: score composto (0-100) para depuração e aferição em terminal
    reliability_score: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'mape': self.mape,
            'rmse': self.rmse,
            'mae': self.mae,
            'r_squared': self.r_squared,
            'coverage': self.coverage,
            'reliability': self.reliability_text,
            'sample_size': self.sample_size,
            'reliability_score': self.reliability_score
        }


class ForecastValidator:
    """Validador de qualidade de forecast com backtesting."""
    
    def __init__(self, confidence_level: float = 0.95):
        """
        Args:
            confidence_level: Nível de confiança para intervalos (default: 95%)
        """
        self.confidence_level = confidence_level
        self.z_score = 1.96  # Z-score para 95% de confiança
    
    def calculate_metrics(
        self, 
        actual: pd.Series, 
        predicted: pd.Series,
        lower_bound: Optional[pd.Series] = None,
        upper_bound: Optional[pd.Series] = None
    ) -> ForecastMetrics:
        """
        Calcula todas as métricas de qualidade do forecast.
        
        Args:
            actual: Valores reais
            predicted: Valores previstos
            lower_bound: Limite inferior do intervalo de confiança
            upper_bound: Limite superior do intervalo de confiança
            
        Returns:
            ForecastMetrics com todas as métricas calculadas
        """
        # Remover NaNs
        mask = ~(actual.isna() | predicted.isna())
        actual_clean = actual[mask]
        predicted_clean = predicted[mask]
        
        if len(actual_clean) < 3:
            # Sample muito pequeno - retornar métricas default
            return ForecastMetrics(
                mape=100.0,
                rmse=float('inf'),
                mae=float('inf'),
                r_squared=-1.0,
                coverage=0.0,
                reliability_text="Insuficiente (< 3 pontos)",
                sample_size=len(actual_clean)
            )
        
        # MAPE
        mape = self._calculate_mape(actual_clean, predicted_clean)
        
        # RMSE / MAE
        rmse = np.sqrt(np.mean((actual_clean - predicted_clean) ** 2))
        mae = np.mean(np.abs(actual_clean - predicted_clean))
        
        # Escala de referência para normalização (evita dependência da moeda)
        mean_actual = float(np.mean(actual_clean)) if len(actual_clean) > 0 else 0.0
        nrmse = (rmse / mean_actual * 100.0) if mean_actual > 0 else None
        nmae = (mae / mean_actual * 100.0) if mean_actual > 0 else None
        
        # R²
        r_squared = self._calculate_r_squared(actual_clean, predicted_clean)
        
        # Coverage (se bounds disponíveis)
        coverage = 0.0
        if lower_bound is not None and upper_bound is not None:
            coverage = self._calculate_coverage(
                actual_clean, 
                lower_bound[mask], 
                upper_bound[mask]
            )
        
        # Heurística poderosa: score + texto (usa MAPE, NRMSE, NMAE, R², Coverage)
        score, reliability_text = self._compute_reliability_score_and_text(
            mape=mape,
            r_squared=r_squared,
            coverage=coverage,
            rmse=rmse,
            mae=mae,
            scale=mean_actual
        )
        
        # Log no terminal para aferição
        try:
            print(
                f"[Forecast Metrics] MAPE={mape:.2f}% | RMSE={rmse:.2f} | MAE={mae:.2f} | "
                f"R2={r_squared:.2f} | Coverage={coverage:.1f}% | Scale(mean)={mean_actual:.2f} | "
                f"NRMSE={(nrmse if nrmse is not None else float('nan')):.2f}% | NMAE={(nmae if nmae is not None else float('nan')):.2f}% | "
                f"ReliabilityScore={score:.1f} | Reliability={reliability_text}"
            )
        except Exception:
            pass
        
        return ForecastMetrics(
            mape=mape,
            rmse=rmse,
            mae=mae,
            r_squared=r_squared,
            coverage=coverage,
            reliability_text=reliability_text,
            sample_size=len(actual_clean),
            reliability_score=score
        )
    
    def _calculate_mape(self, actual: pd.Series, predicted: pd.Series) -> float:
        """Calcula MAPE tratando zeros e outliers."""
        # Filtrar zeros do denominador
        mask = actual != 0
        if mask.sum() == 0:
            return 100.0
        
        actual_filtered = actual[mask]
        predicted_filtered = predicted[mask]
        
        # MAPE
        ape = np.abs((actual_filtered - predicted_filtered) / actual_filtered) * 100
        
        # Remover outliers (> 200%)
        ape_filtered = ape[ape <= 200]
        
        if len(ape_filtered) == 0:
            return 100.0
        
        return float(np.mean(ape_filtered))
    
    def _calculate_r_squared(self, actual: pd.Series, predicted: pd.Series) -> float:
        """Calcula R² (coeficiente de determinação)."""
        ss_res = np.sum((actual - predicted) ** 2)
        ss_tot = np.sum((actual - actual.mean()) ** 2)
        
        if ss_tot == 0:
            return 0.0
        
        r2 = 1 - (ss_res / ss_tot)
        return float(r2)
    
    def _calculate_coverage(
        self, 
        actual: pd.Series, 
        lower: pd.Series, 
        upper: pd.Series
    ) -> float:
        """
        Calcula a cobertura: % de valores reais dentro do intervalo de confiança.
        
        Ideal: ~95% para IC de 95%
        """
        within_bounds = (actual >= lower) & (actual <= upper)
        coverage = within_bounds.sum() / len(actual) * 100
        return float(coverage)
    
    def _compute_reliability_score_and_text(
        self,
        mape: float,
        r_squared: float,
        coverage: float,
        rmse: Optional[float] = None,
        mae: Optional[float] = None,
        scale: Optional[float] = None
    ) -> tuple[float, str]:
        """Calcula um score (0-100) e um texto de confiabilidade.
        
        Heurística:
        - Penalidade principal por MAPE (0–100) com peso 0.8
        - Penalidade por NRMSE e NMAE (normalizados por média do target), pesos 0.4 e 0.6
        - Penalidade por R²: (1-R²) * 20; se R² < 0 adiciona +10
        - Penalidade por Coverage: |95 - coverage| * 0.3 (se coverage>0)
        
        Retorna: (score, texto)
        """
        score = 100.0
        # MAPE
        score -= min(100.0, max(0.0, mape)) * 0.8
        
        # Normalização RMSE/MAE
        if scale and scale > 0:
            nrmse = (rmse / scale * 100.0) if (rmse is not None) else None
            nmae = (mae / scale * 100.0) if (mae is not None) else None
        else:
            nrmse = None
            nmae = None
        
        if nrmse is not None and np.isfinite(nrmse):
            score -= min(100.0, max(0.0, nrmse)) * 0.4
        if nmae is not None and np.isfinite(nmae):
            score -= min(100.0, max(0.0, nmae)) * 0.6
        
        # R²
        r2_clamped = float(r_squared)
        score -= max(0.0, (1.0 - max(0.0, r2_clamped))) * 20.0
        if r2_clamped < 0:
            score -= 10.0
        
        # Coverage (ideal ~95%)
        if coverage and coverage > 0:
            score -= abs(95.0 - coverage) * 0.3
        else:
            score -= 5.0  # leve penalidade quando não disponível
        
        score = max(0.0, min(100.0, score))
        
        # Mapear para texto
        if score >= 80:
            text = "Alta (85-95%)"
        elif score >= 65:
            text = "Boa (75-85%)"
        elif score >= 50:
            text = "Média (60-75%)"
        else:
            text = "Baixa (40-60%)"
        
        return float(score), text
    
    def _calculate_reliability_text(
        self, 
        mape: float, 
        r_squared: float, 
        coverage: float,
        rmse: Optional[float] = None,
        mae: Optional[float] = None,
        scale: Optional[float] = None
    ) -> str:
        """API compatível: retorna apenas o texto a partir do score composto."""
        _, text = self._compute_reliability_score_and_text(mape, r_squared, coverage, rmse, mae, scale)
        return text
    
    def backtest_model(
        self,
        model: Any,
        data: pd.DataFrame,
        date_col: str,
        target_col: str,
        horizon_days: int,
        min_train_days: int = 30
    ) -> ForecastMetrics:
        """
        Realiza backtesting completo de um modelo.
        
        Args:
            model: Modelo de forecast a validar
            data: DataFrame com dados históricos
            date_col: Nome da coluna de data
            target_col: Nome da coluna alvo
            horizon_days: Horizonte de previsão em dias
            min_train_days: Mínimo de dias para treino
            
        Returns:
            ForecastMetrics com resultados do backtesting
        """
        try:
            # Preparar dados
            df = data.copy()
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(date_col)
            
            # Verificar se há dados suficientes
            if len(df) < min_train_days + horizon_days:
                return ForecastMetrics(
                    mape=100.0,
                    rmse=float('inf'),
                    mae=float('inf'),
                    r_squared=-1.0,
                    coverage=0.0,
                    reliability_text="Dados Insuficientes",
                    sample_size=len(df)
                )
            
            # Split train/test
            split_idx = len(df) - horizon_days
            train = df.iloc[:split_idx]
            test = df.iloc[split_idx:]
            
            # Treinar modelo
            model.fit(train, date_col=date_col, target_col=target_col)
            
            # Gerar previsão
            forecast_df = model.predict(periods=horizon_days)
            
            if forecast_df.empty or len(forecast_df) < horizon_days:
                return ForecastMetrics(
                    mape=100.0,
                    rmse=float('inf'),
                    mae=float('inf'),
                    r_squared=-1.0,
                    coverage=0.0,
                    reliability_text="Previsão Falhou",
                    sample_size=0
                )
            
            # Alinhar dados test com forecast
            actual = test[target_col].reset_index(drop=True)
            predicted = forecast_df['forecast'].iloc[:len(actual)].reset_index(drop=True)
            
            # Extrair bounds se disponíveis
            lower_bound = None
            upper_bound = None
            if 'lower_bound' in forecast_df.columns and 'upper_bound' in forecast_df.columns:
                lower_bound = forecast_df['lower_bound'].iloc[:len(actual)].reset_index(drop=True)
                upper_bound = forecast_df['upper_bound'].iloc[:len(actual)].reset_index(drop=True)
            
            # Calcular métricas
            metrics = self.calculate_metrics(actual, predicted, lower_bound, upper_bound)
            
            return metrics
            
        except Exception as e:
            # Em caso de erro, retornar métricas default
            # Nota: erro silencioso para não poluir UI, mas logar no console
            print(f"⚠️ Erro no backtesting: {str(e)}")
            
            return ForecastMetrics(
                mape=100.0,
                rmse=float('inf'),
                mae=float('inf'),
                r_squared=-1.0,
                coverage=0.0,
                reliability_text=f"Erro: {str(e)[:30]}",
                sample_size=0
            )


class EnsembleMetricsAggregator:
    """Agregador de métricas para ensembles."""
    
    @staticmethod
    def aggregate_by_weights(
        metrics_list: list[ForecastMetrics],
        weights: list[float]
    ) -> ForecastMetrics:
        """
        Agrega métricas de múltiplos modelos usando pesos.
        
        Args:
            metrics_list: Lista de métricas de cada modelo
            weights: Lista de pesos (devem somar 1.0)
            
        Returns:
            ForecastMetrics agregado
        """
        # Normalizar pesos
        weights = np.array(weights)
        weights = weights / weights.sum()
        
        # Agregar métricas
        mape = sum(m.mape * w for m, w in zip(metrics_list, weights))
        rmse = np.sqrt(sum((m.rmse ** 2) * w for m, w in zip(metrics_list, weights)))
        mae = sum(m.mae * w for m, w in zip(metrics_list, weights))
        r_squared = sum(m.r_squared * w for m, w in zip(metrics_list, weights))
        coverage = sum(m.coverage * w for m, w in zip(metrics_list, weights))
        
        # Validador para calcular texto/score de confiabilidade (usa RMSE/MAE agregados)
        validator = ForecastValidator()
        reliability_score, reliability_text = validator._compute_reliability_score_and_text(
            mape=mape,
            r_squared=r_squared,
            coverage=coverage,
            rmse=rmse,
            mae=mae,
            scale=None  # sem escala global confiável aqui
        )
        
        total_samples = sum(m.sample_size for m in metrics_list)
        
        return ForecastMetrics(
            mape=mape,
            rmse=rmse,
            mae=mae,
            r_squared=r_squared,
            coverage=coverage,
            reliability_text=reliability_text,
            sample_size=total_samples,
            reliability_score=reliability_score
        )
    
    @staticmethod
    def propagate_confidence_intervals(
        forecasts: list[pd.DataFrame],
        aggregation_method: str = 'weighted'
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Propaga intervalos de confiança de múltiplos forecasts.
        
        Args:
            forecasts: Lista de DataFrames com forecast, lower_bound, upper_bound
            aggregation_method: 'weighted', 'min_max', ou 'variance'
            
        Returns:
            (lower_bound_aggregated, upper_bound_aggregated)
        """
        if aggregation_method == 'weighted':
            # Média ponderada dos bounds
            n = len(forecasts)
            weights = np.ones(n) / n
            
            lower_bounds = [df['lower_bound'] for df in forecasts]
            upper_bounds = [df['upper_bound'] for df in forecasts]
            
            lower_agg = sum(lb * w for lb, w in zip(lower_bounds, weights))
            upper_agg = sum(ub * w for ub, w in zip(upper_bounds, weights))
            
        elif aggregation_method == 'min_max':
            # Min dos lower, max dos upper (mais conservador)
            lower_agg = pd.concat([df['lower_bound'] for df in forecasts], axis=1).min(axis=1)
            upper_agg = pd.concat([df['upper_bound'] for df in forecasts], axis=1).max(axis=1)
            
        elif aggregation_method == 'variance':
            # Propagação por variância (mais rigoroso)
            # σ_total = √(Σσᵢ²)
            stds = []
            for df in forecasts:
                # Estimar σ a partir do IC: IC = ±1.96σ
                std = (df['upper_bound'] - df['lower_bound']) / (2 * 1.96)
                stds.append(std ** 2)
            
            total_std = np.sqrt(sum(stds))
            mean_forecast = pd.concat([df['forecast'] for df in forecasts], axis=1).mean(axis=1)
            
            lower_agg = mean_forecast - (1.96 * total_std)
            upper_agg = mean_forecast + (1.96 * total_std)
            
        else:
            raise ValueError(f"Método desconhecido: {aggregation_method}")
        
        # Garantir lower >= 0
        lower_agg = lower_agg.clip(lower=0)
        
        return lower_agg, upper_agg

