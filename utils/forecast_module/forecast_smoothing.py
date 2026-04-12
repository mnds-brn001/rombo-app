"""
Sistema de Suavização de Forecast
=================================

Implementa suavização entre blocos de forecast de diferentes modelos
para evitar degraus e descontinuidades visuais.

Métodos implementados:
- Linear blending (transição linear entre blocos)
- Spline smoothing (suavização via spline cúbico)
- Moving average (média móvel entre transições)
- Exponential smoothing (suavização exponencial)
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Optional
from scipy import interpolate


class ForecastSmoother:
    """Suavizador de transições entre blocos de forecast."""
    
    def __init__(self, transition_days: int = 3):
        """
        Args:
            transition_days: Número de dias para transição entre modelos
        """
        self.transition_days = transition_days
    
    def smooth_ensemble_blocks(
        self,
        forecast_blocks: List[pd.DataFrame],
        method: str = 'linear'
    ) -> pd.DataFrame:
        """
        Suaviza transições entre blocos de forecast de diferentes modelos.
        
        Args:
            forecast_blocks: Lista de DataFrames com forecast, lower_bound, upper_bound
            method: 'linear', 'spline', 'moving_average', ou 'exponential'
            
        Returns:
            DataFrame com forecast suavizado
        """
        if len(forecast_blocks) == 1:
            # Apenas um bloco, sem necessidade de suavização
            return forecast_blocks[0]
        
        if method == 'linear':
            return self._linear_blending(forecast_blocks)
        elif method == 'spline':
            return self._spline_smoothing(forecast_blocks)
        elif method == 'moving_average':
            return self._moving_average_smoothing(forecast_blocks)
        elif method == 'exponential':
            return self._exponential_smoothing(forecast_blocks)
        else:
            raise ValueError(f"Método desconhecido: {method}")
    
    def _linear_blending(self, forecast_blocks: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Suavização linear entre blocos.
        
        Nos últimos N dias do bloco anterior e primeiros N dias do próximo,
        faz uma transição linear entre as previsões.
        """
        smoothed_blocks = []
        
        for i, block in enumerate(forecast_blocks):
            if i == 0:
                # Primeiro bloco - não suavizar início
                smoothed_blocks.append(block)
            else:
                # Suavizar transição com bloco anterior
                prev_block = smoothed_blocks[-1]
                
                # Extrair overlap
                overlap_days = min(self.transition_days, len(prev_block), len(block))
                
                if overlap_days > 0:
                    # Últimos dias do bloco anterior
                    prev_tail = prev_block.iloc[-overlap_days:].copy()
                    # Primeiros dias do bloco atual
                    curr_head = block.iloc[:overlap_days].copy()
                    
                    # Aplicar blending linear
                    for j in range(overlap_days):
                        weight_curr = (j + 1) / (overlap_days + 1)
                        weight_prev = 1 - weight_curr
                        
                        # Blend forecast
                        smoothed_blocks[-1].iloc[-overlap_days + j, smoothed_blocks[-1].columns.get_loc('forecast')] = (
                            prev_tail.iloc[j]['forecast'] * weight_prev +
                            curr_head.iloc[j]['forecast'] * weight_curr
                        )
                        
                        # Blend bounds
                        if 'lower_bound' in prev_tail.columns:
                            smoothed_blocks[-1].iloc[-overlap_days + j, smoothed_blocks[-1].columns.get_loc('lower_bound')] = (
                                prev_tail.iloc[j]['lower_bound'] * weight_prev +
                                curr_head.iloc[j]['lower_bound'] * weight_curr
                            )
                        
                        if 'upper_bound' in prev_tail.columns:
                            smoothed_blocks[-1].iloc[-overlap_days + j, smoothed_blocks[-1].columns.get_loc('upper_bound')] = (
                                prev_tail.iloc[j]['upper_bound'] * weight_prev +
                                curr_head.iloc[j]['upper_bound'] * weight_curr
                            )
                    
                    # Adicionar resto do bloco atual (exceto overlap)
                    if len(block) > overlap_days:
                        smoothed_blocks.append(block.iloc[overlap_days:])
                else:
                    smoothed_blocks.append(block)
        
        # Concatenar todos os blocos suavizados
        return pd.concat(smoothed_blocks, ignore_index=True)
    
    def _spline_smoothing(self, forecast_blocks: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Suavização via spline cúbico.
        
        Ajusta um spline cúbico sobre toda a série de forecast,
        mantendo os pontos médios dos blocos fixos.
        """
        # Concatenar todos os blocos
        combined = pd.concat(forecast_blocks, ignore_index=True)
        
        # Criar índice temporal
        x = np.arange(len(combined))
        y = combined['forecast'].values
        
        # Identificar pontos de transição (onde os blocos se encontram)
        transition_points = []
        current_idx = 0
        for block in forecast_blocks[:-1]:
            current_idx += len(block)
            transition_points.append(current_idx)
        
        # Criar janela de suavização ao redor de cada transição
        smooth_mask = np.zeros(len(combined), dtype=bool)
        for tp in transition_points:
            start = max(0, tp - self.transition_days)
            end = min(len(combined), tp + self.transition_days)
            smooth_mask[start:end] = True
        
        # Aplicar spline apenas nas regiões de transição
        if smooth_mask.sum() > 3:  # Mínimo para spline cúbico
            # Pontos fixos (não suavizar)
            fixed_x = x[~smooth_mask]
            fixed_y = y[~smooth_mask]
            
            # Pontos a suavizar
            smooth_x = x[smooth_mask]
            
            # Interpolar com spline cúbico
            if len(fixed_x) >= 4:
                spline = interpolate.UnivariateSpline(fixed_x, fixed_y, s=0, k=3)
                y[smooth_mask] = spline(smooth_x)
            else:
                # Fallback para interpolação linear
                y[smooth_mask] = np.interp(smooth_x, fixed_x, fixed_y)
        
        # Atualizar forecast suavizado
        combined['forecast'] = y
        
        # Reajustar bounds proporcionalmente
        if 'lower_bound' in combined.columns and 'upper_bound' in combined.columns:
            original_range = combined['upper_bound'] - combined['lower_bound']
            combined['lower_bound'] = combined['forecast'] - (original_range / 2)
            combined['upper_bound'] = combined['forecast'] + (original_range / 2)
            combined['lower_bound'] = combined['lower_bound'].clip(lower=0)
        
        return combined
    
    def _moving_average_smoothing(self, forecast_blocks: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Suavização via média móvel.
        
        Aplica média móvel nas regiões de transição entre blocos.
        """
        combined = pd.concat(forecast_blocks, ignore_index=True)
        
        # Identificar pontos de transição
        transition_points = []
        current_idx = 0
        for block in forecast_blocks[:-1]:
            current_idx += len(block)
            transition_points.append(current_idx)
        
        # Aplicar média móvel ao redor de cada transição
        window = self.transition_days * 2 + 1
        for tp in transition_points:
            start = max(0, tp - self.transition_days)
            end = min(len(combined), tp + self.transition_days + 1)
            
            if end - start >= 3:
                # Aplicar média móvel
                combined.loc[start:end-1, 'forecast'] = (
                    combined.loc[start:end-1, 'forecast']
                    .rolling(window=min(window, end-start), center=True, min_periods=1)
                    .mean()
                )
        
        # Reajustar bounds
        if 'lower_bound' in combined.columns and 'upper_bound' in combined.columns:
            original_range = combined['upper_bound'] - combined['lower_bound']
            combined['lower_bound'] = combined['forecast'] - (original_range / 2)
            combined['upper_bound'] = combined['forecast'] + (original_range / 2)
            combined['lower_bound'] = combined['lower_bound'].clip(lower=0)
        
        return combined
    
    def _exponential_smoothing(self, forecast_blocks: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Suavização exponencial.
        
        Aplica exponential smoothing sobre toda a série de forecast.
        """
        combined = pd.concat(forecast_blocks, ignore_index=True)
        
        # Aplicar exponential smoothing
        alpha = 2 / (self.transition_days + 1)  # Fator de suavização
        smoothed = combined['forecast'].ewm(alpha=alpha, adjust=False).mean()
        combined['forecast'] = smoothed
        
        # Reajustar bounds
        if 'lower_bound' in combined.columns and 'upper_bound' in combined.columns:
            original_range = combined['upper_bound'] - combined['lower_bound']
            combined['lower_bound'] = combined['forecast'] - (original_range / 2)
            combined['upper_bound'] = combined['forecast'] + (original_range / 2)
            combined['lower_bound'] = combined['lower_bound'].clip(lower=0)
        
        return combined
    
    def detect_discontinuities(
        self,
        forecast_df: pd.DataFrame,
        threshold: float = 0.2
    ) -> List[Tuple[int, float]]:
        """
        Detecta descontinuidades (degraus) no forecast.
        
        Args:
            forecast_df: DataFrame com forecast
            threshold: Limiar de mudança relativa para considerar descontinuidade (20% default)
            
        Returns:
            Lista de (índice, magnitude_mudança)
        """
        discontinuities = []
        
        forecast_values = forecast_df['forecast'].values
        
        for i in range(1, len(forecast_values)):
            if forecast_values[i-1] != 0:
                relative_change = abs(forecast_values[i] - forecast_values[i-1]) / forecast_values[i-1]
                
                if relative_change > threshold:
                    discontinuities.append((i, relative_change))
        
        return discontinuities
    
    def smooth_confidence_intervals(
        self,
        forecast_df: pd.DataFrame,
        method: str = 'variance'
    ) -> pd.DataFrame:
        """
        Suaviza intervalos de confiança ao longo do tempo.
        
        Intervalos tendem a crescer com o horizonte - esta função
        garante uma progressão suave.
        
        Args:
            forecast_df: DataFrame com lower_bound, upper_bound
            method: 'variance' (propagação correta) ou 'linear' (crescimento linear)
            
        Returns:
            DataFrame com bounds suavizados
        """
        if 'lower_bound' not in forecast_df.columns or 'upper_bound' not in forecast_df.columns:
            return forecast_df
        
        df = forecast_df.copy()
        
        if method == 'variance':
            # Calcular std de cada ponto
            std = (df['upper_bound'] - df['lower_bound']) / (2 * 1.96)
            
            # Suavizar std com raiz do tempo (teoria estatística)
            days = np.arange(1, len(df) + 1)
            std_smoothed = std.iloc[0] * np.sqrt(days)
            
            # Recalcular bounds
            df['lower_bound'] = df['forecast'] - (1.96 * std_smoothed)
            df['upper_bound'] = df['forecast'] + (1.96 * std_smoothed)
            
        elif method == 'linear':
            # Crescimento linear dos bounds
            initial_width = df.iloc[0]['upper_bound'] - df.iloc[0]['lower_bound']
            final_width = df.iloc[-1]['upper_bound'] - df.iloc[-1]['lower_bound']
            
            widths = np.linspace(initial_width, final_width, len(df))
            
            df['lower_bound'] = df['forecast'] - (widths / 2)
            df['upper_bound'] = df['forecast'] + (widths / 2)
        
        # Garantir lower >= 0
        df['lower_bound'] = df['lower_bound'].clip(lower=0)
        
        return df


class EnsembleBlender:
    """Blender para combinar forecasts de múltiplos modelos."""
    
    @staticmethod
    def blend_forecasts(
        forecasts: List[pd.DataFrame],
        weights: Optional[List[float]] = None,
        method: str = 'weighted_average'
    ) -> pd.DataFrame:
        """
        Combina múltiplos forecasts em um único forecast.
        
        Args:
            forecasts: Lista de DataFrames com forecast, lower_bound, upper_bound
            weights: Pesos de cada forecast (None = pesos iguais)
            method: 'weighted_average', 'median', ou 'best'
            
        Returns:
            DataFrame com forecast combinado
        """
        if not forecasts:
            return pd.DataFrame()
        
        if len(forecasts) == 1:
            return forecasts[0]
        
        # Normalizar pesos
        if weights is None:
            weights = [1.0 / len(forecasts)] * len(forecasts)
        else:
            weights = np.array(weights)
            weights = weights / weights.sum()
        
        if method == 'weighted_average':
            return EnsembleBlender._weighted_average(forecasts, weights)
        elif method == 'median':
            return EnsembleBlender._median_blend(forecasts)
        elif method == 'best':
            return EnsembleBlender._best_forecast(forecasts, weights)
        else:
            raise ValueError(f"Método desconhecido: {method}")
    
    @staticmethod
    def _weighted_average(forecasts: List[pd.DataFrame], weights: np.ndarray) -> pd.DataFrame:
        """Média ponderada dos forecasts."""
        result = forecasts[0].copy()
        
        # Combinar forecast
        forecast_values = np.array([df['forecast'].values for df in forecasts])
        result['forecast'] = np.average(forecast_values, axis=0, weights=weights)
        
        # Combinar bounds
        if 'lower_bound' in result.columns:
            lower_values = np.array([df['lower_bound'].values for df in forecasts])
            result['lower_bound'] = np.average(lower_values, axis=0, weights=weights)
        
        if 'upper_bound' in result.columns:
            upper_values = np.array([df['upper_bound'].values for df in forecasts])
            result['upper_bound'] = np.average(upper_values, axis=0, weights=weights)
        
        result['lower_bound'] = result['lower_bound'].clip(lower=0)
        
        return result
    
    @staticmethod
    def _median_blend(forecasts: List[pd.DataFrame]) -> pd.DataFrame:
        """Mediana dos forecasts (mais robusto a outliers)."""
        result = forecasts[0].copy()
        
        # Mediana do forecast
        forecast_values = np.array([df['forecast'].values for df in forecasts])
        result['forecast'] = np.median(forecast_values, axis=0)
        
        # Mediana dos bounds
        if 'lower_bound' in result.columns:
            lower_values = np.array([df['lower_bound'].values for df in forecasts])
            result['lower_bound'] = np.median(lower_values, axis=0)
        
        if 'upper_bound' in result.columns:
            upper_values = np.array([df['upper_bound'].values for df in forecasts])
            result['upper_bound'] = np.median(upper_values, axis=0)
        
        result['lower_bound'] = result['lower_bound'].clip(lower=0)
        
        return result
    
    @staticmethod
    def _best_forecast(forecasts: List[pd.DataFrame], weights: np.ndarray) -> pd.DataFrame:
        """Seleciona o melhor forecast baseado nos pesos."""
        best_idx = np.argmax(weights)
        return forecasts[best_idx].copy()


