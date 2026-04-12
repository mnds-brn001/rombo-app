"""
Sistema de Validação Robusta para Dados de Clientes Reais
========================================================

Valida dados de entrada para forecasting, detectando problemas comuns
e fornecendo recomendações para melhorar a qualidade das previsões.

Características:
- Validação de volume mínimo de dados
- Detecção de gaps temporais
- Análise de sazonalidade
- Detecção de outliers
- Validação de tendências
- Recomendações de configuração
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

class ValidationLevel(Enum):
    """Níveis de validação disponíveis."""
    BASIC = "basic"           # Validação mínima
    STANDARD = "standard"     # Validação padrão
    STRICT = "strict"         # Validação rigorosa
    CUSTOM = "custom"         # Validação customizada

@dataclass
class ValidationResult:
    """Resultado da validação de dados."""
    is_valid: bool
    level: ValidationLevel
    issues: List[str]
    warnings: List[str]
    recommendations: List[str]
    data_quality_score: float  # 0-100
    min_forecast_days: int
    max_forecast_days: int
    suggested_models: List[str]
    confidence_level: str  # "high", "medium", "low"

class DataValidator:
    """
    Validador robusto para dados de forecasting de clientes reais.
    """
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STANDARD):
        self.validation_level = validation_level
        self.config = self._load_validation_config()
    
    def _load_validation_config(self) -> Dict[str, Any]:
        """Carrega configuração de validação baseada no nível."""
        configs = {
            ValidationLevel.BASIC: {
                'min_days': 7,
                'max_gap_days': 30,
                'min_orders_per_day': 1,
                'outlier_threshold': 5.0,
                'seasonality_min_periods': 7
            },
            ValidationLevel.STANDARD: {
                'min_days': 30,
                'max_gap_days': 14,
                'min_orders_per_day': 2,
                'outlier_threshold': 3.0,
                'seasonality_min_periods': 14
            },
            ValidationLevel.STRICT: {
                'min_days': 60,
                'max_gap_days': 7,
                'min_orders_per_day': 5,
                'outlier_threshold': 2.0,
                'seasonality_min_periods': 21
            }
        }
        return configs.get(self.validation_level, configs[ValidationLevel.STANDARD])
    
    def validate_for_forecasting(self, df: pd.DataFrame, 
                               date_col: str = 'order_purchase_timestamp',
                               target_col: str = 'price',
                               category_col: str = 'product_category_name') -> ValidationResult:
        """
        Valida dados para forecasting de forma abrangente.
        
        Args:
            df: DataFrame com dados do cliente
            date_col: Nome da coluna de data
            target_col: Nome da coluna de valor
            category_col: Nome da coluna de categoria
            
        Returns:
            ValidationResult com análise completa
        """
        issues = []
        warnings = []
        recommendations = []
        
        # 1. Validação básica de estrutura
        basic_validation = self._validate_basic_structure(df, date_col, target_col, category_col)
        issues.extend(basic_validation['issues'])
        warnings.extend(basic_validation['warnings'])
        
        if not basic_validation['is_valid']:
            return ValidationResult(
                is_valid=False,
                level=self.validation_level,
                issues=issues,
                warnings=warnings,
                recommendations=["Corrija os problemas estruturais antes de prosseguir"],
                data_quality_score=0.0,
                min_forecast_days=0,
                max_forecast_days=0,
                suggested_models=[],
                confidence_level="low"
            )
        
        # 2. Preparar dados para análise
        daily_data = self._prepare_daily_data(df, date_col, target_col)
        
        # 3. Validação de volume
        volume_validation = self._validate_volume(daily_data, target_col)
        issues.extend(volume_validation['issues'])
        warnings.extend(volume_validation['warnings'])
        recommendations.extend(volume_validation['recommendations'])
        
        # 4. Validação temporal
        temporal_validation = self._validate_temporal_continuity(daily_data, target_col)
        issues.extend(temporal_validation['issues'])
        warnings.extend(temporal_validation['warnings'])
        recommendations.extend(temporal_validation['recommendations'])
        
        # 5. Validação de qualidade
        quality_validation = self._validate_data_quality(daily_data, target_col)
        issues.extend(quality_validation['issues'])
        warnings.extend(quality_validation['warnings'])
        recommendations.extend(quality_validation['recommendations'])
        
        # 6. Análise de sazonalidade
        seasonality_analysis = self._analyze_seasonality(daily_data, target_col)
        recommendations.extend(seasonality_analysis['recommendations'])
        
        # 7. Análise de tendências
        trend_analysis = self._analyze_trends(daily_data)
        recommendations.extend(trend_analysis['recommendations'])
        
        # 8. Calcular score de qualidade
        quality_score = self._calculate_quality_score(
            volume_validation, temporal_validation, quality_validation
        )
        
        # 9. Determinar capacidades de forecasting
        forecast_capabilities = self._determine_forecast_capabilities(
            daily_data, quality_score, issues
        )
        
        # 10. Sugerir modelos
        suggested_models = self._suggest_models(
            daily_data, quality_score, seasonality_analysis, trend_analysis
        )
        
        # 11. Determinar nível de confiança
        confidence_level = self._determine_confidence_level(quality_score, len(issues))
        
        is_valid = len(issues) == 0 and quality_score >= 50
        
        return ValidationResult(
            is_valid=is_valid,
            level=self.validation_level,
            issues=issues,
            warnings=warnings,
            recommendations=recommendations,
            data_quality_score=quality_score,
            min_forecast_days=forecast_capabilities['min_days'],
            max_forecast_days=forecast_capabilities['max_days'],
            suggested_models=suggested_models,
            confidence_level=confidence_level
        )
    
    def _validate_basic_structure(self, df: pd.DataFrame, date_col: str, 
                                 target_col: str, category_col: str) -> Dict[str, Any]:
        """Validação básica de estrutura dos dados."""
        issues = []
        warnings = []
        
        # Verificar se DataFrame não está vazio
        if df.empty:
            issues.append("DataFrame está vazio")
            return {'is_valid': False, 'issues': issues, 'warnings': warnings}
        
        # Verificar colunas obrigatórias
        required_cols = [date_col, target_col, category_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Colunas obrigatórias ausentes: {missing_cols}")
            return {'is_valid': False, 'issues': issues, 'warnings': warnings}
        
        # Verificar tipos de dados
        try:
            pd.to_datetime(df[date_col])
        except:
            issues.append(f"Coluna {date_col} não contém datas válidas")
        
        if not pd.api.types.is_numeric_dtype(df[target_col]):
            issues.append(f"Coluna {target_col} não é numérica")
        
        # Verificar valores nulos
        null_counts = df[required_cols].isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                warnings.append(f"Coluna {col} tem {count} valores nulos")
        
        return {'is_valid': len(issues) == 0, 'issues': issues, 'warnings': warnings}
    
    def _prepare_daily_data(self, df: pd.DataFrame, date_col: str, target_col: str) -> pd.DataFrame:
        """Prepara dados agregados por dia."""
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy[date_col]).dt.date
        daily_data = df_copy.groupby('date')[target_col].sum().reset_index()
        daily_data.columns = ['date', target_col]
        daily_data['date'] = pd.to_datetime(daily_data['date'])
        daily_data = daily_data.sort_values('date').reset_index(drop=True)
        return daily_data
    
    def _validate_volume(self, daily_data: pd.DataFrame, target_col: str = 'price') -> Dict[str, Any]:
        """Valida volume de dados."""
        issues = []
        warnings = []
        recommendations = []
        
        min_days = self.config['min_days']
        min_orders_per_day = self.config['min_orders_per_day']
        
        # Verificar número mínimo de dias
        if len(daily_data) < min_days:
            issues.append(f"Dados insuficientes: {len(daily_data)} dias (mínimo: {min_days})")
            return {'issues': issues, 'warnings': warnings, 'recommendations': recommendations}
        
        # Verificar volume médio por dia
        avg_daily_revenue = daily_data[target_col].mean()
        if avg_daily_revenue < min_orders_per_day:
            warnings.append(f"Volume baixo: {avg_daily_revenue:.1f} por dia (recomendado: {min_orders_per_day}+)")
            recommendations.append("Considere agregar dados de períodos mais longos")
        
        # Verificar consistência do volume
        revenue_std = daily_data[target_col].std()
        revenue_cv = revenue_std / avg_daily_revenue if avg_daily_revenue > 0 else float('inf')
        
        if revenue_cv > 2.0:
            warnings.append(f"Alta variabilidade no volume (CV: {revenue_cv:.2f})")
            recommendations.append("Considere usar modelos mais robustos (XGBoost, LightGBM)")
        
        return {'issues': issues, 'warnings': warnings, 'recommendations': recommendations}
    
    def _validate_temporal_continuity(self, daily_data: pd.DataFrame, target_col: str = 'price') -> Dict[str, Any]:
        """Valida continuidade temporal dos dados."""
        issues = []
        warnings = []
        recommendations = []
        
        max_gap_days = self.config['max_gap_days']
        
        # Calcular gaps entre datas
        daily_data['date_diff'] = daily_data['date'].diff().dt.days
        gaps = daily_data['date_diff'].dropna()
        
        # Verificar gaps grandes
        large_gaps = gaps[gaps > max_gap_days]
        if len(large_gaps) > 0:
            warnings.append(f"Encontrados {len(large_gaps)} gaps maiores que {max_gap_days} dias")
            recommendations.append("Considere preencher gaps com interpolação ou usar dados mais recentes")
        
        # Verificar sazonalidade semanal
        daily_data['day_of_week'] = daily_data['date'].dt.dayofweek
        weekly_pattern = daily_data.groupby('day_of_week')[target_col].mean()
        
        # Verificar se há padrão semanal consistente
        if weekly_pattern.std() / weekly_pattern.mean() < 0.1:
            warnings.append("Padrão semanal muito fraco - dados podem não ter sazonalidade")
            recommendations.append("Considere usar modelos mais simples (Baseline, ARIMA)")
        
        return {'issues': issues, 'warnings': warnings, 'recommendations': recommendations}
    
    def _validate_data_quality(self, daily_data: pd.DataFrame, target_col: str = 'price') -> Dict[str, Any]:
        """Valida qualidade dos dados."""
        issues = []
        warnings = []
        recommendations = []
        
        outlier_threshold = self.config['outlier_threshold']
        
        # Detectar outliers usando IQR
        Q1 = daily_data[target_col].quantile(0.25)
        Q3 = daily_data[target_col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - outlier_threshold * IQR
        upper_bound = Q3 + outlier_threshold * IQR
        
        outliers = daily_data[(daily_data[target_col] < lower_bound) | 
                             (daily_data[target_col] > upper_bound)]
        
        if len(outliers) > 0:
            outlier_pct = len(outliers) / len(daily_data) * 100
            if outlier_pct > 10:
                issues.append(f"Muitos outliers detectados: {outlier_pct:.1f}% dos dados")
                recommendations.append("Considere limpar outliers ou usar modelos robustos")
            else:
                warnings.append(f"Outliers detectados: {outlier_pct:.1f}% dos dados")
                recommendations.append("Monitore outliers - podem indicar eventos especiais")
        
        # Verificar valores zero ou negativos
        zero_negative = daily_data[daily_data[target_col] <= 0]
        if len(zero_negative) > 0:
            warnings.append(f"Encontrados {len(zero_negative)} dias com receita zero/negativa")
            recommendations.append("Considere tratar valores zero/negativos antes do forecasting")
        
        return {'issues': issues, 'warnings': warnings, 'recommendations': recommendations}
    
    def _analyze_seasonality(self, daily_data: pd.DataFrame, target_col: str = 'price') -> Dict[str, Any]:
        """Analisa padrões sazonais nos dados."""
        recommendations = []
        
        # Análise de sazonalidade semanal
        daily_data['day_of_week'] = daily_data['date'].dt.dayofweek
        weekly_pattern = daily_data.groupby('day_of_week')[target_col].mean()
        
        # Calcular força da sazonalidade semanal
        weekly_cv = weekly_pattern.std() / weekly_pattern.mean()
        
        if weekly_cv > 0.3:
            recommendations.append("Sazonalidade semanal forte detectada - use Prophet ou modelos com sazonalidade")
        elif weekly_cv > 0.1:
            recommendations.append("Sazonalidade semanal moderada - ARIMA pode ser adequado")
        else:
            recommendations.append("Sazonalidade semanal fraca - modelos simples podem ser suficientes")
        
        # Análise de sazonalidade mensal (se dados suficientes)
        if len(daily_data) >= 60:
            daily_data['month'] = daily_data['date'].dt.month
            monthly_pattern = daily_data.groupby('month')[target_col].mean()
            monthly_cv = monthly_pattern.std() / monthly_pattern.mean()
            
            if monthly_cv > 0.2:
                recommendations.append("Sazonalidade mensal detectada - considere Prophet com sazonalidade anual")
        
        return {'recommendations': recommendations}
    
    def _analyze_trends(self, daily_data: pd.DataFrame) -> Dict[str, Any]:
        """Analisa tendências nos dados."""
        recommendations = []
        
        # Calcular tendência linear
        x = np.arange(len(daily_data))
        y = daily_data['revenue'].values
        slope, intercept = np.polyfit(x, y, 1)
        
        # Calcular força da tendência
        trend_strength = abs(slope) / daily_data['revenue'].mean()
        
        if trend_strength > 0.01:  # 1% de crescimento/declínio por dia
            if slope > 0:
                recommendations.append("Tendência de crescimento forte detectada - use modelos que capturam tendências")
            else:
                recommendations.append("Tendência de declínio detectada - monitore de perto")
        else:
            recommendations.append("Tendência estável - modelos de média móvel podem ser adequados")
        
        return {'recommendations': recommendations}
    
    def _calculate_quality_score(self, volume_validation: Dict, temporal_validation: Dict, 
                               quality_validation: Dict) -> float:
        """Calcula score de qualidade dos dados (0-100)."""
        score = 100.0
        
        # Penalizar por issues
        score -= len(volume_validation['issues']) * 20
        score -= len(temporal_validation['issues']) * 15
        score -= len(quality_validation['issues']) * 10
        
        # Penalizar por warnings
        score -= len(volume_validation['warnings']) * 5
        score -= len(temporal_validation['warnings']) * 3
        score -= len(quality_validation['warnings']) * 2
        
        return max(0.0, min(100.0, score))
    
    def _determine_forecast_capabilities(self, daily_data: pd.DataFrame, 
                                       quality_score: float, issues: List[str]) -> Dict[str, int]:
        """Determina capacidades de forecasting baseadas na qualidade dos dados."""
        base_days = len(daily_data)
        
        if quality_score >= 80 and len(issues) == 0:
            # Dados excelentes - pode prever até 30 dias
            return {'min_days': 7, 'max_days': min(30, base_days // 2)}
        elif quality_score >= 60:
            # Dados bons - pode prever até 21 dias
            return {'min_days': 7, 'max_days': min(21, base_days // 3)}
        elif quality_score >= 40:
            # Dados moderados - pode prever até 14 dias
            return {'min_days': 7, 'max_days': min(14, base_days // 4)}
        else:
            # Dados ruins - apenas 7 dias
            return {'min_days': 7, 'max_days': 7}
    
    def _suggest_models(self, daily_data: pd.DataFrame, quality_score: float,
                       seasonality_analysis: Dict, trend_analysis: Dict) -> List[str]:
        """Sugere modelos baseados na análise dos dados."""
        models = []
        
        # Sempre incluir Baseline
        models.append('baseline')
        
        # Adicionar ARIMA para dados com tendências
        if 'tendência' in ' '.join(trend_analysis['recommendations']).lower():
            models.append('arima')
        
        # Adicionar Prophet para sazonalidade
        if 'sazonalidade' in ' '.join(seasonality_analysis['recommendations']).lower():
            models.append('prophet')
        
        # Adicionar modelos ML para dados complexos
        if quality_score >= 70 and len(daily_data) >= 60:
            models.extend(['xgboost', 'lightgbm'])
        
        # Adicionar LSTM para dados muito complexos
        if quality_score >= 80 and len(daily_data) >= 90:
            models.append('lstm')
        
        return list(set(models))  # Remove duplicatas
    
    def _determine_confidence_level(self, quality_score: float, issue_count: int) -> str:
        """Determina nível de confiança baseado na qualidade dos dados."""
        if quality_score >= 80 and issue_count == 0:
            return "high"
        elif quality_score >= 60 and issue_count <= 2:
            return "medium"
        else:
            return "low"

def validate_client_data(df: pd.DataFrame, client_config: Optional[Dict] = None) -> ValidationResult:
    """
    Função de conveniência para validar dados de cliente.
    
    Args:
        df: DataFrame com dados do cliente
        client_config: Configuração específica do cliente (opcional)
        
    Returns:
        ValidationResult com análise completa
    """
    # Determinar nível de validação baseado na configuração do cliente
    validation_level = ValidationLevel.STANDARD
    if client_config and 'validation_level' in client_config:
        validation_level = ValidationLevel(client_config['validation_level'])
    
    validator = DataValidator(validation_level)
    return validator.validate_for_forecasting(df)

