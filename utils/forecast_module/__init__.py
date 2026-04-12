"""
Forecast Module Package
=====================

Centralized forecasting capabilities for the dashboard.
"""

# Revenue forecasting (ML) - lean specialized module
try:  # pragma: no cover
    from .revenue_forecast_ml import (
        prepare_daily_revenue_series,
        SeasonalNaiveBaseline,
        LightGBMRevenueForecast,
        XGBoostRevenueForecast,
        RevenueEnsemble,
        BacktestResult,
        is_tensorflow_available,
    )
except Exception as e:  # pragma: no cover
    print(f"Warning: Could not import revenue forecast ML module: {e}")
    prepare_daily_revenue_series = None
    SeasonalNaiveBaseline = None
    LightGBMRevenueForecast = None
    XGBoostRevenueForecast = None
    RevenueEnsemble = None
    BacktestResult = None
    is_tensorflow_available = None

# SOTA Revenue Forecast
try:
    from .revenue_forecast_sota import orchestrate_sota_forecast
except Exception as e:
    print(f"Warning: Could not import SOTA forecast module: {e}")
    orchestrate_sota_forecast = None

# Horizon management
from .forecast_horizons import (
    ForecastHorizon,
    get_industry_context
)

# ML ensemble (opcional): importe apenas quando necessário em módulos que o utilizem
try:  # Carregar de forma preguiçosa para evitar dependências desnecessárias
    from .ml_ensemble_forecast import MLStockRecommendationSystem, MLEnsembleForecast
except Exception as e:  # pragma: no cover
    print(f"Warning: Could not import ML ensemble: {e}")
    MLStockRecommendationSystem = None
    MLEnsembleForecast = None

__all__ = [
    # Revenue ML module (optional)
    'prepare_daily_revenue_series',
    'SeasonalNaiveBaseline',
    'LightGBMRevenueForecast',
    'XGBoostRevenueForecast',
    'RevenueEnsemble',
    'BacktestResult',
    'is_tensorflow_available',
    'orchestrate_sota_forecast',
    'ForecastHorizon',
    'get_industry_context'
]
