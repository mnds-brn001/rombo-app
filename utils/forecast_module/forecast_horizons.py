"""
Forecast Horizons (minimal)
==========================

Este módulo foi reduzido para manter o sistema de previsão de receita enxuto.
Ele oferece apenas:
- Enum `ForecastHorizon` com metadados (7/14/21/30 dias)
- `get_industry_context()` para benchmarks simples usados na UI
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict


class ForecastHorizon(Enum):
    """Horizontes suportados pela UI."""
    
    SHORT = {
        "days": 7,
        "mape_threshold": 20,
        "confidence": 0.85,
        "use_case": "Planejamento semanal",
        "reliability": "Boa (75-85%)",
        "roi": "⭐⭐⭐⭐",
    }
    MEDIUM = {
        "days": 14,
        "mape_threshold": 30,
        "confidence": 0.75,
        "use_case": "Planejamento quinzenal",
        "reliability": "Média (60-75%)",
        "roi": "⭐⭐⭐",
    }
    LONG = {
        "days": 21,
        "mape_threshold": 45,
        "confidence": 0.65,
        "use_case": "Tendências direcionais",
        "reliability": "Baixa (40-60%)",
        "roi": "⭐⭐",
    }
    EXTENDED = {
        "days": 30,
        "mape_threshold": 55,
        "confidence": 0.55,
        "use_case": "Planejamento estendido",
        "reliability": "Baixa (40-60%)",
        "roi": "⭐",
    }


def get_industry_context(industry: str = "ecommerce") -> Dict[str, Any]:
    """Benchmarks simples por indústria (usado na sidebar)."""
    industry_map = {
        "ecommerce": {
            "max_recommended_days": 14,
            "target_mape": 20,
            "notes": "Rolling forecasts e revisão frequente para períodos longos.",
        }
    }
    return industry_map.get(industry, industry_map["ecommerce"])

