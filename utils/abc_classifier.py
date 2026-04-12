"""
Classificador ABC (Curva ABC) por performance ou receita.

- Performance: score combinando giro (quando disponível) e composite_score;
  depois ordena por esse score e aplica thresholds 80%/95% sobre a receita acumulada.
- Revenue: ordena por receita e aplica 80%/95% sobre receita acumulada (clássico).

Configurável via utils.config.ABC_CURVE_CONFIG.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Optional

try:
    from utils.config import ABC_CURVE_CONFIG, get_abc_threshold
except ImportError:
    ABC_CURVE_CONFIG = {}
    def get_abc_threshold(key: str, default=None):
        return ABC_CURVE_CONFIG.get(key, default) if ABC_CURVE_CONFIG else default


def calculate_abc_by_performance(sku: pd.DataFrame, total_revenue_all: float) -> pd.DataFrame:
    """
    Classifica SKUs em A/B/C por performance (composite_score + giro quando existir)
    e aplica thresholds 80%/95% sobre a receita acumulada.

    Adiciona colunas: classificacao_abc, receita_acumulada_pct.
    Retorna o DataFrame ordenado por performance (desc).
    """
    if sku is None or sku.empty:
        sku = sku.copy()
        sku["classificacao_abc"] = ""
        sku["receita_acumulada_pct"] = 0.0
        return sku

    sku = sku.copy()
    total_revenue_all = float(total_revenue_all) if total_revenue_all else 1.0

    # Performance score: composite_score + giro (se existir)
    giro_weight = get_abc_threshold("performance_giro_weight", 0.5)
    score_weight = get_abc_threshold("performance_score_weight", 0.5)

    if "composite_score" not in sku.columns:
        sku["composite_score"] = 0.0
    comp = pd.to_numeric(sku["composite_score"], errors="coerce").fillna(0)

    if "giro_anual_projetado" in sku.columns:
        giro = pd.to_numeric(sku["giro_anual_projetado"], errors="coerce").fillna(0)
        g_max = giro.max() or 1
        giro_norm = giro / g_max
        sku["_performance_score"] = (giro_weight * giro_norm + score_weight * comp).values
    else:
        sku["_performance_score"] = comp.values

    # Ordenar por performance (desc) e calcular receita acumulada %
    sku = sku.sort_values("_performance_score", ascending=False).reset_index(drop=True)
    sku["receita_acumulada_pct"] = (sku["total_revenue"].cumsum() / total_revenue_all * 100) if total_revenue_all > 0 else 0.0

    # A/B/C pelos thresholds clássicos (80% / 95%)
    a_pct = get_abc_threshold("class_a_threshold_pct", 80.0)
    b_pct = get_abc_threshold("class_b_threshold_pct", 95.0)

    def _abc(row: float) -> str:
        if row <= a_pct:
            return "A"
        if row <= b_pct:
            return "B"
        return "C"

    sku["classificacao_abc"] = sku["receita_acumulada_pct"].apply(_abc)
    sku = sku.drop(columns=["_performance_score"], errors="ignore")
    return sku
