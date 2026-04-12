"""
Configurações centralizadas para classificação BCG, Curva ABC e scores compostos.
Todos os thresholds e magic numbers são definidos aqui para fácil manutenção.
"""

# -----------------------
# BCG Matrix Configuration
# -----------------------
BCG_CONFIG = {
    # Threshold de composite_score abaixo do qual o SKU/categoria é Abacaxi
    'composite_score_threshold': 0.2,
    
    # Percentual de market share para considerar "alto" (%)
    'market_share_threshold_pct': 15.0,
    
    # Percentual de growth rate para considerar "alto" (%)
    'growth_rate_threshold_pct': 20.0,
    
    # Percentis dinâmicos (usado quando thresholds fixos não aplicam)
    'growth_percentile': 50,  # Mediana (50º percentil)
    'share_percentile': 50,   # Mediana (50º percentil)
}

# -----------------------
# ABC Curve Configuration
# -----------------------
ABC_CURVE_CONFIG = {
    # Thresholds clássicos (percentual acumulado)
    'class_a_threshold_pct': 80.0,   # Top 80% = Classe A
    'class_b_threshold_pct': 95.0,   # 80-95% = Classe B
    # Acima 95% = Classe C (implícito)
    
    # Pesos para performance_score (Giro + Composite Score)
    'performance_giro_weight': 0.5,   # 50% peso para giro anual
    'performance_score_weight': 0.5,  # 50% peso para composite_score
    
    # Método de classificação: 'revenue' (antigo) ou 'performance' (novo)
    'classification_method': 'performance',
}

# -----------------------
# Composite Score Weights
# -----------------------
COMPOSITE_SCORE_WEIGHTS = {
    'revenue_growth': 0.20,
    'market_share': 0.15,
    'total_revenue': 0.15,
    'avg_rating': 0.10,
    'cancellation_rate': 0.10,
    'units_sold': 0.06,
    'avg_price': 0.05,
    'payment_value': 0.05,
    'items_per_order': 0.05,
    'review_rate': 0.05,
    'avg_payment': 0.04
}

# -----------------------
# Outlier Treatment
# -----------------------
OUTLIER_CONFIG = {
    'default_mode': 'winsor',  # 'none', 'cap', ou 'winsor'
    'abs_cap': 1000.0,         # Cap absoluto para growth rate (%)
    'winsor_percentile_low': 2,
    'winsor_percentile_high': 98,
}

# -----------------------
# SKU Filtering
# -----------------------
SKU_FILTERING_CONFIG = {
    'best_skus_min_score': 0.85,  # Composite score mínimo para "best SKUs"
}

# -----------------------
# Helper Functions
# -----------------------
def get_bcg_threshold(key: str, default=None):
    """Retorna threshold BCG com fallback."""
    return BCG_CONFIG.get(key, default)

def get_abc_threshold(key: str, default=None):
    """Retorna threshold ABC com fallback."""
    return ABC_CURVE_CONFIG.get(key, default)

def get_composite_weight(key: str, default=0.0):
    """Retorna peso de métrica do composite score com fallback."""
    return COMPOSITE_SCORE_WEIGHTS.get(key, default)
