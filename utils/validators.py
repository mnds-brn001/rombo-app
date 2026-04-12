"""utils/validators.py
Centraliza validações, *guard-clauses* e utilitários de segurança usados em
todo o dashboard Streamlit.

Objetivos principais
---------------------
1. **Evitar exceções**: garanta que colunas existam e contenham dados antes de
   acessar ou agregar.
2. **Padronizar mensagens**: apresente avisos consistentes ao usuário final
   utilizando `st.info`.
3. **Reduzir duplicação**: abstrair lógicas repetidas que antes estavam
   espalhadas por cada página.

Seções do módulo
----------------
• KPIs / DataFrame base – helpers para criação de colunas obrigatórias.
• Charts / Aggregations – verificações de dados mínimos para plotagens.
• Insights / Others – *placeholder* para futuras funções.
• Helper utilities – funções reutilizáveis adicionadas em 2025-07-11.

Exemplo de uso rápido
---------------------
```python
from utils.validators import abort_if_no_data, abort_if_missing_or_empty

# Dentro de uma página Streamlit
if abort_if_no_data(df, "Dataset vazio", ["price"]):
    st.stop()

# Checar coluna específica
if abort_if_missing_or_empty(df, "review_score", "Sem avaliações disponíveis"):
    st.stop()
```
"""

 

from typing import Any, Dict, List
import pandas as pd
import streamlit as st
from utils.theme_manager import get_theme_manager
 
# --------------------
# UI Helpers (centered info)
# --------------------


def show_centered_info(
    message: str,
    *,
    icon: str = "⚠️",
    style: str | None = None,
) -> None:  # noqa: D401
    """Exibe *message* dentro de um *glass card* amarelo centralizado.

    Ideal para alertas e comunicações de validação.

    Parameters
    ----------
    message: str
        Texto a ser exibido.
    icon: str, optional
        Emoji ou caractere a ser mostrado antes da mensagem. Default "⚠️".
    """

    theme_manager = get_theme_manager()
    glass = theme_manager.get_glass_theme()
    theme = theme_manager.get_theme()

    # Cores específicas para alerta amarelo (ambiente escuro/claro)
    alert_bg = "rgba(251, 191, 36, 0.12)"  # Amarelo
    alert_border = "rgba(251, 191, 36, 0.4)"
    alert_shadow = "rgba(251, 191, 36, 0.35)"
    text_color = theme.get("textColor", "#e2e8f0")

    # Construir o estilo CSS de forma mais robusta
    css_style = f"backdrop-filter: blur({glass.get('cardBlur', '12px')}); -webkit-backdrop-filter: blur({glass.get('cardBlur', '12px')}); background: {alert_bg}; padding: 20px 25px; border-radius: {glass.get('cardBorderRadius', '15px')}; text-align: center; font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif; border: 1px solid {alert_border}; box-shadow: 0 4px 20px {alert_shadow}; margin: 15px 0;"
    
    if style:
        css_style += f"; {style}"
    
    st.markdown(
        f'<div style="{css_style}"><span style="font-size:1.1em; color:{text_color};">{icon} {message}</span></div>',
        unsafe_allow_html=True,
    )

# --------------------
# KPIs / DataFrame base
# --------------------

def ensure_columns(df: pd.DataFrame, default_map: Dict[str, Any]) -> pd.DataFrame:  # noqa: D401
    """Garante que *todas* as colunas do ``default_map`` existam em *df*.

    Se a coluna estiver ausente, é criada com o valor/scalar fornecido.
    Retorna o próprio DataFrame para permitir encadeamento.
    """
    for col, default in default_map.items():
        if col not in df.columns:
            df[col] = default
    return df

# --------------------
# Charts / Aggregations
# --------------------

def has_data(df: pd.DataFrame | None, required_cols: List[str] | None = None, *, min_rows: int = 1) -> bool:  # noqa: D401
    """Verifica rapidamente se *df* possui dados suficientes.

    Parâmetros
    ----------
    df: DataFrame a validar.
    required_cols: Lista de colunas que devem existir (opcional).
    min_rows: Mínimo de linhas necessárias (default = 1).
    """
    if df is None or df.empty or len(df) < min_rows:
        return False
    if required_cols:
        for col in required_cols:
            if col not in df.columns:
                return False
            # Verificar se a coluna tem dados não-nulos
            if df[col].dropna().empty:
                return False
    return True


def abort_if_no_data(
    df: pd.DataFrame | None,
    message: str,
    required_cols: List[str] | None = None,
    *,
    min_rows: int = 1,
) -> bool:  # noqa: D401
    """Exibe *message* e retorna **True** se não houver dados válidos."""
    if not has_data(df, required_cols, min_rows=min_rows):
        show_centered_info(message)
        return True
    return False

# --------------------
# Insights / Others (placeholder)
# --------------------
# Futuras funções específicas podem ser adicionadas aqui. 

# --------------------
# Helper utilities (new)
# --------------------

# ---------------------------------------------------------------------------
# Novas funções de validação adicionadas para compatibilidade com a suíte de
# testes localizada em ``tests/test_utils_validators.py``. Essas funções são
# propositalmente simples e auto-contidas, evitando dependências pesadas. A
# responsabilidade principal é **retornar um booleano** indicando se a coluna
# atende aos critérios especificados. As mensagens de feedback podem ser
# exibidas externamente através de ``show_centered_info`` quando desejado.
# ---------------------------------------------------------------------------

def validate_column_exists(df: pd.DataFrame | None, column: str | None) -> bool:  # noqa: D401
    """Verifica se *column* existe em *df*.

    Parameters
    ----------
    df : pd.DataFrame | None
        DataFrame a ser inspecionado.
    column : str | None
        Nome da coluna a verificar.
    """

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    if column is None:
        return False
    return bool(column in df.columns)


def validate_numeric_column(df: pd.DataFrame | None, column: str | None) -> bool:  # noqa: D401
    """Retorna *True* se *column* contiver apenas valores numéricos.*"""

    from pandas.api.types import is_numeric_dtype  # import local para evitar overhead

    if not validate_column_exists(df, column):
        return False

    series = df[column].dropna()
    if series.empty:
        return False

    # Se o dtype já for numérico retornamos True diretamente
    if is_numeric_dtype(series):
        return True  # Already a Python bool

    # Caso contrário tentamos converter – se todas as conversões forem bem-sucedidas,
    # consideramos a coluna numérica.
    converted = pd.to_numeric(series, errors="coerce")
    return bool(converted.notna().all())


def validate_date_column(df: pd.DataFrame | None, column: str | None) -> bool:  # noqa: D401
    """Retorna *True* se *column* tiver dtype de data ou puder ser convertida.

    A função é **tolerante**: se todas as entradas não nulas forem convertíveis
    para datetime, a validação retorna *True*.
    """

    from pandas.api.types import is_datetime64_any_dtype  # lazy import

    if not validate_column_exists(df, column):
        return False

    series = df[column].dropna()
    if series.empty:
        return False

    if is_datetime64_any_dtype(series):
        return True

    converted = pd.to_datetime(series, errors="coerce")  # infer_datetime_format é padrão agora
    return bool(converted.notna().all())


def validate_category_column(df: pd.DataFrame | None, column: str | None) -> bool:  # noqa: D401
    """Valida se *column* representa dados categóricos (não numéricos)."""

    from pandas.api.types import is_numeric_dtype
    from pandas import CategoricalDtype  # lazy import

    if not validate_column_exists(df, column):
        return False

    series = df[column].dropna()
    if series.empty:
        return False

    # Consideramos categórica se dtype já é category ou object **e** não numérica
    if isinstance(series.dtype, CategoricalDtype) or (series.dtype == object and not is_numeric_dtype(series)):
        return True

    return False


def ensure_required_columns(
    df: pd.DataFrame | None,
    required_columns: List[str] | None,
) -> bool:  # noqa: D401
    """Garante que todas as *required_columns* existam em *df*.

    Se faltar alguma, retorna *False*. Caso a lista seja ``None`` ou vazia,
    assume que não há exigências adicionais.
    """

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False

    if not required_columns:
        return True

    missing = [col for col in required_columns if col not in df.columns]
    return bool(len(missing) == 0)

# ---------------------------------------------------------------------------
# Funções restauradas (foram removidas inadvertidamente em edição anterior)
# ---------------------------------------------------------------------------


def column_has_data(df: pd.DataFrame, column: str) -> bool:  # noqa: D401
    """Retorna *True* se *column* existir e possuir ao menos um valor não nulo."""
    return column in df.columns and df[column].dropna().shape[0] > 0


def abort_if_missing_or_empty(
    df: pd.DataFrame,
    column: str,
    message: str,
) -> bool:  # noqa: D401
    """Mostra *message* e retorna **True** se a *column* não existir ou estiver vazia."""
    if not column_has_data(df, column):
        show_centered_info(message)
        return True
    return False


def safe_get(mapping: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Acesso seguro para dicionários, evitando *KeyError*.

    Útil para acessar métricas opcionais em *insights* ou configurações.
    """
    return mapping.get(key, default) or default


def validate_dataframe_for_dashboard(df: pd.DataFrame | None, page_name: str) -> bool:
    """
    Validação abrangente de DataFrame para uso em dashboards.
    
    Verifica se o DataFrame tem os dados mínimos necessários para
    funcionamento básico de qualquer página do dashboard.
    
    Args:
        df: DataFrame a ser validado
        page_name: Nome da página para mensagem de erro personalizada
        
    Returns:
        bool: True se válido, False caso contrário
    """
    # Verificar se DataFrame existe e não está vazio
    if not has_data(df, min_rows=1):
        show_centered_info(f"Dados insuficientes para {page_name}. Verifique se há dados no período selecionado.")
        return False
    
    # Verificar colunas essenciais
    essential_columns = ["order_purchase_timestamp", "price", "customer_id"]
    missing_columns = [col for col in essential_columns if col not in df.columns]
    
    if missing_columns:
        show_centered_info(
            f"Colunas essenciais ausentes para {page_name}: {', '.join(missing_columns)}. "
            "Verifique a integridade dos dados."
        )
        return False
    
    # Verificar se há dados válidos nas colunas essenciais
    for col in essential_columns:
        if abort_if_missing_or_empty(df, col, f"Coluna '{col}' não possui dados válidos para {page_name}."):
            return False
    
    return True