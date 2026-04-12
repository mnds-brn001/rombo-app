import json
import logging
import os
import streamlit as st
import pandas as pd
import numpy as np
import hashlib
from typing import Dict, Any, List, Tuple, Sequence, cast
from utils.validators import has_data, show_centered_info, abort_if_no_data, abort_if_missing_or_empty
from utils.theme_manager import get_theme_manager
from textwrap import dedent

_logger = logging.getLogger(__name__)

# Fallback: quando o componente streamlit_echarts não carrega (assets/proxy), usar HTML + ECharts CDN.
# Defina ECHARTS_USE_COMPONENT=1 para forçar o uso do componente (se funcionar no seu ambiente).
_USE_ECHARTS_COMPONENT = os.environ.get("ECHARTS_USE_COMPONENT", "").strip().lower() in ("1", "true", "yes")
if _USE_ECHARTS_COMPONENT:
    import streamlit_echarts as echarts  # noqa: F401
else:
    echarts = None


def safe_numeric_to_list(series_or_array, round_digits=None, default_value=0):
    """
    Converte dados numéricos para lista JSON-safe, tratando NaN adequadamente.
    
    Args:
        series_or_array: Pandas Series, numpy array ou lista
        round_digits: Número de casas decimais para arredondar (opcional)
        default_value: Valor para substituir NaN (default: 0)
    
    Returns:
        Lista com valores seguros para JSON (sem NaN)
    """
    import numpy as np
    import pandas as pd
    
    if series_or_array is None:
        return []
    
    # Converter para pandas Series se não for
    if not isinstance(series_or_array, pd.Series):
        if hasattr(series_or_array, 'tolist'):
            series_or_array = pd.Series(series_or_array.tolist())
        else:
            series_or_array = pd.Series([series_or_array] if not isinstance(series_or_array, list) else series_or_array)
    
    # Substituir valores problemáticos (NaN, inf, -inf)
    series_cleaned = series_or_array.replace([np.nan, np.inf, -np.inf], default_value)
    
    # Aplicar arredondamento se especificado
    if round_digits is not None:
        series_cleaned = series_cleaned.round(round_digits)
    
    # Converter para lista
    result = series_cleaned.tolist()
    
    # Garantir que todos os valores sejam JSON-safe
    safe_result = []
    for value in result:
        # Verificar se é NaN usando pd.isna (mais robusto)
        if pd.isna(value):
            safe_result.append(default_value)
        # Converter tipos numpy para tipos Python nativos
        elif isinstance(value, (np.integer, np.floating)):
            if np.isnan(value) or np.isinf(value):
                safe_result.append(default_value)
            else:
                safe_result.append(float(value) if isinstance(value, np.floating) else int(value))
        else:
            safe_result.append(value)
    
    return safe_result


# Meses em português (abreviado) para labels de eixo de datas
_MESES_ABREV = ("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")


def _format_ts_date_label(value: Any) -> str:
    """
    Formata valor de data/período para exibição em gráficos (padrão brasileiro).
    Evita YYYY-MM-DD; usa DD/MM/YYYY ou mmm/YYYY conforme o caso.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # YYYY-MM (período mensal)
    if len(s) == 7 and s[4] == "-":
        try:
            y, m = int(s[:4]), int(s[5:7])
            if 1 <= m <= 12:
                return f"{_MESES_ABREV[m - 1]}/{y}"
        except (ValueError, IndexError):
            pass
    # YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            y, mo, d = s[:4], s[5:7], s[8:10]
            return f"{d}/{mo}/{y}"
        except (ValueError, IndexError):
            pass
    return s


def _format_ts_date_labels(values: List[Any]) -> List[str]:
    """Aplica formatação brasileira a uma lista de valores de data/período."""
    return [_format_ts_date_label(v) for v in values]


def _dynamic_axis_bounds(
    values: Sequence[Any],
    *,
    clamp_min: float | None = None,
    clamp_max: float | None = None,
    pad_abs: float = 0.0,
    pad_ratio: float = 0.15,
    min_range: float = 0.01,
) -> tuple[float, float] | tuple[None, None]:
    """
    Calcula limites dinâmicos de eixo Y (min/max) com margem.
    - Ignora None/NaN/Inf
    - Aplica clamp_min/clamp_max quando fornecidos

    Retorna (None, None) se não houver valores válidos.
    """
    cleaned: list[float] = []
    for v in values:
        try:
            if v is None:
                continue
            fv = float(v)
            if np.isnan(fv) or np.isinf(fv):
                continue
            cleaned.append(fv)
        except Exception:
            continue

    if not cleaned:
        return (None, None)

    vmin = min(cleaned)
    vmax = max(cleaned)
    vrange = max(vmax - vmin, min_range)
    pad = max(pad_abs, vrange * pad_ratio)
    y_min = vmin - pad
    y_max = vmax + pad

    if clamp_min is not None:
        y_min = max(clamp_min, y_min)
    if clamp_max is not None:
        y_max = min(clamp_max, y_max)

    if y_max <= y_min:
        # fallback seguro
        y_min = (clamp_min if clamp_min is not None else vmin) - max(pad_abs, 0.2)
        y_max = (clamp_max if clamp_max is not None else vmax) + max(pad_abs, 0.2)

    return (round(y_min, 2), round(y_max, 2))

def _hash_dataframe(df: pd.DataFrame) -> str:
    """Função customizada para hashear DataFrames baseada no conteúdo."""
    # Hash do DataFrame completo para cache mais preciso.
    # Normaliza colunas com listas/sets/dicts para evitar TypeError: unhashable type: 'list'.
    df_hash = df.copy()
    for col in df_hash.columns:
        if df_hash[col].dtype == "object":
            df_hash[col] = df_hash[col].map(
                lambda v: tuple(v) if isinstance(v, list)
                else tuple(sorted(v)) if isinstance(v, set)
                else tuple(sorted(v.items())) if isinstance(v, dict)
                else v
            )
    # MD5 usado apenas para cache de hash, não para segurança
    return hashlib.md5(pd.util.hash_pandas_object(df_hash, index=True).values.tobytes()).hexdigest()  # nosec B324

def _cached_chart(func):
    return st.cache_data(
        hash_funcs={pd.DataFrame: _hash_dataframe},
        ttl=7200,  # Cache por 2 horas
        max_entries=50,
        show_spinner=False
    )(func)

def get_theme_colors() -> Dict[str, Any]:
    """Retorna as cores do tema atual."""
    theme_manager = get_theme_manager()
    theme = theme_manager.get_theme()
    glass = theme_manager.get_glass_theme()
    
    return {
        "text_color": theme.get('textColor', '#e2e8f0'),
        "border_color": theme.get('secondaryBackgroundColor', '#1e293b'),
        "bg_color": glass.get('cardBackground', 'rgba(30, 41, 59, 0.7)'),
        "shadow_color": glass.get('cardShadow', 'rgba(0, 0, 0, 0.2)'),
        "accent_color": theme.get('primaryColor', '#6366f1'),
        "success_color": '#10b981',
        "warning_color": '#f59e0b',
        "danger_color": '#ef4444',
        "info_color": '#3b82f6'
    }

def apply_theme_to_chart(option: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aplica o tema atual às opções do gráfico ECharts.
    
    Args:
        option: Dicionário com as opções do gráfico
        
    Returns:
        Dicionário com as opções atualizadas
    """
    colors = get_theme_colors()
    
    # Configurações globais
    option.setdefault('backgroundColor', 'transparent')
    option.setdefault('textStyle', {})
    option['textStyle'].update({
        'color': colors['text_color'],
        'fontFamily': 'Inter, Helvetica Neue, Arial, sans-serif'
    })
    
    # Cores da paleta
    option.setdefault('color', [
        colors['accent_color'],
        colors['success_color'],
        colors['warning_color'],
        colors['danger_color'],
        colors['info_color']
    ])
    
    # Estilo dos eixos
    if 'xAxis' in option:
        if isinstance(option['xAxis'], dict):
            option['xAxis'].setdefault('axisLine', {})
            option['xAxis']['axisLine'].setdefault('lineStyle', {})
            option['xAxis']['axisLine']['lineStyle']['color'] = colors['border_color']
            
            option['xAxis'].setdefault('axisLabel', {})
            option['xAxis']['axisLabel']['color'] = colors['text_color']
            
            option['xAxis'].setdefault('splitLine', {})
            option['xAxis']['splitLine'].setdefault('lineStyle', {})
            option['xAxis']['splitLine']['lineStyle']['color'] = colors['border_color']
    
    if 'yAxis' in option:
        if isinstance(option['yAxis'], dict):
            option['yAxis'].setdefault('axisLine', {})
            option['yAxis']['axisLine'].setdefault('lineStyle', {})
            option['yAxis']['axisLine']['lineStyle']['color'] = colors['border_color']
            
            option['yAxis'].setdefault('axisLabel', {})
            option['yAxis']['axisLabel']['color'] = colors['text_color']
            
            option['yAxis'].setdefault('splitLine', {})
            option['yAxis']['splitLine'].setdefault('lineStyle', {})
            option['yAxis']['splitLine']['lineStyle']['color'] = colors['border_color']
    
    # Estilo da legenda
    if 'legend' in option:
        option['legend'].setdefault('textStyle', {})
        option['legend']['textStyle']['color'] = colors['text_color']
    
    # Estilo do tooltip
    if 'tooltip' in option:
        option['tooltip'].setdefault('backgroundColor', colors['bg_color'])
        option['tooltip'].setdefault('borderColor', colors['border_color'])
        option['tooltip'].setdefault('textStyle', {})
        option['tooltip']['textStyle']['color'] = colors['text_color']
    
    return option

def create_satisfaction_chart(monthly_satisfaction: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de linha mostrando a evolução da satisfação do cliente ao longo do tempo.
    
    Args:
        monthly_satisfaction: DataFrame contendo os dados de satisfação mensal
            com colunas 'order_purchase_timestamp' e 'review_score'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Sanitizar: remover NaN para evitar JSON inválido
    if monthly_satisfaction is None or monthly_satisfaction.empty:
        monthly_satisfaction = pd.DataFrame(columns=["order_purchase_timestamp", "review_score"])
    monthly_satisfaction = monthly_satisfaction.dropna(subset=["review_score"]).copy()
    monthly_satisfaction["review_score"] = pd.to_numeric(monthly_satisfaction["review_score"], errors="coerce")
    monthly_satisfaction = monthly_satisfaction.dropna(subset=["review_score"])

    if monthly_satisfaction.empty:
        return {
            "title": {"text": " ", "left": "center"},
            "tooltip": {**_GLASS_TOOLTIP_STYLE, "trigger": "axis"},
            "xAxis": {"type": "category", "data": []},
            "yAxis": {"type": "value", "name": "Nota Média"},
            "series": [{"data": [], "type": "line"}],
        }

    # -----------------------------
    # Eixo Y dinâmico (não fixa em 0..5)
    # Objetivo: deixar variações pequenas visíveis (monitoramento/alerta)
    # -----------------------------
    try:
        vals = monthly_satisfaction["review_score"].astype(float).tolist()
        vmin = min(vals) if vals else 0.0
        vmax = max(vals) if vals else 5.0
        # padding proporcional com fallback para séries quase planas
        vrange = max(vmax - vmin, 0.01)
        pad = max(0.10, vrange * 0.15)  # 0.10 estrelas ou 15% do range
        y_min = max(0.0, round(vmin - pad, 2))
        y_max = min(5.0, round(vmax + pad, 2))
        if y_max <= y_min:
            y_min = max(0.0, round(vmin - 0.2, 2))
            y_max = min(5.0, round(vmax + 0.2, 2))
    except Exception:
        y_min, y_max = 0.0, 5.0

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_satisfaction['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "min": y_min,
            "max": y_max,
            "name": "Nota Média"
        },
        "series": [{
            "data": monthly_satisfaction['review_score'].round(2).tolist(),
            "type": "line",
            "smooth": True,
            "lineStyle": {
                "width": 3
            },
            "areaStyle": {
                "opacity": 0.3
            }
        }]
    }
    return option

def create_cancellation_chart(monthly_cancellation: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de linha mostrando a evolução da taxa de cancelamento.
    
    Args:
        monthly_cancellation: DataFrame contendo os dados de cancelamento mensal
            com colunas 'order_purchase_timestamp' e 'pedido_cancelado'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Eixo dinâmico em % (evita começar em 0 sempre)
    try:
        perc = (pd.to_numeric(monthly_cancellation["pedido_cancelado"], errors="coerce").fillna(0) * 100).tolist()
        vmin = min(perc) if perc else 0.0
        vmax = max(perc) if perc else 100.0
        vrange = max(vmax - vmin, 0.01)
        pad = max(1.0, vrange * 0.15)  # 1pp ou 15% do range
        y_min = max(0.0, round(vmin - pad, 2))
        y_max = round(vmax + pad, 2)
        if y_max <= y_min:
            y_min = max(0.0, round(vmin - 2.0, 2))
            y_max = round(vmax + 2.0, 2)
    except Exception:
        y_min, y_max = 0.0, 100.0

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_cancellation['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "min": y_min,
            "max": y_max,
            "name": "Taxa de Cancelamento (%)",
            "axisLabel": {"formatter": "{value}%"}
        },
        "series": [{
            "data": (monthly_cancellation['pedido_cancelado'] * 100).round(2).tolist(),
            "type": "line",
            "smooth": True,
            "lineStyle": {
                "width": 3
            },
            "areaStyle": {
                "opacity": 0.3
            }
        }]
    }
    return option

def create_revenue_chart(monthly_revenue: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de linha mostrando a evolução da receita mensal.
    
    Args:
        monthly_revenue: DataFrame contendo os dados de receita mensal
            com colunas 'order_purchase_timestamp' e 'price'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Eixo Y dinâmico para evitar “achatamento” (útil para monitoramento)
    y_min, y_max = _dynamic_axis_bounds(
        monthly_revenue["price"].tolist() if monthly_revenue is not None and not monthly_revenue.empty and "price" in monthly_revenue.columns else [],
        clamp_min=0.0,
        clamp_max=None,
        pad_abs=1000.0,  # pelo menos R$ 1k de margem (ajusta conforme magnitude)
        pad_ratio=0.12,
        min_range=1.0,
    )

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_revenue['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Receita (R$)",
            **({"min": y_min} if y_min is not None else {}),
            **({"max": y_max} if y_max is not None else {}),
        },
        "series": [{
            "data": monthly_revenue['price'].round(2).tolist(),
            "type": "line",
            "smooth": True,
            "lineStyle": {
                "width": 3
            },
            "areaStyle": {
                "opacity": 0.3
            }
        }]
    }
    return option

def create_orders_volume_chart(
    weekly_orders: pd.DataFrame,
    stock_movements: pd.DataFrame = None,
    orders_detail: pd.DataFrame = None,
    period: str = "M",
) -> Dict[str, Any]:
    """
    Cria um gráfico de linha mostrando a evolução do volume de pedidos e movimentação de estoque.
    
    Args:
        weekly_orders: DataFrame contendo os dados de volume de pedidos
            com colunas 'order_purchase_timestamp' e 'order_count'
        stock_movements: DataFrame opcional contendo volume de movimentação de estoque
            com colunas 'date' e 'qty' (agregado pelo mesmo período)
        orders_detail: DataFrame opcional com detalhe por pedido (order_purchase_timestamp,
            product_sku ou product_id, order_id) para exibir top 5 SKUs por período no tooltip
        period: Período de agregação ('D', 'W', 'M') usado em weekly_orders; necessário para alinhar orders_detail
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Detectar qual coluna usar para o eixo X (preferência para timestamp padronizado)
    time_column = 'order_purchase_timestamp'
    count_column = 'order_count'
    
    # Preparar eixo X unificado (labels em formato brasileiro: DD/MM/YYYY ou mmm/YYYY)
    x_data_raw = weekly_orders[time_column].tolist()
    x_data = _format_ts_date_labels(x_data_raw)

    # Eixo Y dinâmico: volume de pedidos (e estoque se existir)
    orders_values_raw = (
        weekly_orders[count_column].astype(float).tolist()
        if weekly_orders is not None and not weekly_orders.empty and count_column in weekly_orders.columns
        else []
    )
    orders_values = [float(x) for x in orders_values_raw]

    # Top 5 SKUs por período para o tooltip (quando orders_detail e período estão disponíveis)
    sku_col = "product_sku" if (orders_detail is not None and "product_sku" in orders_detail.columns) else ("product_id" if (orders_detail is not None and "product_id" in orders_detail.columns) else None)
    max_skus_in_tooltip = 5
    orders_volume_data: List[Any] = []
    if orders_detail is not None and not orders_detail.empty and sku_col and period and len(orders_values) == len(x_data_raw):
        od = orders_detail.copy()
        od["_ts"] = pd.to_datetime(od[time_column], errors="coerce")
        od = od.dropna(subset=["_ts"])
        od["_period"] = od["_ts"].dt.to_period(period).astype(str)
        for i, period_str in enumerate(x_data_raw):
            count_val = orders_values[i] if i < len(orders_values) else 0
            skus_list: List[Dict[str, Any]] = []
            period_df = od[od["_period"] == str(period_str)]
            if not period_df.empty:
                sku_counts = period_df.groupby(sku_col)["order_id"].nunique().sort_values(ascending=False).head(max_skus_in_tooltip)
                for sku, val in sku_counts.items():
                    skus_list.append({"sku": str(sku).strip() or "(sem SKU)", "value": int(val)})
            orders_volume_data.append({"value": round(count_val, 0), "skus": skus_list})
    else:
        orders_volume_data = orders_values

    y0_min, y0_max = _dynamic_axis_bounds(
        orders_values,
        clamp_min=0.0,
        clamp_max=None,
        pad_abs=10.0,
        pad_ratio=0.12,
        min_range=1.0,
    )
    
    # Se houver dados de estoque, alinhar as datas
    stock_data_list = []
    if stock_movements is not None and not stock_movements.empty:
        # Garantir que stock_movements tenha as colunas esperadas
        if 'date' in stock_movements.columns and 'qty' in stock_movements.columns:
            # Criar dicionário data -> qty para lookup rápido
            stock_dict = dict(zip(stock_movements['date'].astype(str), stock_movements['qty']))
            # Preencher lista alinhada com x_data_raw (chave no formato original)
            stock_data_list = [stock_dict.get(str(d), 0) for d in x_data_raw]
    
    use_value_encode = bool(orders_volume_data and isinstance(orders_volume_data[0], dict))
    series = [{
        "name": "Volume de Pedidos",
        "data": orders_volume_data,
        "type": "line",
        **({"encode": {"y": "value"}} if use_value_encode else {}),
        "smooth": True,
        "lineStyle": {
            "width": 3,
            "color": "#2ca02c"
        },
        "itemStyle": {
            "color": "#2ca02c"
        },
        "areaStyle": {
            "opacity": 0.3,
            "color": {
                "type": "linear",
                "x": 0,
                "y": 0,
                "x2": 0,
                "y2": 1,
                "colorStops": [
                    {"offset": 0, "color": "rgba(44, 160, 44, 0.4)"},
                    {"offset": 1, "color": "rgba(44, 160, 44, 0.1)"}
                ]
            }
        }
    }]

    # Adicionar série de estoque se houver dados
    y_axis = [
        {
            "type": "value",
            "name": "Volume de Pedidos",
            "position": "left",
            "axisLine": {"show": True, "lineStyle": {"color": "#2ca02c"}},
            "axisLabel": {"formatter": "{value}"},
            **({"min": y0_min} if y0_min is not None else {}),
            **({"max": y0_max} if y0_max is not None else {}),
        }
    ]

    if stock_data_list:
        y1_min, y1_max = _dynamic_axis_bounds(
            stock_data_list,
            clamp_min=0.0,
            clamp_max=None,
            pad_abs=10.0,
            pad_ratio=0.12,
            min_range=1.0,
        )
        series.append({
            "name": "Movimentação de Estoque (Qtd)",
            "data": [float(v) for v in stock_data_list],
            "type": "line",
            "smooth": True,
            "yAxisIndex": 1,  # Usar segundo eixo Y
            "lineStyle": {
                "width": 3,
                "color": "#3b82f6",
                "type": "dashed"
            },
            "itemStyle": {
                "color": "#3b82f6"
            },
            "areaStyle": {
                "opacity": 0.1,
                "color": "#3b82f6"
            }
        })
        
        y_axis.append({
            "type": "value",
            "name": "Movimentação (Itens)",
            "position": "right",
            "axisLine": {"show": True, "lineStyle": {"color": "#3b82f6"}},
            "axisLabel": {"formatter": "{value}"},
            "splitLine": {"show": False}, # Evitar excesso de linhas de grade
            **({"min": y1_min} if y1_min is not None else {}),
            **({"max": y1_max} if y1_max is not None else {}),
        })

    # Tooltip com top 5 SKUs por período quando orders_volume_data for lista de objetos
    tooltip_extra = {}
    if orders_volume_data and isinstance(orders_volume_data[0], dict) and "skus" in orders_volume_data[0]:
        tooltip_extra["formatter"] = (
            r"function(params) {"
            r" var p0 = params[0]; var n = p0.name; var data = p0.data;"
            r" var v = (data && data.value != null ? data.value : p0.value);"
            r" var html = '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + n + '</div>';"
            r" html += '<div style=\"font-size:14px;font-weight:700;color:#34d399;margin-bottom:6px\">Volume de Pedidos: ' + (typeof v === 'number' ? v.toLocaleString('pt-BR') : v) + '</div>';"
            r" if (params.length > 1 && params[1].value != null) {"
            r"   html += '<div style=\"font-size:13px;color:#60a5fa\">Movimentação de Estoque: ' + params[1].value.toLocaleString('pt-BR') + '</div>';"
            r" }"
            r" if (data && data.skus && data.skus.length > 0) {"
            r"   html += '<div style=\"font-size:12px;color:#94a3b8;margin-top:8px;border-top:1px solid rgba(148,163,184,0.3);padding-top:6px\">Top 5 SKUs no período:</div>';"
            r"   for (var i = 0; i < data.skus.length; i++) {"
            r"     var s = data.skus[i]; var sv = s.value != null ? s.value.toLocaleString('pt-BR') : '-';"
            r"     html += '<div style=\"font-size:11px;color:#cbd5e1;margin-left:4px;margin-top:2px\">' + (s.sku || '') + ': ' + sv + ' pedidos</div>';"
            r"   }"
            r" }"
            r" return html;"
            r"}"
        )

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            },
            **tooltip_extra,
        },
        "legend": {
            "data": [s["name"] for s in series],
            "top": 0
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": x_data,
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": y_axis,
        "series": series
    }
    return option


def create_ltv_cac_comparison_chart(monthly_metrics: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de comparação entre LTV e CAC ao longo do tempo.
    
    Args:
        monthly_metrics: DataFrame contendo as métricas mensais
            com colunas 'order_purchase_timestamp', 'monthly_ltv', 'monthly_cac', 'ltv_cac_ratio'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis"
        },
        "legend": {
            "data": ['LTV', 'CAC', 'Razão LTV/CAC'],
            "top": 30
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_metrics['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": [
            {
                "type": "value",
                "name": "Valor (R$)",
                "position": "left"
            },
            {
                "type": "value",
                "name": "Razão LTV/CAC",
                "position": "right",
                "splitLine": {
                    "show": False
                },
                "axisLine": {
                    "show": False
                },
                "axisTick": {
                    "show": False
                },
                "axisLabel": {
                    "show": False
                }
            }
        ],
       "series": [
            {
                "name": "LTV",
                "type": "line",
                "data": monthly_metrics['monthly_ltv'].round(2).tolist(),
                "smooth": True,
                "lineStyle": {
                    "width": 3,
                    "color": "rgb(46, 204, 113)"
                },
                "itemStyle": {
                    "color": "rgb(46, 204, 113)"
                },
                "areaStyle": {
                    "opacity": 0.3,
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 0,
                        "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "rgba(46, 204, 113, 0.4)"},
                            {"offset": 1, "color": "rgba(46, 204, 113, 0.1)"}
                        ]
                    }
                }
            },
            {
                "name": "CAC",
                "type": "line",
                "data": monthly_metrics['monthly_cac'].round(2).tolist(),
                "smooth": True,
                "lineStyle": {
                    "width": 3,
                    "color": "rgb(231, 76, 60)"
                },
                "itemStyle": {
                    "color": "rgb(231, 76, 60)"
                },
                "areaStyle": {
                    "opacity": 0.3,
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 0,
                        "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "rgba(231, 76, 60, 0.4)"},
                            {"offset": 1, "color": "rgba(231, 76, 60, 0.1)"}
                        ]
                    }
                }
            },
            {
                "name": "Razão LTV/CAC",
                "type": "line",
                "yAxisIndex": 1,
                "data": monthly_metrics['ltv_cac_ratio'].round(2).tolist(),
                "smooth": True,
                "lineStyle": {
                    "width": 3,
                    "color": "#6366f1",
                    "type": "dashed"
                },
                "itemStyle": {
                    "color": "#6366f1"
                },
                "areaStyle": {
                    "opacity": 0.3,
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 0,
                        "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "rgba(99, 102, 241, 0.4)"},
                            {"offset": 1, "color": "rgba(99, 102, 241, 0.1)"}
                        ]
                    }
                }
            }
        ]
    }
    return option

def create_customer_evolution_chart(
    new_customers: pd.DataFrame,
    returning_customers: pd.DataFrame,
    stack_bars: bool = False,
) -> Dict[str, Any]:
    """
    Cria um gráfico de barras empilhadas mostrando a evolução de novos e retornando clientes.
    
    Args:
        new_customers: DataFrame com dados de novos clientes
            com colunas 'month' e 'customer_unique_id'
        returning_customers: DataFrame com dados de clientes retornando
            com colunas 'month' e 'customer_unique_id'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Converter valores para tipos Python nativos
    new_customers_data = new_customers['customer_unique_id'].astype(int).tolist()
    returning_customers_data = returning_customers['customer_unique_id'].astype(int).tolist()
    months = new_customers['month'].tolist()

    # Gradientes e estilos metallic/glass
    new_gradient = {
        "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
        "colorStops": [
            {"offset": 0, "color": "rgba(59,130,246,0.95)"},   # azul
            {"offset": 1, "color": "rgba(99,102,241,0.70)"}    # indigo
        ]
    }
    returning_gradient = {
        "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
        "colorStops": [
            {"offset": 0, "color": "rgba(16,185,129,0.95)"},  # emerald
            {"offset": 1, "color": "rgba(34,197,94,0.75)"}     # green
        ]
    }

    option = {
        "title": {"text": " ", "left": "center"},
        "animation": True,
        "animationDuration": 900,
        "animationEasing": "cubicOut",
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"}
        },
        "legend": {
            "data": ['Novos Clientes', 'Clientes Retornando'],
            "top": 30,
            "textStyle": {"color": "#e2e8f0"},
            "icon": "roundRect",
            "itemWidth": 14,
            "itemHeight": 10
        },
        "grid": {
            "left": "3%", "right": "4%", "bottom": "12%", "top": "20%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": months,
            "axisLabel": {"rotate": 45, "color": "#cbd5e1"},
            "axisLine": {"lineStyle": {"color": "rgba(148,163,184,0.35)"}},
            "axisTick": {"show": False}
        },
        "yAxis": {
            "type": "value",
            "name": "Quantidade de Clientes",
            "nameTextStyle": {"color": "#94a3b8"},
            "axisLabel": {"color": "#cbd5e1"},
            "axisLine": {"show": False},
            "splitLine": {"show": True, "lineStyle": {"color": "rgba(148,163,184,0.12)"}}
        },
        "series": [
            {
                "name": "Novos Clientes",
                "type": "bar",
                **({"stack": "total"} if stack_bars else {}),
                "data": new_customers_data,
                "barWidth": 20,
                "barCategoryGap": "55%",
                "itemStyle": {
                    "color": new_gradient,
                    "borderColor": "rgba(255,255,255,0.08)",
                    "borderWidth": 1,
                    "shadowBlur": 12,
                    "shadowColor": "rgba(59,130,246,0.25)",
                    "shadowOffsetY": 2,
                    "borderRadius": [8,8,0,0]
                },
                "emphasis": {
                    "focus": "series",
                    "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(99,102,241,0.35)"}
                }
            },
            {
                "name": "Clientes Retornando",
                "type": "bar",
                **({"stack": "total"} if stack_bars else {}),
                "data": returning_customers_data,
                "barWidth": 20,
                "barGap": "30%",
                "itemStyle": {
                    "color": returning_gradient,
                    "borderColor": "rgba(255,255,255,0.08)",
                    "borderWidth": 1,
                    "shadowBlur": 12,
                    "shadowColor": "rgba(16,185,129,0.25)",
                    "shadowOffsetY": 2,
                    "borderRadius": [8,8,0,0]
                },
                "emphasis": {
                    "focus": "series",
                    "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(16,185,129,0.35)"}
                }
            }
        ]
    }
    return option

def get_funnel_semantic_colors(conversion_rate: float) -> str:
    """
    Retorna cor semântica baseada na taxa de conversão.
    
    Args:
        conversion_rate: Taxa de conversão em porcentagem
    
    Returns:
        Cor hexadecimal semântica
    """
    if conversion_rate >= 95:
        return "#10b981"  # Verde - Excelente
    elif conversion_rate >= 85:
        return "#3b82f6"  # Azul - Boa
    elif conversion_rate >= 70:
        return "#f59e0b"  # Amarelo - Atenção
    else:
        return "#ef4444"  # Vermelho - Crítica

def get_funnel_icon(stage: str) -> str:
    """
    Retorna ícone SVG para cada etapa do funil.
    
    Args:
        stage: Nome da etapa do funil
    
    Returns:
        Ícone SVG formatado
    """
    from utils.svg_icons import get_svg_icon
    
    icon_map = {
        "Visitantes": get_svg_icon("people", size=16, color="#e2e8f0"),
        "Visualizações": get_svg_icon("eye", size=16, color="#e2e8f0"),
        "Carrinho": get_svg_icon("shopping-cart", size=16, color="#e2e8f0"),
        "Checkout": get_svg_icon("credit-card", size=16, color="#e2e8f0"),
        "Criados": get_svg_icon("plus-circle", size=16, color="#e2e8f0"),
        "Aprovados": get_svg_icon("check-circle", size=16, color="#e2e8f0"),
        "Enviados": get_svg_icon("truck", size=16, color="#e2e8f0"),
        "Entregues": get_svg_icon("package", size=16, color="#e2e8f0")
    }
    
    return icon_map.get(stage, get_svg_icon("circle", size=16, color="#e2e8f0"))

def create_metallic_funnel_chart(funnel_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um funil de pedidos com design metallic glassmorphism seguindo o design system atual.
    
    Features:
    - Cores semânticas baseadas em taxas de conversão
    - Gradientes glassmorphism
    - Ícones SVG para cada etapa
    - Tooltips avançados com formatação brasileira
    - Efeitos metallic shine
    
    Args:
        funnel_data: DataFrame contendo os dados do funil
            com colunas 'status_label' e 'count'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Converter valores para tipos Python nativos
    funnel_values = funnel_data['count'].astype(int).tolist()
    funnel_labels = funnel_data['status_label'].tolist()
    max_value = int(funnel_data['count'].max())
    
    # Calcular taxas de conversão
    conversion_rates = []
    for i in range(len(funnel_values)):
        if i == 0:
            conversion_rates.append(100.0)  # Primeira etapa sempre 100%
        else:
            rate = (funnel_values[i] / funnel_values[0]) * 100 if funnel_values[0] > 0 else 0
            conversion_rates.append(rate)
    
    # Preparar dados com cores semânticas e ícones
    funnel_data_formatted = []
    for i, (value, label, rate) in enumerate(zip(funnel_values, funnel_labels, conversion_rates)):
        semantic_color = get_funnel_semantic_colors(rate)
        icon = get_funnel_icon(label)
        
        # Criar gradiente semântico
        gradient = {
            "type": "linear",
            "x": 0, "y": 0, "x2": 1, "y2": 1,
            "colorStops": [
                {"offset": 0, "color": f"{semantic_color}30"},
                {"offset": 0.5, "color": f"{semantic_color}60"},
                {"offset": 1, "color": f"{semantic_color}30"}
            ]
        }
        
        funnel_data_formatted.append({
            "value": value,
            "name": f"{icon} {label}",
            "conversion_rate": rate,
            "itemStyle": {
                "color": gradient,
                "borderColor": semantic_color,
                "borderWidth": 2,
                "shadowBlur": 8,
                "shadowColor": f"{semantic_color}40"
            }
        })
    
    # Formatter customizado para tooltips brasileiros
    tooltip_formatter = [r"function(params) { const value = params.value; const rate = params.data.conversion_rate; const name = params.name.replace(/<[^>]*>/g, ''); const formattedValue = value.toLocaleString('pt-BR'); let statusIcon = ''; if (rate >= 95) statusIcon = '🟢'; else if (rate >= 85) statusIcon = '🔵'; else if (rate >= 70) statusIcon = '🟡'; else statusIcon = '🔴'; return '<div style=\"background: linear-gradient(135deg, rgba(30,41,59,0.95), rgba(45,55,72,0.98)); border: 1px solid rgba(227,236,240,0.3); border-radius: 12px; padding: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); backdrop-filter: blur(16px); color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', Roboto, sans-serif;\"><div style=\"font-weight: 600; margin-bottom: 8px; color: #f8fafc;\">' + name + '</div><div style=\"font-size: 18px; font-weight: 700; color: #10b981; margin-bottom: 4px;\">' + formattedValue + ' pedidos</div><div style=\"font-size: 14px; color: #cbd5e1;\">' + statusIcon + ' Taxa de conversão: ' + rate.toFixed(1) + '%</div></div>'; }"]
    
    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "item",
            "formatter": tooltip_formatter
        },
        "series": [
            {
                "name": "Pedidos",
                "type": "funnel",
                "left": "8%",
                "top": 20,
                "bottom": 50,
                "width": "84%",
                "min": 0,
                "max": max_value,
                "minSize": "0%",
                "maxSize": "100%",
                "sort": "descending",
                "gap": 4,
                "label": {
                    "show": True,
                    "position": "inside",
                    "fontSize": 14,
                    "fontWeight": "600",
                    "color": "#f8fafc",
                    "formatter": "{b}\n{c}",
                    "rich": {
                        "icon": {
                            "fontSize": 16,
                            "color": "#e2e8f0"
                        },
                        "text": {
                            "fontSize": 12,
                            "color": "#cbd5e1",
                            "fontWeight": "500"
                        }
                    }
                },
                "labelLine": {
                    "length": 15,
                    "lineStyle": {
                        "width": 2,
                        "type": "solid",
                        "color": "rgba(227,236,240,0.3)"
                    }
                },
                "itemStyle": {
                    "borderColor": "#e2e8f0",
                    "borderWidth": 1,
                    "shadowBlur": 12,
                    "shadowColor": "rgba(0,0,0,0.4)"
                },
                "emphasis": {
                    "label": {
                        "fontSize": 16,
                        "fontWeight": "700"
                    },
                    "itemStyle": {
                        "shadowBlur": 20,
                        "shadowColor": "rgba(0,0,0,0.6)"
                    }
                },
                "data": funnel_data_formatted
            }
        ]
    }
    
    return option
def create_order_funnel_chart(funnel_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de funil mostrando a evolução dos pedidos.
    Wrapper para manter compatibilidade - chama a nova implementação metallic.
    
    Args:
        funnel_data: DataFrame contendo os dados do funil
            com colunas 'status_label' e 'count'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    return create_metallic_funnel_chart(funnel_data)

def create_exception_orders_chart(exception_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de barras horizontais mostrando pedidos problemáticos/excepcionais.
    
    Args:
        exception_data: DataFrame contendo os dados de exceções
            com colunas 'status_type' e 'count'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Converter valores para tipos Python nativos
    exception_values = exception_data['count'].astype(int).tolist()
    exception_labels = exception_data['status_type'].tolist()
    
    # Cores semânticas por tipo de exceção
    color_map = {
        'Problema Fluxo Postal': '#f59e0b',      # Amarelo - Atenção
        'Pagamento em Análise': '#3b82f6',       # Azul - Info
        'Devolvido Financeiro': '#ef4444',       # Vermelho - Crítico
        'Nota Fiscal Cancelada': '#f97316',      # Laranja - Alerta
        'Pagamento Cancelado': '#dc2626',        # Vermelho escuro - Crítico
        'Troca/Crédito': '#8b5cf6'              # Roxo - Info
    }
    
    # Preparar dados com cores personalizadas
    bar_data = []
    for label, value in zip(exception_labels, exception_values):
        color = color_map.get(label, '#64748b')  # Cor padrão cinza
        bar_data.append({
            'value': value,
            'itemStyle': {
                'color': {
                    'type': 'linear',
                    'x': 0, 'y': 0, 'x2': 1, 'y2': 0,
                    'colorStops': [
                        {'offset': 0, 'color': f'{color}90'},
                        {'offset': 0.5, 'color': f'{color}'},
                        {'offset': 1, 'color': f'{color}60'}
                    ]
                },
                'borderColor': color,
                'borderWidth': 1,
                'shadowBlur': 8,
                'shadowColor': f'{color}40',
                'borderRadius': [0, 8, 8, 0]
            }
        })
    
    option = {
        'title': {
            'text': ' ',
            'left': 'center'
        },
        'animation': True,
        'animationDuration': 800,
        'animationEasing': 'cubicOut',
        'tooltip': {
            'trigger': 'axis',
            'axisPointer': {'type': 'shadow'},
            'backgroundColor': 'rgba(15,23,42,0.92)',
            'borderColor': 'rgba(148,163,184,0.35)',
            'textStyle': {'color': '#e2e8f0'},
            'formatter': [r"function(params) { const name = params[0].name; const value = params[0].value; const formattedValue = value.toLocaleString('pt-BR'); let icon = ''; if (name.includes('Problema')) icon = '⚠️'; else if (name.includes('Análise')) icon = '🔍'; else if (name.includes('Devolvido')) icon = '💸'; else if (name.includes('Cancelad')) icon = '❌'; else if (name.includes('Troca')) icon = '🔄'; return '<div style=\"padding: 8px;\"><strong>' + icon + ' ' + name + '</strong><br/><span style=\"font-size: 16px; color: #10b981;\">' + formattedValue + ' pedidos</span></div>'; }"]
        },
        'grid': {
            'left': '5%',
            'right': '8%',
            'bottom': '8%',
            'top': '5%',
            'containLabel': True
        },
        'xAxis': {
            'type': 'value',
            'name': 'Quantidade de Pedidos',
            'nameLocation': 'middle',
            'nameGap': 30,
            'nameTextStyle': {'color': '#94a3b8', 'fontSize': 12},
            'axisLabel': {'color': '#cbd5e1'},
            'axisLine': {'lineStyle': {'color': 'rgba(148,163,184,0.35)'}},
            'splitLine': {'show': True, 'lineStyle': {'color': 'rgba(148,163,184,0.12)'}}
        },
        'yAxis': {
            'type': 'category',
            'data': exception_labels,
            'axisLabel': {
                'color': '#e2e8f0',
                'fontSize': 12,
                'fontWeight': '500',
                'margin': 12
            },
            'axisLine': {'show': False},
            'axisTick': {'show': False}
        },
        'series': [{
            'name': 'Pedidos Problemáticos',
            'type': 'bar',
            'data': bar_data,
            'barWidth': '65%',
            'label': {
                'show': True,
                'position': 'right',
                'color': '#f8fafc',
                'fontSize': 13,
                'fontWeight': '600',
                'formatter': '{c}'
            },
            'emphasis': {
                'itemStyle': {
                    'shadowBlur': 15,
                    'shadowColor': 'rgba(0,0,0,0.5)'
                }
            }
        }]
    }
    
    return option

def _json_serial(obj: Any) -> Any:
    """Serializa valores não-JSON (numpy, pandas, NaN, datetime) para o HTML do ECharts."""
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    if isinstance(obj, float):
        if obj != obj or not np.isfinite(obj):
            return None
        return obj
    # numpy
    if isinstance(obj, (np.integer, np.int32, np.int64)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float32, np.float64)):
        if obj != obj or not np.isfinite(obj):
            return None
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    # pandas / datetime
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat")):
        return obj.isoformat()
    if hasattr(obj, "__str__") and "Period" in type(obj).__name__:
        return str(obj)
    # Fallback: evita quebrar o gráfico por tipo inesperado (ex.: numpy scalar)
    try:
        return float(obj) if isinstance(obj, (np.floating, np.integer)) else str(obj)
    except (TypeError, ValueError):
        return str(obj)


def _render_echarts_via_html(option: Dict[str, Any], height: int, theme: str) -> None:
    """Renderiza ECharts via HTML + CDN quando o componente streamlit_echarts não carrega."""
    try:
        # Usar as cores reais do tema (custom theme em config.toml), não theme.base
        try:
            bg = st.get_option("theme.secondaryBackgroundColor") or st.get_option("theme.backgroundColor")
        except Exception:
            bg = None
        if not bg or not isinstance(bg, str) or not bg.strip():
            bg = get_theme_colors().get("border_color")  # secondaryBackgroundColor do theme_manager
        if not bg or not isinstance(bg, str):
            bg = "#1e293b" if (theme == "dark") else "#FFFFFF"
        bg = bg.strip()
        option_copy = dict(option)
        option_copy["backgroundColor"] = bg
        option_json = json.dumps(option_copy, default=_json_serial, ensure_ascii=False)
        option_json_escaped = option_json.replace("</script>", "<\\/script>")
        # Tema ECharts: "dark" se a cor de fundo for escura (hex escuro)
        try:
            hex_clean = bg.lstrip("#")[:6]
            if len(hex_clean) >= 6:
                r, g, b = int(hex_clean[0:2], 16), int(hex_clean[2:4], 16), int(hex_clean[4:6], 16)
                luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
                use_dark_theme = luminance < 0.5
            else:
                use_dark_theme = theme == "dark"
        except Exception:
            use_dark_theme = theme == "dark"
        theme_js = json.dumps("dark" if use_dark_theme else None)
        html = f"""
        <style>html, body {{ margin:0; padding:0; background:{bg} !important; }}</style>
        <div id="echarts-root" style="width:100%;height:{height}px;background:{bg} !important;"></div>
        <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
        <script>
        (function() {{
            function formatterStringsToFunctions(obj) {{
                if (obj === null || obj === undefined) return obj;
                if (typeof obj === 'string' && obj.trim().indexOf('function') === 0) {{
                    return (new Function('return (' + obj + ')'))();
                }}
                if (Array.isArray(obj)) {{
                    for (var i = 0; i < obj.length; i++) obj[i] = formatterStringsToFunctions(obj[i]);
                    return obj;
                }}
                if (typeof obj === 'object') {{
                    for (var k in obj) {{
                        if (k === 'formatter') {{
                            var v = obj[k];
                            if (typeof v === 'string' && v.trim().indexOf('function') === 0) {{
                                try {{ obj[k] = (new Function('return (' + v + ')'))(); }} catch (e) {{}}
                            }} else if (Array.isArray(v) && v.length > 0 && typeof v[0] === 'string' && v[0].trim().indexOf('function') === 0) {{
                                try {{ obj[k] = (new Function('return (' + v[0] + ')'))(); }} catch (e) {{}}
                            }} else {{
                                obj[k] = formatterStringsToFunctions(v);
                            }}
                        }} else {{
                            obj[k] = formatterStringsToFunctions(obj[k]);
                        }}
                    }}
                    return obj;
                }}
                return obj;
            }}
            var el = document.getElementById("echarts-root");
            if (!el) return;
            var chart = echarts.init(el, {theme_js});
            var option = {option_json_escaped};
            formatterStringsToFunctions(option);
            chart.setOption(option);
            chart.setOption({{ backgroundColor: {json.dumps(bg)} }});
            window.addEventListener("resize", function() {{ chart.resize(); }});
        }})();
        </script>
        """
        st.components.v1.html(html, height=height + 20, scrolling=False)
    except Exception as e:
        _logger.exception("ECharts HTML fallback falhou: %s", e)
        st.warning("Não foi possível renderizar o gráfico. Verifique os dados.")

def render_echarts_chart(option: Dict[str, Any], height: int = 400) -> None:
    """
    Renderiza um gráfico ECharts com o tema aplicado.
    Usa fallback HTML+CDN quando o componente streamlit_echarts não carrega (proxy/assets).
    Defina ECHARTS_USE_COMPONENT=1 para forçar o uso do componente.
    """
    option = apply_theme_to_chart(option)
    # Custom theme pode não ter theme.base == "dark"; considerar dark se a cor de fundo for escura
    try:
        sec = (st.get_option("theme.secondaryBackgroundColor") or "").strip().lstrip("#")
        if len(sec) >= 6:
            r, g, b = int(sec[0:2], 16), int(sec[2:4], 16), int(sec[4:6], 16)
            theme = "dark" if (0.299 * r + 0.587 * g + 0.114 * b) / 255 < 0.5 else "light"
        else:
            theme = "dark" if st.get_option("theme.base") == "dark" else "light"
    except Exception:
        theme = "dark" if st.get_option("theme.base") == "dark" else "light"

    if _USE_ECHARTS_COMPONENT and echarts is not None:
        echarts.st_echarts(options=option, height=height, theme=theme)
    else:
        _render_echarts_via_html(option, height, theme)


def create_satisfaction_analysis_charts(filtered_df: pd.DataFrame, period: str = 'M') -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Cria dois gráficos relacionados à satisfação do cliente:
    1. Evolução da satisfação ao longo do tempo
    2. Distribuição das notas de satisfação
    
    Args:
        filtered_df: DataFrame contendo os dados filtrados
            com colunas 'order_purchase_timestamp' e 'review_score'
        period: Período de agregação ('D', 'W', 'M')
    
    Returns:
        Tuple contendo dois dicionários com as configurações dos gráficos ECharts
    """
    # Gráfico de Satisfação do Cliente ao Longo do Tempo
    monthly_satisfaction = filtered_df.groupby(filtered_df['order_purchase_timestamp'].dt.to_period(period))['review_score'].mean().reset_index()
    monthly_satisfaction['order_purchase_timestamp'] = monthly_satisfaction['order_purchase_timestamp'].astype(str)

    # Eixo Y dinâmico para monitoramento (evita "0..5" achatar variações)
    y_min, y_max = _dynamic_axis_bounds(
        monthly_satisfaction["review_score"].tolist(),
        clamp_min=0.0,
        clamp_max=5.0,
        pad_abs=0.10,   # pelo menos 0.10 estrela de margem
        pad_ratio=0.15,
        min_range=0.05,
    )
    
    satisfaction_option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "formatter": r"function(params) { return '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + params[0].axisValue + '</div><div style=\"color:#f59e0b;font-weight:700\">Nota Média: ' + params[0].value.toFixed(2) + '</div>'; }"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_satisfaction['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            **({"min": y_min} if y_min is not None else {}),
            **({"max": y_max} if y_max is not None else {}),
            "name": "Nota Média",
            "splitNumber": 5
        },
        "series": [{
            "data": monthly_satisfaction['review_score'].round(2).tolist(),
            "type": "line",
            "smooth": True,
            "symbol": "circle",
            "symbolSize": 8,
            "lineStyle": {
                "width": 3,
                "color": "#f59e0b"
            },
            "itemStyle": {
                "color": "#f59e0b"
            },
            "areaStyle": {
                "opacity": 0.3,
                "color": {
                    "type": "linear",
                    "x": 0,
                    "y": 0,
                    "x2": 0,
                    "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "rgba(245, 158, 11, 0.4)"},
                        {"offset": 1, "color": "rgba(245, 158, 11, 0.1)"}
                    ]
                }
            }
        }]
    }
    
    # Gráfico de Distribuição de Satisfação
    review_counts = filtered_df['review_score'].value_counts().sort_index()
    
    distribution_option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "formatter": r"function(params) { return '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">Nota ' + params[0].name + '</div><div style=\"color:#f59e0b;font-weight:700\">Quantidade: ' + params[0].value + '</div>'; }"
        },
        "xAxis": {
            "type": "category",
            "data": review_counts.index.tolist(),
            "name": "Nota",
            "nameLocation": "middle",
            "nameGap": 25
        },
        "yAxis": {
            "type": "value",
            "name": "Quantidade de Avaliações"
        },
        "series": [{
            "data": review_counts.values.tolist(),
            "type": "bar",
            "itemStyle": {
                "color": "#f59e0b",
                "opacity": 0.8
            }
        }]
    }
    
    return satisfaction_option, distribution_option

def create_delivery_analysis_charts(filtered_df: pd.DataFrame, period: str = 'M') -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Cria dois gráficos relacionados à análise de entrega:
    1. Evolução do tempo de entrega ao longo do tempo
    2. Evolução do ticket médio ao longo do tempo
    
    Args:
        filtered_df: DataFrame contendo os dados filtrados
            com colunas 'order_purchase_timestamp', 'order_delivered_customer_date' e 'price'
        period: Período de agregação ('D', 'W', 'M')
    
    Returns:
        Tuple contendo dois dicionários com as configurações dos gráficos ECharts
    """
    filtered_df = filtered_df.copy()
    
    # Calcular tempo de entrega de forma segura
    try:
        if 'order_delivered_customer_date' in filtered_df.columns:
            delivered_dates = pd.to_datetime(filtered_df['order_delivered_customer_date'], errors='coerce', utc=True).dt.tz_localize(None)
            purchase_dates = pd.to_datetime(filtered_df['order_purchase_timestamp'], errors='coerce', utc=True).dt.tz_localize(None)
            
            # Calcular tempo de entrega apenas para registros válidos
            valid_mask = delivered_dates.notna() & purchase_dates.notna()
            if valid_mask.any():
                filtered_df.loc[valid_mask, 'delivery_time'] = (delivered_dates[valid_mask] - purchase_dates[valid_mask]).dt.days
                # Limpar outliers
                filtered_df.loc[filtered_df['delivery_time'] < 0, 'delivery_time'] = np.nan
                filtered_df.loc[filtered_df['delivery_time'] > 180, 'delivery_time'] = np.nan
            else:
                filtered_df['delivery_time'] = 15  # Valor padrão
        else:
            filtered_df['delivery_time'] = 15  # Valor padrão se não houver coluna
    except Exception:
        filtered_df['delivery_time'] = 15  # Valor padrão em caso de erro
    
    # Garantir que não há NaN em delivery_time
    filtered_df['delivery_time'] = filtered_df['delivery_time'].fillna(15)
    
    # Agrupar por período e limpar dados
    monthly_delivery = filtered_df.groupby(
        pd.to_datetime(filtered_df['order_purchase_timestamp']).dt.to_period(period)
    )['delivery_time'].mean().reset_index()
    monthly_delivery['order_purchase_timestamp'] = monthly_delivery['order_purchase_timestamp'].astype(str)
    
    # Limpar dados para JSON seguro
    delivery_data = monthly_delivery['delivery_time'].fillna(15).round(1).tolist()

    # Eixo Y dinâmico (dias) para destacar variações
    dmin, dmax = _dynamic_axis_bounds(
        delivery_data,
        clamp_min=0.0,
        clamp_max=None,
        pad_abs=1.0,     # pelo menos 1 dia de margem
        pad_ratio=0.15,
        min_range=1.0,
    )
    
    delivery_option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "formatter": r"function(params) { return '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + params[0].axisValue + '</div><div style=\"color:#34d399;font-weight:700\">Tempo Médio: ' + params[0].value.toFixed(1) + ' dias</div>'; }"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(monthly_delivery['order_purchase_timestamp'].tolist()),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Dias",
            "nameLocation": "middle",
            "nameGap": 40,
            **({"min": dmin} if dmin is not None else {}),
            **({"max": dmax} if dmax is not None else {}),
        },
        "series": [{
            "data": delivery_data,
            "type": "line",
            "smooth": True,
            "symbol": "circle",
            "symbolSize": 8,
            "lineStyle": {
                "width": 3,
                "color": "#2ca02c"
            },
            "itemStyle": {
                "color": "#2ca02c"
            },
            "areaStyle": {
                "opacity": 0.3,
                "color": {
                    "type": "linear",
                    "x": 0,
                    "y": 0,
                    "x2": 0,
                    "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "rgba(44, 160, 44, 0.4)"},
                        {"offset": 1, "color": "rgba(44, 160, 44, 0.1)"}
                    ]
                }
            }
        }]
    }
    
    # Gráfico de Ticket Médio ao Longo do Tempo
    try:
        ticket_base = filtered_df.copy()
        if "pedido_cancelado" in ticket_base.columns:
            ticket_base = ticket_base[ticket_base["pedido_cancelado"] == 0]

        use_valor_total = "valorTotal" in ticket_base.columns and "order_id" in ticket_base.columns
        if use_valor_total:
            by_order = ticket_base.groupby("order_id").agg(
                order_purchase_timestamp=("order_purchase_timestamp", "first"),
                valorTotal=("valorTotal", "max"),
            ).reset_index()
            by_order["valorTotal"] = pd.to_numeric(by_order["valorTotal"], errors="coerce").fillna(0)
            if by_order.empty:
                ticket_dates = monthly_delivery['order_purchase_timestamp'].tolist()
                ticket_values = [50.0] * len(ticket_dates)
            else:
                monthly_ticket = by_order.groupby(
                    pd.to_datetime(by_order['order_purchase_timestamp']).dt.to_period(period)
                )['valorTotal'].mean().reset_index()
                monthly_ticket['order_purchase_timestamp'] = monthly_ticket['order_purchase_timestamp'].astype(str)
                ticket_dates = monthly_ticket['order_purchase_timestamp'].tolist()
                ticket_values = monthly_ticket['valorTotal'].fillna(50.0).round(2).tolist()
        else:
            # Garantir que price não tenha NaN
            ticket_base['price'] = pd.to_numeric(ticket_base['price'], errors='coerce').fillna(0)
            price_data = ticket_base[ticket_base['price'] > 0]  # Filtrar apenas preços válidos
            if price_data.empty:
                ticket_dates = monthly_delivery['order_purchase_timestamp'].tolist()
                ticket_values = [50.0] * len(ticket_dates)
            else:
                monthly_ticket = price_data.groupby(
                    pd.to_datetime(price_data['order_purchase_timestamp']).dt.to_period(period)
                )['price'].mean().reset_index()
                monthly_ticket['order_purchase_timestamp'] = monthly_ticket['order_purchase_timestamp'].astype(str)
                ticket_dates = monthly_ticket['order_purchase_timestamp'].tolist()
                ticket_values = monthly_ticket['price'].fillna(50.0).round(2).tolist()
    except Exception:
        # Fallback para dados mínimos
        ticket_dates = monthly_delivery['order_purchase_timestamp'].tolist()
        ticket_values = [50.0] * len(ticket_dates)

    # Eixo Y dinâmico (R$) para destacar variações do ticket
    tmin, tmax = _dynamic_axis_bounds(
        ticket_values,
        clamp_min=0.0,
        clamp_max=None,
        pad_abs=5.0,     # pelo menos R$ 5 de margem
        pad_ratio=0.12,
        min_range=1.0,
    )
    
    ticket_option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "formatter": r"function(params) { var v = params[0].value; var s = v.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}); return '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + params[0].axisValue + '</div><div style=\"color:#38bdf8;font-weight:700\">Ticket Médio: R$ ' + s + '</div>'; }"
        },
        "xAxis": {
            "type": "category",
            "data": _format_ts_date_labels(ticket_dates),
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Valor Médio (R$)",
            "nameLocation": "middle",
            "nameGap": 40,
            **({"min": tmin} if tmin is not None else {}),
            **({"max": tmax} if tmax is not None else {}),
            "axisLabel": {
                "formatter": r"function(value) { return 'R$ ' + value.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}); }"
            }
        },
        "series": [{
            "data": ticket_values,
            "type": "line",
            "smooth": True,
            "symbol": "circle",
            "symbolSize": 8,
            "lineStyle": {
                "width": 3,
                "color": "#6366f1"
            },
            "itemStyle": {
                "color": "#6366f1"
            },
            "areaStyle": {
                "opacity": 0.3,
                "color": {
                    "type": "linear",
                    "x": 0,
                    "y": 0,
                    "x2": 0,
                    "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "rgba(99, 102, 241, 0.4)"},
                        {"offset": 1, "color": "rgba(99, 102, 241, 0.1)"}
                    ]
                }
            }
        }]
    }
    
    return delivery_option, ticket_option

def aggregate_sentiment_by_period(filtered_df: pd.DataFrame, period: str = 'M') -> pd.DataFrame:
    """
    Agrega sentimentos por período temporal.
    
    Args:
        filtered_df: DataFrame com reviews e timestamps
        period: 'D' (diário), 'W' (semanal), 'M' (mensal)
    
    Returns:
        DataFrame com colunas: ['period', 'positive_ratio', 'neutral_ratio', 
                               'negative_ratio', 'total_reviews', 'avg_score']
    """
    # Filtrar apenas reviews com score válido
    df_reviews = filtered_df[filtered_df['review_score'].notna()].copy()
    
    if df_reviews.empty:
        # Retornar DataFrame vazio com estrutura correta
        return pd.DataFrame(columns=['period', 'positive_ratio', 'neutral_ratio', 'negative_ratio', 'total_reviews', 'avg_score'])
    
    # Agregar reviews por período
    df_reviews['period'] = pd.to_datetime(df_reviews['order_purchase_timestamp']).dt.to_period(period)
    
    # Calcular métricas de sentimento por período
    sentiment_metrics = df_reviews.groupby('period').agg({
        'review_score': [
            ('avg_score', 'mean'),
            ('total_reviews', 'count'),
            ('positive_count', lambda x: (x >= 4).sum()),
            ('neutral_count', lambda x: (x == 3).sum()),
            ('negative_count', lambda x: (x <= 2).sum())
        ]
    }).reset_index()
    
    # Flatten column names
    sentiment_metrics.columns = ['period', 'avg_score', 'total_reviews', 'positive_count', 'neutral_count', 'negative_count']
    
    # Calcular proporções e arredondar para 3 casas decimais
    sentiment_metrics['positive_ratio'] = (sentiment_metrics['positive_count'] / sentiment_metrics['total_reviews']).round(2)
    sentiment_metrics['neutral_ratio'] = (sentiment_metrics['neutral_count'] / sentiment_metrics['total_reviews']).round(2)
    sentiment_metrics['negative_ratio'] = (sentiment_metrics['negative_count'] / sentiment_metrics['total_reviews']).round(2)
    
    # Converter período para string para compatibilidade com ECharts
    sentiment_metrics['period'] = sentiment_metrics['period'].astype(str)
    
    return sentiment_metrics

def create_sentiment_timeseries_chart(sentiment_data: pd.DataFrame, selected_sentiments: List[str] = None) -> Dict[str, Any]:
    """
    Cria gráfico de evolução temporal do sentimento com filtros.
    
    Args:
        sentiment_data: DataFrame com métricas de sentimento por período
        selected_sentiments: Lista de sentimentos a exibir ['Positivo', 'Neutro', 'Negativo']
    
    Returns:
        Dict com configuração do gráfico ECharts
    """
    if sentiment_data.empty:
        return create_empty_chart("Nenhum dado de sentimento disponível")
    
    periods = sentiment_data['period'].tolist()
    
    # Definir cores semânticas sofisticadas com gradientes
    sentiment_colors = {
        'Positivo': {
            'line': '#10b981',  # Verde esmeralda
            'gradient': {
                'type': 'linear',
                'x': 0, 'y': 0, 'x2': 0, 'y2': 1,
                'colorStops': [
                    {'offset': 0, 'color': 'rgba(16, 185, 129, 0.6)'},
                    {'offset': 0.5, 'color': 'rgba(16, 185, 129, 0.3)'},
                    {'offset': 1, 'color': 'rgba(16, 185, 129, 0.1)'}
                ]
            }
        },
        'Neutro': {
            'line': '#6366f1',  # Índigo
            'gradient': {
                'type': 'linear',
                'x': 0, 'y': 0, 'x2': 0, 'y2': 1,
                'colorStops': [
                    {'offset': 0, 'color': 'rgba(99, 102, 241, 0.6)'},
                    {'offset': 0.5, 'color': 'rgba(99, 102, 241, 0.3)'},
                    {'offset': 1, 'color': 'rgba(99, 102, 241, 0.1)'}
                ]
            }
        },
        'Negativo': {
            'line': '#ef4444',  # Vermelho coral
            'gradient': {
                'type': 'linear',
                'x': 0, 'y': 0, 'x2': 0, 'y2': 1,
                'colorStops': [
                    {'offset': 0, 'color': 'rgba(239, 68, 68, 0.6)'},
                    {'offset': 0.5, 'color': 'rgba(239, 68, 68, 0.3)'},
                    {'offset': 1, 'color': 'rgba(239, 68, 68, 0.1)'}
                ]
            }
        }
    }
    
    # Definir séries baseadas nos sentimentos selecionados
    series = []
    legend_data = []
    
    sentiment_mapping = {
        'Positivo': ('positive_ratio', 'Positivo'),
        'Neutro': ('neutral_ratio', 'Neutro'),
        'Negativo': ('negative_ratio', 'Negativo')
    }
    
    # Se nenhum sentimento específico foi selecionado, mostrar todos
    if not selected_sentiments:
        selected_sentiments = ['Positivo', 'Neutro', 'Negativo']
    
    for sentiment in selected_sentiments:
        if sentiment in sentiment_mapping:
            ratio_col, display_name = sentiment_mapping[sentiment]
            color_config = sentiment_colors[sentiment]
            
            series.append({
                "name": display_name,
                "type": "line",
                "data": sentiment_data[ratio_col].fillna(0).tolist(),
                "smooth": True,
                "symbol": "circle",
                "symbolSize": 8,
                "lineStyle": {
                    "width": 4,
                    "color": color_config['line'],
                    "shadowBlur": 8,
                    "shadowColor": f"{color_config['line']}40"
                },
                "itemStyle": {
                    "color": color_config['line'],
                    "shadowBlur": 6,
                    "shadowColor": f"{color_config['line']}60"
                },
                "areaStyle": {
                    "opacity": 0.4,
                    "color": color_config['gradient']
                },
                "emphasis": {
                    "focus": "series",
                    "itemStyle": {
                        "shadowBlur": 12,
                        "shadowColor": f"{color_config['line']}80"
                    }
                }
            })
            legend_data.append(display_name)
    
    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "formatter": (
                r"function(params) {"
                r" var result = '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:8px\">' + params[0].axisValue + '</div>';"
                r" params.forEach(function(item) {"
                r"   var pct = (item.value * 100).toFixed(1);"
                r"   result += item.marker + '<span style=\"color:' + item.color + '\">' + item.seriesName + '</span>: <strong>' + pct + '%</strong><br/>';"
                r" });"
                r" return result;"
                r"}"
            )
        },
        "legend": {
            "top": "bottom",
            "left": "center",
            "data": legend_data,
            "textStyle": {
                "color": "#e2e8f0"
            },
            "itemGap": 30
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "15%",
            "top": "10%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": periods,
            "axisLabel": {
                "rotate": 45,
                "color": "#94a3b8",
                "fontSize": 12
            },
            "axisLine": {
                "lineStyle": {
                    "color": "rgba(148, 163, 184, 0.3)"
                }
            },
            "axisTick": {
                "lineStyle": {
                    "color": "rgba(148, 163, 184, 0.3)"
                }
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Proporção (%)",
            "nameLocation": "middle",
            "nameGap": 40,
            "nameTextStyle": {
                "color": "#94a3b8",
                "fontSize": 12
            },
            "axisLabel": {
                "formatter": [r"function(value) { return (value * 100).toFixed(0) + '%'; }"],
                "color": "#94a3b8",
                "fontSize": 11
            },
            "axisLine": {
                "lineStyle": {
                    "color": "rgba(148, 163, 184, 0.3)"
                }
            },
            "axisTick": {
                "lineStyle": {
                    "color": "rgba(148, 163, 184, 0.3)"
                }
            },
            "splitLine": {
                "lineStyle": {
                    "color": "rgba(148, 163, 184, 0.1)",
                    "type": "dashed"
                }
            }
        },
        "series": series
    }
    
    return option

# Estilo de tooltip premium (glass/metálico) reutilizado nos gráficos de performance
_GLASS_TOOLTIP_STYLE: Dict[str, Any] = {
    "backgroundColor": "rgba(30, 41, 59, 0.92)",
    "borderColor": "rgba(148, 163, 184, 0.45)",
    "borderWidth": 1,
    "padding": [14, 18],
    "textStyle": {"color": "#e2e8f0", "fontSize": 13, "fontWeight": "500"},
    "extraCssText": (
        "backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); "
        "box-shadow: 0 8px 32px rgba(0,0,0,0.25), inset 0 1px 0 rgba(255,255,255,0.08); "
        "border-radius: 12px;"
    ),
}


def create_performance_analysis_charts(df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Cria os gráficos de análise de desempenho.
    
    Args:
        df: DataFrame com os dados
        
    Returns:
        Tuple contendo os gráficos:
        - Gráfico de categorias por receita
        - Gráfico de distribuição de preços
        - Gráfico de categorias por quantidade
        - Gráfico de taxa de cancelamento
    """
    return (
        create_category_revenue_chart(df),
        create_price_distribution_chart(df),
        create_category_quantity_chart(df),
        create_cancellation_rate_chart(df)
    )

def create_category_revenue_chart(df: pd.DataFrame) -> Dict[str, Any]:
    """Cria gráfico de top 10 categorias por receita. Contabiliza apenas valorTotal (v2/pedidos)."""
    if df.empty:
        return create_empty_chart("Nenhum dado disponível")
    
    # Tratamento de categorias nulas ou vazias
    df = df.copy()
    df['product_category_name'] = df['product_category_name'].fillna('Sem Categoria').replace(['', 'nan', 'NaN'], 'Sem Categoria')
    
    # Filtrar categorias desconhecidas para não poluir o gráfico de ranking
    df = df[df['product_category_name'] != 'Sem Categoria']
    
    if df.empty:
         return create_empty_chart("Sem dados de categoria identificados")

    # Receita por linha: apenas valorTotal (rateado por pedido quando há várias linhas)
    if "valorTotal" in df.columns and "order_id" in df.columns:
        df["_revenue_line"] = df.groupby("order_id")["valorTotal"].transform(
            lambda s: pd.to_numeric(s, errors="coerce").fillna(0).max() / max(len(s), 1)
        )
    else:
        df["_revenue_line"] = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0)

    category_revenue = df.groupby('product_category_name')['_revenue_line'].sum().sort_values(ascending=False).head(10)
    categories = category_revenue.index.tolist()

    sku_col = "product_sku" if "product_sku" in df.columns else ("product_id" if "product_id" in df.columns else None)
    max_skus_in_tooltip = 5
    series_data: List[Dict[str, Any]] = []
    for cat in categories:
        rev_val = float(category_revenue[cat])
        skus_list: List[Dict[str, Any]] = []
        if sku_col and sku_col in df.columns:
            cat_df = df[df["product_category_name"] == cat]
            if not cat_df.empty:
                sku_revenue = cat_df.groupby(sku_col)["_revenue_line"].sum().round(2).sort_values(ascending=False).head(max_skus_in_tooltip)
                for sku, val in sku_revenue.items():
                    skus_list.append({"sku": str(sku).strip() or "(sem SKU)", "value": float(val)})
        series_data.append({"value": round(rev_val, 2), "skus": skus_list})

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": (
                r"function(params) {"
                r" var p = params[0]; var n = p.name; var v = p.value; var data = p.data;"
                r" var fmt = function(x){ return x.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); };"
                r" var s = (typeof v === 'number' ? v : (data && data.value != null ? data.value : 0)); s = fmt(s);"
                r" var html = '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + n + '</div>';"
                r" html += '<div style=\"font-size:15px;font-weight:700;color:#38bdf8;margin-bottom:8px\">Receita: R$ ' + s + '</div>';"
                r" if (data && data.skus && data.skus.length > 0) {"
                r"   html += '<div style=\"font-size:12px;color:#94a3b8;margin-top:6px;border-top:1px solid rgba(148,163,184,0.3);padding-top:6px\">Por SKU:</div>';"
                r"   for (var i = 0; i < data.skus.length; i++) {"
                r"     var sk = data.skus[i]; var sv = sk.value != null ? fmt(sk.value) : '-';"
                r"     html += '<div style=\"font-size:11px;color:#cbd5e1;margin-left:4px;margin-top:2px\">' + (sk.sku || '') + ': R$ ' + sv + '</div>';"
                r"   }"
                r" }"
                r" return html;"
                r"}"
            ),
        },
        "grid": {
            "left": "10%",
            "right": "4%",
            "bottom": "15%",
            "top": "20%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": categories,
            "axisLabel": {
                "rotate": 45,
                "overflow": "break"
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Receita (R$)",
            "nameLocation": "middle",
            "nameGap": 80,
            "axisLabel": {
                "formatter": r"function(value) { return 'R$ ' + value.toLocaleString('pt-BR', {maximumFractionDigits: 0}).replace(/,/g, '#').replace(/\./g, ',').replace(/#/g, '.'); }"
            }
        },
        "series": [{
            "name": "Receita",
            "type": "bar",
            "data": series_data,
            "itemStyle": {
                "color": "#1f77b4",
                "opacity": 0.9
            },

        }]
    }
    return option


def create_price_distribution_chart(df: pd.DataFrame) -> Dict[str, Any]:
    """Cria gráfico de distribuição de preços por categoria."""
    if df.empty:
        return create_empty_chart("Nenhum dado disponível")
    
    # Filtrar apenas preços maiores que 0 e garantir que sejam numéricos
    df_filtered = df.copy()
    df_filtered['price'] = pd.to_numeric(df_filtered['price'], errors='coerce')
    df_filtered = df_filtered[df_filtered['price'] > 0].copy()
    
    if df_filtered.empty:
        return create_empty_chart("Nenhum dado com preços válidos (> 0)")
        
    # Calcular estatísticas por categoria
    stats: List[Dict[str, Any]] = []    
    # Agrupar por categoria e calcular estatísticas
    for category, group in df_filtered.groupby('product_category_name'):
        # Filtrar novamente preços > 0 dentro do grupo para garantir
        group_valid = group[group['price'] > 0]
        
        if not group_valid.empty and len(group_valid) > 0:  # Verificar se o grupo tem dados
            min_price = group_valid['price'].min()
            
            # Apenas adicionar a categoria se o mínimo for maior que 0
            if min_price > 0:
                price_stats = {
                    'category': category,
                    'min': round(min_price, 2),
                    'q1': round(group_valid['price'].quantile(0.25), 2),
                    'median': round(group_valid['price'].median(), 2),
                    'q3': round(group_valid['price'].quantile(0.75), 2),
                    'max': round(group_valid['price'].max(), 2)
                }
                stats.append(price_stats)
    
    # Se não houver estatísticas calculadas, retornar gráfico vazio
    if not stats:
        return create_empty_chart("Nenhum dado disponível")
    
    # Converter para DataFrame e ordenar
    stats_df = pd.DataFrame(stats)
    stats_df = stats_df.sort_values('median', ascending=False).head(10)  # Top 10 por mediana
    
    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": (
                r"function(params) { var p = params[0]; var d = p.value; if (!d || d.length < 5) return p.name; "
                r"var fmt = function(x){ return x.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); }; "
                r"return '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:8px\">' + p.name + '</div>' + "
                r"'<div style=\"color:#94a3b8;font-size:12px\">Min: R$ ' + fmt(d[0]) + '</div>' + "
                r"'<div style=\"color:#94a3b8;font-size:12px\">Q1: R$ ' + fmt(d[1]) + '</div>' + "
                r"'<div style=\"font-weight:700;color:#38bdf8;font-size:13px\">Mediana: R$ ' + fmt(d[2]) + '</div>' + "
                r"'<div style=\"color:#94a3b8;font-size:12px\">Q3: R$ ' + fmt(d[3]) + '</div>' + "
                r"'<div style=\"color:#94a3b8;font-size:12px\">Max: R$ ' + fmt(d[4]) + '</div>'; }"
            ),
        },
        "grid": {
            "left": "10%",
            "right": "4%",
            "bottom": "15%",
            "top": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": stats_df['category'].tolist(),
            "axisLabel": {
                "rotate": 45,
                "overflow": "break"
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Preço (R$)",
            "nameLocation": "middle",
            "nameGap": 80,
            "axisLabel": {
                "formatter": "R$ {value}"
            }
        },
        "series": [{
            "name": "Distribuição de Preços",
            "type": "boxplot",
            "data": stats_df.apply(
                lambda x: [x['min'], x['q1'], x['median'], x['q3'], x['max']], 
                axis=1
            ).tolist(),
            "itemStyle": {
                "color": "#1f77b4",
                "borderColor": "#1f77b4",
                "opacity": 0.9
            }
        }]
    }
    return option


def create_category_quantity_chart(df: pd.DataFrame) -> Dict[str, Any]:
    """Cria gráfico de top 10 categorias por quantidade."""
    if df.empty:
        return create_empty_chart("Nenhum dado disponível")
    
    # Tratamento de categorias nulas ou vazias
    df = df.copy()
    df['product_category_name'] = df['product_category_name'].fillna('Sem Categoria').replace(['', 'nan', 'NaN'], 'Sem Categoria')
    
    # Filtrar categorias desconhecidas
    df = df[df['product_category_name'] != 'Sem Categoria']
    
    if df.empty:
         return create_empty_chart("Sem dados de categoria identificados")
        
    category_quantity = df.groupby('product_category_name')['order_id'].nunique().sort_values(ascending=False).head(10)
    categories = category_quantity.index.tolist()

    sku_col = "product_sku" if "product_sku" in df.columns else ("product_id" if "product_id" in df.columns else None)
    max_skus_in_tooltip = 5
    series_data: List[Dict[str, Any]] = []
    for cat in categories:
        qty_val = int(category_quantity[cat])
        skus_list: List[Dict[str, Any]] = []
        if sku_col and sku_col in df.columns:
            cat_df = df[df["product_category_name"] == cat]
            if not cat_df.empty:
                sku_qty = cat_df.groupby(sku_col)["order_id"].nunique().sort_values(ascending=False).head(max_skus_in_tooltip)
                for sku, val in sku_qty.items():
                    skus_list.append({"sku": str(sku).strip() or "(sem SKU)", "value": int(val)})
        series_data.append({"value": qty_val, "skus": skus_list})

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": (
                r"function(params) {"
                r" var p = params[0]; var n = p.name; var v = p.value; var data = p.data;"
                r" var q = (typeof v === 'number' ? v : (data && data.value != null ? data.value : 0));"
                r" var html = '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + n + '</div>';"
                r" html += '<div style=\"font-size:15px;font-weight:700;color:#34d399;margin-bottom:8px\">Quantidade: ' + q.toLocaleString('pt-BR') + '</div>';"
                r" if (data && data.skus && data.skus.length > 0) {"
                r"   html += '<div style=\"font-size:12px;color:#94a3b8;margin-top:6px;border-top:1px solid rgba(148,163,184,0.3);padding-top:6px\">Por SKU:</div>';"
                r"   for (var i = 0; i < data.skus.length; i++) {"
                r"     var s = data.skus[i]; var sv = s.value != null ? s.value.toLocaleString('pt-BR') : '-';"
                r"     html += '<div style=\"font-size:11px;color:#cbd5e1;margin-left:4px;margin-top:2px\">' + (s.sku || '') + ': ' + sv + '</div>';"
                r"   }"
                r" }"
                r" return html;"
                r"}"
            ),
        },
        "grid": {
            "left": "10%",
            "right": "4%",
            "bottom": "15%",
            "top": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": categories,
            "axisLabel": {
                "rotate": 45,
                "overflow": "break"
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Quantidade",
            "nameLocation": "middle",
            "nameGap": 80
        },
        "series": [{
            "name": "Quantidade",
            "type": "bar",
            "data": series_data,
            "itemStyle": {
                "color": "#2ca02c",
                "opacity": 0.9
            },
        }]
    }
    return option


def create_cancellation_rate_chart(df: pd.DataFrame) -> Dict[str, Any]:
    """Cria gráfico de taxa de cancelamento por categoria. Tooltip inclui lista de SKUs com taxa individual."""
    if df.empty:
        return create_empty_chart("Nenhum dado disponível")

    df = df.copy()
    df['product_category_name'] = df['product_category_name'].fillna('Sem Categoria').replace(['', 'nan', 'NaN'], 'Sem Categoria')
    df = df[df['product_category_name'] != 'Sem Categoria']

    if df.empty:
        return create_empty_chart("Sem dados de categoria identificados")

    cancellation_rate = (df.groupby('product_category_name')['pedido_cancelado'].mean() * 100).round(1).sort_values(ascending=False).head(10)
    categories = cancellation_rate.index.tolist()

    sku_col = "product_sku" if "product_sku" in df.columns else ("product_id" if "product_id" in df.columns else None)
    max_skus_in_tooltip = 5  # Top 5 mais cancelados para não cortar o tooltip

    series_data: List[Dict[str, Any]] = []
    for cat in categories:
        rate_val = float(cancellation_rate[cat])
        skus_list: List[Dict[str, Any]] = []
        if sku_col and sku_col in df.columns:
            cat_df = df[df["product_category_name"] == cat]
            if not cat_df.empty:
                sku_rates = (cat_df.groupby(sku_col)["pedido_cancelado"].mean() * 100).round(1)
                sku_rates = sku_rates[sku_rates > 0].sort_values(ascending=False).head(max_skus_in_tooltip)  # 5 mais cancelados, só > 0%
                for sku, r in sku_rates.items():
                    skus_list.append({"sku": str(sku).strip() or "(sem SKU)", "rate": float(r)})
        series_data.append({"value": rate_val, "skus": skus_list})

    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": (
                r"function(params) {"
                r" var p = params[0]; var n = p.name; var v = p.value; var data = p.data;"
                r" var html = '<div style=\"font-weight:600;color:#f8fafc;margin-bottom:6px\">' + n + '</div>';"
                r" html += '<div style=\"font-size:15px;font-weight:700;color:#f87171;margin-bottom:8px\">Taxa de Cancelamento: ' + (typeof v === 'number' ? v.toFixed(1) : (data && data.value != null ? data.value.toFixed(1) : '')) + '%</div>';"
                r" if (data && data.skus && data.skus.length > 0) {"
                r"   html += '<div style=\"font-size:12px;color:#94a3b8;margin-top:6px;border-top:1px solid rgba(148,163,184,0.3);padding-top:6px\">Por SKU:</div>';"
                r"   for (var i = 0; i < data.skus.length; i++) {"
                r"     var s = data.skus[i]; var r = s.rate != null ? s.rate.toFixed(1) : '-';"
                r"     html += '<div style=\"font-size:11px;color:#cbd5e1;margin-left:4px;margin-top:2px\">' + (s.sku || '') + ': <span style=\"color:#f87171\">' + r + '%</span></div>';"
                r"   }"
                r" }"
                r" return html;"
                r"}"
            ),
        },
        "grid": {
            "left": "10%",
            "right": "4%",
            "bottom": "15%",
            "top": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": categories,
            "axisLabel": {
                "rotate": 45,
                "overflow": "break"
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Taxa de Cancelamento (%)",
            "nameLocation": "middle",
            "nameGap": 50,
            "axisLabel": {
                "formatter": r"function(value) { return value.toFixed(1) + '%'; }"
            }
        },
        "series": [{
            "name": "Taxa de Cancelamento",
            "type": "bar",
            "data": series_data,
            "itemStyle": {
                "color": "#d62728",
                "opacity": 0.9
            },
        }]
    }
    return option

def create_empty_chart(message: str = "Nenhum dado disponível") -> Dict[str, Any]:
    """Cria um gráfico vazio com uma mensagem."""
    return {
        "title": {
            "text": message,
            "left": "center",
            "top": "center",
            "textStyle": {
                "fontSize": 16,
                "fontWeight": "normal"
            }
        },
        "xAxis": {"show": False},
        "yAxis": {"show": False},
        "series": []
    }


def _get_comparativo_bar_style() -> tuple:
    """Retorna gradientes e estilos das barras Jan (azul) e Pós (verde) no padrão aquisição/retenção."""
    jan_gradient = {
        "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
        "colorStops": [
            {"offset": 0, "color": "rgba(59,130,246,0.95)"},
            {"offset": 1, "color": "rgba(99,102,241,0.70)"},
        ],
    }
    pos_gradient = {
        "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
        "colorStops": [
            {"offset": 0, "color": "rgba(16,185,129,0.95)"},
            {"offset": 1, "color": "rgba(34,197,94,0.75)"},
        ],
    }
    bar_style_jan = {
        "color": jan_gradient,
        "borderColor": "rgba(255,255,255,0.08)",
        "borderWidth": 1,
        "shadowBlur": 12,
        "shadowColor": "rgba(59,130,246,0.25)",
        "shadowOffsetY": 2,
        "borderRadius": [8, 8, 0, 0],
    }
    bar_style_pos = {
        "color": pos_gradient,
        "borderColor": "rgba(255,255,255,0.08)",
        "borderWidth": 1,
        "shadowBlur": 12,
        "shadowColor": "rgba(16,185,129,0.25)",
        "shadowOffsetY": 2,
        "borderRadius": [8, 8, 0, 0],
    }
    return bar_style_jan, bar_style_pos


def create_comparativo_categoria_chart(
    df_cat: pd.DataFrame,
    metric: str = "receita",
) -> Dict[str, Any]:
    """
    Cria gráfico de barras agrupadas (Jan vs Pós) para categorias, no estilo aquisição/retenção.
    metric: 'receita' | 'unidades'
    """
    if df_cat.empty or "categoria" not in df_cat.columns:
        return create_empty_chart("Nenhum dado de categorias disponível")
    col_jan = "receita_jan" if metric == "receita" else "unidades_jan"
    col_pos = "receita_pos" if metric == "receita" else "unidades_pos"
    if col_jan not in df_cat.columns or col_pos not in df_cat.columns:
        return create_empty_chart("Colunas de métrica não encontradas")
    df_cat = df_cat.copy()
    df_cat["categoria"] = df_cat["categoria"].fillna("Sem Categoria").astype(str)
    df_cat = df_cat[df_cat["categoria"] != ""]
    if df_cat.empty:
        return create_empty_chart("Sem dados de categorias")
    df_cat = df_cat.assign(_total=lambda x: x[col_jan].fillna(0) + x[col_pos].fillna(0))
    sorted_df = df_cat.nlargest(12, "_total")
    categories = sorted_df["categoria"].tolist()
    data_jan_raw = sorted_df[col_jan].fillna(0).tolist()
    data_pos = sorted_df[col_pos].fillna(0).tolist()
    scores = sorted_df["composite_score"].round(3).tolist() if "composite_score" in sorted_df.columns else [None] * len(categories)
    def _make_point(val: float, score: Any) -> Any:
        if score is None:
            return val
        try:
            s = float(score)
            if np.isnan(s):
                return val
            return {"value": val, "composite_score": round(s, 3)}
        except (TypeError, ValueError):
            return val

    data_jan = [_make_point(v, sc) for v, sc in zip(data_jan_raw, scores)]
    bar_style_jan, bar_style_pos = _get_comparativo_bar_style()
    y_name = "Receita (R$)" if metric == "receita" else "Unidades"
    fmt_js = (
        r"function(value){ return 'R$ ' + value.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); }"
        if metric == "receita"
        else r"function(value){ return value.toLocaleString('pt-BR',{maximumFractionDigits:0}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); }"
    )
    tooltip_fmt = (
        r"function(params){"
        r" var fmt=function(v){ return (typeof v==='number') ? "
        + (r"('R$ '+v.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'))"
           if metric == "receita"
           else r"v.toLocaleString('pt-BR',{maximumFractionDigits:0}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.')")
        + r" : '-'; };"
        r" var p0=params[0]; var val0=p0.value; if(typeof val0==='object'&&val0!=null) val0=val0.value;"
        r" var d=p0.data; var score=(d&&typeof d==='object'&&d.composite_score!=null)?d.composite_score.toFixed(3):null;"
        r" var html='<div style=\"font-weight:600;color:#f8fafc;margin-bottom:8px\">'+p0.axisValue+'</div>';"
        r" if(score) html+='<div style=\"color:#a78bfa;margin-bottom:6px;font-size:12px\">Score composto: '+score+'</div>';"
        r" params.forEach(function(p){ var v=p.value; if(typeof v==='object'&&v!=null) v=v.value; var s=fmt(v);"
        r"   if(p.seriesName==='Janeiro') html+='<div style=\"color:#60a5fa;margin-top:4px\">'+p.marker+' Janeiro: '+s+'</div>';"
        r"   else html+='<div style=\"color:#34d399;margin-top:4px\">'+p.marker+' Pós decisões: '+s+'</div>';"
        r" }); return html;"
        r"}"
    )
    option = {
        "title": {"text": " ", "left": "center"},
        "animation": True,
        "animationDuration": 900,
        "animationEasing": "cubicOut",
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": tooltip_fmt,
        },
        "legend": {
            "data": ["Janeiro", "Pós decisões"],
            "top": 10,
            "textStyle": {"color": "#e2e8f0"},
            "icon": "roundRect",
            "itemWidth": 14,
            "itemHeight": 10,
        },
        "grid": {"left": "10%", "right": "6%", "bottom": "20%", "top": "22%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": categories,
            "axisLabel": {"rotate": 45, "overflow": "break", "color": "#cbd5e1"},
            "axisLine": {"lineStyle": {"color": "rgba(148,163,184,0.35)"}},
            "axisTick": {"show": False},
        },
        "yAxis": {
            "type": "value",
            "name": y_name,
            "nameTextStyle": {"color": "#94a3b8"},
            "axisLabel": {"color": "#cbd5e1", "formatter": fmt_js},
            "axisLine": {"show": False},
            "splitLine": {"show": True, "lineStyle": {"color": "rgba(148,163,184,0.12)"}},
        },
        "series": [
            {
                "name": "Janeiro",
                "type": "bar",
                "data": data_jan,
                "barWidth": 20,
                "barCategoryGap": "55%",
                "itemStyle": bar_style_jan,
                "emphasis": {"focus": "series", "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(99,102,241,0.35)"}},
            },
            {
                "name": "Pós decisões",
                "type": "bar",
                "data": data_pos,
                "barWidth": 20,
                "barGap": "30%",
                "itemStyle": bar_style_pos,
                "emphasis": {"focus": "series", "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(16,185,129,0.35)"}},
            },
        ],
    }
    return option


def create_comparativo_sku_chart(
    df: pd.DataFrame,
    top_n: int = 15,
    metric: str = "receita",
) -> Dict[str, Any]:
    """
    Cria gráfico de barras horizontais agrupadas (Jan vs Pós) para top SKUs.
    metric: 'receita' | 'unidades'
    """
    if df.empty or "sku" not in df.columns:
        return create_empty_chart("Nenhum dado de SKUs disponível")
    col_jan = "receita_jan" if metric == "receita" else "unidades_jan"
    col_pos = "receita_pos" if metric == "receita" else "unidades_pos"
    if col_jan not in df.columns or col_pos not in df.columns:
        return create_empty_chart("Colunas de métrica não encontradas")
    col_diff = "diff_receita" if metric == "receita" else "diff_unidades"
    top_df = df.nlargest(top_n, col_diff)
    labels = top_df.apply(lambda r: f"{r['sku']} ({r.get('categoria', '')})", axis=1).tolist()
    data_jan_raw = top_df[col_jan].fillna(0).tolist()
    data_pos = top_df[col_pos].fillna(0).tolist()
    scores = top_df["composite_score"].round(3).tolist() if "composite_score" in top_df.columns else [None] * len(labels)
    data_jan = []
    for v, sc in zip(data_jan_raw, scores):
        if sc is not None:
            try:
                s = float(sc)
                if not np.isnan(s):
                    data_jan.append({"value": v, "composite_score": round(s, 3)})
                else:
                    data_jan.append(v)
            except (TypeError, ValueError):
                data_jan.append(v)
        else:
            data_jan.append(v)
    bar_style_jan, bar_style_pos = _get_comparativo_bar_style()
    y_name = "Receita (R$)" if metric == "receita" else "Unidades"
    fmt_js = (
        r"function(v){ return 'R$ ' + v.toLocaleString('pt-BR',{maximumFractionDigits:0}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); }"
        if metric == "receita"
        else r"function(v){ return v.toLocaleString('pt-BR',{maximumFractionDigits:0}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'); }"
    )
    tooltip_fmt = (
        r"function(params){"
        r" var fmt=function(v){ return (typeof v==='number') ? "
        + (r"('R$ '+v.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.'))"
           if metric == "receita"
           else r"v.toLocaleString('pt-BR',{maximumFractionDigits:0}).replace(/,/g,'#').replace(/\./g,',').replace(/#/g,'.')")
        + r" : '-'; };"
        r" var p0=params[0]; var d=p0.data; var score=(d&&typeof d==='object'&&d.composite_score!=null)?d.composite_score.toFixed(3):null;"
        r" var html='<div style=\"font-weight:600;color:#f8fafc;margin-bottom:8px\">'+p0.axisValue+'</div>';"
        r" if(score) html+='<div style=\"color:#a78bfa;margin-bottom:6px;font-size:12px\">Score composto: '+score+'</div>';"
        r" params.forEach(function(p){ var v=p.value; if(typeof v==='object'&&v!=null) v=v.value; var s=fmt(v);"
        r"   if(p.seriesName==='Janeiro') html+='<div style=\"color:#60a5fa;margin-top:4px\">'+p.marker+' Janeiro: '+s+'</div>';"
        r"   else html+='<div style=\"color:#34d399;margin-top:4px\">'+p.marker+' Pós decisões: '+s+'</div>';"
        r" }); return html;"
        r"}"
    )
    option = {
        "title": {"text": " ", "left": "center"},
        "animation": True,
        "animationDuration": 900,
        "animationEasing": "cubicOut",
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": tooltip_fmt,
        },
        "legend": {
            "data": ["Janeiro", "Pós decisões"],
            "top": 5,
            "textStyle": {"color": "#e2e8f0"},
            "icon": "roundRect",
            "itemWidth": 14,
            "itemHeight": 10,
        },
        "grid": {"left": "25%", "right": "12%", "bottom": "8%", "top": "18%", "containLabel": True},
        "xAxis": {
            "type": "value",
            "name": y_name,
            "nameTextStyle": {"color": "#94a3b8"},
            "axisLabel": {"color": "#cbd5e1", "formatter": fmt_js},
            "axisLine": {"show": False},
            "splitLine": {"show": True, "lineStyle": {"color": "rgba(148,163,184,0.12)"}},
        },
        "yAxis": {
            "type": "category",
            "data": labels,
            "axisLabel": {"overflow": "truncate", "width": 140, "color": "#cbd5e1"},
            "axisLine": {"lineStyle": {"color": "rgba(148,163,184,0.35)"}},
            "axisTick": {"show": False},
        },
        "series": [
            {
                "name": "Janeiro",
                "type": "bar",
                "data": data_jan,
                "barWidth": 14,
                "barCategoryGap": "55%",
                "itemStyle": bar_style_jan,
                "emphasis": {"focus": "series", "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(99,102,241,0.35)"}},
            },
            {
                "name": "Pós decisões",
                "type": "bar",
                "data": data_pos,
                "barWidth": 14,
                "barGap": "30%",
                "itemStyle": bar_style_pos,
                "emphasis": {"focus": "series", "itemStyle": {"shadowBlur": 18, "shadowColor": "rgba(16,185,129,0.35)"}},
            },
        ],
    }
    return option


def create_price_volume_scatter_chart(product_metrics: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico de dispersão mostrando a relação entre preço e volume de vendas.
    
    Args:
        product_metrics: DataFrame com métricas dos produtos
            com colunas 'avg_price', 'total_sales', 'total_revenue', 'category', 'avg_rating', 'composite_score'
    
    Returns:
        Dict com a configuração do gráfico ECharts
    """
    # Preparar os dados em formato de dicionário para melhor acesso no tooltip
    data = []
    for _, row in product_metrics.iterrows():
        data.append({
            'value': [
                float(row['avg_price']),
                int(row['total_sales']),
                float(row['total_revenue']),
                float(row['avg_rating']),
                float(row['composite_score'])
            ],
            'name': str(row['category']),
            'itemStyle': {
                'color': 'auto'
            }
        })
    
    # Calcular o tamanho dos círculos baseado no composite_score
    max_score = product_metrics['composite_score'].max()
    for item in data:
        values= cast(Sequence[float], item['value'])
        score = float(values[4])
        item['symbolSize'] = ((score / max_score) * 50) + 10
    
    option = {
        "title": {
            "text": " ",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "item",
            "formatter": r"{b}<br/>" +
                       "Preço Médio: R$ {c0}<br/>" +
                       "Total de Vendas: {c1}<br/>" +
                       "Receita Total: R$ {c2}<br/>" +
                       "Avaliação Média: {c3}<br/>" +
                       "Score Composto: {c4:.2f}"
        },
        "grid": {
            "left": "10%",
            "right": "15%",
            "top": "10%",
            "bottom": "15%"
        },
        "xAxis": {
            "type": "value",
            "name": "Preço Médio (R$)",
            "nameLocation": "middle",
            "nameGap": 30,
            "axisLabel": {
                "formatter": r"R$ {value}"
            },
            "splitLine": {
                "show": True,
                "lineStyle": {
                    "color": "lightgray",
                    "width": 1
                }
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Total de Vendas",
            "nameLocation": "middle",
            "nameGap": 50,
            "splitLine": {
                "show": True,
                "lineStyle": {
                    "color": "lightgray",
                    "width": 1
                }
            }
        },
        "visualMap": {
            "type": "continuous",
            "min": float(product_metrics['avg_rating'].min()),
            "max": float(product_metrics['avg_rating'].max()),
            "text": ["Alta Avaliação", "Baixa Avaliação"],
            "inRange": {
                "color": ["#ef5350", "#ffeb3b", "#2196f3"]
            },
            "calculable": True,
            "dimension": 3,
            "orient": "vertical",
            "right": 0,
            "top": "center"
        },
        "series": [{
            "type": "scatter",
            "data": data,
            "itemStyle": {
                "opacity": 0.8,
                "borderColor": "#666",
                "borderWidth": 1
            },
            "emphasis": {
                "itemStyle": {
                    "borderColor": "#333",
                    "borderWidth": 2,
                    "opacity": 1
                }
            }
        }],
        "graphic": [{
            "type": "text",
            "left": "center",
            "top": "bottom",
            "style": {
                "text": "Score Composto: Métrica que combina receita (40%), volume de vendas (30%) e satisfação do cliente (30%) para avaliar o desempenho geral de cada categoria.",
                "fontSize": 12,
                "fill": "#666",
                "textAlign": "center",
                "textVerticalAlign": "middle",
                "padding": [10, 10, 10, 10],
                "backgroundColor": "rgba(255, 255, 255, 0.8)",
                "borderColor": "rgba(0, 0, 0, 0.1)",
                "borderWidth": 1,
                "borderRadius": 4
            }
        }]
    }
    
    return option

def create_revenue_forecast_echart(daily_revenue: pd.DataFrame, forecast_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Cria um gráfico ECharts mostrando a evolução histórica e previsão de receita.

    A série histórica deve usar a MESMA regra do gráfico de evolução da receita:
    valorTotal por pedido (1 valor por order_id) quando existir, senão price+freight;
    excluir cancelados. Assim os valores diários batem com create_revenue_chart (visão geral).
    
    Args:
        daily_revenue (pd.DataFrame): DataFrame contendo os dados históricos de receita
            com colunas 'date' e 'price'
        forecast_df (pd.DataFrame): DataFrame contendo os dados de previsão
            com colunas 'date', 'forecast', 'lower_bound', 'upper_bound'
    
    Returns:
        Dict[str, Any]: Configuração do gráfico ECharts
    """
    # Normalizar datas como datetime para comparação robusta (evitar tz-aware vs naive)
    dr = daily_revenue.copy()
    fc = forecast_df.copy()
    dr['date'] = pd.to_datetime(dr['date'], errors='coerce')
    fc['date'] = pd.to_datetime(fc['date'], errors='coerce')
    for _df in (dr, fc):
        if getattr(_df['date'].dtype, 'tz', None) is not None:
            _df['date'] = _df['date'].dt.tz_convert('UTC').dt.tz_localize(None)
    dr = dr.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
    fc = fc.dropna(subset=['date']).sort_values('date').reset_index(drop=True)

    # Garantir que as datas de previsão comecem após a última data dos dados históricos
    last_historical_date = dr['date'].max() if not dr.empty else None
    if last_historical_date is None:
        last_historical_date = fc['date'].min() - pd.Timedelta(days=1)

    forecast_start_index = int((fc['date'] > last_historical_date).to_numpy().argmax()) if (fc['date'] > last_historical_date).any() else int(len(fc))
    forecast_dates_after = fc['date'].iloc[forecast_start_index:].tolist()

    # Eixo X: datas históricas + datas de previsão (apenas após o histórico)
    x_axis_dt = dr['date'].tolist() + forecast_dates_after
    x_axis = [d.strftime('%d/%m/%Y') for d in x_axis_dt]

    # Preencher listas de previsão e intervalo de confiança com None até o início da previsão
    n_hist = len(dr['date'])
    n_forecast = len(forecast_dates_after)
    
    # Limpar dados de previsão para JSON seguro e arredondar para 2 casas decimais
    forecast_clean = pd.to_numeric(fc['forecast'], errors='coerce').fillna(0).round(2).tolist()[forecast_start_index:]
    upper_clean = pd.to_numeric(fc['upper_bound'], errors='coerce').fillna(0).round(2).tolist()[forecast_start_index:]
    lower_clean = pd.to_numeric(fc['lower_bound'], errors='coerce').fillna(0).round(2).tolist()[forecast_start_index:]
    
    forecast_values = [None] * n_hist + forecast_clean
    upper_values = [None] * n_hist + upper_clean
    lower_values = [None] * n_hist + lower_clean

    # Coletar todos os valores válidos para calcular eixo Y dinâmico
    # (histórico, previsão, limites superior e inferior)
    historical_values = pd.to_numeric(dr['price'], errors='coerce').fillna(0).round(2).tolist()
    all_values = historical_values + forecast_clean + upper_clean + lower_clean
    
    # Eixo Y dinâmico para destacar variações (incluindo intervalo de confiança)
    y_min, y_max = _dynamic_axis_bounds(
        all_values,
        clamp_min=0.0,
        clamp_max=None,
        pad_abs=1000.0,  # pelo menos R$ 1k de margem
        pad_ratio=0.12,
        min_range=1.0,
    )

    option = {
        "title": {
            "text": "",
            "left": "center"
        },
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            }
        },
        "legend": {
            "data": ['Receita Histórica', 'Previsão', 'Intervalo de Confiança'],
            "top": 30
        },
        "xAxis": {
            "type": "category",
            "data": x_axis,
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Receita (R$)",
            **({"min": y_min} if y_min is not None else {}),
            **({"max": y_max} if y_max is not None else {}),
        },
        "dataZoom": [
            {
                "type": "inside",
                "start": 0,
                "end": 100
            },
            {
                "type": "slider",
                "start": 0,
                "end": 100,
                "height": 20,
                "bottom": 10,
                "handleStyle": {
                    "color": "#3b82f6"
                },
                "textStyle": {
                    "color": "#64748b"
                }
            }
        ],
        "series": [
            {
                "name": "Receita Histórica",
                "type": "line",
                "data": pd.to_numeric(dr['price'], errors='coerce').fillna(0).round(2).tolist() + [None] * n_forecast,
                "smooth": True,
                "lineStyle": {
                    "width": 3,
                    "color": "#6366f1"
                },
                "itemStyle": {
                    "color": "#6366f1"
                },
                "areaStyle": {
                    "opacity": 0.3,
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 0,
                        "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "rgba(99, 102, 241, 0.4)"},
                            {"offset": 1, "color": "rgba(99, 102, 241, 0.1)"}
                        ]
                    }
                }
            },
            {
                "name": "Previsão",
                "type": "line",
                "data": forecast_values,
                "smooth": True,
                "lineStyle": {
                    "width": 3,
                    "color": "#f59e0b",
                    "type": "dashed"
                },
                "itemStyle": {
                    "color": "#f59e0b"
                },
                "areaStyle": {
                    "opacity": 0.3,
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 0,
                        "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": "rgba(245, 158, 11, 0.4)"},
                            {"offset": 1, "color": "rgba(245, 158, 11, 0.1)"}
                        ]
                    }
                }
            },
            {
                "name": "IC 95% (lower)",
                "type": "line",
                "data": lower_values,
                "smooth": True,
                "lineStyle": {
                    "width": 0
                },
                "itemStyle": {
                    "color": "rgba(245, 158, 11, 0)"
                },
                "stack": "confidence",
            }
            ,
            {
                "name": "Intervalo de Confiança",
                "type": "line",
                "data": upper_values,
                "smooth": True,
                "lineStyle": {"width": 0},
                "itemStyle": {"color": "rgba(245, 158, 11, 0)"},
                "areaStyle": {"color": "rgba(245, 158, 11, 0.15)"},
                "stack": "confidence",
            },
        ]
    }
    return option


def create_roi_timeline_echart(
    agg_revenue: pd.DataFrame,
    agg_cancel: pd.DataFrame,
    markers: List[Dict[str, str]],
    period: str = "M",
) -> Dict[str, Any]:
    """
    Timeline de receita e taxa de cancelamento com marcadores de marcos da consultoria.
    markers: lista de {"date": "YYYY-MM-DD", "label": "Texto"}.
    """
    if agg_revenue is None or agg_revenue.empty:
        return create_empty_chart("Sem dados de receita no período.")
    x_cats = _format_ts_date_labels(agg_revenue["order_purchase_timestamp"].tolist())
    revenue_vals = safe_numeric_to_list(agg_revenue["price"].round(2), round_digits=2)
    cancel_vals = []
    if agg_cancel is not None and not agg_cancel.empty:
        # Alinhar cancel por data (pode ter menos pontos)
        cancel_map = dict(zip(
            _format_ts_date_labels(agg_cancel["order_purchase_timestamp"].tolist()),
            (pd.to_numeric(agg_cancel["pedido_cancelado"], errors="coerce").fillna(0) * 100).round(2).tolist()
        ))
        cancel_vals = [cancel_map.get(x, 0) for x in x_cats]
    else:
        cancel_vals = [0] * len(x_cats)

    mark_line_data = []
    for m in markers:
        d = m.get("date", "")
        label = m.get("label", d)
        if not d:
            continue
        try:
            dt = pd.to_datetime(d)
            if period == "D":
                x_val = dt.strftime("%d/%m/%Y")
            elif period == "W":
                x_val = _format_ts_date_label(dt.to_period("W").astype(str))
            else:
                x_val = _format_ts_date_label(dt.to_period("M").astype(str))
            if x_val in x_cats:
                mark_line_data.append({"name": label, "xAxis": x_val})
        except Exception:
            continue

    y_min_r, y_max_r = _dynamic_axis_bounds(
        revenue_vals, clamp_min=0.0, pad_abs=500.0, pad_ratio=0.12, min_range=1.0
    )
    option = {
        "title": {"text": "Receita e Taxa de Cancelamento", "left": "center"},
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "cross"},
        },
        "legend": {"data": ["Receita (R$)", "Taxa Cancel. (%)"], "top": 30},
        "xAxis": {
            "type": "category",
            "data": x_cats,
            "axisLabel": {"rotate": 45},
        },
        "yAxis": [
            {
                "type": "value",
                "name": "Receita (R$)",
                "position": "left",
                **({"min": y_min_r} if y_min_r is not None else {}),
                **({"max": y_max_r} if y_max_r is not None else {}),
            },
            {
                "type": "value",
                "name": "Taxa (%)",
                "position": "right",
                "axisLabel": {"formatter": "{value}%"},
                "min": 0,
                "max": max(100, max(cancel_vals) * 1.2) if cancel_vals else 100,
            },
        ],
        "series": [
            {
                "name": "Receita (R$)",
                "type": "line",
                "data": revenue_vals,
                "smooth": True,
                "yAxisIndex": 0,
                "lineStyle": {"width": 3, "color": "#10b981"},
                "areaStyle": {"opacity": 0.25},
                **({"markLine": {"data": mark_line_data, "lineStyle": {"color": "#f59e0b", "type": "dashed"}}} if mark_line_data else {}),
            },
            {
                "name": "Taxa Cancel. (%)",
                "type": "line",
                "data": cancel_vals,
                "smooth": True,
                "yAxisIndex": 1,
                "lineStyle": {"width": 2, "color": "#ef4444"},
                "itemStyle": {"color": "#ef4444"},
            },
        ],
    }
    return option


def create_roi_comparison_echart(roi_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gráficos de barras: Receita, Ticket Médio e Taxa de Cancelamento por período (2 ou 3 períodos).
    Valores formatados com 2 casas decimais; eixos e tooltip legíveis.
    roi_data: saída de calculate_roi_insights_case_atual ou calculate_roi_insights_pos_decisoes (period_comparison).
    """
    pc = roi_data.get("period_comparison", {})
    baseline = pc.get("baseline", {})
    improvement = pc.get("improvement", {})
    growth = pc.get("growth", {})

    def _cat(p: dict, default: str) -> str:
        return (p or {}).get("period_name") or default

    categories = [_cat(baseline, "Baseline"), _cat(improvement, "Melhoria")]
    if growth and _cat(growth, ""):
        categories.append(_cat(growth, "Crescimento"))

    def _round2(x: float) -> float:
        return round(float(x), 2)

    rev = [_round2(baseline.get("revenue", 0)), _round2(improvement.get("revenue", 0))]
    ticket = [_round2(baseline.get("avg_ticket", 0)), _round2(improvement.get("avg_ticket", 0))]
    cancel = [_round2(baseline.get("cancel_rate", 0)), _round2(improvement.get("cancel_rate", 0))]
    if growth and _cat(growth, ""):
        rev.append(_round2(growth.get("revenue", 0)))
        ticket.append(_round2(growth.get("avg_ticket", 0)))
        cancel.append(_round2(growth.get("cancel_rate", 0)))

    # Tooltip: 2 decimais; Receita e Ticket em R$, Taxa em %
    tooltip_formatter = (
        r"function(params){ "
        r"var r2=function(v){ return typeof v==='number' ? v.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}) : v; }; "
        r"var h='<div style=\"font-weight:600;margin-bottom:6px\">'+params[0].axisValue+'</div>'; "
        r"params.forEach(function(p){ "
        r"var s=p.seriesName, v=p.value; "
        r"if(s.indexOf('Taxa')>=0) h += p.marker + ' ' + s + ': <strong>' + (typeof v==='number' ? v.toFixed(2) : v) + '%</strong><br/>'; "
        r"else h += p.marker + ' ' + s + ': <strong>R$ ' + r2(v) + '</strong><br/>'; "
        r"}); return h; }"
    )

    n_grid = 3
    option = {
        "title": {"text": "Comparação por Período", "left": "center"},
        "tooltip": {
            **_GLASS_TOOLTIP_STYLE,
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "formatter": tooltip_formatter,
        },
        "grid": [
            {"left": "12%", "right": "10%", "top": "12%", "height": "26%", "containLabel": True},
            {"left": "12%", "right": "10%", "top": "42%", "height": "26%", "containLabel": True},
            {"left": "12%", "right": "10%", "top": "72%", "height": "26%", "containLabel": True},
        ],
        "xAxis": [
            {"type": "category", "data": categories, "gridIndex": 0, "axisLabel": {"rotate": 25, "fontSize": 12}},
            {"type": "category", "data": categories, "gridIndex": 1, "axisLabel": {"rotate": 25, "fontSize": 12}},
            {"type": "category", "data": categories, "gridIndex": 2, "axisLabel": {"rotate": 25, "fontSize": 12}},
        ],
        "yAxis": [
            {
                "type": "value",
                "name": "Receita (R$)",
                "gridIndex": 0,
                "nameGap": 50,
                "axisLabel": {"fontSize": 12, "formatter": r"function(v){ var s=v.toLocaleString('pt-BR',{minimumFractionDigits:0,maximumFractionDigits:0}); return (s+'').replace(/\u00A0/g,' '); }"},
            },
            {
                "type": "value",
                "name": "Ticket (R$)",
                "gridIndex": 1,
                "nameGap": 50,
                "axisLabel": {"fontSize": 12, "formatter": r"function(v){ var s=v.toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}); return (s+'').replace(/\u00A0/g,' '); }"},
            },
            {
                "type": "value",
                "name": "Taxa (%)",
                "gridIndex": 2,
                "nameGap": 40,
                "axisLabel": {"fontSize": 12, "formatter": r"function(v){ return (typeof v==='number' ? v.toFixed(1) : v) + '%'; }"},
            },
        ],
        "series": [
            {"name": "Receita (R$)", "type": "bar", "data": rev, "xAxisIndex": 0, "yAxisIndex": 0, "itemStyle": {"color": "#06b6d4"}},
            {"name": "Ticket Médio (R$)", "type": "bar", "data": ticket, "xAxisIndex": 1, "yAxisIndex": 1, "itemStyle": {"color": "#10b981"}},
            {"name": "Taxa Cancel. (%)", "type": "bar", "data": cancel, "xAxisIndex": 2, "yAxisIndex": 2, "itemStyle": {"color": "#ef4444"}},
        ],
    }
    return option


def _apply_chart_caching():
    for name, func in list(globals().items()):
        if not name.startswith("create_") or not callable(func):
            continue
        if getattr(func, "__wrapped__", None) is not None:
            continue
        globals()[name] = _cached_chart(func)

_apply_chart_caching()

def render_executive_forecast_calendar(forecast_df: pd.DataFrame) -> None:
    """
    Renderiza um calendário executivo moderno mostrando as previsões dia a dia.
    Design inspirado em dashboards executivos com tema dark elegante.
    
    Args:
        forecast_df (pd.DataFrame): DataFrame contendo os dados de previsão
            com colunas 'date' e 'forecast'
    """
    import calendar
    from datetime import datetime
    import holidays
    
    # Preparar dados
    forecast_copy = forecast_df.copy()
    forecast_copy['date'] = pd.to_datetime(forecast_copy['date'])

    # Calcular min/max para escala de cores
    min_value = float(forecast_copy['forecast'].min())
    max_value = float(forecast_copy['forecast'].max())

    # Agrupar por mês
    forecast_copy['year_month'] = forecast_copy['date'].dt.to_period('M')
    months = sorted(forecast_copy['year_month'].unique())
    if not months:
        st.info("Nenhuma previsão disponível para exibir no calendário.")
        return

    # Controle de navegação entre meses
    state_key = "executive_calendar_month_idx"
    st.session_state.setdefault(state_key, 0)
    month_idx = st.session_state[state_key]
    # Garantir que o índice atual esteja dentro dos limites
    if month_idx < 0 or month_idx >= len(months):
        month_idx = 0
        st.session_state[state_key] = month_idx

    current_month_period = months[month_idx]
    forecast_copy = forecast_copy[forecast_copy['year_month'] == current_month_period]

    # Definir semana começando no domingo (alinha dias corretamente)
    calendar.setfirstweekday(calendar.SUNDAY)

    # CSS customizado para o calendário executivo
    calendar_css = dedent("""
    <style>
    .executive-calendar {
        background: linear-gradient(135deg, #0D1117 0%, #121A28 100%);
        border-radius: 16px;
        padding: 24px;
        margin: 24px auto 8px auto;  /* margem inferior menor para aproximar os controles */
        box-shadow: 0 12px 32px rgba(0, 0, 0, 0.45);
        width: calc(100% - 14px);
        max-width: 880px;
        border: 1px solid rgba(148, 163, 184, 0.15);
    }

    .calendar-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 24px;
        padding-bottom: 16px;
        border-bottom: 1px solid rgba(148, 163, 184, 0.2);
        position: relative;
    }

    .calendar-title {
        font-size: 22px;
        font-weight: 600;
        color: #E6E8EB;
        letter-spacing: -0.3px;
    }

    .calendar-best-day {
        font-size: 14px;
        font-weight: 500;
        color: #9BA6B5;
        text-align: right;
    }

    .calendar-grid {
        display: grid;
        grid-template-columns: repeat(7, minmax(72px, 1fr));
        gap: 10px;
        margin-top: 12px;
    }

    .calendar-weekday {
        text-align: center;
        font-size: 11px;
        font-weight: 600;
        color: #8B949E;
        text-transform: uppercase;
        letter-spacing: 0.4px;
        padding: 6px 0;
    }

    .calendar-day {
        height: 88px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        background: #1E2533;
        border-radius: 10px;
        border: 2px solid transparent;
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
        position: relative;
    }

    .calendar-day-markers {
        position: absolute;
        top: 10px;
        right: 10px;
        display: flex;
        gap: 6px;
        z-index: 6;
        pointer-events: none;
    }

    .calendar-marker {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.18);
        box-shadow: 0 0 0 2px rgba(0,0,0,0.15);
    }

    .calendar-marker-holiday {
        background: rgba(245, 158, 11, 0.85); /* amber */
    }

    .calendar-marker-double {
        background: rgba(99, 102, 241, 0.85); /* indigo */
    }

    .calendar-marker-blackfriday {
        background: rgba(16, 185, 129, 0.85); /* emerald */
    }

    .calendar-day:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 24px rgba(66, 165, 245, 0.2);
        border-color: rgba(66, 165, 245, 0.55);
        z-index: 5;
    }

    .calendar-day-number {
        font-size: 18px;
        font-weight: 600;
        color: #E6E8EB;
        margin-bottom: 6px;
    }

    .calendar-day-value {
        font-size: 11px;
        font-weight: 500;
        color: #9BA6B5;
    }

    .calendar-day-empty {
        background: rgba(30, 37, 51, 0.35);
        border: 1px dashed rgba(148, 163, 184, 0.18);
        cursor: default;
    }

    .calendar-day-empty:hover {
        transform: none;
        box-shadow: none;
        border-color: rgba(148, 163, 184, 0.18);
    }

    .intensity-very-high {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.38) 0%, rgba(16, 185, 129, 0.18) 100%);
        border-color: rgba(16, 185, 129, 0.6);
    }

    .intensity-high {
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.38) 0%, rgba(59, 130, 246, 0.18) 100%);
        border-color: rgba(59, 130, 246, 0.6);
    }

    .intensity-medium {
        background: linear-gradient(135deg, rgba(245, 158, 11, 0.38) 0%, rgba(245, 158, 11, 0.18) 100%);
        border-color: rgba(245, 158, 11, 0.6);
    }

    .intensity-low {
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.38) 0%, rgba(239, 68, 68, 0.18) 100%);
        border-color: rgba(239, 68, 68, 0.6);
    }

    .intensity-very-low {
        background: linear-gradient(135deg, rgba(100, 116, 139, 0.38) 0%, rgba(100, 116, 139, 0.18) 100%);
        border-color: rgba(100, 116, 139, 0.6);
    }

    .calendar-legend {
        display: flex;
        justify-content: center;
        gap: 18px;
        margin-top: 24px;
        padding-top: 16px;
        border-top: 1px solid rgba(148, 163, 184, 0.18);
        flex-wrap: wrap;
    }

    .legend-item {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 12px;
        color: #8B949E;
    }

    .legend-color {
        width: 18px;
        height: 18px;
        border-radius: 6px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }

    @media (max-width: 1400px) {
        .executive-calendar {
            padding: 22px;
            margin: 20px auto;
        }
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(64px, 1fr));
            gap: 9px;
        }
        .calendar-day {
            height: 82px;
        }
    }

    @media (max-width: 1200px) {
        .executive-calendar {
            padding: 20px;
            margin: 18px auto;
        }
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(60px, 1fr));
            gap: 8px;
        }
        .calendar-day {
            height: 76px;
        }
    }

    @media (max-width: 1100px) {
        .executive-calendar {
            padding: 22px;
            margin: 18px auto;
        }
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(56px, 1fr));
            gap: 8px;
        }
        .calendar-day {
            height: 72px;
        }
    }

    @media (max-width: 950px) {
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(50px, 1fr));
            gap: 7px;
        }
        .calendar-day {
            height: 68px;
        }
        .calendar-day-number {
            font-size: 15px;
        }
        .calendar-day-value {
            font-size: 10px;
        }
    }

    @media (max-width: 820px) {
        .executive-calendar {
            padding: 18px;
            margin: 14px auto;
        }
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(46px, 1fr));
            gap: 6px;
        }
        .calendar-day {
            height: 64px;
        }
        .calendar-day-number {
            font-size: 14px;
        }
        .calendar-day-value {
            font-size: 9px;
        }
    }

    @media (max-width: 680px) {
        .executive-calendar {
            padding: 16px;
            margin: 12px auto;
        }
        .calendar-grid {
            grid-template-columns: repeat(7, minmax(42px, 1fr));
            gap: 5px;
        }
        .calendar-day {
            height: 60px;
        }
        .calendar-day-number {
            font-size: 13px;
        }
        .calendar-day-value {
            font-size: 8px;
        }
        .calendar-legend {
            justify-content: center;
            gap: 10px;
        }
    }
    </style>
    """)
    
    st.markdown(calendar_css, unsafe_allow_html=True)
    
    # Função para determinar a classe de intensidade
    def get_intensity_class(value):
        if value >= max_value * 0.8:
            return "intensity-very-high"
        elif value >= max_value * 0.6:
            return "intensity-high"
        elif value >= max_value * 0.4:
            return "intensity-medium"
        elif value >= max_value * 0.2:
            return "intensity-low"
        else:
            return "intensity-very-low"
    
    # Renderizar mês selecionado
    month_data = forecast_copy
    if month_data.empty:
        st.info("Nenhuma previsão para este mês.")
        return

    # Informações do mês
    first_date = month_data['date'].min()
    year = first_date.year
    month = first_date.month
    month_name = calendar.month_name[month]

    # Feriados BR (dinâmicos)
    # Usa apenas o ano corrente do calendário exibido
    br_holidays = holidays.country_holidays("BR", years=[int(year)])

    # Dia com maior previsão
    best_day_text = "-"
    if 'forecast' in month_data.columns and not month_data.empty:
        max_idx = month_data['forecast'].idxmax()
        best_day = month_data.loc[max_idx, 'date']
        best_day_text = best_day.strftime("%d/%m/%Y")

    # Criar calendário do mês
    cal = calendar.monthcalendar(year, month)

    # Detectar colunas de intervalo de confiança
    lower_col = next((c for c in ['lower', 'lower_conf', 'forecast_lower', 'lower_ci', 'ci_lower'] if c in month_data.columns), None)
    upper_col = next((c for c in ['upper', 'upper_conf', 'forecast_upper', 'upper_ci', 'ci_upper'] if c in month_data.columns), None)

    parts: List[str] = []
    parts.append('<div class="executive-calendar">')
    parts.append('  <div class="calendar-header">')
    parts.append(f'    <div class="calendar-title">📅 {month_name} {year}</div>')
    parts.append(f'    <div class="calendar-best-day">Dia com Maior Receita Prevista: {best_day_text}</div>')
    parts.append('  </div>')
    parts.append('  <div class="calendar-grid">')
    for weekday in ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]:
        parts.append(f'    <div class="calendar-weekday">{weekday}</div>')

    for week in cal:
        for day in week:
            if day == 0:
                parts.append('    <div class="calendar-day calendar-day-empty"></div>')
            else:
                day_date = datetime(year, month, day)
                day_data = month_data[month_data['date'].dt.date == day_date.date()]
                if not day_data.empty:
                    value = float(day_data.iloc[0]['forecast'])
                    intensity_class = get_intensity_class(value)
                    value_formatted = f"R$ {value:,.0f}".replace(",", ".")

                    # Montar tooltip com intervalo de confiança quando disponível
                    tooltip = f"{day_date.strftime('%d/%m/%Y')}: {value_formatted}"
                    if lower_col and upper_col:
                        lower_val = float(day_data.iloc[0][lower_col])
                        upper_val = float(day_data.iloc[0][upper_col])
                        lower_fmt = f"R$ {lower_val:,.0f}".replace(",", ".")
                        upper_fmt = f"R$ {upper_val:,.0f}".replace(",", ".")
                        tooltip = f"{tooltip} | IC: {lower_fmt} a {upper_fmt}"

                    # Flags de calendário (feriados e datas especiais)
                    is_holiday = day_date.date() in br_holidays
                    holiday_name = br_holidays.get(day_date.date()) if is_holiday else None
                    is_double_date = (day_date.month == day_date.day)
                    is_black_friday = (day_date.month == 11 and day_date.weekday() == 4 and 23 <= day_date.day <= 29)

                    flags: List[str] = []
                    if is_holiday:
                        flags.append(f"Feriado: {holiday_name}" if holiday_name else "Feriado")
                    if is_double_date:
                        flags.append("Data dupla")
                    if is_black_friday:
                        flags.append("Black Friday")
                    if flags:
                        tooltip = f"{tooltip} | " + " • ".join(flags)

                    parts.append(
                        f'    <div class="calendar-day {intensity_class}" title="{tooltip}">'
                    )
                    # Marcadores visuais
                    markers_html: List[str] = []
                    if is_holiday:
                        markers_html.append('<span class="calendar-marker calendar-marker-holiday"></span>')
                    if is_double_date:
                        markers_html.append('<span class="calendar-marker calendar-marker-double"></span>')
                    if is_black_friday:
                        markers_html.append('<span class="calendar-marker calendar-marker-blackfriday"></span>')
                    if markers_html:
                        parts.append('      <div class="calendar-day-markers">' + "".join(markers_html) + "</div>")
                    parts.append(f'      <div class="calendar-day-number">{day}</div>')
                    parts.append(f'      <div class="calendar-day-value">{value_formatted}</div>')
                    parts.append('    </div>')
                else:
                    parts.append('    <div class="calendar-day calendar-day-empty">')
                    parts.append(f'      <div class="calendar-day-number" style="color: #4a5568;">{day}</div>')
                    parts.append('    </div>')

    parts.append('  </div>')
    parts.append('  <div class="calendar-legend">')
    parts.append('    <div class="legend-item"><div class="legend-color" style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.38) 0%, rgba(16, 185, 129, 0.18) 100%); border-color: rgba(16, 185, 129, 0.6);"></div><span>Muito Alto</span></div>')
    parts.append('    <div class="legend-item"><div class="legend-color" style="background: linear-gradient(135deg, rgba(59, 130, 246, 0.38) 0%, rgba(59, 130, 246, 0.18) 100%); border-color: rgba(59, 130, 246, 0.6);"></div><span>Alto</span></div>')
    parts.append('    <div class="legend-item"><div class="legend-color" style="background: linear-gradient(135deg, rgba(245, 158, 11, 0.38) 0%, rgba(245, 158, 11, 0.18) 100%); border-color: rgba(245, 158, 11, 0.6);"></div><span>Médio</span></div>')
    parts.append('    <div class="legend-item"><div class="legend-color" style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.38) 0%, rgba(239, 68, 68, 0.18) 100%); border-color: rgba(239, 68, 68, 0.6);"></div><span>Baixo</span></div>')
    parts.append('    <div class="legend-item"><div class="legend-color" style="background: linear-gradient(135deg, rgba(100, 116, 139, 0.38) 0%, rgba(100, 116, 139, 0.18) 100%); border-color: rgba(100, 116, 139, 0.6);"></div><span>Muito Baixo</span></div>')
    parts.append('  </div>')
    parts.append('</div>')

    calendar_html = "\n".join(parts)
    st.markdown(calendar_html, unsafe_allow_html=True)

    # Controles de navegação posicionados logo abaixo da legenda, alinhados à direita
    spacer_col, prev_col, next_col = st.columns([6, 1, 1])
    with prev_col:
        if st.button("◀", key="exec_cal_prev"):
            if st.session_state[state_key] > 0:
                st.session_state[state_key] -= 1
                st.rerun()
    with next_col:
        if st.button("▶", key="exec_cal_next"):
            if st.session_state[state_key] < len(months) - 1:
                st.session_state[state_key] += 1
                st.rerun()