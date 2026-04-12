from __future__ import annotations
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional, Callable
import os
 
try:
    import streamlit as st
except ImportError:  # pragma: no cover
    st = None
from components.glass_card import (
    render_kpi_block, 
    render_plotly_glass_card,
    render_kpi_title,
    render_page_title,
    render_analysis_title_with_stars,
    render_insight_card
)
import plotly.graph_objects as go
import numpy as np
from utils.svg_icons import get_svg_icon
from utils.theme_manager import get_theme_manager
from utils.validators import show_centered_info
from pathlib import Path
from utils.db_manager import get_db
import hashlib
from io import BytesIO
import re
import time
from utils.forecast_module.ml_ensemble_forecast import MLStockRecommendationSystem
from utils.rules import load_strategic_rules, select_prescriptive_rule
from utils.filtros import filter_reviews_by_period
from utils.config import (
    BCG_CONFIG, ABC_CURVE_CONFIG, COMPOSITE_SCORE_WEIGHTS,
    OUTLIER_CONFIG, SKU_FILTERING_CONFIG
)
from utils.abc_classifier import calculate_abc_by_performance

def _hash_path(path: Path) -> str:
    """Função customizada para hashear objetos Path."""
    return str(path.absolute())

def _hash_dataframe(df: pd.DataFrame) -> str:
    """Função customizada para hashear DataFrames baseada no conteúdo."""
    # Hash do DataFrame completo para cache mais preciso.
    # Normaliza colunas com listas/sets/dicts para evitar TypeError: unhashable type: 'list'.
    # Usa iloc para evitar retorno de DataFrame quando há colunas duplicadas.
    df_hash = df.copy()
    for idx, _ in enumerate(df_hash.columns):
        series = df_hash.iloc[:, idx]
        if series.dtype == "object":
            df_hash.iloc[:, idx] = series.map(
                lambda v: tuple(v) if isinstance(v, list)
                else tuple(sorted(v)) if isinstance(v, set)
                else tuple(sorted(v.items())) if isinstance(v, dict)
                else v
            )
    # MD5 usado apenas para cache de hash, não para segurança
    return hashlib.md5(pd.util.hash_pandas_object(df_hash, index=True).values.tobytes()).hexdigest()  # nosec B324

def _get_secret_or_env(key: str, default: Optional[str] = None) -> Optional[str]:
    if st is not None:
        try:
            if key in st.secrets:
                v = st.secrets.get(key)
                return None if v is None else str(v)
        except Exception:
            pass
    v = os.getenv(key, default)
    return None if v is None else str(v)

def _ensure_sslmode(db_url: str) -> str:
    if "sslmode=" in db_url:
        return db_url
    joiner = "&" if "?" in db_url else "?"
    return f"{db_url}{joiner}sslmode=require"

def _load_stock_movements_source() -> Tuple[pd.DataFrame, str]:
    """
    Carrega movimentações de estoque priorizando Supabase.
    Retorna (df, source) onde source é: 'supabase', 'local' ou 'none'.
    """
    db_url = _get_secret_or_env("SUPABASE_DB_URL")
    if db_url:
        try:
            import psycopg2
            query = """
                select
                    movement_date as date,
                    product_id,
                    qty,
                    origin,
                    operation_type,
                    observation
                from public.stock_movements
            """
            with psycopg2.connect(_ensure_sslmode(db_url)) as conn:
                df = pd.read_sql_query(query, conn)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df, "supabase"
        except Exception:
            pass

    # Fallback local
    sm_path = "data/raw/stock_movements.parquet"
    try:
        if os.path.exists(sm_path):
            df = pd.read_parquet(sm_path)
            return df, "local"
    except Exception:
        pass

    return pd.DataFrame(), "none"


def load_reviews_aggregated(path: Path = Path("data/processed/product_reviews_aggregated.parquet")) -> Optional[pd.DataFrame]:
    """Carrega avaliações agregadas por produto; retorna None se não existir."""
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def enrich_with_review_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimento opcional com métricas de avaliações.
    Usa product_id como chave primária e product_sku como fallback.
    """
    df_out = df.copy()

    # Garantir colunas alvo
    if "avg_review_score" not in df_out.columns:
        df_out["avg_review_score"] = np.nan
    if "review_count" not in df_out.columns:
        df_out["review_count"] = 0

    # Normalizar tipos para chaves de merge (evita int64 x object)
    if "product_id" in df_out.columns:
        df_out["product_id"] = df_out["product_id"].astype(str)
    if "product_sku" in df_out.columns:
        df_out["product_sku"] = df_out["product_sku"].astype(str)

    reviews = load_reviews_aggregated()
    if reviews is None or reviews.empty:
        return df_out

    reviews_base = reviews.copy()
    if "product_sku" not in reviews_base.columns:
        reviews_base["product_sku"] = ""

    # Normalizar tipos em reviews também
    if "product_id" in reviews_base.columns:
        reviews_base["product_id"] = reviews_base["product_id"].astype(str)
    reviews_base["product_sku"] = reviews_base["product_sku"].astype(str)

    # Merge primário por product_id
    merged = df_out.merge(
        reviews_base[["product_id", "avg_review_score", "review_count"]],
        on="product_id",
        how="left",
        suffixes=("", "_rev"),
    )

    # Fallback por SKU (apenas para linhas sem match)
    if "product_sku" in merged.columns:
        fallback = reviews_base[
            ["product_sku", "avg_review_score", "review_count"]
        ].rename(
            columns={
                "avg_review_score": "avg_review_score_sku",
                "review_count": "review_count_sku",
            }
        )
        merged = merged.merge(fallback, on="product_sku", how="left")
        merged["avg_review_score"] = merged["avg_review_score"].fillna(
            merged.get("avg_review_score_sku")
        )
        merged["review_count"] = merged["review_count"].fillna(
            merged.get("review_count_sku")
        )
        merged = merged.drop(columns=["avg_review_score_sku", "review_count_sku"])

    merged["avg_review_score"] = merged["avg_review_score"].fillna(np.nan)
    merged["review_count"] = merged["review_count"].fillna(0)

    return merged
def _get_net_revenue_series(df: pd.DataFrame) -> pd.Series:
    """
    Retorna a série de receita líquida para análises (prioriza 'margin_net_revenue').
    Fallback: usa 'price' quando a coluna específica não existir.
    """
    if 'margin_net_revenue' in df.columns:
        return pd.to_numeric(df['margin_net_revenue'], errors='coerce').fillna(0.0)
    return pd.to_numeric(df.get('price', pd.Series(dtype=float)), errors='coerce').fillna(0.0)

def _get_contribution_margin_series(df: pd.DataFrame) -> pd.Series:
    """
    Retorna a série da margem de contribuição (R$) se existir; caso contrário, zeros.
    """
    if 'contribution_margin' in df.columns:
        return pd.to_numeric(df['contribution_margin'], errors='coerce').fillna(0.0)
    return pd.Series(0.0, index=df.index)

def safe_to_datetime(series_or_value) -> pd.Series:
    """
    Converte uma série de datas ou valor individual de forma segura para datetimes sem tz.
    Retorna uma Series em UTC sem timezone para evitar erros de arithmetic op.
    """
    if isinstance(series_or_value, str):
        series = pd.Series([series_or_value])
    else:
        series = series_or_value
    
    if hasattr(series, "empty") and getattr(series, "empty", False):
        return series
    
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    if hasattr(dt, "dt") and dt.dt.tz is not None:
        try:
            dt = dt.dt.tz_convert("UTC")
        except Exception:
            pass
        dt = dt.dt.tz_localize(None)
    return dt

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_revenue_insights(df: pd.DataFrame, eligible_only: bool = False) -> Dict[str, Any]:
    """
    Calcula insights relacionados à receita. Otimizado com DuckDB.
    eligible_only: se True (padrão), considera apenas pedidos elegíveis. Se False, usa todos os pedidos (sem máscara de elegibilidade).
    """
    # Proteção para DataFrame vazio ou sem receita (aceita price ou valorTotal)
    if df.empty:
        return {
            'growth_rate': 0,
            'trend': 'N/A',
            'trend_icon': get_svg_icon("trend", size=24, color="#3b82f6"),
            'monthly_revenue': pd.DataFrame(columns=['order_purchase_timestamp', 'price'])
        }
    vt_sum = pd.to_numeric(df.get("valorTotal", 0), errors="coerce").fillna(0).sum() if "valorTotal" in df.columns else 0
    price_sum = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0).sum() if "price" in df.columns else 0
    if price_sum == 0 and vt_sum == 0:
        return {
            'growth_rate': 0,
            'trend': 'N/A',
            'trend_icon': get_svg_icon("trend", size=24, color="#3b82f6"),
            'monthly_revenue': pd.DataFrame(columns=['order_purchase_timestamp', 'price'])
        }
    
    # 1. Aplicar filtros de elegibilidade (opcional)
    if eligible_only:
        eligible_mask = df.get("pedido_cancelado", 0) == 0
        if "order_status" in df.columns:
            def _normalize_status(value):
                if pd.isna(value): return ""
                s = str(value).lower().strip()
                s = pd.Series([s]).str.replace(r"^\d+\s*-\s*", "", regex=True).iloc[0]
                s = (s.replace("ã","a").replace("á","a").replace("â","a")
                       .replace("ç","c").replace("é","e").replace("ê","e")
                       .replace("í","i").replace("ó","o").replace("ô","o")
                       .replace("ú","u"))
                return s
            status_norm = df["order_status"].apply(_normalize_status)
            approved_mask = status_norm.str.contains("aprov", na=False) | status_norm.str.contains("approved", na=False)
            transport_mask = status_norm.str.contains("transp|transit|envio|shipp", regex=True, na=False)
            delivered_mask = status_norm.str.contains("entreg|deliver", regex=True, na=False)
            pending_mask = status_norm.str.contains("aguard|pending", regex=True, na=False)
            special_mask = status_norm.str.contains("credito por troca|troca", regex=True, na=False)
            status_criteria = (approved_mask | transport_mask | delivered_mask) & (~pending_mask) & (~special_mask)
            eligible_mask = eligible_mask & status_criteria
        df_eligible = df[eligible_mask].copy()
    else:
        df_eligible = df.copy()
    
    if len(df_eligible) < 10000:
        # --- Lógica Pandas Original (Dataset Pequeno) ---
        net_revenue_series = _get_net_revenue_series(df_eligible)
        margin_series = _get_contribution_margin_series(df_eligible)

        monthly_index = safe_to_datetime(df_eligible['order_purchase_timestamp']).dt.to_period('M')
        # Contabilização apenas valorTotal (v2/pedidos): um valor por pedido, depois agrupa por mês
        if "valorTotal" in df_eligible.columns and "order_id" in df_eligible.columns:
            by_order = df_eligible.groupby("order_id").agg(
                order_purchase_timestamp=("order_purchase_timestamp", "first"),
                valorTotal=("valorTotal", "max")
            ).reset_index()
            by_order["valorTotal"] = pd.to_numeric(by_order["valorTotal"], errors="coerce").fillna(0)
            mon_idx = safe_to_datetime(by_order["order_purchase_timestamp"]).dt.to_period("M")
            monthly_revenue = by_order.groupby(mon_idx)["valorTotal"].sum().reset_index()
            monthly_revenue.rename(columns={"valorTotal": "price"}, inplace=True)
            monthly_revenue["order_purchase_timestamp"] = monthly_revenue["order_purchase_timestamp"].astype(str)
        else:
            monthly_revenue = df_eligible.groupby(monthly_index)["price"].sum().reset_index()
            monthly_revenue["order_purchase_timestamp"] = monthly_revenue["order_purchase_timestamp"].astype(str)

        monthly_net_revenue = net_revenue_series.groupby(monthly_index).sum().reset_index(name='net_revenue')
        monthly_margin = margin_series.groupby(monthly_index).sum().reset_index(name='margin')
        monthly_net_revenue.rename(columns={'order_purchase_timestamp': 'month'}, inplace=True)
        monthly_margin.rename(columns={'order_purchase_timestamp': 'month'}, inplace=True)
        
        total_net_revenue = float(net_revenue_series.sum())
        total_margin = float(margin_series.sum())
    else:
        # --- Lógica DuckDB (Dataset Grande) ---
        try:
            db = get_db()
            db.conn.register('temp_rev_insights', df_eligible)
            
            has_net = 'margin_net_revenue' in df_eligible.columns
            has_margin = 'contribution_margin' in df_eligible.columns
            use_valor_total = "valorTotal" in df_eligible.columns and "order_id" in df_eligible.columns
            net_col = "margin_net_revenue" if has_net else "price"
            margin_col = "contribution_margin" if has_margin else "0"

            if use_valor_total:
                query = f"""
                    WITH order_totals AS (
                        SELECT order_id, MIN(order_purchase_timestamp) AS ts, MAX(CAST(valorTotal AS DOUBLE)) AS vt
                        FROM temp_rev_insights
                        GROUP BY order_id
                    )
                    SELECT 
                        strftime(date_trunc('month', ts), '%Y-%m') AS month_ts,
                        SUM(vt) AS price
                    FROM order_totals
                    GROUP BY 1
                    ORDER BY 1
                """
                monthly_agg = db.query(query)
                monthly_agg["net_revenue"] = monthly_agg["price"]
                monthly_agg["margin"] = 0.0
                if has_margin:
                    margin_q = """
                        SELECT strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') AS month_ts,
                               SUM(contribution_margin) AS margin
                        FROM temp_rev_insights GROUP BY 1 ORDER BY 1
                    """
                    margin_df = db.query(margin_q)
                    monthly_agg = monthly_agg.drop(columns=["margin"]).merge(margin_df, on="month_ts", how="left")
                    monthly_agg["margin"] = monthly_agg["margin"].fillna(0)
            else:
                query = f"""
                    SELECT 
                        strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as month_ts,
                        SUM(price) as price,
                        SUM({net_col}) as net_revenue,
                        SUM({margin_col}) as margin
                    FROM temp_rev_insights
                    GROUP BY 1
                    ORDER BY 1
                """
                monthly_agg = db.query(query)
            
            monthly_revenue = monthly_agg[['month_ts', 'price']].rename(columns={'month_ts': 'order_purchase_timestamp'})
            monthly_net_revenue = monthly_agg[['month_ts', 'net_revenue']].rename(columns={'month_ts': 'month'})
            monthly_margin = monthly_agg[['month_ts', 'margin']].rename(columns={'month_ts': 'month'})
            
            # Totais
            total_net_revenue = float(monthly_agg['net_revenue'].sum())
            total_margin = float(monthly_agg['margin'].sum())
            
            db.conn.unregister('temp_rev_insights')
            
        except Exception as e:
            # Fallback em caso de erro no SQL
            print(f"Erro DuckDB Revenue: {e}")
            return calculate_revenue_insights.__wrapped__(df)

    # --- Lógica de Negócio Pós-Agregação (Comum) ---
    # Calcular crescimento usando média dos N primeiros meses vs N últimos meses (N=3 ou menor)
    if len(monthly_revenue) >= 2:
        n_months = min(3, len(monthly_revenue))
        old_mean = monthly_revenue['price'].head(n_months).mean()
        recent_mean = monthly_revenue['price'].tail(n_months).mean()
        growth_rate = ((recent_mean - old_mean) / old_mean) * 100 if old_mean > 0 else 0
        
        # Determinar tendência
        if abs(growth_rate) < 5:
            trend = "estável"
            trend_icon = get_svg_icon("minus-circle", size=24, color="#3b82f6")
        elif growth_rate > 0:
            trend = "crescimento"
            trend_icon = get_svg_icon("trend", size=24, color="#10b981")
        else:
            trend = "queda"
            trend_icon = get_svg_icon("trend", size=24, color="#ef4444")
    else:
        growth_rate = 0
        trend = "indeterminado"
        trend_icon = get_svg_icon("details", size=24, color="#6b7280")
    
    # Identificar melhor mês
    if not monthly_revenue.empty:
        best_month_idx = monthly_revenue['price'].idxmax()
        best_month = monthly_revenue.iloc[best_month_idx]
        if not monthly_margin.empty and 'margin' in monthly_margin.columns:
            # Encontrar índice do mês com maior margem
            best_margin_idx = monthly_margin['margin'].idxmax()
            best_month_margin = monthly_margin.iloc[best_margin_idx]['month']
        else:
            best_month_margin = best_month['order_purchase_timestamp']
            
        best_month_ts = best_month['order_purchase_timestamp']
        best_month_val = best_month['price']
    else:
        best_month_ts = "N/A"
        best_month_val = 0
        best_month_margin = "N/A"
    
    return {
        "growth_rate": growth_rate,
        "trend": trend,
        "trend_icon": trend_icon,
        "best_month": best_month_ts,
        "best_month_revenue": best_month_val,
        "best_month_margin": best_month_margin,
        "monthly_revenue": monthly_revenue,
        "monthly_net_revenue": monthly_net_revenue,
        "monthly_margin": monthly_margin,
        "total_net_revenue": total_net_revenue,
        "total_margin": total_margin,
    }

def calculate_satisfaction_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula insights relacionados à satisfação do cliente.
    
    Se o dataset não possuir coluna de review ou não houver
    avaliações, devolve um bloco neutro e marca has_data=False
    para que a UI exiba apenas um aviso sutil.
    """
    import numpy as np
    
    # Tentar usar reviews separados se o DF principal não tiver review_score
    # Priorizar reviews filtrados temporários (aplicados pelos mesmos filtros do filtered_df)
    fallback_reviews = None
    try:
        # Primeiro tentar reviews filtrados (se disponíveis)
        filtered_reviews_temp = st.session_state.get("_filtered_reviews_temp")
        if filtered_reviews_temp is not None and not filtered_reviews_temp.empty:
            fallback_reviews = filtered_reviews_temp
            # print(f"[DEBUG] Usando reviews filtrados temporários: {len(fallback_reviews)} linhas")
        else:
            # Fallback para reviews completos do session_state
            fallback_reviews = st.session_state.get("reviews_df")
        if fallback_reviews is not None:
            #    print(f"[DEBUG] Reviews do session_state carregadas: {len(fallback_reviews)} linhas")
            #    print("[DEBUG] session_state.reviews_df é None")
            pass
    except Exception as e:
        # print(f"[DEBUG] Erro ao acessar session_state.reviews_df: {e}")
        fallback_reviews = None

    has_inline_reviews = ("review_score" in df.columns) and (not df["review_score"].dropna().empty)
    base_reviews_df = None

    if has_inline_reviews:
    #    print("[DEBUG] Usando reviews inline do DF principal")
        base_reviews_df = df[["order_purchase_timestamp", "review_score"]].copy()
    elif fallback_reviews is not None and not fallback_reviews.empty:
    #    print(f"[DEBUG] Usando reviews fallback ({len(fallback_reviews)} linhas)")
        base_reviews_df = fallback_reviews.copy()
        # Normalizar coluna de tempo
        if "review_date" in base_reviews_df.columns:
    #        print("[DEBUG] Normalizando review_date -> order_purchase_timestamp")
            base_reviews_df["order_purchase_timestamp"] = base_reviews_df["review_date"]
        elif "order_purchase_timestamp" not in base_reviews_df.columns:
    #        print("[DEBUG] AVISO: nenhuma coluna de data encontrada!")
            base_reviews_df["order_purchase_timestamp"] = pd.NaT
    else:
    #    print("[DEBUG] Nenhuma fonte de reviews encontrada!")
        base_reviews_df = None

    if base_reviews_df is None or "review_score" not in base_reviews_df.columns or base_reviews_df["review_score"].dropna().empty:
    #    print("[DEBUG] RETORNANDO has_data=False (sem reviews validas)")
        empty_monthly = pd.DataFrame(
            columns=["order_purchase_timestamp", "review_score"]
        )
        empty_dist = pd.Series(dtype=float)
        return {
            "avg_satisfaction": np.nan,
            "satisfaction_trend": "Sem dados de avaliação",
            "trend_icon": get_svg_icon("details", size=24, color="#6b7280"),
            "satisfaction_change": 0.0,
            "distribution": empty_dist,
            "monthly_satisfaction": empty_monthly,
            "top_score_percentage": 0.0,
            "lowest_score_percentage": 0.0,
            "has_data": False,
        }
    
    # Calcular satisfação mensal
    # --- DUCKDB OPTIMIZATION ---
    if len(base_reviews_df) >= 10000:
        try:
            db = get_db()
            db.conn.register('temp_sat_insights', base_reviews_df)
            
            # Mensal
            monthly_query = """
                SELECT 
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as order_purchase_timestamp,
                    AVG(review_score) as review_score
                FROM temp_sat_insights
                WHERE review_score IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """
            monthly_satisfaction = db.query(monthly_query)
            
            # Média geral
            avg_query = "SELECT AVG(review_score) as avg FROM temp_sat_insights"
            avg_res = db.query(avg_query)
            avg_satisfaction = avg_res['avg'][0]
            
            # Distribuição
            dist_query = """
                SELECT review_score, COUNT(*) as count
                FROM temp_sat_insights
                WHERE review_score IS NOT NULL
                GROUP BY 1
            """
            dist_df = db.query(dist_query)
            total_reviews = dist_df['count'].sum()
            satisfaction_distribution = pd.Series(
                data=(dist_df['count'] / total_reviews).values,
                index=dist_df['review_score']
            ).sort_index()
            
            db.conn.unregister('temp_sat_insights')
        except Exception as e:
            print(f"Erro DuckDB Satisfaction: {e}")
            # Fallback Pandas inline
            monthly_satisfaction = (
                base_reviews_df.groupby(safe_to_datetime(base_reviews_df["order_purchase_timestamp"]).dt.to_period("M"))[
                    "review_score"
                ]
                .mean()
                .reset_index()
            )
            monthly_satisfaction["order_purchase_timestamp"] = monthly_satisfaction["order_purchase_timestamp"].astype(str)
            avg_satisfaction = base_reviews_df["review_score"].mean()
            satisfaction_distribution = base_reviews_df["review_score"].value_counts(normalize=True).sort_index()
    else:
        # Pandas (Original)
        monthly_satisfaction = (
            base_reviews_df.groupby(safe_to_datetime(base_reviews_df["order_purchase_timestamp"]).dt.to_period("M"))[
                "review_score"
            ]
            .mean()
            .reset_index()
        )
        monthly_satisfaction["order_purchase_timestamp"] = monthly_satisfaction[
            "order_purchase_timestamp"
        ].astype(str)
        
        avg_satisfaction = base_reviews_df["review_score"].mean()
        satisfaction_distribution = base_reviews_df["review_score"].value_counts(normalize=True).sort_index()
    # --- DUCKDB OPTIMIZATION END ---
    
    # Analisar tendência
    if len(monthly_satisfaction) >= 3:
        recent_avg = monthly_satisfaction["review_score"].tail(3).mean()
        old_avg = monthly_satisfaction["review_score"].head(3).mean()
        satisfaction_change = (
            (recent_avg - old_avg) / old_avg * 100 if old_avg > 0 else 0
        )
        
        if abs(satisfaction_change) < 5:
            satisfaction_trend = "estável"
            trend_icon = get_svg_icon("minus-circle", size=24, color="#3b82f6")
        elif satisfaction_change > 0:
            satisfaction_trend = "melhorando"
            trend_icon = get_svg_icon("trend", size=24, color="#10b981")
        else:
            satisfaction_trend = "piorando"
            trend_icon = get_svg_icon("trend", size=24, color="#ef4444")
    else:
        satisfaction_change = 0.0
        satisfaction_trend = "indeterminado"
        trend_icon = get_svg_icon("details", size=24, color="#6b7280")
    
    result = {
        "avg_satisfaction": float(avg_satisfaction) if pd.notna(avg_satisfaction) else np.nan,
        "satisfaction_trend": satisfaction_trend,
        "trend_icon": trend_icon,
        "satisfaction_change": float(satisfaction_change),
        "distribution": satisfaction_distribution,
        "monthly_satisfaction": monthly_satisfaction,
        "top_score_percentage": float(satisfaction_distribution.get(5, 0)),
        "lowest_score_percentage": float(satisfaction_distribution.get(1, 0)),
        "has_data": True,
    }
#    print(f"[DEBUG] RETORNANDO has_data=True | avg={result['avg_satisfaction']:.2f} | meses={len(monthly_satisfaction)}")
    return result

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_cancellation_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula insights relacionados a cancelamentos. Otimizado com DuckDB.
    """
    DEFAULTS = {
        "default_cogs_ratio": 0.60,
        "marketplace_commission_rate": 0.15,
        "payment_gateway_rate": 0.025,
        "tax_rate": 0.17,
        "packaging_cost_default": 1.20,
    }

    if len(df) < 10000:
        # --- PANDAS (Original) ---
        monthly_cancellation = df.groupby(safe_to_datetime(df['order_purchase_timestamp']).dt.to_period('M'))['pedido_cancelado'].mean().reset_index()
        monthly_cancellation['order_purchase_timestamp'] = monthly_cancellation['order_purchase_timestamp'].astype(str)
        
        cancellation_rate = df['pedido_cancelado'].mean()
        
        cancelled = df[df['pedido_cancelado'] == 1].copy()
        total_cancelled = cancelled['order_id'].nunique()
        lost_revenue = cancelled['price'].sum()
        
        lost_margin = 0.0
        if not cancelled.empty:
            base_price = cancelled['price_original'] if 'price_original' in cancelled.columns else cancelled['price']
            base_price = pd.to_numeric(base_price, errors='coerce').fillna(0.0)
            product_cost = pd.to_numeric(cancelled.get('product_cost', base_price * DEFAULTS["default_cogs_ratio"]), errors='coerce').fillna(base_price * DEFAULTS["default_cogs_ratio"])
            commission = pd.to_numeric(cancelled.get('marketplace_commission', base_price * DEFAULTS["marketplace_commission_rate"]), errors='coerce').fillna(base_price * DEFAULTS["marketplace_commission_rate"])
            gateway = pd.to_numeric(cancelled.get('payment_gateway_fee', base_price * DEFAULTS["payment_gateway_rate"]), errors='coerce').fillna(base_price * DEFAULTS["payment_gateway_rate"])
            tax = pd.to_numeric(cancelled.get('tax_amount', base_price * DEFAULTS["tax_rate"]), errors='coerce').fillna(base_price * DEFAULTS["tax_rate"])
            if 'packaging_cost' in cancelled.columns:
                packaging = pd.to_numeric(cancelled['packaging_cost'], errors='coerce').fillna(DEFAULTS["packaging_cost_default"])
            else:
                packaging = pd.Series(DEFAULTS["packaging_cost_default"], index=cancelled.index)
            lost_margin = float((base_price - product_cost - commission - gateway - tax - packaging).sum())
    else:
        # --- DUCKDB ---
        try:
            db = get_db()
            db.conn.register('temp_cancel_insights', df)
            
            # 1. Taxa Mensal
            monthly_query = """
                SELECT 
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as order_purchase_timestamp,
                    AVG(pedido_cancelado) as pedido_cancelado
                FROM temp_cancel_insights
                GROUP BY 1
                ORDER BY 1
            """
            monthly_cancellation = db.query(monthly_query)
            
            # 2. Métricas Gerais e Margem Perdida (Cálculo Complexo com COALESCE)
            # Definir colunas base
            col_price_orig = "price_original" if "price_original" in df.columns else "price"
            col_prod_cost = "product_cost" if "product_cost" in df.columns else f"({col_price_orig} * {DEFAULTS['default_cogs_ratio']})"
            col_comm = "marketplace_commission" if "marketplace_commission" in df.columns else f"({col_price_orig} * {DEFAULTS['marketplace_commission_rate']})"
            col_gateway = "payment_gateway_fee" if "payment_gateway_fee" in df.columns else f"({col_price_orig} * {DEFAULTS['payment_gateway_rate']})"
            col_tax = "tax_amount" if "tax_amount" in df.columns else f"({col_price_orig} * {DEFAULTS['tax_rate']})"
            col_pack = "packaging_cost" if "packaging_cost" in df.columns else str(DEFAULTS["packaging_cost_default"])
            
            metrics_query = f"""
                SELECT
                    AVG(pedido_cancelado) as cancellation_rate,
                    COUNT(DISTINCT CASE WHEN pedido_cancelado = 1 THEN order_id END) as total_cancelled,
                    SUM(CASE WHEN pedido_cancelado = 1 THEN price ELSE 0 END) as lost_revenue,
                    SUM(CASE WHEN pedido_cancelado = 1 THEN 
                        (COALESCE({col_price_orig}, 0) - 
                         COALESCE({col_prod_cost}, COALESCE({col_price_orig}, 0) * {DEFAULTS['default_cogs_ratio']}) - 
                         COALESCE({col_comm}, COALESCE({col_price_orig}, 0) * {DEFAULTS['marketplace_commission_rate']}) - 
                         COALESCE({col_gateway}, COALESCE({col_price_orig}, 0) * {DEFAULTS['payment_gateway_rate']}) - 
                         COALESCE({col_tax}, COALESCE({col_price_orig}, 0) * {DEFAULTS['tax_rate']}) - 
                         COALESCE({col_pack}, {DEFAULTS['packaging_cost_default']})
                        )
                    ELSE 0 END) as lost_margin
                FROM temp_cancel_insights
            """
            metrics = db.query(metrics_query)
            
            cancellation_rate = metrics['cancellation_rate'][0]
            total_cancelled = metrics['total_cancelled'][0]
            lost_revenue = metrics['lost_revenue'][0]
            lost_margin = metrics['lost_margin'][0]
            
            db.conn.unregister('temp_cancel_insights')
        except Exception as e:
            print(f"Erro DuckDB Cancellation: {e}")
            return calculate_cancellation_insights.__wrapped__(df) # Fallback

    # --- Analisar tendência (Comum) ---
    if len(monthly_cancellation) >= 3:
        recent_rate = monthly_cancellation['pedido_cancelado'].tail(3).mean()
        old_rate = monthly_cancellation['pedido_cancelado'].head(3).mean()
        rate_change = ((recent_rate - old_rate) / old_rate) * 100 if old_rate > 0 else 0
        
        if abs(rate_change) < 5:
            cancellation_trend = "estável"
            trend_icon = get_svg_icon("minus-circle", size=24, color="#3b82f6")
        elif rate_change > 0:
            cancellation_trend = "aumentando"
            trend_icon = get_svg_icon("trend", size=24, color="#ef4444")
        else:
            cancellation_trend = "diminuindo"
            trend_icon = get_svg_icon("trend", size=24, color="#10b981")
    else:
        rate_change = 0
        cancellation_trend = "indeterminado"
        trend_icon = get_svg_icon("details", size=24, color="#6b7280")
    
    return {
        "cancellation_rate": cancellation_rate,
        "total_cancelled": total_cancelled,
        "lost_revenue": lost_revenue,
        "lost_margin": lost_margin,
        "cancellation_trend": cancellation_trend,
        "trend_icon": trend_icon,
        "rate_change": rate_change,
        "monthly_cancellation": monthly_cancellation
    }

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_delivery_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula insights relacionados a entregas. Otimizado com DuckDB.
    """
    if 'order_delivered_customer_date' not in df.columns or df['order_delivered_customer_date'].isna().all():
        return {
            "avg_delivery_time": 0,
            "avg_delivery_time_last_10": 0,
            "delivery_trend": "N/A",
            "trend_icon": get_svg_icon("details", size=24, color="#6b7280"),
            "time_change": 0,
            "monthly_delivery": pd.DataFrame(columns=["order_purchase_timestamp", "delivery_time"]),
            "delivery_stats": {}
        }

    # Definições
    FAST_DELIVERY = 7
    NORMAL_DELIVERY = 15

    if len(df) < 10000:
        # --- PANDAS (Original) ---
        df_proc = df.copy()
        df_proc['delivery_time'] = (safe_to_datetime(df_proc['order_delivered_customer_date']) - 
                             safe_to_datetime(df_proc['order_purchase_timestamp'])).dt.days
        
        monthly_delivery = df_proc.groupby(safe_to_datetime(df_proc['order_purchase_timestamp']).dt.to_period('M'))['delivery_time'].mean().reset_index()
        monthly_delivery['order_purchase_timestamp'] = monthly_delivery['order_purchase_timestamp'].astype(str)
        
        avg_delivery_time = df_proc['delivery_time'].mean()
        
        df_sorted = df_proc.sort_values('order_purchase_timestamp', ascending=False)
        last_10_delivered = df_sorted.dropna(subset=['delivery_time']).head(10)
        avg_delivery_time_last_10 = last_10_delivered['delivery_time'].mean() if len(last_10_delivered) > 0 else avg_delivery_time
        
        fast_deliveries = df_proc[df_proc['delivery_time'] <= FAST_DELIVERY]['order_id'].nunique()
        normal_deliveries = df_proc[(df_proc['delivery_time'] > FAST_DELIVERY) & (df_proc['delivery_time'] <= NORMAL_DELIVERY)]['order_id'].nunique()
        slow_deliveries = df_proc[df_proc['delivery_time'] > NORMAL_DELIVERY]['order_id'].nunique()
        total_deliveries = df_proc['order_id'].nunique()
    else:
        # --- DUCKDB ---
        try:
            db = get_db()
            db.conn.register('temp_deliv_insights', df)
            
            # Pré-cálculo de delivery time
            base_query = """
                SELECT 
                    *,
                    date_diff('day', CAST(order_purchase_timestamp AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP)) as delivery_time
                FROM temp_deliv_insights
                WHERE order_delivered_customer_date IS NOT NULL
            """
            
            # Mensal
            monthly_query = f"""
                WITH processed AS ({base_query})
                SELECT 
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as order_purchase_timestamp,
                    AVG(delivery_time) as delivery_time
                FROM processed
                GROUP BY 1
                ORDER BY 1
            """
            monthly_delivery = db.query(monthly_query)
            
            # Média Geral
            avg_query = f"""
                WITH processed AS ({base_query})
                SELECT AVG(delivery_time) as avg_time FROM processed
            """
            avg_res = db.query(avg_query)
            avg_delivery_time = avg_res['avg_time'][0]
            
            # Média Últimos 10
            last10_query = f"""
                WITH processed AS ({base_query})
                SELECT AVG(delivery_time) as avg_last_10
                FROM (
                    SELECT delivery_time
                    FROM processed
                    ORDER BY order_purchase_timestamp DESC
                    LIMIT 10
                )
            """
            last10_res = db.query(last10_query)
            avg_delivery_time_last_10 = last10_res['avg_last_10'][0] if not last10_res.empty and last10_res['avg_last_10'][0] is not None else avg_delivery_time
            
            # Stats de Distribuição
            stats_query = f"""
                WITH processed AS ({base_query})
                SELECT
                    COUNT(DISTINCT CASE WHEN delivery_time <= {FAST_DELIVERY} THEN order_id END) as fast,
                    COUNT(DISTINCT CASE WHEN delivery_time > {FAST_DELIVERY} AND delivery_time <= {NORMAL_DELIVERY} THEN order_id END) as normal,
                    COUNT(DISTINCT CASE WHEN delivery_time > {NORMAL_DELIVERY} THEN order_id END) as slow,
                    COUNT(DISTINCT order_id) as total
                FROM processed
            """
            stats = db.query(stats_query)
            fast_deliveries = stats['fast'][0]
            normal_deliveries = stats['normal'][0]
            slow_deliveries = stats['slow'][0]
            total_deliveries = stats['total'][0]
            
            db.conn.unregister('temp_deliv_insights')
        except Exception as e:
            print(f"Erro DuckDB Delivery: {e}")
            return calculate_delivery_insights.__wrapped__(df) # Fallback

    # --- Pós-processamento (Comum) ---
    fast_rate = fast_deliveries / total_deliveries if total_deliveries > 0 else 0
    normal_rate = normal_deliveries / total_deliveries if total_deliveries > 0 else 0
    slow_rate = slow_deliveries / total_deliveries if total_deliveries > 0 else 0
    
    if len(monthly_delivery) >= 3:
        recent_time = monthly_delivery['delivery_time'].tail(3).mean()
        old_time = monthly_delivery['delivery_time'].head(3).mean()
        time_change = ((recent_time - old_time) / old_time) * 100 if old_time > 0 else 0
        
        if abs(time_change) < 5:
            delivery_trend = "estável"
            trend_icon = get_svg_icon("minus-circle", size=24, color="#3b82f6")
        elif time_change > 0:
            delivery_trend = "aumentando"
            trend_icon = get_svg_icon("trend", size=24, color="#ef4444")
        else:
            delivery_trend = "melhorando"
            trend_icon = get_svg_icon("trend", size=24, color="#10b981")
    else:
        time_change = 0
        delivery_trend = "indeterminado"
        trend_icon = get_svg_icon("details", size=24, color="#6b7280")
    
    return {
        "avg_delivery_time": avg_delivery_time,
        "avg_delivery_time_last_10": avg_delivery_time_last_10,
        "delivery_trend": delivery_trend,
        "trend_icon": trend_icon,
        "time_change": time_change,
        "monthly_delivery": monthly_delivery,
        "delivery_stats": {
            "fast_rate": fast_rate,
            "normal_rate": normal_rate,
            "slow_rate": slow_rate,
            "fast_count": fast_deliveries,
            "normal_count": normal_deliveries,
            "slow_count": slow_deliveries,
            "total_deliveries": total_deliveries
        }
    }

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_customer_behavior_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula insights relacionados ao comportamento do cliente. Otimizado com DuckDB.
    """
    # Inicializar variáveis com valores padrão para evitar UnboundLocalError
    delivery_satisfaction_corr = 0.0
    price_satisfaction_corr = 0.0
    repurchase_satisfaction_corr = 0.0
    delivery_trend = "indisponivel"
    satisfaction_data = pd.DataFrame()
    review_distribution = pd.Series(dtype=float)

    # ----------------------------
    # Guardrails (evita loop/erro)
    # ----------------------------
    if df is None or df.empty:
        return {
            "satisfaction_evolution": pd.DataFrame(columns=["order_purchase_timestamp", "mean", "count", "std"]),
            "review_distribution": pd.Series(dtype=float),
            "correlations": {
                "delivery_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                "ticket_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                "repurchase_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
            },
        }

    # Sem review_score, não há o que calcular (evita exceptions + reruns infinitos)
    if "review_score" not in df.columns or "order_purchase_timestamp" not in df.columns:
        return {
            "satisfaction_evolution": pd.DataFrame(columns=["order_purchase_timestamp", "mean", "count", "std"]),
            "review_distribution": pd.Series(dtype=float),
            "correlations": {
                "delivery_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                "ticket_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                "repurchase_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
            },
        }

    def _pandas_impl(df_in: pd.DataFrame) -> Dict[str, Any]:
        # --- PANDAS (Seguro) ---
        d = df_in.copy()
        d["review_score"] = pd.to_numeric(d["review_score"], errors="coerce")
        d = d.loc[d["review_score"].notna()].copy()
        if d.empty:
            return {
                "satisfaction_evolution": pd.DataFrame(columns=["order_purchase_timestamp", "mean", "count", "std"]),
                "review_distribution": pd.Series(dtype=float),
                "correlations": {
                    "delivery_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                    "ticket_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                    "repurchase_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
                },
            }

        purchase_dt = safe_to_datetime(d["order_purchase_timestamp"])
        satisfaction_data = (
            d.groupby(purchase_dt.dt.to_period("M"))["review_score"]
            .agg(["mean", "count", "std"])
            .reset_index()
        )
        satisfaction_data["order_purchase_timestamp"] = satisfaction_data["order_purchase_timestamp"].astype(str)
        
        review_distribution = d["review_score"].value_counts(normalize=True).sort_index()
        
        # Ticket vs satisfação
        price_satisfaction_corr = 0.0
        if "price" in d.columns:
            try:
                d["price"] = pd.to_numeric(d["price"], errors="coerce")
                price_by_score = d.groupby("review_score")["price"].mean().dropna()
                if len(price_by_score) >= 2:
                    price_satisfaction_corr = float(price_by_score.corr(pd.Series(price_by_score.index)))
            except Exception:
                pass
        
        # Recompra vs satisfação (proxy: clientes únicos por nota)
        repurchase_satisfaction_corr = 0.0
        if "customer_unique_id" in d.columns:
            try:
                repurchase_by_score = d.groupby("review_score")["customer_unique_id"].nunique()
                if len(repurchase_by_score) >= 2:
                    repurchase_satisfaction_corr = float(repurchase_by_score.corr(pd.Series(repurchase_by_score.index)))
            except Exception:
                pass
            
        # Entrega vs satisfação (se existir coluna)
        delivery_satisfaction_corr = 0.0
        delivery_trend = "indisponivel"
        if "order_delivered_customer_date" in d.columns and not d["order_delivered_customer_date"].isna().all():
            try:
                dd = d.copy()
                dd["delivery_time"] = (
                    pd.to_datetime(dd["order_delivered_customer_date"], errors="coerce")
                    - pd.to_datetime(dd["order_purchase_timestamp"], errors="coerce")
                ).dt.days
                dd = dd.loc[
                    dd["delivery_time"].notna()
                    & (dd["delivery_time"] >= 0)
                    & (dd["delivery_time"] <= 180)
                ].copy()
                if len(dd) > 5:
                    delivery_by_score = dd.groupby("review_score")["delivery_time"].mean()
                    if len(delivery_by_score) >= 3:
                        delivery_satisfaction_corr = float(delivery_by_score.corr(pd.Series(delivery_by_score.index)))
                        delivery_trend = "maior" if delivery_satisfaction_corr > 0 else "menor"
            except Exception:
                pass

        correlations_out = {
            "delivery_vs_satisfaction": {"correlation": delivery_satisfaction_corr, "trend": delivery_trend},
            "ticket_vs_satisfaction": {"correlation": price_satisfaction_corr, "trend": "maior" if price_satisfaction_corr > 0 else "menor"},
            "repurchase_vs_satisfaction": {"correlation": repurchase_satisfaction_corr, "trend": "menor" if repurchase_satisfaction_corr > 0 else "maior"},
        }
        return {
            "satisfaction_evolution": satisfaction_data,
            "review_distribution": review_distribution,
            "correlations": correlations_out,
        }

    if len(df) < 10000:
        return _pandas_impl(df)

    # --- DUCKDB (quando fizer sentido) ---
        try:
            db = get_db()
            db.conn.register("temp_behavior", df)
            
            evol_query = """
                SELECT 
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as order_purchase_timestamp,
                    AVG(review_score) as mean,
                    COUNT(review_score) as count,
                    STDDEV_SAMP(review_score) as std
                FROM temp_behavior
                WHERE review_score IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """
            satisfaction_data = db.query(evol_query)
            
            dist_query = """
                SELECT review_score, COUNT(*) as cnt
                FROM temp_behavior
                WHERE review_score IS NOT NULL
                GROUP BY 1
            """
            dist_df = db.query(dist_query)
    
            total = dist_df["cnt"].sum() if not dist_df.empty else 0
            review_distribution = (
            pd.Series(data=(dist_df["cnt"] / total).values, index=dist_df["review_score"]).sort_index()
            if total
            else pd.Series(dtype=float)
        )

            has_price = "price" in df.columns
            has_customer = "customer_unique_id" in df.columns
            has_delivery = "order_delivered_customer_date" in df.columns and not df["order_delivered_customer_date"].isna().all()

            avg_price_expr = "AVG(price) as avg_price" if has_price else "NULL as avg_price"
            uniq_customers_expr = "COUNT(DISTINCT customer_unique_id) as unique_customers" if has_customer else "NULL as unique_customers"
            avg_delivery_expr = (
                "AVG(date_diff('day', CAST(order_purchase_timestamp AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP))) as avg_delivery"
                if has_delivery
                else "NULL as avg_delivery"
            )

            corr_query = f"""
                    SELECT 
                        review_score,
                    {avg_price_expr},
                    {uniq_customers_expr},
                    {avg_delivery_expr}
                    FROM temp_behavior
                    WHERE review_score IS NOT NULL
                    GROUP BY 1
                    ORDER BY 1
                """
            corr_df = db.query(corr_query).set_index("review_score")
            
            price_satisfaction_corr = 0.0
            if has_price and "avg_price" in corr_df.columns and len(corr_df) >= 2:
                try:
                    price_satisfaction_corr = float(corr_df["avg_price"].corr(pd.Series(corr_df.index)))
                except Exception:
                    price_satisfaction_corr = 0.0

            repurchase_satisfaction_corr = 0.0
            if has_customer and "unique_customers" in corr_df.columns and len(corr_df) >= 2:
                try:
                    repurchase_satisfaction_corr = float(corr_df["unique_customers"].corr(pd.Series(corr_df.index)))
                except Exception:
                    repurchase_satisfaction_corr = 0.0
                
            delivery_satisfaction_corr = 0.0
            delivery_trend = "indisponivel"
            if has_delivery and "avg_delivery" in corr_df.columns and len(corr_df) >= 3:
                valid_deliv = corr_df["avg_delivery"].dropna()
                if len(valid_deliv) >= 3:
                    delivery_satisfaction_corr = float(valid_deliv.corr(pd.Series(valid_deliv.index)))
                    delivery_trend = "maior" if delivery_satisfaction_corr > 0 else "menor"
            
            db.conn.unregister("temp_behavior")

        except Exception as e:
        # IMPORTANT: nunca chamar __wrapped__ aqui (causa loop infinito com df grande)
            print(f"Erro DuckDB Behavior: {e}")
        try:
            db.conn.unregister("temp_behavior")  # type: ignore
        except Exception:
            pass
        return _pandas_impl(df)

    # --- Retorno Comum ---
    correlations = {
        "delivery_vs_satisfaction": {
            "correlation": delivery_satisfaction_corr,
            "trend": delivery_trend
        },
        "ticket_vs_satisfaction": {
            "correlation": price_satisfaction_corr,
            "trend": "maior" if price_satisfaction_corr > 0 else "menor"
        },
        "repurchase_vs_satisfaction": {
            "correlation": repurchase_satisfaction_corr,
            "trend": "menor" if repurchase_satisfaction_corr > 0 else "maior"
        }
    }
    
    return {
        "satisfaction_evolution": satisfaction_data,
        "review_distribution": review_distribution,
        "correlations": correlations
    }

def _get_filtered_reviews(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra reviews do session_state pelos mesmos critérios aplicados ao filtered_df.
    
    Args:
        filtered_df: DataFrame de pedidos já filtrado (marketplace, categoria, período)
        
    Returns:
        DataFrame de reviews filtrado pelos mesmos critérios
    """
    import streamlit as st
    from utils.filtros import filter_reviews_by_period
    
    # Importar aqui para evitar circular imports
    if not hasattr(st, 'session_state'):
        return pd.DataFrame()
    
    reviews_df = st.session_state.get("reviews_df")
    if reviews_df is None or reviews_df.empty:
        return pd.DataFrame()
    
    filtered_reviews = reviews_df.copy()
    
    # 1. Filtrar por marketplace (se aplicado ao filtered_df)
    if "marketplace" in filtered_df.columns and "marketplace" in filtered_reviews.columns:
        marketplaces = filtered_df["marketplace"].dropna().unique()
        if len(marketplaces) > 0:
            filtered_reviews = filtered_reviews[filtered_reviews["marketplace"].isin(marketplaces)]
    
    # 2. Filtrar por categoria (se aplicado ao filtered_df)
    if "product_category_name" in filtered_df.columns and "product_category_name" in filtered_reviews.columns:
        categories = filtered_df["product_category_name"].dropna().unique()
        if len(categories) == 1:  # Apenas se for uma categoria específica (não "Todas")
            filtered_reviews = filtered_reviews[
                filtered_reviews["product_category_name"] == categories[0]
            ]
    
    # 3. Filtrar por período (usando date_range do session_state se disponível)
    date_range = st.session_state.get("current_date_range") or st.session_state.get("date_range")
    if date_range:
        filtered_reviews = filter_reviews_by_period(filtered_reviews, date_range)
    
    return filtered_reviews


def generate_overview_insights(df: pd.DataFrame, eligible_only: bool = True) -> Dict[str, Any]:
    """
    Gera todos os insights para a página de Visão Geral.
    
    Args:
        df: DataFrame com os dados filtrados
        
    Returns:
        Dict com todos os insights organizados por categoria
    """
    # Obter reviews filtrados pelos mesmos critérios
    filtered_reviews = _get_filtered_reviews(df)
    
    # Armazenar reviews filtrados temporariamente no session_state para calculate_satisfaction_insights usar
    import streamlit as st
    original_reviews = st.session_state.get("reviews_df")
    if not filtered_reviews.empty:
        st.session_state["_filtered_reviews_temp"] = filtered_reviews
    else:
        st.session_state["_filtered_reviews_temp"] = None
    
    revenue_insights = calculate_revenue_insights(df, eligible_only=eligible_only)
    satisfaction_insights = calculate_satisfaction_insights(df)
    cancellation_insights = calculate_cancellation_insights(df)
    delivery_insights = calculate_delivery_insights(df)
    
    # Identificar principais oportunidades de melhoria
    # Importante: a seção "Plano de Ação" deve SEMPRE mostrar as metas
    # (mesmo quando já estão dentro do alvo), para dar contexto executivo.
    improvement_opportunities: List[Dict[str, Any]] = []

    # 1) Cancelamento (meta < 5%)
    cancel_rate = float(cancellation_insights.get("cancellation_rate", 0) or 0)
    if cancel_rate > 0.10:
        cancel_priority = "Alta"
    elif cancel_rate > 0.05:
        cancel_priority = "Média"
    else:
        cancel_priority = "Baixa"
    improvement_opportunities.append({
        "area": "Taxa de Cancelamento",
        "current": cancel_rate,
        "goal": "< 5%",
        "priority": cancel_priority,
        "format_as_percentage": True,
    })

    # 2) Entrega (agora focado em % de Atraso > 15 dias)
    # Meta: Menos de 5% dos pedidos com atraso crítico
    delivery_stats = delivery_insights.get("delivery_stats", {})
    slow_rate = float(delivery_stats.get("slow_rate", 0) or 0)
    
    if slow_rate > 0.15:
        deliv_priority = "Alta"
    elif slow_rate > 0.05:
        deliv_priority = "Média"
    else:
        deliv_priority = "Baixa"
        
    improvement_opportunities.append({
        "area": "Entregas Críticas (>15 dias)",
        "current": slow_rate,
        "goal": "< 5%",
        "priority": deliv_priority,
        "format_as_percentage": True,
    })

    # 3) Satisfação (agora focado em % de 5 Estrelas)
    # Meta: Mais de 85% de avaliações máximas (Excelência)
    top_score_pct = float(satisfaction_insights.get("top_score_percentage", 0) or 0)
    
    if top_score_pct < 0.70:
        sat_priority = "Alta"
    elif top_score_pct < 0.85:
        sat_priority = "Média"
    else:
        sat_priority = "Baixa"
        
    improvement_opportunities.append({
        "area": "Excelência (5 Estrelas)",
        "current": top_score_pct,
        "goal": "> 85%",
        "priority": sat_priority,
        "format_as_percentage": True,
    })
    
    return {
        "revenue": revenue_insights,
        "satisfaction": satisfaction_insights,
        "cancellation": cancellation_insights,
        "delivery": delivery_insights,
        "improvement_opportunities": improvement_opportunities
    }


def _safe_month_str(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.to_period("M").astype(str)


def _compute_macro_correlations_by_month(
    reviews_df: pd.DataFrame,
    orders_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Fallback de correlação quando NÃO é possível ligar review → pedido.

    Método: correlaciona séries mensais:
    - Satisfação média mensal (reviews) vs
    - Ticket médio mensal (orders), Entrega média mensal (orders), Recompra mensal (orders)

    Isso preserva impacto executivo/capital sem inventar join por pedido.
    """
    out: Dict[str, Any] = {
        "method": "macro_monthly",
        "min_points": 3,
        "points_used": 0,
        "ticket_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "delivery_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "repurchase_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
    }

    if reviews_df is None or reviews_df.empty or orders_df is None or orders_df.empty:
        return out

    # --- Reviews: satisfação mensal ---
    rev = reviews_df.copy()
    date_col = "review_date" if "review_date" in rev.columns else "order_purchase_timestamp"
    if date_col not in rev.columns or "review_score" not in rev.columns:
        return out
    rev = rev.loc[pd.to_numeric(rev["review_score"], errors="coerce").notna()].copy()
    rev["review_score"] = pd.to_numeric(rev["review_score"], errors="coerce")
    rev["month"] = _safe_month_str(rev[date_col])
    rev_month = (
        rev.groupby("month")
        .agg(avg_satisfaction=("review_score", "mean"), reviews=("review_score", "count"))
        .reset_index()
    )

    # --- Orders: métricas mensais ---
    ords = orders_df.copy()
    if "order_purchase_timestamp" not in ords.columns:
        return out
    ords["order_purchase_timestamp"] = pd.to_datetime(ords["order_purchase_timestamp"], errors="coerce")
    ords = ords.loc[ords["order_purchase_timestamp"].notna()].copy()
    ords["month"] = _safe_month_str(ords["order_purchase_timestamp"])

    # Ticket médio mensal (por pedido)
    if "price" in ords.columns:
        ords["price"] = pd.to_numeric(ords["price"], errors="coerce")
    ticket_month = (
        ords.groupby("month")
        .agg(avg_ticket=("price", "mean"))
        .reset_index()
        if "price" in ords.columns
        else pd.DataFrame(columns=["month", "avg_ticket"])
    )

    # Entrega média mensal (dias)
    delivery_month = pd.DataFrame(columns=["month", "avg_delivery_days"])
    if "order_delivered_customer_date" in ords.columns:
        d = ords.copy()
        d["order_delivered_customer_date"] = pd.to_datetime(d["order_delivered_customer_date"], errors="coerce")
        d["delivery_days"] = (d["order_delivered_customer_date"] - d["order_purchase_timestamp"]).dt.days
        d = d.loc[d["delivery_days"].notna() & (d["delivery_days"] >= 0) & (d["delivery_days"] <= 180)].copy()
        if not d.empty:
            delivery_month = d.groupby("month").agg(avg_delivery_days=("delivery_days", "mean")).reset_index()

    # Recompra mensal: % de clientes com >1 pedido no mês / clientes no mês
    repurchase_month = pd.DataFrame(columns=["month", "repurchase_rate_month"])
    if "customer_unique_id" in ords.columns and "order_id" in ords.columns:
        cust_orders = (
            ords.groupby(["month", "customer_unique_id"])["order_id"]
            .nunique()
            .reset_index(name="orders_in_month")
        )
        month_totals = cust_orders.groupby("month")["customer_unique_id"].nunique().reset_index(name="customers")
        month_returning = cust_orders.loc[cust_orders["orders_in_month"] > 1].groupby("month")[
            "customer_unique_id"
        ].nunique().reset_index(name="returning_customers")
        repurchase_month = month_totals.merge(month_returning, on="month", how="left")
        repurchase_month["returning_customers"] = repurchase_month["returning_customers"].fillna(0)
        repurchase_month["repurchase_rate_month"] = repurchase_month.apply(
            lambda r: (r["returning_customers"] / r["customers"]) if r["customers"] else 0.0,
            axis=1,
        )
        repurchase_month = repurchase_month[["month", "repurchase_rate_month"]]

    # --- Merge mensal ---
    merged = rev_month.merge(ticket_month, on="month", how="left").merge(delivery_month, on="month", how="left").merge(
        repurchase_month, on="month", how="left"
    )
    merged = merged.dropna(subset=["avg_satisfaction"])
    if merged.empty:
        return out

    # Usar apenas meses com dados suficientes
    out["points_used"] = int(merged["month"].nunique())
    if out["points_used"] < out["min_points"]:
        return out

    s = merged.set_index("month")["avg_satisfaction"]

    # Correlação satisfação vs ticket
    if "avg_ticket" in merged.columns and merged["avg_ticket"].notna().sum() >= out["min_points"]:
        out["ticket_vs_satisfaction"]["correlation"] = float(s.corr(merged.set_index("month")["avg_ticket"]))
        out["ticket_vs_satisfaction"]["trend"] = "maior" if out["ticket_vs_satisfaction"]["correlation"] > 0 else "menor"

    # Correlação satisfação vs entrega (dias)
    if "avg_delivery_days" in merged.columns and merged["avg_delivery_days"].notna().sum() >= out["min_points"]:
        out["delivery_vs_satisfaction"]["correlation"] = float(s.corr(merged.set_index("month")["avg_delivery_days"]))
        out["delivery_vs_satisfaction"]["trend"] = "maior" if out["delivery_vs_satisfaction"]["correlation"] > 0 else "menor"

    # Correlação satisfação vs recompra mensal
    if "repurchase_rate_month" in merged.columns and merged["repurchase_rate_month"].notna().sum() >= out["min_points"]:
        out["repurchase_vs_satisfaction"]["correlation"] = float(s.corr(merged.set_index("month")["repurchase_rate_month"]))
        out["repurchase_vs_satisfaction"]["trend"] = "maior" if out["repurchase_vs_satisfaction"]["correlation"] > 0 else "menor"

    return out


def _compute_direct_correlations(
    reviews_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Calcula correlações DIRETAS quando reviews estão linkados 1:1 com pedidos.
    
    Pré-requisito: reviews_df já contém colunas enriquecidas do pedido:
    - price (ticket do pedido)
    - freight_value (frete)
    - lead_time_delivery_days (dias de entrega)
    - customer_id (para análise de recompra)
    - review_score (nota da avaliação)
    
    Este método é mais preciso que correlações macro pois usa link direto.
    """
    out: Dict[str, Any] = {
        "method": "direct_link",
        "link_rate": 0.0,
        "sample_size": 0,
        "ticket_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "delivery_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "repurchase_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "freight_vs_satisfaction": {"correlation": 0.0, "trend": "indisponivel"},
        "category_satisfaction": {},
    }
    
    if reviews_df is None or reviews_df.empty:
        return out
    
    # Garantir que review_score é numérico
    df = reviews_df.copy()
    df["review_score"] = pd.to_numeric(df.get("review_score", pd.Series()), errors="coerce")
    df = df.loc[df["review_score"].notna()].copy()
    
    if df.empty:
        return out
    
    out["sample_size"] = len(df)
    
    # Calcular link rate (quantos reviews têm dados de pedido)
    linked_mask = df["customer_id"].notna() if "customer_id" in df.columns else pd.Series([False] * len(df))
    out["link_rate"] = float(linked_mask.mean())
    
    # Filtrar apenas reviews linkados para correlações
    df_linked = df.loc[linked_mask].copy() if linked_mask.any() else df
    
    if len(df_linked) < 5:
        return out
    
    # 1. Correlação Nota vs Ticket (price)
    if "price" in df_linked.columns:
        df_linked["price"] = pd.to_numeric(df_linked["price"], errors="coerce")
        valid = df_linked.loc[df_linked["price"].notna() & (df_linked["price"] > 0)]
        if len(valid) >= 5:
            corr = valid["review_score"].corr(valid["price"])
            if pd.notna(corr):
                out["ticket_vs_satisfaction"]["correlation"] = float(corr)
                out["ticket_vs_satisfaction"]["trend"] = "maior" if corr > 0 else "menor"
    
    # 2. Correlação Nota vs Frete (freight_value)
    if "freight_value" in df_linked.columns:
        df_linked["freight_value"] = pd.to_numeric(df_linked["freight_value"], errors="coerce")
        valid = df_linked.loc[df_linked["freight_value"].notna()]
        if len(valid) >= 5:
            corr = valid["review_score"].corr(valid["freight_value"])
            if pd.notna(corr):
                out["freight_vs_satisfaction"]["correlation"] = float(corr)
                out["freight_vs_satisfaction"]["trend"] = "maior" if corr > 0 else "menor"
    
    # 3. Correlação Nota vs Lead Time (dias de entrega)
    if "lead_time_delivery_days" in df_linked.columns:
        df_linked["lead_time_delivery_days"] = pd.to_numeric(df_linked["lead_time_delivery_days"], errors="coerce")
        valid = df_linked.loc[df_linked["lead_time_delivery_days"].notna() & (df_linked["lead_time_delivery_days"] >= 0)]
        if len(valid) >= 5:
            corr = valid["review_score"].corr(valid["lead_time_delivery_days"])
            if pd.notna(corr):
                out["delivery_vs_satisfaction"]["correlation"] = float(corr)
                out["delivery_vs_satisfaction"]["trend"] = "maior" if corr > 0 else "menor"
    
    # 4. Análise de Recompra vs Satisfação (clientes com múltiplas avaliações)
    if "customer_id" in df_linked.columns:
        cust_stats = df_linked.groupby("customer_id").agg(
            num_reviews=("review_score", "count"),
            avg_score=("review_score", "mean")
        ).reset_index()
        
        # Clientes com >1 review = "recompradores"
        recompradores = cust_stats.loc[cust_stats["num_reviews"] > 1]
        unicos = cust_stats.loc[cust_stats["num_reviews"] == 1]
        
        if len(recompradores) >= 3 and len(unicos) >= 3:
            avg_recomp = recompradores["avg_score"].mean()
            avg_unico = unicos["avg_score"].mean()
            # Correlação simplificada: comparar médias
            diff = avg_recomp - avg_unico
            out["repurchase_vs_satisfaction"]["correlation"] = float(diff)  # Não é Pearson, é diferença
            out["repurchase_vs_satisfaction"]["trend"] = "maior" if diff > 0 else "menor"
            out["repurchase_vs_satisfaction"]["avg_recompradores"] = float(avg_recomp)
            out["repurchase_vs_satisfaction"]["avg_unicos"] = float(avg_unico)
            out["repurchase_vs_satisfaction"]["n_recompradores"] = int(len(recompradores))
    
    # 5. Satisfação por Categoria (Top 10)
    if "category_name" in df_linked.columns:
        cat_stats = df_linked.groupby("category_name").agg(
            nota_media=("review_score", "mean"),
            qtd=("review_score", "count")
        ).reset_index()
        cat_stats = cat_stats.sort_values("qtd", ascending=False).head(10)
        out["category_satisfaction"] = cat_stats.to_dict(orient="records")
    
    return out


def format_insight_message(insights: Dict[str, Any]) -> str:
    """
    Formata os insights em uma mensagem legível.
    
    Args:
        insights: Dicionário com os insights calculados
        
    Returns:
        String formatada com os insights
    """
    revenue = insights['revenue']
    satisfaction = insights['satisfaction']
    cancellation = insights['cancellation']
    delivery = insights['delivery']
    
    # Criar ícones SVG
    money_icon = get_svg_icon("money", size=16)
    star_icon = get_svg_icon("star", size=16, color="#fbbf24")
    cancel_icon = get_svg_icon("cancel", size=16, color="#ef4444")
    box_icon = get_svg_icon("box", size=16)
    
    message = f"""
    {money_icon} **Desempenho Financeiro**
    - Crescimento: {revenue['growth_rate']:.1f}% ({revenue['trend']})
    - Melhor mês: {revenue['best_month']} (R$ {revenue['best_month_revenue']:,.2f})
    
    {star_icon} **Satisfação do Cliente**
    - Nota média: {satisfaction['avg_satisfaction']:.1f}/5.0
    - Tendência: {satisfaction['satisfaction_trend']} {satisfaction['trend_icon']}
    - {satisfaction['top_score_percentage']*100:.1f}% notas máximas
    
    {cancel_icon} **Cancelamentos**
    - Taxa de {(cancellation['cancellation_rate']*100):.1f}%
    - Tendência {cancellation['cancellation_trend']} {cancellation['trend_icon']}
    - Receita perdida: R$ {cancellation['lost_revenue']:,.2f}
    
    {box_icon} **Entregas**
    - Tempo médio: {delivery['avg_delivery_time']:.1f} dias
    - Tendência {delivery['delivery_trend']} {delivery['trend_icon']}
    - {(delivery['on_time_rate']*100):.1f}% no prazo
    """
    
    return message


def _format_month_display(val: Any) -> str:
    """Formata mês para exibição no padrão MM-YYYY (ex: 02-2026). Aceita Timestamp, datetime ou str YYYY-MM."""
    if val is None or (isinstance(val, str) and val.strip().upper() == "N/A"):
        return "N/A"
    try:
        if hasattr(val, "strftime"):
            return val.strftime("%m-%Y")
        if isinstance(val, str):
            dt = pd.to_datetime(val, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%m-%Y")
    except Exception:
        pass
    return str(val)


def render_revenue_insights(insights: Dict[str, Any]) -> None:
    """Renderiza insights de receita."""
    revenue = insights['revenue']
    
    # Criar ícones SVG
    trophy_icon = get_svg_icon("target", size=24, color="#fbbf24")

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(
            render_insight_card(
                "Crescimento da Receita",
                f"{revenue['growth_rate']:.1f}%",
                revenue['trend'],
                revenue['trend_icon'],
                "Comparação com o período anterior"
            ),
            unsafe_allow_html=True
        )
    
    with col2:
        # Ajuste visual para consistência; data no padrão MM-YYYY (ex: 02-2026)
        best_rev = revenue.get("best_month_revenue")
        best_month_label = _format_month_display(revenue.get("best_month", "N/A"))

        # Sempre renderizar o card; exibir "N/A" quando não houver dados válidos
        if best_rev is None or (isinstance(best_rev, float) and np.isnan(best_rev)):
            display_value = "N/A"
        else:
            display_value = f"R$ {best_rev:,.2f}"

        st.markdown(
            render_insight_card(
                "Melhor Mês",
                display_value,
                best_month_label,
                trophy_icon,
                "Mês com maior faturamento"
            ),
            unsafe_allow_html=True
        )

def render_satisfaction_insights(insights: Dict[str, Any]) -> None:
    """Renderiza insights de satisfação."""
    satisfaction = insights['satisfaction']
    
    # Se não há dados de avaliação, mostrar aviso sutil e não tentar renderizar cartões detalhados
    if not satisfaction.get("has_data", True):
        st.caption("⚠️ Ainda não há avaliações de clientes neste dataset. Os KPIs e gráficos de satisfação foram ocultados.")
        return
    
    # Criar ícones SVG
    star_icon = get_svg_icon("star", size=24, color="#fbbf24")
    warning_icon = get_svg_icon("warning", size=24, color="#ef4444")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(
            render_insight_card(
                "Satisfação Média",
                f"{satisfaction['avg_satisfaction']:.1f}/5.0",
                satisfaction['satisfaction_trend'],
                satisfaction['trend_icon'],
                "Média geral das avaliações"
            ),
            unsafe_allow_html=True
        )
    
    with col2:
        st.markdown(
            render_insight_card(
                "Avaliações 5 Estrelas",
                f"{(satisfaction['top_score_percentage']*100):.1f}%",
                "dos clientes",
                star_icon,
                "Porcentagem de notas máximas"
            ),
            unsafe_allow_html=True
        )
    
    with col3:
        st.markdown(
            render_insight_card(
                "Avaliações Baixas",
                f"{(satisfaction['lowest_score_percentage']*100):.1f}%",
                "dos clientes",
                warning_icon,
                "Porcentagem de notas mínimas"
            ),
            unsafe_allow_html=True
        )

def render_cancellation_insights(insights: Dict[str, Any]) -> None:
    """Renderiza insights de cancelamento."""
    cancellation = insights['cancellation']
    
    # Criar ícones SVG
    cancel_icon = get_svg_icon("cancel", size=24, color="#ef4444")
    
    st.markdown(f"### {cancel_icon} Cancelamentos")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(
            render_insight_card(
                "Taxa de Cancelamento",
                f"{(cancellation['cancellation_rate']*100):.1f}%",
                cancellation['cancellation_trend'],
                cancellation['trend_icon'],
                "Percentual de pedidos cancelados"
            ),
            unsafe_allow_html=True
        )
    
    with col2:
        # Criar ícone SVG para dinheiro perdido
        money_loss_icon = get_svg_icon("money_loss", size=32)
        
        st.markdown(
            render_insight_card(
                "Receita Perdida",
                f"R$ {cancellation['lost_revenue']:,.2f}",
                f"{cancellation['total_cancelled']} pedidos",
                money_loss_icon,
                "Valor total de cancelamentos"
            ),
            unsafe_allow_html=True
        )

def render_delivery_insights(insights: Dict[str, Any]) -> None:
    """Renderiza insights de entrega."""
    delivery = insights['delivery']
    stats = delivery['delivery_stats']
    
    # Criar ícones SVG
    rocket_icon = get_svg_icon("trend", size=24, color="#10b981")  # Para entregas rápidas
    warning_icon = get_svg_icon("warning", size=24, color="#ef4444")  # Para entregas atrasadas
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(
            render_insight_card(
                "Tempo Médio de Entrega",
                f"{delivery['avg_delivery_time_last_10']:.1f} dias",
                delivery['delivery_trend'],
                delivery['trend_icon'],
                "Média dos últimos 10 pedidos"
            ),
            unsafe_allow_html=True
        )
    
    # Valores com fallback para evitar KeyError
    fast_rate = stats.get("fast_rate", 0)
    fast_count = stats.get("fast_count", 0)
    slow_rate = stats.get("slow_rate", 0)
    slow_count = stats.get("slow_count", 0)
    
    with col2:
        st.markdown(
            render_insight_card(
                "Entregas Rápidas (até 7 dias)",
                f"{(fast_rate*100):.1f}%",
                f"{fast_count} pedidos",
                rocket_icon,
                "Entregas realizadas em até 7 dias"
            ),
            unsafe_allow_html=True
        )
    
    with col3:
        st.markdown(
            render_insight_card(
                "Entregas Atrasadas (>15 dias)",
                f"{(slow_rate*100):.1f}%",
                f"{slow_count} pedidos",
                warning_icon,
                "Entregas que levaram mais de 15 dias"
            ),
            unsafe_allow_html=True
        )

def render_improvement_opportunities(insights: Dict[str, Any]) -> None:
    """Renderiza oportunidades de melhoria."""
    opportunities = insights['improvement_opportunities']
    
    if opportunities:
        cols = st.columns(len(opportunities))
        
        for idx, opportunity in enumerate(opportunities):
            with cols[idx]:
                priority_colors = {
                    "Alta": "#e74c3c",
                    "Média": "#f1c40f",
                    "Baixa": "#10b981",
                }
                priority_icons = {
                    "Alta": get_svg_icon("warning", size=24, color="#e74c3c"),
                    "Média": get_svg_icon("warning", size=24, color="#f1c40f"),
                    "Baixa": get_svg_icon("trend", size=24, color="#10b981"),
                }
                
                # Formatar o valor atual baseado no tipo de métrica
                if opportunity['format_as_percentage']:
                    try:
                        current_value = f"{(float(opportunity['current'])*100):.1f}%"
                    except Exception:
                        current_value = "N/A"
                else:
                    try:
                        cur = float(opportunity['current'])
                        current_value = "N/A" if (isinstance(cur, float) and np.isnan(cur)) else f"{cur:.1f}"
                    except Exception:
                        current_value = "N/A"
                
                pr = opportunity.get("priority", "Média")
                if pr not in priority_icons:
                    pr = "Média"
                
                # Tooltip específico para Taxa de Cancelamento
                tt_text = None
                if opportunity['area'] == "Taxa de Cancelamento":
                    tt_text = "Volume de itens devolvidos/cancelados sobre o total vendido. Foco: Operação/Estoque."
                
                st.markdown(
                    render_insight_card(
                        opportunity['area'],
                        current_value,
                        f"Meta: {opportunity['goal']}",
                        priority_icons[pr],
                        f"Prioridade {pr}",
                        tooltip_text=tt_text
                    ),
                    unsafe_allow_html=True
                )

def render_overview_insights(insights: Dict[str, Any]) -> None:
    """
    Renderiza todos os insights da visão geral de forma visual.
    
    Args:
        insights: Dicionário com todos os insights calculados
    """
    render_revenue_insights(insights)
    st.markdown("---")
    render_satisfaction_insights(insights)
    st.markdown("---")
    render_cancellation_insights(insights)
    st.markdown("---")
    render_delivery_insights(insights)
    st.markdown("---")
    render_improvement_opportunities(insights)

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def generate_recovery_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera uma lista de recuperação de clientes com base em sinais de pior experiência.

    Critérios:
    1) Entregas críticas (> 15 dias) ou estagnadas (> 30 dias sem entrega)
    2) Cancelamentos
    3) Avaliações baixas (<= 2), se disponíveis

    Retorna:
        DataFrame formatado para exportação
    """
    if df.empty:
        return pd.DataFrame()

    work = df.copy()

    # Garantir colunas necessárias com defaults
    if 'pedido_cancelado' not in work.columns:
        work['pedido_cancelado'] = 0

    # Calcular delivery_time se não existir
    if 'delivery_time' not in work.columns:
        if 'order_delivered_customer_date' in work.columns and 'order_purchase_timestamp' in work.columns:
            work['order_delivered_customer_date'] = safe_to_datetime(work['order_delivered_customer_date'])
            work['order_purchase_timestamp'] = safe_to_datetime(work['order_purchase_timestamp'])
            work['delivery_time'] = (
                work['order_delivered_customer_date'] - work['order_purchase_timestamp']
            ).dt.days
        else:
            work['delivery_time'] = np.nan

    now = pd.Timestamp.now(tz="UTC")
    cutoff_stagnation = (now - pd.Timedelta(days=30)).tz_convert(None)
    purchase_ts = safe_to_datetime(work.get('order_purchase_timestamp'))

    # Critérios
    mask_late = (work['delivery_time'] > 15)
    mask_stagnated = (
        (purchase_ts < cutoff_stagnation) &
        (work.get('order_delivered_customer_date').isna()) &
        (work['pedido_cancelado'] == 0)
    )
    mask_cancel = (work['pedido_cancelado'] == 1)
    if 'review_score' in work.columns:
        mask_bad_review = (pd.to_numeric(work['review_score'], errors='coerce') <= 2)
    else:
        mask_bad_review = False

    problem_orders = work[mask_late | mask_stagnated | mask_cancel | mask_bad_review].copy()
    if problem_orders.empty:
        return pd.DataFrame()

    def _reason(row: pd.Series) -> str:
        reasons = []
        if row.get('pedido_cancelado') == 1:
            reasons.append("Cancelado")
        del_time = row.get('delivery_time')
        if pd.notna(del_time) and del_time > 15:
            reasons.append(f"Atraso ({int(del_time)} dias)")
        elif (
            pd.isna(row.get('order_delivered_customer_date')) and
            safe_to_datetime(pd.Series([row.get('order_purchase_timestamp')])).iloc[0] < cutoff_stagnation
        ):
            reasons.append("Estagnado (>30d sem entrega)")
        score = row.get('review_score')
        if pd.notna(score) and float(score) <= 2:
            reasons.append(f"Avaliação Baixa ({int(score)}★)")
        return " + ".join(reasons)

    problem_orders['Motivo_Recuperacao'] = problem_orders.apply(_reason, axis=1)

    # Garantir coluna de transportadora
    if 'transportadoraNome' not in problem_orders.columns:
        if 'carrier_name' in problem_orders.columns:
             problem_orders['transportadoraNome'] = problem_orders['carrier_name']

    # Seleção de colunas disponíveis
    export_cols = [
        'order_id',
        'customer_unique_id',
        'order_purchase_timestamp',
        'Motivo_Recuperacao',
        'customer_state',
        'customer_city',
        'product_category_name',
        'product_name',
        'transportadoraNome',
        'review_comment_message',
        'price'
    ]
    final_cols = [c for c in export_cols if c in problem_orders.columns]

    rename_map = {
        'order_id': 'ID Pedido',
        'customer_unique_id': 'ID Cliente',
        'order_purchase_timestamp': 'Data Compra',
        'Motivo_Recuperacao': 'Motivo Recuperação',
        'customer_state': 'UF',
        'customer_city': 'Cidade',
        'product_category_name': 'Categoria',
        'product_name': 'Produto',
        'transportadoraNome': 'Transportadora',
        'review_comment_message': 'Comentário',
        'price': 'Valor (R$)'
    }

    result = problem_orders[final_cols].rename(columns=rename_map)

    if 'Data Compra' in result.columns:
        result['Data Compra'] = pd.to_datetime(result['Data Compra']).dt.strftime('%d/%m/%Y')

    return result.sort_values('Data Compra', ascending=False)

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def render_customer_behavior_insights(df: pd.DataFrame) -> None:
    """
    Renderiza insights de comportamento do cliente.
    Args:
        df: DataFrame com os dados filtrados (já pode estar filtrado por categoria)
    """
    # Detectar se o df original tinha filtro de categoria aplicado
    # Se o df tem product_category_name com valores únicos = 1, significa que foi filtrado
    selected_category = None
    if "product_category_name" in df.columns and df["product_category_name"].notna().any():
        unique_categories = df["product_category_name"].dropna().unique()
        if len(unique_categories) == 1:
            selected_category = unique_categories[0]
    
    # Se o dataframe recebido estiver vazio ou sem review_score, tentar fallback GLOBAL
    # Isso cobre o caso onde o df passado já veio do fallback mas pode ter sido filtrado incorretamente antes
    target_df = df
    if target_df.empty or ("review_score" not in target_df.columns) or target_df["review_score"].isna().all():
         reviews_df = st.session_state.get("reviews_df") if "reviews_df" in st.session_state else None
         if reviews_df is not None and not reviews_df.empty:
             target_df = reviews_df.copy()
             
             # APLICAR FILTRO DE CATEGORIA se detectado no df original
             if (
                 selected_category is not None
                 and selected_category != "Todas as categorias"
                 and "product_category_name" in target_df.columns
             ):
                 target_df = target_df[target_df["product_category_name"] == selected_category].copy()
                 
                 if target_df.empty:
                     show_centered_info(f"Não há avaliações para a categoria '{selected_category}' nesta seleção.")
                     return
             
             if "review_date" in target_df.columns:
                 target_df["order_purchase_timestamp"] = pd.to_datetime(target_df["review_date"])
    
    # Validação final antes de chamar o cálculo
    if target_df.empty or ("review_score" not in target_df.columns) or target_df["review_score"].dropna().empty:
        show_centered_info("Não há avaliações suficientes para o Sistema de Avaliações.")
        return

    try:
        insights = calculate_customer_behavior_insights(target_df)
        
        render_analysis_title_with_stars("Sistema de Avaliações")

        satisfaction_data = insights['satisfaction_evolution']
        
        # Validação se há dados de evolução temporal
        if satisfaction_data.empty:
            # Tentar recuperar dados globais se a evolução temporal falhar
            if not df.empty and 'review_score' in df.columns:
                mean_score = df['review_score'].mean()
                std_score = df['review_score'].std()
                count_score = len(df)
                latest_satisfaction = pd.Series({
                    'mean': mean_score,
                    'std': std_score,
                    'count': count_score
                })
            else:
                show_centered_info("Não há dados suficientes para análise temporal de satisfação.")
                return
        else:
            latest_satisfaction = satisfaction_data.iloc[-1]
            
        distribution = insights['review_distribution']

        # ------------------------------------------------------------
        # Normalizar distribuição (robusto para chaves "5", 5.0, Decimal etc.)
        # Objetivo: garantir dict[int, float] com chaves 1..5 (percentuais)
        # ------------------------------------------------------------
        def _normalize_review_distribution(dist_obj):
            try:
                if dist_obj is None:
                    return {i: 0.0 for i in range(1, 6)}

                if isinstance(dist_obj, dict):
                    if not dist_obj:
                        return {i: 0.0 for i in range(1, 6)}
                    s = pd.Series(dist_obj)
                elif isinstance(dist_obj, pd.Series):
                    s = dist_obj.copy()
                else:
                    return {i: 0.0 for i in range(1, 6)}

                if s is None or s.empty:
                    return {i: 0.0 for i in range(1, 6)}

                idx = pd.to_numeric(pd.Index(s.index), errors="coerce")
                vals = pd.to_numeric(pd.Series(s.values), errors="coerce")
                tmp = pd.DataFrame({"score": idx, "v": vals}).dropna()
                if tmp.empty:
                    return {i: 0.0 for i in range(1, 6)}

                tmp["score"] = tmp["score"].round().astype(int).clip(1, 5)
                grouped = tmp.groupby("score")["v"].sum()

                # Se vier como contagem, normalizar para percentual
                total = float(grouped.sum() or 0.0)
                if total > 0:
                    grouped = grouped / total

                return {i: float(grouped.get(i, 0.0) or 0.0) for i in range(1, 6)}
            except Exception:
                return {i: 0.0 for i in range(1, 6)}

        distribution = _normalize_review_distribution(distribution)

        # Fonte única e consistente (evita mismatch de tipos no modo Supabase/DuckDB):
        # recalcular distribuição diretamente do target_df, igual ao histograma.
        try:
            _scores = pd.to_numeric(target_df.get("review_score"), errors="coerce").dropna()
            _scores = _scores.round().astype(int).clip(1, 5)
            _dist = _scores.value_counts(normalize=True).sort_index()
            distribution = {i: float(_dist.get(i, 0.0) or 0.0) for i in range(1, 6)}
        except Exception:
            distribution = {i: 0.0 for i in range(1, 6)}

        # Total de avaliações (para contagem por estrela nos cards)
        try:
            total_reviews = int(pd.to_numeric(target_df.get("review_score"), errors="coerce").dropna().shape[0])
        except Exception:
            total_reviews = int(len(target_df))
        correlations = insights['correlations']

        # PRIORIDADE 1: Link direto (reviews enriquecidos com dados do pedido)
        # Observação: quando link_rate cai (ex.: muitas reviews Shopee sem order_code),
        # ainda precisamos cair para o fallback macro mensal; antes isso não acontecia.
        reviews_linked = st.session_state.get("reviews_linked", False)
        reviews_df_enriched = st.session_state.get("reviews_df")
        direct = None

        if reviews_linked and reviews_df_enriched is not None and not reviews_df_enriched.empty:
            direct = _compute_direct_correlations(reviews_df_enriched)
            # Preferir correlação direta somente quando a maioria está linkada
            if direct.get("link_rate", 0) > 0.5:
                correlations = {
                    "ticket_vs_satisfaction": direct["ticket_vs_satisfaction"],
                    "delivery_vs_satisfaction": direct["delivery_vs_satisfaction"],
                    "repurchase_vs_satisfaction": direct["repurchase_vs_satisfaction"],
                    "freight_vs_satisfaction": direct.get("freight_vs_satisfaction", {"correlation": 0, "trend": "indisponivel"}),
                    "_meta": {
                        "method": "direct_link",
                        "link_rate": direct.get("link_rate", 0),
                        "sample_size": direct.get("sample_size", 0),
                    },
                    "_category_satisfaction": direct.get("category_satisfaction", []),
                }

        # PRIORIDADE 2: Fallback macro (mensal) — funciona mesmo sem link review→pedido
        # Critérios para tentar:
        # - colunas diretas ausentes no target_df OU
        # - existe direct mas link_rate insuficiente (<= 50%)
        needs_macro = False
        for needed_col in ["price", "customer_unique_id", "order_delivered_customer_date"]:
            if needed_col not in target_df.columns:
                needs_macro = True
                break
        if direct is not None and direct.get("link_rate", 0) <= 0.5:
            needs_macro = True

        if needs_macro:
            orders_df = st.session_state.get("df_all") if "df_all" in st.session_state else None
            if isinstance(orders_df, pd.DataFrame) and not orders_df.empty:
                macro = _compute_macro_correlations_by_month(target_df, orders_df)
                # Só substituir se macro for utilizável (>= min_points)
                if macro.get("points_used", 0) >= macro.get("min_points", 3):
                    correlations = {
                        "ticket_vs_satisfaction": macro["ticket_vs_satisfaction"],
                        "delivery_vs_satisfaction": macro["delivery_vs_satisfaction"],
                        "repurchase_vs_satisfaction": macro["repurchase_vs_satisfaction"],
                        "_meta": {
                            "method": macro.get("method", "macro_monthly"),
                            "points_used": macro.get("points_used", 0),
                        },
                    }
                else:
                    correlations = dict(correlations)
                    correlations["_meta"] = {
                        "method": "macro_monthly_insufficient",
                        "points_used": macro.get("points_used", 0),
                    }
            
        chart_icon = get_svg_icon("chart", size=24)
        money_icon = get_svg_icon("money", size=24)
        trend_icon = get_svg_icon("trend", size=24)
        cycle_icon = get_svg_icon("cycle", size=24)
        star_icon = get_svg_icon("star", size=24, color="#fbbf24")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                render_insight_card(
                    "Distribuição das Avaliações",
                    f"{star_icon} {latest_satisfaction['mean']:.2f}/5.0",
                    f"{(distribution.get(5,0)*100):.1f}% nota máxima",
                    chart_icon,
                    f"{(distribution.get(1,0)*100):.1f}% nota mínima"
                ),
                unsafe_allow_html=True
            )
    except Exception as e:
        # Show actual error for debugging
        st.error(f"❌ Erro ao renderizar insights: {e}")
        import traceback
        st.code(traceback.format_exc())
        show_centered_info("Não há avaliações suficientes para o Sistema de Avaliações.")
        return

    with col2:
        st.markdown(
            render_insight_card(
                "Tendências",
                f"Desvio Padrão: {latest_satisfaction['std']:.2f}",
                f"Variabilidade {'alta' if latest_satisfaction['std'] > 1 else 'baixa'}",
                trend_icon,
                f"{latest_satisfaction['count']} avaliações"
            ),
            unsafe_allow_html=True
        )
    dist_cols = st.columns(5)
    for i in range(1, 6):
        with dist_cols[i-1]:
            stars_html = "".join([get_svg_icon("star", size=16, color="#fbbf24") for _ in range(i)])
            percent = float(distribution.get(i, 0.0) or 0.0)
            count = int(round(percent * total_reviews))
            if count == 0:
                msg = "Sem avaliações para esta nota"
            else:
                msg = f"{count} avaliações"
            st.markdown(
                render_insight_card(
                    f"{i} Estrela{'s' if i > 1 else ''}",
                    f"{percent*100:.1f}%",
                    "",
                    stars_html,
                    msg,
                    "#fbbf24"  # Cor dourada das estrelas
                ),
                unsafe_allow_html=True
            )
    st.markdown("---")
    # Nova seção: Análise de Correlações
    render_kpi_title("Análise de Correlações")

    meta = correlations.get("_meta", {}) if isinstance(correlations, dict) else {}
    if meta.get("method") == "macro_monthly":
        st.caption(f"Correlação macro (mensal) entre satisfação média (reviews) e métricas operacionais (pedidos). Pontos: {meta.get('points_used', 0)} meses.")
    
    # Cards de correlação na nova seção
    corr_analysis_cols = st.columns(3)
    with corr_analysis_cols[0]:
        ticket_corr = correlations['ticket_vs_satisfaction']
        if ticket_corr['correlation'] != 0:
            if ticket_corr['correlation'] > 0:
                correlation_type = "Positiva"
                ticket_insight = "Clientes mais satisfeitos têm ticket maior"
                ticket_message = "Investimento maior leva a maior satisfação"
            else:
                correlation_type = "Negativa"
                ticket_insight = "Clientes mais satisfeitos têm ticket menor"
                ticket_message = "Produtos baratos geram mais satisfação"
        else:
            correlation_type = "Não Disponível"
            ticket_insight = "Dados de preço não disponíveis"
            ticket_message = "Use dados completos de pedidos para ver esta correlação"
            
        st.markdown(
            render_insight_card(
                "Ticket Médio vs Satisfação",
                f"Correlação {correlation_type}",
                ticket_insight,
                money_icon,
                ticket_message
            ),
            unsafe_allow_html=True
        )
    with corr_analysis_cols[1]:
        delivery_icon = get_svg_icon("truck", size=24)
        delivery_corr = correlations['delivery_vs_satisfaction']
        if delivery_corr['correlation'] != 0:
            # Correlação negativa = entregas rápidas têm avaliações melhores (CORRETO)
            # Correlação positiva = entregas lentas têm avaliações melhores (INCORRETO)
            if delivery_corr['correlation'] < 0:
                correlation_type = "Negativa"
                delivery_insight = "Entregas mais rápidas têm avaliações melhores"
                delivery_message = "Velocidade de entrega impacta diretamente na satisfação"
            else:
                correlation_type = "Positiva" 
                delivery_insight = "Entregas mais lentas têm avaliações melhores"
                delivery_message = "Pode indicar problema na logística"
        else:
            correlation_type = "Não detectada"
            delivery_insight = "Dados insuficientes para análise"
            delivery_message = "Necessário mais dados de entrega"
            
        st.markdown(
            render_insight_card(
                "Entrega vs Satisfação",
                f"Correlação {correlation_type}",
                delivery_insight,
                delivery_icon,
                delivery_message
            ),
            unsafe_allow_html=True
        )
    with corr_analysis_cols[2]:
        repurchase_corr = correlations['repurchase_vs_satisfaction']
        if repurchase_corr['correlation'] != 0:
            if repurchase_corr['correlation'] > 0:
                correlation_type = "Positiva"
                repurchase_insight = "Clientes mais satisfeitos compram mais vezes"
                repurchase_message = "Satisfação gera fidelização e recompra"
            else:
                correlation_type = "Negativa"
                repurchase_insight = "Clientes insatisfeitos têm menor recompra"
                repurchase_message = "Notas baixas reduzem taxa de retenção"
        else:
            correlation_type = "Não Disponível"
            repurchase_insight = "Dados de cliente não disponíveis"
            repurchase_message = "Use dados completos de pedidos para ver esta correlação"
            
        st.markdown(
            render_insight_card(
                "Recompra vs Satisfação",
                f"Correlação {correlation_type}",
                repurchase_insight,
                cycle_icon,
                repurchase_message
            ),
            unsafe_allow_html=True
        )

def calculate_bcg_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas BCG (growth_rate e market_share) por categoria. Otimizado com DuckDB.
    """
    if df.empty or 'product_category_name' not in df.columns or 'price' not in df.columns:
        return pd.DataFrame()
    
    total_revenue = df['price'].sum()
    if total_revenue == 0:
        return pd.DataFrame()

    if len(df) < 10000:
        # --- PANDAS (Original) ---
        try:
            df_not_cancelled = df[df["pedido_cancelado"] == 0].copy() if 'pedido_cancelado' in df.columns else df.copy()
            df_not_cancelled['month'] = safe_to_datetime(df_not_cancelled['order_purchase_timestamp']).dt.to_period('M')
            
            monthly_category_revenue = (
                df_not_cancelled.groupby(['month', 'product_category_name'])['price']
                .sum()
                .reset_index()
            )
            
            bcg_metrics = []
            
            for category in df['product_category_name'].unique():
                # Market Share
                category_revenue = df[df['product_category_name'] == category]['price'].sum()
                market_share = (category_revenue / total_revenue * 100) if total_revenue > 0 else 0
                
                # Growth Rate (usando mesma lógica do calculate_revenue_insights)
                category_monthly = monthly_category_revenue[
                    monthly_category_revenue['product_category_name'] == category
                ]
                
                if len(category_monthly) >= 2:
                    n_months = min(3, len(category_monthly))
                    old_mean = category_monthly['price'].head(n_months).mean()
                    recent_mean = category_monthly['price'].tail(n_months).mean()
                    growth_rate = ((recent_mean - old_mean) / old_mean) * 100 if old_mean > 0 else 0
                else:
                    growth_rate = 0
                
                bcg_metrics.append({
                    'category': category,
                    'market_share': market_share,
                    'growth_rate': growth_rate
                })
            
            return pd.DataFrame(bcg_metrics)
        except Exception:
            return pd.DataFrame()
    else:
        # --- DUCKDB (Otimizado com Window Functions) ---
        try:
            db = get_db()
            db.conn.register('temp_bcg', df)
            
            # 1. Agregação mensal por categoria
            monthly_query = """
                WITH monthly_sales AS (
                    SELECT 
                        product_category_name,
                        strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as month,
                        SUM(price) as revenue
                    FROM temp_bcg
                    WHERE product_category_name IS NOT NULL AND pedido_cancelado = 0
                    GROUP BY 1, 2
                ),
                category_total AS (
                    SELECT product_category_name, SUM(price) as total_rev
                    FROM temp_bcg
                    WHERE product_category_name IS NOT NULL
                    GROUP BY 1
                ),
                growth_calc AS (
                    SELECT 
                        m.product_category_name,
                        -- Usar Window Function para pegar média dos últimos 3 vs primeiros 3
                        -- Simplificação: Pegar crescimento do último mês vs média móvel anterior para performance
                        -- Ou replicar lógica exata:
                        AVG(revenue) OVER (PARTITION BY product_category_name ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as avg_rev
                    FROM monthly_sales m
                )
                SELECT 
                    ct.product_category_name as category,
                    (ct.total_rev / (SELECT SUM(price) FROM temp_bcg)) * 100 as market_share
                FROM category_total ct
            """
            # Nota: O cálculo exato de growth rate "head(3) vs tail(3)" é complexo em SQL puro sem subqueries pesadas.
            # Vamos buscar os dados mensais agregados e fazer o loop final em Python (muito mais rápido que agrupar do zero)
            
            # Passo 1: Dados mensais agregados (reduz dataset de 1M linhas para ~500 linhas)
            agg_query = """
                SELECT 
                    product_category_name,
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as month,
                    SUM(price) as price
                FROM temp_bcg
                WHERE product_category_name IS NOT NULL AND pedido_cancelado = 0
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            monthly_df = db.query(agg_query)
            
            # Passo 2: Total por categoria para Market Share
            share_query = """
                SELECT 
                    product_category_name as category,
                    SUM(price) as category_revenue
                FROM temp_bcg
                WHERE product_category_name IS NOT NULL
                GROUP BY 1
            """
            share_df = db.query(share_query)
            
            db.conn.unregister('temp_bcg')
            
            # Passo 3: Finalizar em Python (rápido com dados agregados)
            if share_df.empty: return pd.DataFrame()
            
            bcg_metrics = []
            for cat in share_df['category']:
                cat_data = monthly_df[monthly_df['product_category_name'] == cat]
                cat_rev = share_df[share_df['category'] == cat]['category_revenue'].iloc[0]
                
                market_share = (cat_rev / total_revenue * 100)
                
                growth_rate = 0
                if len(cat_data) >= 2:
                    n = min(3, len(cat_data))
                    old = cat_data['price'].head(n).mean()
                    recent = cat_data['price'].tail(n).mean()
                    growth_rate = ((recent - old) / old * 100) if old > 0 else 0
                
                bcg_metrics.append({
                    'category': cat,
                    'market_share': market_share,
                    'growth_rate': growth_rate
                })
                
            return pd.DataFrame(bcg_metrics)

        except Exception as e:
            print(f"Erro DuckDB BCG: {e}")
            return calculate_bcg_metrics.__wrapped__(df) # Fallback seguro

def get_strategic_analysis(row: pd.Series) -> Dict[str, Any]:
    """
    Gera uma análise estratégica completa por categoria, incluindo:
    - key_insight: a ação/alerta mais urgente (10s)
    - recommendations_list: lista de recomendações detalhadas
    """
    quadrant = row.get('bcg_quadrant', 'Indefinido')
    score = float(row.get('composite_score', 0) or 0)
    satisfaction = float(row.get('avg_satisfaction', 0) or 0)
    growth = float(row.get('growth_rate', 0) or 0)
    ticket = float(row.get('avg_ticket', 0) or 0)

    recommendations_list: List[str] = []
    key_insight = ""

    if quadrant == 'Estrela Digital':
        key_insight = "Manter investimento pesado para solidificar liderança."
        recommendations_list.append("Ação: Alocar capital (Marketing/Estoque) para acompanhar o alto crescimento e defender o market share.")

        if 0 < satisfaction < 4.0:
            key_insight = "⚠️ Alerta de Satisfação: Otimizar logística/qualidade AGORA. O crescimento está impactando a avaliação."
            recommendations_list.append(f"Alerta: A satisfação ({satisfaction:.2f}/5.0) está abaixo do ideal. O crescimento rápido pode estar impactando qualidade/entrega.")
        elif growth > 100:
            key_insight = f"🔥 Hiper-Crescimento ({growth:.0f}%): Dobrar foco em estoque e marketing para capturar a demanda."
            recommendations_list.append("Insight: Crescimento explosivo. Garantir que a cadeia de suprimentos aguenta a demanda.")
        elif score > 0.5:
            recommendations_list.append("Insight: Score alto sugere potencial para virar 'Vaca Leiteira' dominante.")

    elif quadrant == 'Vaca Leiteira':
        key_insight = "Otimizar margem e colher lucros para reinvestir."
        recommendations_list.append("Ação: Reduzir custo de aquisição e focar em lucratividade. Financiar 'Estrelas' e 'Interrogações'.")

        if growth < -25:
            key_insight = f"📉 Risco de Declínio ({growth:.0f}%): Focar em retenção AGORA para não virar 'Abacaxi'."
            recommendations_list.append(f"Alerta: Crescimento negativo ({growth:.1f}%). Monitorar de perto. Reativar base com fidelidade.")
        elif ticket > 200:
            key_insight = f"💎 Oportunidade de Margem (Ticket R$ {ticket:.0f}): Focar em upsell e cross-sell."
            recommendations_list.append(f"Insight: Ticket Médio (R$ {ticket:.2f}) alto. Focar em upsell e cross-sell para aumentar margem.")

    elif quadrant == 'Interrogação':
        key_insight = "🧪 Testar Mercado: Investimento controlado para validar potencial."
        recommendations_list.append("Ação: Investimento de teste (marketing/promos) para ganhar market share com prazo e orçamento definidos.")

        if score > 0.35 and satisfaction > 4.1:
            key_insight = f"🎯 Ouro! (Score {score:.2f} | Aval {satisfaction:.1f}). Produto validado. Investir para ganhar share."
            recommendations_list.append("Insight: Alto potencial (score e satisfação altos). Precisa de visibilidade (nichos e reviews).")
        elif growth < 0:
            key_insight = f"🚫 Sinal Vermelho: Crescimento negativo ({growth:.0f}%) em 'Interrogação'. Reavaliar ou cortar."
            recommendations_list.append("Alerta: Potencial de risco. Crescimento negativo. Avaliar produto/preço vs mercado antes de investir.")

    elif quadrant == 'Abacaxi':
        key_insight = "✂️ Ação Imediata: Descontinuar e liquidar estoque para liberar capital."
        recommendations_list.append("Ação: Minimizar perdas e liberar capital (estoque). Parar investimento de marketing.")
        recommendations_list.append("Ação: Fazer liquidação/bundles com 'Vaca Leiteira' para limpar inventário.")

        if satisfaction > 4.2 and score > 0.18:
            key_insight = f"🤔 Nicho Oculto? (Aval {satisfaction:.1f}). Clientes amam, mas não vende. Tentar bundle antes de cortar."
            recommendations_list.append("Insight: Apesar do volume baixo, satisfação alta. Avaliar nicho de cauda longa antes de eliminar.")

    else:
        key_insight = "Dados insuficientes para um insight."
        recommendations_list = ["Não foi possível gerar recomendações."]

    return {
        'key_insight': key_insight,
        'recommendations_list': recommendations_list,
    }



def get_strategic_analysis(row: pd.Series, quadrant: str, rules: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Return prescriptive plan for a category/row using contextual rules.

    The first matching rule for the quadrant is applied. Falls back to a
    generic guidance if no rule matches.
    """
    try:
        from utils.rules import load_strategic_rules, select_prescriptive_rule
    except Exception:  # Very defensive: return minimal default
        return {
            'key_insight': 'Analyser categoria manualmente: regras indisponíveis.',
            'plano_capital': [],
            'plano_operacional': [],
            'plano_mercado': []
        }

    rules_cfg = rules or load_strategic_rules()

    context = {
        'avg_ticket': float(row.get('avg_ticket', 0) or 0),
        'total_items': float(row.get('total_items', 0) or 0),
        'unique_orders': float(row.get('unique_orders', 0) or 0),
        'satisfaction': float(row.get('avg_satisfaction', 0) or 0),
        'growth_rate': float(row.get('growth_rate', 0) or 0),
        'market_share': float(row.get('market_share', 0) or 0),
        'composite_score': float(row.get('composite_score', 0) or 0),
        'is_high_value_tier': bool(row.get('is_high_value_tier', False)),
        'is_high_volume_tier': bool(row.get('is_high_volume_tier', False)),
    }
    tier = row.get('tier', 'baseline')
    if isinstance(tier, str):
        context['tier'] = tier
    else:
        context['tier'] = 'baseline'

    matched = select_prescriptive_rule(str(quadrant), context, rules_cfg)
    if matched:
        return {
            'key_insight': matched.get('key_insight', ''),
            'plano_capital': matched.get('plano_capital', []) or [],
            'plano_operacional': matched.get('plano_operacional', []) or [],
            'plano_mercado': matched.get('plano_mercado', []) or [],
        }

    # Fallback por quadrante
    defaults = {
        'Estrela Digital': {
            'key_insight': 'Manter liderança com foco em retenção e margem.',
            'plano_capital': ['Continuar investimento eficiente de marketing.'],
            'plano_operacional': ['Revisar SLA e custos logísticos.'],
            'plano_mercado': ['Programas de fidelidade e defesa de share.'],
        },
        'Vaca Leiteira': {
            'key_insight': 'Maximizar lucratividade enquanto estabilidade permanece.',
            'plano_capital': ['Realocar verba para iniciativas de margem.'],
            'plano_operacional': ['Reduzir desperdícios e rupturas.'],
            'plano_mercado': ['Ações de recompra e cross-sell.'],
        },
        'Interrogação': {
            'key_insight': 'Testar hipóteses de crescimento com disciplina.',
            'plano_capital': ['Micro-orçamentos com metas claras.'],
            'plano_operacional': ['Pilotos controlados de fulfillment.'],
            'plano_mercado': ['Campanhas de validação de PMF.'],
        },
        'Abacaxi': {
            'key_insight': 'Desinvestir gradualmente e realocar capital.',
            'plano_capital': ['Cortar investimentos e liquidar inventário.'],
            'plano_operacional': ['Encerrar SKUs de baixa margem.'],
            'plano_mercado': ['Comunicar saída planejada.'],
        },
    }
    return defaults.get(str(quadrant), {
        'key_insight': 'Sem regra correspondente.',
        'plano_capital': [],
        'plano_operacional': [],
        'plano_mercado': [],
    })


def analyze_category_performance(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analisa o desempenho das categorias com base em múltiplas métricas.
    Agora inclui classificação BCG híbrida.
    
    Args:
        df: DataFrame com os dados filtrados
        
    Returns:
        Dict com insights sobre categorias, incluindo:
        - top_categories: Categorias com melhor desempenho
        - bottom_categories: Categorias que precisam de atenção
        - category_metrics: Métricas detalhadas por categoria
        - bcg_classification: Classificação BCG híbrida
    """
    # Check if required columns exist
    if df.empty:
        return {
            'top_categories': pd.DataFrame(),
            'bottom_categories': pd.DataFrame(),
            'category_metrics': pd.DataFrame()
        }
    
    required_columns = ['product_category_name', 'price', 'order_purchase_timestamp']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        # Return empty results if required columns are missing
        return {
            'top_categories': pd.DataFrame(),
            'bottom_categories': pd.DataFrame(),
            'category_metrics': pd.DataFrame()
        }

    # Higieniza categorias inválidas para evitar cards/filtros com "nan"/"0"
    df = df.copy()
    cat_series = df["product_category_name"].astype(str).str.strip()
    invalid_mask = cat_series.str.lower().isin({"", "nan", "none", "null", "0"})
    df["product_category_name"] = cat_series.mask(invalid_mask, "Sem Categoria")
    
    # Calcular tempo de entrega apenas se a coluna existir
    if 'order_delivered_customer_date' in df.columns:
        df['delivery_time'] = (
            safe_to_datetime(df['order_delivered_customer_date']) - 
            safe_to_datetime(df['order_purchase_timestamp'])
        ).dt.days
    else:
        df['delivery_time'] = 0  # Default value
    
    # Prepare aggregation dictionary with only available columns
    agg_dict = {
        'total_revenue': ('price', 'sum'),
        'avg_ticket': ('price', 'mean'),
        'total_items': ('price', 'count'),
        'unique_orders': ('order_id', 'nunique') if 'order_id' in df.columns else ('price', 'count'),
        'unique_customers': ('customer_unique_id', 'nunique') if 'customer_unique_id' in df.columns else ('price', 'count'),
        'avg_delivery_time': ('delivery_time', 'mean'),
    }
    
    # Add optional columns if they exist
    if 'review_score' in df.columns:
        agg_dict.update({
            'avg_satisfaction': ('review_score', 'mean'),
            'total_reviews': ('review_score', 'count')
        })
    
    if 'pedido_cancelado' in df.columns:
        agg_dict['cancellation_rate'] = ('pedido_cancelado', 'mean')
    
    # Agrupar dados por categoria (métricas base, sem dependências opcionais)
    base_metrics = (
        df.groupby('product_category_name')
        .agg(**agg_dict)
        .reset_index()
        .rename(columns={'product_category_name': 'category'})
    )
    
    # Fill missing columns with default values
    if 'avg_satisfaction' not in base_metrics.columns:
        base_metrics['avg_satisfaction'] = 0
    if 'total_reviews' not in base_metrics.columns:
        base_metrics['total_reviews'] = 0
    if 'cancellation_rate' not in base_metrics.columns:
        base_metrics['cancellation_rate'] = 0

    # Adicionar pagamentos se a coluna existir; caso contrário, usar fallback seguro
    if 'payment_value' in df.columns:
        payments = (
            df.groupby('product_category_name')['payment_value']
            .agg(total_payment='sum', avg_payment='mean')
            .reset_index()
            .rename(columns={'product_category_name': 'category'})
        )
        category_metrics = base_metrics.merge(payments, on='category', how='left')
    else:
        category_metrics = base_metrics.copy()
        category_metrics['total_payment'] = category_metrics['total_revenue']
        category_metrics['avg_payment'] = category_metrics['avg_ticket']
    
    # Calcular métricas adicionais
    category_metrics['revenue_per_customer'] = category_metrics['total_revenue'] / category_metrics['unique_customers']
    category_metrics['items_per_order'] = category_metrics['total_items'] / category_metrics['unique_orders']
    category_metrics['review_rate'] = category_metrics['total_reviews'] / category_metrics['unique_orders']

    # Tiers: high value e high volume via percentis p80
    try:
        from utils.rules import load_strategic_rules
        rules_cfg = load_strategic_rules()
        p_val = float(rules_cfg.get('TIER_DEFINITIONS', {}).get('value_percentile', 80)) / 100.0
        p_vol = float(rules_cfg.get('TIER_DEFINITIONS', {}).get('volume_percentile', 80)) / 100.0
    except Exception:
        p_val, p_vol = 0.8, 0.8

    if not category_metrics.empty:
        val_thr = category_metrics['avg_ticket'].quantile(p_val)
        vol_thr = category_metrics['total_items'].quantile(p_vol)
        is_high_value = category_metrics['avg_ticket'] >= val_thr
        is_high_volume = category_metrics['total_items'] >= vol_thr
        category_metrics['is_high_value_tier'] = is_high_value.fillna(False)
        category_metrics['is_high_volume_tier'] = is_high_volume.fillna(False)
        category_metrics['tier'] = 'baseline'
        category_metrics.loc[is_high_value & ~is_high_volume, 'tier'] = 'high_value_tier'
        category_metrics.loc[~is_high_value & is_high_volume, 'tier'] = 'high_volume_tier'
        category_metrics.loc[is_high_value & is_high_volume, 'tier'] = 'high_value_and_volume'
    else:
        category_metrics['is_high_value_tier'] = False
        category_metrics['is_high_volume_tier'] = False
        category_metrics['tier'] = 'baseline'
    
    # Enriquecer com avaliações agregadas externas (reviews_df) sem mexer no DF de pedidos
    if st is not None:
        try:
            reviews_df = st.session_state.get("reviews_df") if "reviews_df" in st.session_state else None
            if reviews_df is not None and not reviews_df.empty:
                # Respeitar filtros globais (período e marketplace) quando reviews estão em session_state
                try:
                    selected_mps = st.session_state.get("selected_marketplaces", [])
                    if isinstance(selected_mps, list) and len(selected_mps) > 0 and "marketplace" in reviews_df.columns:
                        reviews_df = reviews_df[reviews_df["marketplace"].isin(selected_mps)].copy()
                except Exception:
                    pass
                try:
                    date_range = st.session_state.get("current_date_range")
                    if date_range:
                        reviews_df = filter_reviews_by_period(reviews_df, date_range)
                except Exception:
                    pass

                # fallback de nomenclatura para categoria
                if "product_category_name" not in reviews_df.columns and "category_name" in reviews_df.columns:
                    reviews_df = reviews_df.assign(product_category_name=reviews_df["category_name"])
                if "product_category_name" not in reviews_df.columns:
                    raise KeyError("reviews_df precisa de 'product_category_name' para agregar avaliações por categoria")

            rev = reviews_df[["product_category_name", "review_score"]].copy()
            rev["review_score"] = pd.to_numeric(rev["review_score"], errors="coerce")
            rev = rev.dropna(subset=["review_score"])
            if not rev.empty:
                cat_rev = rev.groupby("product_category_name")["review_score"].agg(
                    avg_satisfaction_ext="mean",
                    total_reviews_ext="count",
                ).reset_index()
                category_metrics = category_metrics.merge(
                    cat_rev,
                    left_on="category",
                    right_on="product_category_name",
                    how="left",
                )
                # Priorizar nota externa quando existir
                category_metrics["avg_satisfaction"] = category_metrics["avg_satisfaction"].where(
                    category_metrics["avg_satisfaction"] > 0,
                    category_metrics["avg_satisfaction_ext"]
                )
                category_metrics["total_reviews"] = category_metrics["total_reviews"].where(
                    category_metrics["total_reviews"] > 0,
                    category_metrics["total_reviews_ext"]
                )
                category_metrics.drop(
                    columns=[c for c in ["product_category_name", "avg_satisfaction_ext", "total_reviews_ext"] if c in category_metrics.columns],
                    inplace=True,
                    errors="ignore",
                )
                category_metrics = category_metrics.fillna(0)
        except Exception:
            pass  # fallback silencioso

    # 1. Calcular métricas BCG (growth_rate e market_share)
    bcg_metrics = calculate_bcg_metrics(df)
    
    # 2. Merge com category_metrics para ter growth e share ANTES do score
    if not bcg_metrics.empty and 'category' in bcg_metrics.columns:
        category_metrics = category_metrics.merge(bcg_metrics, on='category', how='left')
    
    category_metrics['growth_rate'] = pd.to_numeric(category_metrics.get('growth_rate', 0), errors='coerce').fillna(0)
    category_metrics['market_share'] = pd.to_numeric(category_metrics.get('market_share', 0), errors='coerce').fillna(0)

    # 3. Normalizar métricas usando as 11 definidas em COMPOSITE_SCORE_WEIGHTS
    # Mapeamento: Coluna Interna -> Chave no config.py
    column_mapping = {
        'total_revenue': 'total_revenue',
        'avg_ticket': 'avg_price',
        'avg_satisfaction': 'avg_rating',
        'total_items': 'units_sold',
        'cancellation_rate': 'cancellation_rate',
        'total_payment': 'payment_value',
        'items_per_order': 'items_per_order',
        'review_rate': 'review_rate',
        'avg_payment': 'avg_payment',
        'growth_rate': 'revenue_growth',
        'market_share': 'market_share'
    }

    metrics_to_normalize = list(column_mapping.keys())
    
    # Criar colunas de score normalizadas (Min-Max)
    for col in metrics_to_normalize:
        series = pd.to_numeric(category_metrics.get(col, 0), errors="coerce").fillna(0)
        
        # Inverter cancelamento (quanto menor, melhor)
        if col == 'cancellation_rate':
            series = series.max() - series
            
        c_min, c_max = series.min(), series.max()
        if c_max > c_min:
            category_metrics[f'{col}_score'] = (series - c_min) / (c_max - c_min)
        else:
            category_metrics[f'{col}_score'] = 0.5

    # 4. Calcular score composto unificado
    total_w = sum(COMPOSITE_SCORE_WEIGHTS.get(w_key, 0) for w_key in column_mapping.values())
    category_metrics['composite_score'] = sum(
        category_metrics[f'{col}_score'] * (COMPOSITE_SCORE_WEIGHTS.get(column_mapping[col], 0) / total_w)
        for col in metrics_to_normalize
    )

    # -----------------------------
    # Tratativa de outliers (growth)
    # -----------------------------
    # Mantém valor raw para auditoria/tooltip, e usa growth tratado para
    # thresholds/plot/estratégia (evita picos tipo 10k% dominarem percentis e escala).
    if "growth_rate" in category_metrics.columns:
        if "growth_rate_raw" not in category_metrics.columns:
            category_metrics["growth_rate_raw"] = category_metrics["growth_rate"]

        try:
            mode = str(st.session_state.get("growth_outlier_mode", "winsor") or "winsor").lower()
        except Exception:
            mode = "winsor"
        try:
            abs_cap = float(st.session_state.get("growth_outlier_abs_cap", 1000.0))
        except Exception:
            abs_cap = 1000.0
        try:
            p_low = int(st.session_state.get("growth_outlier_winsor_low", 2))
        except Exception:
            p_low = 2
        try:
            p_high = int(st.session_state.get("growth_outlier_winsor_high", 98))
        except Exception:
            p_high = 98

        s = pd.to_numeric(category_metrics["growth_rate"], errors="coerce").replace([np.inf, -np.inf], np.nan)
        if mode == "none":
            category_metrics["growth_rate"] = s.fillna(0)
        elif mode == "cap":
            category_metrics["growth_rate"] = s.clip(lower=-abs_cap, upper=abs_cap).fillna(0)
        else:
            valid = s.dropna()
            if len(valid) == 0:
                category_metrics["growth_rate"] = s.fillna(0)
            else:
                p_low = max(0, min(49, p_low))
                p_high = max(51, min(100, p_high))
                if p_low >= p_high:
                    p_low, p_high = 2, 98
                lo = float(np.nanpercentile(valid, p_low))
                hi = float(np.nanpercentile(valid, p_high))
                category_metrics["growth_rate"] = (
                    s.clip(lower=lo, upper=hi)
                    .clip(lower=-abs_cap, upper=abs_cap)
                    .fillna(0)
                )

    # Normalizar growth_rate e market_share para classificação BCG
    if not category_metrics.empty:
        category_metrics['growth_norm'] = (
            (category_metrics['growth_rate'] - category_metrics['growth_rate'].min()) / 
            (category_metrics['growth_rate'].max() - category_metrics['growth_rate'].min())
        ).fillna(0.5)
        
        category_metrics['market_share_norm'] = (
            (category_metrics['market_share'] - category_metrics['market_share'].min()) / 
            (category_metrics['market_share'].max() - category_metrics['market_share'].min())
        ).fillna(0.5)
        
        # Classificação BCG Híbrida com thresholds via percentis (default: mediana 50º)
        try:
            growth_pct = int(st.session_state.get('bcg_growth_percentile', 50))
        except Exception:
            growth_pct = 50
        try:
            share_pct = int(st.session_state.get('bcg_share_percentile', 50))
        except Exception:
            share_pct = 50

        growth_series = category_metrics['growth_rate'].replace([np.inf, -np.inf], np.nan).dropna()
        share_series = category_metrics['market_share'].replace([np.inf, -np.inf], np.nan).dropna()

        growth_threshold = float(np.nanpercentile(growth_series, growth_pct)) if len(growth_series) else 0.0
        market_share_threshold = float(np.nanpercentile(share_series, share_pct)) if len(share_series) else 0.0

        def classify_bcg_hybrid(row):
            composite_score = row['composite_score']
            growth_rate = row.get('growth_rate', 0)
            market_share = row.get('market_share', 0)

            is_high_growth = growth_rate >= growth_threshold
            is_high_market = market_share >= market_share_threshold

            if is_high_growth and is_high_market:
                return 'Estrela Digital'
            elif (not is_high_growth) and is_high_market:
                return 'Vaca Leiteira'
            elif is_high_growth and (not is_high_market):
                return 'Interrogação'
            else:
                if composite_score < 0.2:
                    return 'Abacaxi'
                return 'Interrogação'
        
        category_metrics['bcg_quadrant'] = category_metrics.apply(classify_bcg_hybrid, axis=1)
        
        # Estratégias específicas por quadrante
        def get_bcg_strategy(quadrant, composite_score):
            icon_map = {
                'Estrela Digital': get_svg_icon("estrela", size=32),
                'Vaca Leiteira': get_svg_icon("vaca", size=32),
                'Interrogação': get_svg_icon("interrogacao", size=32),
                'Abacaxi': get_svg_icon("abacaxi", size=32)
            }
            strategies = {
                'Estrela Digital': f"{icon_map['Estrela Digital']} INVESTIR PESADO - Score: {composite_score:.2f}",
                'Vaca Leiteira': f"{icon_map['Vaca Leiteira']} OTIMIZAR MARGEM - Score: {composite_score:.2f}",
                'Interrogação': f"{icon_map['Interrogação']} TESTAR ESTRATÉGIAS - Score: {composite_score:.2f}",
                'Abacaxi': f"{icon_map['Abacaxi']} DESCONTINUAR - Score: {composite_score:.2f}"
            }
            default_icon = get_svg_icon("insights", size=18)
            return strategies.get(quadrant, f"{default_icon} ANALISAR - Score: {composite_score:.2f}")
        
        # Título da estratégia (mantém compatibilidade com bcg_strategy)
        def get_bcg_strategy_title(quadrant, composite_score):
            return get_bcg_strategy(quadrant, composite_score)
        
        category_metrics['bcg_strategy'] = category_metrics.apply(
            lambda row: get_bcg_strategy(row['bcg_quadrant'], row['composite_score']), axis=1
        )
        category_metrics['bcg_strategy_title'] = category_metrics.apply(
            lambda row: get_bcg_strategy_title(row['bcg_quadrant'], row['composite_score']), axis=1
        )
        # Análise estratégica (key_insight + planos prescritivos)
        try:
            from utils.rules import load_strategic_rules
            _rules_cfg = load_strategic_rules()
        except Exception:
            _rules_cfg = None
        _analysis_results = category_metrics.apply(
            lambda row: get_strategic_analysis(row, row.get('bcg_quadrant', ''), _rules_cfg), axis=1
        )
        category_metrics['key_insight'] = _analysis_results.apply(lambda x: x.get('key_insight', ''))
        category_metrics['recommendations'] = _analysis_results.apply(
            lambda x: (x.get('plano_capital', []) + x.get('plano_operacional', []) + x.get('plano_mercado', []))
        )
    else:
        category_metrics['bcg_quadrant'] = 'Indefinido'
        category_metrics['bcg_strategy'] = 'Dados insuficientes'
        category_metrics['bcg_strategy_title'] = 'Dados insuficientes'
        category_metrics['key_insight'] = pd.Series(["Dados insuficientes para insight"] * len(category_metrics))
        category_metrics['recommendations'] = pd.Series([["Dados insuficientes para recomendações"]] * len(category_metrics))
        # Garantir que as colunas BCG existam mesmo quando não há dados
        category_metrics['market_share'] = 0
        category_metrics['growth_rate'] = 0
    
    # Identificar categorias em destaque
    top_categories = category_metrics.nlargest(5, 'composite_score')
    
    # Identificar categorias que precisam de atenção (bottom 5)
    bottom_categories = category_metrics.nsmallest(5, 'composite_score')
    
    # Anexar prévia de plano estratégico (insight chave) para cada linha
    if 'bcg_quadrant' in category_metrics.columns:
        try:
            rules_for_preview = None
            def _plan_and_insight(row):
                nonlocal rules_for_preview
                if rules_for_preview is None:
                    from utils.rules import load_strategic_rules
                    rules_for_preview = load_strategic_rules()
                plan = get_strategic_analysis(row, row.get('bcg_quadrant', ''), rules_for_preview)
                return plan, plan.get('key_insight', '')
            pairs = category_metrics.apply(_plan_and_insight, axis=1)
            category_metrics['strategic_plan'] = [p[0] for p in pairs]
            category_metrics['strategic_key_insight'] = [p[1] for p in pairs]
        except Exception:
            category_metrics['strategic_plan'] = [{} for _ in range(len(category_metrics))]
            category_metrics['strategic_key_insight'] = ''
    
    # Classificação BCG por quadrante
    bcg_classification = {}
    if 'bcg_quadrant' in category_metrics.columns:
        for quadrant in ['Estrela Digital', 'Vaca Leiteira', 'Interrogação', 'Abacaxi']:
            quadrant_data = category_metrics[category_metrics['bcg_quadrant'] == quadrant]
            if not quadrant_data.empty:
                bcg_classification[quadrant] = quadrant_data.sort_values('composite_score', ascending=False)
    
    return {
        'top_categories': top_categories,
        'bottom_categories': bottom_categories,
        'category_metrics': category_metrics,
        'bcg_classification': bcg_classification
    }

def _get_semantic_colors(revenue: float, satisfaction: float, customers: int, is_top_category: bool = True) -> Dict[str, str]:
    """
    Determina cores semânticas baseadas nos valores das métricas.
    
    Args:
        revenue: Receita da categoria
        satisfaction: Satisfação média (0-5)
        customers: Número de clientes únicos
        is_top_category: Se é categoria em destaque (True) ou em atenção (False)
    
    Returns:
        Dicionário com as cores para cada métrica
    """
    colors = {}
    
    # Receita - sempre verde (positiva)
    colors['revenue'] = '#10b981'
    
    # Satisfação - baseada em thresholds claros
    if satisfaction >= 4.0:
        colors['satisfaction'] = '#10b981'  # Verde - Excelente
    elif satisfaction >= 3.5:
        colors['satisfaction'] = '#f59e0b'  # Laranja - Boa
    elif satisfaction >= 3.0:
        colors['satisfaction'] = '#f59e0b'  # Laranja - Média
    else:
        colors['satisfaction'] = '#ef4444'  # Vermelho - Baixa
    
    # Clientes - baseado em thresholds relativos
    if is_top_category:
        # Para categorias em destaque, thresholds mais altos
        if customers >= 5000:
            colors['customers'] = '#10b981'     # Verde - Muitos clientes
        elif customers >= 1000:
            colors['customers'] = '#3b82f6'     # Azul - Clientes moderados
        else:
            colors['customers'] = '#f59e0b'     # Laranja - Poucos clientes
    else:
        # Para categorias em atenção, thresholds mais baixos
        if customers >= 1000:
            colors['customers'] = '#10b981'     # Verde - Muitos clientes
        elif customers >= 100:
            colors['customers'] = '#3b82f6'     # Azul - Clientes moderados
        else:
            colors['customers'] = '#ef4444'     # Vermelho - Muito poucos clientes
    
    return colors

def render_category_recommendations(analysis: Dict[str, Any], period: str = 'M') -> None:
    """
    Renderiza as recomendações de categorias de forma visual com cores semânticas.
    Agora inclui classificação BCG híbrida.
    
    Args:
        analysis: Dicionário com a análise de categorias incluindo BCG
    """
    # Importar renderizadores do glass_card
    from components.glass_card import get_bcg_styles, render_bcg_product_card, render_bcg_quadrant_card

    # Criar ícones SVG
    star_icon = get_svg_icon("star", size=32, color="#fbbf24")
    money_icon = get_svg_icon("money", size=32)
    people_icon = get_svg_icon("people", size=32)
    warning_icon = get_svg_icon("warning", size=32, color="#ef4444")
    potential_icon = get_svg_icon("target", size=32, color="#38bdf8")
    
    # Verificar se temos classificação BCG
    has_bcg = 'bcg_classification' in analysis and analysis['bcg_classification']
    
    if has_bcg:
        from io import BytesIO
        import pandas as _pd
        import base64 as _b64

        filtered_df = st.session_state.get("filtered_df")
        if not isinstance(filtered_df, pd.DataFrame) or filtered_df.empty:
            df_all = st.session_state.get("df_all")
            filtered_df = df_all.copy() if isinstance(df_all, pd.DataFrame) else None
        
        # Inserir CSS global do BCG
        st.markdown(get_bcg_styles(), unsafe_allow_html=True)
        
        # ==========================
        # Valor de estoque por categoria (para hover)
        # ==========================
        category_stock_value: Dict[str, float] = {}
        category_stock_units: Dict[str, float] = {}
        stock_detail_df: pd.DataFrame = pd.DataFrame()

        if isinstance(filtered_df, pd.DataFrame) and not filtered_df.empty:
            df_inv = filtered_df.copy()
            available_columns = df_inv.columns.tolist()
            # Detectar coluna de produto
            product_id_col = None
            for cand in ["product_id", "sku", "produto_id", "codigo_produto", "order_id"]:
                if cand in available_columns:
                    product_id_col = cand
                    break
            if product_id_col is None:
                product_id_col = "order_id"

            # 1) Tentar usar SNAPSHOT DE ESTOQUE (Calculado via Pipeline)
            used_real_stock = False
            try:
                from pathlib import Path
                # Snapshot pode existir como parquet e/ou CSV; parquet pode falhar em alguns ambientes (pyarrow).
                # Vamos usar o loader com fallback para CSV para evitar cair no estoque estimado (1.2x pedidos).
                stock_df = None
                try:
                    from utils.stock_loader import load_latest_stock  # type: ignore
                    stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="parquet")
                    if stock_df is None or (isinstance(stock_df, pd.DataFrame) and stock_df.empty):
                        stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="csv")
                except Exception:
                    # fallback mínimo mantendo comportamento anterior
                    stock_snapshot_path = Path("data/stock_snapshot/estoque_snapshot_latest.parquet")
                    if stock_snapshot_path.exists():
                        stock_df = pd.read_parquet(stock_snapshot_path)

                if (
                    isinstance(stock_df, pd.DataFrame)
                    and not stock_df.empty
                    and ("produto_id" in stock_df.columns or "product_id" in stock_df.columns)
                ):
                    # Garantir coluna de chave unificada (SKU)
                    stock_detail_df = stock_df.copy()
                    if "produto_id" in stock_detail_df.columns and "product_id" not in stock_detail_df.columns:
                        stock_detail_df["product_id"] = stock_detail_df["produto_id"]
                    stock_detail_df["product_id"] = stock_detail_df["product_id"].astype(str).str.strip().str.upper()

                    # Garantir colunas base de unidades/valor
                    if "quantidade_disponivel_venda" in stock_detail_df.columns and "stock_level" not in stock_detail_df.columns:
                        stock_detail_df = stock_detail_df.rename(columns={"quantidade_disponivel_venda": "stock_level"})

                    # Em produção o "valor" no hover tem sido tratado como potencial de venda (não custo).
                    # Então priorizamos valor_potencial_venda; se não existir, caímos para capital_imobilizado.
                    if "valor_potencial_venda" in stock_detail_df.columns:
                        stock_detail_df = stock_detail_df.rename(columns={"valor_potencial_venda": "stock_value"})
                    elif "capital_imobilizado" in stock_detail_df.columns:
                        stock_detail_df = stock_detail_df.rename(columns={"capital_imobilizado": "stock_value"})
                    else:
                        stock_detail_df["stock_value"] = 0.0

                    # Categoria:
                    # - Se o snapshot já tiver categoria, usamos
                    # - Senão, mapeamos via df_inv usando a melhor coluna de chave detectada (product_id_col)
                    cat_col_group = None
                    if "product_category_name" in stock_detail_df.columns:
                        cat_col_group = "product_category_name"
                    elif "categoria" in stock_detail_df.columns:
                        stock_detail_df = stock_detail_df.rename(columns={"categoria": "product_category_name"})
                        cat_col_group = "product_category_name"
                    else:
                        try:
                            prod_cat = (
                                df_inv[[product_id_col, "product_category_name"]]
                                .dropna(subset=[product_id_col, "product_category_name"])
                                .drop_duplicates(subset=[product_id_col])
                                .copy()
                            )
                            prod_cat[product_id_col] = prod_cat[product_id_col].astype(str).str.strip().str.upper()
                            prod_cat = prod_cat.rename(columns={product_id_col: "product_id"})
                            stock_detail_df = stock_detail_df.merge(prod_cat, on="product_id", how="left")
                            cat_col_group = "product_category_name"

                            # Diagnóstico: taxa de match (se baixa, é sinal claro de divergência de chaves)
                            matched = int(stock_detail_df["product_category_name"].notna().sum())
                            total = int(len(stock_detail_df))
                            if total > 0:
                                print(f"[BCG] Snapshot rows={total}. Category match={matched}/{total} ({(matched/total):.1%}).")
                        except Exception:
                            cat_col_group = None

                    # Garantir numéricos e preencher nulos
                    stock_detail_df["stock_value"] = pd.to_numeric(stock_detail_df.get("stock_value", 0), errors="coerce").fillna(0)
                    stock_detail_df["stock_level"] = pd.to_numeric(stock_detail_df.get("stock_level", 0), errors="coerce").fillna(0)

                    # Fallback de preço (somente se stock_value zerado e tivermos price no df_inv)
                    if "price" in df_inv.columns:
                        try:
                            avg_price = df_inv.groupby(product_id_col)["price"].mean().rename("__avg_dashboard")
                            avg_price.index = avg_price.index.astype(str).str.strip().str.upper()
                            stock_detail_df = stock_detail_df.merge(avg_price, left_on="product_id", right_index=True, how="left")
                            stock_detail_df["__avg_dashboard"] = pd.to_numeric(stock_detail_df.get("__avg_dashboard", 0), errors="coerce").fillna(0)
                            mask_fix_price = (stock_detail_df["stock_value"] == 0) & (stock_detail_df["stock_level"] > 0) & (stock_detail_df["__avg_dashboard"] > 0)
                            if mask_fix_price.any():
                                stock_detail_df.loc[mask_fix_price, "stock_value"] = (
                                    stock_detail_df.loc[mask_fix_price, "stock_level"] * stock_detail_df.loc[mask_fix_price, "__avg_dashboard"]
                                )
                            stock_detail_df = stock_detail_df.drop(columns=["__avg_dashboard"], errors="ignore")
                        except Exception:
                            pass

                    # Calcular agregados finais
                    if cat_col_group and cat_col_group in stock_detail_df.columns:
                        stock_detail_df[cat_col_group] = stock_detail_df[cat_col_group].fillna("Sem Categoria")
                        category_stock_value = stock_detail_df.groupby(cat_col_group)["stock_value"].sum().to_dict()
                        category_stock_units = stock_detail_df.groupby(cat_col_group)["stock_level"].sum().to_dict()
                        used_real_stock = True

                # Fallback para o arquivo RAW se o snapshot não existir (Mantendo lógica antiga como backup)
                elif Path("data/raw/magazord_stock_raw.parquet").exists():
                    from magazord_pipeline.transformers import normalize_stock  # type: ignore
                    stock_raw = pd.read_parquet("data/raw/magazord_stock_raw.parquet")
                    stock_df = normalize_stock(stock_raw)

                    if (
                        isinstance(stock_df, pd.DataFrame)
                        and not stock_df.empty
                        and "product_id" in stock_df.columns
                        and "stock_level" in stock_df.columns
                    ):
                        # ... lógica legada de merge e fallback de preço ...
                        prod_cat = (
                            df_inv[["product_id", "product_category_name"]]
                            .dropna(subset=["product_id", "product_category_name"])
                            .drop_duplicates(subset=["product_id"])
                        )
                        stock_df["product_id"] = stock_df["product_id"].astype(str).str.strip()
                        prod_cat["product_id"] = prod_cat["product_id"].astype(str).str.strip()

                        stock_join = stock_df.merge(prod_cat, on="product_id", how="left")
                        if "product_category_name" in stock_join.columns:
                            # Tenta calcular custo/preço
                            unit_cost = None
                            if "cost_price" in stock_join.columns:
                                unit_cost = stock_join["cost_price"]
                            
                            stock_join["__unit_cost"] = pd.to_numeric(unit_cost, errors="coerce").fillna(0.0)
                            
                            # Fallback preço médio dos pedidos
                            if "price" in df_inv.columns:
                                avg_price_orders = df_inv.groupby(product_id_col)["price"].mean()
                                stock_join = stock_join.merge(avg_price_orders.rename("__avg"), left_on="product_id", right_index=True, how="left")
                                stock_join["__unit_cost"] = stock_join["__unit_cost"].replace(0, np.nan).fillna(stock_join["__avg"]).fillna(0)
                            
                            stock_join["stock_value"] = stock_join["__unit_cost"] * stock_join["stock_level"]
                            
                            category_stock_value = stock_join.groupby("product_category_name")["stock_value"].sum().to_dict()
                            category_stock_units = stock_join.groupby("product_category_name")["stock_level"].sum().to_dict()
                            
                            stock_detail_df = stock_join.copy()
                            used_real_stock = True
            except Exception as e:
                # print(f"Erro no cálculo de estoque: {e}") # Debug silencioso
                used_real_stock = False
                category_stock_value = {}
                category_stock_units = {}
                stock_detail_df = pd.DataFrame()

            # 2) Fallback: usar colunas de estoque dentro do próprio dataset (cliente legado)
            if not used_real_stock:
                stock_col_candidates = [
                    "stock_quantity",
                    "estoque_atual",
                    "inventory",
                    "stock",
                    "quantidade_estoque",
                    "stock_level",
                ]
                real_stock_col = next((c for c in stock_col_candidates if c in available_columns), None)
                try:
                    if real_stock_col:
                        # Estoque real: último valor conhecido por produto
                        last_stock = (
                            df_inv.groupby(["product_category_name", product_id_col])[real_stock_col]
                            .last()
                            .fillna(0)
                        )
                        if "product_cost" in available_columns:
                            cost_basis = (
                                df_inv.groupby(["product_category_name", product_id_col])["product_cost"]
                                .median()
                                .fillna(0)
                            )
                        else:
                            cost_basis = (
                                df_inv.groupby(["product_category_name", product_id_col])["price"]
                                .mean()
                                .fillna(0)
                            )
                        stock_value_series = (last_stock * cost_basis).groupby(level=0).sum()
                        category_stock_value = stock_value_series.to_dict()
                        # Unidades em estoque
                        stock_units_series = last_stock.groupby(level=0).sum()
                        category_stock_units = stock_units_series.to_dict()
                    else:
                        # Estimativa: 1.2x pedidos no período * preço médio por produto
                        orders_per_product = (
                            df_inv.groupby(["product_category_name", product_id_col])["order_id"]
                            .nunique()
                            .rename("orders")
                        )
                        avg_price_per_product = (
                            df_inv.groupby(["product_category_name", product_id_col])["price"]
                            .mean()
                            .rename("avg_price")
                        )
                        est = (
                            (orders_per_product * 1.2)
                            .fillna(0)
                            .to_frame()
                            .join(avg_price_per_product, how="left")
                            .fillna(0)
                        )
                        est["stock_value"] = est["orders"] * est["avg_price"]
                        category_stock_value = est["stock_value"].groupby(level=0).sum().to_dict()
                        # Unidades estimadas (1.2x pedidos)
                        category_stock_units = est["orders"].groupby(level=0).sum().to_dict()
                except Exception:
                    category_stock_value = {}
                    category_stock_units = {}
        
        def _fmt_currency_br(value: float) -> str:
            try:
                return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                return "0,00"
        
        # Cores do tema para padronização visual
        theme_manager = get_theme_manager()
        glass = theme_manager.get_glass_theme()
        theme = theme_manager.get_theme()
        text_color = theme.get('textColor', '#e2e8f0')
        border_color_default = glass.get('cardBorder', 'rgba(148, 163, 184, 0.3)')
        muted_text_color = '#cbd5e1'
        highlight_text_color = text_color
        
        # -----------------------------
        # Config: tratativa de outliers no growth
        # -----------------------------
        with st.expander("Configurar Growth (anti-outliers)", expanded=False):
            # Defaults seguros
            if "growth_outlier_mode" not in st.session_state:
                st.session_state["growth_outlier_mode"] = "winsor"
            if "growth_outlier_abs_cap" not in st.session_state:
                st.session_state["growth_outlier_abs_cap"] = 1000.0
            if "growth_outlier_winsor_low" not in st.session_state:
                st.session_state["growth_outlier_winsor_low"] = 2
            if "growth_outlier_winsor_high" not in st.session_state:
                st.session_state["growth_outlier_winsor_high"] = 98

            mode_label = st.selectbox(
                "Modo",
                options=["winsor", "cap", "none"],
                index=["winsor", "cap", "none"].index(str(st.session_state.get("growth_outlier_mode", "winsor"))),
                help="winsor: corta extremos por percentil; cap: corta por valor absoluto; none: sem tratativa.",
                key="growth_outlier_mode",
            )
            st.number_input(
                "Cap absoluto (±%): limite de segurança",
                min_value=50.0,
                max_value=20000.0,
                value=float(st.session_state.get("growth_outlier_abs_cap", 1000.0)),
                step=50.0,
                help="Mesmo no winsor, aplicamos este cap como 'airbag' para evitar spikes absurdos.",
                key="growth_outlier_abs_cap",
            )
            c1, c2 = st.columns(2)
            with c1:
                st.slider(
                    "Winsor low (percentil)",
                    min_value=0,
                    max_value=49,
                    value=int(st.session_state.get("growth_outlier_winsor_low", 2)),
                    help="Ex.: 2 => corta abaixo do P2",
                    key="growth_outlier_winsor_low",
                    disabled=(mode_label != "winsor"),
                )
            with c2:
                st.slider(
                    "Winsor high (percentil)",
                    min_value=51,
                    max_value=100,
                    value=int(st.session_state.get("growth_outlier_winsor_high", 98)),
                    help="Ex.: 98 => corta acima do P98",
                    key="growth_outlier_winsor_high",
                    disabled=(mode_label != "winsor"),
                )

        # Criar gráfico BCG + Time Series de Growth
        from utils.charts import create_bcg_matrix_chart, create_category_growth_timeseries
        bcg_fig = create_bcg_matrix_chart(analysis['category_metrics'])
        st.plotly_chart(bcg_fig, use_container_width=True)
        
        # ---------------- Time series com filtro por quadrante ----------------
        # Opções de quadrante na ordem semântica
        wanted_order = ['Estrela Digital', 'Vaca Leiteira', 'Interrogação', 'Abacaxi']
        available_quadrants = [q for q in wanted_order if q in analysis.get('bcg_classification', {})]
        if len(available_quadrants) == 0:
            available_quadrants = list(analysis.get('bcg_classification', {}).keys())
        
        # Seleção do quadrante (default: Estrela Digital se existir)
        selected_quadrant = None
        if len(available_quadrants) > 0:
            default_idx = 0
            if 'Estrela Digital' in available_quadrants:
                default_idx = available_quadrants.index('Estrela Digital')
            selected_quadrant = st.selectbox(
                'Quadrante (pré-seleção de categorias para o gráfico)',
                options=available_quadrants,
                index=default_idx,
                key='growth_ts_quadrant'
            )
        
        # Pré-seleção de categorias a partir do quadrante escolhido
        try:
            if selected_quadrant:
                qdf = analysis['bcg_classification'].get(selected_quadrant, pd.DataFrame())
                preset_categories = qdf['category'].tolist() if not qdf.empty else []
            else:
                preset_categories = []
        except Exception:
            preset_categories = []
        
        # Opções disponíveis de categorias (ordenadas por receita no período filtrado)
        try:
            if isinstance(filtered_df, pd.DataFrame) and not filtered_df.empty:
                cat_order = (
                    filtered_df.groupby('product_category_name')['price']
                    .sum()
                    .sort_values(ascending=False)
                )
                all_available = list(cat_order.index)
            else:
                all_available = []
        except Exception:
            all_available = []
        
        # Default da multiseleção: categorias do quadrante (se existirem); fallback top 12
        if preset_categories:
            default_sel = [c for c in all_available if c in preset_categories]
        else:
            default_sel = all_available[:12] if len(all_available) > 12 else all_available
        
        selected_ts_categories = st.multiselect(
            'Categorias no gráfico',
            options=all_available,
            default=default_sel,
            key='growth_ts_categories'
        )
        
        # Filtrar DF para o gráfico de linha
        if selected_ts_categories:
            ts_df = filtered_df[filtered_df['product_category_name'].isin(selected_ts_categories)].copy()
        else:
            ts_df = filtered_df
        
        # Time series: growth mensal por categoria com tooltip de score/market share
        try:
            df_all = st.session_state.get("df_all")
            # Subset apenas colunas necessárias para evitar erro de hash com colunas complexas (listas)
            ref_df = None
            if isinstance(df_all, pd.DataFrame):
                cols_needed = ['order_purchase_timestamp', 'product_category_name', 'price']
                if 'order_id' in df_all.columns: cols_needed.append('order_id')
                if 'customer_unique_id' in df_all.columns: cols_needed.append('customer_unique_id')
                # Filtrar apenas colunas existentes
                cols_needed = [c for c in cols_needed if c in df_all.columns]
                ref_df = df_all[cols_needed].copy()
            
            growth_ts_fig = create_category_growth_timeseries(ts_df, reference_df=ref_df, period=period)
            st.plotly_chart(growth_ts_fig, use_container_width=True)
        except Exception:
            pass
        
        # Renderizar quadrantes BCG com layout responsivo
        # Usar 2x2 grid para melhor espaçamento
        
        # Definições de ícones e textos estáticos
        quadrant_meta = {
            'Estrela Digital': {'icon': get_svg_icon('estrela_digital', size=32), 'subtitle': 'Investir Pesado', 'insight': 'Produtos com alta participação e crescimento explosivo. Prioridade máxima para investimento em estoque, marketing e expansão de SKUs.'},
            'Vaca Leiteira': {'icon': get_svg_icon('vaca_leiteira', size=32), 'subtitle': 'Otimizar Margem', 'insight': 'Produtos maduros com alta participação mas crescimento em declínio. Focar em otimização de margem, retenção de clientes e colheita de lucros para reinvestir em Estrelas.'},
            'Interrogação': {'icon': get_svg_icon('interrogacao', size=32), 'subtitle': 'Testar Estratégias', 'insight': 'Produtos com alto potencial mas baixa participação. Requerem investimento controlado para validação de mercado antes de escalar.'},
            'Abacaxi': {'icon': get_svg_icon('abacaxi', size=32), 'subtitle': 'Descontinuar', 'insight': 'Produtos com baixa participação e crescimento negativo. Prioridade: liquidar estoque com descontos agressivos e descontinuar linhas.'}
        }
        
        # Layout 2x2 para evitar compactação
        row1 = st.columns(2)
        row2 = st.columns(2)
        columns_layout = [row1[0], row1[1], row2[0], row2[1]]
        
        # Ordem desejada
        ordered_quadrants = ['Estrela Digital', 'Vaca Leiteira', 'Interrogação', 'Abacaxi']

        # Nota: removemos o toggle via query params porque recarregava a página inteira.
        # Agora usamos um switch nativo do Streamlit (st.toggle) dentro de tabs (deep dive opcional).
        
        quadrant_summaries: Dict[str, Dict[str, Any]] = {}

        for i, quadrant in enumerate(ordered_quadrants):
            with columns_layout[i]:
                if quadrant in analysis['bcg_classification']:
                    quadrant_data = analysis['bcg_classification'][quadrant]
                    
                    # Calcular agregados do quadrante
                    q_count = len(quadrant_data)
                    q_score = float(quadrant_data['composite_score'].mean()) if not quadrant_data.empty else 0.0
                    
                    q_stock_val = 0.0
                    q_stock_units = 0.0
                    if not quadrant_data.empty and 'category' in quadrant_data.columns:
                        for cat in quadrant_data['category']:
                            q_stock_val += float(category_stock_value.get(cat, 0.0) or 0.0)
                            q_stock_units += float(category_stock_units.get(cat, 0.0) or 0.0)
                    
                    total_q = len(quadrant_data)

                    # Renderizar Card de Quadrante (Resumo) com botão HTML (link)
                    st.markdown(render_bcg_quadrant_card(
                        quadrant=quadrant,
                        subtitle=quadrant_meta[quadrant]['subtitle'],
                        icon=quadrant_meta[quadrant]['icon'],
                        stats={
                            'categories': q_count,
                            'units': int(q_stock_units),
                            'avg_score': f"{q_score:.2f}"
                        },
                        insight_text=quadrant_meta[quadrant]['insight'],
                        stock_total={
                            'value': f"R$ {_fmt_currency_br(q_stock_val)}",
                            'units': f"{int(q_stock_units)}"
                        },
                        action_text=None,
                        action_href=None
                    ), unsafe_allow_html=True)

                    quadrant_summaries[quadrant] = {
                        "quadrant_data": quadrant_data,
                        "total_q": total_q,
                    }
                else:
                    # Quadrante Vazio
                    st.markdown(render_bcg_quadrant_card(
                        quadrant=quadrant,
                        subtitle=quadrant_meta[quadrant]['subtitle'],
                        icon=quadrant_meta[quadrant]['icon'],
                        stats={'categories': 0, 'units': 0, 'avg_score': "0.00"},
                        insight_text="Nenhuma categoria classificada neste quadrante no momento.",
                        stock_total={'value': "R$ 0,00", 'units': "0"},
                        action_text=None
                    ), unsafe_allow_html=True)
                    quadrant_summaries[quadrant] = {"quadrant_data": pd.DataFrame(), "total_q": 0}

        # ---------------------------------------------------------
        # Deep dive opcional: tabs com toggle + lista de categorias
        # ---------------------------------------------------------
        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
        # Nota: `st.tabs()` não renderiza HTML nos títulos; mantemos os labels limpos (sem emoji/SVG).
        tabs = st.tabs(["Estrela Digital", "Vaca Leiteira", "Interrogação", "Abacaxi"])
        tab_order = ['Estrela Digital', 'Vaca Leiteira', 'Interrogação', 'Abacaxi']

        for tab_idx, quadrant in enumerate(tab_order):
            with tabs[tab_idx]:
                qinfo = quadrant_summaries.get(quadrant, {"quadrant_data": pd.DataFrame(), "total_q": 0})
                quadrant_data = qinfo.get("quadrant_data", pd.DataFrame())
                total_q = int(qinfo.get("total_q", 0) or 0)

                if quadrant_data is None or (isinstance(quadrant_data, pd.DataFrame) and quadrant_data.empty):
                    show_centered_info("Nenhuma categoria disponível neste quadrante.")
                    continue

                q_key = f"view_all_{quadrant}"
                if q_key not in st.session_state:
                    st.session_state[q_key] = False
                    
                # Toggle (Top 5 vs Todos) dentro do tab
                c_lbl, c_tgl = st.columns([4, 1])
                with c_lbl:
                    lbl = "Mostrar todas" if bool(st.session_state[q_key]) else "Mostrar top 5"
                    st.markdown(
                        f"<div style='padding: 6px 6px 0 6px; color: rgba(203,210,220,0.85); font-weight:600; letter-spacing:0.3px;'>{lbl} <span style='opacity:0.8;'>({total_q} categorias)</span></div>",
                        unsafe_allow_html=True,
                    )
                with c_tgl:
                    show_all = st.toggle(
                        " ",
                        value=bool(st.session_state[q_key]),
                        key=f"toggle_{quadrant}",
                        label_visibility="collapsed",
                    )
                    st.session_state[q_key] = bool(show_all)

                limit = total_q if bool(st.session_state[q_key]) else 5
                sorted_q_data = quadrant_data.sort_values('composite_score', ascending=False)
                # Helper local: renderizar um card (evita duplicação entre layout 1-col e 2-col)
                def _render_category_card(row: pd.Series) -> None:
                    cat_name = row['category']
                    score = row['composite_score']
                    share = row['market_share']
                    growth = row['growth_rate']
                    price = row.get('avg_ticket', 0)
                    sales = row.get('total_items', 0)

                    # Usar satisfação média e contagem de reviews (já agregados)
                    rating_score = float(row.get('avg_satisfaction', row.get('avg_rating', 0.0)) or 0.0)
                    rating_count = int(row.get('total_reviews', row.get('review_count', 0)) or 0)

                    c_stock_val = float(category_stock_value.get(cat_name, 0.0) or 0.0)
                    c_stock_units = float(category_stock_units.get(cat_name, 0.0) or 0.0)

                    raw_insight = row.get('key_insight', '')
                    if ": " in raw_insight:
                        ins_title, ins_text = raw_insight.split(": ", 1)
                    else:
                        ins_title, ins_text = "Insight-Chave", raw_insight

                    download_link = ""
                    if filtered_df is not None and not filtered_df.empty and "product_id" in filtered_df.columns:
                        df_cat = filtered_df[filtered_df["product_category_name"] == cat_name].copy()
                        if not df_cat.empty:
                            # Agregar métricas básicas por produto
                            # FIX: Incluir product_name e product_sku na agregação inicial para garantir que existam
                            agg_dict = {
                                "price": "sum",
                                "order_id": "count"
                            }
                            # Tenta preservar o nome e sku se existirem
                            if "product_name" in df_cat.columns:
                                agg_dict["product_name"] = "first"
                            if "product_sku" in df_cat.columns:
                                agg_dict["product_sku"] = "first"
                                
                            df_export = (
                                df_cat.groupby(["product_category_name", "product_id"], as_index=False)
                                .agg(agg_dict)
                            )
                            
                            # Renomear colunas agregadas
                            df_export = df_export.rename(columns={
                                "price": "total_revenue", 
                                "order_id": "total_orders"
                            })
                            
                            # Calcular ticket médio manual
                            df_export["avg_price"] = df_export["total_revenue"] / df_export["total_orders"]
                            
                            # ENRIQUECER COM REVIEWS
                            try:
                                if st is None:
                                    raise RuntimeError("Streamlit não disponível")
                                reviews_df = st.session_state.get("reviews_df")
                                if reviews_df is not None and not reviews_df.empty:
                                    # Respeitar filtros globais (período e marketplace)
                                    try:
                                        selected_mps = st.session_state.get("selected_marketplaces", [])
                                        if isinstance(selected_mps, list) and len(selected_mps) > 0 and "marketplace" in reviews_df.columns:
                                            reviews_df = reviews_df[reviews_df["marketplace"].isin(selected_mps)].copy()
                                    except Exception:
                                        pass
                                    try:
                                        date_range = st.session_state.get("current_date_range")
                                        if date_range:
                                            reviews_df = filter_reviews_by_period(reviews_df, date_range)
                                    except Exception:
                                        pass
                                
                                # Inicializa colunas de review com 0
                                df_export["avg_rating"] = 0.0
                                df_export["total_reviews"] = 0
                                
                                if reviews_df is not None and not reviews_df.empty:
                                    # Normaliza ID no export para garantir match
                                    df_export["match_id"] = df_export["product_id"].astype(str).str.strip().str.upper()
                                        
                                    # Prepara reviews para lookup
                                    # Agrupa reviews por SKU/ID normalizado
                                    rev_metrics = reviews_df.groupby("product_sku", as_index=False).agg(
                                        rating=("review_score", "mean"),
                                        count=("review_id", "count")
                                    )
                                    rev_metrics["product_sku"] = rev_metrics["product_sku"].astype(str).str.strip().str.upper()
                                        
                                    # Cria dicionário de lookup para velocidade e precisão
                                    rating_map = rev_metrics.set_index("product_sku")["rating"].to_dict()
                                    count_map = rev_metrics.set_index("product_sku")["count"].to_dict()
                                    
                                    # Aplica o map
                                    # Tenta usar o SKU primeiro (mais preciso), depois o ID
                                    if "product_sku" in df_export.columns:
                                        df_export["sku_norm"] = df_export["product_sku"].astype(str).str.strip().str.upper()
                                        df_export["avg_rating"] = df_export["sku_norm"].map(rating_map).fillna(0.0)
                                        df_export["total_reviews"] = df_export["sku_norm"].map(count_map).fillna(0)
                                    else:
                                        # Fallback para product_id
                                        df_export["avg_rating"] = df_export["match_id"].map(rating_map).fillna(0.0)
                                        df_export["total_reviews"] = df_export["match_id"].map(count_map).fillna(0)
                                                    
                                    # Limpeza de colunas auxiliares
                                    df_export = df_export.drop(columns=["match_id", "sku_norm"], errors="ignore")

                            except Exception as e:
                            #    print(f"[DEBUG] Erro leve ao enriquecer exportação: {e}")
                                # Não quebra o download, apenas vai sem reviews
                                pass
                            
                            # CALCULAR COMPOSITE_SCORE por produto (se não existir)
                            if "composite_score" not in df_export.columns or df_export["composite_score"].isna().all():
                                # Calcular composite_score baseado em receita, vendas e avaliação
                                # Normalizar cada métrica para 0-1
                                if df_export["total_revenue"].max() > 0:
                                    revenue_norm = (df_export["total_revenue"] - df_export["total_revenue"].min()) / (df_export["total_revenue"].max() - df_export["total_revenue"].min())
                                else:
                                    revenue_norm = pd.Series([0.5] * len(df_export))
                                
                                if df_export["total_orders"].max() > 0:
                                    orders_norm = (df_export["total_orders"] - df_export["total_orders"].min()) / (df_export["total_orders"].max() - df_export["total_orders"].min())
                                else:
                                    orders_norm = pd.Series([0.5] * len(df_export))
                                
                                # Normalizar rating (0-5 para 0-1)
                                rating_norm = df_export["avg_rating"] / 5.0
                                
                                # Composite score: 40% receita, 30% vendas, 30% avaliação
                                df_export["composite_score"] = (
                                    revenue_norm * 0.4 +
                                    orders_norm * 0.3 +
                                    rating_norm * 0.3
                                )
                            else:
                                # Se já existe, tentar mapear do df_cat
                                if "composite_score" in df_cat.columns:
                                    df_cat_unique = df_cat[["product_id", "composite_score"]].drop_duplicates("product_id")
                                    df_cat_unique["product_id"] = df_cat_unique["product_id"].astype(str).str.strip()
                                    df_export = df_export.merge(
                                        df_cat_unique,
                                        on="product_id",
                                        how="left",
                                        suffixes=("", "_from_cat")
                                    )
                                    if "composite_score_from_cat" in df_export.columns:
                                        df_export["composite_score"] = df_export["composite_score"].fillna(df_export["composite_score_from_cat"])
                                        df_export.drop(columns=["composite_score_from_cat"], inplace=True, errors="ignore")
                            if not stock_detail_df.empty and "product_id" in stock_detail_df.columns:
                                df_export["product_id"] = df_export["product_id"].astype(str).str.strip()
                                stock_unique = stock_detail_df[["product_id", "stock_level", "stock_value"]].drop_duplicates(subset=["product_id"]) if "stock_level" in stock_detail_df.columns else pd.DataFrame()

                                if not stock_unique.empty:
                                    df_export = df_export.merge(stock_unique, on="product_id", how="left")
                                    df_export["stock_level"] = df_export["stock_level"].fillna(0)
                                    df_export["stock_value"] = df_export["stock_value"].fillna(0)

                            df_export = df_export.rename(columns={
                                "stock_level": "estoque_atual_unidades",
                                "stock_value": "estoque_valor_estimado"
                            })
                            # Garantir que product_name existe
                            if "product_name" not in df_export.columns:
                                df_export["product_name"] = ""
                            
                            desired_cols = [
                                "product_category_name", "product_name", "product_id", "total_revenue", "total_orders",
                                "avg_rating", "avg_price", "composite_score", "estoque_atual_unidades", "estoque_valor_estimado"
                            ]
                            for col in desired_cols:
                                if col not in df_export.columns:
                                    df_export[col] = np.nan
                            df_export = df_export.reindex(columns=desired_cols)

                            buffer = BytesIO()
                            with _pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                                df_export.to_excel(writer, index=False, sheet_name="Produtos")
                            buffer.seek(0)
                            download_url = "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," + _b64.b64encode(buffer.getvalue()).decode("utf-8")
                            download_link = (
                                f"<a href='{download_url}' download='{cat_name}_produtos.xlsx' "
                                "style='display:inline-block;margin-top:8px;font-size:11px;color:#38bdf8;"
                                "text-decoration:none;'>📊 Baixar Planilha com os Produtos</a>"
                            )
                            
                    st.markdown(render_bcg_product_card(
                        title=cat_name,
                        score=score,
                        quadrant=quadrant,
                        metrics={
                            'share': f"{share:.1f}%",
                            'growth': f"{growth:.1f}%",
                            'growth_val': growth,
                            'price': f"R$ {price:,.2f}",
                            'sales': f"{int(sales)}"
                        },
                        insight_title=ins_title,
                        insight_text=ins_text,
                        stock_info={
                            'units': int(c_stock_units),
                            'value_fmt': f"R$ {_fmt_currency_br(c_stock_val)}"
                        },
                        rating_data={
                            'score': rating_score,
                            'count': rating_count
                        },
                        download_html=download_link,
                        detailed_plans=row.get('strategic_plan', {})
                    ), unsafe_allow_html=True)

                # Layout: quando mostrar todas, renderizar em grid 2 colunas
                if bool(st.session_state[q_key]):
                    cols = st.columns(2)
                    for idx, (_, row) in enumerate(sorted_q_data.head(limit).iterrows()):
                        with cols[idx % 2]:
                            _render_category_card(row)
                else:
                    for _, row in sorted_q_data.head(limit).iterrows():
                        _render_category_card(row)

    

# Painel removido

def render_text_glass_card(title: str, content: List[str], icon: str = "", help_text: Optional[str] = None) -> str:
    """
    Creates a text card with a glass effect.

    Args:
        title: Title of the card
        content: List of strings to display as content
        icon: Icon to display next to the title
        help_text: Optional help text to display at the bottom

    Returns:
        HTML formatted card
    """
    theme_manager = get_theme_manager()
    glass = theme_manager.get_glass_theme()
    theme = theme_manager.get_theme()
    text_color = theme.get('textColor', '#e2e8f0')
    bg_color = glass.get('cardBackground')
    border_color = glass.get('cardBorder')
    shadow_color = glass.get('cardShadow')
    blur = glass.get('cardBlur')
    radius = glass.get('cardBorderRadius')

    content_html = "<ul style='padding-left: 20px;'>" + "".join(f"<li>{item}</li>" for item in content) + "</ul>"

    html = f"""
    <div style="
        backdrop-filter: blur({blur});
        background: {bg_color};
        padding: 20px;
        border-radius: {radius};
        text-align: left;
        font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
        border: 1px solid {border_color};
        box-shadow: 0 8px 32px {shadow_color};
        margin-bottom: 15px;
        overflow: hidden;
        ">
        <div style="font-size: 18px; margin-bottom: 10px; position: relative; color: {text_color};">
            {icon} {title}
        </div>
        <div style="font-size: 16px; position: relative; color: {text_color};">
            {content_html}
        </div>
        {f'<div style="font-size: 14px; margin-top: 10px; opacity: 0.8; position: relative; color: {text_color};">{help_text}</div>' if help_text else ''}
    """
    return html

def render_premium_sentiment_card(title: str, count: int, avg_length: int, percentage: float, 
                                 color: str, gradient_start: str, gradient_end: str, icon_name: str) -> str:
    """Cria um card premium para análise de sentimento."""
    icon = get_svg_icon(icon_name, size=32, color=color)
    
    return f"""
    <div style="
        background: linear-gradient(135deg, {gradient_start}, {gradient_end});
        border: 1px solid rgba(148, 163, 184, 0.3);
        border-radius: 16px;
        padding: 24px;
        backdrop-filter: blur(16px);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1);
        margin-bottom: 20px;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    ">
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px;">
            <div style="display: flex; align-items: center; gap: 12px;">
                {icon}
                <h3 style="margin: 0; color: #f8fafc; font-size: 18px; font-weight: 600;">{title}</h3>
            </div>
            <div style="
                background: {color}22;
                border: 1px solid {color}44;
                padding: 6px 14px;
                border-radius: 999px;
                font-size: 14px;
                font-weight: 700;
                color: {color};
            ">
                {percentage:.1f}%
            </div>
        </div>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
            <div style="
                background: rgba(30, 41, 59, 0.6);
                border: 1px solid rgba(148, 163, 184, 0.2);
                border-radius: 12px;
                padding: 14px;
                text-align: center;
            ">
                <div style="color: #cbd5e1; font-size: 12px; margin-bottom: 4px;">Avaliações</div>
                <div style="color: #f8fafc; font-size: 24px; font-weight: 700;">{count:,}</div>
            </div>
            <div style="
                background: rgba(30, 41, 59, 0.6);
                border: 1px solid rgba(148, 163, 184, 0.2);
                border-radius: 12px;
                padding: 14px;
                text-align: center;
            ">
                <div style="color: #cbd5e1; font-size: 12px; margin-bottom: 4px;">Média</div>
                <div style="color: #f8fafc; font-size: 24px; font-weight: 700;">{avg_length}</div>
                <div style="color: #94a3b8; font-size: 11px;">caracteres</div>
            </div>
        </div>
    </div>
    """

def render_premium_word_list(title: str, items: list, icon_color: str, icon_name: str = "search") -> str:
    """Cria uma lista premium de palavras frequentes."""
    icon = get_svg_icon(icon_name, size=24, color=icon_color)
    
    items_html = ""
    for item in items[:5]:
        word = item.split(':')[0]
        count = item.split(':')[1].strip()
        items_html += f'<div style="background: linear-gradient(90deg, rgba(30, 41, 59, 0.4) 0%, rgba(51, 65, 85, 0.6) 50%, rgba(30, 41, 59, 0.4) 100%); border: 1px solid rgba(148, 163, 184, 0.2); border-left: 3px solid {icon_color}; border-radius: 8px; padding: 10px 14px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center;"><span style="color: #e2e8f0; font-weight: 500;">{word}</span><span style="color: {icon_color}; font-weight: 700; background: {icon_color}22; padding: 4px 10px; border-radius: 6px; font-size: 13px;">{count}</span></div>'
    
    return f'<div style="background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%); border: 1px solid rgba(148, 163, 184, 0.3); border-radius: 14px; padding: 20px; backdrop-filter: blur(16px); box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1); margin-bottom: 16px;"><div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">{icon}<h4 style="margin: 0; color: #f1f5f9; font-size: 16px; font-weight: 600;">{title}</h4></div>{items_html}</div>'

def render_premium_topics_list(title: str, topics: list, icon_color: str) -> str:
    """Cria uma lista premium de tópicos."""
    icon = get_svg_icon("clipboard", size=24, color=icon_color)
    
    topics_html = ""
    for i, topic in enumerate(topics[:3], 1):
        topics_html += f'<div style="background: rgba(30, 41, 59, 0.5); border: 1px solid {icon_color}33; border-radius: 10px; padding: 12px 14px; margin-bottom: 10px; position: relative; overflow: hidden;"><div style="position: absolute; top: 0; left: 0; width: 4px; height: 100%; background: linear-gradient(180deg, {icon_color}, {icon_color}66);"></div><div style="color: {icon_color}; font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; padding-left: 10px;">Tópico {i}</div><div style="color: #e2e8f0; font-size: 14px; line-height: 1.5; padding-left: 10px;">{topic}</div></div>'
    
    return f'<div style="background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%); border: 1px solid rgba(148, 163, 184, 0.3); border-radius: 14px; padding: 20px; backdrop-filter: blur(16px); box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1);"><div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">{icon}<h4 style="margin: 0; color: #f1f5f9; font-size: 16px; font-weight: 600;">{title}</h4></div>{topics_html}</div>'


def render_ecommerce_topics_list(title: str, semantic_topics: list, base_color: str) -> str:
    """
    Cria uma lista premium de tópicos semânticos de e-commerce.
    
    Exibe os tópicos categorizados (Produto, Entrega, Atendimento, Preço)
    com ícones e cores específicas para cada categoria.
    
    Args:
        title: Título do card
        semantic_topics: Lista de strings formatadas (ex: "📦 Produto: qualidade, bom, excelente")
        base_color: Cor base do card (para fallback)
        
    Returns:
        HTML formatado para renderização
    """
    icon = get_svg_icon("clipboard", size=24, color=base_color)
    
    # Mapeamento de cores por categoria (Expandido)
    topic_colors = {
        '📦': '#3b82f6',  # Produto - Azul
        '💄': '#ec4899',  # Cosméticos - Rosa
        '🚚': '#f59e0b',  # Entrega - Laranja
        '💬': '#8b5cf6',  # Atendimento - Roxo
        '💰': '#10b981',  # Preço - Verde
        '🛍️': '#a855f7',  # Experiência - Roxo Claro
        '⚠️': '#ef4444',  # Problemas - Vermelho
        '🌟': '#eab308',  # Elogios - Amarelo/Dourado
        '📝': '#64748b',  # Outros - Cinza
    }
    
    topic_cards: list[str] = []
    for raw_topic in semantic_topics[:4]:  # Máximo 4 tópicos
        topic = raw_topic.strip()
        topic_icon = topic[:2] if len(topic) >= 2 else '📝'
        topic_color = topic_colors.get(topic_icon, base_color)
        
        parts = topic.split(':', 1)
        label_part = parts[0].strip()
        words_part = parts[1].strip() if len(parts) > 1 else ''
        label_clean = label_part.replace(topic_icon, '').strip()
        
        topic_cards.append(
            f'<div style="background: rgba(30,41,59,0.5); '
            f'border: 1px solid {topic_color}33; border-radius: 10px; '
            f'padding: 12px 14px; margin-bottom: 10px; position: relative; overflow: hidden;">'
            f'<div style="position: absolute; top: 0; left: 0; width: 4px; height: 100%; '
            f'background: linear-gradient(180deg, {topic_color}, {topic_color}66);"></div>'
            f'<div style="display: flex; align-items: center; gap: 8px; margin-bottom: 6px; padding-left: 10px;">'
            f'<span style="font-size: 16px;">{topic_icon}</span>'
            f'<span style="color: {topic_color}; font-weight: 700; font-size: 12px; '
            f'text-transform: uppercase; letter-spacing: 0.5px;">{label_clean}</span>'
            f'</div>'
            f'<div style="color: #e2e8f0; font-size: 13px; line-height: 1.5; padding-left: 10px;">'
            f'{words_part}</div>'
            f'</div>'
        )
    
    if not topic_cards:
        topic_cards.append(
            f'<div style="background: rgba(30,41,59,0.5); border: 1px solid {base_color}33; '
            f'border-radius: 10px; padding: 12px 14px; text-align: center;">'
            f'<span style="color: #94a3b8; font-size: 13px;">Não há dados suficientes</span>'
            f'</div>'
        )
    
    topics_html = "".join(topic_cards)
    
    return (
        f'<div style="background: linear-gradient(135deg, rgba(30,41,59,0.95) 0%, rgba(15,23,42,0.95) 100%); '
        f'border: 1px solid rgba(148,163,184,0.3); border-radius: 14px; padding: 20px; '
        f'backdrop-filter: blur(16px); box-shadow: 0 4px 16px rgba(0,0,0,0.4), '
        f'inset 0 1px 0 rgba(255,255,255,0.1);">'
        f'<div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">'
        f'{icon}'
        f'<h4 style="margin: 0; color: #f1f5f9; font-size: 16px; font-weight: 600;">{title}</h4>'
        f'</div>'
        f'{topics_html}'
        f'</div>'
    )

def render_sentiment_analysis(nlp_results: Dict[str, Any]) -> None:
    """
    Renderiza a análise de sentimentos com design premium glassmorphism metálico escuro.
    
    Args:
        nlp_results: Dicionário com os resultados da análise de sentimentos
    """
    # Calcular totais
    total_reviews = (nlp_results['metrics']['positive_count'] + 
                    nlp_results['metrics']['neutral_count'] + 
                    nlp_results['metrics']['negative_count'])
    
    # Criar colunas para os cards de sentimentos
    col1, col2, col3 = st.columns(3)
    
    # Card de Avaliações Positivas
    with col1:
        positive_pct = (nlp_results['metrics']['positive_count'] / total_reviews * 100) if total_reviews > 0 else 0
        
        st.markdown(
            render_premium_sentiment_card(
                "Avaliações Positivas",
                nlp_results['metrics']['positive_count'],
                int(nlp_results['metrics']['avg_positive_length']),
                positive_pct,
                "#10b981",  # Verde
                "rgba(16, 185, 129, 0.15)",
                "rgba(5, 150, 105, 0.1)",
                "sun"
            ),
            unsafe_allow_html=True
        )
        
        # Wordcloud
        if nlp_results['positive_wordcloud'] is not None:
            st.pyplot(nlp_results['positive_wordcloud'], use_container_width=True)
        else:
            st.info("Não há dados suficientes para gerar wordcloud das avaliações positivas")
        
        # Palavras mais frequentes
        st.markdown(
            render_premium_word_list(
                "Palavras Mais Frequentes",
                [f"{word}: {freq} ocorrências" for word, freq in list(nlp_results['positive_freq'].items())[:5]],
                "#10b981",
                "search"
            ),
            unsafe_allow_html=True
        )
        
        # Tópicos - Usar tópicos semânticos de e-commerce se disponíveis
        if 'positive_topics_semantic' in nlp_results and nlp_results['positive_topics_semantic']:
            st.markdown(
                render_ecommerce_topics_list(
                    "Principais Tópicos",
                    nlp_results['positive_topics_semantic'],
                    "#10b981"
                ),
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                render_premium_topics_list(
                    "Principais Tópicos",
                    nlp_results['positive_topics_lda'][:3],
                    "#10b981"
                ),
                unsafe_allow_html=True
            )
    
    # Card de Avaliações Neutras
    with col2:
        neutral_pct = (nlp_results['metrics']['neutral_count'] / total_reviews * 100) if total_reviews > 0 else 0
        
        st.markdown(
            render_premium_sentiment_card(
                "Avaliações Neutras",
                nlp_results['metrics']['neutral_count'],
                int(nlp_results['metrics']['avg_neutral_length']),
                neutral_pct,
                "#6366f1",  # Roxo/Indigo
                "rgba(99, 102, 241, 0.15)",
                "rgba(79, 70, 229, 0.1)",
                "minus-circle"
            ),
            unsafe_allow_html=True
        )
        
        # Wordcloud
        if nlp_results['neutral_wordcloud'] is not None:
            st.pyplot(nlp_results['neutral_wordcloud'], use_container_width=True)
        else:
            st.info("Não há dados suficientes para gerar wordcloud das avaliações neutras")
        
        # Palavras mais frequentes
        st.markdown(
            render_premium_word_list(
                "Palavras Mais Frequentes",
                [f"{word}: {freq} ocorrências" for word, freq in list(nlp_results['neutral_freq'].items())[:5]],
                "#6366f1",
                "search"
            ),
            unsafe_allow_html=True
        )
        
        # Tópicos - Usar tópicos semânticos de e-commerce se disponíveis
        if 'neutral_topics_semantic' in nlp_results and nlp_results['neutral_topics_semantic']:
            st.markdown(
                render_ecommerce_topics_list(
                    "Principais Tópicos",
                    nlp_results['neutral_topics_semantic'],
                    "#6366f1"
                ),
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                render_premium_topics_list(
                    "Principais Tópicos",
                    nlp_results['neutral_topics_lda'][:3],
                    "#6366f1"
                ),
                unsafe_allow_html=True
            )
    
    # Card de Avaliações Negativas
    with col3:
        negative_pct = (nlp_results['metrics']['negative_count'] / total_reviews * 100) if total_reviews > 0 else 0
        
        st.markdown(
            render_premium_sentiment_card(
                "Avaliações Negativas",
                nlp_results['metrics']['negative_count'],
                int(nlp_results['metrics']['avg_negative_length']),
                negative_pct,
                "#ef4444",  # Vermelho
                "rgba(239, 68, 68, 0.15)",
                "rgba(220, 38, 38, 0.1)",
                "warning"
            ),
            unsafe_allow_html=True
        )
        
        # Wordcloud
        if nlp_results['negative_wordcloud'] is not None:
            st.pyplot(nlp_results['negative_wordcloud'], use_container_width=True)
        else:
            st.info("Não há dados suficientes para gerar wordcloud das avaliações negativas")
        
        # Palavras mais frequentes
        st.markdown(
            render_premium_word_list(
                "Palavras Mais Frequentes",
                [f"{word}: {freq} ocorrências" for word, freq in list(nlp_results['negative_freq'].items())[:5]],
                "#ef4444",
                "search"
            ),
            unsafe_allow_html=True
        )
        
        # Tópicos - Usar tópicos semânticos de e-commerce se disponíveis
        if 'negative_topics_semantic' in nlp_results and nlp_results['negative_topics_semantic']:
            st.markdown(
                render_ecommerce_topics_list(
                    "Principais Tópicos",
                    nlp_results['negative_topics_semantic'],
                    "#ef4444"
                ),
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                render_premium_topics_list(
                    "Principais Tópicos",
                    nlp_results['negative_topics_lda'][:3],
                    "#ef4444"
                ),
                unsafe_allow_html=True
            )
@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_market_analysis_insights(product_metrics: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate insights for market analysis including premium and popular categories.

    Args:
        product_metrics: DataFrame containing product metrics (categories).

    Returns:
        A dictionary with insights about premium and popular categories and market metrics.
    """
    # Calcular métricas gerais
    avg_price_market = product_metrics['avg_price'].mean()
    avg_rating_market = product_metrics['avg_rating'].mean()
    
    # Identificar categorias premium (alto preço, alta avaliação)
    premium_categories = product_metrics[
        (product_metrics['avg_price'] > avg_price_market) &
        (product_metrics['avg_rating'] > avg_rating_market)
    ].sort_values('avg_rating', ascending=False)
    
    # Identificar categorias populares (alto volume, preço acessível)
    popular_categories = product_metrics[
        (product_metrics['total_sales'] > product_metrics['total_sales'].mean()) &
        (product_metrics['avg_price'] <= avg_price_market)
    ].sort_values('total_sales', ascending=False)
    
    premium_info = {
        'count': len(premium_categories),
        'avg_price': premium_categories['avg_price'].mean() if len(premium_categories) > 0 else 0,
        'avg_rating': premium_categories['avg_rating'].mean() if len(premium_categories) > 0 else 0
    }
    
    popular_info = {
        'count': len(popular_categories),
        'avg_volume': popular_categories['total_sales'].mean() if len(popular_categories) > 0 else 0,
        'avg_price': popular_categories['avg_price'].mean() if len(popular_categories) > 0 else 0
    }
    
    market_metrics = {
        'avg_market_price': avg_price_market,
        'avg_market_rating': avg_rating_market
    }
    
    return {
        'premium_info': premium_info,
        'popular_info': popular_info,
        'premium_categories': premium_categories,
        'popular_categories': popular_categories,
        'market_metrics': market_metrics
    }

def render_revenue_and_product_insights(
                                        kpis: Dict[str, Any], 
                                        growth_percentage: float, 
                                        best_day: str, 
                                        best_month: str, 
                                        best_category: str, 
                                        best_category_profit: float, 
                                        best_state: str, 
                                        state_ticket: pd.Series, 
                                        sorted_categories: List[Tuple[str, float]],
                                        format_value: Callable[[float], str]
                                        ) -> None:
    """
    Renderiza os insights de receita e produto no final da seção 'Análise Estratégica'.

    Args:
        kpis: Dicionário com os KPIs calculados.
        growth_percentage: Percentual de crescimento previsto.
        best_day: Melhor dia para vendas.
        best_month: Melhor mês para vendas.
        best_category: Categoria mais rentável.
        best_category_profit: Lucro da categoria mais rentável.
        best_state: Estado com maior ticket médio.
        state_ticket: Série com o ticket médio por estado.
        sorted_categories: Lista de categorias ordenadas por crescimento.
    """
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        **Insights de Receita:**
        - **Receita Total**: R$ {format_value(kpis['total_revenue'])}
        - **Crescimento Previsto**: {format_value(growth_percentage)}%
        - **Melhor dia para vendas**: {best_day}
        - **Melhor mês para vendas**: {best_month}
        """)

    with col2:
        st.markdown(f"""
        **Insights de Produtos:**
        - **Melhor Resultado Operacional**: {best_category} (R$ {format_value(best_category_profit)})
        - **Estado com maior ticket médio**: {best_state} (R$ {format_value(state_ticket.max() if 'state_ticket' in locals() else 0)})
        - **Categoria com maior crescimento**: {sorted_categories[0][0] if sorted_categories else "N/A"} ({format_value(sorted_categories[0][1] if sorted_categories else 0)}%)
        """)

def get_status_icon(rate: float) -> str:
    """
    Retorna o ícone de status baseado na taxa de conversão.
    
    Args:
        rate (float): Taxa de conversão em porcentagem
    
    Returns:
        str: Ícone de status (🟢, 🟡 ou 🔴)
    """
    if rate >= 95:
        return "🟢"
    elif rate >= 85:
        return "🟡"
    else:
        return "🔴"

def generate_funnel_conversion_insights(funnel_counts: Dict[str, int]) -> str:
    """
    Gera insights sobre taxas de conversão para um funil dinâmico.

    O funil pode conter estágios adicionais além de
    created/approved/shipped/delivered — por exemplo, visitantes,
    visualizações, carrinho etc. A função calcula as taxas de conversão
    apenas para pares consecutivos de estágios CUJAS CONTAGENS existam
    em ``funnel_counts`` e cujo valor do estágio de origem seja > 0.
    
    Args:
        funnel_counts (dict): Dicionário {stage: count}
    
    Returns:
        str: HTML formatado com os insights de conversão
    """
    # Ordem lógica dos estágios (topo ➜ base)
    stage_order = [
        "visitors",
        "product_views",
        "add_to_cart",
        "checkout",
        "created",
        "approved",
        "shipped",
        "delivered",
    ]

    # Rótulos amigáveis
    stage_labels = {
        "visitors": "Visitantes",
        "product_views": "Visualizações",
        "add_to_cart": "Itens no Carrinho",
        "checkout": "Check-out Iniciado",
        "created": "Pedidos Criados",
        "approved": "Pedidos Aprovados",
        "shipped": "Pedidos Enviados",
        "delivered": "Pedidos Entregues",
    }

    # Filtrar estágios presentes no dicionário e manter a ordem definida
    ordered_stages = [s for s in stage_order if s in funnel_counts]

    # Calcular taxas de conversão entre pares consecutivos
    conversion_rates = []  # Lista de tuplas (src, tgt, rate)
    for i in range(len(ordered_stages) - 1):
        src, tgt = ordered_stages[i], ordered_stages[i + 1]
        src_count = funnel_counts.get(src, 0)
        tgt_count = funnel_counts.get(tgt, 0)
        if src_count > 0:
            rate = (tgt_count / src_count) * 100
            conversion_rates.append((src, tgt, rate))

    # Caso não existam pares válidos, retornar mensagem padrão
    if not conversion_rates:
        return "<p>Dados insuficientes para calcular taxas de conversão.</p>"

    # Montar lista HTML de conversões
    items_html = ""
    for src, tgt, rate in conversion_rates:
        items_html += (
            f"<li>{get_status_icon(rate)} <strong>{stage_labels[src]} → {stage_labels[tgt]}:</strong> "
            f"{rate:.1f}%</li>"
        )

    # Insight geral: todas as taxas ≥ 95%?
    all_healthy = all(rate >= 95 for _, _, rate in conversion_rates)
    insight_icon = get_svg_icon("target", size=16, color="#fbbf24")
    insight_text = (
        "Funil operando com <strong>taxas saudáveis</strong> de conversão." if all_healthy
        else "Oportunidades de melhoria identificadas nas taxas de conversão."
    )
    
    conversion_section = f"""
    <div style="
        backdrop-filter: blur(10px);
        background: rgba(255, 255, 255, 0.08);
        border-radius: 20px;
        padding: 25px;
        margin: 0px 0px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
    ">
        <h3 style="margin-top: 0;">{get_svg_icon('conversion', size=32)} Taxa de Conversão entre Etapas</h3>
        <ul style="font-size: 1.1em; padding-left: 20px;">
            {items_html}
        </ul>
        <div style="
            margin-top: 20px;
            background: rgba(0, 255, 100, 0.1);
            padding: 15px;
            border-left: 4px solid #00ff66;
            border-radius: 10px;
            font-size: 1.05em;
        ">
            {insight_icon} <strong>Insight:</strong> {insight_text}
        </div>
    </div>
    """
    
    return conversion_section

@st.cache_data(ttl=3600, show_spinner="🤖 Sugerindo gestão de estoque (ML + fallback)...")
def generate_category_recommendations(filtered_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Gera recomendações de estoque por categoria: tenta ML (ensemble) primeiro;
    em caso de falha ou dados insuficientes, usa fallback heurístico.
    """
    try:
        # 1) Tentar ML: carregar movimentações e snapshot de estoque (opcionais)
        stock_movements_df, sm_source = _load_stock_movements_source()
        if st is not None:
            st.session_state["stock_movements_source"] = sm_source

        current_stock_df = None
        try:
            from utils.stock_loader import load_latest_stock  # type: ignore
            current_stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="parquet")
            if current_stock_df is None or (isinstance(current_stock_df, pd.DataFrame) and current_stock_df.empty):
                current_stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="csv")
        except Exception:
            current_stock_df = None

        ml_system = MLStockRecommendationSystem(
            min_revenue=3500,
            min_data_points=30,
            mape_threshold=40.0,
            use_dynamic_horizon=True,
        )
        recs = ml_system.generate_recommendations(
            filtered_df,
            stock_movements=stock_movements_df if not stock_movements_df.empty else None,
            current_stock=current_stock_df,
        )
        if recs:
            return recs
    except Exception as e:
        if st is not None:
            st.warning(f"Recomendações ML indisponíveis ({e}), usando heurística tradicional.")
    try:
        return _generate_category_recommendations_fallback(filtered_df)
    except Exception as e:
        if st is not None:
            st.error(f"Ocorreu um erro ao gerar recomendações: {e}")
        return []

def _generate_category_recommendations_fallback(filtered_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Método de fallback usando a heurística tradicional.
    Usado quando o sistema de validação automática não está disponível.
    """
    # Definir limites mínimos mais realísticos
    MIN_MONTHLY_ORDERS = 5
    MIN_TOTAL_REVENUE = 500
    
    # Usar todas as categorias presentes no DataFrame filtrado
    all_categories = filtered_df['product_category_name'].dropna().unique().tolist()
    
    # Preparar dados mensais por categoria (otimizado)
    filtered_df_copy = filtered_df.copy()
    
    # Fallback de receita
    if 'valorTotalFinal' in filtered_df_copy.columns:
        vtf = pd.to_numeric(filtered_df_copy['valorTotalFinal'], errors='coerce').fillna(0)
        if vtf.sum() > 0:
            filtered_df_copy['price'] = vtf
            
    filtered_df_copy['month'] = safe_to_datetime(filtered_df_copy['order_purchase_timestamp']).dt.to_period('M')
    monthly_category_sales = filtered_df_copy.groupby(['month', 'product_category_name']).agg({
        'price': 'sum',
        'order_id': 'count',
        'pedido_cancelado': 'mean'
    }).reset_index()
    monthly_category_sales['month'] = monthly_category_sales['month'].astype(str)
    
    # Se não houver histórico, retornar lista vazia
    if monthly_category_sales.empty:
        return []
    
    # Previsão de demanda para cada categoria
    forecast_data = []
    
    # Safely extract the last month as a scalar Timestamp
    last_month_series = safe_to_datetime(monthly_category_sales['month'].iloc[-1])
    if last_month_series.empty or pd.isna(last_month_series.iloc[0]):
        # Fallback: use the last month from the original data
        last_month = pd.to_datetime(monthly_category_sales['month'].iloc[-1], errors='coerce')
    else:
        last_month = last_month_series.iloc[0]
    
    # Ensure we have a valid Timestamp
    if pd.isna(last_month):
        return []  # Cannot generate forecasts without a valid date
    
    forecast_months = pd.date_range(start=last_month, periods=4, freq='M')[1:]
    
    for category in all_categories:
        category_data = monthly_category_sales[monthly_category_sales['product_category_name'] == category]
        category_revenue = filtered_df[filtered_df['product_category_name'] == category]['price'].sum()
        avg_monthly_orders = category_data['order_id'].mean() if not category_data.empty else 0
        
        if avg_monthly_orders >= MIN_MONTHLY_ORDERS and category_revenue >= MIN_TOTAL_REVENUE:
            if len(category_data) >= 3:
                ma3 = category_data['order_id'].rolling(window=3).mean().iloc[-1]
                recent_data = category_data.tail(3)
                x = np.arange(len(recent_data))
                y = recent_data['order_id'].values
                z = np.polyfit(x, y, 1)
                trend = z[0]
                
                for i, month in enumerate(forecast_months):
                    forecast = ma3 + (trend * (i + 1))
                    forecast_data.append({
                        'month': month,
                        'product_category_name': category,
                        'forecast': max(0, forecast)
                    })
    
    forecast_df = pd.DataFrame(forecast_data)
    recommendations = []
    
    for category in all_categories:
        category_data = monthly_category_sales[monthly_category_sales['product_category_name'] == category]
        category_revenue = filtered_df[filtered_df['product_category_name'] == category]['price'].sum()
        avg_monthly_orders = category_data['order_id'].mean() if not category_data.empty else 0
        category_forecast = forecast_df[forecast_df['product_category_name'] == category] if not forecast_df.empty else pd.DataFrame()
        
        if avg_monthly_orders >= MIN_MONTHLY_ORDERS and category_revenue >= MIN_TOTAL_REVENUE:
            if not category_data.empty and not category_forecast.empty:
                last_month_sales = category_data.iloc[-1]['order_id']
                next_month_forecast = category_forecast.iloc[0]['forecast']
                variation = (next_month_forecast - last_month_sales) / last_month_sales * 100 if last_month_sales > 0 else 0
                inventory_turnover = avg_monthly_orders / 30
                lead_time_days = 15
                safety_stock_days = 7
                ideal_stock = (next_month_forecast / 30) * (lead_time_days + safety_stock_days)
                
                # LÓGICA MELHORADA: Mais sensível e dinâmica
                if variation > 15:  # Reduzido de 20 para 15
                    if inventory_turnover > 2:
                        action = "Aumentar significativamente"
                        reason = f"Alto crescimento previsto ({variation:.1f}%) com excelente giro de estoque"
                    else:
                        action = "Aumentar moderadamente"
                        reason = f"Alto crescimento previsto ({variation:.1f}%) mas giro moderado"
                elif variation > 5:  # Reduzido de 10 para 5
                    if inventory_turnover > 1:
                        action = "Aumentar moderadamente"
                        reason = f"Crescimento moderado ({variation:.1f}%) com bom giro"
                    else:
                        action = "Manter"
                        reason = f"Crescimento leve ({variation:.1f}%) mas giro baixo"
                elif variation < -15:  # Reduzido de -20 para -15
                    if inventory_turnover < 1:
                        action = "Reduzir significativamente"
                        reason = f"Queda significativa ({variation:.1f}%) com baixo giro"
                    else:
                        action = "Reduzir moderadamente"
                        reason = f"Queda significativa ({variation:.1f}%) mas giro ainda bom"
                elif variation < -5:  # Reduzido de -10 para -5
                    if inventory_turnover < 1.5:
                        action = "Reduzir moderadamente"
                        reason = f"Queda moderada ({variation:.1f}%) com giro baixo"
                    else:
                        action = "Manter"
                        reason = f"Queda leve ({variation:.1f}%) mas giro ainda adequado"
                else:
                    # Para variações entre -5% e +5%
                    if inventory_turnover > 3:
                        action = "Aumentar moderadamente"
                        reason = f"Demanda estável ({variation:.1f}%) mas giro excelente - oportunidade"
                    elif inventory_turnover < 1:
                        action = "Reduzir moderadamente"
                        reason = f"Demanda estável ({variation:.1f}%) mas giro muito baixo"
                    else:
                        action = "Manter"
                        reason = f"Demanda estável ({variation:.1f}%) com giro adequado"
                
                fd = {
                    'model_used': 'traditional_heuristic',
                    'horizon_days': 30,
                    'mape': None,
                    'forecast_period': '30 dias (método tradicional)'
                }
                rec = {
                    'category': category,
                    'variation': variation,
                    'action': action,
                    'reason': reason,
                    'ideal_stock': ideal_stock,
                    'inventory_turnover': inventory_turnover,
                    'forecast_details': fd,
                    # Mesmo formato do ML para o card
                    'Estoque Sugerido (Vendas)': f"{int(ideal_stock)} unidades",
                    'Velocidade de Vendas': f"{inventory_turnover:.2f} vendas/dia",
                    'model': fd['model_used'],
                    'horizon': f"{fd['horizon_days']} dias (método tradicional)",
                    'details': {
                        'estoque_sugerido_vendas': int(ideal_stock),
                        'velocidade_vendas': inventory_turnover,
                        'estoque_atual_fisico': None,
                        'stock_gap': None,
                    }
                }
                recommendations.append(rec)

    return recommendations


def generate_strategic_insights(filtered_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera insights estratégicos para a página de análise.
    
    Args:
        filtered_df: DataFrame com os dados filtrados
        
    Returns:
        Dicionário com insights estratégicos
    """
    # Calcular métricas de receita
    revenue_insights = calculate_revenue_insights(filtered_df)
    
    # Calcular métricas de satisfação
    satisfaction_insights = calculate_satisfaction_insights(filtered_df)
    
    # Calcular métricas de cancelamento
    cancellation_insights = calculate_cancellation_insights(filtered_df)
    
    # Calcular métricas de entrega
    delivery_insights = calculate_delivery_insights(filtered_df)
    
    return {
        'revenue': revenue_insights,
        'satisfaction': satisfaction_insights,
        'cancellation': cancellation_insights,
        'delivery': delivery_insights
    }

def render_recommendations_and_insights(filtered_df: pd.DataFrame) -> None:
    """
    Renderiza a seção de recomendações e insights com duas tabs.
    
    Args:
        filtered_df: DataFrame com os dados filtrados
    """
    # Enriquecer com métricas de avaliações (opcional, não quebra se ausente)
    filtered_df = enrich_with_review_metrics(filtered_df)

    # Salvar DataFrame no session_state para uso nos downloads
    if st is not None:
        st.session_state['filtered_df'] = filtered_df
    
    # Criar ícones SVG com tamanhos maiores
    target_icon = get_svg_icon("target", size=32)  # Para representar insights
    box_icon = get_svg_icon("machine_working", size=48)  # Para estoque/previsão
    chart_icon = get_svg_icon("insight_final", size=48)
    
    # Gerar recomendações e insights
    recommendations = generate_category_recommendations(filtered_df)
    insights = generate_strategic_insights(filtered_df)

    # Informar fonte das movimentações de estoque (quando disponível)
    if st is not None:
        sm_source = st.session_state.get("stock_movements_source")
        if sm_source == "supabase":
            st.caption("🔌 Movimentações de estoque carregadas do Supabase.")
        elif sm_source == "local":
            st.caption("📁 Movimentações de estoque carregadas do arquivo local.")

    # -----------------------------------------------------------
    # Enriquecer recomendações com classificação BCG (Estrela, Vaca, etc.)
    # -----------------------------------------------------------
    try:
        # Calcular BCG para mapear categorias
        bcg_data = analyze_category_performance(filtered_df)
        cat_metrics = bcg_data.get('category_metrics', pd.DataFrame())
        
        bcg_map = {}
        if not cat_metrics.empty and 'bcg_quadrant' in cat_metrics.columns:
            # Verificar índice ou coluna de categoria
            if 'product_category_name' in cat_metrics.columns:
                bcg_map = cat_metrics.set_index('product_category_name')['bcg_quadrant'].to_dict()
            elif 'category' in cat_metrics.columns:
                bcg_map = cat_metrics.set_index('category')['bcg_quadrant'].to_dict()
            else:
                bcg_map = cat_metrics['bcg_quadrant'].to_dict()
        
        # Injetar quadrant em cada recomendação
        for rec in recommendations:
            cat_name = rec.get('category')
            # Tentar match exato, se não, normalizar
            quadrant = bcg_map.get(cat_name)
            if not quadrant:
                # Tentativa de match normalizado (se necessário)
                try:
                    norm_map = {k.strip().upper(): v for k, v in bcg_map.items() if isinstance(k, str)}
                    quadrant = norm_map.get(str(cat_name).strip().upper(), 'Interrogação')
                except Exception:
                    quadrant = 'Interrogação'
            
            rec['bcg_quadrant'] = quadrant
            
    except Exception as e:
        # Silencioso em caso de erro, mantém padrão
        pass
    # -----------------------------------------------------------
    
    # Debug: verificar se as recomendações foram geradas
    # st.write(f"🔍 DEBUG: {len(recommendations)} recomendações geradas")
    # if recommendations:
    #     st.write(f"🔍 DEBUG: Primeira recomendação: {recommendations[0]}")
    # else:
    #     st.write("🔍 DEBUG: Lista de recomendações vazia!")
    
    # Tabs para separar Resumo e Recomendações
    tab1, tab2 = st.tabs(["Resumo", "Previsão de Vendas e Recomendações"])
    
    with tab2:
        render_kpi_title(f"{box_icon} Recomendações Baseadas em Previsão de Vendas")
        
        # Filtros em duas colunas
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            action_options = [
                "Todas as ações",
                "Aumentar significativamente",
                "Aumentar moderadamente",
                "Reduzir moderadamente",
                "Reduzir significativamente",
                "Manter"
            ]
            selected_action = st.selectbox(
                "Filtrar por ação:",
                options=action_options,
                index=0,
                help="Selecione a ação de estoque que deseja visualizar"
            )
        
        with col_filter2:
            category_options = ["Todas as categorias"] + sorted(list({rec['category'] for rec in recommendations}))
            selected_category = st.selectbox(
                "Filtrar por categoria:",
                options=category_options,
                index=0,
                help="Selecione a categoria que deseja visualizar"
            )
        
        filtered_recommendations = recommendations
        
        # Aplicar filtros
        if selected_action != "Todas as ações":
            filtered_recommendations = [r for r in filtered_recommendations if r['action'] == selected_action]
            # st.write(f"🔍 DEBUG: Após filtro de ação: {len(filtered_recommendations)} recomendações")
        
        if selected_category != "Todas as categorias":
            filtered_recommendations = [r for r in filtered_recommendations if r['category'] == selected_category]
            # st.write(f"🔍 DEBUG: Após filtro de categoria: {len(filtered_recommendations)} recomendações")

        # Salvaguarda: se "Todas as categorias" estiver selecionado e não houver resultados,
        # mas existirem recomendações, reexibir todas para evitar UI vazia.
        if selected_category == "Todas as categorias" and not filtered_recommendations and recommendations:
            filtered_recommendations = recommendations
        
        # Mostrar contagem de recomendações
        st.write(f"Mostrando {len(filtered_recommendations)} recomendações de {len(recommendations)} total")
        
        # Debug: verificar filtros (comentado para limpar interface)
        # st.write(f"🔍 DEBUG: Ação selecionada: {selected_action}")
        # st.write(f"🔍 DEBUG: Categoria selecionada: {selected_category}")
        # st.write(f"🔍 DEBUG: Recomendações filtradas: {len(filtered_recommendations)}")
        
        # Debug: mostrar todas as ações disponíveis
        # if recommendations:
        #     available_actions = list(set([r['action'] for r in recommendations]))
        #     st.write(f"🔍 DEBUG: Ações disponíveis: {available_actions}")
        #     available_categories = list(set([r['category'] for r in recommendations]))
        #     st.write(f"🔍 DEBUG: Categorias disponíveis: {available_categories}")
        #     
        #     # Debug: mostrar primeira recomendação completa
        #     st.write(f"🔍 DEBUG: Primeira recomendação completa: {recommendations[0]}")
        #     
        #     # Debug: verificar se há problema com os filtros
        #     if selected_action != "Todas as ações":
        #         matching_actions = [r for r in recommendations if r['action'] == selected_action]
        #         st.write(f"🔍 DEBUG: Recomendações com ação '{selected_action}': {len(matching_actions)}")
        #     
        #     if selected_category != "Todas as categorias":
        #         matching_categories = [r for r in recommendations if r['category'] == selected_category]
        #         st.write(f"🔍 DEBUG: Recomendações com categoria '{selected_category}': {len(matching_categories)}")
        
        # Botão de download geral para todas as categorias filtradas
        if filtered_recommendations:
            # Gerar planilha consolidada (duas abas): Resumo_Categorias + Produtos_Estoque
            from io import BytesIO
            from components.glass_card import create_styled_download_button

            # 1) Resumo por categoria
            # Buscar dados de estoque para incluir no export.
            # Preferimos o snapshot enriquecido (unidades + valor potencial e capital).
            try:
                category_stock_data = {}
                stock_df = None
                try:
                    from utils.stock_loader import load_latest_stock  # type: ignore
                    stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="parquet")
                    if stock_df is None or (isinstance(stock_df, pd.DataFrame) and stock_df.empty):
                        stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="csv")
                except Exception:
                    stock_df = None

                if isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
                    tmp = stock_df.copy()
                    # Normalizar ID e colunas
                    if "produto_id" in tmp.columns and "product_id" not in tmp.columns:
                        tmp["product_id"] = tmp["produto_id"]
                    if "quantidade_disponivel_venda" in tmp.columns and "stock_level" not in tmp.columns:
                        tmp["stock_level"] = tmp["quantidade_disponivel_venda"]
                    tmp["product_id"] = tmp["product_id"].astype(str).str.strip().str.upper()
                    tmp["stock_level"] = pd.to_numeric(tmp.get("stock_level", 0), errors="coerce").fillna(0)

                    # Categoria: se não existir no snapshot, mapear via vendas
                    if "product_category_name" not in tmp.columns:
                        prod_cat = (
                            st.session_state["filtered_df"][["product_id", "product_category_name"]]
                            .dropna(subset=["product_id", "product_category_name"])
                            .drop_duplicates(subset=["product_id"])
                            .copy()
                        )
                        prod_cat["product_id"] = prod_cat["product_id"].astype(str).str.strip().str.upper()
                        tmp = tmp.merge(prod_cat, on="product_id", how="left")

                    tmp["product_category_name"] = tmp.get("product_category_name").fillna("Sem Categoria")

                    # Valor potencial (preferido) e capital (opcional)
                    if "valor_potencial_venda" in tmp.columns:
                        tmp["stock_value_potencial"] = pd.to_numeric(tmp["valor_potencial_venda"], errors="coerce").fillna(0)
                    else:
                        tmp["stock_value_potencial"] = 0.0
                    if "capital_imobilizado" in tmp.columns:
                        tmp["stock_value_capital"] = pd.to_numeric(tmp["capital_imobilizado"], errors="coerce").fillna(0)
                    else:
                        tmp["stock_value_capital"] = 0.0

                    category_stock_data = (
                        tmp.groupby("product_category_name")
                        .agg(
                            stock_level_num=("stock_level", "sum"),
                            stock_value_potencial=("stock_value_potencial", "sum"),
                            stock_value_capital=("stock_value_capital", "sum"),
                        )
                        .to_dict("index")
                    )
            except Exception:
                category_stock_data = {}
            
            summary_rows = []
            for rec in filtered_recommendations:
                cat_name = rec.get('category')
                stock_info = category_stock_data.get(cat_name, {})
                
                summary_rows.append({
                    'categoria': cat_name,
                    'acao': rec.get('action'),
                    'variacao_prevista': rec.get('variation'),
                    'estoque_intervalo': rec.get('Estoque Sugerido (Vendas)'),
                    'velocidade_vendas': rec.get('Velocidade de Vendas'),
                    'modelo_usado': rec.get('model'),
                    'horizonte': rec.get('horizon'),
                    'estoque_unidades_atual': stock_info.get('stock_level_num', 0),
                    # Mantemos o nome para compatibilidade, mas o conteúdo é "valor potencial de venda" (produção).
                    'estoque_valor_atual_R$': stock_info.get('stock_value_potencial', 0),
                    # Coluna extra (opcional): capital imobilizado (custo)
                    'estoque_capital_imobilizado_R$': stock_info.get('stock_value_capital', 0)
                })
            resumo_categorias_df = pd.DataFrame(summary_rows)

            # 2) Produtos_Estoque consolidado
            produtos_list = []
            if 'filtered_df' in st.session_state:
                for rec in filtered_recommendations:
                    try:
                        df_cat = generate_stock_recommendation_download_data(
                            st.session_state['filtered_df'], rec['category']
                        )
                    except Exception:
                        df_cat = pd.DataFrame()
                    if df_cat is not None and not df_cat.empty:
                        df_cat = df_cat.copy()
                        df_cat['categoria'] = rec.get('category')
                        produtos_list.append(df_cat)

            if produtos_list:
                produtos_estoque_df = pd.concat(produtos_list, ignore_index=True)
            else:
                # Cabeçalho padrão mesmo quando vazio (schema atualizado com novas métricas)
                produtos_estoque_df = pd.DataFrame(columns=[
                    'product_category_name', 'product_id', 'total_revenue', 'avg_price', 'total_orders', 
                    'avg_rating', 'cancellation_rate', 
                    'current_stock', 'stock_source', 'stock_value',
                    'daily_demand', 'adjusted_daily_demand', 'coefficient_variation',
                    'safety_stock', 'estoque_para_vendas_periodo', 'stock_gap', 'days_until_stockout',
                    'avg_delivery_time_days', 'monthly_turnover', 'categoria'
                ])

            # Criar arquivo Excel em memória com as duas abas
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                resumo_categorias_df.to_excel(writer, sheet_name='Resumo_Categorias', index=False)
                produtos_estoque_df.to_excel(writer, sheet_name='Produtos_Estoque', index=False)
            output.seek(0)

            col_download1, col_download2, col_download3 = st.columns([1, 2, 1])
            with col_download2:
                create_styled_download_button(
                    label=f"📥 Baixar Todas as Recomendações ({len(filtered_recommendations)} categorias)",
                    data=output.getvalue(),
                    file_name=f"recomendacoes_vendas_{len(filtered_recommendations)}_categorias.xlsx"
                )
        
        st.markdown("---")
        
        # Layout em 3 colunas para melhor organização
        if filtered_recommendations:
            cols = st.columns(3)
            for idx, recommendation in enumerate(filtered_recommendations):
                with cols[idx % 3]:
                    render_recommendation_card(recommendation)
        else:
            # st.write("🔍 DEBUG: Nenhuma recomendação para exibir após filtros")
            # st.write(f"🔍 DEBUG: Recomendações originais: {len(recommendations)}")
            # if recommendations:
            #     st.write(f"🔍 DEBUG: Primeira recomendação original: {recommendations[0]}")
            st.info("Nenhuma recomendação encontrada com os filtros selecionados.")

    with tab1:
        render_kpi_title(f"{chart_icon} Resumo dos Insights Principais")
        
        # Layout em 2x2 para melhor organização
        col1, col2 = st.columns(2)
        
        with col1:
            col1_1, col1_2 = st.columns(2)
            
            with col1_1:
                st.markdown(render_insight_card(
                    "Tendência de Receita",
                    f"{insights['revenue']['growth_rate']:.1f}%",
                    insights['revenue']['trend'],
                    insights['revenue']['trend_icon'],
                    "Comparação entre o 1º e o último mês"
                ), unsafe_allow_html=True)
            
            with col1_2:
                st.markdown(render_insight_card(
                    "Satisfação Média",
                    f"{insights['satisfaction']['avg_satisfaction']:.2f}/5",
                    insights['satisfaction']['satisfaction_trend'],
                    insights['satisfaction']['trend_icon'],
                    "Nota média nas últimas avaliações"
                ), unsafe_allow_html=True)
        
        with col2:
            col2_1, col2_2 = st.columns(2)
            
            with col2_1:
                st.markdown(render_insight_card(
                    "Taxa de Cancelamento",
                    f"{insights['cancellation']['cancellation_rate']*100:.1f}%",
                    insights['cancellation']['cancellation_trend'],
                    insights['cancellation']['trend_icon'],
                    "Tendência dos cancelamentos"
                ), unsafe_allow_html=True)
            
            with col2_2:
                st.markdown(render_insight_card(
                    "Tempo Médio de Entrega",
                    f"{insights['delivery']['avg_delivery_time']:.1f} dias",
                    insights['delivery']['delivery_trend'],
                    insights['delivery']['trend_icon'],
                    "Tendência do tempo de entrega"
                ), unsafe_allow_html=True)

def format_value(value: Any, is_integer: bool = False) -> str:
    """Formata valores numéricos ou Series para exibição."""
    if isinstance(value, pd.Series):
        value = value.sum()
    if isinstance(value, (int, float)):
        if is_integer:
            return f"{int(value):,}".replace(",", ".")
        return f"{value:,.2f}".replace(",", ".")
    return str(value)

def render_recommendation_card(recommendation: Dict[str, Any]) -> None:
    """
    Renderiza um card de recomendação de estoque com estilização glassmorphism metálico
    e faz o card inteiro ser clicável para download do Excel.
    """
    import streamlit.components.v1 as components
    import pandas as pd
    import base64
    import re
    from io import BytesIO
    from utils.theme_manager import get_theme_manager

    def clean_text(value):
        """Remove HTML e garante string limpa"""
        if value is None:
            return ""
        return re.sub(r'<.*?>', '', str(value))

    theme_manager = get_theme_manager()
    theme = theme_manager.get_theme()
    glass = theme_manager.get_glass_theme()

    # Definir cores baseadas na ação
    action_colors = {
        "Aumentar significativamente": "#10b981",  # Verde
        "Aumentar moderadamente": "#3b82f6",      # Azul
        "Manter": "#f59e0b",                      # Amarelo
        "Reduzir moderadamente": "#f97316",       # Laranja
        "Reduzir significativamente": "#ef4444"   # Vermelho
    }
    color = action_colors.get(recommendation['action'], theme.get('primaryColor', '#6366f1'))

    # Definir ícones baseados na ação
    action_icons = {
        "Aumentar significativamente": get_svg_icon("trend", size=24, color=color),
        "Aumentar moderadamente": get_svg_icon("chart", size=24, color=color),
        "Manter": get_svg_icon("cycle", size=24, color=color),
        "Reduzir moderadamente": get_svg_icon("minus", size=24, color=color),
        "Reduzir significativamente": get_svg_icon("warning", size=24, color=color)
    }
    icon = action_icons.get(recommendation['action'], get_svg_icon("clipboard", size=24, color=color))

    # -----------------------------------------------
    # Preparar dados do BCG (Pill)
    # -----------------------------------------------
    bcg_quadrant = recommendation.get('bcg_quadrant', 'Indefinido')
    
    # Ícones específicos para cada quadrante
    bcg_icons_map = {
        'Estrela Digital': get_svg_icon("estrela_digital", size=14, color="#fbbf24"),
        'Vaca Leiteira': get_svg_icon("vaca_leiteira", size=14, color="#cbd5e1"),
        'Interrogação': get_svg_icon("interrogacao", size=14, color="#38bdf8"),
        'Abacaxi': get_svg_icon("abacaxi", size=14, color="#ef4444"),
        'Indefinido': get_svg_icon("alert_triangle", size=14, color="#94a3b8")
    }
    bcg_icon_svg = bcg_icons_map.get(bcg_quadrant, bcg_icons_map['Indefinido'])
    
    # Estilo da Pill BCG
    bcg_pill_style = "display: flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 20px; background: rgba(255, 255, 255, 0.08); border: 1px solid rgba(255, 255, 255, 0.15); color: #cbd5e1; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;"


    # Carregar CSS global uma vez só
    if "recommendation_css_loaded" not in st.session_state:
        components.html("""
        <style>
        @keyframes metallicShine {
            0% { transform: translateX(-50%); }
            100% { transform: translateX(50%); }
        }
        .recommendation-card:hover {
            transform: translateY(-4px);
            box-shadow: 
                0 12px 40px rgba(0, 0, 0, 0.4),
                0 4px 20px rgba(227, 236, 240, 0.15),
                inset 0 1px 0 rgba(255, 255, 255, 0.2);
            border-color: rgba(227, 236, 240, 0.4);
        }
        .metric-item { transition: all 0.2s ease; }
        .metric-item:hover {
            background: rgba(227, 236, 240, 0.08) !important;
            transform: translateX(2px);
        }
        </style>
        """, height=0)
        st.session_state["recommendation_css_loaded"] = True

    # Preparar valores limpos antes de interpolar no HTML (aceita formato ML ou fallback)
    action = clean_text(recommendation.get('action', ''))
    reason = clean_text(recommendation.get('reason', ''))
    category = clean_text(recommendation.get('category', ''))

    # Variação: pode vir como número (fallback) ou string "X.XX%" (ML)
    raw_variation = recommendation.get('variation', 0)
    if isinstance(raw_variation, (int, float)):
        variation_value = float(raw_variation)
        variation = f"{variation_value:.2f}%" if abs(variation_value) <= 999 else f"{variation_value:,.0f}%".replace(",", ".")
    else:
        variation = clean_text(raw_variation) if raw_variation else "0.00%"
        try:
            import re as _re
            variation_value = float(_re.sub(r"[^0-9\.\-]", "", variation))
        except Exception:
            variation_value = 0.0
        if not variation.strip().endswith("%"):
            variation = f"{variation_value:.2f}%"

    # Campos renomeados: usar diretamente as strings já formatadas (ML ou fallback)
    ideal_stock = clean_text(recommendation.get('Estoque Sugerido (Vendas)', '0 unidades'))
    turnover = clean_text(recommendation.get('Velocidade de Vendas', '0.00 vendas/dia'))

    # Informações do modelo e horizonte (ML ou fallback)
    model_info = clean_text(recommendation.get('model', 'N/A'))
    horizon_info = clean_text(recommendation.get('horizon', 'N/A'))
    
    # Truncar motivo para visualização, mantendo "title" com texto completo
    reason_full = reason
    if len(reason) > 110:
        reason = reason[:107] + "..."

    # Barra de variação: escala 0–100% (valores >100% aparecem como barra cheia)
    progress_width = max(0, min(100, abs(variation_value)))
    is_positive = variation_value >= 0

    # Altura fixa mais compacta
    card_height = 340

    # Gerar arquivo Excel em memória
    download_data = None
    if 'filtered_df' in st.session_state:
        try:
            download_data = generate_stock_recommendation_download_data(
                st.session_state['filtered_df'], recommendation['category']
            )
        except Exception as e:
            # st.write(f"🔍 DEBUG: Erro ao gerar download_data para {recommendation['category']}: {e}")
            download_data = None

    # Sempre renderizar o card; preparar arquivo com duas abas (Resumo + Produtos)
    if True:
        # Resumo da categoria (sempre presente)
        resumo_df = pd.DataFrame([{
            'categoria': category,
            'acao': action,
            'motivo': reason,
            'variacao_prevista': variation,
            'estoque_ideal': ideal_stock,
            'estoque_atual': recommendation.get('details', {}).get('estoque_atual_fisico', 'N/A'),
            'gap_estoque': recommendation.get('details', {}).get('stock_gap', 'N/A'),
            'velocidade_vendas': turnover,
            'modelo_usado': model_info,
            'horizonte': horizon_info
        }])

        # Produtos_Estoque: usar dados gerados ou cabeçalho vazio
        if download_data is not None and not download_data.empty:
            produtos_df = download_data.copy()
            # Consistência: incluir coluna de categoria
            if 'categoria' not in produtos_df.columns:
                produtos_df['categoria'] = category
            
            # --- CORREÇÃO EXCEL: Atualizar current_stock com valor real se disponível ---
            # Se tivermos estoque_atual_fisico nos details, podemos tentar distribuir ou apenas marcar
            # Mas o ideal é que generate_stock_recommendation_download_data já pegue o real.
            # Como fallback, vamos garantir que a coluna current_stock venha numérica e não 'estimado'
            if 'current_stock' in produtos_df.columns:
                 # Se for string 'estimado', tentar limpar
                 produtos_df['current_stock'] = pd.to_numeric(produtos_df['current_stock'], errors='coerce').fillna(0)
            
            if 'stock_value' in produtos_df.columns:
                 produtos_df['stock_value'] = pd.to_numeric(produtos_df['stock_value'], errors='coerce').fillna(0)

        else:
            produtos_df = pd.DataFrame(columns=[
                'product_category_name', 'product_id', 'total_revenue', 'avg_price', 'total_orders', 
                'avg_rating', 'cancellation_rate', 
                'current_stock', 'stock_source', 'stock_value',
                'daily_demand', 'adjusted_daily_demand', 'coefficient_variation',
                'safety_stock', 'estoque_para_vendas_periodo', 'stock_gap', 'days_until_stockout',
                'avg_delivery_time_days', 'monthly_turnover'
            ])

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            resumo_df.to_excel(writer, sheet_name='Resumo_Categoria', index=False)
            produtos_df.to_excel(writer, sheet_name='Produtos_Estoque', index=False)
            from utils.excel_style import style_excel_workbook
            style_excel_workbook(writer.book)
        output.seek(0)

        # Codificar em base64 para <a download>
        b64 = base64.b64encode(output.read()).decode()
        filename = f"{category}_recomendacoes_estoque.xlsx"
        href = f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}'

        # Card clicável (todo card vira link para download) — versão compacta e digerível
        card_html = f'''<a href="{href}" download="{filename}" style="text-decoration: none;">
        <div class="recommendation-card" style="
            background: linear-gradient(135deg, rgba(30,41,59,0.82), rgba(51,65,85,0.9) 60%, rgba(30,41,59,0.82));
            border-radius: 16px;
            padding: 14px 16px 16px 16px;
            margin: 0px 0;
            border: 1px solid rgba(227,236,240,0.25);
            box-shadow: 0 10px 28px rgba(0,0,0,0.35), 0 2px 12px rgba(227,236,240,0.10), inset 0 1px 0 rgba(255,255,255,0.08);
            backdrop-filter: blur(14px);
            -webkit-backdrop-filter: blur(14px);
            transition: all 0.25s ease;
            cursor: pointer;
            position: relative;
            overflow: hidden;
            color: #e3ecf0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', sans-serif;
            min-height: 0;
        ">
            <!-- Brilho metálico -->
            <div class="metallic-shine" style="
                position: absolute; top: 0; left: -100%; width: 200%; height: 100%;
                background: linear-gradient(90deg, transparent 0%, rgba(227,236,240,0.05) 40%, rgba(227,236,240,0.15) 50%, rgba(227,236,240,0.05) 60%, transparent 100%);
                z-index: 1; animation: metallicShine 8s infinite linear;
            "></div>

            <!-- Conteúdo -->
            <div class="card-content" style="position: relative; z-index: 2;">
                <!-- Top row: ícone + categoria + BCG Pill + Ação Pill -->
                <div class="card-header" style="display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px;">
                    <div style="display:flex; align-items:center; gap:10px;">
                        <span style="font-size:20px; color:{color};">{icon}</span>
                        <h3 style="margin:0; font-size:16px; font-weight:700; letter-spacing:0.2px;">{category}</h3>
                    </div>
                    
                    <div style="display:flex; align-items:center; gap:8px;">
                        <!-- BCG Pill -->
                        <div style="{bcg_pill_style}">
                            {bcg_icon_svg} {bcg_quadrant}
                        </div>
                        <!-- Action Pill -->
                    <div style="background: {color}22; color:{color}; border:1px solid {color}55; padding:4px 8px; border-radius:999px; font-size:12px; font-weight:600;">{action}</div>
                    </div>
                </div>

                <!-- Linha da variação: valor + barra -->
                <div style="display:flex; align-items:center; gap:10px; margin-bottom:10px;">
                    <div style="min-width:92px; text-align:center; padding:6px 8px; border-radius:10px; border:1px solid {('#10b981' if is_positive else '#ef4444')}55; background:{('#10b981' if is_positive else '#ef4444')}22;">
                        <div style="font-size:11px; color:#94a3b8;">Variação</div>
                        <div style="font-size:18px; font-weight:800; color:{'#10b981' if is_positive else '#ef4444'}; line-height:1;">{variation}</div>
                    </div>
                    <div style="flex:1; display:flex; flex-direction:column; gap:6px;">
                        <div style="height:8px; border-radius:999px; background:rgba(148,163,184,0.2); overflow:hidden;">
                            <div style="width:{progress_width}%; height:100%; background: linear-gradient(90deg, {('#16a34a' if is_positive else '#ef4444')}, {('#22c55e' if is_positive else '#f97316')});"></div>
                        </div>
                        <div style="display:flex; justify-content:space-between; font-size:11px; color:#94a3b8;">
                            <span>0%</span><span>50%</span><span>100%</span>
                        </div>
                    </div>
                </div>

                <!-- Métricas principais em 2 colunas -->
                <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-bottom:10px;">
                    <div class="metric-item" style="background: linear-gradient(90deg, transparent 0%, #10b98122 50%, transparent 100%); padding:10px; border:1px solid #10b98155; border-radius:12px; text-align:center;">
                        <div style="font-size:11px; color:#94a3b8;">Estoque sugerido (vendas)</div>
                        <div style="font-size:16px; font-weight:700; color:#10b981;">{ideal_stock}</div>
                    </div>
                    <div class="metric-item" style="background: linear-gradient(90deg, transparent 0%, #8b5cf622 50%, transparent 100%); padding:10px; border:1px solid #8b5cf655; border-radius:12px; text-align:center;">
                        <div style="font-size:11px; color:#94a3b8;">Velocidade média de vendas</div>
                        <div style="font-size:16px; font-weight:700; color:#8b5cf6;">{turnover}</div>
                    </div>
                </div>

                <!-- Motivo + rodapé -->
                <div style="background: linear-gradient(90deg, transparent 0%, #f59e0b22 50%, transparent 100%); padding:10px; border:1px solid #f59e0b55; border-radius:12px; margin-bottom:10px;">
                    <div style="font-size:11px; color:#94a3b8; margin-bottom:4px;">Motivo</div>
                    <div style="font-size:13px;" title="{reason_full}">{reason}</div>
                </div>

                <div style="display:flex; justify-content:space-between; align-items:center; font-size:12px; color:#94a3b8;">
                    <div><span style="color:#8b5cf6; font-weight:600;">Modelo</span>: {model_info}</div>
                    <div><span style="color:#06b6d4; font-weight:600;">Horizonte</span>: {horizon_info}</div>
                </div>
            </div>
        </div>
        </a>'''
        components.html(card_html, height=card_height, scrolling=False)

    else:
        st.info(f"Sem dados disponíveis para {category}")

def render_category_highlight_insights(filtered_df: pd.DataFrame, best_category: str, best_category_profit: float, sorted_categories: List[Tuple[str, float]]) -> None:
    """
    Renderiza os insights da categoria em destaque, mostrando informações sobre a categoria mais rentável,
    a categoria com maior participação e a categoria com maior margem de rentabilidade.
    
    Args:
        filtered_df (pd.DataFrame): DataFrame filtrado com os dados
        best_category (str): Nome da categoria mais rentável (maior lucro absoluto)
        best_category_profit (float): Lucro da categoria mais rentável
        sorted_categories (List[Tuple[str, float]]): Lista de tuplas contendo (categoria, margem_rentabilidade_%)
            ordenadas por margem de rentabilidade
    
    Returns:
        None: A função apenas renderiza os insights na interface
    """
    # Criar ícones SVG
    trophy_icon = get_svg_icon("target", size=24)  # Para representar destaque
    money_icon = get_svg_icon("money", size=24)
    trend_icon = get_svg_icon("trend", size=24)
    
    # Se não houver categoria válida, exibir aviso
    if not best_category:
        show_centered_info("Não há dados suficientes para insights da categoria em destaque.")
        return
    
    #st.markdown("---")
    #render_kpi_title(f"{trophy_icon} Insights da Categoria em Destaque")
    col1, col2, col3 = st.columns(3)
    
    # Métricas auxiliares para participação
    revenue_col = 'revenue' if 'revenue' in filtered_df.columns else 'price'
    revenue_by_category = (
        filtered_df.groupby('product_category_name')[revenue_col].sum()
        if not filtered_df.empty else pd.Series(dtype=float)
    )
    best_revenue_category = revenue_by_category.idxmax() if not revenue_by_category.empty else "N/A"
    best_revenue_total = revenue_by_category.max() if not revenue_by_category.empty else 0
    total_revenue = revenue_by_category.sum()
    avg_revenue_per_category = revenue_by_category.mean() if not revenue_by_category.empty else 0
    revenue_share = (best_revenue_total / total_revenue * 100) if total_revenue > 0 else 0

    with col1:
        # Lucro médio por categoria (usa margin_net_revenue se disponível, senão revenue como proxy)
        profit_col = 'margin_net_revenue' if 'margin_net_revenue' in filtered_df.columns else 'price'
        category_profit = filtered_df.groupby('product_category_name')[profit_col].sum() if not filtered_df.empty else pd.Series(dtype=float)
        avg_profit = category_profit.mean() if not category_profit.empty else 0

        st.markdown(render_insight_card(
            "Melhor Resultado Operacional",
            best_category,
            f"R$ {format_value(best_category_profit)}",
            get_svg_icon("money", size=24, color="#10b981"),
            f"Resultado médio por categoria: R$ {format_value(avg_profit)}"
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(render_insight_card(
            "Categoria com Maior Participação",
            best_revenue_category,
            f"{format_value(revenue_share)}% do faturamento total",
            get_svg_icon("chart", size=24, color="#3b82f6"),
            f"Categoria faturou R$ {format_value(best_revenue_total - avg_revenue_per_category)} mais do que o faturamento médio por categoria (R$ {format_value(avg_revenue_per_category)})."
        ), unsafe_allow_html=True)

    with col3:
        top_margin = sorted_categories[0][1] if sorted_categories else 0
        avg_margin = float(pd.Series([m for _, m in sorted_categories]).mean()) if sorted_categories else 0.0
        margin_diff = top_margin - avg_margin

        st.markdown(render_insight_card(
            "Maior Eficiência Operacional",
            sorted_categories[0][0] if sorted_categories else "N/A",
            f"{format_value(top_margin)}%",
            get_svg_icon("trend", size=24, color="#10b981"),
            f"Margem de {format_value(margin_diff)}pp acima da margem média de {format_value(avg_margin)}%."
        ), unsafe_allow_html=True)

@st.cache_data(ttl=3600, show_spinner="Preparando dados para download...")
def generate_stock_recommendation_download_data(
    filtered_df: pd.DataFrame, 
    category: str
) -> pd.DataFrame:
    """
    Gera dados detalhados de produtos para download da seção de Recomendações de Estoque.
    
    Esta função cria um DataFrame com informações que permitem ao analista:
    1. Entender a situação atual de cada produto
    2. Ver as recomendações específicas
    3. Calcular o impacto financeiro das ações
    4. Planejar estoque ideal por produto
    5. Otimizar tempo de entrega
    6. Analisar giro mensal individual
    
    Cache: TTL de 1 hora baseado no hash do DataFrame filtrado e categoria
    
    Args:
        filtered_df: DataFrame com os dados filtrados
        category: Categoria selecionada para download
        
    Returns:
        DataFrame com dados detalhados para download incluindo:
        - Métricas básicas (receita, preço, pedidos, rating)
        - Estoque atual e valor
        - Estoque ideal ponderado por produto
        - Tempo de entrega médio
        - Giro mensal individual
    """
    # Filtrar produtos da categoria selecionada
    category_products = filtered_df[filtered_df['product_category_name'] == category].copy()
    
    if category_products.empty:
        return pd.DataFrame()
    
    # Verificar se product_id está disponível
    available_columns = category_products.columns.tolist()
    
    # Determinar coluna de produto para agrupamento
    product_id_col = None
    if "product_id" in available_columns:
        product_id_col = "product_id"
    elif "sku" in available_columns:
        product_id_col = "sku"
    elif "produto_id" in available_columns:
        product_id_col = "produto_id"
    elif "codigo_produto" in available_columns:
        product_id_col = "codigo_produto"
    else:
        # Se não houver coluna de produto, usar order_id como fallback
        product_id_col = "order_id"
    
    # Calcular métricas por produto
    agg_dict = {
        'price': ['sum', 'mean', 'count'],
        'order_purchase_timestamp': 'count'  # Frequência de vendas
    }
    
    # Adicionar métricas opcionais apenas se as colunas existirem
    if 'review_score' in available_columns:
        agg_dict['review_score'] = 'mean'
    
    if 'pedido_cancelado' in available_columns:
        agg_dict['pedido_cancelado'] = 'mean'
        
    product_metrics = category_products.groupby([product_id_col, 'product_category_name']).agg(agg_dict).reset_index()
    
    # Renomear a coluna de produto para consistência
    if product_id_col != "product_id":
        # Se for MultiIndex, o rename pode ser complexo, melhor achatar antes
        pass
    
    # Achatar MultiIndex de colunas gerado pelo agg
    new_cols = []
    for col in product_metrics.columns:
        if isinstance(col, tuple):
            # col[0] é o nome da coluna original, col[1] é a métrica (sum, mean, etc)
            if col[1]:
                new_cols.append(f"{col[0]}_{col[1]}")
            else:
                new_cols.append(col[0])
        else:
            new_cols.append(col)
    product_metrics.columns = new_cols

    # Mapear para nomes finais esperados
    rename_map = {
        f'{product_id_col}': 'product_id',
        'product_category_name': 'product_category_name',
        'price_sum': 'total_revenue',
        'price_mean': 'avg_price',
        'price_count': 'total_orders',
        'order_purchase_timestamp_count': 'sales_frequency',
        'review_score_mean': 'avg_rating',
        'pedido_cancelado_mean': 'cancellation_rate'
    }
    
    # Aplicar renomeação
    product_metrics = product_metrics.rename(columns=rename_map)
    
    # Garantir colunas essenciais que podem faltar se a agregação opcional não ocorreu
    if 'avg_rating' not in product_metrics.columns:
        product_metrics['avg_rating'] = 0.0
    if 'cancellation_rate' not in product_metrics.columns:
        product_metrics['cancellation_rate'] = 0.0
    
    # ============================================
    # VALIDAÇÃO E CÁLCULO DE ESTOQUE INTELIGENTE
    # ============================================
    
    # 1. TENTAR CARREGAR ESTOQUE REAL DO SNAPSHOT (PRIORIDADE MÁXIMA)
    # (evita divergências com BCG e garante unidades reais por SKU)
    stock_from_parquet = False
    try:
        from pathlib import Path

        # Preferir snapshot (parquet->csv fallback)
        stock_df = None
        try:
            from utils.stock_loader import load_latest_stock  # type: ignore
            stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="parquet")
            if stock_df is None or (isinstance(stock_df, pd.DataFrame) and stock_df.empty):
                stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="csv")
        except Exception:
            stock_df = None

        if isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
            tmp = stock_df.copy()
            if "produto_id" in tmp.columns and "product_id" not in tmp.columns:
                tmp["product_id"] = tmp["produto_id"]
            if "quantidade_disponivel_venda" in tmp.columns and "stock_level" not in tmp.columns:
                tmp["stock_level"] = tmp["quantidade_disponivel_venda"]
            tmp["product_id"] = tmp["product_id"].astype(str).str.strip().str.upper()
            tmp["stock_level"] = pd.to_numeric(tmp.get("stock_level", 0), errors="coerce").fillna(0)

            # Mapas por produto (somar depósitos)
            stock_qty_map = tmp.groupby("product_id")["stock_level"].sum()

            # Valores do snapshot (já totais por linha, mas somamos por produto)
            stock_val_pot_map = None
            if "valor_potencial_venda" in tmp.columns:
                tmp["_val_pot"] = pd.to_numeric(tmp["valor_potencial_venda"], errors="coerce").fillna(0)
                stock_val_pot_map = tmp.groupby("product_id")["_val_pot"].sum()
            stock_val_cap_map = None
            if "capital_imobilizado" in tmp.columns:
                tmp["_val_cap"] = pd.to_numeric(tmp["capital_imobilizado"], errors="coerce").fillna(0)
                stock_val_cap_map = tmp.groupby("product_id")["_val_cap"].sum()

            # Mapear para product_metrics
            product_metrics["product_id_str"] = product_metrics["product_id"].astype(str).str.strip().str.upper()
            product_metrics["current_stock"] = product_metrics["product_id_str"].map(stock_qty_map).fillna(0)
            product_metrics["stock_source"] = "real (snapshot)"

            # Valor: manter padrão produção = potencial de venda
            if stock_val_pot_map is not None:
                product_metrics["stock_value"] = product_metrics["product_id_str"].map(stock_val_pot_map).fillna(0)
            else:
                product_metrics["stock_value"] = np.nan

            # Coluna extra opcional: capital imobilizado
            if stock_val_cap_map is not None:
                product_metrics["stock_value_capital"] = product_metrics["product_id_str"].map(stock_val_cap_map).fillna(0)
            else:
                product_metrics["stock_value_capital"] = 0.0

            product_metrics.drop(columns=["product_id_str"], inplace=True)

            if float(product_metrics["current_stock"].sum()) > 0:
                stock_from_parquet = True

        # Fallback: arquivo RAW listEstoque se snapshot não existir/estiver vazio
        if not stock_from_parquet:
            from magazord_pipeline.transformers import normalize_stock
            stock_path = Path("data/raw/magazord_stock_raw.parquet")
            if stock_path.exists():
                stock_raw = pd.read_parquet(stock_path)
                stock_df = normalize_stock(stock_raw)
                if not stock_df.empty and "product_id" in stock_df.columns:
                    stock_df["product_id"] = stock_df["product_id"].astype(str).str.strip().str.upper()
                    stock_map = stock_df.groupby("product_id")["stock_level"].sum()
                    product_metrics["product_id_str"] = product_metrics["product_id"].astype(str).str.strip().str.upper()
                    product_metrics["current_stock"] = product_metrics["product_id_str"].map(stock_map).fillna(0)
                    product_metrics["stock_source"] = "real (magazord)"
                    # não temos valor_potencial aqui; deixar NaN para calcular via avg_price depois
                    product_metrics["stock_value"] = np.nan
                    product_metrics["stock_value_capital"] = 0.0
                    product_metrics.drop(columns=["product_id_str"], inplace=True)
                    if float(product_metrics["current_stock"].sum()) > 0:
                        stock_from_parquet = True
    except Exception as e:
        # Silenciosamente falhar e tentar outros métodos
        pass

    if not stock_from_parquet:
        # 2. VERIFICAR SE HÁ COLUNA DE ESTOQUE NO DATAFRAME ORIGINAL
        stock_column_candidates = ['stock_quantity', 'estoque_atual', 'inventory', 'stock', 'quantidade_estoque', 'stock_level']
        real_stock_col = None
        for col in stock_column_candidates:
            if col in category_products.columns:
                real_stock_col = col
                break
        
        # 3. USAR ESTOQUE REAL (DATASET) OU SIMULAR
        if real_stock_col:
            # Pegar último estoque conhecido por produto
            real_stock = category_products.groupby(product_id_col)[real_stock_col].last()
            product_metrics['current_stock'] = product_metrics['product_id'].map(real_stock).fillna(0)
            product_metrics['stock_source'] = 'real (dataset)'
        else:
            # Simulação: 1.2x total de pedidos (buffer para reposição)
            product_metrics['current_stock'] = product_metrics['total_orders'] * 1.2
            product_metrics['stock_source'] = 'estimado'
    
    # 3. CALCULAR PERÍODO DE DADOS (dias únicos no filtro)
    if 'order_purchase_timestamp' in category_products.columns:
        dates = pd.to_datetime(category_products['order_purchase_timestamp'], errors='coerce')
        days_in_period = (dates.max() - dates.min()).days + 1
        days_in_period = max(days_in_period, 30)  # Mínimo 30 dias
    else:
        days_in_period = 90  # Fallback
    
    # 4. DEMANDA DIÁRIA E VOLATILIDADE
    product_metrics['daily_demand'] = product_metrics['total_orders'] / days_in_period
    
    # Calcular desvio padrão das vendas diárias (proxy de volatilidade)
    daily_sales_std = category_products.groupby([product_id_col, pd.to_datetime(category_products['order_purchase_timestamp'], errors='coerce').dt.date]).size().groupby(level=0).std()
    product_metrics['demand_volatility'] = product_metrics['product_id'].map(daily_sales_std).fillna(0)
    
    # Coeficiente de variação (CV = std / mean)
    product_metrics['coefficient_variation'] = np.where(
        product_metrics['daily_demand'] > 0,
        product_metrics['demand_volatility'] / product_metrics['daily_demand'],
        0
    ).clip(0, 2)  # Limitar entre 0 e 2
    
    # 5. OBTER HORIZONTE E VARIAÇÃO DA RECOMENDAÇÃO ML
    recommendations = generate_category_recommendations(filtered_df)
    category_rec = next((r for r in recommendations if r['category'] == category), None)
    
    horizon_days = 14
    ml_variation_pct = 0.0
    
    if category_rec:
        # Extrair horizonte
        horizon_str = str(category_rec.get('horizon', '14 dias'))
        try:
            import re as _re
            horizon_match = _re.search(r'(\d+)\s*dias', horizon_str)
            if horizon_match:
                horizon_days = int(horizon_match.group(1))
        except Exception:
            pass
        
        # Extrair variação ML
        variation_str = str(category_rec.get('variation', '0%'))
        try:
            ml_variation_pct = float(_re.sub(r"[^0-9\.-]", "", variation_str))
        except Exception:
            pass
    
    # 6. DEMANDA AJUSTADA PELA PREVISÃO ML
    product_metrics['adjusted_daily_demand'] = product_metrics['daily_demand'] * (1 + ml_variation_pct / 100)
    
    # 7. LEAD TIME (tempo de reposição)
    # Usar avg_delivery_time_days se disponível, senão 15 dias
    lead_time_days = 15
    
    # 8. ESTOQUE DE SEGURANÇA (baseado em volatilidade e nível de serviço 95%)
    z_score_95 = 1.65  # Nível de serviço 95%
    product_metrics['safety_stock'] = (
        z_score_95 * 
        product_metrics['adjusted_daily_demand'] * 
        np.sqrt(lead_time_days) * 
        product_metrics['coefficient_variation']
    ).round(0)
    
    # 9. ESTOQUE IDEAL = (Demanda Ajustada * Horizonte) + Estoque de Segurança
    product_metrics['estoque_para_vendas_periodo'] = (
        (product_metrics['adjusted_daily_demand'] * horizon_days) + 
        product_metrics['safety_stock']
    ).round(0)
    
    # 10. GAP DE ESTOQUE (diferença entre ideal e atual)
    product_metrics['stock_gap'] = (
        product_metrics['estoque_para_vendas_periodo'] - product_metrics['current_stock']
    ).round(0)
    
    # 11. RISCO DE RUPTURA (dias até stockout se demanda continuar)
    product_metrics['days_until_stockout'] = np.where(
        product_metrics['adjusted_daily_demand'] > 0,
        (product_metrics['current_stock'] / product_metrics['adjusted_daily_demand']).round(1),
        999  # Sem risco se não há demanda
    )
    
    # Valor do estoque atual
    # Preferir valor do snapshot (potencial de venda) quando disponível; senão calcular via avg_price do período filtrado.
    if "stock_value" in product_metrics.columns:
        product_metrics["stock_value"] = pd.to_numeric(product_metrics["stock_value"], errors="coerce")
    else:
        product_metrics["stock_value"] = np.nan
    mask_nan_val = product_metrics["stock_value"].isna()
    if mask_nan_val.any():
        product_metrics.loc[mask_nan_val, "stock_value"] = (
            pd.to_numeric(product_metrics.loc[mask_nan_val, "current_stock"], errors="coerce").fillna(0)
            * pd.to_numeric(product_metrics.loc[mask_nan_val, "avg_price"], errors="coerce").fillna(0)
        )
    product_metrics["stock_value"] = product_metrics["stock_value"].fillna(0)
    
    # Calcular tempo de entrega médio (baseado na frequência de vendas)
    product_metrics['avg_delivery_time_days'] = np.where(
        product_metrics['sales_frequency'] > 0,
        (30 / product_metrics['sales_frequency']).clip(1, 30),  # Entre 1 e 30 dias
        15  # Padrão se não houver vendas
    ).round(1)
    
    # Calcular giro mensal individual por produto
    product_metrics['monthly_turnover'] = (
        product_metrics['total_orders'] / 3  # Assumindo 3 meses de dados
    ).round(2)
    
    # Selecionar e ordenar colunas para download (métricas inteligentes por produto)
    download_columns = [
        'product_category_name', 'product_id', 'total_revenue', 'avg_price', 'total_orders', 
        'avg_rating', 'cancellation_rate', 
        'current_stock', 'stock_source', 'stock_value',
        'daily_demand', 'adjusted_daily_demand', 'coefficient_variation',
        'safety_stock', 'estoque_para_vendas_periodo', 'stock_gap', 'days_until_stockout',
        'avg_delivery_time_days', 'monthly_turnover'
    ]
    
    # Formatar valores para melhor legibilidade
    product_metrics['avg_rating'] = product_metrics['avg_rating'].round(2)
    product_metrics['cancellation_rate'] = (product_metrics['cancellation_rate'] * 100).round(2)
    product_metrics['current_stock'] = product_metrics['current_stock'].round(0)
    product_metrics['stock_value'] = product_metrics['stock_value'].round(2)
    product_metrics['daily_demand'] = product_metrics['daily_demand'].round(2)
    product_metrics['adjusted_daily_demand'] = product_metrics['adjusted_daily_demand'].round(2)
    product_metrics['coefficient_variation'] = product_metrics['coefficient_variation'].round(2)
    product_metrics['safety_stock'] = product_metrics['safety_stock'].round(0)
    product_metrics['estoque_para_vendas_periodo'] = product_metrics['estoque_para_vendas_periodo'].round(0)
    product_metrics['stock_gap'] = product_metrics['stock_gap'].round(0)
    product_metrics['days_until_stockout'] = product_metrics['days_until_stockout'].round(1)
    product_metrics['avg_delivery_time_days'] = product_metrics['avg_delivery_time_days'].round(1)
    product_metrics['monthly_turnover'] = product_metrics['monthly_turnover'].round(2)
    
    return product_metrics[download_columns].sort_values('total_revenue', ascending=False)


def generate_best_skus_export(
    filtered_df: pd.DataFrame,
    composite_score_min: float = 0.850,
    include_all_skus: bool = False,
) -> pd.DataFrame:
    """
    Gera dataset dos melhores SKUs com composite_score, classificação BCG, estoque e Curva ABC Premium.

    Usado pelo painel Power BI para:
    - Lista dos melhores SKUs (composite_score > threshold, ex.: > 0.850)
    - Classificação BCG por SKU (Estrela, Vaca Leiteira, Interrogação, Abacaxi)
    - Informações de estoque (snapshot)
    - Curva ABC Premium (classificação A/B/C por receita acumulada, nos SKUs premium)

    Args:
        filtered_df: DataFrame de pedidos (orders) filtrado por período/loja.
        composite_score_min: Limiar mínimo de composite_score para "melhores SKUs" (default 0.850).
        include_all_skus: Se True, retorna todos os SKUs; se False, apenas composite_score >= composite_score_min.

    Returns:
        DataFrame com colunas: product_id, descricao, product_category_name, composite_score,
        bcg_quadrant, market_share_pct, growth_rate_pct, receita_total, receita_acumulada_pct,
        classificacao_abc, + colunas de estoque (quantidade_disponivel_venda, dias_cobertura, status_estoque, etc.).
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()

    available = filtered_df.columns.tolist()
    product_id_col = next(
        (c for c in ["product_id", "sku", "produto_id", "codigo_produto"] if c in available),
        "order_id",
    )
    if product_id_col not in available or "price" not in available:
        return pd.DataFrame()

    df = filtered_df.copy()
    df["order_purchase_timestamp"] = safe_to_datetime(df["order_purchase_timestamp"])
    df = df[df["order_purchase_timestamp"].notna()]
    df["_effective_price"] = np.where(df.get("pedido_cancelado", 0) == 1, 0.0, df.get("price", 0.0))

    # Agregação por SKU
    agg_map = {
        "_effective_price": "sum",
        "price": ["sum", "mean", "count"],
        "order_purchase_timestamp": "min",
    }
    if "review_score" in available:
        agg_map["review_score"] = ["mean", "count"]
    if "pedido_cancelado" in available:
        agg_map["pedido_cancelado"] = "mean"
    if "payment_value" in available:
        agg_map["payment_value"] = ["sum", "mean"]

    sku = (
        df.groupby([product_id_col, "product_category_name"])
        .agg(agg_map)
        .reset_index()
    )
    # Flatten multiindex columns
    new_cols = []
    for col in sku.columns:
        if isinstance(col, tuple):
            new_cols.append(f"{col[0]}_{col[1]}" if col[1] else col[0])
        else:
            new_cols.append(col)
    sku.columns = new_cols

    sku["product_id"] = sku[product_id_col]
    sku["product_category_name"] = sku["product_category_name"] if "product_category_name" in sku.columns else ""
    sku["total_revenue"] = pd.to_numeric(sku.get("_effective_price_sum", sku.get("price_sum", 0)), errors="coerce").fillna(0)
    sku["avg_price"] = pd.to_numeric(sku.get("price_mean", 0), errors="coerce").fillna(0)
    sku["total_orders"] = pd.to_numeric(sku.get("price_count", 0), errors="coerce").fillna(0)
    sku["avg_rating"] = pd.to_numeric(sku.get("review_score_mean", 0), errors="coerce").fillna(0)
    sku["total_reviews"] = pd.to_numeric(sku.get("review_score_count", 0), errors="coerce").fillna(0)
    sku["cancellation_rate"] = pd.to_numeric(sku.get("pedido_cancelado_mean", 0), errors="coerce").fillna(0)
    sku["total_payment"] = pd.to_numeric(sku.get("payment_value_sum", sku["total_revenue"]), errors="coerce").fillna(0)
    sku["avg_payment"] = pd.to_numeric(sku.get("payment_value_mean", sku["avg_price"]), errors="coerce").fillna(0)
    
    # Métricas derivadas
    sku["items_per_order"] = 1.0 # Proxy simples para SKU individual
    sku["review_rate"] = (sku["total_reviews"] / sku["total_orders"]).fillna(0)

    # Vendas nos últimos 30 dias (por SKU)
    try:
        max_date = df["order_purchase_timestamp"].max()
        cutoff_date = max_date - pd.Timedelta(days=30)
        vendas_30d = (
            df[df["order_purchase_timestamp"] > cutoff_date]
            .groupby(product_id_col)["order_id"]
            .nunique()
            .rename("vendas_30d")
        )
        sku = sku.merge(vendas_30d, on=product_id_col, how="left")
        sku["vendas_30d"] = sku["vendas_30d"].fillna(0).astype(int)
    except Exception:
        sku["vendas_30d"] = 0

    # Share e Growth (por SKU)
    total_rev_all = sku["total_revenue"].sum()
    sku["market_share_pct"] = (sku["total_revenue"] / total_rev_all * 100) if total_rev_all > 0 else 0
    sku["growth_rate_pct"] = 0.0
    try:
        # Growth simples: último mês vs mês anterior dentro do período disponível no DF.
        # Se não houver pelo menos 2 meses no recorte, mantém 0 (neutro).
        df["_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)
        monthly = (
            df.groupby([product_id_col, "_month"])["_effective_price"]
            .sum()
            .reset_index()
        )
        months = sorted([m for m in monthly["_month"].unique() if m])
        if len(months) >= 2:
            last_m, prev_m = months[-1], months[-2]
            last = (
                monthly[monthly["_month"] == last_m]
                .set_index(product_id_col)["_effective_price"]
            )
            prev = (
                monthly[monthly["_month"] == prev_m]
                .set_index(product_id_col)["_effective_price"]
            )
            growth = (last - prev) / prev.replace(0, np.nan) * 100.0
            growth = growth.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            # Cap conservador para não distorcer min-max em datasets pequenos
            growth = growth.clip(lower=-1000.0, upper=1000.0)
            sku = sku.merge(
                growth.rename("growth_rate_pct"),
                left_on=product_id_col,
                right_index=True,
                how="left",
            )
            sku["growth_rate_pct"] = sku["growth_rate_pct"].fillna(0.0)
    except Exception:
        sku["growth_rate_pct"] = sku.get("growth_rate_pct", 0.0)

    # ===================================================================
    # SCORE COMPOSTO REFINADO (11 MÉTRICAS - MIN-MAX)
    # ===================================================================
    column_mapping = {
        "total_revenue": "total_revenue",
        "total_orders": "units_sold",
        "avg_rating": "avg_rating",
        "cancellation_rate": "cancellation_rate",
        "market_share_pct": "market_share",
        "growth_rate_pct": "revenue_growth",
        "avg_price": "avg_price",
        "total_payment": "payment_value",
        "avg_payment": "avg_payment",
        "items_per_order": "items_per_order",
        "review_rate": "review_rate"
    }

    metrics_to_score = list(column_mapping.keys())
    sku["composite_score"] = 0.0
    
    # Normalizamos cada métrica e aplicamos os pesos do config.py
    total_w = sum(COMPOSITE_SCORE_WEIGHTS.get(w_key, 0) for w_key in column_mapping.values())
    
    for col in metrics_to_score:
        series = pd.to_numeric(sku.get(col, 0), errors="coerce").fillna(0)
        
        # Inverter cancelamento
        if col == "cancellation_rate":
            series = series.max() - series
        
        c_min, c_max = series.min(), series.max()
        if c_max > c_min:
            norm = (series - c_min) / (c_max - c_min)
        else:
            norm = 0.5
            
        weight = COMPOSITE_SCORE_WEIGHTS.get(column_mapping[col], 0) / total_w
        sku["composite_score"] += norm * weight

    # ===================================================================
    # SCORE COMPOSTO POR CATEGORIA (0-1 dentro de cada categoria, como no export Streamlit)
    # ===================================================================
    def _compute_composite_within_group(g: pd.DataFrame) -> pd.Series:
        out = pd.Series(0.0, index=g.index)
        for col in metrics_to_score:
            if col not in g.columns:
                continue
            series = pd.to_numeric(g[col], errors="coerce").fillna(0)
            if col == "cancellation_rate":
                series = series.max() - series
            c_min, c_max = series.min(), series.max()
            if c_max > c_min:
                norm = (series - c_min) / (c_max - c_min)
            else:
                norm = 0.5
            weight = COMPOSITE_SCORE_WEIGHTS.get(column_mapping[col], 0) / total_w
            out = out + norm * weight
        return out

    sku["composite_score_in_category"] = sku.groupby("product_category_name", group_keys=False).apply(_compute_composite_within_group)

    # ===================================================================
    # BCG HÍBRIDO E CURVA ABC
    # ===================================================================
    # 1. Quadrante da CATEGORIA (Contexto Estratégico)
    try:
        # Usar a análise principal de categorias para obter os quadrantes
        cat_analysis = analyze_category_performance(df)
        cat_metrics = cat_analysis.get('category_metrics', pd.DataFrame())
        
        if not cat_metrics.empty and "bcg_quadrant" in cat_metrics.columns:
            # Normalizar nomes para o map ser resiliente
            cat_bcg_map = dict(zip(
                cat_metrics["category"].astype(str).str.strip().str.upper(), 
                cat_metrics["bcg_quadrant"]
            ))
            sku["_cat_norm"] = sku["product_category_name"].astype(str).str.strip().str.upper()
            sku["bcg_quadrant_category"] = sku["_cat_norm"].map(cat_bcg_map).fillna("Indefinido")
            sku = sku.drop(columns=["_cat_norm"])
        else:
            sku["bcg_quadrant_category"] = "Indefinido"
    except Exception:
        sku["bcg_quadrant_category"] = "Indefinido"

    # 2. Quadrante do SKU (Performance Individual)
    # Usar mediano de valores maiores que zero para evitar que o threshold seja 0 em datasets esparsos
    ms_nonzero = sku[sku["market_share_pct"] > 0]["market_share_pct"]
    gr_nonzero = sku[sku["growth_rate_pct"] > 0]["growth_rate_pct"]
    
    ms_threshold = ms_nonzero.median() if not ms_nonzero.empty else 0.01
    gr_threshold = gr_nonzero.median() if not gr_nonzero.empty else 0.01
    score_threshold = BCG_CONFIG.get('composite_score_threshold', 0.2)

    def _bcg_sku(row):
        ms = row.get("market_share_pct", 0) or 0
        gr = row.get("growth_rate_pct", 0) or 0
        score = row.get("composite_score", 0) or 0
        
        # Abacaxi por baixo score (Critério de Performance Médica/Baixa)
        if score < score_threshold: return "Abacaxi"
        
        is_high_share = ms >= ms_threshold
        is_high_growth = gr >= gr_threshold
        
        if is_high_share and is_high_growth: return "Estrela Digital"
        if is_high_share and not is_high_growth: return "Vaca Leiteira"
        if not is_high_share and is_high_growth: return "Interrogação"
        return "Abacaxi"

    sku["bcg_quadrant"] = sku.apply(_bcg_sku, axis=1)
    
    # IMPORTANTE: Calcular Curva ABC e manter essa ordem para o gráfico acumulado
    sku = calculate_abc_by_performance(sku, total_rev_all)
    
    # Estoque: carregar snapshot e join
    sku["product_id_norm"] = sku["product_id"].astype(str).str.strip().str.upper()
    stock_df = None
    try:
        from utils.stock_loader import load_latest_stock  # type: ignore
        stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="parquet")
        if stock_df is None or (isinstance(stock_df, pd.DataFrame) and stock_df.empty):
            stock_df = load_latest_stock(stock_dir="data/stock_snapshot", file_format="csv")
    except Exception:
        pass

    # Preparar dados de estoque agregados
    stk_agg = pd.DataFrame()
    if isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
        stk = stock_df.copy()
        if "produto_id" in stk.columns and "product_id" not in stk.columns:
            stk["product_id"] = stk["produto_id"]
        stk["product_id_norm"] = stk["product_id"].astype(str).str.strip().str.upper()
        
        stock_cols = ["product_id_norm"]
        for c in ["descricao", "quantidade_disponivel_venda", "dias_cobertura", "status_estoque", "preco_medio_venda", "custo_unitario", "capital_imobilizado", "valor_potencial_venda", "margem_percentual", "vendas_90d", "giro_anual_projetado"]:
            if c in stk.columns:
                stock_cols.append(c)
        stk_agg = stk.groupby("product_id_norm").first().reset_index()[stock_cols]

    if include_all_skus and not stk_agg.empty:
        # Base = todos os SKUs do estoque; enriquecer com métricas de pedidos onde houver
        sku_order = sku.copy()
        sku = stk_agg.merge(
            sku_order,
            on="product_id_norm",
            how="left",
            suffixes=("", "_order"),
        )
        if "product_id_order" in sku.columns:
            sku["product_id"] = sku["product_id"].fillna(sku["product_id_order"])
            sku = sku.drop(columns=["product_id_order"], errors="ignore")
        sku["product_id"] = sku["product_id"].astype(str)
        for col in ["composite_score", "market_share_pct", "growth_rate_pct", "receita_acumulada_pct", "total_revenue", "avg_rating", "cancellation_rate", "total_orders"]:
            if col in sku.columns:
                sku[col] = pd.to_numeric(sku[col], errors="coerce").fillna(0)
        sku["composite_score"] = sku["composite_score"].fillna(0)
        sku["bcg_quadrant"] = sku["bcg_quadrant"].fillna("Sem vendas")
        sku["classificacao_abc"] = sku["classificacao_abc"].fillna("C")
        sku["product_category_name"] = sku["product_category_name"].fillna("").astype(str)
    elif not stk_agg.empty:
        # Base = Pedidos; enriquecer com dados de estoque
        sku = sku.merge(
            stk_agg,
            on="product_id_norm",
            how="left",
            suffixes=("", "_estoque"),
        )
        if "descricao" not in sku.columns and "descricao_estoque" in sku.columns:
            sku["descricao"] = sku["descricao_estoque"]
    else:
        # Placeholder se não houver estoque disponível
        for c in ["descricao", "quantidade_disponivel_venda", "dias_cobertura", "status_estoque", "capital_imobilizado", "valor_potencial_venda", "margem_percentual", "vendas_90d", "giro_anual_projetado"]:
            if c not in sku.columns:
                sku[c] = np.nan if c != "status_estoque" and c != "descricao" else ""

    # Colunas finais para PBI/Frontend
    out_cols = [
        "product_id",
        "descricao",
        "product_category_name",
        "composite_score",
        "composite_score_in_category",
        "bcg_quadrant",
        "bcg_quadrant_category",
        "market_share_pct",
        "growth_rate_pct",
        "total_revenue",
        "receita_acumulada_pct",
        "classificacao_abc",
        "total_orders",
        "avg_rating",
        "cancellation_rate",
        "quantidade_disponivel_venda",
        "dias_cobertura",
        "status_estoque",
        "preco_medio_venda",
        "capital_imobilizado",
        "valor_potencial_venda",
        "margem_percentual",
        "vendas_90d",
        "vendas_30d",
        "giro_anual_projetado",
    ]
    out_cols = [c for c in out_cols if c in sku.columns]
    result = sku[out_cols].copy()
    result = result.rename(columns={"total_revenue": "receita_total", "total_orders": "total_pedidos"})
    
    # Arredondamentos
    result["composite_score"] = pd.to_numeric(result["composite_score"], errors="coerce").fillna(0).round(3)
    result["composite_score_in_category"] = pd.to_numeric(result.get("composite_score_in_category", 0), errors="coerce").fillna(0).round(3)
    result["market_share_pct"] = pd.to_numeric(result["market_share_pct"], errors="coerce").fillna(0).round(2)
    result["growth_rate_pct"] = pd.to_numeric(result.get("growth_rate_pct", 0), errors="coerce").fillna(0).round(2)
    result["receita_acumulada_pct"] = pd.to_numeric(result.get("receita_acumulada_pct", 0), errors="coerce").fillna(0).round(2)
    
    # Arredondamentos Financeiros e Estoque
    for col in ["receita_total", "preco_medio_venda", "capital_imobilizado", "valor_potencial_venda", "margem_percentual"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).round(2)
    
    if "avg_rating" in result.columns:
        result["avg_rating"] = pd.to_numeric(result["avg_rating"], errors="coerce").fillna(0).round(1)

    if not include_all_skus:
        result = result[result["composite_score"] >= composite_score_min].copy()

    return result.reset_index(drop=True)


@st.cache_data(ttl=1800, show_spinner="Preparando export de Aquisição & Retenção...",
    hash_funcs={Path: _hash_path, pd.DataFrame: _hash_dataframe})
def generate_acquisition_retention_export(
    filtered_df: pd.DataFrame,
    marketing_spend: float = 0.0,
    inactivity_threshold_days: int = 90
) -> pd.DataFrame:
    """
    Gera tabela por cliente com métricas de aquisição e retenção.

    Colunas: customer_id, data_primeira_compra, marketplace_primeira_compra, 
    marketplace_principal, marketplaces_utilizados, nº_pedidos, dias_entre_compras,
    receita_total_cliente, CAC_estimado, LTV_estimado, churn_flag.
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()
    df = filtered_df.copy()
    df['order_purchase_timestamp'] = safe_to_datetime(df['order_purchase_timestamp'])
    df = df[df['order_purchase_timestamp'].notna()]
    # Receita somente de pedidos não cancelados
    df['effective_price'] = np.where(df.get('pedido_cancelado', 0) == 1, 0.0, df.get('price', 0.0))
    
    # Escolher ID interno de cliente quando existir; senão, fallback para customer_unique_id
    id_col = 'customer_id' if 'customer_id' in df.columns else 'customer_unique_id'
    df = df[df[id_col].notna()]

    # Métricas por cliente
    grouped = df.sort_values('order_purchase_timestamp').groupby(id_col)
    first_purchase = grouped['order_purchase_timestamp'].min().rename('first_purchase_date')
    total_orders = grouped['order_id'].nunique().rename('num_orders')
    total_revenue = grouped['effective_price'].sum().rename('total_revenue_customer')

    # Dias entre compras (média entre deltas)
    def _avg_days_between(series: pd.Series) -> float:
        dates = series.dropna().values
        if len(dates) < 2:
            return 0.0
        diffs = np.diff(dates) / np.timedelta64(1, 'D')
        return float(np.mean(diffs)) if len(diffs) > 0 else 0.0

    avg_days_between = grouped['order_purchase_timestamp'].apply(_avg_days_between).rename('days_between_purchases')
    last_purchase = grouped['order_purchase_timestamp'].max().rename('last_purchase_date')

    # Marketplace da primeira compra e marketplaces utilizados
    if 'marketplace' in df.columns:
        # Marketplace da primeira compra
        first_purchase_marketplace = df.loc[df.groupby(id_col)['order_purchase_timestamp'].idxmin()].set_index(id_col)['marketplace'].rename('marketplace_primeira_compra')
        
        # Lista de todos os marketplaces utilizados pelo cliente
        def _get_marketplaces_used(group):
            marketplaces = group['marketplace'].dropna().unique()
            return ', '.join(sorted(marketplaces)) if len(marketplaces) > 0 else 'N/A'
        
        marketplaces_used = grouped.apply(_get_marketplaces_used).rename('marketplaces_utilizados')
        
        # Marketplace mais utilizado pelo cliente
        def _get_most_used_marketplace(group):
            marketplace_counts = group['marketplace'].value_counts()
            return marketplace_counts.index[0] if len(marketplace_counts) > 0 else 'N/A'
        
        most_used_marketplace = grouped.apply(_get_most_used_marketplace).rename('marketplace_principal')
    else:
        first_purchase_marketplace = pd.Series(index=grouped.groups.keys(), data='N/A', name='marketplace_primeira_compra')
        marketplaces_used = pd.Series(index=grouped.groups.keys(), data='N/A', name='marketplaces_utilizados')
        most_used_marketplace = pd.Series(index=grouped.groups.keys(), data='N/A', name='marketplace_principal')

    export_df = pd.concat([
        first_purchase, last_purchase, total_orders, avg_days_between, total_revenue,
        first_purchase_marketplace, marketplaces_used, most_used_marketplace
    ], axis=1).reset_index()
    
    # Garantir que customer_id existe (pode vir como customer_unique_id ou index)
    if 'customer_id' not in export_df.columns and 'customer_unique_id' in export_df.columns:
        export_df = export_df.rename(columns={'customer_unique_id': 'customer_id'})
    if 'customer_id' not in export_df.columns:
        export_df['customer_id'] = export_df.index

    # CAC estimado: usar CAC global do período (constante por novo cliente)
    try:
        from utils.KPIs import calculate_acquisition_retention_kpis
        kpis = calculate_acquisition_retention_kpis(filtered_df, marketing_spend)
        cac_est = kpis.get('cac', 0.0)
    except Exception:
        cac_est = 0.0

    export_df['CAC_estimado'] = float(cac_est)
    # LTV estimado simples: receita total do cliente no período
    export_df['LTV_estimado'] = export_df['total_revenue_customer']

    # Churn flag: inativo se última compra há mais que threshold
    now_ref = df['order_purchase_timestamp'].max() if not df.empty else pd.Timestamp.utcnow()
    export_df['days_since_last'] = (now_ref - export_df['last_purchase_date']).dt.days
    export_df['churn_flag'] = np.where(export_df['days_since_last'] > inactivity_threshold_days, 'inativo', 'ativo')

    # Renomear e selecionar colunas finais
    export_df = export_df.rename(columns={
        'first_purchase_date': 'data_primeira_compra',
        'num_orders': 'nº_pedidos',
        'days_between_purchases': 'dias_entre_compras',
        'total_revenue_customer': 'receita_total_cliente'
    })
    # Adicionar flag de cliente multi-marketplace
    export_df['cliente_multi_marketplace'] = export_df['marketplaces_utilizados'].str.contains(',', na=False)
    export_df['qtd_marketplaces_utilizados'] = export_df['marketplaces_utilizados'].str.split(',').str.len()
    
    # Selecionar apenas as colunas que existem
    columns_to_export = [
        'customer_id', 'data_primeira_compra', 'marketplace_primeira_compra', 'marketplace_principal',
        'marketplaces_utilizados', 'cliente_multi_marketplace', 'qtd_marketplaces_utilizados',
        'nº_pedidos', 'dias_entre_compras', 'receita_total_cliente', 'CAC_estimado', 'LTV_estimado', 'churn_flag'
    ]
    existing_columns = [col for col in columns_to_export if col in export_df.columns]
    
    return export_df[existing_columns]

@st.cache_data(ttl=1800, show_spinner="Preparando export de Categorias & Portfólio...",
    hash_funcs={Path: _hash_path, pd.DataFrame: _hash_dataframe})
def generate_category_portfolio_export(
    filtered_df: pd.DataFrame, 
    full_df: pd.DataFrame, 
    date_range: List[str],
    estimated_margin_pct: float = 30.0
) -> pd.DataFrame:
    """
    Gera tabela por categoria com métricas de portfólio.
    - `margem_media` é calculada se `product_cost` existir, senão usa estimativa.
    - `crescimento_%` compara o período selecionado com o período anterior.
    - `giro_de_estoque` é um placeholder; `vendas_diarias_media` é calculado.
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()

    # --- Configuração Inicial ---
    df = filtered_df.copy()
    df['order_purchase_timestamp'] = safe_to_datetime(df['order_purchase_timestamp'])
    df = df[df['order_purchase_timestamp'].notna()]
    df['effective_price'] = np.where(df.get('pedido_cancelado', 0) == 1, 0.0, df.get('price', 0.0))

    # --- 1. Cálculo de Crescimento (Period-over-Period) ---
    if date_range:
        current_start, current_end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        period_duration = current_end - current_start
        previous_start, previous_end = current_start - period_duration, current_start

        previous_df = full_df[
            (safe_to_datetime(full_df['order_purchase_timestamp']) >= previous_start) &
            (safe_to_datetime(full_df['order_purchase_timestamp']) < previous_end)
        ].copy()
        previous_df['effective_price'] = np.where(previous_df.get('pedido_cancelado', 0) == 1, 0.0, previous_df.get('price', 0.0))

        current_revenue = df.groupby('product_category_name')['effective_price'].sum()
        previous_revenue = previous_df.groupby('product_category_name')['effective_price'].sum()
        
        growth_pct = ((current_revenue - previous_revenue) / previous_revenue).fillna(0) * 100
        growth_pct = growth_pct.rename('crescimento_%')
    else:
        # Se não houver range de data, o crescimento não pode ser calculado,
        # mas definimos o período com base nos dados filtrados.
        growth_pct = pd.Series(name='crescimento_%', dtype=float)
        if not df['order_purchase_timestamp'].empty:
            current_start = df['order_purchase_timestamp'].min()
            current_end = df['order_purchase_timestamp'].max()
        else:
            # Fallback se o DataFrame estiver vazio
            current_start, current_end = pd.Timestamp.now(), pd.Timestamp.now()

    # --- 2. Métricas Agregadas por Categoria ---
    total_revenue_all = df['effective_price'].sum()
    
    # Determinar coluna de produto para contagem de SKUs
    product_col = None
    if 'product_id' in df.columns:
        product_col = 'product_id'
    elif 'sku' in df.columns:
        product_col = 'sku'
    elif 'codigo' in df.columns:
        product_col = 'codigo'
    elif 'produto_id' in df.columns:
        product_col = 'produto_id'
    else:
        # Fallback: usar order_id se não houver coluna de produto
        product_col = 'order_id'
    
    # Garantir review_score opcional para evitar KeyError em datasets sem avaliações
    if "review_score" not in df.columns:
        df["review_score"] = np.nan
    
    agg_dict = {
        'n_skus': (product_col, 'nunique'),
        'receita_total': ('effective_price', 'sum'),
        'preco_medio': ('price', 'mean'),
        'pedidos': ('order_id', 'count'),
        'review_score_medio': ('review_score', 'mean')
    }
    
    # --- 3. Cálculo de Margem (Dinâmico com Fallback) ---
    has_cost_data = 'product_cost' in df.columns and df['product_cost'].notna().any()
    if has_cost_data:
        agg_dict['custo_total'] = ('product_cost', 'sum')

    agg = df.groupby('product_category_name').agg(**agg_dict).reset_index().rename(columns={'product_category_name': 'categoria'})
    
    agg['margem_calculada'] = has_cost_data
    if has_cost_data:
        agg['margem_media'] = ((agg['receita_total'] - agg['custo_total']) / agg['receita_total']).fillna(0) * 100
    else:
        agg['margem_media'] = estimated_margin_pct

    # --- 4. Cálculo de Vendas Diárias e Placeholder de Giro de Estoque ---
    days_in_period = (current_end - current_start).days + 1
    agg['vendas_diarias_media'] = agg['pedidos'] / days_in_period
    agg['giro_de_estoque'] = np.nan  # Placeholder

    # --- 5. Montagem Final ---
    agg.rename(columns={'product_category_name': 'categoria'}, inplace=True)
    
    # --- 3. Join e Cálculos Finais ---
    if not growth_pct.empty:
        # Renomear o índice para 'categoria' para garantir a junção
        growth_df = growth_pct.reset_index()
        growth_df.rename(columns={growth_df.columns[0]: 'categoria'}, inplace=True)
        agg = agg.merge(growth_df, on='categoria', how='left')
    else:
        # Se não houver dados de crescimento, adicionar coluna vazia
        agg['crescimento_%'] = np.nan

    # Se a coluna de crescimento não existir após a tentativa de merge (ex: categorias novas)
    if 'crescimento_%' not in agg.columns:
        agg['crescimento_%'] = np.nan

    participacao = np.where(
        total_revenue_all > 0,
        (agg['receita_total'] / total_revenue_all) * 100,
        np.nan,
    )
    # Converter para Series para usar fillna e alinhar pelo índice
    agg['participacao_no_faturamento'] = pd.Series(participacao, index=agg.index).fillna(0)

    # Seleção e ordenação de colunas
    final_cols = [
        'categoria', 'n_skus', 'receita_total', 'margem_media', 'margem_calculada',
        'preco_medio', 'crescimento_%', 'participacao_no_faturamento', 
        'vendas_diarias_media', 'giro_de_estoque', 'review_score_medio'
    ]
    
    # Adicionar colunas que podem estar faltando
    for col in final_cols:
        if col not in agg.columns:
            agg[col] = np.nan if col not in ['margem_calculada'] else False

    result = agg[final_cols].rename(columns={
        'n_skus': 'nº_SKUs',
        'preco_medio': 'preço_médio'
    })
    
    return result.sort_values('receita_total', ascending=False)

@st.cache_data(ttl=1800, show_spinner="Preparando export de Reviews & Sentimentos...",
    hash_funcs={Path: _hash_path, pd.DataFrame: _hash_dataframe})
def generate_reviews_sentiment_export(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera tabela de reviews com sentimento.

    Se texto de review não existir, preenche com vazio e classifica sentimento pelo score.
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()
    df = filtered_df.copy()
    # Campos potenciais de texto
    text_cols = [c for c in df.columns if c.lower() in {'review_comment_message', 'review_text', 'comment'}]
    review_text_col = text_cols[0] if text_cols else None

    out = pd.DataFrame()
    
    # Determinar coluna de produto com fallback
    product_col = None
    if 'product_id' in df.columns:
        product_col = 'product_id'
    elif 'sku' in df.columns:
        product_col = 'sku'
    elif 'codigo' in df.columns:
        product_col = 'codigo'
    elif 'produto_id' in df.columns:
        product_col = 'produto_id'
    else:
        # Fallback: usar order_id se não houver coluna de produto
        product_col = 'order_id'
    
    out['product_id'] = df[product_col] if product_col in df.columns else df['order_id']
    out['categoria'] = df.get('product_category_name')
    out['transportadora'] = df.get('transportadoraNome', df.get('carrier_name', ''))
    out['review_text'] = df[review_text_col] if review_text_col else ""
    out['review_score'] = df.get('review_score')

    # Sentimento baseado no score
    def _label_sentiment(score: Any) -> str:
        try:
            s = float(score)
            if s >= 4:
                return 'positivo'
            if s <= 2:
                return 'negativo'
            return 'neutro'
        except Exception:
            return 'neutro'

    out['sentimento'] = out['review_score'].apply(_label_sentiment)
    out['tópico'] = ''  # tópico LDA não disponível sem texto consolidado
    out['peso_sentimento_%'] = ''
    out['frequencia_palavras_chave'] = ''
    return out.dropna(subset=['product_id', 'review_score']).reset_index(drop=True)

@st.cache_data(ttl=1800, show_spinner="Preparando export de Campanhas & Canais...",
    hash_funcs={Path: _hash_path, pd.DataFrame: _hash_dataframe})
def generate_campaigns_performance_export(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera tabela de performance por canal se colunas de canal estiverem disponíveis.

    Requer qualquer coluna entre: ['channel', 'acquisition_channel', 'utm_source'].
    Caso contrário, retorna DataFrame vazio.
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()
    df = filtered_df.copy()
    channel_col = None
    for c in df.columns:
        if c.lower() in {'channel', 'acquisition_channel', 'utm_source'}:
            channel_col = c
            break
    if channel_col is None:
        return pd.DataFrame()

    df['effective_price'] = np.where(df.get('pedido_cancelado', 0) == 1, 0.0, df.get('price', 0.0))
    agg = df.groupby(channel_col).agg(
        custo_total=('freight_value', 'sum'),  # proxy se custo de mídia não existir
        receita_gerada=('effective_price', 'sum'),
        clientes_novos=('customer_unique_id', 'nunique'),
        pedidos=('order_id', 'nunique'),
        review_score_medio=('review_score', 'mean')
    ).reset_index().rename(columns={channel_col: 'canal'})
    # Sem dados reais de CAC/ROI por canal, definir placeholders
    agg['CAC'] = 0.0
    agg['ROI_%'] = np.where(agg['custo_total'] > 0, (agg['receita_gerada'] - agg['custo_total']) / agg['custo_total'] * 100, 0.0)
    agg['ticket_médio'] = np.where(agg['pedidos'] > 0, agg['receita_gerada'] / agg['pedidos'], 0.0)
    return agg[['canal', 'custo_total', 'receita_gerada', 'clientes_novos', 'CAC', 'ROI_%', 'ticket_médio']]

@st.cache_data(ttl=1800, show_spinner="Preparando export de Logística & Entregas...",
    hash_funcs={Path: _hash_path, pd.DataFrame: _hash_dataframe})
def generate_logistics_deliveries_export(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera tabela por pedido com métricas de entrega.

    Colunas: pedido_id, data_pedido, categoria, SLA_estimado(dias - mediana),
    tempo_real_entrega(dias), atraso_flag, custo_envio, satisfação_pós_entrega.
    """
    if filtered_df is None or filtered_df.empty:
        return pd.DataFrame()
    df = filtered_df.copy()
    df['order_purchase_timestamp'] = safe_to_datetime(df['order_purchase_timestamp'])
    if 'order_delivered_customer_date' not in df.columns:
        return pd.DataFrame()
    df['order_delivered_customer_date'] = safe_to_datetime(df['order_delivered_customer_date'])
    df = df[df['order_purchase_timestamp'].notna()]

    # Tempo real de entrega
    df['tempo_real_entrega'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    valid_delivery = df['tempo_real_entrega'].dropna()
    sla_est = float(valid_delivery.median()) if len(valid_delivery) > 0 else 12.0

    out = pd.DataFrame({
        'pedido_id': df.get('order_id'),
        'data_pedido': df['order_purchase_timestamp'],
        'categoria': df.get('product_category_name'),
        'transportadora': df.get('transportadoraNome', df.get('carrier_name')),
        'SLA_estimado_dias': sla_est,
        'tempo_real_entrega_dias': df['tempo_real_entrega'],
        'atraso_flag': np.where(df['tempo_real_entrega'] > sla_est, 1, 0),
        'custo_envio': df.get('freight_value', 0.0),
        'satisfacao_pos_entrega': df.get('review_score')
    })
    return out.dropna(subset=['pedido_id']).reset_index(drop=True)

def calculate_roi_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula insights de ROI baseados nos períodos definidos na narrativa.
    
    Args:
        df: DataFrame com dados de pedidos
        
    Returns:
        Dict com insights de ROI incluindo:
        - roi_metrics: Métricas principais de ROI
        - period_comparison: Comparação entre períodos
        - insights: Insights e recomendações
    """
    # Converter coluna de data se necessário
    if 'order_purchase_timestamp' not in df.columns:
        return {}
    
    df['order_date'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')
    
    # Definir períodos da narrativa de ROI
    baseline_start = pd.to_datetime("2023-01-01")
    baseline_end = pd.to_datetime("2023-06-30")
    improvement_start = pd.to_datetime("2023-07-01")
    improvement_end = pd.to_datetime("2023-12-31")
    growth_start = pd.to_datetime("2024-01-01")
    
    # Filtrar dados por período
    baseline_data = df[(df['order_date'] >= baseline_start) & (df['order_date'] <= baseline_end)]
    improvement_data = df[(df['order_date'] >= improvement_start) & (df['order_date'] <= improvement_end)]
    growth_data = df[df['order_date'] >= growth_start]
    
    # Calcular métricas por período
    def calculate_period_metrics(period_df, period_name):
        if period_df.empty:
            return {
                'revenue': 0,
                'orders': 0,
                'avg_ticket': 0,
                'cancel_rate': 0,
                'period_name': period_name
            }
        
        revenue = period_df['price'].sum() if 'price' in period_df.columns else 0
        orders = len(period_df)
        avg_ticket = revenue / orders if orders > 0 else 0
        
        # Calcular taxa de cancelamento
        if 'order_status' in period_df.columns:
            cancelled = period_df['order_status'].str.lower().isin(['cancelado', 'canceled', 'cancelled']).sum()
            cancel_rate = (cancelled / orders * 100) if orders > 0 else 0
        else:
            cancel_rate = 0
        
        return {
            'revenue': revenue,
            'orders': orders,
            'avg_ticket': avg_ticket,
            'cancel_rate': cancel_rate,
            'period_name': period_name
        }
    
    baseline_metrics = calculate_period_metrics(baseline_data, "Baseline (Jan-Jun 2023)")
    improvement_metrics = calculate_period_metrics(improvement_data, "Melhoria (Jul-Dez 2023)")
    growth_metrics = calculate_period_metrics(growth_data, "Crescimento (2024)")
    
    # Calcular ROI
    revenue_increase = improvement_metrics['revenue'] - baseline_metrics['revenue']
    revenue_growth_pct = (revenue_increase / baseline_metrics['revenue'] * 100) if baseline_metrics['revenue'] > 0 else 0
    
    ticket_increase = improvement_metrics['avg_ticket'] - baseline_metrics['avg_ticket']
    ticket_growth_pct = (ticket_increase / baseline_metrics['avg_ticket'] * 100) if baseline_metrics['avg_ticket'] > 0 else 0
    
    cancel_reduction = baseline_metrics['cancel_rate'] - improvement_metrics['cancel_rate']
    
    # Criar ícones SVG para os insights
    from utils.svg_icons import get_svg_icon
    
    return {
        'roi_metrics': {
            'revenue_increase': revenue_increase,
            'revenue_growth_pct': revenue_growth_pct,
            'ticket_increase': ticket_increase,
            'ticket_growth_pct': ticket_growth_pct,
            'cancel_reduction': cancel_reduction,
            'annual_projection': revenue_increase * 2  # 6 meses * 2 = 12 meses
        },
        'period_comparison': {
            'baseline': baseline_metrics,
            'improvement': improvement_metrics,
            'growth': growth_metrics
        },
        'insights': {
            'revenue_icon': get_svg_icon("trend", size=20, color="#4ECDC4"),
            'ticket_icon': get_svg_icon("trend", size=20, color="#4ECDC4"),
            'cancel_icon': get_svg_icon("minus-circle", size=20, color="#FF6B6B"),
            'projection_icon': get_svg_icon("performance", size=20, color="#45B7D1")
        }
    }


def calculate_roi_insights_case_atual(
    df: pd.DataFrame,
    min_date: str = "2025-07-01",
) -> Dict[str, Any]:
    """
    Calcula insights de ROI para o case atual (Dados Integrados).
    Períodos: Baseline (01/07–11/10/2025), Melhoria (12/10/2025–02/02/2026), Crescimento (03/02/2026+).
    Marcadores: 12/10 consultoria, 27/11 homologação, 27/01 produção, 03/02 decisões.
    """
    if "order_purchase_timestamp" not in df.columns:
        return _empty_roi_insights()

    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df = df.dropna(subset=["order_date"])
    min_dt = pd.to_datetime(min_date)
    _tz = getattr(df["order_date"].dtype, "tz", None)
    if _tz is not None:
        min_dt = min_dt.tz_localize("UTC")
    df = df[df["order_date"] >= min_dt]

    baseline_start = pd.to_datetime("2025-07-01")
    baseline_end = pd.to_datetime("2025-10-11")
    improvement_start = pd.to_datetime("2025-10-12")
    improvement_end = pd.to_datetime("2026-02-02")
    growth_start = pd.to_datetime("2026-02-03")
    if _tz is not None:
        baseline_start = baseline_start.tz_localize("UTC")
        baseline_end = baseline_end.tz_localize("UTC")
        improvement_start = improvement_start.tz_localize("UTC")
        improvement_end = improvement_end.tz_localize("UTC")
        growth_start = growth_start.tz_localize("UTC")

    baseline_data = df[(df["order_date"] >= baseline_start) & (df["order_date"] <= baseline_end)]
    improvement_data = df[(df["order_date"] >= improvement_start) & (df["order_date"] <= improvement_end)]
    growth_data = df[df["order_date"] >= growth_start]

    def _revenue_series(d: pd.DataFrame):
        if d.empty:
            return 0.0
        if "valorTotal" in d.columns and "order_id" in d.columns:
            by_order = d.groupby("order_id").agg(
                valorTotal=("valorTotal", "max"),
                pedido_cancelado=("pedido_cancelado", "max") if "pedido_cancelado" in d.columns else ("order_id", "size"),
            ).reset_index()
            if "pedido_cancelado" in by_order.columns:
                by_order = by_order[by_order["pedido_cancelado"] == 0]
            return pd.to_numeric(by_order["valorTotal"], errors="coerce").fillna(0).sum()
        return pd.to_numeric(d.get("price", 0), errors="coerce").fillna(0).sum()

    def calculate_period_metrics(period_df: pd.DataFrame, period_name: str) -> Dict[str, Any]:
        if period_df.empty:
            return {
                "revenue": 0.0,
                "orders": 0,
                "avg_ticket": 0.0,
                "cancel_rate": 0.0,
                "period_name": period_name,
            }
        revenue = _revenue_series(period_df)
        orders = int(period_df["order_id"].nunique()) if "order_id" in period_df.columns else len(period_df)
        avg_ticket = revenue / orders if orders > 0 else 0.0
        if "order_status" in period_df.columns:
            cancelled = period_df["order_status"].astype(str).str.lower().isin(["cancelado", "canceled", "cancelled"]).sum()
        elif "pedido_cancelado" in period_df.columns:
            cancelled = (period_df["pedido_cancelado"].fillna(0) != 0).sum()
        else:
            cancelled = 0
        cancel_rate = (cancelled / orders * 100) if orders > 0 else 0.0
        return {
            "revenue": float(revenue),
            "orders": int(orders),
            "avg_ticket": float(avg_ticket),
            "cancel_rate": float(cancel_rate),
            "period_name": period_name,
        }

    baseline_metrics = calculate_period_metrics(baseline_data, "Baseline (Jul–Out 2025)")
    improvement_metrics = calculate_period_metrics(improvement_data, "Melhoria (Out 2025 – Fev 2026)")
    growth_metrics = calculate_period_metrics(growth_data, "Crescimento (Pós-decisões)")

    revenue_increase = improvement_metrics["revenue"] - baseline_metrics["revenue"]
    revenue_growth_pct = (revenue_increase / baseline_metrics["revenue"] * 100) if baseline_metrics["revenue"] > 0 else 0.0
    ticket_increase = improvement_metrics["avg_ticket"] - baseline_metrics["avg_ticket"]
    ticket_growth_pct = (ticket_increase / baseline_metrics["avg_ticket"] * 100) if baseline_metrics["avg_ticket"] > 0 else 0.0
    cancel_reduction = baseline_metrics["cancel_rate"] - improvement_metrics["cancel_rate"]

    from utils.svg_icons import get_svg_icon
    return {
        "roi_metrics": {
            "revenue_increase": revenue_increase,
            "revenue_growth_pct": revenue_growth_pct,
            "ticket_increase": ticket_increase,
            "ticket_growth_pct": ticket_growth_pct,
            "cancel_reduction": cancel_reduction,
            "annual_projection": revenue_increase * 2,
        },
        "period_comparison": {
            "baseline": baseline_metrics,
            "improvement": improvement_metrics,
            "growth": growth_metrics,
        },
        "insights": {
            "revenue_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "ticket_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "cancel_icon": get_svg_icon("minus-circle", size=20, color="#FF6B6B"),
            "projection_icon": get_svg_icon("performance", size=20, color="#45B7D1"),
        },
    }


def calculate_roi_insights_pos_decisoes(df: pd.DataFrame) -> Dict[str, Any]:
    """
    KPIs de ROI pós decisões: compara os 31 dias anteriores (03/01–02/02) com o período
    pós decisões (03/02–06/03). O delta alimenta os cards e a projeção anual.
    """
    if "order_purchase_timestamp" not in df.columns:
        return _empty_roi_insights()

    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df = df.dropna(subset=["order_date"])
    _tz = getattr(df["order_date"].dtype, "tz", None)

    # 31 dias antes de 03/02: 03/01/2026 00:00 até 02/02/2026 (inclusive) → order_date < 2026-02-03
    antes_start = pd.to_datetime("2026-01-03")
    antes_end_excl = pd.to_datetime("2026-02-03")  # exclusivo: order_date < este valor
    # Pós decisões: 03/02/2026 até 06/03/2026 (inclusive) → order_date < 2026-03-07
    pos_start = pd.to_datetime("2026-02-03")
    pos_end_excl = pd.to_datetime("2026-03-07")  # exclusivo: order_date < este valor
    if _tz is not None:
        antes_start = antes_start.tz_localize("UTC")
        antes_end_excl = antes_end_excl.tz_localize("UTC")
        pos_start = pos_start.tz_localize("UTC")
        pos_end_excl = pos_end_excl.tz_localize("UTC")

    antes_data = df[(df["order_date"] >= antes_start) & (df["order_date"] < antes_end_excl)]
    pos_data = df[(df["order_date"] >= pos_start) & (df["order_date"] < pos_end_excl)]

    def _revenue_series(d: pd.DataFrame):
        if d.empty:
            return 0.0
        if "valorTotal" in d.columns and "order_id" in d.columns:
            by_order = d.groupby("order_id").agg(
                valorTotal=("valorTotal", "max"),
                pedido_cancelado=("pedido_cancelado", "max") if "pedido_cancelado" in d.columns else ("order_id", "size"),
            ).reset_index()
            if "pedido_cancelado" in by_order.columns:
                by_order = by_order[by_order["pedido_cancelado"] == 0]
            return pd.to_numeric(by_order["valorTotal"], errors="coerce").fillna(0).sum()
        return pd.to_numeric(d.get("price", 0), errors="coerce").fillna(0).sum()

    def _period_metrics(period_df: pd.DataFrame, period_name: str) -> Dict[str, Any]:
        if period_df.empty:
            return {"revenue": 0.0, "orders": 0, "avg_ticket": 0.0, "cancel_rate": 0.0, "period_name": period_name}
        revenue = _revenue_series(period_df)
        # Pedidos exibidos = apenas não cancelados (alinhado ao Resumo Executivo / Visão Geral)
        if "pedido_cancelado" in period_df.columns:
            eligible = period_df[period_df["pedido_cancelado"].fillna(0) == 0]
        else:
            eligible = period_df
        orders = int(eligible["order_id"].nunique()) if "order_id" in eligible.columns else len(eligible)
        avg_ticket = revenue / orders if orders > 0 else 0.0
        total_orders = int(period_df["order_id"].nunique()) if "order_id" in period_df.columns else len(period_df)
        if "order_status" in period_df.columns:
            cancelled_ids = period_df[period_df["order_status"].astype(str).str.lower().isin(["cancelado", "canceled", "cancelled"])]["order_id"].nunique() if "order_id" in period_df.columns else 0
        elif "pedido_cancelado" in period_df.columns:
            cancelled_ids = period_df[period_df["pedido_cancelado"].fillna(0) != 0]["order_id"].nunique() if "order_id" in period_df.columns else 0
        else:
            cancelled_ids = 0
        cancel_rate = (cancelled_ids / total_orders * 100) if total_orders > 0 else 0.0
        return {
            "revenue": float(revenue),
            "orders": int(orders),
            "avg_ticket": float(avg_ticket),
            "cancel_rate": float(cancel_rate),
            "period_name": period_name,
        }

    antes_metrics = _period_metrics(antes_data, "Jan (31 dias)")
    pos_metrics = _period_metrics(pos_data, "Pós decisões (03/02–06/03)")

    revenue_increase = pos_metrics["revenue"] - antes_metrics["revenue"]
    revenue_growth_pct = (revenue_increase / antes_metrics["revenue"] * 100) if antes_metrics["revenue"] > 0 else 0.0
    ticket_increase = pos_metrics["avg_ticket"] - antes_metrics["avg_ticket"]
    ticket_growth_pct = (ticket_increase / antes_metrics["avg_ticket"] * 100) if antes_metrics["avg_ticket"] > 0 else 0.0
    cancel_reduction = antes_metrics["cancel_rate"] - pos_metrics["cancel_rate"]

    from utils.svg_icons import get_svg_icon
    empty_growth = {"revenue": 0.0, "orders": 0, "avg_ticket": 0.0, "cancel_rate": 0.0, "period_name": ""}
    return {
        "roi_metrics": {
            "revenue_increase": revenue_increase,
            "revenue_growth_pct": revenue_growth_pct,
            "ticket_increase": ticket_increase,
            "ticket_growth_pct": ticket_growth_pct,
            "cancel_reduction": cancel_reduction,
            "annual_projection": revenue_increase * 2,
        },
        "period_comparison": {
            "baseline": antes_metrics,
            "improvement": pos_metrics,
            "growth": empty_growth,
        },
        "insights": {
            "revenue_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "ticket_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "cancel_icon": get_svg_icon("minus-circle", size=20, color="#FF6B6B"),
            "projection_icon": get_svg_icon("performance", size=20, color="#45B7D1"),
        },
    }


def _empty_roi_insights() -> Dict[str, Any]:
    from utils.svg_icons import get_svg_icon
    empty = {
        "revenue": 0.0,
        "orders": 0,
        "avg_ticket": 0.0,
        "cancel_rate": 0.0,
        "period_name": "—",
    }
    return {
        "roi_metrics": {
            "revenue_increase": 0.0,
            "revenue_growth_pct": 0.0,
            "ticket_increase": 0.0,
            "ticket_growth_pct": 0.0,
            "cancel_reduction": 0.0,
            "annual_projection": 0.0,
        },
        "period_comparison": {"baseline": empty, "improvement": empty, "growth": empty},
        "insights": {
            "revenue_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "ticket_icon": get_svg_icon("trend", size=20, color="#4ECDC4"),
            "cancel_icon": get_svg_icon("minus-circle", size=20, color="#FF6B6B"),
            "projection_icon": get_svg_icon("performance", size=20, color="#45B7D1"),
        },
    }


def render_roi_insights(roi_data: Dict[str, Any], pos_decisoes: bool = False) -> None:
    """
    Renderiza insights de ROI usando o padrão consistente com outras páginas.
    
    Args:
        roi_data: Dados de ROI calculados por calculate_roi_insights() ou calculate_roi_insights_pos_decisoes().
        pos_decisoes: Se True, usa rótulos "pós decisões" (31d antes vs 03/02–06/03).
    """
    from components.glass_card import render_insight_card
    
    rm = roi_data["roi_metrics"]
    rev_str = f"R$ {rm['revenue_increase']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    ticket_str = f"R$ {rm['ticket_increase']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    pct_str = f"+{rm['revenue_growth_pct']:.1f}%" if rm["revenue_growth_pct"] >= 0 else f"{rm['revenue_growth_pct']:.1f}%"
    ticket_pct_str = f"+{rm['ticket_growth_pct']:.1f}%" if rm["ticket_growth_pct"] >= 0 else f"{rm['ticket_growth_pct']:.1f}%"
    proj_str = f"R$ {rm['annual_projection']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    if pos_decisoes:
        title_rev = "Aumento de Receita (pós decisões)"
        desc_rev = "Jan (31 dias) vs 03/02–06/03"
        title_proj = "Projeção ROI Anual"
        desc_proj = "2× o delta do período (extrapolação)"
    else:
        title_rev = "Aumento de Receita (6 meses)"
        desc_rev = "Comparação com período anterior"
        title_proj = "Projeção ROI Anual"
        desc_proj = "Baseado em 6 meses"

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            render_insight_card(title_rev, rev_str, pct_str, roi_data["insights"]["revenue_icon"], desc_rev),
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            render_insight_card(
                "Aumento do Ticket Médio",
                ticket_str,
                ticket_pct_str,
                roi_data["insights"]["ticket_icon"],
                "Jan (31 dias) vs 03/02–06/03" if pos_decisoes else "Melhoria no valor médio por pedido",
            ),
            unsafe_allow_html=True,
        )
    with col3:
        help_cancel = "Jan (31 dias) vs 03/02–06/03" if pos_decisoes else "Redução em pontos percentuais"
        st.markdown(
            render_insight_card(
                "Redução de Cancelamentos",
                f"{rm['cancel_reduction']:.1f} pp",
                "Melhoria significativa",
                roi_data["insights"]["cancel_icon"],
                help_cancel,
            ),
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            render_insight_card(
                title_proj, proj_str,
                "2× o delta" if pos_decisoes else "Baseado em 6 meses",
                roi_data["insights"]["projection_icon"],
                desc_proj,
            ),
            unsafe_allow_html=True,
        )
def analyze_multi_marketplace_customers(filtered_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analisa clientes que utilizam múltiplos marketplaces.
    
    Returns:
        Dict com estatísticas e insights sobre clientes multi-marketplace
    """
    if filtered_df is None or filtered_df.empty or 'marketplace' not in filtered_df.columns:
        return {}
    
    df = filtered_df.copy()
    
    # Agrupar por cliente e contar marketplaces únicos
    customer_marketplaces = df.groupby('customer_unique_id').agg({
        'marketplace': lambda x: x.dropna().unique(),
        'order_id': 'nunique',
        'price': 'sum',
        'order_purchase_timestamp': ['min', 'max']
    }).reset_index()
    
    # Flatten column names
    customer_marketplaces.columns = [
        'customer_id', 'marketplaces', 'total_orders', 'total_revenue', 'first_purchase', 'last_purchase'
    ]
    
    # Calcular métricas
    customer_marketplaces['num_marketplaces'] = customer_marketplaces['marketplaces'].apply(len)
    customer_marketplaces['is_multi_marketplace'] = customer_marketplaces['num_marketplaces'] > 1
    customer_marketplaces['marketplaces_list'] = customer_marketplaces['marketplaces'].apply(
        lambda x: ', '.join(sorted(x)) if len(x) > 0 else 'N/A'
    )
    
    # Estatísticas gerais
    total_customers = len(customer_marketplaces)
    multi_marketplace_customers = customer_marketplaces['is_multi_marketplace'].sum()
    multi_marketplace_pct = (multi_marketplace_customers / total_customers * 100) if total_customers > 0 else 0
    
    # Comparação de métricas: multi vs single marketplace
    multi_customers = customer_marketplaces[customer_marketplaces['is_multi_marketplace']]
    single_customers = customer_marketplaces[~customer_marketplaces['is_multi_marketplace']]
    
    # Métricas comparativas
    comparison = {
        'multi_marketplace': {
            'count': len(multi_customers),
            'avg_orders': multi_customers['total_orders'].mean() if len(multi_customers) > 0 else 0,
            'avg_revenue': multi_customers['total_revenue'].mean() if len(multi_customers) > 0 else 0,
            'total_revenue': multi_customers['total_revenue'].sum() if len(multi_customers) > 0 else 0
        },
        'single_marketplace': {
            'count': len(single_customers),
            'avg_orders': single_customers['total_orders'].mean() if len(single_customers) > 0 else 0,
            'avg_revenue': single_customers['total_revenue'].mean() if len(single_customers) > 0 else 0,
            'total_revenue': single_customers['total_revenue'].sum() if len(single_customers) > 0 else 0
        }
    }
    
    # Distribuição por número de marketplaces
    marketplace_count_dist = customer_marketplaces['num_marketplaces'].value_counts().sort_index()
    
    # Combinações mais comuns de marketplaces
    multi_combinations = multi_customers['marketplaces_list'].value_counts().head(10)
    
    # Análise de migração (primeira vs principal)
    migration_analysis = {}
    if len(multi_customers) > 0:
        # Para cada cliente multi-marketplace, verificar se mudou de preferência
        for _, customer in multi_customers.iterrows():
            customer_orders = df[df['customer_unique_id'] == customer['customer_id']].sort_values('order_purchase_timestamp')
            if len(customer_orders) > 1:
                first_marketplace = customer_orders.iloc[0]['marketplace']
                last_marketplace = customer_orders.iloc[-1]['marketplace']
                
                if first_marketplace != last_marketplace:
                    migration_key = f"{first_marketplace} → {last_marketplace}"
                    migration_analysis[migration_key] = migration_analysis.get(migration_key, 0) + 1
    
    return {
        'total_customers': total_customers,
        'multi_marketplace_customers': multi_marketplace_customers,
        'multi_marketplace_percentage': multi_marketplace_pct,
        'comparison': comparison,
        'marketplace_count_distribution': dict(marketplace_count_dist),
        'top_combinations': dict(multi_combinations),
        'migration_patterns': migration_analysis,
        'customer_details': customer_marketplaces.to_dict('records')
    }
