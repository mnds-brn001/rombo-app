"""
Reviews Loader - Helper para carregar reviews (local ou Supabase)
=================================================================
"""

import os
import pandas as pd
from typing import Optional, Tuple
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def _get_secret_or_env(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        if key in st.secrets:
            v = st.secrets.get(key)
            return None if v is None else str(v)
    except Exception:
        pass
    v = os.getenv(key, default)
    return None if v is None else str(v)

@st.cache_data(ttl=3600)
def load_reviews(source_type: str = "aggregated") -> pd.DataFrame:
    """
    Carrega reviews.
    Args:
        source_type: 'aggregated', 'shopee', 'site', 'mercadolivre'
    """
    use_supabase = (_get_secret_or_env("USE_SUPABASE", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    
    if use_supabase:
        try:
            return load_reviews_supabase(source_type)
        except Exception as e:
            logger.error(f"Erro ao carregar reviews do Supabase: {e}")
            # Fallback para local
            pass
            
    # Local loading logic (existing paths)
    paths = {
        "aggregated": "data/processed/product_reviews_aggregated.parquet",
        "shopee": "data/processed/shopee_reviews_integrated.parquet",
        "site": "data/processed/product_reviews_individual.parquet",
        # ML não tem parquet consolidado local ainda, mas se tiver seria aqui
    }
    
    path = paths.get(source_type)
    if path and os.path.exists(path):
        return pd.read_parquet(path)
        
    return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_reviews_supabase(source_type: str) -> pd.DataFrame:
    import psycopg2
    db_url = _get_secret_or_env("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not set")
        
    tables = {
        "shopee": "reviews_shopee",
        "site": "reviews_site",
        "mercadolivre": "reviews_mercadolivre"
    }
    
    if source_type == "aggregated":
        # UNION ALL das 3 tabelas
        query = """
            SELECT
                CAST(review_id AS TEXT) as review_id,
                review_date,
                review_score,
                review_comment_message,
                product_name,
                product_category_name,
                'shopee' as review_source
            FROM public.reviews_shopee
            
            UNION ALL
            
            SELECT
                CAST(review_id AS TEXT) as review_id,
                review_date,
                review_score,
                review_comment_message,
                product_name,
                NULL as product_category_name, -- Site geralmente não tem essa coluna normalizada aqui
                'site' as review_source
            FROM public.reviews_site
            
            UNION ALL
            
            SELECT
                review_id,
                review_date,
                rating as review_score,
                comment as review_comment_message,
                product_name,
                product_category as product_category_name,
                'mercadolivre' as review_source
            FROM public.reviews_mercadolivre
        """
    elif source_type == "mercadolivre":
        # Mapeamento explícito de colunas para ML
        query = """
            SELECT 
                review_id,
                review_date,
                rating as review_score,
                comment as review_comment_message,
                product_name,
                product_category as product_category_name,
                product_sku,
                likes,
                dislikes,
                'mercadolivre' as review_source
            FROM public.reviews_mercadolivre
        """
    else:
        # Whitelist de tabelas permitidas (segurança: prevenir SQL injection)
        ALLOWED_TABLES = {
            "shopee": "reviews_shopee",
            "site": "reviews_site",
            "mercadolivre": "reviews_mercadolivre"
        }
        table = ALLOWED_TABLES.get(source_type)
        if not table:
            return pd.DataFrame()
        # Seguro: table vem de whitelist, não de input do usuário
        query = f"SELECT * FROM public.{table}"  # nosec B608
    
    with psycopg2.connect(db_url) as conn:
        df = pd.read_sql_query(query, conn)
        
    # Garantir datas
    if "review_date" in df.columns:
        df["review_date"] = pd.to_datetime(df["review_date"], utc=True).dt.tz_localize(None)
    
    # Mapear review_source para marketplace para compatibilidade com filtros
    if "review_source" in df.columns and "marketplace" not in df.columns:
        mapping = {
            "shopee": "Shopee",
            "site": "Site Próprio",
            "mercadolivre": "Mercado Livre"
        }
        df["marketplace"] = df["review_source"].map(mapping).fillna(df["review_source"])
        
    return df


def load_reviews_with_origin(source_type: str = "aggregated") -> Tuple[pd.DataFrame, str]:
    """
    Helper para a UI saber de onde veio (supabase vs local).
    """
    use_supabase = (_get_secret_or_env("USE_SUPABASE", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    if use_supabase:
        try:
            df = load_reviews_supabase(source_type)
            return df, "supabase"
        except Exception:
            pass
    # local fallback
    paths = {
        "aggregated": "data/processed/product_reviews_aggregated.parquet",
        "shopee": "data/processed/shopee_reviews_integrated.parquet",
        "site": "data/processed/product_reviews_individual.parquet",
    }
    path = paths.get(source_type)
    if path and os.path.exists(path):
        return pd.read_parquet(path), "local"
    return pd.DataFrame(), "missing"
