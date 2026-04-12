"""
Stock Loader - Helper para carregar snapshot de estoque nas análises
===================================================================

Funções utilitárias para carregar e integrar dados de estoque
(gerados pelo snapshot) nas análises do dashboard.

Uso típico:
    from utils.stock_loader import load_latest_stock, merge_stock_with_sales
    
    df_stock = load_latest_stock()
    df_enriched = merge_stock_with_sales(df_sales, df_stock)
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict
import logging
import os
import streamlit as st

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

def _keep_only_latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas o snapshot mais recente quando a coluna `snapshot_date` existir.
    Isso evita inflar estoque ao carregar arquivos históricos completos (várias datas).
    """
    if not isinstance(df, pd.DataFrame) or df.empty or "snapshot_date" not in df.columns:
        return df
    try:
        tmp = df.copy()
        tmp["snapshot_date"] = pd.to_datetime(tmp["snapshot_date"], errors="coerce")
        max_snapshot = tmp["snapshot_date"].max()
        if pd.notna(max_snapshot):
            tmp = tmp[tmp["snapshot_date"] == max_snapshot].copy()
            logger.info(f"✓ Snapshot filtrado para data mais recente: {max_snapshot.date()} ({len(tmp)} linhas)")
        return tmp
    except Exception as e:
        logger.warning(f"Falha ao filtrar snapshot_date para última data: {e}")
        return df


@st.cache_data(ttl=3600)
def load_latest_stock(
    stock_dir: str = "data/stock_snapshot",
    file_format: str = "parquet"
) -> Optional[pd.DataFrame]:
    """
    Carrega o snapshot mais recente de estoque.
    Suporta Local (Parquet/CSV) e Supabase (stock_snapshot_enriched).
    
    Args:
        stock_dir: Diretório onde estão os snapshots (para modo local)
        file_format: 'parquet' (rápido) ou 'csv' (compatível)
        
    Returns:
        DataFrame com estoque ou None se não encontrado
    """
    
    # 1. Tentar Supabase se habilitado
    use_supabase = (_get_secret_or_env("USE_SUPABASE", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    
    if use_supabase:
        try:
            logger.info("Tentando carregar estoque do Supabase...")
            df_supa = _load_stock_supabase()
            if df_supa is not None and not df_supa.empty:
                logger.info(f"✓ Estoque carregado do Supabase: {len(df_supa)} SKUs")
                return df_supa
        except Exception as e:
            logger.error(f"Erro ao carregar estoque do Supabase: {e}")
            # Fallback para local
    
    # 2. Path explícito via ENV (prioritário no modo local)
    stock_env_path = _get_secret_or_env("INSIGHTX_STOCK_PATH")
    if stock_env_path:
        explicit = Path(stock_env_path)
        if explicit.exists() and explicit.is_file():
            try:
                if explicit.suffix.lower() == ".parquet":
                    df = pd.read_parquet(explicit)
                else:
                    df = pd.read_csv(explicit, encoding="utf-8-sig")
                df = _keep_only_latest_snapshot(df)
                logger.info(f"✓ Estoque carregado via INSIGHTX_STOCK_PATH: {len(df)} produtos ({explicit.name})")
                return df
            except Exception as e:
                logger.error(f"Erro ao carregar INSIGHTX_STOCK_PATH={explicit}: {e}")

    # 3. Modo Local (Existente)
    stock_path = Path(stock_dir)
    
    if not stock_path.exists():
        logger.warning(f"Diretório de estoque não encontrado: {stock_path}")
        return None
    
    # Tentar carregar versão "latest"
    if file_format == "parquet":
        latest_file = stock_path / "estoque_snapshot_latest.parquet"
        if latest_file.exists():
            try:
                df = pd.read_parquet(latest_file)
                df = _keep_only_latest_snapshot(df)
                logger.info(f"✓ Estoque carregado: {len(df)} produtos ({latest_file.name})")
                return df
            except Exception as e:
                logger.error(f"Erro ao carregar {latest_file}: {e}")
    else:
        latest_file = stock_path / "estoque_snapshot_latest.csv"
        if latest_file.exists():
            try:
                df = pd.read_csv(latest_file, encoding='utf-8-sig')
                df = _keep_only_latest_snapshot(df)
                logger.info(f"✓ Estoque carregado: {len(df)} produtos ({latest_file.name})")
                return df
            except Exception as e:
                logger.error(f"Erro ao carregar {latest_file}: {e}")
    
    # Fallback: buscar arquivo mais recente por timestamp
    pattern = f"estoque_snapshot_*.{file_format}"
    files = list(stock_path.glob(pattern))
    
    if not files:
        logger.warning(f"Nenhum arquivo de estoque encontrado em {stock_path}")
        return None
    
    # Pegar o mais recente (por nome, que inclui timestamp)
    latest_file = max(files, key=lambda p: p.name)
    
    try:
        if file_format == "parquet":
            df = pd.read_parquet(latest_file)
        else:
            df = pd.read_csv(latest_file, encoding='utf-8-sig')
        df = _keep_only_latest_snapshot(df)
        
        logger.info(f"✓ Estoque carregado (fallback): {len(df)} produtos ({latest_file.name})")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar estoque: {e}")
        return None


def _load_stock_supabase() -> Optional[pd.DataFrame]:
    """Helper para carregar o snapshot mais recente do Supabase"""
    import psycopg2
    db_url = _get_secret_or_env("SUPABASE_DB_URL")
    if not db_url:
        return None
        
    # Consulta otimizada: Pega todas as linhas onde snapshot_date é o máximo
    query = """
        SELECT *
        FROM public.stock_snapshot_enriched
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date) FROM public.stock_snapshot_enriched
        )
    """
    
    with psycopg2.connect(db_url) as conn:
        df = pd.read_sql_query(query, conn)

    # Schema base usa apenas public.stock_snapshot_enriched (sem fallback para stock_snapshot).
    
    # Normalização de tipos
    if "snapshot_date" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    if "data_hora_atualizacao" in df.columns:
        df["data_hora_atualizacao"] = pd.to_datetime(df["data_hora_atualizacao"], utc=True).dt.tz_localize(None)

    return df


def merge_stock_with_sales(
    df_sales: pd.DataFrame,
    df_stock: pd.DataFrame,
    on_col: str = "product_id",
    how: str = "left",
    stock_cols: Optional[list] = None
) -> pd.DataFrame:
    """
    Merge dados de vendas com estoque atual.
    
    Args:
        df_sales: DataFrame de vendas/pedidos
        df_stock: DataFrame de estoque (do snapshot)
        on_col: Coluna para join (geralmente 'product_id' ou 'product_sku')
        how: Tipo de join ('left', 'inner')
        stock_cols: Colunas de estoque a incluir (default: todas relevantes)
        
    Returns:
        DataFrame enriquecido com dados de estoque
    """
    if df_stock is None or df_stock.empty:
        logger.warning("DataFrame de estoque vazio ou nulo. Retornando vendas original.")
        return df_sales
    
    # Garantir que as chaves sejam do mesmo tipo (str)
    df_sales = df_sales.copy()
    df_stock = df_stock.copy()
    
    if on_col not in df_sales.columns:
        # Tentar mapear se nome for diferente (ex: produto_id -> product_id)
        if on_col == "product_id" and "produto_id" in df_sales.columns:
            df_sales["product_id"] = df_sales["produto_id"]
        elif on_col == "product_sku" and "sku" in df_sales.columns:
            df_sales["product_sku"] = df_sales["sku"]
        else:
            logger.warning(f"Coluna chave {on_col} não encontrada em vendas")
            return df_sales

    # Normalizar chaves para string
    df_sales[on_col] = df_sales[on_col].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Se estoque tem 'produto_id' mas precisamos de 'product_id'
    if on_col == "product_id" and "product_id" not in df_stock.columns and "produto_id" in df_stock.columns:
        df_stock["product_id"] = df_stock["produto_id"]
        
    if on_col in df_stock.columns:
        df_stock[on_col] = df_stock[on_col].astype(str).str.replace(r'\.0$', '', regex=True)
    else:
        logger.warning(f"Coluna chave {on_col} não encontrada em estoque")
        return df_sales

    # Selecionar colunas
    if stock_cols is None:
        # Default: colunas úteis para análise
        stock_cols = [
            c for c in [
                "product_id", "product_sku", "product_name", 
                "quantidade_disponivel_venda", "quantidade_fisica", 
                "custo_medio", "status_estoque", "dias_cobertura",
                "giro_anual_projetado", "snapshot_date"
            ] if c in df_stock.columns
        ]
        # Garantir que a chave esteja presente (mas não duplicada no merge se não for o índice)
        if on_col not in stock_cols:
             stock_cols.append(on_col)
    
    # Remover duplicatas no estoque (um registro por produto no snapshot)
    df_stock_unique = df_stock.drop_duplicates(subset=[on_col], keep='first')
    
    # Merge
    merged = df_sales.merge(
        df_stock_unique[stock_cols],
        on=on_col,
        how=how,
        suffixes=("", "_stock")
    )
    
    return merged
