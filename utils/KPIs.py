import re
from pathlib import Path
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import numpy as np
from typing import Any, Dict, Tuple, Optional, List
import hashlib
from datetime import datetime, timedelta
import os

# ---------------------------------------------------------
#  Função Auxiliar de Mapeamento de Funil (Centralizada)
# ---------------------------------------------------------
def _map_order_status_to_funnel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia status de pedidos para etapas de funil padronizadas.
    Adaptado do adaptador_cosmeticos.py para uso global.
    """
    if 'order_status' not in df.columns:
        return df
        
    # Normalização de status
    def normalize_status(status_value):
        if pd.isna(status_value): return ''
        s = str(status_value).lower().strip()
        s = re.sub(r'^\d+\s*-\s*', '', s) # Remove "7 - " prefix
        return (s.replace('ã', 'a').replace('á', 'a').replace('â', 'a')
                 .replace('ç', 'c').replace('é', 'e').replace('ê', 'e')
                 .replace('í', 'i').replace('ó', 'o').replace('ô', 'o')
                 .replace('ú', 'u'))

    status_normalized = df['order_status'].apply(normalize_status)
    
    # Padrões de status (Português/Inglês)
    awaiting = ['aguardando', 'pendente', 'waiting', 'pending', 'analise']
    paid = ['aprovado', 'pago', 'paid', 'confirmado', 'confirmed', 'integrado']
    invoice = ['nota fiscal', 'nf emitida', 'invoice', 'fatura', 'faturado']
    transit = ['transporte', 'enviado', 'shipped', 'postado', 'envio', 'transit']
    delivered = ['entregue', 'delivered', 'entreg']
    cancelled = ['cancelado', 'cancel', 'estornado', 'devolvido', 'reembolso', 'fraude', 'chargeback']
    problem = ['problema', 'erro', 'falha', 'issue', 'logistica reversa', 'suspenso']
    exchange = ['troca', 'exchange']

    # Criar colunas de funil
    # Etapas cumulativas
    df['funnel_awaiting_payment'] = status_normalized.apply(lambda x: any(p in x for p in awaiting)).astype(int)
    
    # Pago inclui: pago, faturado, enviado, entregue
    df['funnel_paid'] = status_normalized.apply(
        lambda x: any(p in x for p in paid + invoice + transit + delivered)
    ).astype(int)
    
    # NF inclui: faturado, enviado, entregue
    df['funnel_invoice_issued'] = status_normalized.apply(
        lambda x: any(p in x for p in invoice + transit + delivered)
    ).astype(int)
    
    # Transporte inclui: enviado, entregue
    df['funnel_in_transit'] = status_normalized.apply(
        lambda x: any(p in x for p in transit + delivered)
    ).astype(int)
    
    # Entregue
    df['funnel_delivered'] = status_normalized.apply(
        lambda x: any(p in x for p in delivered)
    ).astype(int)
    
    # Exceções (não cumulativas)
    df['funnel_cancelled'] = status_normalized.apply(lambda x: any(p in x for p in cancelled)).astype(int)
    df['funnel_problem'] = status_normalized.apply(lambda x: any(p in x for p in problem)).astype(int)
    df['funnel_exchange'] = status_normalized.apply(lambda x: any(p in x for p in exchange)).astype(int)

    # Aliases de compatibilidade
    df['funnel_approved'] = df['funnel_paid']
    df['funnel_shipped'] = df['funnel_in_transit']
    # Alias esperado pelo Power BI (Visão Geral / VisaoGeral_RAW)
    df['funnel_shipped_dup'] = df['funnel_shipped']

    # Flag de cancelado global (se não existir)
    if 'pedido_cancelado' not in df.columns:
        df['pedido_cancelado'] = df['funnel_cancelled']
    else:
        # Se já existe, atualizar com a lógica robusta se for 0
        df['pedido_cancelado'] = df['pedido_cancelado'] | df['funnel_cancelled']

    return df

# ---------------------------------------------------------
#  Colunas realmente usadas em KPIs, páginas e gráficos
# ---------------------------------------------------------
# Utilizaremos este subconjunto para carregar o Parquet mais rápido
# e com menor uso de memória. Se alguma nova coluna passar a ser
# necessária, basta adicioná-la aqui.

REQUIRED_COLUMNS: list[str] = [
    # Identificadores
    "order_id",
    "customer_id",
    "customer_unique_id",
    "product_id",
    # Datas
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "marketplace_date",
    # "approval_date", # Removido (default)
    # Categorias / Produto
    "product_category_name",
    "category_name",  # Alias comum em integrações Magazord/VTEX
    # Cliente (localização)
    "customer_state",
    # Transportadora
    "transportadoraNome",
    # Origem
    "marketplace",
    # Monetário
    "price",
    "freight_value",
    "valorTotal",
    # "product_cost", # Removido (default)
    # Status / Flags
    "order_status",
    "pedido_cancelado",
    # "carrinho_abandonado", # Removido (default)
    # Métricas
    # "review_score", # Removido (default)
    # "review_comment_message", # Removido (default)
    # Funil de conversão - Etapas principais (Removidos pois não existem no dataset de pedidos)
    # "visitors",
    # "product_views",
    # "product_view",
    # "add_to_cart",
    # "checkout",
    # Funil de conversão - Etapas específicas para cosméticos
    # "newsletter_signup",
    # "wishlist_add", 
    # "sample_request",
    # Inventário
    # "stock_level", # Removido (default)
]

# ---------------------------------------------------------
#  Mapas de valores default para colunas obrigatórias
# ---------------------------------------------------------

REQUIRED_DEFAULTS: dict[str, Any] = {
    # Identificadores / textos
    "order_id": pd.Series(dtype=object),
    "customer_id": pd.Series(dtype=object),
    "customer_unique_id": pd.Series(dtype=object),
    "product_id": pd.Series(dtype=object),
    "product_category_name": "",
    "order_status": "",
    "customer_state": "Desconhecido",
    "marketplace": "Desconhecido",
    # Datas e métricas – NaN por padrão
    "approval_date": pd.NaT,
    "review_score": np.nan,
    "review_comment_message": "",
    "order_delivered_customer_date": pd.NaT,
    # Numéricos
    "price": 0.0,
    "freight_value": 0.0,
    "valorTotal": 0.0,
    "product_cost": 0.0,
    "pedido_cancelado": 0,
    "carrinho_abandonado": 0,
    "visitors": 0,
    "product_views": 0,
    "product_view": 0,
    "add_to_cart": 0,
    "checkout": 0,
    "newsletter_signup": 0,
    "wishlist_add": 0,
    "sample_request": 0,
    # Avaliações / reviews
    "review_creation_date": pd.NaT,
    "avg_review_score": np.nan,
    "review_count": 0,
}

DATA_CANDIDATES = [
    Path("data/processed/pedidos"), # Nova estrutura Data Lakehouse (particionada)
    Path("data/processed/pedidos.parquet"), # Backup/Legacy do pipeline novo
    Path("dados_consolidados/cliente_merged.parquet"),  # dataset cliente
    Path("dados_consolidados_teste/olist_merged_data.parquet"),  # dataset demo (Olist)
    Path("dados_consolidados/ga_data.parquet"),  # dataset Google Analytics (novo)
    Path("dados_consolidados/api_data.parquet"),  # dataset API (novo)
]

def _hash_path(path: Path) -> str:
    """Função customizada para hashear objetos Path."""
    return str(path.absolute())

def _hash_dataframe(df: pd.DataFrame) -> str:
    """Função customizada para hashear DataFrames baseada no conteúdo."""
    # Hash do DataFrame completo para cache mais preciso
    return hashlib.md5(pd.util.hash_pandas_object(df).values.tobytes()).hexdigest()

def _hash_path_with_mtime(path: Path) -> str:
    """Função customizada para hashear paths incluindo data de modificação."""
    try:
        # Se for diretório, pega o mtime mais recente dos arquivos internos
        if path.is_dir():
            mtimes = [p.stat().st_mtime for p in path.glob("**/*") if p.is_file()]
            if mtimes:
                return f"{path.absolute()}_{max(mtimes)}"
            return str(path.absolute())
        
        # Incluir data de modificação no hash para invalidar cache quando arquivo mudar
        mtime = path.stat().st_mtime
        return f"{path.absolute()}_{mtime}"
    except (OSError, FileNotFoundError):
        return str(path.absolute())

def _safe_is_dir(path: Path) -> bool:
    """Versão segura de is_dir que tolera mocks sem st_mode."""
    try:
        return os.path.isdir(str(path))
    except Exception:
        try:
            return path.is_dir()
        except Exception:
            return False

@st.cache_data(
    hash_funcs={
        Path: _hash_path_with_mtime,  # Usar hash com data de modificação
        pd.DataFrame: _hash_dataframe
    },
    ttl=7200,  # Cache por 2 horas
    max_entries=5  # Limitar número de entradas no cache
)
def load_data(use_required_subset: bool = True, custom_path: Optional[str | Path] = None, days: Optional[int] = None) -> pd.DataFrame:
    """Carrega o arquivo Parquet consolidado ou particionado.

    Ordem de procura:
    1. `custom_path` (se fornecido)
    2. data/processed/pedidos (Particionado)
    3. data/processed/pedidos.parquet (Arquivo único)
    4. dados_consolidados/cliente_merged.parquet (Legado)
    ...
    """
    import time
    start_time = time.time()

    # ------------------------------------------------------------------
    # Optional: load from Supabase (Postgres) instead of Parquet.
    # This is designed for Streamlit Cloud "free tier" deployments where
    # disk persistence is limited and DB pushdown can reduce load time.
    # ------------------------------------------------------------------
    def _get_secret_or_env(key: str, default: Optional[str] = None) -> Optional[str]:
        try:
            if key in st.secrets:
                v = st.secrets.get(key)
                return None if v is None else str(v)
        except Exception:
            pass
        v = os.getenv(key, default)
        return None if v is None else str(v)

    use_supabase = (_get_secret_or_env("USE_SUPABASE", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    supabase_db_url = _get_secret_or_env("SUPABASE_DB_URL")
    if use_supabase and supabase_db_url:
        try:
            import psycopg2  # type: ignore

            cols = REQUIRED_COLUMNS if use_required_subset else None
            if cols is None:
                select_cols = "oe.*, o.valor_total as \"valorTotal\""
                select_cols_oe = "oe.*"
            else:
                # Algumas colunas no Postgres podem estar em lowercase (criação sem aspas).
                # Ex.: transportadoranome (db) vs transportadoraNome (código).
                col_exprs: list[str] = []
                for c in cols:
                    if c == "category_name":
                        continue
                    if c == "valorTotal":
                        # valorTotal vem da tabela de cabeçalho (orders), não de orders_enriched
                        continue
                    if c == "transportadoraNome":
                        # db column: transportadoranome  -> alias em camelCase para manter compatibilidade
                        col_exprs.append("oe.\"transportadoranome\" as \"transportadoraNome\"")
                        continue
                    col_exprs.append(f"oe.\"{c}\"")
                # manter category_name por último (se existir)
                col_exprs.append("oe.\"category_name\"")
                # adicionar valorTotal do cabeçalho (orders)
                col_exprs_with_orders = col_exprs + ["o.valor_total as \"valorTotal\""]
                select_cols = ", ".join(col_exprs_with_orders)
                select_cols_oe = ", ".join(col_exprs)

            where = ""
            params: list[Any] = []
            if days is not None:
                start_date = (datetime.now() - timedelta(days=days))
                where = " where oe.marketplace_date >= %s"
                params.append(start_date)

            sql_join = f"""
                select {select_cols}
                from public.orders_enriched oe
                left join public.orders o on oe.order_id = o.order_id
                {where};
            """
            sql_no_join = f"""
                select {select_cols_oe}
                from public.orders_enriched oe
                {where};
            """
            with psycopg2.connect(supabase_db_url) as conn:
                try:
                    df = pd.read_sql_query(sql_join, conn, params=params)
                except Exception as e:
                    msg = str(e).lower()
                    if "orders" in msg or "relation" in msg:
                        print("[CARREGANDO] Aviso: tabela public.orders não encontrada. Usando somente orders_enriched.")
                        df = pd.read_sql_query(sql_no_join, conn, params=params)
                    else:
                        raise

            # Alias + defaults + funnel mapping (same behaviour as Parquet loader)
            if "product_category_name" not in df.columns and "category_name" in df.columns:
                df["product_category_name"] = df["category_name"]
            if "customer_id" not in df.columns and "customer_unique_id" in df.columns:
                df["customer_id"] = df["customer_unique_id"]
            
            # Garantir tipos de datas
            date_cols = [
                "order_purchase_timestamp", 
                "marketplace_date", 
                "order_delivered_customer_date",
                "dataLimiteEntregaCliente",
                "dataLimitePostagem"
            ]
            for dc in date_cols:
                if dc in df.columns:
                    df[dc] = pd.to_datetime(df[dc], utc=True).dt.tz_localize(None)

            for col, default in REQUIRED_DEFAULTS.items():
                if col not in df.columns:
                    df[col] = default
            df = _map_order_status_to_funnel(df)

            total_duration = time.time() - start_time
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"[CARREGANDO] Supabase (orders_enriched)")
            print(f"   [OK] Carregado {len(df):,} registros via Supabase")
            print(f"   [MEMORIA] Uso de memória: {memory_mb:.1f} MB")
            print(f"   [TEMPO] Tempo total: {total_duration:.2f}s")
            return df
        except Exception as e:
            print(f"   [ERRO] Falha ao carregar via Supabase: {e}")
            # Fall back to Parquet paths below.

    def _resolve_custom_paths(path_str: str) -> tuple[list[Path], Optional[Path]]:
        path_obj = Path(path_str)
        candidates: list[Path] = []
        order_lookup: Optional[Path] = None
        if path_obj.is_dir():
            enriched = path_obj / "public_orders_enriched.parquet"
            if enriched.exists():
                candidates.append(enriched)
            else:
                candidates.extend(sorted(path_obj.glob("public_orders_enriched*.parquet")))
            orders_candidate = path_obj / "public_orders.parquet"
            if orders_candidate.exists():
                order_lookup = orders_candidate
        elif path_obj.exists():
            candidates.append(path_obj)
            # Lookup de valor por pedido precisa ser 1 linha por order_id (public_orders.parquet).
            # public_orders_enriched é itemizado: usar como order_lookup quebra o merge e subconta
            # receita / receita perdida vs. rodar com INSIGHTX_ORDERS_PATH apontando para public_orders.
            if "public_orders" in path_obj.name.lower() and path_obj.suffix == ".parquet":
                name_l = path_obj.name.lower()
                if "enriched" in name_l:
                    sibling = path_obj.parent / "public_orders.parquet"
                    order_lookup = sibling if sibling.exists() else None
                elif path_obj.name.startswith("public_orders"):
                    order_lookup = path_obj
        return candidates, order_lookup

    paths_to_try = []
    orders_backup_path: Optional[Path] = None
    if custom_path is not None:
        resolved_candidates, orders_candidate = _resolve_custom_paths(str(custom_path))
        for candidate in resolved_candidates:
            if candidate is not None:
                paths_to_try.append(candidate)
        if orders_candidate is not None:
            orders_backup_path = orders_candidate

    if not paths_to_try:
        paths_to_try.extend(DATA_CANDIDATES)

    # INSIGHTX_ORDERS_PATH env override
    orders_env = _get_secret_or_env("INSIGHTX_ORDERS_PATH")
    if orders_env:
        env_path = Path(orders_env)
        if env_path.exists():
            orders_backup_path = env_path
    if orders_backup_path is None:
        orders_backup_path = _find_latest_orders_backup()

    # Encontrar primeiro caminho existente
    parquet_path = next((p for p in paths_to_try if p.exists()), None)
    
    if parquet_path is None:
        searched_paths = [str(p) for p in paths_to_try]
        error_msg = f"""Nenhum dado encontrado nas pastas esperadas.
        
Caminhos verificados:
{chr(10).join(f"- {path}" for path in searched_paths)}"""
        raise FileNotFoundError(error_msg)

    # Log do arquivo sendo carregado
    is_partitioned = _safe_is_dir(parquet_path)
    size_str = "DIR" if is_partitioned else f"{parquet_path.stat().st_size / 1024 / 1024:.1f} MB"
    print(f"[CARREGANDO] {parquet_path.name} ({size_str})")

    read_kwargs = {}
    if use_required_subset:
        read_kwargs["columns"] = REQUIRED_COLUMNS
        print(f"   [INFO] Carregando apenas {len(REQUIRED_COLUMNS)} colunas essenciais")

    # Configurar filtros de partição se solicitado
    filters = None
    if days is not None and is_partitioned:
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        # Filtro PyArrow para pushdown predicate
        # Assumindo que a partição é 'marketplace_date' ou 'order_purchase_timestamp' convertida
        filters = [('marketplace_date', '>=', start_date)]
        print(f"   [INFO] Filtrando partições >= {start_date}")

    try:
        load_start = time.time()
        
        # Leitura
        if is_partitioned:
             # Leitura de dataset particionado
             # Se filters for passado e colunas tb, o PyArrow lida com otimização
             df = pd.read_parquet(parquet_path, filters=filters, **read_kwargs)
        else:
             df = pd.read_parquet(parquet_path, **read_kwargs)
             
        load_duration = time.time() - load_start
        
        # Normalizar colunas de data (Parquet local pode vir com timezone)
        date_cols = [
            "order_purchase_timestamp",
            "marketplace_date",
            "order_delivered_customer_date",
            "dataLimiteEntregaCliente",
            "dataLimitePostagem",
        ]
        for dc in date_cols:
            if dc in df.columns:
                df[dc] = (
                    pd.to_datetime(df[dc], errors="coerce", utc=True)
                    .dt.tz_localize(None)
                )

        # Alias para category_name -> product_category_name se necessário
        if "product_category_name" not in df.columns and "category_name" in df.columns:
            df["product_category_name"] = df["category_name"]
            
        # Alias para customer_unique_id -> customer_id (necessário para validadores)
        if "customer_id" not in df.columns and "customer_unique_id" in df.columns:
            df["customer_id"] = df["customer_unique_id"]

        # Alias backup Supabase: transportadoranome (export) -> transportadoraNome (app)
        if "transportadoraNome" not in df.columns and "transportadoranome" in df.columns:
            df["transportadoraNome"] = df["transportadoranome"]
        
        # Garantir que todas as colunas requeridas existam
        for col, default in REQUIRED_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default
                
        # Aplicar mapeamento de funil robusto (Magazord/Geral)
        df = _map_order_status_to_funnel(df)

        df = _merge_orders_valor_total(df, orders_backup_path)
        
        total_duration = time.time() - start_time
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        print(f"   [OK] Carregado {len(df):,} registros em {load_duration:.2f}s")
        print(f"   [MEMORIA] Uso de memória: {memory_mb:.1f} MB")
        print(f"   [TEMPO] Tempo total: {total_duration:.2f}s")
        
        return df
    except Exception as e:
        print(f"   [ERRO] Erro ao carregar dados: {e}")
        # Se falhar ao ler subset, faz fallback para leitura total
        if "columns" in read_kwargs:
            print("   [TENTANDO] Carregar arquivo completo...")
            read_kwargs.pop("columns", None)
            load_start = time.time()
            df = pd.read_parquet(parquet_path, **read_kwargs)
            load_duration = time.time() - load_start
            if "product_category_name" not in df.columns and "category_name" in df.columns:
                df["product_category_name"] = df["category_name"]
            if "customer_id" not in df.columns and "customer_unique_id" in df.columns:
                df["customer_id"] = df["customer_unique_id"]
            if "transportadoraNome" not in df.columns and "transportadoranome" in df.columns:
                df["transportadoraNome"] = df["transportadoranome"]
            for col, default in REQUIRED_DEFAULTS.items():
                if col not in df.columns:
                    df[col] = default
            df = _map_order_status_to_funnel(df)
            df = _merge_orders_valor_total(df, orders_backup_path)
            print(f"   [OK] Carregado completo em {load_duration:.2f}s")
            return df
        raise

def filter_by_date_range(df: pd.DataFrame, date_range: Optional[List[str]]) -> pd.DataFrame:
    """Filtra o DataFrame pelo período selecionado."""
    if not date_range or len(date_range) != 2:
        return df
    
    # Garantir que a coluna de timestamp está em datetime e sem timezone para evitar comparações inválidas
    df['order_purchase_timestamp'] = (
        pd.to_datetime(df['order_purchase_timestamp'], errors='coerce', utc=True)
        .dt.tz_localize(None)
    )
    
    start_date = pd.to_datetime(date_range[0], utc=True).tz_localize(None)
    end_date = pd.to_datetime(date_range[1], utc=True).tz_localize(None)
    
    return df[
        (df['order_purchase_timestamp'] >= start_date) & 
        (df['order_purchase_timestamp'] <= end_date)
    ]

def _sum_order_value(df: pd.DataFrame, value_col: str) -> Optional[float]:
    """Soma valores por pedido evitando duplicação em datasets itemizados."""
    if value_col not in df.columns:
        return None
    series = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    if "order_id" in df.columns:
        return float(series.groupby(df["order_id"]).max().sum())
    return float(series.sum())

def _find_latest_orders_backup() -> Optional[Path]:
    root = Path("data/supabase_backup")
    if not root.exists():
        return None
    candidates = []
    for entry in root.iterdir():
        if entry.is_dir():
            candidate = entry / "public_orders.parquet"
            if candidate.exists():
                candidates.append(candidate)
    return max(candidates, key=lambda p: p.stat().st_mtime, default=None)


def _resolve_revenue_total(df: pd.DataFrame) -> float:
    """
    Resolve o total de faturamento priorizando o valor total do pedido.
    Ordem de preferência:
    1) valorTotal (pedido)
    2) valorTotalFinal (pedido)
    3) ticket_liquido_linha (linha)
    4) total_item_value (linha)
    5) price (+ freight_value se disponível)
    """
    # TODO: garantir que o pipeline persista "valorTotal" do endpoint /site/pedido
    # como coluna canônica no dataset final (evita fallback por linha).
    for col in ["valorTotal", "valorTotalFinal"]:
        total = _sum_order_value(df, col)
        if total is not None and total > 0:
            return total
    if "ticket_liquido_linha" in df.columns:
        return float(pd.to_numeric(df["ticket_liquido_linha"], errors="coerce").fillna(0).sum())
    if "total_item_value" in df.columns:
        return float(pd.to_numeric(df["total_item_value"], errors="coerce").fillna(0).sum())
    price_series = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0)
    if "freight_value" in df.columns:
        price_series = price_series + pd.to_numeric(df["freight_value"], errors="coerce").fillna(0)
    return float(price_series.sum())


def _merge_orders_valor_total(df: pd.DataFrame, orders_path: Optional[Path]) -> pd.DataFrame:
    if orders_path is None or not orders_path.exists():
        return df

    try:
        orders = pd.read_parquet(orders_path)
    except Exception:
        return df

    valor_col = next(
        (col for col in ("valorTotal", "valor_total", "valor_total_final") if col in orders.columns),
        None,
    )
    if valor_col is None or "order_id" not in orders.columns:
        return df

    orders = orders[["order_id", valor_col]].rename(columns={valor_col: "valorTotal_from_orders"})

    def _norm_order_id(s: pd.Series) -> pd.Series:
        out = s.astype("string").str.strip()
        out = out.str.replace(r"\.0$", "", regex=True)
        return out

    orders["order_id"] = _norm_order_id(orders["order_id"])
    if "order_id" in df.columns:
        df["order_id"] = _norm_order_id(df["order_id"])

    merged = df.merge(orders, on="order_id", how="left")

    # IMPORTANT:
    # Em modo local, REQUIRED_DEFAULTS injeta valorTotal=0.0 quando a coluna não existe.
    # Se mantivermos esse zero, os KPIs ficam subestimados mesmo com public_orders disponível.
    if "valorTotal" not in merged.columns:
        merged["valorTotal"] = np.nan

    current_vt = pd.to_numeric(merged["valorTotal"], errors="coerce")
    orders_vt = pd.to_numeric(merged.get("valorTotal_from_orders"), errors="coerce")

    # Preencher quando valorTotal estiver ausente OU zerado (zero sintético/default).
    merged["valorTotal"] = current_vt.where(current_vt.notna() & (current_vt != 0), orders_vt)
    merged["valorTotal"] = pd.to_numeric(merged["valorTotal"], errors="coerce").fillna(0.0)

    merged.drop(columns=["valorTotal_from_orders"], inplace=True, errors="ignore")
    return merged
def _resolve_revenue_total_valor_total_only(df: pd.DataFrame) -> float:
    """
    Contabiliza faturamento apenas com valorTotal (payload v2/pedidos).
    Soma por pedido (evita duplicar em datasets itemizados). Fallback para _resolve_revenue_total.
    """
    if "valorTotal" not in df.columns:
        return _resolve_revenue_total(df)
    total = _sum_order_value(df, "valorTotal")
    if total is not None and total > 0:
        return total
    return _resolve_revenue_total(df)


def per_line_lost_revenue(df: pd.DataFrame) -> pd.Series:
    """
    Valor monetário por linha para pedidos cancelados / análise de sangramento por SKU.

    Quando ``valorTotal`` vem zerado no dataset itemizado (comum em linhas canceladas),
    usa a mesma cadeia de fallback de ``_resolve_revenue_total`` ao nível de linha:
    ticket_liquido_linha → total_item_value → price + freight_value.
    """
    idx = df.index
    if "valorTotal" in df.columns:
        vt = pd.to_numeric(df["valorTotal"], errors="coerce").fillna(0.0)
    else:
        vt = pd.Series(0.0, index=idx)
    tl = (
        pd.to_numeric(df["ticket_liquido_linha"], errors="coerce").fillna(0.0)
        if "ticket_liquido_linha" in df.columns
        else pd.Series(0.0, index=idx)
    )
    ti = (
        pd.to_numeric(df["total_item_value"], errors="coerce").fillna(0.0)
        if "total_item_value" in df.columns
        else pd.Series(0.0, index=idx)
    )
    pr = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0.0)
    if not isinstance(pr, pd.Series):
        pr = pd.Series(float(pr), index=idx)
    fr = (
        pd.to_numeric(df["freight_value"], errors="coerce").fillna(0.0)
        if "freight_value" in df.columns
        else pd.Series(0.0, index=idx)
    )
    out = vt.copy()
    need = out <= 0
    out = out.where(~need, tl)
    need = out <= 0
    out = out.where(~need, ti)
    need = out <= 0
    out = out.where(~need, pr + fr)
    return out


def lost_revenue_cancelled(df: pd.DataFrame) -> float:
    """
    Receita perdida com pedidos cancelados: total agregado com fallbacks quando valorTotal soma zero.
    Espelha a hierarquia de ``_resolve_revenue_total``; se ainda zero, tenta ``price_original`` (exports legados).
    """
    df_c = df[df["pedido_cancelado"] == 1]
    if df_c.empty:
        return 0.0
    total = float(_resolve_revenue_total(df_c))
    if total > 0:
        return total
    if "price_original" in df_c.columns:
        return float(pd.to_numeric(df_c["price_original"], errors="coerce").fillna(0).sum())
    return 0.0


from utils.db_manager import get_db

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_acquisition_retention_kpis(df: pd.DataFrame, marketing_spend: float = 50000, date_range: Optional[List[str]] = None, eligible_only: bool = False) -> Dict[str, float]:
    """Calcula KPIs específicos para análise de aquisição e retenção.
    eligible_only: se True (padrão), considera apenas pedidos elegíveis. Se False, usa todos os pedidos do período."""
    # Filtrar dados pelo período
    df = filter_by_date_range(df, date_range).copy()
    
    # Converter colunas de data para datetime
    df['order_purchase_timestamp'] = (
        pd.to_datetime(df['order_purchase_timestamp'], errors='coerce', utc=True)
        .dt.tz_localize(None)
    )
    
    if eligible_only:
        eligible_mask = df["pedido_cancelado"] == 0
        status_criteria = None
        if "order_status" in df.columns:
            def _normalize_status(value: Any) -> str:
                if pd.isna(value):
                    return ""
                s = str(value).lower().strip()
                s = pd.Series([s]).str.replace(r"^\d+\s*-\s*", "", regex=True).iloc[0]
                s = (s.replace("ã","a").replace("á","a").replace("â","a")
                       .replace("ç","c").replace("é","e").replace("ê","e")
                       .replace("í","i").replace("ó","o").replace("ô","o")
                       .replace("ú","u"))
                return s
            status_norm = df["order_status"].apply(_normalize_status)
            approved_mask = status_norm.str.contains("aprov", na=False) | status_norm.str.contains("approved", na=False)
            transport_mask = (
                status_norm.str.contains("transp", na=False) |
                status_norm.str.contains("transit", na=False) |
                status_norm.str.contains("envio", na=False) |
                status_norm.str.contains("shipp", na=False)
            )
            delivered_mask = status_norm.str.contains("entreg", na=False) | status_norm.str.contains("deliver", na=False)
            pending_mask = status_norm.str.contains("aguard", na=False) | status_norm.str.contains("pending", na=False)
            special_mask = status_norm.str.contains("credito por troca", na=False) | status_norm.str.contains("troca", na=False)
            status_criteria = (approved_mask | transport_mask | delivered_mask) & (~pending_mask) & (~special_mask)
        status_code_criteria = None
        if "order_status_code" in df.columns:
            eligible_codes = [4, 5, 6, 7, 8, 12, 19, 23, 26, 27, 29, 30]
            status_code = pd.to_numeric(df["order_status_code"], errors="coerce")
            status_code_criteria = status_code.isin(eligible_codes)
        funnel_cols = ["funnel_paid", "funnel_in_transit", "funnel_delivered"]
        has_funnel = any(col in df.columns for col in funnel_cols)
        funnel_criteria = None
        if has_funnel:
            funnel_paid = (df.get("funnel_paid", 0) == 1)
            funnel_transit = (df.get("funnel_in_transit", 0) == 1)
            funnel_delivered = (df.get("funnel_delivered", 0) == 1)
            funnel_criteria = funnel_paid | funnel_transit | funnel_delivered
        if status_code_criteria is not None:
            eligible_mask = eligible_mask & status_code_criteria
        elif status_criteria is not None and funnel_criteria is not None:
            eligible_mask = eligible_mask & (status_criteria | funnel_criteria)
        elif status_criteria is not None:
            eligible_mask = eligible_mask & status_criteria
        elif funnel_criteria is not None:
            eligible_mask = eligible_mask & funnel_criteria
        df = df[eligible_mask].copy()
    
    # Se o DF for pequeno (<10k linhas), usar Pandas puro para evitar overhead de registro no DuckDB
    if len(df) < 10000:
        # --- Lógica Original Pandas ---
        df['month'] = df['order_purchase_timestamp'].dt.to_period('M')
        df['month_str'] = df['month'].astype(str)
        
        first_purchases = df.groupby('customer_unique_id')['order_purchase_timestamp'].min().reset_index()
        first_purchases['month'] = first_purchases['order_purchase_timestamp'].dt.to_period('M')
        
        new_customers = first_purchases.groupby('month')['customer_unique_id'].count().reset_index()
        new_customers['month'] = new_customers['month'].astype(str)
        total_new_customers = first_purchases['customer_unique_id'].nunique()
        
        customer_orders = df.groupby(['customer_unique_id', 'month'])['order_id'].nunique().reset_index()
        customer_orders['month'] = customer_orders['month'].astype(str)
        returning_customers = customer_orders[customer_orders['order_id'] > 1].groupby('month')['customer_unique_id'].nunique().reset_index()
    
        total_customers = df['customer_unique_id'].nunique()
        customers_with_multiple_orders = df.groupby('customer_unique_id')['order_id'].nunique()
        customers_with_multiple_orders_count = customers_with_multiple_orders[customers_with_multiple_orders > 1].count()
        repurchase_rate = customers_with_multiple_orders_count / total_customers if total_customers > 0 else 0
    
        # Tempo até a segunda compra: intervalo entre 1º e 2º pedido por cliente
        df_sorted = df[['customer_unique_id', 'order_purchase_timestamp']].sort_values(
            ['customer_unique_id', 'order_purchase_timestamp']
        )
        first_purchase = df_sorted.groupby('customer_unique_id')['order_purchase_timestamp'].nth(0)
        second_purchase = df_sorted.groupby('customer_unique_id')['order_purchase_timestamp'].nth(1)
        time_to_second = second_purchase - first_purchase
        if time_to_second.empty:
            avg_time_to_second = 0
        else:
            valid_times = time_to_second.dropna()
            valid_times = valid_times[valid_times > pd.Timedelta(0)]
            valid_times_days = valid_times.dt.total_seconds() / 86400
            avg_time_to_second = valid_times_days.mean() if not valid_times_days.empty else 0
        customers_rebuy = customers_with_multiple_orders_count
    else:
        # --- Lógica Otimizada DuckDB ---
        try:
            db = get_db()
            db.conn.register('temp_acq_df', df)
            
            # 1. Novos clientes por mês
            query_new = """
                WITH first_purchases AS (
                    SELECT 
                        customer_unique_id, 
                        MIN(order_purchase_timestamp) as first_purchase_date
                    FROM temp_acq_df
                    GROUP BY customer_unique_id
                )
                SELECT 
                    strftime(date_trunc('month', first_purchase_date), '%Y-%m') as month,
                    COUNT(customer_unique_id) as customer_unique_id
                FROM first_purchases
                GROUP BY 1
                ORDER BY 1
            """
            new_customers = db.query(query_new)
            
            # 2. Clientes recorrentes por mês
            query_returning = """
                WITH orders_per_month AS (
                    SELECT 
                        customer_unique_id,
                        strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as month,
                        COUNT(DISTINCT order_id) as order_count
                    FROM temp_acq_df
                    GROUP BY 1, 2
                )
                SELECT 
                    month,
                    COUNT(customer_unique_id) as customer_unique_id
                FROM orders_per_month
                WHERE order_count > 1
                GROUP BY 1
                ORDER BY 1
            """
            returning_customers = db.query(query_returning)
            
            # 3. Métricas globais (recompra, tempo médio)
            query_metrics = """
                WITH customer_stats AS (
                    SELECT 
                        customer_unique_id,
                        COUNT(DISTINCT order_id) as total_orders
                    FROM temp_acq_df
                    GROUP BY 1
                ),
                ranked_orders AS (
                    SELECT 
                        customer_unique_id,
                        order_purchase_timestamp,
                        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) as rn
                    FROM temp_acq_df
                ),
                first_second AS (
                    SELECT 
                        customer_unique_id,
                        MAX(CASE WHEN rn = 1 THEN order_purchase_timestamp END) AS first_order,
                        MAX(CASE WHEN rn = 2 THEN order_purchase_timestamp END) AS second_order
                    FROM ranked_orders
                    GROUP BY 1
                ),
                time_to_second AS (
                    SELECT 
                        customer_unique_id,
                        date_diff('day', first_order, second_order) AS days_to_second
                    FROM first_second
                    WHERE second_order IS NOT NULL AND second_order > first_order
                )
                SELECT
                    (SELECT COUNT(*) FROM customer_stats) as total_customers,
                    (SELECT COUNT(*) FROM customer_stats WHERE total_orders > 1) as customers_rebuy,
                    (SELECT AVG(days_to_second) FROM time_to_second) as avg_days_between
            """
            metrics_df = db.query(query_metrics)
            
            if not metrics_df.empty:
                total_customers = metrics_df['total_customers'][0]
                customers_rebuy = metrics_df['customers_rebuy'][0]
                repurchase_rate = customers_rebuy / total_customers if total_customers > 0 else 0
                avg_time_to_second = metrics_df['avg_days_between'][0] or 0
            else:
                total_new_customers = 0
                customers_rebuy = 0
                repurchase_rate = 0
                avg_time_to_second = 0
                
            total_new_customers = new_customers['customer_unique_id'].sum() if not new_customers.empty else 0
            
            db.conn.unregister('temp_acq_df')
            
        except Exception as e:
            print(f"Erro DuckDB Acquisition: {e}")
            # Fallback seguro para Pandas (código duplicado simplificado para não estourar contexto)
            return calculate_acquisition_retention_kpis.__wrapped__(df, marketing_spend, date_range)

    # --- Cálculos Financeiros (Comuns) ---
    cac = marketing_spend / total_new_customers if total_new_customers > 0 else 0
    
    total_net_revenue = 0.0
    df_calc = df.copy()
    df_calc['price'] = pd.to_numeric(df_calc['price'], errors='coerce').fillna(0)

    # Contabilização apenas valorTotal (payload v2/pedidos)
    total_revenue = _resolve_revenue_total_valor_total_only(df_calc)
    if "margin_net_revenue" in df_calc.columns:
        net_sum = float(df_calc["margin_net_revenue"].sum())
        total_net_revenue = net_sum if net_sum > 0 else total_revenue
    else:
        total_net_revenue = total_revenue

    total_margin = float(df_calc["contribution_margin"].sum()) if "contribution_margin" in df_calc.columns else 0.0
    total_customers_count = df["customer_unique_id"].nunique() # Recalcular para garantir consistência
    
    ltv_revenue = total_net_revenue / total_customers_count if total_customers_count > 0 else 0
    ltv_margin = total_margin / total_customers_count if total_customers_count > 0 else 0
    
    ltv_cac_ratio = (ltv_revenue / cac) if cac > 0 else 0
    ltv_cac_ratio_margin = (ltv_margin / cac) if cac > 0 else 0
    
    return {
        "new_customers": new_customers,
        "returning_customers": returning_customers,
        "repurchase_rate": repurchase_rate,
        "avg_time_to_second": avg_time_to_second,
        "cac": cac,
        "ltv": ltv_revenue,
        "ltv_revenue": ltv_revenue,
        "ltv_margin": ltv_margin,
        "ltv_cac_ratio": ltv_cac_ratio,
        "ltv_cac_ratio_margin": ltv_cac_ratio_margin,
        "total_new_customers": total_new_customers,
        "total_returning_customers": customers_rebuy,
        "total_net_revenue": total_net_revenue,
        "total_margin": total_margin,
        "total_revenue": total_revenue,
    }

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_kpis(df: pd.DataFrame, marketing_spend: float = 50000, date_range: Optional[List[str]] = None, eligible_only: bool = True) -> Dict[str, float]:
    """Calcula os principais KPIs do negócio.
    eligible_only: se True (padrão), considera apenas pedidos elegíveis (não cancelados, status aprovado/transito/entregue).
    Se False, contabiliza todos os pedidos do período, sem máscara de elegibilidade."""
    # Garantir colunas mínimas para evitar KeyError quando dataset estiver vazio
    required_defaults = {
        "price": 0.0,
        "pedido_cancelado": 0,
        "order_id": pd.Series(dtype=object),
        "customer_unique_id": pd.Series(dtype=object),
        "product_id": pd.Series(dtype=object),
        "product_category_name": pd.Series(dtype=object),
        "review_score": np.nan,
    }
    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default
    
    # Filtrar dados pelo período
    df = filter_by_date_range(df, date_range).copy()
    
    # Converter colunas de data para datetime
    df['order_purchase_timestamp'] = (
        pd.to_datetime(df['order_purchase_timestamp'], errors='coerce', utc=True)
        .dt.tz_localize(None)
    )
    if 'order_delivered_customer_date' in df.columns:
        df = df.copy()  # Additional copy before modifying
        df['order_delivered_customer_date'] = (
            pd.to_datetime(df['order_delivered_customer_date'], errors='coerce', utc=True)
            .dt.tz_localize(None)
        )
    
    # Garantir que price seja numérico
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)
    
    # Normalizar status para elegibilidade (quando disponível)
    # Regra: considerar apenas pedidos em situação "Normal" (tipo 1)
    # EXCLUIR: Cancelados (tipo 3), Anomalias (tipo 2), Aguardando Terceiro (tipo 4)
    # Baseado na tabela de situações Magazord (ver docs)
    eligible_mask = df["pedido_cancelado"] == 0
    status_criteria = None
    if "order_status" in df.columns:
        def _normalize_status(value: Any) -> str:
            if pd.isna(value):
                return ""
            s = str(value).lower().strip()
            s = pd.Series([s]).str.replace(r"^\d+\s*-\s*", "", regex=True).iloc[0]
            # remover acentos básicos
            s = (s.replace("ã","a").replace("á","a").replace("â","a")
                   .replace("ç","c").replace("é","e").replace("ê","e")
                   .replace("í","i").replace("ó","o").replace("ô","o")
                   .replace("ú","u"))
            return s
        status_norm = df["order_status"].apply(_normalize_status)
        approved_mask = status_norm.str.contains("aprov", na=False) | status_norm.str.contains("approved", na=False)
        transport_mask = (
            status_norm.str.contains("transp", na=False) |
            status_norm.str.contains("transit", na=False) |
            status_norm.str.contains("envio", na=False) |
            status_norm.str.contains("shipp", na=False)
        )
        delivered_mask = status_norm.str.contains("entreg", na=False) | status_norm.str.contains("deliver", na=False)
        pending_mask = status_norm.str.contains("aguard", na=False) | status_norm.str.contains("pending", na=False)
        special_mask = status_norm.str.contains("credito por troca", na=False) | status_norm.str.contains("troca", na=False)
        status_criteria = (approved_mask | transport_mask | delivered_mask) & (~pending_mask) & (~special_mask)
    
    # Filtro adicional: usar código de situação quando disponível (mais confiável)
    # Situações ELEGÍVEIS para receita (tipo Normal = 1):
    # 4,5,6,7,8,12,19,23,26,27,29,30 (Aprovado, Integrado, NF, Transporte, Entregue, etc.)
    # EXCLUIR: 2,14,24 (Cancelados), 1,3,15,18 (Aguardando), 9,10,11,16,17,20,21,22,25,28,31 (Anomalias)
    status_code_criteria = None
    if "order_status_code" in df.columns:
        eligible_codes = [4, 5, 6, 7, 8, 12, 19, 23, 26, 27, 29, 30]
        status_code = pd.to_numeric(df["order_status_code"], errors="coerce")
        status_code_criteria = status_code.isin(eligible_codes)
    
    # Considerar colunas de funil quando existirem (paid, in_transit, delivered)
    funnel_cols = ["funnel_paid", "funnel_in_transit", "funnel_delivered"]
    has_funnel = any(col in df.columns for col in funnel_cols)
    if has_funnel:
        funnel_paid = (df.get("funnel_paid", 0) == 1)
        funnel_transit = (df.get("funnel_in_transit", 0) == 1)
        funnel_delivered = (df.get("funnel_delivered", 0) == 1)
        funnel_criteria = funnel_paid | funnel_transit | funnel_delivered
    else:
        funnel_criteria = None

    # Combinar critérios (prioridade: status_code > funnel > status textual)
    if eligible_only:
        if status_code_criteria is not None:
            eligible_mask = eligible_mask & status_code_criteria
        elif status_criteria is not None and funnel_criteria is not None:
            eligible_mask = eligible_mask & (status_criteria | funnel_criteria)
        elif status_criteria is not None:
            eligible_mask = eligible_mask & status_criteria
        elif funnel_criteria is not None:
            eligible_mask = eligible_mask & funnel_criteria
    else:
        eligible_mask = pd.Series(True, index=df.index)
    df_eligible = df[eligible_mask].copy()
    
    # Calcular KPIs (com ou sem máscara de elegibilidade)
    # Contabilização apenas valorTotal (payload v2/pedidos)
    total_revenue = _resolve_revenue_total_valor_total_only(df_eligible)

    total_orders = int(df_eligible["order_id"].nunique())
    total_customers = int(df_eligible["customer_unique_id"].nunique())
    
    # Determinar coluna de produto com fallback
    product_col = None
    if 'product_id' in df_eligible.columns:
        product_col = 'product_id'
    elif 'sku' in df_eligible.columns:
        product_col = 'sku'
    elif 'codigo' in df_eligible.columns:
        product_col = 'codigo'
    elif 'produto_id' in df_eligible.columns:
        product_col = 'produto_id'
    else:
        # Fallback: usar order_id se não houver coluna de produto
        product_col = 'order_id'
    
    total_products = int(df_eligible[product_col].nunique())
    unique_categories = int(df_eligible["product_category_name"].nunique())
    
    # Taxa de abandono (com cálculo progressivo de churn por estagnação)
    # 1. Base Clássica: Pedidos explicitamente cancelados (usuário cancelou ou sistema cancelou)
    base_abandonments = int(df[df["pedido_cancelado"] == 1]["order_id"].nunique())
    
    # 2. Churn Progressivo (Pedidos "Zumbis"): Pedidos antigos não faturados e não cancelados
    # NOVA REGRA: Só calcular para pedidos que NÃO foram faturados (funnel_invoice_issued = 0)
    # Degradando elegantemente: 30d (crítico), 21d (alto risco)
    churn_count = 0
    abandonment_message = None
    
    try:
        # Verificar se temos período suficiente (mínimo 21 dias)
        if date_range and len(date_range) >= 2:
            start_date = pd.to_datetime(date_range[0])
            end_date = pd.to_datetime(date_range[1])
            period_days = (end_date - start_date).days
            if period_days < 21:
                abandonment_message = "Período Insuficiente: Mínimo de 21 dias necessário para calcular abandono por estagnação"
                # Retornar apenas abandono base (cancelados explícitos)
                total_carts = int(df["order_id"].nunique())
                abandonment_rate = float(base_abandonments / total_carts if total_carts > 0 else 0)
                csat_early = float(df["review_score"].mean()) if not df["review_score"].isna().all() else np.nan
                avg_ticket_early = float(total_revenue / total_orders if total_orders > 0 else 0)
                if "order_delivered_customer_date" in df.columns:
                    df_tmp = df.copy()
                    df_tmp["delivery_time"] = (
                        df_tmp["order_delivered_customer_date"] - df_tmp["order_purchase_timestamp"]
                    ).dt.days
                    avg_del_early = (
                        float(df_tmp["delivery_time"].mean())
                        if not df_tmp["delivery_time"].isna().all()
                        else np.nan
                    )
                else:
                    avg_del_early = np.nan
                if len(df) > 0 and "order_id" in df.columns:
                    tuo = df["order_id"].nunique()
                    cuo = df[df["pedido_cancelado"] == 1]["order_id"].nunique()
                    cancel_early = float(cuo / tuo * 100) if tuo > 0 else 0.0
                else:
                    cancel_early = float(df["pedido_cancelado"].mean() * 100) if len(df) > 0 else np.nan
                lost_early = lost_revenue_cancelled(df)
                return {
                    "total_revenue": total_revenue,
                    "total_orders": total_orders,
                    "total_customers": total_customers,
                    "total_products": total_products,
                    "unique_categories": unique_categories,
                    "abandonment_rate": abandonment_rate,
                    "abandonment_message": abandonment_message,
                    "csat": csat_early,
                    "average_ticket": avg_ticket_early,
                    "avg_delivery_time": avg_del_early,
                    "cancellation_rate": cancel_early,
                    "lost_revenue": lost_early,
                }
        
        now = datetime.now()
        # Garantir que funnel_invoice_issued existe (calcular se necessário)
        if "funnel_invoice_issued" not in df.columns and "order_status" in df.columns:
            df = _map_order_status_to_funnel(df)
        
        # Pedidos não finalizados: não cancelados E não faturados
        # NOVA REGRA: Só considerar pedidos que NÃO foram faturados
        pending_mask = (df["pedido_cancelado"] == 0)
        if "funnel_invoice_issued" in df.columns:
            pending_mask = pending_mask & (df["funnel_invoice_issued"] == 0)
        
        if "order_status" in df.columns:
            # Filtrar o que não é "entregue" (redundante se já filtrou por invoice, mas mantém robustez)
            not_delivered = ~df["order_status"].astype(str).str.lower().str.contains("entreg", na=False)
            pending_orders = df[pending_mask & not_delivered].copy()
            
            if not pending_orders.empty:
                # Contar pedidos únicos em cada faixa de estagnação
                # > 30 dias: Churn certo (considerar abandono)
                churn_30d = pending_orders[pending_orders["order_purchase_timestamp"] < (now - timedelta(days=30))]["order_id"].nunique()
                
                # > 21 dias: Risco Altíssimo (considerar 50% como abandono estatístico)
                risk_21d = pending_orders[
                    (pending_orders["order_purchase_timestamp"] < (now - timedelta(days=21))) & 
                    (pending_orders["order_purchase_timestamp"] >= (now - timedelta(days=30)))
                ]["order_id"].nunique()
                
                # Somar ao abandono base
                churn_count = churn_30d + int(risk_21d * 0.5)
                
    except Exception:
        churn_count = 0

    total_cart_abandonments = base_abandonments + churn_count
    total_carts = int(df["order_id"].nunique())
    abandonment_rate = float(total_cart_abandonments / total_carts if total_carts > 0 else 0)
    
    # CSAT
    csat = float(df["review_score"].mean()) if not df["review_score"].isna().all() else np.nan
    
    # Ticket médio
    average_ticket = float(total_revenue / total_orders if total_orders > 0 else 0)
    
    # Tempo médio de entrega
    if 'order_delivered_customer_date' in df.columns:
        df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
        avg_delivery_time = float(df['delivery_time'].mean()) if not df['delivery_time'].isna().all() else np.nan
    else:
        avg_delivery_time = np.nan
    
    # Taxa de cancelamento
    # O cálculo deve ser a proporção de pedidos únicos cancelados sobre o total de pedidos únicos
    if len(df) > 0 and "order_id" in df.columns:
        total_unique_orders = df["order_id"].nunique()
        cancelled_unique_orders = df[df["pedido_cancelado"] == 1]["order_id"].nunique()
        cancellation_rate = float(cancelled_unique_orders / total_unique_orders * 100) if total_unique_orders > 0 else 0.0
    else:
        cancellation_rate = float(df["pedido_cancelado"].mean() * 100) if len(df) > 0 else np.nan
    
    # Receita perdida: não usar só _sum_order_value(valorTotal) — em cancelados valorTotal costuma vir zerado.
    lost_revenue = lost_revenue_cancelled(df)
    
    result = {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "total_customers": total_customers,
        "total_products": total_products,
        "unique_categories": unique_categories,
        "abandonment_rate": abandonment_rate,
        "csat": csat,
        "average_ticket": average_ticket,
        "avg_delivery_time": avg_delivery_time,
        "cancellation_rate": cancellation_rate,
        "lost_revenue": lost_revenue
    }
    if abandonment_message:
        result["abandonment_message"] = abandonment_message
    return result

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})  # 24h
def calculate_product_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas por produto/categoria para análise.
    Otimizado com DuckDB para grandes volumes.
    """
    # Se pequeno, usar Pandas (overhead menor)
    if len(df) < 10000:
        df_calc = df.copy()
        if "review_score" not in df_calc.columns:
            df_calc["review_score"] = np.nan

        aggregation = {
            'price': ["mean", "sum", "count"],
            "review_score": ["mean", "count"],
        }
        if 'margin_net_revenue' in df_calc.columns:
            aggregation['margin_net_revenue'] = ['sum', 'mean']
        if 'contribution_margin' in df_calc.columns:
            aggregation['contribution_margin'] = ['sum', 'mean']

        product_metrics = df_calc.groupby(['product_category_name']).agg(aggregation).reset_index()

        # Renomear as colunas
        cols = ['category']
        cols += ['avg_price', 'total_revenue', 'total_sales', 'avg_rating', 'total_reviews']
        has_net = 'margin_net_revenue' in df.columns
        has_margin = 'contribution_margin' in df.columns
        if has_net:
            cols += ['total_net_revenue', 'avg_net_revenue']
        if has_margin:
            cols += ['total_contribution_margin', 'avg_contribution_margin']
        product_metrics.columns = cols
    else:
        # DuckDB Implementation
        try:
            db = get_db()
            db.conn.register('temp_prod_metrics', df)
            
            # Construir query dinâmica baseada nas colunas existentes
            select_clauses = [
                "product_category_name as category",
                "AVG(price) as avg_price",
                "SUM(price) as total_revenue",
                "COUNT(*) as total_sales",
                "AVG(review_score) as avg_rating",
                "COUNT(review_score) as total_reviews"
            ]
            
            if 'margin_net_revenue' in df.columns:
                select_clauses.append("SUM(margin_net_revenue) as total_net_revenue")
                select_clauses.append("AVG(margin_net_revenue) as avg_net_revenue")
                
            if 'contribution_margin' in df.columns:
                select_clauses.append("SUM(contribution_margin) as total_contribution_margin")
                select_clauses.append("AVG(contribution_margin) as avg_contribution_margin")
                
            query = f"""
                SELECT 
                    {', '.join(select_clauses)}
                FROM temp_prod_metrics
                WHERE product_category_name IS NOT NULL
                GROUP BY 1
            """
            product_metrics = db.query(query)
            db.conn.unregister('temp_prod_metrics')
            
        except Exception as e:
            print(f"Erro DuckDB Product Metrics: {e}")
            return calculate_product_metrics.__wrapped__(df)

    # Calcular score composto para ranking (Python é rápido aqui pois o DF é pequeno - 1 linha por categoria)
    revenue_column = 'total_net_revenue' if 'total_net_revenue' in product_metrics.columns else 'total_revenue'
    
    # Preencher NaN
    product_metrics = product_metrics.fillna(0)

    # ------------------------------------------------------------------
    # Enriquecer avaliação por categoria sem merge com pedidos
    # Usa reviews_df se disponível (já processado em st.session_state)
    # ------------------------------------------------------------------
    try:
        import streamlit as st  # já importado no topo, reforço para ambientes isolados
        reviews_df = st.session_state.get("reviews_df") if "reviews_df" in st.session_state else None
        if reviews_df is not None and not reviews_df.empty:
            # fallback de nome de coluna para garantir categoria
            if "product_category_name" not in reviews_df.columns and "category_name" in reviews_df.columns:
                reviews_df = reviews_df.assign(product_category_name=reviews_df["category_name"])
            if "product_category_name" not in reviews_df.columns:
                raise KeyError("reviews_df precisa de 'product_category_name' para agregar avaliações por categoria")

            rev = reviews_df[["product_category_name", "review_score"]].copy()
            rev["review_score"] = pd.to_numeric(rev["review_score"], errors="coerce")
            rev = rev.dropna(subset=["review_score"])
            if not rev.empty:
                cat_rev = rev.groupby("product_category_name")["review_score"].agg(
                    avg_rating="mean", total_reviews="count"
                ).reset_index()
                product_metrics = product_metrics.merge(
                    cat_rev,
                    left_on="category",
                    right_on="product_category_name",
                    how="left",
                )
                # Priorizar avaliação vinda das reviews quando existir
                if "avg_rating_y" in product_metrics.columns:
                    product_metrics["avg_rating"] = product_metrics["avg_rating_y"].fillna(product_metrics["avg_rating_x"])
                    product_metrics.drop(columns=["avg_rating_x", "avg_rating_y"], inplace=True, errors="ignore")
                if "total_reviews" in product_metrics.columns and "total_reviews_y" in product_metrics.columns:
                    product_metrics["total_reviews"] = product_metrics["total_reviews_y"].fillna(product_metrics["total_reviews"])
                elif "total_reviews_y" in product_metrics.columns:
                    product_metrics["total_reviews"] = product_metrics["total_reviews_y"]
                product_metrics.drop(columns=[c for c in ["total_reviews_x", "total_reviews_y", "product_category_name"] if c in product_metrics.columns], inplace=True, errors="ignore")
                product_metrics = product_metrics.fillna(0)
    except Exception:
        pass  # fallback silencioso
    
    product_metrics['composite_score'] = (
        product_metrics[revenue_column].rank(pct=True) * 0.4 +
        product_metrics['total_sales'].rank(pct=True) * 0.3 +
        product_metrics['avg_rating'].rank(pct=True) * 0.3
    )

    return product_metrics

@st.cache_data(ttl=86400, hash_funcs={pd.DataFrame: _hash_dataframe})
def calculate_sku_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas por produto individual (SKU).
    Otimizado com DuckDB para grandes volumes.
    """
    # 1. Determinar coluna de produto
    available_columns = df.columns.tolist()
    product_id_col = next((col for col in ["product_id", "sku", "produto_id", "codigo_produto"] if col in available_columns), "order_id")
    
    # 2. Agregação Híbrida
    if len(df) < 10000:
        # Pandas
        df_calc = df.copy()
        aggregation = {
            'price': ['sum', 'mean', 'count'],
            'review_score': 'mean' if 'review_score' in available_columns else lambda x: np.nan,
            'pedido_cancelado': 'mean' if 'pedido_cancelado' in available_columns else lambda x: 0
        }
        
        sku_metrics = df_calc.groupby([product_id_col, 'product_category_name']).agg(aggregation).reset_index()
        
        # Renomear (flatten multiindex)
        sku_metrics.columns = ['product_id', 'category', 'total_revenue', 'avg_price', 'total_sales', 'avg_rating', 'cancel_rate']
        
    else:
        # DuckDB
        try:
            db = get_db()
            db.conn.register('temp_sku_metrics', df)
            
            # Construir query dinâmica
            has_review = 'review_score' in available_columns
            has_cancel = 'pedido_cancelado' in available_columns
            
            query = f"""
                SELECT 
                    {product_id_col} as product_id,
                    product_category_name as category,
                    SUM(price) as total_revenue,
                    AVG(price) as avg_price,
                    COUNT(*) as total_sales,
                    {'AVG(review_score)' if has_review else 'NULL'} as avg_rating,
                    {'AVG(pedido_cancelado)' if has_cancel else '0'} as cancel_rate
                FROM temp_sku_metrics
                WHERE {product_id_col} IS NOT NULL
                GROUP BY 1, 2
            """
            sku_metrics = db.query(query)
            db.conn.unregister('temp_sku_metrics')
            
        except Exception as e:
            # Fallback Pandas
            return calculate_sku_metrics.__wrapped__(df)

    # 3. Pós-processamento (Score)
    # Preencher NaNs
    sku_metrics = sku_metrics.fillna({'avg_rating': 0, 'cancel_rate': 0, 'total_revenue': 0, 'total_sales': 0})
    
    # Normalizar para score (evitar divisão por zero)
    max_rev = sku_metrics['total_revenue'].max() or 1
    max_sales = sku_metrics['total_sales'].max() or 1
    
    sku_metrics['composite_score'] = (
        0.4 * (sku_metrics['total_revenue'] / max_rev) +
        0.3 * (sku_metrics['total_sales'] / max_sales) +
        0.2 * (sku_metrics['avg_rating'] / 5) +
        0.1 * (1 - sku_metrics['cancel_rate'])
    )
    
    return sku_metrics

def calculate_revenue_forecast(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calcula a previsão de receita para os próximos 30 dias."""
    # Calcular média diária de receita
    if df is None or df.empty:
        raise IndexError("Dataset vazio para previsão de receita")
    if 'order_purchase_timestamp' not in df.columns or 'price' not in df.columns:
        raise IndexError("Colunas necessárias ausentes para previsão de receita")

    df_calc = df.copy()

    # Agregação Diária (Otimizada com DuckDB se grande)
    if len(df) < 10000:
        df_temp = df_calc.copy()
        df_temp['date'] = pd.to_datetime(df_temp['order_purchase_timestamp']).dt.date
        daily_revenue = df_temp.groupby('date')['price'].sum().reset_index()
    else:
        try:
            db = get_db()
            db.conn.register('temp_forecast_src', df)
            query = """
                SELECT 
                    CAST(order_purchase_timestamp AS DATE) as date,
                    SUM(price) as price
                FROM temp_forecast_src
                GROUP BY 1
                ORDER BY 1
            """
            daily_revenue = db.query(query)
            db.conn.unregister('temp_forecast_src')
            # Garantir tipos
            daily_revenue['date'] = pd.to_datetime(daily_revenue['date']).dt.date
        except Exception:
            df_temp = df.copy()
            df_temp['date'] = pd.to_datetime(df_temp['order_purchase_timestamp']).dt.date
            daily_revenue = df_temp.groupby('date')['price'].sum().reset_index()

    if daily_revenue.empty:
        raise IndexError("Sem histórico suficiente para previsão")
    
    # Adicionar dia da semana para análise de sazonalidade
    daily_revenue['day_of_week'] = pd.to_datetime(daily_revenue['date']).dt.day_name()
    
    # Calcular média móvel de 7 dias
    daily_revenue['ma7'] = daily_revenue['price'].rolling(window=7).mean()
    
    # Calcular fatores de sazonalidade semanal
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekly_seasonality = daily_revenue.groupby('day_of_week')['price'].mean().reindex(day_order)
    weekly_seasonality = weekly_seasonality / weekly_seasonality.mean()  # Normalizar
    
    # Calcular tendência de crescimento (últimos 30 dias)
    recent_data = daily_revenue.tail(30)
    if len(recent_data) >= 2:
        x = np.arange(len(recent_data))
        y = recent_data['price'].values
        z = np.polyfit(x, y, 1)
        growth_rate = z[0]  # Coeficiente de crescimento diário
    else:
        growth_rate = 0
    
    # Calcular previsão para os próximos 30 dias
    last_date = daily_revenue['date'].iloc[-1]
    forecast_dates = pd.date_range(start=last_date, periods=31, freq='D')[1:]
    
    # Criar DataFrame para previsão
    forecast_df = pd.DataFrame({'date': forecast_dates})
    forecast_df['day_of_week'] = forecast_df['date'].dt.day_name()
    
    # Aplicar fatores de sazonalidade
    forecast_df['seasonality_factor'] = forecast_df['day_of_week'].map(weekly_seasonality)
    
    # Calcular previsão base
    base_forecast = daily_revenue['ma7'].iloc[-1]
    
    # Aplicar tendência de crescimento e sazonalidade
    for i in range(len(forecast_df)):
        days_ahead = i + 1
        forecast_df.loc[i, 'forecast'] = base_forecast * forecast_df.loc[i, 'seasonality_factor'] + (growth_rate * days_ahead)
    
    # Calcular intervalo de confiança (simplificado)
    std_dev = daily_revenue['price'].std()
    forecast_df['lower_bound'] = forecast_df['forecast'] - (1.96 * std_dev)
    forecast_df['upper_bound'] = forecast_df['forecast'] + (1.96 * std_dev)
    
    return daily_revenue, forecast_df

def calculate_category_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calcula métricas por categoria de produto."""
    
    if len(df) < 10000:
        # Pandas implementation (original)
        df = df.copy()
        df['month'] = pd.to_datetime(df['order_purchase_timestamp']).dt.to_period('M')
        monthly_category_sales = df.groupby(['month', 'product_category_name']).agg({
            'price': 'sum',
            'order_id': pd.Series.nunique,
            'pedido_cancelado': 'mean'
        }).reset_index()
        monthly_category_sales['month'] = monthly_category_sales['month'].astype(str)
        
        category_profit = df.groupby('product_category_name').agg({
            'price': 'sum',
            'order_id': pd.Series.nunique
        }).reset_index()
        
        top_categories = df.groupby('product_category_name')['order_id'].nunique().sort_values(ascending=False).head(5).index.tolist()
    else:
        # DuckDB implementation
        try:
            db = get_db()
            db.conn.register('temp_cat_metrics', df)
            
            # Agregação mensal
            monthly_query = """
                SELECT 
                    strftime(date_trunc('month', order_purchase_timestamp), '%Y-%m') as month,
                    product_category_name,
                    SUM(price) as price,
                    COUNT(DISTINCT order_id) as order_id,
                    AVG(pedido_cancelado) as pedido_cancelado
                FROM temp_cat_metrics
                WHERE product_category_name IS NOT NULL
                GROUP BY 1, 2
            """
            monthly_category_sales = db.query(monthly_query)
            
            # Profit/Stats totais
            profit_query = """
                SELECT 
                    product_category_name,
                    SUM(price) as price,
                    COUNT(DISTINCT order_id) as order_id
                FROM temp_cat_metrics
                WHERE product_category_name IS NOT NULL
                GROUP BY 1
            """
            category_profit = db.query(profit_query)
            
            # Top categories (subquery ou pandas dps de agregar)
            top_query = """
                SELECT product_category_name 
                FROM temp_cat_metrics 
                WHERE product_category_name IS NOT NULL
                GROUP BY 1 
                ORDER BY COUNT(DISTINCT order_id) DESC 
                LIMIT 5
            """
            top_cat_df = db.query(top_query)
            top_categories = top_cat_df['product_category_name'].tolist() if not top_cat_df.empty else []
            
            db.conn.unregister('temp_cat_metrics')
        except Exception as e:
            # Fallback recurse
            return calculate_category_metrics.__wrapped__(df)

    # Lógica comum pós-agregação
    # Filtrar apenas as categorias principais
    top_category_sales = monthly_category_sales[monthly_category_sales['product_category_name'].isin(top_categories)]
    
    category_profit['avg_price'] = category_profit['price'] / category_profit['order_id']
    category_profit['profit_margin'] = 0.3  # Simulando margem de 30%
    category_profit['profit'] = category_profit['price'] * category_profit['profit_margin']
    
    # Calcular taxas de crescimento
    category_growth = {}
    for category in top_categories:
        category_data = top_category_sales[top_category_sales['product_category_name'] == category]
        if len(category_data) >= 2:
            first_month = category_data.iloc[0]['order_id']
            last_month = category_data.iloc[-1]['order_id']
            growth_rate = (last_month - first_month) / first_month * 100 if first_month > 0 else 0
            category_growth[category] = growth_rate
    
    return {
        'monthly_category_sales': monthly_category_sales,
        'top_categories': top_categories,
        'top_category_sales': top_category_sales,
        'category_profit': category_profit,
        'category_growth': category_growth
    }

def calculate_seasonality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calcula métricas de sazonalidade."""
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                  'July', 'August', 'September', 'October', 'November', 'December']

    if len(df) < 10000:
        # Pandas
        df = df.copy()
        df['day_of_week'] = pd.to_datetime(df['order_purchase_timestamp']).dt.day_name()
        day_revenue = df.groupby('day_of_week')['price'].sum().reindex(day_order)
        
        df['month'] = pd.to_datetime(df['order_purchase_timestamp']).dt.month_name()
        month_revenue = df.groupby('month')['price'].sum().reindex(month_order)
        
        state_ticket = df.groupby('customer_state')['price'].mean().sort_values(ascending=False)
    else:
        # DuckDB
        try:
            db = get_db()
            db.conn.register('temp_seasonality', df)
            
            # Sazonalidade Dia
            day_query = """
                SELECT dayname(order_purchase_timestamp) as day_of_week, SUM(price) as price
                FROM temp_seasonality
                GROUP BY 1
            """
            day_df = db.query(day_query)
            day_revenue = day_df.set_index('day_of_week')['price'].reindex(day_order)
            
            # Sazonalidade Mês
            month_query = """
                SELECT monthname(order_purchase_timestamp) as month, SUM(price) as price
                FROM temp_seasonality
                GROUP BY 1
            """
            month_df = db.query(month_query)
            month_revenue = month_df.set_index('month')['price'].reindex(month_order)
            
            # Estado
            state_query = """
                SELECT customer_state, AVG(price) as price
                FROM temp_seasonality
                GROUP BY 1
                ORDER BY 2 DESC
            """
            state_df = db.query(state_query)
            state_ticket = state_df.set_index('customer_state')['price']
            
            db.conn.unregister('temp_seasonality')
        except Exception:
            return calculate_seasonality_metrics.__wrapped__(df)
    
    return {
        'day_revenue': day_revenue,
        'month_revenue': month_revenue,
        'state_ticket': state_ticket
    }
