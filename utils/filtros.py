"""
Módulo de Filtros para Dashboard Streamlit - Versão Simplificada

Este módulo fornece uma estrutura simples e robusta para gerenciar filtros
usando chaves únicas no session_state.

NOTA - Fonte de dados:
- Por padrão apenas "Supabase (Nuvem)" é exibido (dados via API → Supabase).
- Para reativar "Dados Integrados (Local)" e "Dados do Cliente (Upload)", defina
  ENABLE_LOCAL_AND_UPLOAD=true no ambiente (sistema reutilizável para outros clientes).
"""
from __future__ import annotations
import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import base64

# Constantes globais para chaves dos filtros
PERIOD_OPTIONS = [
            "Todo o período",
            "Últimos 7 dias",
            "Último mês",
            "Mês Atual",
            "Últimos 2 meses",
            "Último trimestre",
            "Último semestre",
            "Último ano",
            "Últimos 2 anos"
]

# Fonte de dados: False = apenas Supabase (Nuvem); True = Local + Upload + Supabase (sistema reutilizável).
# Para outro cliente que comece com dados locais, defina ENABLE_LOCAL_AND_UPLOAD=true no ambiente.
ENABLE_LOCAL_AND_UPLOAD = os.getenv("ENABLE_LOCAL_AND_UPLOAD", "false").lower() in ("true", "1")

# Co-branding na sidebar (quando só Supabase está ativo): logo do cliente + "Insight Expert × Nome".
# SIDEBAR_CLIENT_LOGO: caminho para a imagem (ex: components/img/client_logo.svg ou .png). Vazio = tenta defaults.
# SIDEBAR_CLIENT_NAME: texto ao lado do "×" (ex: Dica de Madame).
# SIDEBAR_IE_LOGO_WIDTH / SIDEBAR_CLIENT_LOGO_WIDTH: largura em px dos SVGs (lado a lado no eixo diagonal).
SIDEBAR_CLIENT_LOGO = os.getenv("SIDEBAR_CLIENT_LOGO", "").strip()
SIDEBAR_CLIENT_NAME = os.getenv("SIDEBAR_CLIENT_NAME", "Dica de Madame").strip()
def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default
SIDEBAR_IE_LOGO_WIDTH = _int_env("SIDEBAR_IE_LOGO_WIDTH", 100)
SIDEBAR_CLIENT_LOGO_WIDTH = _int_env("SIDEBAR_CLIENT_LOGO_WIDTH", 100)
# Ordem de busca quando SIDEBAR_CLIENT_LOGO não está definido (primeiro existente vence)
SIDEBAR_CLIENT_LOGO_CANDIDATES = ("components/img/client_logo.svg", "components/img/client_logo.png")

def initialize_filters() -> None:
    """
    Inicializa os filtros a partir da URL ou valores padrão.
    Query parameters têm prioridade sobre session_state.
    """
    # Valores padrão
    defaults = {
        "periodo_analise": "Todo o período",
        "marketing_spend": 50000,
        "categoria_selecionada": "Todas as categorias",
        "pagina_atual": "Visão Geral",
        "dataset_selecionado": "Dados Integrados (Local)",
        "selected_marketplaces": []
    }
    
    # Ler filtros da URL (query parameters)
    query_filters = {}
    if "periodo" in st.query_params:
        # Validar se o período está na lista de opções ou é "Período personalizado"
        periodo_url = st.query_params["periodo"]
        if periodo_url in PERIOD_OPTIONS or periodo_url == "Período personalizado":
            query_filters["periodo_analise"] = periodo_url
    
    if "marketing" in st.query_params:
        try:
            marketing_url = int(st.query_params["marketing"])
            if marketing_url >= 0:  # Validação básica
                query_filters["marketing_spend"] = marketing_url
        except (ValueError, TypeError):
            pass  # Usar valor padrão se inválido
    
    if "categoria" in st.query_params:
        categoria_url = st.query_params["categoria"]
        # Validar se a categoria não está vazia
        if categoria_url and len(categoria_url.strip()) > 0:
            query_filters["categoria_selecionada"] = categoria_url
        else:
            # Remover parâmetro inválido da URL
            del st.query_params["categoria"]
    
    if "page" in st.query_params:
        pagina_url = st.query_params["page"]
        query_filters["pagina_atual"] = pagina_url
    
    if "dataset" in st.query_params:
        dataset_url = st.query_params["dataset"]
        # Validar se o dataset não está vazio
        if dataset_url and len(dataset_url.strip()) > 0:
            query_filters["dataset_selecionado"] = dataset_url

    # Novo: marketplaces via URL (CSV ou único)
    if "marketplaces" in st.query_params:
        try:
            raw = st.query_params["marketplaces"]
            if isinstance(raw, list):
                raw = raw[0]
            val = str(raw).strip()
            if val:
                from urllib.parse import unquote
                decoded = unquote(val)
                # Se tiver vírgula, trata como CSV (legado), senão item único
            if "," in decoded:
                parsed = [m for m in decoded.split(",") if m.strip()]
                query_filters["selected_marketplaces"] = parsed
            else:
                query_filters["selected_marketplaces"] = [decoded]
        except Exception:
            pass
    
    # Novo: transportadoras via URL (CSV ou único)
    if "carriers" in st.query_params:
        try:
            raw = st.query_params["carriers"]
            if isinstance(raw, list):
                raw = raw[0]
            val = str(raw).strip()
            if val:
                from urllib.parse import unquote
                decoded = unquote(val)
                if "," in decoded:
                    parsed = [c for c in decoded.split(",") if c.strip()]
                    query_filters["selected_carriers"] = parsed
                else:
                    query_filters["selected_carriers"] = [decoded]
        except Exception:
            pass
    
    # Novo: suporte para datas personalizadas via URL (padrão DD-MM-YYYY; aceita também YYYY-MM-DD)
    if "start_date" in st.query_params and "end_date" in st.query_params:
        try:
            start_str = st.query_params["start_date"].strip()
            end_str = st.query_params["end_date"].strip()
            for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
                try:
                    start_date = datetime.strptime(start_str, fmt).date()
                    end_date = datetime.strptime(end_str, fmt).date()
                    query_filters["custom_start_date"] = start_date
                    query_filters["custom_end_date"] = end_date
                    break
                except ValueError:
                    continue
        except (ValueError, TypeError, AttributeError):
            pass  # Ignorar datas inválidas
    
    # Aplicar valores: URL > session_state > padrão
    for key, default_value in defaults.items():
        if key in query_filters:
            # Query param tem prioridade
            st.session_state[key] = query_filters[key]
        else:
            # Usar session_state existente ou padrão
            st.session_state.setdefault(key, default_value)
    
    # Aplicar datas personalizadas separadamente (não têm valores padrão)
    if "custom_start_date" in query_filters:
        st.session_state.custom_start_date = query_filters["custom_start_date"]
    if "custom_end_date" in query_filters:
        st.session_state.custom_end_date = query_filters["custom_end_date"]
    
    # Manter compatibilidade com código existente (temporário)
    st.session_state.current_page = st.session_state.pagina_atual
    st.session_state.filter_period = st.session_state.periodo_analise
    st.session_state.filter_marketing = st.session_state.marketing_spend
    st.session_state.selected_category = st.session_state.categoria_selecionada
    # Garantir lista de marketplaces sempre presente
    if "selected_marketplaces" not in st.session_state:
        st.session_state.selected_marketplaces = []

def apply_sidebar_background(image_path: str = "components/img/sidebar_background.png", overlay_opacity: float = 0.35) -> None:
    """Aplica uma imagem de fundo na barra lateral via CSS (base64 embed).

    Silenciosamente não faz nada se a imagem não puder ser lida.
    """
    try:
        with open(image_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")

        st.sidebar.markdown(
            f"""
            <style>
            [data-testid="stSidebar"] {{
                position: relative;
                background: transparent;
            }}
            [data-testid="stSidebar"]::before {{
                content: "";
                position: absolute;
                inset: 0;
                background-image: url('data:image/png;base64,{encoded}');
                background-size: cover;
                background-position: center center;
                background-repeat: no-repeat;
                opacity: 1;
                z-index: -1;
                pointer-events: none;
            }}
            [data-testid="stSidebar"]::after {{
                content: "";
                position: absolute;
                inset: 0;
                background: linear-gradient(180deg, rgba(15,23,42,{overlay_opacity}), rgba(15,23,42,{overlay_opacity}));
                z-index: -1;
                pointer-events: none;
            }}
            [data-testid="stSidebar"] > div {{
                position: relative;
                z-index: 1;
            }}
            /* Preservar a funcionalidade de redimensionamento */
            [data-testid="stSidebar"] .css-1d391kg {{
                pointer-events: auto !important;
                z-index: 999 !important;
            }}
            /* Garantir que controles de redimensionamento funcionem */
            [data-testid="stSidebar"] [data-testid="stSidebarNav"] {{
                pointer-events: auto !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
    except Exception:
        # Falha silenciosa para não quebrar a UI caso a imagem não exista
        pass

def _first_day_of_month(d: pd.Timestamp) -> pd.Timestamp:
    """Retorna o primeiro dia do mês à meia-noite."""
    return pd.Timestamp(year=d.year, month=d.month, day=1)

def _last_day_of_month(d: pd.Timestamp) -> pd.Timestamp:
    """Retorna o último dia do mês (23:59:59.999999)."""
    if d.month == 12:
        next_first = pd.Timestamp(year=d.year + 1, month=1, day=1)
    else:
        next_first = pd.Timestamp(year=d.year, month=d.month + 1, day=1)
    return next_first - pd.Timedelta(microseconds=1)

def get_date_range(periodo: str, df: pd.DataFrame) -> Optional[List[datetime]]:
    """
    Calcula o intervalo de datas baseado no período selecionado.
    
    - "Últimos 7 dias": últimos 7 dias corridos.
    - "Último mês": mês calendário anterior (do dia 1 ao último dia).
    - "Mês Atual": do dia 1 do mês atual até a data máxima dos dados.
    - "Último ano": ano civil anterior completo (1 jan a 31 dez).
    - "Últimos 2 anos": dois anos civis anteriores (1 jan do ano-2 a 31 dez do ano-1).
    - Demais: janelas rolantes em dias.
    
    Args:
        periodo: Período selecionado pelo usuário
        df: DataFrame contendo os dados
        
    Returns:
        Lista com data inicial e final, ou None se for "Todo o período"
    """
    max_date = pd.to_datetime(df['order_purchase_timestamp']).max()
    # Normalizar para date-like (meia-noite) para cálculos de mês
    hoje = max_date.normalize() if hasattr(max_date, 'normalize') else pd.Timestamp(max_date.date())
    
    if periodo == "Todo o período":
        return None
    
    if periodo == "Últimos 7 dias":
        return [hoje - timedelta(days=6), hoje]
    
    if periodo == "Último mês":
        primeiro_ultimo_mes = hoje.replace(day=1) - pd.Timedelta(days=1)
        inicio = _first_day_of_month(primeiro_ultimo_mes)
        fim = _last_day_of_month(primeiro_ultimo_mes)
        return [inicio, fim]
    
    if periodo == "Mês Atual":
        inicio = _first_day_of_month(hoje)
        return [inicio, hoje]
    
    # Último ano = ano civil anterior completo (1 jan a 31 dez)
    if periodo == "Último ano":
        ano_anterior = hoje.year - 1
        inicio = pd.Timestamp(year=ano_anterior, month=1, day=1)
        fim = _last_day_of_month(pd.Timestamp(year=ano_anterior, month=12, day=1))
        return [inicio, fim]
    
    # Últimos 2 anos = dois anos civis anteriores (1 jan do ano-2 a 31 dez do ano-1)
    if periodo == "Últimos 2 anos":
        inicio = pd.Timestamp(year=hoje.year - 2, month=1, day=1)
        fim = _last_day_of_month(pd.Timestamp(year=hoje.year - 1, month=12, day=1))
        return [inicio, fim]
    
    date_ranges_days = {
        "Últimos 2 meses": 60,
        "Último trimestre": 90,
        "Último semestre": 180,
    }
    days = date_ranges_days.get(periodo)
    if days is None:
        return None
    return [hoje - timedelta(days=days), hoje]


def _parse_date_flexible(s: Any) -> Optional[pd.Timestamp]:
    """Converte string ou outro valor para Timestamp. Aceita DD-MM-YYYY, YYYY-MM-DD e formatos ISO."""
    if s is None:
        return None
    if isinstance(s, (datetime, pd.Timestamp)):
        return pd.Timestamp(s)
    if hasattr(s, "year"):
        return pd.Timestamp(s)
    if not isinstance(s, str):
        return pd.to_datetime(s, errors="coerce")
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return pd.Timestamp(datetime.strptime(s, fmt))
        except ValueError:
            continue
    return pd.to_datetime(s, errors="coerce")


def _normalize_date_range(date_range: Any) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Normaliza date_range (start,end) para timestamps, com end inclusivo.

    Aceita date_range vindo do calendário (date/datetime/str). Strings em DD-MM-YYYY ou YYYY-MM-DD.
    Retorna None se inválido.
    """
    if not date_range or not isinstance(date_range, (list, tuple)) or len(date_range) < 2:
        return None
    start, end = date_range[0], date_range[1]
    try:
        start_ts = _parse_date_flexible(start) if isinstance(start, str) else pd.to_datetime(start, errors="coerce")
        end_ts = _parse_date_flexible(end) if isinstance(end, str) else pd.to_datetime(end, errors="coerce")
        if start_ts is None or end_ts is None or pd.isna(start_ts) or pd.isna(end_ts):
            return None
        start_ts = pd.Timestamp(start_ts)
        end_ts = pd.Timestamp(end_ts)
        # Se vier como "date" (sem hora), tornar o end inclusivo até o fim do dia
        if getattr(end_ts, "hour", 0) == 0 and getattr(end_ts, "minute", 0) == 0:
            end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        return start_ts, end_ts
    except Exception:
        return None


def _coerce_epoch_to_datetime(series: pd.Series) -> pd.Series:
    """Converte série (epoch ms/s/ns ou strings) para datetime (naive), de forma robusta."""
    if series is None:
        return pd.to_datetime(pd.Series([], dtype="datetime64[ns]"))
    s = series
    # Numérico (epoch)
    if pd.api.types.is_numeric_dtype(s):
        v = pd.to_numeric(s, errors="coerce")
        vv = v.dropna()
        if vv.empty:
            return pd.to_datetime(v, errors="coerce")
        med = float(vv.median())
        # Heurística de unidade
        if med > 1e17:
            unit = "ns"
        elif med > 1e14:
            unit = "us"
        elif med > 1e11:
            unit = "ms"
        else:
            unit = "s"
        dt = pd.to_datetime(v, unit=unit, errors="coerce", utc=True)
        return dt.dt.tz_convert(None)
    # Strings / datetime
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    try:
        return dt.dt.tz_convert(None)
    except Exception:
        return pd.to_datetime(s, errors="coerce")


def filter_reviews_by_period(reviews_df: pd.DataFrame, date_range: Any) -> pd.DataFrame:
    """Aplica filtro de período em reviews usando colunas de data disponíveis.

    Prioriza 'review_date' (que pode estar em epoch ms), e cai para
    'order_purchase_timestamp' se existir.
    """
    if reviews_df is None or reviews_df.empty:
        return reviews_df.copy() if isinstance(reviews_df, pd.DataFrame) else pd.DataFrame()
    norm = _normalize_date_range(date_range)
    if norm is None:
        return reviews_df.copy()
    start_ts, end_ts = norm

    df = reviews_df.copy()
    date_col = None
    if "review_date" in df.columns:
        date_col = "review_date"
        df["_review_dt"] = _coerce_epoch_to_datetime(df["review_date"])
    elif "order_purchase_timestamp" in df.columns:
        date_col = "order_purchase_timestamp"
        df["_review_dt"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    else:
        return df

    df = df[df["_review_dt"].notna()]
    df = df[(df["_review_dt"] >= start_ts) & (df["_review_dt"] <= end_ts)].copy()
    df.drop(columns=["_review_dt"], inplace=True, errors="ignore")
    return df

def filter_dataframe_by_carrier(df: pd.DataFrame, selected_carriers: List[str]) -> pd.DataFrame:
    """Aplica filtro de transportadora ao DataFrame."""
    if not selected_carriers or "transportadoraNome" not in df.columns:
        return df
    return df[df["transportadoraNome"].isin(selected_carriers)]


def coerce_review_date_to_datetime(series: pd.Series) -> pd.Series:
    """API pública: converte 'review_date' (epoch ms/s/ns ou string) para datetime naive."""
    return _coerce_epoch_to_datetime(series)

def update_url_with_filters(page: Optional[str] = None) -> None:
    """
    Atualiza a URL com os filtros atuais do session_state.
    Permite compartilhamento de links e persistência natural.
    """
    query_params = {}
    
    # Página atual (resiliente a ausência de chave no início do app)
    try:
        current_page = page or st.session_state.get("pagina_atual", "Visão Geral")
        query_params["page"] = current_page
    except Exception:
        query_params["page"] = "Visão Geral"
    
    # Filtros apenas se diferentes dos padrões
    if st.session_state.get("periodo_analise", "Todo o período") != "Todo o período":
        query_params["periodo"] = st.session_state.get("periodo_analise")
    
    if st.session_state.get("marketing_spend", 50000) != 50000:
        query_params["marketing"] = str(st.session_state.get("marketing_spend"))
    
    if st.session_state.get("categoria_selecionada", "Todas as categorias") != "Todas as categorias":
        query_params["categoria"] = st.session_state.get("categoria_selecionada")
    
    if st.session_state.get("dataset_selecionado", "Dados Integrados (Local)") != "Dados Integrados (Local)":
        query_params["dataset"] = st.session_state.get("dataset_selecionado")

    # Persistir escolha de dataset na URL
    dataset_choice = st.session_state.get("dataset_choice", "local")
    if dataset_choice in ["upload", "local", "supabase"]:
        query_params["data"] = dataset_choice
        
        # Se for upload e houver caminho do arquivo processado, incluir na URL
        if dataset_choice == "upload" and "processed_file_path" in st.session_state:
            try:
                from urllib.parse import quote
                query_params["parquet"] = quote(st.session_state.processed_file_path)
            except Exception:
                pass
    # Demo Olist desativada (mantido comentado para referência)
    # elif dataset_choice == "olist":
    #     query_params["data"] = dataset_choice

    # Novo: persistir marketplaces na URL quando houver seleção
    selected_marketplaces = st.session_state.get("selected_marketplaces", [])
    if isinstance(selected_marketplaces, list) and len(selected_marketplaces) > 0:
        try:
            from urllib.parse import quote
            # Se for apenas 1, salva direto. Se mais, CSV.
            if len(selected_marketplaces) == 1:
                query_params["marketplaces"] = quote(selected_marketplaces[0])
            else:
                csv_val = ",".join(selected_marketplaces)
                query_params["marketplaces"] = quote(csv_val)
        except Exception:
            pass

    # Novo: persistir transportadoras na URL quando houver seleção
    selected_carriers = st.session_state.get("selected_carriers", [])
    if isinstance(selected_carriers, list) and len(selected_carriers) > 0:
        try:
            from urllib.parse import quote
            if len(selected_carriers) == 1:
                query_params["carriers"] = quote(selected_carriers[0])
            else:
                csv_val = ",".join(selected_carriers)
                query_params["carriers"] = quote(csv_val)
        except Exception:
            pass
    
    # Novo: persistir datas personalizadas na URL quando período for personalizado
    if st.session_state.get("periodo_analise") == "Período personalizado":
        custom_start = st.session_state.get("custom_start_date")
        custom_end = st.session_state.get("custom_end_date")
        if custom_start and custom_end:
            query_params["start_date"] = custom_start.strftime('%d-%m-%Y')
            query_params["end_date"] = custom_end.strftime('%d-%m-%Y')
    
    # Atualizar query_params do Streamlit
    for key, value in query_params.items():
        st.query_params[key] = value
    
    # Remover parâmetros que voltaram ao padrão
    params_to_remove = []
    for key in st.query_params.keys():
        if key not in query_params:
            params_to_remove.append(key)
    
    for key in params_to_remove:
        if key != "page":  # Sempre manter a página
            del st.query_params[key]

# Callbacks para atualizar URL quando filtros mudarem
def on_periodo_change():
    """Callback para atualizar URL quando período mudar."""
    update_url_with_filters()

def on_marketing_change():
    """Callback para atualizar URL quando marketing mudar."""
    update_url_with_filters()

def on_categoria_change():
    """Callback para atualizar URL quando categoria mudar."""
    update_url_with_filters()

def on_marketplace_change():
    """Callback para atualizar URL quando marketplace mudar."""
    try:
        selected = st.session_state.get("marketplace_filter", "Todos os Marketplaces")
        all_label = "Todos os Marketplaces"
        
        if selected == all_label:
            st.session_state.selected_marketplaces = []
        else:
            # Armazenar como lista para compatibilidade com .isin()
            st.session_state.selected_marketplaces = [selected]
    except Exception:
        st.session_state.selected_marketplaces = []
    update_url_with_filters()

def on_carrier_change():
    """Callback para atualizar URL quando transportadora mudar."""
    try:
        selected = st.session_state.get("carrier_filter", "Todas as Transportadoras")
        all_label = "Todas as Transportadoras"
        
        if selected == all_label:
            st.session_state.selected_carriers = []
        else:
            st.session_state.selected_carriers = [selected]
    except Exception:
        st.session_state.selected_carriers = []
    update_url_with_filters()

def on_dataset_change():
    """Callback para atualizar URL quando dataset mudar."""
    # Atualizar session_state com a seleção atual do radio button
    if "dataset_radio" in st.session_state:
        st.session_state.dataset_selecionado = st.session_state.dataset_radio
    update_url_with_filters()

def on_dataset_source_change():
    """Callback para atualizar URL quando fonte de dados mudar."""
    if "dataset_source_selector" in st.session_state:
        selected_option = st.session_state.dataset_source_selector
        dataset_options = {
            "Dados Integrados (Local)": "local",
            "Dados do Cliente (Upload)": "upload",
            # "Dados Olist (Demo)": "olist"  # desativado
        }
        st.session_state.dataset_choice = dataset_options.get(selected_option, "local")
    update_url_with_filters()

def render_sidebar_filters(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Renderiza todos os filtros na sidebar com persistência via URL.
    Adapta-se automaticamente para modo franquia vs modo rede.
    
    Args:
        df: DataFrame com os dados para análise
        
    Returns:
        Dicionário com os valores dos filtros e date_range
    """
    # Verificar se é modo franquia
    franchise_mode = 'franchise' in st.query_params
    
    if franchise_mode:
        return render_franchise_filters(df)
    else:
        return render_network_filters(df)

def render_marketplace_filter(df: pd.DataFrame) -> List[str]:
    """
    Renderiza filtro de marketplace na sidebar.
    
    Args:
        df: DataFrame contendo a coluna 'marketplace'
        
    Returns:
        Lista de marketplaces selecionados. Se 'Todos' for selecionado, retorna lista vazia.
    """
    
    if "marketplace" not in df.columns or df["marketplace"].dropna().empty:
        st.sidebar.info("Nenhum marketplace disponível para filtrar.")
        return []
    
    # Obter marketplaces disponíveis
    marketplaces = sorted([m for m in df["marketplace"].unique() if pd.notnull(m)])
    all_label = "Todos os Marketplaces"
    
    # Inicializar estado se não existir
    if "selected_marketplaces" not in st.session_state:
        st.session_state.selected_marketplaces = []
    
    # Determinar seleção padrão com base no estado atual
    current_selection_list = st.session_state.get("selected_marketplaces", [])
    # Se houver seleção, pega o primeiro item (modo single select), senão 'Todos'
    current_val = current_selection_list[0] if current_selection_list and current_selection_list[0] in marketplaces else all_label
    
    options = [all_label] + marketplaces
    try:
        default_index = options.index(current_val)
    except ValueError:
        default_index = 0

    # Renderizar selectbox (única seleção)
    selected = st.sidebar.selectbox(
        "Selecione o marketplace:",
        options=options,
        index=default_index,
        key="marketplace_filter",
        help="Selecione um marketplace para análise.",
        on_change=on_marketplace_change
    )
    
    # Lógica de seleção
    if selected == all_label:
        # Atualizar estado e retornar vazio
        st.session_state.selected_marketplaces = []
        return []
    else:
        # Atualizar estado com a seleção válida (como lista de 1 elemento)
        st.session_state.selected_marketplaces = [selected]
        return [selected]

def render_carrier_filter(df: pd.DataFrame) -> List[str]:
    """
    Renderiza filtro de transportadora na sidebar.
    
    Args:
        df: DataFrame contendo a coluna 'transportadoraNome'
        
    Returns:
        Lista de transportadoras selecionadas. Se 'Todas' for selecionado, retorna lista vazia.
    """
    # Verificar se a coluna existe e tem dados
    col_name = "transportadoraNome"
    
    # Fallback para carrier_name se necessário
    if col_name not in df.columns and "carrier_name" in df.columns:
        col_name = "carrier_name"
        
    if col_name not in df.columns:
        # Debug temporário: Avisar se a coluna falta (provável necessidade de re-rodar pipeline)
        # st.sidebar.warning("⚠️ Coluna 'transportadoraNome' ausente. Re-rode o pipeline.")
        return []
    
    if df[col_name].dropna().empty:
        return []
    
    # Obter transportadoras disponíveis
    carriers = sorted([str(c) for c in df[col_name].unique() if pd.notnull(c) and str(c).strip() != ""])
    all_label = "Todas as Transportadoras"
    
    if not carriers:
        return []
    
    # Inicializar estado se não existir
    if "selected_carriers" not in st.session_state:
        st.session_state.selected_carriers = []
    
    # Determinar seleção padrão
    current_selection_list = st.session_state.get("selected_carriers", [])
    current_val = current_selection_list[0] if current_selection_list and current_selection_list[0] in carriers else all_label
    
    options = [all_label] + carriers
    try:
        default_index = options.index(current_val)
    except ValueError:
        default_index = 0

    # Renderizar selectbox
    selected = st.selectbox(
        "Selecione a transportadora:",
        options=options,
        index=default_index,
        key="carrier_filter",
        help="Selecione uma transportadora para análise.",
        on_change=on_carrier_change
    )
    
    # Lógica de seleção
    if selected == all_label:
        st.session_state.selected_carriers = []
        return []
    else:
        st.session_state.selected_carriers = [selected]
        return [selected]

def apply_custom_sidebar_style() -> None:
    """Aplica estilo personalizado aos componentes da sidebar."""
    st.sidebar.markdown("""
    <style>
    div[data-baseweb="select"] {
        margin-top: 10px;
        margin-bottom: 15px;
    }
    div[data-baseweb="select"] > div {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%) !important;
        border: 1px solid rgba(148, 163, 184, 0.3) !important;
        border-radius: 12px !important;
        backdrop-filter: blur(16px) !important;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
        transition: all 0.3s ease !important;
        color: #e2e8f0 !important;
    }
    div[data-baseweb="select"] > div:hover {
        border-color: rgba(99, 102, 241, 0.6) !important;
        box-shadow: 0 6px 20px rgba(99, 102, 241, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.15) !important;
        transform: translateY(-1px);
    }
    div[data-baseweb="select"] svg {
        fill: #94a3b8 !important;
    }
    </style>
    """, unsafe_allow_html=True)

def render_network_filters(df: pd.DataFrame) -> Dict[str, Any]:
    """Renderiza filtros para modo rede com hierarquia visual e expanders."""
    
    # Aplicar estilo personalizado
    apply_custom_sidebar_style()
    
    # 1. Filtro de Período (Prioridade Máxima)
    from components.calendar_filter import render_calendar_sidebar_section
    periodo, date_range_calendar = render_calendar_sidebar_section()
    date_range = date_range_calendar if date_range_calendar else get_date_range(periodo, df)
    
    # 2. Filtro de Marketplace
    selected_marketplaces = render_marketplace_filter(df)
    
    # 3. Filtro de Categoria (Movido para Global)
    selected_category = render_category_filter(df)
    
    # Inicializar variáveis de controle
    simulate_margins = False
    use_net_revenue = st.session_state.get("use_net_revenue", False)
    marketing_spend = st.session_state.get("marketing_spend", 50000)
    selected_carriers = []

    # 4. Filtros Operacionais (Expander)
    with st.sidebar.expander("🚚 Filtros Operacionais", expanded=False):
        # Renderizar filtro de transportadora
        selected_carriers = render_carrier_filter(df)
    
    # 5. Simulação & Financeiro (Expander)
    with st.sidebar.expander("💰 Simulação & Financeiro", expanded=False):
        # Renderizar gasto com marketing com callback para URL
        marketing_spend = st.number_input(
            "Total Gasto com Marketing",
            min_value=0,
            value=marketing_spend,
            step=1000,
            key="marketing_spend",
            on_change=on_marketing_change
        )
        
        # Checkbox: Simular margens
        simulate_margins = st.checkbox(
            "Simular margens realistas (demo)",
            value=False,
            help="Calcula colunas de margem a partir de parâmetros típicos caso não existam no dataset."
        )
        
        # Checkbox: Receita Líquida
        use_net_revenue = st.checkbox(
            "Usar Receita Líquida (deduzida)",
            value=use_net_revenue,
            help="Substitui 'price' por 'margin_net_revenue' (quando existir) para refletir comissões/taxas."
        )
    
    # Manter compatibilidade com código existente (temporário)
    st.session_state.filter_period = periodo
    st.session_state.filter_marketing = marketing_spend
    st.session_state.use_net_revenue = use_net_revenue
    
    # Montar resultados no formato esperado pelo código existente
    results = {
        "Período de Análise": periodo,
        "Total Gasto com Marketing": marketing_spend,
        "date_range": date_range,
        "franchise_mode": False,
        "selected_marketplaces": selected_marketplaces,
        "selected_carriers": selected_carriers,
        "simulate_margins": simulate_margins,
        "use_net_revenue": use_net_revenue,
        "categoria_selecionada": selected_category
    }
    
    return results

def render_franchise_filters(df: pd.DataFrame) -> Dict[str, Any]:
    """Renderiza filtros específicos para modo franquia."""
    st.sidebar.markdown("### 🏪 Filtros da Franquia")
    
    # Período (mantém igual ao da rede)
    periodo = st.sidebar.selectbox(
        "Período de Análise",
        options=PERIOD_OPTIONS,
        index=PERIOD_OPTIONS.index(st.session_state.get('periodo_analise', 'Todo o período')),
        key="franchise_periodo_analise"
    )
    
    # Gasto com marketing (específico da franquia)
    marketing_spend = st.sidebar.number_input(
        "Gasto com Marketing da Franquia",
        min_value=0,
        value=st.session_state.get('franchise_marketing_spend', 10000),
        step=500,
        key="franchise_marketing_spend",
        help="Investimento em marketing específico desta franquia"
    )
    
    # Filtros específicos de franquia farmacêutica
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💊 Filtros Farmacêuticos")
    
    # Categorias terapêuticas
    if 'product_category_name' in df.columns:
        therapeutic_categories = ['Todas as categorias'] + sorted(df['product_category_name'].unique().tolist())
        selected_category = st.sidebar.selectbox(
            "Categoria Terapêutica",
            options=therapeutic_categories,
            key="franchise_therapeutic_category"
        )
    else:
        selected_category = "Todas as categorias"
    
    # Forma de pagamento
    if 'payment_type' in df.columns:
        payment_types = ['Todos os tipos'] + sorted(df['payment_type'].unique().tolist())
        selected_payment = st.sidebar.multiselect(
            "Formas de Pagamento",
            options=payment_types[1:],  # Excluir "Todos os tipos"
            default=payment_types[1:],  # Selecionar todos por padrão
            key="franchise_payment_types"
        )
    else:
        selected_payment = []
    
    # Faixa de ticket
    if 'payment_value' in df.columns:
        min_ticket = float(df['payment_value'].min())
        max_ticket = float(df['payment_value'].max())
        
        ticket_range = st.sidebar.slider(
            "Faixa de Ticket",
            min_value=min_ticket,
            max_value=max_ticket,
            value=(min_ticket, max_ticket),
            format="R$ %.2f",
            key="franchise_ticket_range"
        )
    else:
        ticket_range = (0, 1000)
    
    # Campanhas (se disponível)
    if 'campaign_id' in df.columns:
        campaigns = ['Todas as campanhas'] + sorted(df['campaign_id'].dropna().unique().tolist())
        selected_campaigns = st.sidebar.multiselect(
            "Campanhas Ativas",
            options=campaigns[1:],
            key="franchise_campaigns"
        )
    else:
        selected_campaigns = []
    
    # Informações da franquia (somente leitura)
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ Informações da Franquia")
    
    if not df.empty:
        franchise_info = df.iloc[0]
        st.sidebar.info(f"**Brick:** {franchise_info.get('franchise_brick', 'N/A')}")
        st.sidebar.info(f"**Região:** {franchise_info.get('franchise_region', 'N/A')}")
        st.sidebar.info(f"**Store ID:** {franchise_info.get('franchise_store_id', 'N/A')}")
    
    # Montar resultados
    results = {
        "Período de Análise": periodo,
        "Total Gasto com Marketing": marketing_spend,
        "date_range": get_date_range(periodo, df),
        "categoria_selecionada": selected_category,
        "payment_types": selected_payment,
        "ticket_range": ticket_range,
        "campanhas": selected_campaigns,
        "franchise_mode": True
    }
    
    return results

def render_category_filter(df: pd.DataFrame, allow_multiple: bool = False) -> Optional[str]:
    """
    Renderiza filtro de categoria na sidebar com persistência via URL.
    
    Args:
        df: DataFrame contendo a coluna *product_category_name*.
        allow_multiple: Quando True usa *multiselect* e devolve `List[str]`;
            caso contrário usa *selectbox* e devolve `str`.

    Returns:
        Seleção do usuário (None se não houver categorias).
    """

    if "product_category_name" not in df.columns or df["product_category_name"].dropna().empty:
        st.sidebar.info("Nenhuma categoria disponível para filtrar.")
        return None

    # Obter categorias disponíveis (com higienização de valores inválidos)
    cat_series = df["product_category_name"].astype(str).str.strip()
    invalid_mask = cat_series.str.lower().isin({"", "nan", "none", "null", "0"})
    cat_series = cat_series.mask(invalid_mask, "Sem Categoria")
    categorias = sorted(cat_series.unique().tolist())
    all_label = "Todas as categorias"
    options = [all_label] + categorias

    # Inicializar o widget key se não existir
    if "categoria_widget_key" not in st.session_state:
        st.session_state.categoria_widget_key = "categoria_selecionada"

    # Garantir valor inicial válido antes de criar o widget
    if st.session_state.categoria_widget_key not in st.session_state:
        st.session_state[st.session_state.categoria_widget_key] = all_label

    # Validar categoria atual
    try:
        categoria_atual = st.session_state[st.session_state.categoria_widget_key]
        # Validar se a categoria é válida e não está vazia
        if not categoria_atual or not isinstance(categoria_atual, str) or categoria_atual.strip() == "" or categoria_atual not in options:
            st.session_state[st.session_state.categoria_widget_key] = all_label
            categoria_atual = all_label
            # Limpar URL se categoria inválida
            if "categoria" in st.query_params:
                del st.query_params["categoria"]
    except Exception:
        st.session_state[st.session_state.categoria_widget_key] = all_label
        categoria_atual = all_label

    try:
        if allow_multiple:
            # Versão com multiselect (menos comum)
            selecionadas = st.sidebar.multiselect(
                "Selecione as categorias",
                options=options,
                default=[categoria_atual],
                key=f"{st.session_state.categoria_widget_key}_multi",
                on_change=on_categoria_change,
                help="Selecione uma ou mais categorias para análise"
            )
            selected = selecionadas[0] if selecionadas else all_label
        else:
            # Versão com selectbox (mais comum)
            selected = st.sidebar.selectbox(
                "Selecione a categoria:",
                options=options,
                index=options.index(categoria_atual),
                key=st.session_state.categoria_widget_key,
                on_change=on_categoria_change
            )

        # Manter compatibilidade com código existente
        st.session_state.selected_category = selected
        
        return selected

    except Exception as e:
        st.error(f"Erro ao renderizar filtro de categoria: {str(e)}")
        return all_label

def initialize_dataset_selection() -> None:
    """
    Inicializa a seleção de dataset a partir da URL ou valores padrão.
    Tratado separadamente dos outros filtros para melhor controle.
    """
    local_default_path = "data/processed/pedidos.parquet"
    if ENABLE_LOCAL_AND_UPLOAD:
        default_dataset = "Dados Integrados (Local)"
        default_choice = "local"
    else:
        default_dataset = "Supabase (Nuvem)"
        default_choice = "supabase"
    
    # Verificar se há parâmetro de dataset ou indicador de upload/local na URL
    if not ENABLE_LOCAL_AND_UPLOAD:
        # Modo só Supabase: forçar escolha e ignorar data=local/upload na URL
        st.session_state.dataset_selecionado = default_dataset
        st.session_state.dataset_choice = default_choice
    elif "data" in st.query_params and st.query_params["data"] == "upload":
        # Forçar modo upload
        st.session_state.dataset_selecionado = "Dataset Cliente"
        st.session_state.dataset_choice = "upload"
        # Se vier o caminho do parquet na URL, persistir em sessão
        parquet_url = st.query_params.get("parquet")
        if parquet_url and 'processed_file_path' not in st.session_state:
            from urllib.parse import unquote
            st.session_state.processed_file_path = unquote(parquet_url)
    elif "data" in st.query_params and st.query_params["data"] == "local":
        # Forçar modo local
        st.session_state.dataset_selecionado = "Dados Integrados (Local)"
        st.session_state.dataset_choice = "local"
        # Garantir que o caminho padrão esteja disponível para o app.py
        st.session_state.setdefault("processed_file_path", local_default_path)
    elif "data" in st.query_params and st.query_params["data"] == "olist":
        # Demo Olist desativada: redirecionar para local
        st.session_state.dataset_selecionado = "Dados Integrados (Local)"
        st.session_state.dataset_choice = "local"
        st.session_state.setdefault("processed_file_path", local_default_path)
    elif "data" in st.query_params and st.query_params["data"] == "supabase":
        # Modo Supabase via URL
        st.session_state.dataset_selecionado = "Supabase (Nuvem)"
        st.session_state.dataset_choice = "supabase"
    elif "dataset" in st.query_params:
        dataset_url = st.query_params["dataset"]
        # Validar se o dataset está nas opções válidas
        valid_datasets = ["Dados Integrados (Local)", "Dataset Cliente", "Supabase (Nuvem)"]
        if dataset_url in valid_datasets:
            st.session_state.dataset_selecionado = dataset_url
            # Mapear para dataset_choice
            if dataset_url == "Dados Integrados (Local)":
                st.session_state.dataset_choice = "local"
                st.session_state.setdefault("processed_file_path", local_default_path)
            elif dataset_url == "Supabase (Nuvem)":
                st.session_state.dataset_choice = "supabase"
            else:
                st.session_state.dataset_choice = "upload"
        else:
            # Se inválido, usar padrão
            st.session_state.dataset_selecionado = default_dataset
            st.session_state.dataset_choice = default_choice
            if default_choice == "local":
                st.session_state.setdefault("processed_file_path", local_default_path)
    else:
        # Usar session_state existente ou padrão
        st.session_state.setdefault("dataset_selecionado", default_dataset)
        st.session_state.setdefault("dataset_choice", default_choice)
        if st.session_state.get("dataset_choice") == "local":
            st.session_state.setdefault("processed_file_path", local_default_path)

def select_dataset() -> str:
    """
    Renderiza sistema de seleção de dataset na sidebar.
    Se ENABLE_LOCAL_AND_UPLOAD=False (padrão): apenas Supabase (Nuvem).
    Se True: Dados Integrados (Local) + Dados do Cliente (Upload) + Supabase.
    Returns:
        'supabase' | 'local' | 'upload' conforme a fonte selecionada.
    """
    # Aplicar background da sidebar (imagem metálica)
    apply_sidebar_background()

    # Topo da sidebar: um logo ou os dois lado a lado no eixo diagonal (co-branding)
    if not ENABLE_LOCAL_AND_UPLOAD and SIDEBAR_CLIENT_NAME:
        client_logo_path = SIDEBAR_CLIENT_LOGO
        if not client_logo_path or not os.path.isfile(client_logo_path):
            for candidate in SIDEBAR_CLIENT_LOGO_CANDIDATES:
                if os.path.isfile(candidate):
                    client_logo_path = candidate
                    break
            else:
                client_logo_path = None
        has_client_logo = client_logo_path and os.path.isfile(client_logo_path)
        ie_logo_path = "components/img/logo3.svg"
        w_ie, w_cl = SIDEBAR_IE_LOGO_WIDTH, SIDEBAR_CLIENT_LOGO_WIDTH
        if has_client_logo and os.path.isfile(ie_logo_path):
            # Dois logos lado a lado: cliente (Dica de Madame) mais alto no eixo Y, IE abaixo (plataforma sustentando)
            st.sidebar.markdown(
                '<style>'
                '[data-testid="stSidebar"] div[data-testid="stImage"]:nth-of-type(1) img { margin-top: 14px; } '
                '[data-testid="stSidebar"] div[data-testid="stImage"]:nth-of-type(2) img { margin-top: -50px; }'
                '</style>',
                unsafe_allow_html=True,
            )
            col_ie, col_cl = st.sidebar.columns(2)
            with col_ie:
                st.image(ie_logo_path, width=w_ie)
            with col_cl:
                st.image(client_logo_path, width=w_cl)
        else:
            try:
                st.sidebar.image(ie_logo_path, width=min(w_ie * 2, 200), use_container_width=True)
            except FileNotFoundError:
                st.sidebar.markdown("""
                <div style="text-align: center; padding: 20px;">
                    <h2 style="color: #4ECDC4; margin: 0;">⚡ Insight Expert</h2>
                    <p style="color: #888; margin: 5px 0;">E-commerce Analytics</p>
                </div>
                """, unsafe_allow_html=True)
        st.sidebar.markdown(
            f'<p style="text-align: center; font-size: 0.85rem; color: #94a3b8; margin: 4px 0;">'
            f'Insight Expert <span style="color: #64748b;">×</span> {SIDEBAR_CLIENT_NAME}</p>',
            unsafe_allow_html=True,
        )
    else:
        # Modo com seletor de fonte: logo IE centralizado
        try:
            st.sidebar.image("components/img/logo3.svg", width=200, use_container_width=True)
        except FileNotFoundError:
            st.sidebar.markdown("""
            <div style="text-align: center; padding: 20px;">
                <h2 style="color: #4ECDC4; margin: 0;">⚡ Insight Expert</h2>
                <p style="color: #888; margin: 5px 0;">E-commerce Analytics</p>
            </div>
            """, unsafe_allow_html=True)
        if ENABLE_LOCAL_AND_UPLOAD:
            st.sidebar.markdown("---")
    
    # Seção de seleção de fonte de dados
    #st.sidebar.markdown("### 📊 Fonte de Dados")
    
    # DEBUG: Diagnóstico de ambiente
    # st.sidebar.caption(f"Debug: Env={bool(os.getenv('SUPABASE_DB_URL'))}, Secrets={bool('SUPABASE_DB_URL' in st.secrets if hasattr(st, 'secrets') else False)}")
    
    # Verificar datasets disponíveis (código mantido para reutilização: próximo cliente pode usar local/upload)
    from pathlib import Path
    local_path = Path("data/processed/pedidos.parquet")
    has_local = local_path.exists()

    # Verificação Supabase (local e produção)
    has_supabase_secret = False
    has_supabase_env = bool(os.getenv("SUPABASE_DB_URL"))
    try:
        if hasattr(st, 'secrets') and st.secrets:
            if hasattr(st.secrets, 'get') and st.secrets.get("SUPABASE_DB_URL"):
                has_supabase_secret = True
            elif hasattr(st.secrets, '__contains__') and "SUPABASE_DB_URL" in st.secrets:
                has_supabase_secret = True
    except (FileNotFoundError, AttributeError, KeyError, TypeError):
        pass
    except Exception:
        pass

    # Modo atual: apenas Supabase. Para reativar Local + Upload, use ENABLE_LOCAL_AND_UPLOAD=true.
    if not ENABLE_LOCAL_AND_UPLOAD:
        # Só Supabase: ocultar seletor e mensagem de conexão (cliente já sabe que está conectado)
        st.session_state.dataset_choice = "supabase"
        if not (has_supabase_env or has_supabase_secret):
            st.sidebar.warning("⚠️ Supabase não configurado. Defina SUPABASE_DB_URL para habilitar.")
        else:
            st.session_state.use_supabase = True
        return "supabase"
    else:
        # Sistema completo: Local (se existir) + Upload + Supabase
        dataset_options = {"Dados do Cliente (Upload)": "upload"}
        if has_local:
            new_options = {"Dados Integrados (Local)": "local"}
            new_options.update(dataset_options)
            dataset_options = new_options
        cloud_options = {"Supabase (Nuvem)": "supabase"}
        cloud_options.update(dataset_options)
        dataset_options = cloud_options

        default_index = 0
        if 'dataset_choice' not in st.session_state:
            st.session_state.dataset_choice = "local" if has_local else "upload"
        current_choice = st.session_state.dataset_choice
        found_key = None
        for k, v in dataset_options.items():
            if v == current_choice:
                found_key = k
                break
        if found_key:
            default_index = list(dataset_options.keys()).index(found_key)
        else:
            st.session_state.dataset_choice = "local" if has_local else "upload"

    # Selectbox para escolha de dataset (apenas quando Local/Upload estão habilitados)
    selected_option = st.sidebar.selectbox(
        "Escolha a fonte de dados:",
        options=list(dataset_options.keys()),
        index=default_index,
        key="dataset_source_selector",
        help="Selecione a fonte de dados para análise",
        on_change=on_dataset_source_change
    )
    
    dataset_choice = dataset_options[selected_option]
    st.session_state.dataset_choice = dataset_choice
    
    # Renderizar seção apropriada baseada na escolha
    if dataset_choice == "local":
        #st.sidebar.markdown("---")
        #st.sidebar.success(f"📂 **Dados Integrados**\nCarregado de: `{local_path.name}`")
        
        # Definir caminho no session state para o app.py usar
        st.session_state.processed_file_path = str(local_path)
        
        # Informações básicas do arquivo
        try:
            size_mb = local_path.stat().st_size / (1024 * 1024)
            mtime = datetime.fromtimestamp(local_path.stat().st_mtime).strftime('%d/%m/%Y %H:%M')
            st.sidebar.caption(f"Tamanho: {size_mb:.1f} MB | Atualizado: {mtime}")
        except:
            pass
            
        return "local"

    elif dataset_choice == "supabase":
        if not (has_supabase_env or has_supabase_secret):
            st.sidebar.warning("⚠️ Supabase não configurado. Defina SUPABASE_DB_URL para habilitar.")
            st.session_state.dataset_choice = "local" if has_local else "upload"
            return st.session_state.dataset_choice
        st.session_state.use_supabase = True
        # Mensagem de conexão só quando o seletor de fonte está visível (Local/Upload habilitados)
        st.sidebar.caption("🟢 Conectado ao Data Warehouse")
        return "supabase"
        
    elif dataset_choice == "upload":
        # Importar e usar o sistema de upload
        from utils.file_upload_manager import render_file_upload_section
        
        processed_file = render_file_upload_section()
        
        # Considerar arquivo processado mesmo que a checagem de existência falhe
        if (processed_file is not None) or ('processed_file_path' in st.session_state):
            st.session_state.dataset_choice = "upload"
            return "upload"
        else:
            # Mostrar interface de upload sem fallback automático
            st.sidebar.markdown("---")
            st.sidebar.info("💡 **Aguardando upload**\nSelecione um arquivo CSV para carregar seus dados")
            return "upload"
    else:
        # Demo Olist desativada: manter compatibilidade retornando escolha atual
        st.sidebar.warning("⚠️ Dados Olist (Demo) desativados. Selecione uma fonte local ou upload.")
        st.session_state.dataset_choice = "local" if has_local else "upload"
        return st.session_state.dataset_choice

def create_top_n_categories_filter(
    df: pd.DataFrame,
    metric_column: str,
    default_options: List[int] = [5, 10, 15, 20],
    key_suffix: str = ""
) -> Tuple[int, pd.DataFrame]:
    """
    Cria um filtro para selecionar top N categorias baseado em uma métrica.
    
    Args:
        df: DataFrame com os dados
        metric_column: Nome da coluna usada para ordenar as categorias
        default_options: Lista de opções para o número de categorias
        key_suffix: Sufixo para a key do componente Streamlit
        
    Returns:
        Tuple contendo:
        - Número de categorias selecionado
        - DataFrame filtrado com as top N categorias
    """
    # Identificar a coluna de categoria
    if 'category' in df.columns:
        category_col = 'category'
    elif 'product_category_name' in df.columns:
        category_col = 'product_category_name'
    else:
        st.error("Coluna de categoria não encontrada no DataFrame")
        return 0, df

    # Agregação por categoria
    agg_dict = {metric_column: 'sum'}
    if 'avg_price' in df.columns:
        agg_dict['avg_price'] = 'mean'
    if 'total_sales' in df.columns:
        agg_dict['total_sales'] = 'sum'
    if 'avg_rating' in df.columns:
        agg_dict['avg_rating'] = 'mean'

    # Agregar dados por categoria
    df_agg = df.groupby(category_col).agg(agg_dict).reset_index()
    
    # Criar duas colunas para o layout
    col1, col2 = st.columns([0.2, 0.8])
    
    with col1:
        # Selectbox reduzido e orientado à esquerda
        top_n = st.selectbox(
            "Número de categorias",  # Label apropriado para acessibilidade
            options=default_options,
            format_func=lambda x: f"Top {x}",
            key=f"top_n_filter_{key_suffix}",
            label_visibility="collapsed"  # Oculta completamente o label
        )
    
    with col2:
        # Total de categorias únicas no lado oposto
        st.write("Total de categorias únicas:", len(df_agg[category_col].unique()))
    
    # Ordenar e filtrar as top N categorias
    top_categories = df_agg.nlargest(top_n, metric_column)
    
    return top_n, top_categories

# Função de debug temporariamente desativada
def render_debug_filters(filter_results: Dict[str, Any], pagina: str) -> None:
    """Renderiza botão de debug melhorado para monitorar filtros."""
    # Debug desativado em produção
    pass
