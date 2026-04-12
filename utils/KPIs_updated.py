import os
from pathlib import Path
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import numpy as np
from typing import Any, Dict, Tuple, Optional, List
import hashlib

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
    "approval_date",
    # Categorias / Produto
    "product_category_name",
    # Monetário
    "price",
    "freight_value",
    # Status / Flags
    "order_status",
    "pedido_cancelado",
    "carrinho_abandonado",
    # Métricas
    "review_score",
    # Funil de conversão - Etapas principais
    "visitors",
    "product_views",
    "product_view",
    "add_to_cart",
    "checkout",
    # Funil de conversão - Etapas específicas para cosméticos
    "newsletter_signup",
    "wishlist_add", 
    "sample_request",
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
    # Datas e métricas – NaN por padrão
    "review_score": np.nan,
    # Numéricos
    "price": 0.0,
    "freight_value": 0.0,
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
    "order_delivered_customer_date": pd.NaT,
}

DATA_CANDIDATES = [
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
    # MD5 usado apenas para cache de hash, não para segurança
    return hashlib.md5(pd.util.hash_pandas_object(df).values.tobytes()).hexdigest()  # nosec B324

@st.cache_data(
    hash_funcs={
        Path: _hash_path,
        pd.DataFrame: _hash_dataframe
    }
)
def load_data(use_required_subset: bool = True, custom_path: Optional[str | Path] = None) -> pd.DataFrame:
    """Carrega o arquivo Parquet consolidado de acordo com a nova estrutura.

    Ordem de procura:
    1. `custom_path` (se fornecido)
    2. dados_consolidados/cliente_merged.parquet (dataset do cliente)
    3. dados_consolidados_teste/olist_merged_data.parquet (dataset demo)
    4. olist_merged_data.parquet (caminho legado)
    """

    paths_to_try = []
    if custom_path is not None:
        paths_to_try.append(Path(custom_path))
    paths_to_try.extend(DATA_CANDIDATES)

    parquet_path = next((p for p in paths_to_try if p.exists()), None)
    if parquet_path is None:
        searched_paths = [str(p) for p in paths_to_try]
        error_msg = f"""Nenhum arquivo Parquet consolidado encontrado nas pastas esperadas.
        
Caminhos verificados:
{chr(10).join(f"- {path}" for path in searched_paths)}

Para resolver:
1. Execute os pipelines de dados primeiro:
   - Olist: python dados_teste_Olist/data_pipeline.py
   - Cliente: python dados_cliente/cliente_pipeline.py
2. Verifique se os arquivos foram gerados em:
   - dados_consolidados_teste/olist_merged_data.parquet
   - dados_consolidados/cliente_merged.parquet"""
        raise FileNotFoundError(error_msg)

    read_kwargs = {}
    if use_required_subset:
        read_kwargs["columns"] = REQUIRED_COLUMNS

    try:
        df = pd.read_parquet(parquet_path, **read_kwargs)
        # Garantir que todas as colunas requeridas existam
        for col, default in REQUIRED_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default
        return df
    except Exception:
        # Se falhar ao ler subset, faz fallback para leitura total
        if "columns" in read_kwargs:
            read_kwargs.pop("columns")
            return pd.read_parquet(parquet_path, **read_kwargs)
        raise

def filter_by_date_range(df: pd.DataFrame, date_range: Optional[List[str]]) -> pd.DataFrame:
    """Filtra o DataFrame pelo período selecionado."""
    if not date_range or len(date_range) != 2:
        return df
    
    # Garantir que a coluna de timestamp está no formato datetime
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1])
    
    return df[
        (df['order_purchase_timestamp'] >= start_date) & 
        (df['order_purchase_timestamp'] <= end_date)
    ]

def calculate_acquisition_retention_kpis(df: pd.DataFrame, marketing_spend: float = 50000, date_range: Optional[List[str]] = None) -> Dict[str, float]:
    """Calcula KPIs específicos para análise de aquisição e retenção."""
    
    # Filtrar dados pelo período
    df = filter_by_date_range(df, date_range).copy()
    
    # Converter colunas de data para datetime
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    
    # Identificar novos vs clientes recorrentes por mês
    df['month'] = df['order_purchase_timestamp'].dt.to_period('M')
    df['month_str'] = df['month'].astype(str)
    
    # Identificar primeira compra de cada cliente
    first_purchases = df.groupby('customer_unique_id')['order_purchase_timestamp'].min().reset_index()
    first_purchases['month'] = first_purchases['order_purchase_timestamp'].dt.to_period('M')
    
    # Novos clientes por mês (corrigido)
    new_customers = first_purchases.groupby('month')['customer_unique_id'].count().reset_index()
    new_customers['month'] = new_customers['month'].astype(str)
    
    # Total de novos clientes no período (corrigido)
    total_new_customers = first_purchases['customer_unique_id'].nunique()
    
    # Clientes recorrentes por mês (corrigido)
    # Primeiro, identificar todas as compras de cada cliente por mês
    customer_orders = df.groupby(['customer_unique_id', 'month'])['order_id'].nunique().reset_index()
    customer_orders['month'] = customer_orders['month'].astype(str)
    
    # Depois, identificar clientes que fizeram mais de uma compra no mês
    returning_customers = customer_orders[customer_orders['order_id'] > 1].groupby('month')['customer_unique_id'].nunique()
    returning_customers = returning_customers.reset_index()
    
    # Taxa de recompra
    total_customers = df['customer_unique_id'].nunique()
    customers_with_multiple_orders = df.groupby('customer_unique_id')['order_id'].nunique()
    customers_with_multiple_orders = customers_with_multiple_orders[customers_with_multiple_orders > 1].count()
    repurchase_rate = customers_with_multiple_orders / total_customers if total_customers > 0 else 0
    
    # Tempo médio até segunda compra (corrigido)
    # Calcular tempo até a segunda compra em **dias**
    customer_orders = (
        df.sort_values("order_purchase_timestamp")
        .groupby("customer_unique_id")
        ["order_purchase_timestamp"]
        .apply(list)
        .reset_index()
    )

    def _diff_days(purchases: List[pd.Timestamp]) -> Optional[int]:
        if len(purchases) > 1:
            delta = purchases[1] - purchases[0]
            return delta / pd.Timedelta(days=1) # type: ignore
        return None

    customer_orders["time_to_second_purchase"] = customer_orders["order_purchase_timestamp"].apply(_diff_days)
    # Filtrar apenas clientes com múltiplas compras e tempo positivo
    valid_times = pd.to_numeric(
        customer_orders["time_to_second_purchase"], errors="coerce"
    ).dropna()
    valid_times = valid_times[valid_times > 0]
    avg_time_to_second = valid_times.mean() if not valid_times.empty else 0
    
    # CAC (corrigido)
    cac = marketing_spend / total_new_customers if total_new_customers > 0 else 0
    
    # LTV (corrigido)
    # Calcular receita total de pedidos não cancelados
    total_revenue = df[df["pedido_cancelado"] == 0]["price"].sum()
    # Calcular número total de clientes únicos
    total_customers = df["customer_unique_id"].nunique()
    # LTV = Receita total / Número total de clientes (com sinal negativo para visualização)
    ltv = total_revenue / total_customers if total_customers > 0 else 0
    
    return {
        "new_customers": new_customers,
        "returning_customers": returning_customers,
        "repurchase_rate": repurchase_rate,
        "avg_time_to_second": avg_time_to_second,
        "cac": cac,
        "ltv": ltv,
        "total_new_customers": total_new_customers
    }

def calculate_kpis(df: pd.DataFrame, marketing_spend: float = 50000, date_range: Optional[List[str]] = None) -> Dict[str, float]:
    """Calcula os principais KPIs do negócio."""

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
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    if 'order_delivered_customer_date' in df.columns:
        df = df.copy()  # Additional copy before modifying
        df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')
    
    # Calcular KPIs
    total_revenue = df[df["pedido_cancelado"] == 0]["price"].sum()
    total_orders = df["order_id"].nunique()
    total_customers = df["customer_unique_id"].nunique()
    
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
    
    total_products = df[product_col].nunique()
    unique_categories = df["product_category_name"].nunique()
    
    # Taxa de abandono (corrigido para considerar o período)
    total_cart_abandonments = df[df["pedido_cancelado"] == 1]["order_id"].nunique()
    total_carts = df["order_id"].nunique()
    abandonment_rate = total_cart_abandonments / total_carts if total_carts > 0 else 0
    
    # CSAT
    csat = df["review_score"].mean()
    
    # Ticket médio
    average_ticket = total_revenue / total_orders if total_orders > 0 else 0
    
    # Tempo médio de entrega
    if 'order_delivered_customer_date' in df.columns:
        df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
        avg_delivery_time = df['delivery_time'].mean()
    else:
        avg_delivery_time = 0
    
    # Taxa de cancelamento
    cancellation_rate = df["pedido_cancelado"].mean()
    
    # Receita perdida
    lost_revenue = df[df["pedido_cancelado"] == 1]["price"].sum()
    
    return {
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

def calculate_product_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas por produto/categoria para análise.
    
    Args:
        df: DataFrame com os dados dos produtos
        
    Returns:
        DataFrame com as métricas calculadas
    """
    # Primeiro, calcular métricas por produto
    product_metrics = df.groupby(['product_category_name']).agg({
        'price': ['mean', 'sum', 'count'],
        'review_score': 'mean'
    }).reset_index()

    # Renomear as colunas
    product_metrics.columns = [
        'category',
        'avg_price',
        'total_revenue',
        'total_sales',
        'avg_rating'
    ]

    # Calcular score composto para ranking
    product_metrics['composite_score'] = (
        product_metrics['total_revenue'].rank(pct=True) * 0.4 +
        product_metrics['total_sales'].rank(pct=True) * 0.3 +
        product_metrics['avg_rating'].rank(pct=True) * 0.3
    )

    return product_metrics

def calculate_revenue_forecast(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calcula a previsão de receita para os próximos 30 dias."""
    # Calcular média diária de receita
    if df is None or df.empty:
        # Padrão exigido pelos testes de KPIs: lançar IndexError em dataset vazio
        raise IndexError("Dataset vazio para previsão de receita")
    if 'order_purchase_timestamp' not in df.columns or 'price' not in df.columns:
        raise IndexError("Colunas necessárias ausentes para previsão de receita")

    df = df.copy()
    df['date'] = pd.to_datetime(df['order_purchase_timestamp']).dt.date
    daily_revenue = df.groupby('date')['price'].sum().reset_index()

    # Se não houver histórico, seguir o mesmo padrão de erro esperado pelos testes de KPIs
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
    # Preparar dados para análise
    df['month'] = pd.to_datetime(df['order_purchase_timestamp']).dt.to_period('M')
    monthly_category_sales = df.groupby(['month', 'product_category_name']).agg({
        'price': 'sum',
        'order_id': 'count',
        'pedido_cancelado': 'mean'
    }).reset_index()
    monthly_category_sales['month'] = monthly_category_sales['month'].astype(str)
    
    # Identificar as 5 categorias com maior volume de vendas
    top_categories = df.groupby('product_category_name')['order_id'].nunique().sort_values(ascending=False).head(5).index.tolist()
    
    # Filtrar apenas as categorias principais
    top_category_sales = monthly_category_sales[monthly_category_sales['product_category_name'].isin(top_categories)]
    
    # Calcular métricas de rentabilidade
    category_profit = df.groupby('product_category_name').agg({
        'price': 'sum',
        'order_id': 'count'
    }).reset_index()
    
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
    # Sazonalidade de Vendas
    df['day_of_week'] = pd.to_datetime(df['order_purchase_timestamp']).dt.day_name()
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_revenue = df.groupby('day_of_week')['price'].sum().reindex(day_order)
    
    df['month'] = pd.to_datetime(df['order_purchase_timestamp']).dt.month_name()
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                  'July', 'August', 'September', 'October', 'November', 'December']
    month_revenue = df.groupby('month')['price'].sum().reindex(month_order)
    
    # Ticket Médio por Estado
    state_ticket = df.groupby('customer_state')['price'].mean().sort_values(ascending=False)
    
    return {
        'day_revenue': day_revenue,
        'month_revenue': month_revenue,
        'state_ticket': state_ticket
    }


