import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Any, Optional
import streamlit as st
from utils.filtros import create_top_n_categories_filter
from pathlib import Path
import hashlib



def create_satisfaction_chart(monthly_satisfaction: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de linha mostrando a evolução da satisfação do cliente ao longo do tempo.
    
    Args:
        monthly_satisfaction (pd.DataFrame): DataFrame contendo os dados de satisfação mensal
            com colunas 'order_purchase_timestamp' e 'review_score'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de satisfação
    """
    fig = px.line(
        monthly_satisfaction,
        x='order_purchase_timestamp',
        y='review_score',
        title=" ",
        labels={'review_score': 'Nota Média', 'order_purchase_timestamp': 'Mês'}
    )
    fig.update_layout(
        yaxis=dict(range=[0, 5]),
        showlegend=False
    )
    return fig

def create_cancellation_chart(monthly_cancellation: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de linha mostrando a evolução da taxa de cancelamento ao longo do tempo.
    
    Args:
        monthly_cancellation (pd.DataFrame): DataFrame contendo os dados de cancelamento mensal
            com colunas 'order_purchase_timestamp' e 'pedido_cancelado'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de cancelamento
    """
    fig = px.line(
        monthly_cancellation,
        x='order_purchase_timestamp',
        y='pedido_cancelado',
        title=" ",
        labels={'pedido_cancelado': 'Taxa de Cancelamento', 'order_purchase_timestamp': 'Mês'}
    )
    fig.update_layout(
        yaxis=dict(tickformat=".1%"),
        showlegend=False
    )
    return fig

def create_revenue_chart(monthly_revenue: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de linha mostrando a evolução da receita mensal ao longo do tempo.
    
    Args:
        monthly_revenue (pd.DataFrame): DataFrame contendo os dados de receita mensal
            com colunas 'order_purchase_timestamp' e 'price'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de receita
    """
    fig = px.line(
        monthly_revenue,
        x='order_purchase_timestamp',
        y='price',
        title=" ",
        labels={'price': 'Receita (R$)', 'order_purchase_timestamp': 'Mês'}
    )
    fig.update_layout(showlegend=False)
    return fig

def create_ltv_cac_comparison_chart(monthly_metrics: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de comparação entre LTV e CAC ao longo do tempo.
    
    Args:
        monthly_metrics (pd.DataFrame): DataFrame contendo as métricas mensais
            com colunas 'order_purchase_timestamp', 'monthly_ltv', 'monthly_cac', 'ltv_cac_ratio'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de comparação LTV/CAC
    """
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=monthly_metrics['order_purchase_timestamp'],
        y=monthly_metrics['monthly_ltv'],
        name='LTV (sinal invertido para visualização)',
        fill='tozeroy',
        line=dict(color='rgba(46, 204, 113, 0.3)'),
        fillcolor='rgba(46, 204, 113, 0.3)'
    ))
    
    fig.add_trace(go.Scatter(
        x=monthly_metrics['order_purchase_timestamp'],
        y=monthly_metrics['monthly_cac'],
        name='CAC',
        fill='tozeroy',
        line=dict(color='rgba(231, 76, 60, 0.3)'),
        fillcolor='rgba(231, 76, 60, 0.3)'
    ))
    
    fig.add_trace(go.Scatter(
        x=monthly_metrics['order_purchase_timestamp'],
        y=monthly_metrics['ltv_cac_ratio'],
        name='Razão LTV/CAC',
        line=dict(color='#2c3e50', width=2),
        yaxis='y2'
    ))
    
    fig.add_annotation(
        x=0.5,
        y=-0.2,
        xref="paper",
        yref="paper",
        text="Nota: O LTV está representado com sinal invertido apenas para facilitar a visualização no gráfico",
        showarrow=False,
        font=dict(size=12, color="#666")
    )
    
    fig.update_layout(
        showlegend=True,
        yaxis2=dict(
            title="Razão LTV/CAC",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(t=50, b=100, l=50, r=50)
    )
    
    return fig

def create_customer_evolution_chart(new_customers: pd.DataFrame, returning_customers: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de barras empilhadas mostrando a evolução de novos e retornando clientes.
    
    Args:
        new_customers (pd.DataFrame): DataFrame com dados de novos clientes
            com colunas 'month' e 'customer_unique_id'
        returning_customers (pd.DataFrame): DataFrame com dados de clientes retornando
            com colunas 'month' e 'customer_unique_id'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de evolução de clientes
    """
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=new_customers['month'],
        y=new_customers['customer_unique_id'],
        name='Novos Clientes',
        marker_color='#1f77b4'
    ))
    
    fig.add_trace(go.Bar(
        x=returning_customers['month'],
        y=returning_customers['customer_unique_id'],
        name='Clientes Retornando',
        marker_color='#2ca02c'
    ))
    
    fig.update_layout(
        title=" ",
        barmode='stack',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        height=400,  # Altura fixa
        margin=dict(t=50, b=50, l=50, r=50),  # Margens ajustadas
        xaxis=dict(
            tickangle=45,  # Rotacionar labels para melhor legibilidade
            tickmode='array',
            ticktext=new_customers['month'],  # Usar o mês diretamente
            tickvals=new_customers['month']
        )
    )
    
    return fig

def create_order_funnel_chart(funnel_data: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de funil mostrando a evolução dos pedidos através das diferentes etapas.
    
    Args:
        funnel_data (pd.DataFrame): DataFrame contendo os dados do funil
            com colunas 'status_label' e 'count'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de funil
    """
    fig = go.Figure(go.Funnel(
        y=funnel_data['status_label'],
        x=funnel_data['count'],
        textinfo="value+percent initial",
        textposition="inside",
        marker=dict(color=["#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd"])
    ))
    
    fig.update_layout(
        title=" ",
        showlegend=False
    )
    
    return fig

def create_satisfaction_analysis_charts(filtered_df: pd.DataFrame) -> Tuple[go.Figure, go.Figure]:
    """
    Cria dois gráficos relacionados à satisfação do cliente:
    1. Evolução da satisfação ao longo do tempo
    2. Distribuição das notas de satisfação
    
    Args:
        filtered_df (pd.DataFrame): DataFrame contendo os dados filtrados
            com colunas 'order_purchase_timestamp' e 'review_score'
    
    Returns:
        tuple: Tupla contendo duas figuras do Plotly (fig_satisfaction, fig_dist)
    """
    # Gráfico de Satisfação do Cliente ao Longo do Tempo
    monthly_satisfaction = filtered_df.groupby(filtered_df['order_purchase_timestamp'].dt.to_period('M'))['review_score'].mean().reset_index()
    monthly_satisfaction['order_purchase_timestamp'] = monthly_satisfaction['order_purchase_timestamp'].astype(str)
    
    fig_satisfaction = go.Figure()
    fig_satisfaction.add_trace(go.Scatter(
        x=monthly_satisfaction['order_purchase_timestamp'],
        y=monthly_satisfaction['review_score'],
        mode='lines+markers',
        name='Satisfação Média',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=8, symbol='circle')
    ))
    
    fig_satisfaction.update_layout(
        title=" ",
        yaxis=dict(range=[0, 5], title="Nota Média"),
        xaxis=dict(title="Mês"),
        showlegend=False
    )
    
    # Gráfico de Distribuição de Satisfação
    fig_dist = go.Figure()
    fig_dist.add_trace(go.Histogram(
        x=filtered_df['review_score'],
        nbinsx=5,
        marker_color='#1f77b4',
        opacity=0.7
    ))
    
    fig_dist.update_layout(
        title=" ",
        xaxis=dict(
            title="Nota",
            range=[0, 5],
            tickmode='linear',
            tick0=0,
            dtick=1
        ),
        yaxis=dict(title="Quantidade de Avaliações"),
        showlegend=False
    )
    
    return fig_satisfaction, fig_dist

def create_delivery_analysis_charts(filtered_df: pd.DataFrame) -> Tuple[go.Figure, go.Figure]:
    """
    Cria dois gráficos relacionados à análise de entrega:
    1. Evolução do tempo de entrega ao longo do tempo
    2. Evolução do ticket médio ao longo do tempo
    
    Args:
        filtered_df (pd.DataFrame): DataFrame contendo os dados filtrados
            com colunas 'order_purchase_timestamp', 'order_delivered_customer_date' e 'price'
    
    Returns:
        tuple: Tupla contendo duas figuras do Plotly (fig_delivery, fig_ticket)
    """
    # Gráfico de Tempo de Entrega ao Longo do Tempo
    filtered_df['delivery_time'] = (pd.to_datetime(filtered_df['order_delivered_customer_date']) - 
                                  pd.to_datetime(filtered_df['order_purchase_timestamp'])).dt.days
    
    monthly_delivery = filtered_df.groupby(filtered_df['order_purchase_timestamp'].dt.to_period('M'))['delivery_time'].mean().reset_index()
    monthly_delivery['order_purchase_timestamp'] = monthly_delivery['order_purchase_timestamp'].astype(str)
    
    fig_delivery = go.Figure()
    fig_delivery.add_trace(go.Scatter(
        x=monthly_delivery['order_purchase_timestamp'],
        y=monthly_delivery['delivery_time'],
        mode='lines+markers',
        name='Tempo Médio de Entrega',
        line=dict(color='#2ca02c', width=2),
        marker=dict(size=8, symbol='circle')
    ))
    
    fig_delivery.update_layout(
        title=" ",
        yaxis=dict(title="Dias"),
        xaxis=dict(title="Mês"),
        showlegend=False
    )
    
    # Gráfico de Ticket Médio ao Longo do Tempo
    monthly_ticket = filtered_df.groupby(filtered_df['order_purchase_timestamp'].dt.to_period('M'))['price'].mean().reset_index()
    monthly_ticket['order_purchase_timestamp'] = monthly_ticket['order_purchase_timestamp'].astype(str)
    
    fig_ticket = go.Figure()
    fig_ticket.add_trace(go.Scatter(
        x=monthly_ticket['order_purchase_timestamp'],
        y=monthly_ticket['price'],
        mode='lines+markers',
        name='Ticket Médio',
        line=dict(color='#ff7f0e', width=2),
        marker=dict(size=8, symbol='circle')
    ))
    
    fig_ticket.update_layout(
        title=" ",
        yaxis=dict(title="Valor Médio (R$)"),
        xaxis=dict(title="Mês"),
        showlegend=False
    )
    
    return fig_delivery, fig_ticket

def _hash_path(path: Path) -> str:
    """Função customizada para hashear objetos Path."""
    return str(path.absolute())

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
        hash_funcs={
            Path: _hash_path,
            pd.DataFrame: _hash_dataframe
        },
        ttl=7200,  # Cache por 2 horas
        max_entries=50,
        show_spinner=False
    )(func)

@st.cache_data(
    hash_funcs={
        Path: _hash_path,
        pd.DataFrame: _hash_dataframe
    },
    ttl=7200,  # Cache por 2 horas
    max_entries=50,
    show_spinner=False
)
def create_performance_analysis_charts(df: pd.DataFrame) -> Tuple[go.Figure, go.Figure, go.Figure, go.Figure]:
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

def create_category_revenue_chart(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de top 10 categorias por receita."""
    category_revenue = df.groupby('product_category_name')['price'].sum().sort_values(ascending=False).head(10)
    
    fig = px.bar(
        category_revenue,
        title="Top 10 Categorias por Receita",
        labels={'value': 'Receita (R$)', 'product_category_name': 'Categoria'},
        color=category_revenue.values,
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-45,
        yaxis=dict(tickformat=",.2f")
    )
    
    return fig

def create_price_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de distribuição de preços por categoria."""
    fig = px.box(
        df,
        x='product_category_name',
        y='price',
        title="Distribuição de Preços por Categoria",
        labels={'price': 'Preço (R$)', 'product_category_name': 'Categoria'}
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis=dict(tickformat=",.2f")
    )
    
    return fig

def create_category_quantity_chart(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de top 10 categorias por quantidade."""
    category_quantity = df.groupby('product_category_name')['order_id'].nunique().sort_values(ascending=False).head(10)
    
    fig = px.bar(
        category_quantity,
        title="Top 10 Categorias por Quantidade de Pedidos",
        labels={'value': 'Quantidade', 'product_category_name': 'Categoria'},
        color=category_quantity.values,
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-45
    )
    
    return fig

def create_cancellation_rate_chart(df: pd.DataFrame) -> go.Figure:
    """Cria gráfico de taxa de cancelamento por categoria."""
    cancellation_rate = df.groupby('product_category_name')['pedido_cancelado'].mean().sort_values(ascending=False)
    
    fig = px.bar(
        cancellation_rate,
        title="Taxa de Cancelamento por Categoria",
        labels={'value': 'Taxa de Cancelamento', 'product_category_name': 'Categoria'},
        color=cancellation_rate.values,
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-45,
        yaxis=dict(tickformat=".1%")
    )
    
    return fig


def create_bcg_matrix_chart(category_metrics: pd.DataFrame) -> go.Figure:
    """
    Cria gráfico da Matriz BCG Híbrida para E-commerce.
    
    Args:
        category_metrics: DataFrame com métricas das categorias incluindo BCG
        
    Returns:
        Figura do Plotly com a matriz BCG
    """
    if category_metrics.empty or 'bcg_quadrant' not in category_metrics.columns:
        fig = go.Figure()
        fig.add_annotation(
            text="Dados insuficientes para criar a Matriz BCG",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#888")
        )
        return fig
    
    # Definir cores para cada quadrante
    quadrant_colors = {
        'Estrela Digital': '#fbbf24',  # Dourado
        'Vaca Leiteira': '#10b981',    # Verde
        'Interrogação': '#3b82f6',     # Azul
        'Abacaxi': '#ef4444'           # Vermelho
    }
    
    # Estratégias em texto puro (evitar SVG/HTML no hover)
    strategy_plain = {
        'Estrela Digital': 'INVESTIR PESADO',
        'Vaca Leiteira': 'OTIMIZAR MARGEM',
        'Interrogação': 'TESTAR ESTRATÉGIAS',
        'Abacaxi': 'DESCONTINUAR',
    }

    # Criar scatter plot
    fig = go.Figure()
    
    # -----------------------------
    # Tratativa de outliers (growth)
    # -----------------------------
    def _handle_growth_outliers(series: pd.Series) -> pd.Series:
        """
        Aplica tratativa para valores extremos de growth, focando em legibilidade/robustez.
        Config via st.session_state:
          - growth_outlier_mode: 'none' | 'cap' | 'winsor'
          - growth_outlier_abs_cap: float (cap absoluto em %)
          - growth_outlier_winsor_low: int (percentil inferior)
          - growth_outlier_winsor_high: int (percentil superior)
        """
        s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)
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

        if mode == "none":
            return s.fillna(0)

        if mode == "cap":
            return s.clip(lower=-abs_cap, upper=abs_cap).fillna(0)

        # winsor (p_low / p_high) + segurança por cap absoluto
        valid = s.dropna()
        if len(valid) == 0:
            return s.fillna(0)
        p_low = max(0, min(49, p_low))
        p_high = max(51, min(100, p_high))
        if p_low >= p_high:
            p_low, p_high = 2, 98
        lo = float(np.nanpercentile(valid, p_low))
        hi = float(np.nanpercentile(valid, p_high))
        out = s.clip(lower=lo, upper=hi)
        out = out.clip(lower=-abs_cap, upper=abs_cap)
        return out.fillna(0)

    cm = category_metrics.copy()
    if "growth_rate" in cm.columns:
        if "growth_rate_raw" not in cm.columns:
            cm["growth_rate_raw"] = cm["growth_rate"]
        cm["growth_rate"] = _handle_growth_outliers(cm["growth_rate"])

    for quadrant in quadrant_colors.keys():
        quadrant_data = cm[cm['bcg_quadrant'] == quadrant]
        
        if not quadrant_data.empty:
            # Adicionar pequeno jitter para reduzir sobreposição
            np.random.seed(42)  # Para resultados consistentes
            jitter_x = np.random.normal(0, 0.1, len(quadrant_data))
            jitter_y = np.random.normal(0, 2, len(quadrant_data))
            
            strategy_text = strategy_plain.get(quadrant, 'ANALISAR')

            fig.add_trace(go.Scatter(
                x=quadrant_data['market_share'] + jitter_x,
                y=quadrant_data['growth_rate'] + jitter_y,
                mode='markers',
                marker=dict(
                    size=quadrant_data['composite_score'] * 50 + 20,  # Tamanho baseado no composite_score
                    color=quadrant_colors[quadrant],
                    opacity=0.8,
                    line=dict(width=2, color='white')
                ),
                text=quadrant_data['category'] + '<br>' +
                     'Market Share: ' + quadrant_data['market_share'].round(1).astype(str) + '%<br>' +
                     'Growth (clipped): ' + quadrant_data['growth_rate'].round(1).astype(str) + '%<br>' +
                     (
                         'Growth (raw): ' +
                         quadrant_data['growth_rate_raw'].round(1).astype(str) + '%<br>'
                         if 'growth_rate_raw' in quadrant_data.columns else ''
                     ) +
                     'Composite Score: ' + quadrant_data['composite_score'].round(3).astype(str) + '<br>' +
                     'Estratégia: ' + strategy_text,
                hovertemplate='%{text}<extra></extra>',
                name=quadrant,
                showlegend=True
            ))
    
    # Percentis para linhas divisórias (default: 50º percentil)
    try:
        growth_pct = int(st.session_state.get('bcg_growth_percentile', 50))
    except Exception:
        growth_pct = 50
    try:
        share_pct = int(st.session_state.get('bcg_share_percentile', 50))
    except Exception:
        share_pct = 50

    growth_series = cm['growth_rate'].replace([np.inf, -np.inf], np.nan).dropna()
    share_series = cm['market_share'].replace([np.inf, -np.inf], np.nan).dropna()

    growth_thresh = float(np.nanpercentile(growth_series, growth_pct)) if len(growth_series) else 0.0
    share_thresh = float(np.nanpercentile(share_series, share_pct)) if len(share_series) else 0.0

    # Define ranges com pequena margem
    min_growth = float(np.nanmin(growth_series)) if len(growth_series) else 0.0
    max_growth = float(np.nanmax(growth_series)) if len(growth_series) else 0.0
    min_share = float(np.nanmin(share_series)) if len(share_series) else 0.0
    max_share = float(np.nanmax(share_series)) if len(share_series) else 0.0

    growth_pad = max(5.0, (max_growth - min_growth) * 0.1)
    share_pad = max(0.5, (max_share - min_share) * 0.05)

    # Linha horizontal (percentil de growth)
    fig.add_hline(
        y=growth_thresh,
        line_dash="dash",
        line_color="gray",
        opacity=0.5,
        annotation_text=f"Growth P{growth_pct}"
    )
    # Linha vertical (percentil de market share)
    fig.add_vline(
        x=share_thresh,
        line_dash="dash",
        line_color="gray",
        opacity=0.5,
        annotation_text=f"Share P{share_pct}"
    )
    
    # Layout
    fig.update_layout(
        xaxis_title="Market Share (%)",
        yaxis_title="Taxa de Crescimento (%)",
        width=800,
        height=600,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        ),
        margin=dict(l=50, r=150, t=80, b=50),
        xaxis=dict(
            range=[min_share - share_pad, max_share + share_pad],
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)'
        ),
        yaxis=dict(
            range=[min_growth - growth_pad, max_growth + growth_pad],
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)'
        )
    )
    
    # Anotações dos quadrantes removidas para limpar o gráfico
    
    return fig

def create_category_growth_timeseries(df: pd.DataFrame, top_n: int = 12, reference_df: Optional[pd.DataFrame] = None, period: str = 'M') -> go.Figure:
    """
    Cria um time series do growth rate por categoria, com tooltip
    contendo Composite Score e Market Share.
    
    Args:
        df: DataFrame bruto filtrado (pedidos)
        top_n: número de categorias a exibir
        reference_df: DataFrame completo para referência
        period: 'D', 'W' ou 'M' (default: 'M')
    """
    required = {'order_purchase_timestamp', 'product_category_name', 'price'}
    if df is None or df.empty or not required.issubset(df.columns):
        fig = go.Figure()
        fig.add_annotation(text="Dados insuficientes para time series de growth",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False, font=dict(size=14, color="#888"))
        return fig

    dfx = df.copy()
    ts = pd.to_datetime(dfx['order_purchase_timestamp'], errors='coerce')
    dfx = dfx.loc[ts.notna()].copy()
    # Converte para Period e depois volta para Timestamp (início do período) para garantir plotagem correta no eixo X
    # Isso resolve problemas com filtros semanais ('W') que geram strings de intervalo incompatíveis com Plotly
    try:
        dfx['month'] = ts[ts.notna()].dt.to_period(period).dt.to_timestamp()
    except Exception:
        # Fallback robusto se falhar
        dfx['month'] = pd.to_datetime(ts[ts.notna()].dt.to_period(period).astype(str).str.split('/').str[0], errors='coerce')

    # Guardas para colunas opcionais
    has_order = 'order_id' in dfx.columns
    has_customer = 'customer_unique_id' in dfx.columns
    has_review = 'review_score' in dfx.columns
    has_payment = 'payment_value' in dfx.columns

    agg_dict = {
        'total_revenue': ('price', 'sum'),
        'avg_ticket': ('price', 'mean'),
        'total_items': ('price', 'count'),
    }
    if has_order:
        agg_dict['unique_orders'] = ('order_id', 'nunique')
    if has_customer:
        agg_dict['unique_customers'] = ('customer_unique_id', 'nunique')
    if has_review:
        agg_dict['avg_satisfaction'] = ('review_score', 'mean')
        agg_dict['total_reviews'] = ('review_score', 'count')
    if has_payment:
        agg_dict['avg_payment'] = ('payment_value', 'mean')

    monthly = (
        dfx.groupby(['month', 'product_category_name'])
        .agg(**agg_dict)
        .reset_index()
        .rename(columns={'product_category_name': 'category'})
    )

    # Fallbacks para colunas ausentes
    for col in ['unique_orders', 'unique_customers', 'avg_satisfaction', 'total_reviews', 'avg_payment']:
        if col not in monthly.columns:
            monthly[col] = 0

    # Métricas derivadas mensais
    monthly['revenue_per_customer'] = monthly['total_revenue'] / monthly['unique_customers'].replace(0, np.nan)
    monthly['items_per_order'] = monthly['total_items'] / monthly['unique_orders'].replace(0, np.nan)
    monthly['review_rate'] = monthly['total_reviews'] / monthly['unique_orders'].replace(0, np.nan)
    monthly[['revenue_per_customer', 'items_per_order', 'review_rate']] = monthly[['revenue_per_customer', 'items_per_order', 'review_rate']].fillna(0)

    # Referência para normalização e market share (padrão: usar df global quando fornecido)
    ref = reference_df if isinstance(reference_df, pd.DataFrame) and not reference_df.empty else dfx
    ref = ref.copy()
    ref_ts = pd.to_datetime(ref['order_purchase_timestamp'], errors='coerce')
    ref = ref.loc[ref_ts.notna()].copy()
    try:
        ref['month'] = ref_ts[ref_ts.notna()].dt.to_period(period).dt.to_timestamp()
    except Exception:
        ref['month'] = pd.to_datetime(ref_ts[ref_ts.notna()].dt.to_period(period).astype(str).str.split('/').str[0], errors='coerce')

    ref_agg_dict = {
        'total_revenue': ('price', 'sum'),
        'avg_ticket': ('price', 'mean'),
        'total_items': ('price', 'count'),
    }
    if 'order_id' in ref.columns:
        ref_agg_dict['unique_orders'] = ('order_id', 'nunique')
    if 'customer_unique_id' in ref.columns:
        ref_agg_dict['unique_customers'] = ('customer_unique_id', 'nunique')
    if 'review_score' in ref.columns:
        ref_agg_dict['avg_satisfaction'] = ('review_score', 'mean')
        ref_agg_dict['total_reviews'] = ('review_score', 'count')
    if 'payment_value' in ref.columns:
        ref_agg_dict['avg_payment'] = ('payment_value', 'mean')

    monthly_ref = (
        ref.groupby(['month', 'product_category_name'])
        .agg(**ref_agg_dict)
        .reset_index()
        .rename(columns={'product_category_name': 'category'})
    )

    # Market share mensal baseado na referência
    total_by_month_ref = monthly_ref.groupby('month')['total_revenue'].sum().rename('ref_total_revenue')
    monthly = monthly.merge(total_by_month_ref, on='month', how='left')
    monthly['market_share'] = np.where(monthly['ref_total_revenue'] > 0, monthly['total_revenue'] / monthly['ref_total_revenue'] * 100, 0)
    monthly.drop(columns=['ref_total_revenue'], inplace=True)

    # Composite mensal: normalização por mês
    metrics_to_normalize = [
        'total_revenue','avg_ticket','avg_satisfaction','unique_orders','unique_customers',
        'revenue_per_customer','items_per_order','review_rate','avg_payment'
    ]
    # Normalização por mês usando a referência quando disponível
    for m in metrics_to_normalize:
        if m not in monthly.columns:
            continue
        if m in monthly_ref.columns:
            ref_min = monthly_ref.groupby('month')[m].min().rename('ref_min')
            ref_max = monthly_ref.groupby('month')[m].max().rename('ref_max')
            monthly = monthly.merge(ref_min, on='month', how='left')
            monthly = monthly.merge(ref_max, on='month', how='left')
            denom = (monthly['ref_max'] - monthly['ref_min'])
            monthly[f'{m}_score'] = np.where(denom > 0, (monthly[m] - monthly['ref_min']) / denom, 0.5)
            monthly.drop(columns=['ref_min', 'ref_max'], inplace=True)
        else:
            m_min = monthly.groupby('month')[m].transform('min')
            m_max = monthly.groupby('month')[m].transform('max')
            denom = (m_max - m_min)
            monthly[f'{m}_score'] = np.where(denom > 0, (monthly[m] - m_min) / denom, 0.5)

    weights = {
        'total_revenue': 0.20,'avg_ticket': 0.15,'avg_satisfaction': 0.20,
        'unique_orders': 0.10,'unique_customers': 0.10,'revenue_per_customer': 0.10,
        'items_per_order': 0.05,'review_rate': 0.05,'avg_payment': 0.05
    }
    monthly['composite_score'] = 0
    for m, w in weights.items():
        monthly['composite_score'] += monthly[f'{m}_score'] * w

    # Growth MoM por categoria
    monthly = monthly.sort_values(['category', 'month'])
    monthly['growth_rate'] = (
        monthly.groupby('category')['total_revenue']
        .pct_change()
        .fillna(0) * 100
    )

    # Guardar o growth raw e aplicar tratativa anti-outlier para visualização/robustez
    monthly['growth_rate_raw'] = monthly['growth_rate']
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

    # Aplicar a mesma lógica do scatter (winsor/cap)
    gr = pd.to_numeric(monthly['growth_rate'], errors="coerce").replace([np.inf, -np.inf], np.nan)
    if mode == "none":
        monthly['growth_rate'] = gr.fillna(0)
    elif mode == "cap":
        monthly['growth_rate'] = gr.clip(lower=-abs_cap, upper=abs_cap).fillna(0)
    else:
        valid = gr.dropna()
        if len(valid) == 0:
            monthly['growth_rate'] = gr.fillna(0)
        else:
            p_low = max(0, min(49, p_low))
            p_high = max(51, min(100, p_high))
            if p_low >= p_high:
                p_low, p_high = 2, 98
            lo = float(np.nanpercentile(valid, p_low))
            hi = float(np.nanpercentile(valid, p_high))
            monthly['growth_rate'] = gr.clip(lower=lo, upper=hi).clip(lower=-abs_cap, upper=abs_cap).fillna(0)

    # Seleção de categorias (top N por receita total)
    if isinstance(top_n, int) and top_n > 0:
        top_cats = (
            monthly.groupby('category')['total_revenue']
            .sum()
            .sort_values(ascending=False)
            .head(top_n)
            .index
        )
        plot_df = monthly[monthly['category'].isin(top_cats)].copy()
    else:
        plot_df = monthly.copy()

    # Arredondar para tooltip legível
    plot_df['composite_score'] = plot_df['composite_score'].round(3)
    plot_df['market_share'] = plot_df['market_share'].round(1)

    fig = px.line(
        plot_df,
        x='month', y='growth_rate', color='category',
        hover_data={
            'category': True,
            'month': True,
            'growth_rate': ':.1f',
            'growth_rate_raw': ':.1f',
            'composite_score': ':.3f',
            'market_share': ':.1f',
            'total_revenue': ':.2f'
        },
        labels={'month': 'Mês', 'growth_rate': 'Growth Rate (%)', 'category': 'Categoria'},
        title=" "
    )

    fig.update_traces(mode='lines+markers')
    fig.update_layout(legend_title_text='Categoria')

    return fig

def create_price_volume_chart(product_metrics: pd.DataFrame) -> go.Figure:
    """
    Cria gráfico de análise de preço vs volume de vendas.
    
    Args:
        product_metrics: DataFrame com métricas dos produtos
        
    Returns:
        Figura do Plotly com o gráfico
    """
    # Arredondar valores para evitar decimais excessivos nos tooltips
    product_metrics['avg_price'] = product_metrics['avg_price'].round(2)
    product_metrics['composite_score'] = product_metrics['composite_score'].round(3)
    product_metrics['avg_rating'] = product_metrics['avg_rating'].round(2)
    
    fig = px.scatter(
        product_metrics,
        x='avg_price',
        y='total_sales',
        size='composite_score',
        color='avg_rating',
        hover_data=['category'],
        title=" ",
        labels={
            'avg_price': 'Preço Médio (R$)',
            'total_sales': 'Total de Vendas',
            'composite_score': 'Score Composto',
            'avg_rating': 'Avaliação Média',
            'category': 'Categoria'
        },
        color_continuous_scale='RdYlBu',
        opacity=0.9
    )
    
    # Adicionar borda aos círculos
    fig.update_traces(
        marker=dict(
            opacity=0.85,
            line=dict(
                width=1,
                color='blue'
            )
        )
    )
    
    fig.update_layout(
        xaxis=dict(tickformat=",.2f"),
        yaxis=dict(tickformat=",.0f"),
        coloraxis_colorbar=dict(
            title="Avaliação Média",
            tickformat=".1f",
            len=0.95,
            thickness=3,
            x= 1.02,
            xanchor= 'left'
        ),
        autosize=True,
        margin=dict(l=20, r=20 , t=40, b=80)
    )
    
    # Adicionar grid para melhor referência visual
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#334155')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#334155')
    
    # Adicionar anotação explicando o score composto
    fig.add_annotation(
        x=0.5,
        y=-0.25, 
        xref="paper",
        yref="paper",
        text="<b>Score Composto:</b> Métrica que combina receita (40%), volume de vendas (30%) e satisfação do cliente (30%) para avaliar o desempenho geral de cada categoria.",
        showarrow=False,
        font=dict(size=12, color="#666"),
        align="center",
        bgcolor="rgba(0, 0, 0, 0.1)",
        bordercolor="rgba(0, 255, 255, 0.17)",
        borderwidth=1,
        borderpad=4
    )
    
    return fig

def create_state_ticket_chart(df: pd.DataFrame) -> Tuple[go.Figure, str, float]:
    """
    Cria gráfico de ticket médio por estado.
    """
    df_calc = df.copy()
    # Ajuste GMV: Ticket Médio deve considerar o valor total pago (Produto + Frete)
    if 'freight_value' in df_calc.columns:
        df_calc['price'] = df_calc['price'] + df_calc['freight_value'].fillna(0)

    state_ticket = df_calc.groupby('customer_state')['price'].mean().sort_values(ascending=False)
    best_state = state_ticket.index[0]
    best_ticket = state_ticket.iloc[0]
    
    fig = px.bar(
        state_ticket,
        title="Ticket Médio por Estado",
        labels={'value': 'Ticket Médio (R$)', 'customer_state': 'Estado'},
        color=state_ticket.values,
        color_continuous_scale='Viridis'
    )
    
    # Remover color do tooltip
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Ticket Médio (R$): %{y:,.2f}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        yaxis=dict(tickformat=",.2f")
    )
    
    return fig, best_state, best_ticket


def create_seasonality_chart(df: pd.DataFrame) -> Tuple[go.Figure, str, float, str, float]:
    """
    Cria gráfico de análise de sazonalidade.
    """
    df_calc = df.copy()
    # Ajuste GMV: Sazonalidade deve refletir picos de faturamento total
    if 'freight_value' in df_calc.columns:
        df_calc['price'] = df_calc['price'] + df_calc['freight_value'].fillna(0)

    # Mapas de tradução para pt-BR
    day_map = {
        "Monday": "Segunda",
        "Tuesday": "Terça",
        "Wednesday": "Quarta",
        "Thursday": "Quinta",
        "Friday": "Sexta",
        "Saturday": "Sábado",
        "Sunday": "Domingo",
    }
    ordered_days = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    month_map = {
        "January": "Janeiro", "February": "Fevereiro", "March": "Março",
        "April": "Abril", "May": "Maio", "June": "Junho",
        "July": "Julho", "August": "Agosto", "September": "Setembro",
        "October": "Outubro", "November": "Novembro", "December": "Dezembro",
    }
    ordered_months = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                      "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    # Análise por dia da semana (traduzido)
    df_calc['day_of_week_en'] = pd.to_datetime(df_calc['order_purchase_timestamp']).dt.day_name()
    df_calc['day_of_week'] = df_calc['day_of_week_en'].map(day_map)
    daily_revenue = df_calc.groupby('day_of_week')['price'].sum().reindex(ordered_days).fillna(0)
    best_day = daily_revenue.idxmax()
    best_day_revenue = daily_revenue.max()
    
    # Análise por mês (traduzido)
    df_calc['month_en'] = pd.to_datetime(df_calc['order_purchase_timestamp']).dt.strftime('%B')
    df_calc['month'] = df_calc['month_en'].map(month_map)
    monthly_revenue = df_calc.groupby('month')['price'].sum().reindex(ordered_months).fillna(0)
    best_month = monthly_revenue.idxmax()
    best_month_revenue = monthly_revenue.max()
    
    # Criar gráfico
    fig = go.Figure()
    
    # Adicionar barras para dias da semana
    fig.add_trace(go.Bar(
        x=daily_revenue.index,
        y=daily_revenue.values,
        name='Receita por Dia',
        marker_color='#1f77b4'
    ))
    
    # Adicionar barras para meses
    fig.add_trace(go.Bar(
        x=monthly_revenue.index,
        y=monthly_revenue.values,
        name='Receita por Mês',
        marker_color='#ff7f0e'
    ))
    
    fig.update_layout(
        title="Análise de Sazonalidade",
        xaxis_title="Período",
        yaxis_title="Receita (R$)",
        barmode='group',
        yaxis=dict(tickformat=",.2f")
    )
    
    return fig, best_day, best_day_revenue, best_month, best_month_revenue


def create_profitability_chart(df: pd.DataFrame) -> Tuple[go.Figure, str, float]:
    """
    Cria gráfico de análise de rentabilidade por categoria.
    
    Args:
        df: DataFrame com os dados
        
    Returns:
        Tuple contendo:
        - Figura do Plotly com o gráfico
        - Melhor Resultado Operacional
        - Lucro da categoria mais rentável
    """
    # Garantir colunas necessárias
    df = df.copy()
    if 'freight_value' not in df.columns:
        df['freight_value'] = 0.0
    if 'pedido_cancelado' not in df.columns:
        df['pedido_cancelado'] = 0
    
    # Calcular Resultado Operacional por categoria
    # Aqui mantemos a lógica de "Sobrar Dinheiro":
    # Profit = Receita Líquida (Produto) - Frete (Custo Logístico)
    # Mas para exibir a "Receita" nos cálculos intermediários, poderíamos usar GMV.
    # Vamos manter 'price' como Produto para o cálculo de lucro ser conservador (Margem sobre Produto),
    # mas se quisermos Margem sobre GMV, teríamos que somar frete no denominador.
    
    # Decisão: Manter 'price' como Produto Bruto para cálculo de Resultado (Visão Operacional)
    # Isso alinha com a estratégia de "Sobrar Dinheiro" que vendemos.
    # O cliente quer ver Faturamento Total no KPI macro, mas aqui estamos vendo eficiência.
    
    category_profit = df.groupby('product_category_name').agg({
        'price': 'sum',
        'freight_value': 'sum',  # Adicionar frete como custo
        'pedido_cancelado': 'mean'
    }).reset_index()
    
    # Fórmula melhorada: Receita - Frete - Cancelamentos
    category_profit['profit'] = (
        category_profit['price'] * (1 - category_profit['pedido_cancelado']) -  # Receita líquida
        category_profit['freight_value']  # Custo de frete
    )
    
    category_profit = category_profit.sort_values('profit', ascending=False).head(20)
    
    best_category = category_profit.iloc[0]['product_category_name']
    best_profit = category_profit.iloc[0]['profit']
    
    fig = px.bar(
        category_profit,
        x='product_category_name',
        y='profit',
        title="Top 20 Categorias por Resultado Operacional",
        labels={'profit': 'Resultado (R$)', 'product_category_name': 'Categoria'},
        color='profit',
        color_continuous_scale='Viridis'
    )
    
    # Personalizar tooltip para arredondar valor
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Resultado (R$): %{y:,.2f}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-65,  # Ângulo maior para 20 barras
        yaxis=dict(tickformat=",.2f"),
        height=500,  # Altura aumentada para melhor visualização
        margin=dict(b=120)  # Margem inferior para labels longos
    )
    
    return fig, best_category, best_profit

def create_growth_chart(df: pd.DataFrame, top_categories: List[str]) -> Tuple[go.Figure, List[Tuple[str, float]]]:
    """
    Cria gráfico de análise de crescimento por categoria.
    """
    # Ajuste GMV para crescimento: usar faturamento total
    df_calc = df.copy()
    if 'freight_value' in df_calc.columns:
        df_calc['price'] = df_calc['price'] + df_calc['freight_value'].fillna(0)

    # Calcular crescimento mensal por categoria
    df_calc['month'] = pd.to_datetime(df_calc['order_purchase_timestamp']).dt.to_period('M')
    monthly_category = df_calc.groupby(['month', 'product_category_name'])['price'].sum().reset_index()
    # Calcular taxa de crescimento
    growth_rates = []
    for category in top_categories:
        category_data = monthly_category[monthly_category['product_category_name'] == category]
        if len(category_data) >= 2:
            first_month = category_data.iloc[0]['price']
            last_month = category_data.iloc[-1]['price']
            growth_rate = (last_month - first_month) / first_month * 100 if first_month > 0 else 0
            growth_rates.append((category, growth_rate))
    # Ordenar por taxa de crescimento
    growth_rates.sort(key=lambda x: x[1], reverse=True)
    # Se não houver dados suficientes, retornar gráfico vazio e lista vazia
    if not growth_rates:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem dados suficientes para calcular o crescimento das categorias neste período.",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#888")
        )
        fig.update_layout(
            title="Taxa de Crescimento por Categoria",
            xaxis_title="Categoria",
            yaxis_title="Taxa de Crescimento (%)"
        )
        return fig, []
    categories, rates = zip(*growth_rates)
    fig = px.bar(
        x=categories,
        y=rates,
        title="Taxa de Crescimento por Categoria",
        labels={'x': 'Categoria', 'y': 'Taxa de Crescimento (%)'},
        color=rates,
        color_continuous_scale='RdYlGn'
    )
    
    # Remover color do tooltip
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Taxa de Crescimento (%): %{y:,.1f}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-45,
        yaxis=dict(tickformat=".1f")
    )
    return fig, growth_rates

def create_profitability_margin_chart(df: pd.DataFrame, top_categories: List[str] = None) -> Tuple[go.Figure, List[Tuple[str, float]]]:
    """
    Cria gráfico de margem de rentabilidade por categoria.
    Margem = (Lucro / Receita) * 100
    """
    # Copiar DataFrame
    df_copy = df.copy()
    
    # Garantir colunas necessárias
    if 'freight_value' not in df_copy.columns:
        df_copy['freight_value'] = 0
    if 'pedido_cancelado' not in df_copy.columns:
        df_copy['pedido_cancelado'] = 0
    
    # Calcular resultado operacional e receita por categoria
    category_metrics = df_copy.groupby('product_category_name').agg({
        'price': 'sum',  # Receita produto (base)
        'freight_value': 'sum',  # Custo (frete)
        'pedido_cancelado': 'mean'  # Taxa de cancelamento
    }).reset_index()
    
    # Calcular Resultado Operacional (Lucro)
    # Profit = (Produto * (1-Cancel)) - Frete
    category_metrics['profit'] = (
        category_metrics['price'] * (1 - category_metrics['pedido_cancelado']) 
        - category_metrics['freight_value']
    )
    
    # Calcular Receita GMV para denominador (Produto + Frete)
    # Eficiência = Quanto sobra do que entrou?
    category_metrics['gmv'] = category_metrics['price'] + category_metrics['freight_value']
    
    # Calcular Eficiência Operacional (%)
    category_metrics['margin_percentage'] = (
        (category_metrics['profit'] / category_metrics['gmv'] * 100)
        .fillna(0)
    )
    
    # Ordenar por eficiência e pegar Top 20
    category_metrics = category_metrics.sort_values('margin_percentage', ascending=False).head(20)
    
    # Preparar dados para retorno
    margin_rates = list(zip(
        category_metrics['product_category_name'].tolist(),
        category_metrics['margin_percentage'].tolist()
    ))
    
    # Se não houver dados suficientes, retornar gráfico vazio
    if category_metrics.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Sem dados suficientes para calcular a eficiência operacional das categorias.",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#888")
        )
        fig.update_layout(
            title="Eficiência Operacional por Categoria",
            xaxis_title="Categoria",
            yaxis_title="Eficiência (%)"
        )
        return fig, []
    
    # Criar gráfico
    fig = px.bar(
        category_metrics,
        x='product_category_name',
        y='margin_percentage',
        title="Top 20 Categorias por Eficiência Operacional",
        labels={'product_category_name': 'Categoria', 'margin_percentage': 'Eficiência (%)'},
        color='margin_percentage',
        color_continuous_scale='RdYlGn'
    )
    
    # Customizar tooltip
    fig.update_traces(
        hovertemplate='<b>%{x}</b><br>Eficiência: %{y:.1f}%<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=False,
        xaxis_tickangle=-65,  # Ângulo maior para 20 barras
        yaxis=dict(tickformat=".1f", title="Eficiência (%)"),
        height=500,  # Altura aumentada para melhor visualização
        margin=dict(b=120)  # Margem inferior para labels longos
    )
    
    return fig, margin_rates

def create_capital_allocation_chart(recommendations: pd.DataFrame) -> go.Figure:
    """
    Cria gráfico de alocação de capital atual vs recomendada.
    
    Args:
        recommendations: DataFrame com recomendações de realocação
        
    Returns:
        Figura Plotly com waterfall chart
    """
    if recommendations.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Dados insuficientes para análise de capital",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#888")
        )
        return fig
    
    # Preparar dados para waterfall
    categories = recommendations['category'].tolist()
    current_capital = recommendations['capital_imobilizado'].tolist()
    capital_change = recommendations['capital_change'].tolist()
    
    # Cores por ação
    color_map = {
        'LIQUIDAR': '#e74c3c',
        'REDUZIR': '#f39c12',
        'MANTER': '#95a5a6',
        'AUMENTAR': '#3498db',
        'ESTRELA': '#2ecc71'
    }
    
    colors = [color_map.get(action, '#95a5a6') for action in recommendations['reallocation_action']]
    
    # Criar waterfall chart
    fig = go.Figure(go.Waterfall(
        name="Capital",
        orientation="v",
        measure=["relative"] * len(categories),
        x=categories,
        textposition="outside",
        text=[f"R$ {change:,.0f}" for change in capital_change],
        y=capital_change,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        marker=dict(color=colors)
    ))
    
    fig.update_layout(
        title={
            'text': "Realocação de Capital por Categoria",
            'x': 0.5,
            'xanchor': 'center'
        },
        showlegend=False,
        xaxis_tickangle=-45,
        yaxis=dict(title="Mudança de Capital (R$)"),
        height=500
    )
    
    return fig


def create_capital_reallocation_sunburst(recommendations: pd.DataFrame) -> go.Figure:
    """
    Cria sunburst de capital imobilizado por ação e categoria.
    
    Args:
        recommendations: DataFrame com recomendações
        
    Returns:
        Figura Plotly sunburst
    """
    if recommendations.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="Dados insuficientes",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return fig
    
    # Preparar dados hierárquicos
    sunburst_data = []
    
    # Nível 1: Ação
    for action in recommendations['reallocation_action'].unique():
        action_data = recommendations[recommendations['reallocation_action'] == action]
        sunburst_data.append({
            'labels': action,
            'parents': '',
            'values': action_data['capital_imobilizado'].sum()
        })
        
        # Nível 2: Categorias dentro de cada ação
        for _, row in action_data.iterrows():
            sunburst_data.append({
                'labels': row['category'],
                'parents': action,
                'values': row['capital_imobilizado']
            })
    
    sunburst_df = pd.DataFrame(sunburst_data)
    
    fig = go.Figure(go.Sunburst(
        labels=sunburst_df['labels'],
        parents=sunburst_df['parents'],
        values=sunburst_df['values'],
        branchvalues="total",
        marker=dict(
            colorscale='RdYlGn',
            cmid=2
        ),
        hovertemplate='<b>%{label}</b><br>Capital: R$ %{value:,.0f}<extra></extra>'
    ))
    
    fig.update_layout(
        title={
            'text': "Distribuição de Capital por Ação Estratégica",
            'x': 0.5,
            'xanchor': 'center'
        },
        height=600
    )
    
    return fig


def create_capital_flow_sankey(summary: Dict[str, Any]) -> go.Figure:
    """
    Cria diagrama Sankey do fluxo de capital.
    
    Args:
        summary: Dicionário com resumo executivo
        
    Returns:
        Figura Plotly Sankey
    """
    # Nós
    node_labels = [
        "Capital Atual",
        "LIQUIDAR",
        "REDUZIR",
        "MANTER",
        "AUMENTAR",
        "ESTRELA",
        "Capital Liberado",
        "Capital Realocado"
    ]
    
    # Links (source -> target)
    sources = []
    targets = []
    values = []
    colors = []
    
    # Capital Atual -> Ações
    actions = ['liquidar', 'reduzir', 'manter', 'aumentar', 'estrela']
    for i, action in enumerate(actions, 1):
        capital = summary.get(f'{action}_capital', 0)
        if capital > 0:
            sources.append(0)  # Capital Atual
            targets.append(i)  # Ação
            values.append(capital)
            colors.append('rgba(231, 76, 60, 0.5)' if action in ['liquidar', 'reduzir'] else 'rgba(46, 204, 113, 0.5)')
    
    # Ações -> Capital Liberado/Realocado
    if summary.get('capital_to_free', 0) > 0:
        sources.append(1)  # LIQUIDAR
        targets.append(6)  # Capital Liberado
        values.append(summary['capital_to_free'] * 0.6)
        colors.append('rgba(231, 76, 60, 0.8)')
        
        sources.append(2)  # REDUZIR
        targets.append(6)  # Capital Liberado
        values.append(summary['capital_to_free'] * 0.4)
        colors.append('rgba(243, 156, 18, 0.8)')
    
    if summary.get('capital_to_allocate', 0) > 0:
        sources.append(6)  # Capital Liberado
        targets.append(4)  # AUMENTAR
        values.append(summary['capital_to_allocate'] * 0.4)
        colors.append('rgba(52, 152, 219, 0.8)')
        
        sources.append(6)  # Capital Liberado
        targets.append(5)  # ESTRELA
        values.append(summary['capital_to_allocate'] * 0.6)
        colors.append('rgba(46, 204, 113, 0.8)')
    
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels,
            color=['#3498db', '#e74c3c', '#f39c12', '#95a5a6', '#3498db', '#2ecc71', '#e67e22', '#2ecc71']
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=colors
        )
    )])
    
    fig.update_layout(
        title={
            'text': "Fluxo de Realocação de Capital",
            'x': 0.5,
            'xanchor': 'center'
        },
        font_size=12,
        height=500
    )
    
    return fig


def create_revenue_forecast_chart(daily_revenue: pd.DataFrame, forecast_df: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de linha mostrando a evolução histórica e previsão de receita.
    
    Args:
        daily_revenue (pd.DataFrame): DataFrame contendo os dados históricos de receita
            com colunas 'date' e 'price'
        forecast_df (pd.DataFrame): DataFrame contendo os dados de previsão
            com colunas 'date', 'forecast', 'lower_bound', 'upper_bound'
    
    Returns:
        plotly.graph_objects.Figure: Figura do gráfico de previsão de receita
    """
    fig = go.Figure()
    
    # Adicionar dados históricos
    fig.add_trace(go.Scatter(
        x=daily_revenue['date'],
        y=daily_revenue['price'],
        name='Receita Histórica',
        line=dict(width=2, color='#1f77b4'),
        mode='lines+markers',
        marker=dict(size=6, symbol='circle')
    ))
    
    # Adicionar previsão
    fig.add_trace(go.Scatter(
        x=forecast_df['date'],
        y=forecast_df['forecast'],
        name='Previsão',
        line=dict(width=2, color='#ff7f0e', dash='dash'),
        mode='lines+markers',
        marker=dict(size=6, symbol='diamond')
    ))
    
    # Adicionar intervalo de confiança
    fig.add_trace(go.Scatter(
        x=forecast_df['date'].tolist() + forecast_df['date'].tolist()[::-1],
        y=forecast_df['upper_bound'].tolist() + forecast_df['lower_bound'].tolist()[::-1],
        fill='toself',
        fillcolor='rgba(255, 127, 14, 0.2)',
        line=dict(color='rgba(255, 127, 14, 0)'),
        name='Intervalo de Confiança'
    ))
    
    fig.update_layout(
        title="",
        xaxis_title="Data",
        yaxis_title="Receita (R$)",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=12)
        ),
        hovermode='x unified',
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Rockwell"
        )
    )
    
    return fig

def _apply_chart_caching():
    skip_cache = {
        "create_bcg_matrix_chart",
        "create_category_growth_timeseries"
    }
    for name, func in list(globals().items()):
        if not name.startswith("create_") or not callable(func):
            continue
        if name in skip_cache:
            continue
        if getattr(func, "__wrapped__", None) is not None:
            continue
        globals()[name] = _cached_chart(func)

_apply_chart_caching()
