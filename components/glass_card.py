from __future__ import annotations
import html
import streamlit as st
from typing import Dict, Any, List, Union, Optional
import plotly.graph_objects as go
import streamlit.components.v1 as components
from utils.theme_manager import get_theme_manager
from utils.svg_icons import get_svg_icon
from utils.filtros import initialize_filters
import time
import uuid
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Global counter for unique chart keys
_chart_counter = 0

def _generate_unique_key(prefix: str = "chart") -> str:
    """
    Generates a unique key for Streamlit elements.
    
    Args:
        prefix: Prefix for the key
        
    Returns:
        Unique key string
    """
    global _chart_counter
    _chart_counter += 1
    timestamp = int(time.time() * 1000)  # Milliseconds timestamp
    unique_id = str(uuid.uuid4())[:8]    # Short unique identifier
    return f"{prefix}_{timestamp}_{_chart_counter}_{unique_id}"

def get_theme_colors() -> Dict[str, str]:
    """Retorna as cores do tema atual."""
    is_dark_theme = st.get_option("theme.base") == "dark"
    theme_manager = get_theme_manager()
    theme = theme_manager.get_theme()
    glass = theme_manager.get_glass_theme()
    
    return {
        "text_color": theme.get('textColor', '#e2e8f0'),
        "border_color": theme.get('secondaryBackgroundColor', '#1e293b'),
        "bg_color": glass.get('cardBackground', 'rgba(30, 41, 59, 0.7)'),
        "shadow_color": glass.get('cardShadow', 'rgba(0, 0, 0, 0.2)'),
        "accent_color": theme.get('primaryColor', '#6366f1')
    }

def apply_enhanced_background() -> None:
    """
    Aplica o fundo radial com padrões diagonais e efeito metálico ao dashboard.
    Esta função deve ser chamada no início de cada página para garantir consistência visual.
    """
    theme_manager = get_theme_manager()
    theme_manager.apply_theme()
def render_glass_card_html(content: str, title: str = "") -> None:
    """
    Renders content inside a glass-effect card using streamlit.components.v1.html for better control.
    
    Args:
        content (str): The content of the card (can be HTML).
        title (str): Optional title for the card.
    """
    import streamlit.components.v1 as components
    
    # Calcular altura dinâmica baseada no conteúdo
    content_length = len(content)
    dynamic_height = max(3, min(250, content_length * 0.6))
    
    card_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            .glass-card {{
                background: linear-gradient(135deg, 
                    rgba(30, 41, 59, 0.8) 0%, 
                    rgba(45, 55, 72, 0.9) 50%, 
                    rgba(30, 41, 59, 0.8) 100%);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid rgba(227, 236, 240, 0.25);
                border-radius: 20px;
                padding: 15px;
                box-shadow: 
                    0 8px 32px rgba(0, 0, 0, 0.3),
                    0 2px 16px rgba(227, 236, 240, 0.1),
                    inset 0 1px 0 rgba(255, 255, 255, 0.1);
                font-family: 'Inter', sans-serif;
                line-height: 1.6;
                color: #cbd5e1;
                overflow: visible;
                margin-bottom: 15px;
            }}
            
            .card-title {{
                color: #e2e8f0;
                font-size: 1.4em;
                font-weight: 600;
                margin-bottom: 15px;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
            }}
            
            .card-content {{
                font-size: 16px;
                line-height: 1.6;
            }}
            
            .card-content h3 {{
                color: #e2e8f0;
                margin-bottom: 15px;
                font-size: 1.4em;
                font-weight: 600;
            }}
            
            .card-content ul {{
                color: #cbd5e1;
                padding-left: 20px;
            }}
            
            .card-content li {{
                margin-bottom: 8px;
            }}
            
            .card-content strong {{
                color: #e2e8f0;
            }}
            
            .card-content .grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
            }}
        </style>
    </head>
    <body>
        <div class="glass-card">
            {f'<div class="card-title">{title}</div>' if title else ''}
            <div class="card-content">
                {content}
            </div>
        </div>
    </body>
    </html>
    """
    
    components.html(card_html, height=dynamic_height, scrolling=False)

def render_glass_card(content: str) -> None:
    """
    Renders content inside a glass-effect card.

    Args:
        content (str): The content of the card.
    """
    try:
        # Sanitizar o conteúdo para evitar problemas de renderização
        sanitized_content = _sanitize_html_content(content)
        
        colors = get_theme_colors()
        
        # Calcular altura dinâmica baseado no conteúdo
        content_length = len(sanitized_content)
        # Estimativa: 1 caractere = 0.5 px de altura, mínimo 150px
        estimated_height = max(150, min(400, content_length * 0.5))
                              
        st.markdown(
            f"""
            <div style="
                backdrop-filter: blur(12px);
                background: {colors['bg_color']};
                padding: 25px;
                border-radius: 20px;
                text-align: left;
                font-size: 16px;
                font-family: 'Inter', sans-serif;
                border: 1px solid {colors['border_color']};
                box-shadow: 0 4px 20px {colors['shadow_color']};
                min-height: {estimated_height}px;
                overflow-y: visible;
                line-height: 1.6;
                ">
                {sanitized_content}
            </div>
            """,
            unsafe_allow_html=True
        )
    except Exception as e:
        # Fallback para renderização robusta usando components.html
        logger.warning(f"Erro na renderização padrão, usando fallback: {e}")
        render_glass_card_html(content)

def _sanitize_html_content(content: str) -> str:
    """
    Sanitiza conteúdo HTML para evitar problemas de renderização.
    
    Args:
        content: Conteúdo HTML original
        
    Returns:
        Conteúdo sanitizado
    """
    import html
    
    # Escapar caracteres problemáticos mas preservar HTML válido
    # Primeiro, proteger tags HTML válidas
    protected_content = content
    
    # Converter caracteres especiais problemáticos para HTML entities
    replacements = {
        '×': '&times;',
        '÷': '&divide;',
        '–': '&ndash;',
        '—': '&mdash;',
        ''': '&lsquo;',
        ''': '&rsquo;',
        '"': '&ldquo;',
        '"': '&rdquo;',
        '…': '&hellip;'
    }
    
    for char, entity in replacements.items():
        protected_content = protected_content.replace(char, entity)
    
    return protected_content

def render_page_title(
    title: str,
    icon: Optional[str] = None,
    subtitle: Optional[str] = None,
    wrapper_class: Optional[str] = None,
) -> None:
    """
    Renders a centered and styled page title with enhanced visual impact.

    Args:
        title (str): The title text to display
        icon (str, optional): An emoji or icon to display before the title
        subtitle (str, optional): Plain-text subtitle below the title (escaped for HTML)
        wrapper_class (str, optional): Extra CSS class on the outer card (for page-specific themes)
    """
    title_text = f"{icon} {title}" if icon else title
    extra_class = f" {wrapper_class}" if wrapper_class else ""
    # Subtítulo em uma linha: indentação + quebras no markdown do Streamlit viram bloco de código
    # e o <p> aparece como texto cru na interface.
    subtitle_block = ""
    if subtitle:
        esc = html.escape(subtitle)
        subtitle_block = (
            '<p class="insight-page-title-subtitle" style="position:relative;z-index:2;'
            "margin:18px 0 0 0;font-size:1.05rem;font-weight:500;line-height:1.45;"
            'color:rgba(203,213,225,0.95);max-width:52rem;margin-left:auto;margin-right:auto;">'
            f"{esc}</p>"
        )

    st.markdown(
        f"""
        <div class="insight-page-title-card{extra_class}" style="
            position: relative;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            background: linear-gradient(135deg,
                rgba(30, 41, 59, 0.8) 0%,
                rgba(45, 55, 72, 0.9) 50%,
                rgba(30, 41, 59, 0.8) 100%);
            padding: 35px 25px;
            border-radius: 20px;
            text-align: center;
            font-family: 'Inter', sans-serif;
            border: 1px solid rgba(227, 236, 240, 0.3);
            box-shadow:
                0 8px 32px rgba(0, 0, 0, 0.4),
                0 2px 16px rgba(227, 236, 240, 0.1),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
            margin-bottom: 30px;
            overflow: hidden;
            ">
            <div style="
                position: absolute;
                top: 0;
                left: -100%;
                width: 200%;
                height: 100%;
                background: linear-gradient(
                    90deg,
                    transparent 0%,
                    rgba(227, 236, 240, 0.05) 40%,
                    rgba(227, 236, 240, 0.15) 50%,
                    rgba(227, 236, 240, 0.05) 60%,
                    transparent 100%
                );
                z-index: 1;
                animation: titleShimmer 6s infinite linear;
                "></div>
            <h1 style="
                position: relative;
                z-index: 2;
                margin: 0;
                font-size: 3em;
                font-weight: 800;
                letter-spacing: -1px;
                background: linear-gradient(135deg,
                    #e3ecf0 0%,
                    #cbd5e1 30%,
                    #e3ecf0 60%,
                    #94a3b8 100%);
                background-clip: text;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: 0 4px 8px rgba(0, 0, 0, 0.4);
                filter: drop-shadow(0 2px 4px rgba(0, 0, 0, 0.6));
                ">
                {title_text}
            </h1>{subtitle_block}
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_kpi_title(title: str, icon: Optional[str] = None, section_type: str = "default", help_text: Optional[str] = None) -> None:
    """
    Renders a centered and styled KPI block title with enhanced visual hierarchy.

    Args:
        title (str): The title text to display
        icon (str, optional): An emoji or icon to display before the title
        help_text (str, optional): Tooltip text to display on hover
    """
    colors = get_theme_colors()
    
    # Criar o componente de título com tooltip opcional
    title_content = title
    if help_text:
        safe_tooltip = (help_text or "").replace('"', "&quot;")
        tooltip_html = (
            f"<span title=\"{safe_tooltip}\" "
            f"style=\"display:inline-flex;align-items:center;justify-content:center;"
            f"width:20px;height:20px;margin-left:8px;border-radius:999px;"
            f"background:rgba(255,255,255,0.1);color:rgba(226,232,240,0.9);"
            f"font-size:12px;font-weight:700;cursor:help;vertical-align:middle;\">ⓘ</span>"
        )
        title_content = f"{title}{tooltip_html}"

    title_text = f"{icon} {title_content}" if icon else title_content
    
    # Definir cores específicas para cada seção (estilo render_interpretation_guide)
    section_colors = {
        "gestao_clientes": {
            "primary": "rgba(59, 130, 246, 0.15)",      # Azul vibrante
            "secondary": "rgba(96, 165, 250, 0.08)",     # Azul claro
            "accent": "rgba(59, 130, 246, 0.3)"          # Azul para bordas
        },
        "analise_estrategica": {
            "primary": "rgba(139, 92, 246, 0.15)",      # Roxo vibrante
            "secondary": "rgba(167, 139, 250, 0.08)",   # Roxo claro
            "accent": "rgba(139, 92, 246, 0.3)"         # Roxo para bordas
        },
        "otimizacao_produtos": {
            "primary": "rgba(245, 158, 11, 0.15)",      # Âmbar vibrante
            "secondary": "rgba(251, 191, 36, 0.08)",    # Âmbar claro
            "accent": "rgba(245, 158, 11, 0.3)"         # Âmbar para bordas
        },
        "default": {
            "primary": "rgba(30, 41, 59, 0.7)",
            "secondary": "rgba(45, 55, 72, 0.8)",
            "accent": "rgba(227, 236, 240, 0.2)"
        }
    }
    
    # Obter cores da seção ou usar padrão
    section_color = section_colors.get(section_type, section_colors["default"])
    
    # Criar cor mais intensa para o gradiente central de forma segura
    accent_color = section_color['accent']
    accent_intense = accent_color.replace('0.3', '0.8') if '0.3' in accent_color else accent_color

    st.markdown(
        f"""
        <div style="
            position: relative;
            backdrop-filter: blur(14px);
            -webkit-backdrop-filter: blur(14px);
            background: linear-gradient(135deg, 
                {section_color['primary']} 0%, 
                {section_color['secondary']} 50%, 
                {section_color['primary']} 100%);
            padding: 20px 25px;
            border-radius: 16px;
            text-align: center;
            font-family: 'Inter', sans-serif;
            border: 1px solid {accent_color};
            box-shadow: 
                0 6px 24px rgba(0, 0, 0, 0.3),
                0 1px 8px {accent_color};
            margin-bottom: 20px;
            overflow: hidden;
            ">
            <div style="
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 2px;
                background: linear-gradient(90deg, 
                    transparent 0%,
                    {accent_color} 20%,
                    {accent_intense} 50%,
                    {accent_color} 80%,
                    transparent 100%);
                "></div>
            <h2 style="
                margin: 0;
                font-size: 1.8em;
                font-weight: 700;
                letter-spacing: -0.5px;
                background: linear-gradient(135deg, 
                    #e3ecf0 0%, 
                    #cbd5e1 50%, 
                    #e3ecf0 100%);
                background-clip: text;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.4);
                filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.5));
                ">
                {title_text}
            </h2>
        </div>
        """,
        unsafe_allow_html=True
    )

def render_analysis_title_with_stars(title: str, icon: Optional[str] = None) -> None:
    """
    Renders a centered and styled title with a 5-star arc below it.

    Args:
        title (str): The title text to display
        icon (str, optional): An emoji or icon to display before the title
    """
    colors = get_theme_colors()
    title_text = f"{icon} {title}" if icon else title
    
    # Criar o arco de 5 estrelas (mesmo componente visual usado antes)
    star_icon = get_svg_icon("star", size=24, color="#fbbf24")
    
    # Novo layout: segue o mesmo padrão visual de render_page_title()
    st.markdown(
        f"""
        <div style="
            position: relative;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            background: linear-gradient(135deg, 
                rgba(30, 41, 59, 0.8) 0%, 
                rgba(45, 55, 72, 0.9) 50%, 
                rgba(30, 41, 59, 0.8) 100%);
            padding: 35px 25px 28px;
            border-radius: 20px;
            text-align: center;
            font-family: 'Inter', sans-serif;
            border: 1px solid rgba(227, 236, 240, 0.3);
            box-shadow: 
                0 8px 32px rgba(0, 0, 0, 0.4),
                0 2px 16px rgba(227, 236, 240, 0.1),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
            margin-bottom: 30px;
            overflow: hidden;
            ">
            <div style="
                position: absolute;
                top: 0;
                left: -100%;
                width: 200%;
                height: 100%;
                background: linear-gradient(
                    90deg, 
                    transparent 0%,
                    rgba(227, 236, 240, 0.05) 40%,
                    rgba(227, 236, 240, 0.15) 50%,
                    rgba(227, 236, 240, 0.05) 60%,
                    transparent 100%
                );
                z-index: 1;
                animation: titleShimmer 6s infinite linear;
                "></div>
            <h1 style="
                position: relative;
                z-index: 2;
                margin: 0 0 10px 0;
                font-size: 3em;
                font-weight: 800;
                letter-spacing: -1px;
                background: linear-gradient(135deg, 
                    #e3ecf0 0%, 
                    #cbd5e1 30%, 
                    #e3ecf0 60%, 
                    #94a3b8 100%);
                background-clip: text;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: 0 4px 8px rgba(0, 0, 0, 0.4);
                filter: drop-shadow(0 2px 4px rgba(0, 0, 0, 0.6));
                ">
                {title_text}
            </h1>
            <div style="
                position: relative;
                width: 200px;
                height: 40px;
                margin: 6px auto 0;
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 2;
                ">
                <div style="
                    position: absolute;
                    top: 0;
                    left: 10px;
                    right: 10px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    ">
                    <div style="transform: translateY(-6px);">{star_icon}</div>
                    <div style="transform: translateY(-10px);">{star_icon}</div>
                    <div style="transform: translateY(-12px);">{star_icon}</div>
                    <div style="transform: translateY(-10px);">{star_icon}</div>
                    <div style="transform: translateY(-6px);">{star_icon}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def render_market_analysis_card_glass(insights: Dict[str, Dict[str, float]]) -> None:
    """
    Renders market analysis cards with glass effect.

    Args:
        insights (Dict): Dictionary containing premium and popular category insights
    """
    import pandas as pd
    colors = get_theme_colors()
    premium_info = insights['premium_info']
    popular_info = insights['popular_info']
    premium_categories = insights.get('premium_categories', pd.DataFrame())
    popular_categories = insights.get('popular_categories', pd.DataFrame())

    premium_color = "#8b5cf6"  # Roxo (Premium)
    popular_color = "#f59e0b"  # Âmbar (Populares)
    premium_bg = "linear-gradient(135deg, rgba(139,92,246,0.10), rgba(139,92,246,0.06))"
    popular_bg = "linear-gradient(135deg, rgba(245,158,11,0.10), rgba(245,158,11,0.06))"
    
    card_style = f"""
        background: {colors['bg_color']};
        backdrop-filter: blur(12px);
        border-radius: 20px;
        border: 1px solid {colors['border_color']};
        box-shadow: 0 4px 20px {colors['shadow_color']};
        padding: 25px;
        height: 100%;
        font-family: 'Inter', sans-serif;
    """

    # Contêiner externo transparente (remove o fundo escuro ao redor)
    outer_style = """
        background: transparent;
        backdrop-filter: none;
        border: none;
        box-shadow: none;
        padding: 0;
        height: 100%;
        font-family: 'Inter', sans-serif;
    """

    # Card único com divisão cromática entre Premium (esquerda) e Populares (direita)
    combined_html = f"""
    <div style="{outer_style}">
        <div style="display:flex; gap:14px; align-items:stretch;">
            <div style="flex:1; background: {premium_bg}; border:1px solid rgba(139,92,246,0.35); border-radius:14px; padding:16px;">
                <div style="display:flex; align-items:center; gap:8px; font-size:18px; font-weight:700; margin-bottom:10px; color:{premium_color}">💎 Categorias Premium</div>
                <div style="font-size:15px; line-height:1.7; color:{colors['text_color']};">
                    <div style="margin-bottom:6px;">{premium_info['count']} categorias com alto valor e avaliação</div>
                    <div style="margin-bottom:6px;">Preço médio: <strong>R$ {premium_info['avg_price']:.2f}</strong></div>
                    <div>Avaliação média: <strong>{premium_info['avg_rating']:.2f} / 5.0</strong></div>
                </div>
            </div>
            <div style="width:2px; background: linear-gradient(180deg, {premium_color}, {popular_color}); border-radius:2px;"></div>
            <div style="flex:1; background: {popular_bg}; border:1px solid rgba(245,158,11,0.35); border-radius:14px; padding:16px;">
                <div style="display:flex; align-items:center; gap:8px; font-size:18px; font-weight:700; margin-bottom:10px; color:{popular_color}">🔥 Categorias Populares</div>
                <div style="font-size:15px; line-height:1.7; color:{colors['text_color']};">
                    <div style="margin-bottom:6px;">{popular_info['count']} com alto volume e preço acessível</div>
                    <div style="margin-bottom:6px;">Volume médio: <strong>{popular_info['avg_volume']:.0f} unidades</strong></div>
                    <div>Preço médio: <strong>R$ {popular_info['avg_price']:.2f}</strong></div>
                </div>
            </div>
        </div>
    </div>
    """
    st.markdown(combined_html, unsafe_allow_html=True)
    
    # Renderizar listas expansíveis abaixo dos cards
    if not premium_categories.empty or not popular_categories.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            if not premium_categories.empty:
                with st.expander(f"💎 Ver {len(premium_categories)} Categorias Premium", expanded=False):
                    for idx, row in premium_categories.head(10).iterrows():
                        st.markdown(f"""
                        <div style="
                            background: {premium_bg};
                            border-left: 3px solid {premium_color};
                            padding: 10px 12px;
                            margin-bottom: 8px;
                            border-radius: 8px;
                            font-size: 14px;
                            line-height: 1.6;
                        ">
                            <div style="font-weight: 600; color: {premium_color}; margin-bottom: 4px;">{row['category']}</div>
                            <div style="color: {colors['text_color']}; font-size: 13px;">
                                💰 Preço médio: <strong>R$ {row['avg_price']:.2f}</strong><br>
                                ⭐ Avaliação: <strong>{row['avg_rating']:.2f} / 5.0</strong><br>
                                📦 Vendas: <strong>{int(row['total_sales'])} unidades</strong>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    if len(premium_categories) > 10:
                        st.info(f"Mostrando top 10 de {len(premium_categories)} categorias premium")
        
        with col2:
            if not popular_categories.empty:
                with st.expander(f"🔥 Ver {len(popular_categories)} Categorias Populares", expanded=False):
                    for idx, row in popular_categories.head(10).iterrows():
                        st.markdown(f"""
                        <div style="
                            background: {popular_bg};
                            border-left: 3px solid {popular_color};
                            padding: 10px 12px;
                            margin-bottom: 8px;
                            border-radius: 8px;
                            font-size: 14px;
                            line-height: 1.6;
                        ">
                            <div style="font-weight: 600; color: {popular_color}; margin-bottom: 4px;">{row['category']}</div>
                            <div style="color: {colors['text_color']}; font-size: 13px;">
                                📦 Vendas: <strong>{int(row['total_sales'])} unidades</strong><br>
                                💰 Preço médio: <strong>R$ {row['avg_price']:.2f}</strong><br>
                                ⭐ Avaliação: <strong>{row['avg_rating']:.2f} / 5.0</strong>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    if len(popular_categories) > 10:
                        st.info(f"Mostrando top 10 de {len(popular_categories)} categorias populares")

def render_insight_card(title: str, value: str, trend: str, trend_icon: str, help_text: Optional[str] = None, help_text_color: Optional[str] = None, tooltip_text: Optional[str] = None) -> str:
    """
    Creates an insight card with enhanced glass effect and visual impact.

    Args:
        title: Title of the insight
        value: Main value of the insight
        trend: Trend description
        trend_icon: Trend icon
        help_text: Optional help text (appears below trend)
        help_text_color: Optional custom color for help text (defaults to trend_color if None)
        tooltip_text: Optional tooltip text to appear next to title (via hover icon)

    Returns:
        HTML formatted card
    """
    colors = get_theme_colors()
    
    # For SVG icons, extract color from the icon itself or use default
    # If trend_icon contains SVG, use a default color, otherwise use emoji mapping
    if trend_icon.startswith('<svg'):
        trend_color = "#4ECDC4"  # Default teal color for SVG icons
    else:
        trend_colors = {
            "📈": "#10b981",  # Verde esmeralda
            "📉": "#ef4444",  # Vermelho
            "➡️": "#3b82f6",  # Azul
            "❓": "#94a3b8"   # Cinza
        }
        trend_color = trend_colors.get(trend_icon, "#94a3b8")
    
    # Construct title with optional tooltip
    title_html = title
    if tooltip_text:
        safe_tooltip = (tooltip_text or "").replace('"', "&quot;")
        tooltip_icon = (
            f"<span title=\"{safe_tooltip}\" "
            f"style=\"display:inline-flex;align-items:center;justify-content:center;"
            f"width:16px;height:16px;margin-left:6px;border-radius:999px;"
            f"background:rgba(255,255,255,0.1);color:rgba(226,232,240,0.9);"
            f"font-size:10px;font-weight:700;cursor:help;vertical-align:middle;\">ⓘ</span>"
        )
        title_html = f"{title}{tooltip_icon}"

    html = f"""
    <div style="
        position: relative;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        background: linear-gradient(135deg, 
            rgba(30, 41, 59, 0.8), 
            rgba(45, 55, 72, 0.9), 
            rgba(30, 41, 59, 0.8));
        padding: 30px 25px;
        border-radius: 20px;
        text-align: center;
        font-family: 'Inter', sans-serif;
        border: 1px solid rgba(227, 236, 240, 0.2);
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.3),
            0 2px 16px rgba(227, 236, 240, 0.1),
            inset 0 1px 0 rgba(255, 255, 255, 0.1);
        margin-bottom: 15px;
        overflow: hidden;
        ">
        <div style="
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, rgba(227, 236, 240, 0.05), rgba(227, 236, 240, 0.02));
            z-index: -1;
            "></div>
        <div style="
            font-size: 16px; 
            margin-bottom: 15px; 
            position: relative; 
            color: rgba(148, 163, 184, 0.9);
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
            display: flex;
            align-items: center;
            justify-content: center;
            ">
            {title_html}
        </div>
        <div style="
            font-size: 36px; 
            font-weight: 800; 
            margin-bottom: 15px; 
            position: relative; 
            background: linear-gradient(135deg, #e3ecf0, #cbd5e1);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 0 20px rgba(227, 236, 240, 0.3);
            filter: drop-shadow(0 2px 4px rgba(0, 0, 0, 0.5));
            letter-spacing: -1px;
            ">
            {value}
        </div>
        <div style="
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
            color: {trend_color};
            position: relative;
            font-weight: 600;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
            ">
            <span style="margin-right: 8px; font-size: 18px;">{trend_icon}</span>
            <span>{trend}</span>
        </div>
        {f'<div style="font-size: 14px; margin-top: 15px; opacity: 0.85; position: relative; color: {help_text_color or trend_color}; font-style: italic; padding: 10px; background: rgba(227, 236, 240, 0.05); border-radius: 8px; border-left: 3px solid rgba(227, 236, 240, 0.3);">{help_text}</div>' if help_text else ''}
    </div>
    """
    return html

    
def render_text_glass_card(title: str, content: List[str], icon: str = "", help_text: Optional[str] = None) -> str:
    """
    Creates a text card with enhanced glass effect and improved contrast.

    Args:
        title: Title of the card
        content: List of strings to display as content
        icon: Icon to display next to the title
        help_text: Optional help text

    Returns:
        HTML formatted card
    """
    colors = get_theme_colors()
    content_html = "<ul style='padding-left: 20px; margin: 0;'>" + "".join(f"<li style='margin-bottom: 8px; color: #0e2949; font-weight: 500; line-height: 1.6;'>{item}</li>" for item in content) + "</ul>"

    html = f"""
    <div style="
        position: relative;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        background: linear-gradient(135deg, 
            rgba(30, 41, 59, 0.85), 
            rgba(45, 55, 72, 0.9), 
            rgba(30, 41, 59, 0.85));
        padding: 30px 25px;
        border-radius: 20px;
        text-align: left;
        font-family: 'Inter', sans-serif;
        border: 1px solid rgba(227, 236, 240, 0.25);
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.3),
            0 2px 16px rgba(227, 236, 240, 0.1),
            inset 0 1px 0 rgba(255, 255, 255, 0.1);
        margin-bottom: 15px;
        overflow: hidden;
        ">
        <div style="
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, rgba(227, 236, 240, 0.04), rgba(227, 236, 240, 0.02));
            z-index: -1;
            "></div>
        <div style="
            font-size: 20px; 
            margin-bottom: 18px; 
            position: relative; 
            color: #e3ecf0;
            font-weight: 700;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
            letter-spacing: -0.3px;
            ">
            {icon} {title}
        </div>
        <div style="
            font-size: 16px; 
            position: relative; 
            ">
            {content_html}
        </div>
        {f'<div style="font-size: 14px; margin-top: 15px; opacity: 0.85; position: relative; color: rgba(148, 163, 184, 0.9); font-style: italic; padding: 10px; background: rgba(227, 236, 240, 0.05); border-radius: 8px; border-left: 3px solid rgba(227, 236, 240, 0.3);">{help_text}</div>' if help_text else ''}
    </div>
    """
    return html

def render_recommendation_card(rec: Dict[str, Any]) -> None:
    """
    Renders a recommendation card with enhanced glass effect and improved contrast.
    
    Args:
        rec: Dictionary containing recommendation data
    """
    colors = get_theme_colors()
    cor_map = {
        "Aumentar significativamente": "#2ecc71",
        "Aumentar moderadamente": "#27ae60",
        "Reduzir moderadamente": "#e67e22",
        "Reduzir significativamente": "#e74c3c",
        "Manter": "#3498db"
    }
    cor = cor_map.get(rec['action'], "#7f8c8d")
    
    st.markdown(f"""
    <div style="
        position: relative;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        background: linear-gradient(135deg, 
            rgba(30, 41, 59, 0.9), 
            rgba(45, 55, 72, 0.95), 
            rgba(30, 41, 59, 0.9));
        border-radius: 20px;
        padding: 28px; 
        margin-bottom: 15px; 
        border: 1px solid rgba(227, 236, 240, 0.2);
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.3),
            0 2px 16px rgba(227, 236, 240, 0.1),
            inset 0 1px 0 rgba(255, 255, 255, 0.1);
        overflow: hidden;
        ">
        <div style="
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, rgba(227, 236, 240, 0.03), rgba(227, 236, 240, 0.01));
            z-index: -1;
            "></div>
        <h4 style="
            margin: 0 0 18px 0; 
            color: #e3ecf0;
            font-size: 18px;
            font-weight: 700;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
            letter-spacing: -0.3px;
            ">📦 {rec['category']}</h4>
        <ul style="
            margin: 0; 
            padding-left: 20px; 
            font-size: 16px; 
            color: #0e2949;
            line-height: 1.8;
            font-weight: 600;
            ">
            <li style="margin-bottom: 8px;"><strong>Ação:</strong> {rec['action']} <span style="color:{cor}; font-weight: 700; text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);">({rec['reason']})</span></li>
            <li style="margin-bottom: 8px;"><strong>Variação Prevista:</strong> {rec['variation']:.1f}%</li>
            <li style="margin-bottom: 8px;"><strong>Giro Diário:</strong> {rec['inventory_turnover']:.2f}</li>
            <li><strong>Estoque Ideal:</strong> {rec['ideal_stock']:.0f} unidades</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

def render_top_category_cards(top_categories_df: pd.DataFrame) -> None:
    """Renderiza cards das Top Categorias com cores semânticas dos quadrantes BCG."""
    colors = get_theme_colors()
    cols = st.columns(len(top_categories_df))

    quadrant_styles = {
        "Estrela Digital": {
            "bg": "linear-gradient(135deg, rgba(251,191,36,0.12), rgba(250,204,21,0.06))",
            "border": "rgba(251,191,36,0.35)",
            "header": "#fbbf24",
            "icon": "⭐",
            "badge": "INVESTIR"
        },
        "Vaca Leiteira": {
            "bg": "linear-gradient(135deg, rgba(34,197,94,0.12), rgba(16,185,129,0.06))",
            "border": "rgba(16,185,129,0.35)",
            "header": "#10b981",
            "icon": "🐄",
            "badge": "OTIMIZAR"
        },
        "Interrogação": {
            "bg": "linear-gradient(135deg, rgba(59,130,246,0.12), rgba(96,165,250,0.06))",
            "border": "rgba(59,130,246,0.35)",
            "header": "#3b82f6",
            "icon": "❓",
            "badge": "TESTAR"
        },
        "Abacaxi": {
            "bg": "linear-gradient(135deg, rgba(239,68,68,0.12), rgba(248,113,113,0.06))",
            "border": "rgba(239,68,68,0.35)",
            "header": "#ef4444",
            "icon": "🐢",
            "badge": "DESCONTINUAR"
        }
    }

    from io import BytesIO
    import pandas as _pd
    import base64 as _b64

    filtered_df = st.session_state.get("filtered_df")
    if not isinstance(filtered_df, pd.DataFrame) or filtered_df.empty:
        df_all = st.session_state.get("df_all")
        filtered_df = df_all.copy() if isinstance(df_all, pd.DataFrame) else None

    for idx, (_, row) in enumerate(top_categories_df.iterrows()):
        category_name = row["category"]
        quadrant = row.get("bcg_quadrant", "Interrogação")
        strategy = row.get("bcg_strategy", "")
        market_share = row.get("market_share", 0)
        growth_rate = row.get("growth_rate", 0)
        composite_score = row.get("composite_score", 0)

        style = quadrant_styles.get(quadrant, quadrant_styles["Interrogação"])

        buffer = None
        if filtered_df is not None and not filtered_df.empty and "product_id" in filtered_df.columns:
            df_cat = filtered_df[filtered_df["product_category_name"] == category_name].copy()
            if not df_cat.empty:
                df_export = (
                    df_cat.groupby(["product_category_name", "product_id"], as_index=False)
                    .agg(
                        total_revenue=("price", "sum"),
                        total_orders=("order_id", "count"),
                        avg_rating=("review_score", "mean"),
                        avg_price=("price", "mean")
                    )
                )
                if "composite_score" in df_cat.columns:
                    df_export["composite_score"] = df_export["product_id"].map(
                        df_cat.drop_duplicates("product_id").set_index("product_id")["composite_score"]
                    )
                desired_cols = [
                    "product_category_name",
                    "product_id",
                    "total_revenue",
                    "total_orders",
                    "avg_rating",
                    "avg_price",
                    "composite_score",
                ]
                df_export = df_export.reindex(columns=desired_cols)
                buffer = BytesIO()
                with _pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    df_export.to_excel(writer, index=False, sheet_name="Produtos")
                buffer.seek(0)

        with cols[idx]:
            hint_text = "Sem dados para exportar"
            download_html = ""
            if buffer is not None:
                download_url = (
                    "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," +
                    _b64.b64encode(buffer.getvalue()).decode("utf-8")
                )
                hint_text = "⬇️ Aprofundar Análise ⬇️"
                download_html = (
                    f"<a style='display:inline-block;width:100%;margin-top:12px;padding:10px 18px;"
                    "text-decoration:none;text-align:center;background:linear-gradient(135deg, rgba(52,63,82,0.95), rgba(28,36,48,0.95));"
                    "color:#e3ecf0 !important;border:1px solid rgba(227,236,240,0.25);border-radius:12px;font-weight:700;font-size:13px;"
                    "letter-spacing:0.4px;text-transform:uppercase;box-shadow:0 6px 18px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.08);"
                    "transition: transform .2s ease, box-shadow .2s ease, border-color .2s ease;' "
                    f"href='{download_url}' download='{category_name}_produtos.xlsx'>📊 Baixar {category_name}</a>"
                )

            st.markdown(
                f"""
                <div style="
                    position: relative;
                    backdrop-filter: blur(14px) saturate(120%);
                    -webkit-backdrop-filter: blur(14px) saturate(120%);
                    background: {style['bg']};
                    border-radius: 18px;
                    border: 1px solid {style['border']};
                    padding: 22px;
                    margin-bottom: 16px;
                    text-align: center;
                    min-width: 220px;
                    overflow: hidden;
                    box-shadow: 0 8px 28px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.08);
                ">
                    <div style="font-size: 2.1em; margin-bottom: 6px; filter: drop-shadow(0 2px 6px rgba(0,0,0,0.35));">{style['icon']}</div>
                    <div style="font-weight: 700; font-size: 1.12em; margin-bottom: 8px; letter-spacing: 0.2px; color: {style['header']}; text-shadow: 0 1px 2px rgba(0,0,0,0.35);">
                        {category_name}
                    </div>
                    <div style="
                        display: inline-block;
                        padding: 6px 14px;
                        margin-bottom: 12px;
                        border-radius: 999px;
                        background: rgba(255,255,255,0.08);
                        border: 1px solid {style['header']}88;
                        color: {style['header']};
                        font-weight: 700;
                        font-size: 0.75rem;
                        letter-spacing: 0.3px;
                        text-transform: uppercase;
                    ">
                        {style['badge']}
                    </div>
                    <div style="font-size: 0.95em; line-height: 1.55; color: {colors['text_color']}; text-align:left;">
                        <div><b>Estratégia:</b> {strategy}</div>
                        <div><b>Market Share:</b> {market_share:.1f}%</div>
                        <div><b>Growth Rate:</b> {growth_rate:.1f}%</div>
                        <div><b>Score:</b> {composite_score:.3f}</div>
                        <div><b>Preço Médio:</b> R$ {row.get('avg_price', 0):,.2f}</div>
                        <div><b>Avaliação Média:</b> {row.get('avg_rating', 0):.2f}/5.0</div>
                        <div><b>Volume de Vendas:</b> {int(row.get('total_sales', 0))} pedidos</div>
                    </div>
                    <div style="font-size: 0.85em; opacity: 0.75; margin-top: 8px; color: {colors['text_color']};">
                        {hint_text}
                    </div>
                    {download_html or "<div style='display:inline-block;width:100%;margin-top:12px;padding:10px 18px;text-align:center;border:1px dashed rgba(255,255,255,0.25);border-radius:12px;font-weight:700;font-size:13px;opacity:0.6;'>📁 Sem dados</div>"}
                </div>
                """,
                unsafe_allow_html=True,
            )

def kpi_card(title: str, value: str, help_text: Optional[str] = None) -> None:
    """
    Creates a KPI card with glass effect and enhanced visual impact.

    Args:
        title: Title of the KPI
        value: Value of the KPI
        help_text: Optional help text
    """
    colors = get_theme_colors()

    st.markdown(
        f"""
        <div style="
            position: relative;
            backdrop-filter: blur(16px);
            background: linear-gradient(135deg, {colors['bg_color']}, rgba(227, 236, 240, 0.08));
            padding: 25px;
            border-radius: 20px;
            text-align: center;
            font-family: 'Inter', sans-serif;
            border: 1px solid rgba(227, 236, 240, 0.2);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3), 0 2px 8px rgba(227, 236, 240, 0.1);
            overflow: hidden;
            ">
            <div style="
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(135deg, rgba(227, 236, 240, 0.05), rgba(227, 236, 240, 0.02));
                z-index: -1;
                "></div>
            <div style="
                color: rgba(203, 213, 225, 0.9);
                font-size: 16px;
                font-weight: 600;
                margin-bottom: 12px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
                ">{title}</div>  
            <div style="
                font-size: 42px;
                font-weight: 700;
                color: #f8fafc;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.4);
                letter-spacing: -1px;
                background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
                background-clip: text;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.3));
                ">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    if help_text:
        st.markdown(
        f"""
        <p style="font-size: 18px; color: gray; text-align: center; font-style: italic;">
            ℹ️ {help_text}
        </p>
        """,
        unsafe_allow_html=True
    )

def render_kpi_block(kpi_values: Optional[Dict[str, str]] = None, cols_per_row: int = 3) -> None:
    """
    Renders a block of KPIs with glass effect.

    Args:
        kpi_values: Dictionary with KPI values
        cols_per_row: Number of columns per row (default: 3)
    """
    if kpi_values:
        num_kpis = len(kpi_values)
        num_rows = (num_kpis + cols_per_row - 1) // cols_per_row
        cols = st.columns(cols_per_row)
        
        for i, (kpi_name, kpi_value) in enumerate(kpi_values.items()):
            row = i // cols_per_row
            col = i % cols_per_row
            with cols[col]:
                kpi_card(kpi_name, kpi_value)
                st.markdown("<div style='margin-bottom: 18px;'></div>", unsafe_allow_html=True)

def render_plotly_glass_card(title: str, fig: go.Figure, height: int = 620) -> None:
    """
    Renders a Plotly figure with glass effect.

    Args:
        title: Title of the chart
        fig: Plotly figure object
        height: Height of the container in pixels (default: 620)
    """
    colors = get_theme_colors()
    
    fig.update_layout(
        margin=dict(l=80, r=20, t=40, b=80),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Inter, sans-serif",
            size=14,
        ),
        title=dict(
            text=title,
            font=dict(size=22),
            x=0.5,
            xanchor='center'
        ),
        shapes=[
            dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(
                    color=colors['border_color'],
                    width=1
                ),
                fillcolor=colors['bg_color'],
                layer="below"
            )
        ]
    )
    
    fig.update_yaxes(
        automargin=True,
        ticks="outside",
        ticklabelposition="outside left",
        zerolinewidth=1
    )
    
    fig.update_xaxes(
        ticklabelposition="outside bottom",
        ticks="outside",
        zerolinewidth=1
    )
    
    fig.update_traces(
        hoverlabel=dict(
            bgcolor=colors['bg_color'],
            bordercolor=colors['border_color'],
        )
    )
    
    # Generate unique key for the chart
    unique_key = _generate_unique_key("plotly")
    
    st.plotly_chart(fig, use_container_width=True, height=height, key=unique_key)

def render_silver_gradient_navbar() -> str:
    """
    Renderiza uma barra de navegação com efeito de gradiente prateado inspirado na imagem de referência.
    
    Returns:
        str: Página atual selecionada
    """
    # Aplicar o tema de fundo aprimorado
    apply_enhanced_background()
    
    pages = [
        "Visão Geral",
        "Aquisição e Retenção", 
        "Comportamento do Cliente",
        "Análise de Portfólio",
        "Análise Estratégica",
        "Análise de ROI",
        "Casos de Uso"
    ]
    
    # Garantir que os filtros estão inicializados
    if 'pagina_atual' not in st.session_state:
        # Importar apenas se necessário para evitar ciclos de importação
        from utils.filtros import initialize_filters
        initialize_filters()
    
    # Verificar se há uma página na URL e se ela mudou
    if 'page' in st.query_params:
        page_from_url = st.query_params['page']
        if page_from_url in pages and page_from_url != st.session_state.pagina_atual:
            # Atualizar a página atual
            st.session_state.pagina_atual = page_from_url
            st.session_state.current_page = page_from_url  # compatibilidade
            st.rerun()
    
    # Usar a chave única
    current_page = st.session_state.pagina_atual
    
    # CSS para o gradiente prateado avançado
    silver_gradient_css = """
    <style>
    .silver-gradient-navbar {
        position: relative;
        background: linear-gradient(135deg, 
            rgba(30, 41, 59, 0.8) 0%, 
            rgba(45, 55, 72, 0.9) 50%, 
            rgba(30, 41, 59, 0.8) 100%);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        padding: 18px 24px;
        margin-bottom: 24px;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
        overflow: hidden;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        border-top: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    /* Efeito de brilho prateado principal */
    .silver-gradient-navbar::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 200%;
        height: 100%;
        background: linear-gradient(
            90deg, 
            transparent 0%,
            rgba(255, 255, 255, 0.05) 40%,
            rgba(255, 255, 255, 0.2) 50%,
            rgba(255, 255, 255, 0.05) 60%,
            transparent 100%
        );
        z-index: 1;
        animation: silver-shine 8s infinite linear;
    }
    
    /* Efeito de textura metálica sutil */
    .silver-gradient-navbar::after {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-image: 
            repeating-linear-gradient(
                45deg,
                rgba(255, 255, 255, 0.03),
                rgba(255, 255, 255, 0.03) 1px,
                transparent 1px,
                transparent 2px
            );
        z-index: 0;
        pointer-events: none;
    }
    
    @keyframes silver-shine {
        0% { transform: translateX(-50%); }
        100% { transform: translateX(50%); }
    }
    
    .silver-gradient-navbar nav {
        position: relative;
        z-index: 2;
        display: inline-block;
        font-size: 15px;
        font-weight: 500;
    }
    
    .nav-separator {
        color: rgba(148, 163, 184, 0.5);
        margin: 0 8px;
        text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
    }
    
    .silver-gradient-navbar .nav-item {
        position: relative;
        display: inline-block;
        padding: 5px 10px;
        margin: 0 2px;
        transition: all 0.3s ease;
        text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
        cursor: pointer;
        border-radius: 6px;
        user-select: none;
        text-decoration: none;
    }
    
    .silver-gradient-navbar .nav-item:hover:not(.active) {
        background: rgba(255, 255, 255, 0.1);
        color: #cbd5e1;
    }
    
    .silver-gradient-navbar .nav-item:active {
        transform: scale(0.96);
        opacity: 0.9;
    }
    
    .silver-gradient-navbar .nav-item.active {
        color: #e3ecf0;
        text-shadow: 0 0 10px rgba(227, 236, 240, 0.5);
        background: linear-gradient(135deg, 
            rgba(255, 255, 255, 0.15) 0%, 
            rgba(255, 255, 255, 0.05) 100%);
    }
    
    .silver-gradient-navbar .nav-item:not(.active) {
        color: #94a3b8;
    }
    
    /* Indicador da página ativa */
    .silver-gradient-navbar .nav-item.active::after {
        content: '';
        position: absolute;
        bottom: -6px;
        left: 50%;
        transform: translateX(-50%);
        width: 30px;
        height: 3px;
        background: linear-gradient(90deg, 
            rgba(227, 236, 240, 0.1) 0%,
            rgba(227, 236, 240, 0.8) 50%,
            rgba(227, 236, 240, 0.1) 100%);
        border-radius: 3px;
        box-shadow: 0 0 8px rgba(227, 236, 240, 0.5);
    }
    
    /* Responsividade para telas menores */
    @media (max-width: 768px) {
        .silver-gradient-navbar nav {
            font-size: 13px;
        }
        
        .nav-separator {
            margin: 0 4px;
        }
        
        .silver-gradient-navbar .nav-item {
            padding: 4px 6px;
            margin: 0 1px;
        }
    }
    </style>
    """
    
    # Renderizar a barra de navegação com o estilo de gradiente prateado e links
    nav_items = []
    
    # Construir query string base com filtros atuais (sem page)
    filter_params = []
    
    # Adicionar filtros apenas se diferentes dos padrões
    if st.session_state.periodo_analise != "Todo o período":
        from urllib.parse import quote
        filter_params.append(f"periodo={quote(st.session_state.periodo_analise)}")
    
    if st.session_state.marketing_spend != 50000:
        filter_params.append(f"marketing={st.session_state.marketing_spend}")
    
    if st.session_state.categoria_selecionada != "Todas as categorias":
        from urllib.parse import quote
        filter_params.append(f"categoria={quote(st.session_state.categoria_selecionada)}")
    
    # Persistir escolha de dataset (upload/local) e parâmetros relacionados
    try:
        from urllib.parse import quote
        dataset_choice = st.session_state.get('dataset_choice')
        parquet_path = st.session_state.get('processed_file_path')
        if dataset_choice == 'upload' and parquet_path:
            filter_params.append("data=upload")
            filter_params.append(f"parquet={quote(str(parquet_path))}")
        elif dataset_choice == 'local':
            filter_params.append("data=local")
        else:
            # Compatibilidade antiga: preservar dataset selecionado quando não for demo
            if st.session_state.get('dataset_selecionado', "Dados Integrados (Local)") != "Dados Integrados (Local)":
                filter_params.append(f"dataset={quote(st.session_state['dataset_selecionado'])}")
    except Exception:
        pass

    # Novo: persistir marketplaces selecionados na navbar
    try:
        selected_marketplaces = st.session_state.get("selected_marketplaces", [])
        if isinstance(selected_marketplaces, list) and len(selected_marketplaces) > 0:
            from urllib.parse import quote
            csv_val = ",".join(selected_marketplaces)
            filter_params.append(f"marketplaces={quote(csv_val)}")
    except Exception:
        pass
    
    # Novo: persistir datas personalizadas na navbar quando período for personalizado
    if st.session_state.get("periodo_analise") == "Período personalizado":
        custom_start = st.session_state.get("custom_start_date")
        custom_end = st.session_state.get("custom_end_date")
        if custom_start and custom_end:
            filter_params.append(f"start_date={custom_start.strftime('%Y-%m-%d')}")
            filter_params.append(f"end_date={custom_end.strftime('%Y-%m-%d')}")
    
    base_filters = "&".join(filter_params)
    
    for page in pages:
        is_active = page == current_page
        active_class = "active" if is_active else ""
        
        # Construir URL com filtros preservados
        from urllib.parse import quote
        page_query = f"page={quote(page)}"
        if base_filters:
            page_url = f"?{page_query}&{base_filters}"
        else:
            page_url = f"?{page_query}"
        
        nav_items.append(f'<a href="{page_url}" class="nav-item {active_class}" target="_self">{page}</a>')
    
    nav_html = f"""
    {silver_gradient_css}
    <div class="silver-gradient-navbar">
        <nav>
            {' <span class="nav-separator">•</span> '.join(nav_items)}
        </nav>
    </div>
    
    <script>
    // JavaScript melhorado para Streamlit Cloud
    document.addEventListener('DOMContentLoaded', function() {{
        var navLinks = document.querySelectorAll('.silver-gradient-navbar .nav-item');
        navLinks.forEach(function(link) {{
            link.addEventListener('click', function(e) {{
                e.preventDefault();
                
                // Usar window.location.href em vez de pushState + reload
                // Isso funciona melhor no Streamlit Cloud
                window.location.href = this.getAttribute('href');
            }});
        }});
    }});
    </script>
    """
    st.markdown(nav_html, unsafe_allow_html=True)
    
    return current_page

def render_echarts_glass_card(title: str, option: Dict[str, Any], height: int = 620) -> None:
    """
    Renders an ECharts chart with glass effect.

    Args:
        title: Title of the chart
        option: ECharts configuration dictionary
        height: Height of the container in pixels (default: 620)
    """
    colors = get_theme_colors()
    
    # Verificar se o título deve ser exibido (não vazio e não apenas espaços)
    show_title = title and title.strip()
    title_html = ""
    
    if show_title:
        title_html = f"""
            <h3 style="
                text-align: center;
                margin-bottom: 20px;
                font-family: 'Inter', sans-serif;
                font-size: 1.8em;
                font-weight: 700;
                letter-spacing: -0.5px;
                background: linear-gradient(135deg, 
                    #e3ecf0 0%, 
                    #cbd5e1 50%, 
                    #e3ecf0 100%);
                background-clip: text;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.4);
                filter: drop-shadow(0 1px 2px rgba(0, 0, 0, 0.5));
            ">{title}</h3>
        """
    
    # Adicionar estilo de vidro ao container
    st.markdown(
        f"""
        <div style="
            position: relative;
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            background: linear-gradient(135deg, {colors['bg_color']}, rgba(99, 102, 241, 0.1));
            padding: 25px;
            border-radius: 20px;
            border: 1px solid {colors['border_color']};
            box-shadow: 0 4px 20px {colors['shadow_color']};
            margin-bottom: 15px;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(135deg, rgba(99, 102, 241, 0.1), rgba(99, 102, 241, 0.05));
                z-index: -1;
            "></div>
            {title_html}
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Usa render_echarts_chart centralizado (fallback HTML+CDN quando componente não carrega)
    try:
        from utils.echarts_charts import render_echarts_chart
        render_echarts_chart(option, height=height)
    except ImportError:
        st.warning("Não foi possível carregar o gráfico.")

def render_use_case_card_glass(title: str, description: str, impact: str, feasibility: str, solution: str, icon_name: str, section_type: str = "default", article_url: str = "") -> None:
    """
    Renders a use case card with glass effect styling using streamlit.components.v1.html.
    
    Args:
        title (str): Title of the use case
        description (str): Description of the use case
        impact (str): Impact description
        feasibility (str): Feasibility assessment
        solution (str): Insight Expert solution description
        icon_name (str): Name of the SVG icon to display
        section_type (str): Type of the section (default: "default")
        article_url (str): URL of the article (default: "")
    """
    from utils.svg_icons import get_svg_icon
    import streamlit.components.v1 as components
    
    icon = get_svg_icon(icon_name, size=32)

    # Definir cores específicas para cada seção (MUITO MAIS SUTIS)
    section_colors = {
        "gestao_clientes": {
            "primary": "rgba(59, 130, 246, 0.03)",      # Azul muito sutil
            "secondary": "rgba(96, 165, 250, 0.02)",     # Azul ultra sutil
            "accent": "rgba(59, 130, 246, 0.08)"         # Azul sutil para bordas
        },
        "analise_estrategica": {
            "primary": "rgba(139, 92, 246, 0.03)",      # Roxo muito sutil
            "secondary": "rgba(167, 139, 250, 0.02)",   # Roxo ultra sutil
            "accent": "rgba(139, 92, 246, 0.08)"        # Roxo sutil para bordas
        },
        "otimizacao_produtos": {
            "primary": "rgba(245, 158, 11, 0.03)",      # Âmbar muito sutil
            "secondary": "rgba(251, 191, 36, 0.02)",    # Âmbar ultra sutil
            "accent": "rgba(245, 158, 11, 0.08)"        # Âmbar sutil para bordas
        },
        "default": {
            "primary": "rgba(30, 41, 59, 0.8)",
            "secondary": "rgba(45, 55, 72, 0.9)",
            "accent": "rgba(227, 236, 240, 0.25)"
        }
    }
    section_color= section_colors.get(section_type, section_colors["default"])

    # Determinar se o card é clicável
    is_clickable= bool(article_url.strip())
    cursor_stytle= "pointer" if is_clickable else "default"
    click_behavior = f"onclick=\"window.open('{article_url}', '_blank')\"" if is_clickable else ""
    # HTML completo com CSS inline para efeito glass
    card_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            @keyframes glass-shimmer {{
                0% {{ transform: translateX(-50%); }}
                100% {{ transform: translateX(50%); }}
            }}
            
            .glass-card {{
                background: linear-gradient(150deg,
                    {section_color['primary']} 0%,
                    {section_color['secondary']} 50%,
                    {section_color['primary']} 100%);
                backdrop-filter: blur(18px);
                -webkit-backdrop-filter: blur(18px);
                border: 1px solid {section_color['accent']};
                border-radius: 14px;
                padding: 16px 16px 14px 16px;
                box-shadow:
                    0 10px 36px rgba(0, 0, 0, 0.35),
                    0 2px 16px {section_color['accent']},
                    inset 0 1px 0 rgba(255, 255, 255, 0.08);
                position: relative;
                overflow: hidden;
                margin: 6px 0 22px 0;
                font-family: 'Inter', sans-serif;
                transition: all 0.25s ease;
                cursor: {cursor_stytle};
                display: flex;
                flex-direction: column;
                gap: 10px;
            }}
            
            .glass-card:hover {{
                transform: translateY(-2px);
                box-shadow: 
                    0 14px 44px rgba(0, 0, 0, 0.45),
                    0 4px 20px {section_color['accent']};
            }}
            
            .glass-card::before {{
                content: '';
                position: absolute;
                top: -20%;
                left: -120%;
                width: 240%;
                height: 140%;
                background: linear-gradient(90deg,
                    transparent 0%,
                    {section_color['accent']} 18%,
                    {section_color['accent']} 32%,
                    transparent 48%);
                transform: translateX(-100%);
                animation: glass-shimmer 9s infinite ease-in-out;
            }}
            
            .glass-card-content {{
                position: relative;
                z-index: 2;
                display: flex;
                flex-direction: column;
                gap: 10px;
            }}
            
            .card-title {{
                justify-content: center;
                color: #e2e8f0;
                margin: 0;
                font-size: 1.1em;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 10px;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
                line-height: 1.3;
            }}
            
            .card-section {{
                color: #e2e8f0;
                line-height: 1.6;
                margin: 0;
                text-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
                font-weight: 500;
                font-size: 0.96em;
            }}
            
            .card-section:last-child {{
                margin: 0;
            }}
            
            .section-label {{
                color: #94a3b8;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                font-size: 0.85em;
            }}

            .article-indicator {{
                position: absolute;
                top: 14px;
                right: 14px;
                background: rgba(59, 130, 246, 0.2);
                color: #60a5fa;
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 0.7em;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border: 1px solid rgba(59, 130, 246, 0.3);
                z-index: 3;
            }}
            .click-hint {{
                position: absolute;
                bottom: 12px;
                right: 12px;
                color: #94a3b8;
                font-size: 0.78em;
                font-style: italic;
                opacity: 0.7;
                z-index: 3;
            }}

        </style>
    </head>
    <body>
        <div class="glass-card" {click_behavior}>
            <div class="glass-card-content">
            {f'<div class="article-indicator">Artigo</div>' if is_clickable else ''}
                <div class="card-title">
                    {icon} {title}
                </div>          
                <div class="card-section">
                    <span class="section-label">Descrição:</span> {description}
                </div>
                <div class="card-section">
                    <span class="section-label">Impacto:</span> {impact}
                </div>
                <div class="card-section">
                    <span class="section-label">Viabilidade:</span> {feasibility}
                </div>
                <div class="card-section">
                    <span class="section-label">Solução Insight Expert:</span> {solution}
                </div>
                {f'<div class="click-hint">Clique para ver o artigo</div>' if is_clickable else ''}
            </div>
        </div>
    </body>
    </html>
    """
    # Calcular altura dinâmica baseada no conteúdo
    content_length = len(description) + len(impact) + len(feasibility) + len(solution)
    dynamic_height = max(360, min(520, 260 + (content_length // 70) * 32))
    
    # Renderizar usando components.html
    components.html(card_html, height=dynamic_height, scrolling=False)

def render_use_case_card_metallic_html(title: str, description: str, impact: str, feasibility: str, solution: str, icon_name: str, section_type: str = "default", article_url: str = "") -> None:
    """
    Renders a use case card with metallic styling using streamlit.components.v1.html.
    
    Args:
        title (str): Title of the use case
        description (str): Description of the use case
        impact (str): Impact description
        feasibility (str): Feasibility assessment
        solution (str): Insight Expert solution description
        icon_name (str): Name of the SVG icon to display
        section_type (str): Type of the section (default: "default")
        article_url (str): URL of the article (default: "")
    """
    from utils.svg_icons import get_svg_icon
    import streamlit.components.v1 as components
    
    icon = get_svg_icon(icon_name, size=32)

    # Definir cores específicas para cada seção (MUITO MAIS SUTIS)
    section_colors = {
        "gestao_clientes": {
            "primary": "rgba(59, 130, 246, 0.03)",      # Azul muito sutil
            "secondary": "rgba(96, 165, 250, 0.02)",     # Azul ultra sutil
            "accent": "rgba(59, 130, 246, 0.08)"         # Azul sutil para bordas
        },
        "analise_estrategica": {
            "primary": "rgba(139, 92, 246, 0.03)",      # Roxo muito sutil
            "secondary": "rgba(167, 139, 250, 0.02)",   # Roxo ultra sutil
            "accent": "rgba(139, 92, 246, 0.08)"        # Roxo sutil para bordas
        },
        "otimizacao_produtos": {
            "primary": "rgba(245, 158, 11, 0.03)",      # Âmbar muito sutil
            "secondary": "rgba(251, 191, 36, 0.02)",    # Âmbar ultra sutil
            "accent": "rgba(245, 158, 11, 0.08)"        # Âmbar sutil para bordas
        },
        "default": {
            "primary": "rgba(30, 41, 59, 0.8)",
            "secondary": "rgba(45, 55, 72, 0.9)",
            "accent": "rgba(227, 236, 240, 0.25)"
        }
    }
    is_clickable = bool(article_url.strip())
    cursor_style = "pointer" if is_clickable else "default"
    click_behavior = f"onclick=\"window.open('{article_url}', '_blank')\"" if is_clickable else ""

    # Obter cores da seção ou usar padrão
    section_color = section_colors.get(section_type, section_colors["default"])
    

    # HTML completo com CSS inline para efeito glass
    card_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            @keyframes glass-shimmer {{
                0% {{ transform: translateX(-100%); }}
                100% {{ transform: translateX(100%); }}
            }}
            
    .glass-card {{
        background: linear-gradient(150deg,
            {section_color['primary']} 0%,
            {section_color['secondary']} 50%,
            {section_color['primary']} 100%);
        backdrop-filter: blur(18px);
        -webkit-backdrop-filter: blur(18px);
        border: 1px solid {section_color['accent']};
        border-radius: 14px;
        padding: 16px 16px 14px 16px;
        box-shadow:
            0 10px 36px rgba(0, 0, 0, 0.35),
            0 2px 16px {section_color['accent']},
            inset 0 1px 0 rgba(255, 255, 255, 0.08);
        position: relative;
        overflow: hidden;
        margin: 6px 0 22px 0;
        font-family: 'Inter', sans-serif;
        transition: all 0.25s ease;
        cursor: {cursor_style};
        display: flex;
        flex-direction: column;
        gap: 10px;
    }}
            
            .glass-card:hover {{
                transform: translateY(-2px);
                box-shadow: 
            0 14px 44px rgba(0, 0, 0, 0.45),
                    0 4px 20px {section_color['accent']};
            }}
            
            .glass-card::before {{
                content: '';
                position: absolute;
        top: -20%;
        left: -120%;
        width: 240%;
        height: 140%;
        background: linear-gradient(
            90deg, 
            transparent 0%,
            {section_color['accent']} 18%,
            {section_color['accent']} 32%,
            transparent 48%
        );
        transform: translateX(-100%);
        z-index: 1;
        animation: glass-shimmer 9s infinite ease-in-out;
            }}
            
            .glass-card-content {{
                position: relative;
                z-index: 2;
        display: flex;
        flex-direction: column;
        gap: 10px;
            }}
            
            .card-title {{
                justify-content: center;
                color: #e2e8f0;
        margin: 0;
        font-size: 1.1em;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 10px;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
        line-height: 1.3;
            }}
            
            .card-section {{
                color: #e2e8f0;
                line-height: 1.6;
        margin: 0;
                text-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
        font-weight: 500;
        font-size: 0.96em;
            }}
            
            .card-section:last-child {{
                margin: 0;
            }}
            
            .section-label {{
                color: #94a3b8;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
        font-size: 0.85em;
            }}
                        .article-indicator {{
                position: absolute;
        top: 14px;
        right: 14px;
                background: rgba(139, 92, 246, 0.2);
                color: #a78bfa;
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 0.7em;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border: 1px solid rgba(139, 92, 246, 0.3);
                z-index: 3;
            }}
            
            .click-hint {{
                position: absolute;
        bottom: 12px;
        right: 12px;
                color: #94a3b8;
        font-size: 0.78em;
                font-style: italic;
                opacity: 0.7;
                z-index: 3;
            }}
        </style>
    </head>
    <body>
        <div class="glass-card" {click_behavior}>
            <div class="glass-card-content">
                {f'<div class="article-indicator">Artigo</div>' if is_clickable else ''}
                <div class="card-title">
                    {icon} {title}
                </div>
                <div class="card-section">
                    <span class="section-label">Descrição:</span> {description}
                </div>
                <div class="card-section">
                    <span class="section-label">Impacto:</span> {impact}
                </div>
                <div class="card-section">
                    <span class="section-label">Viabilidade:</span> {feasibility}
                </div>
                <div class="card-section">
                    <span class="section-label">Solução Insight Expert:</span> {solution}
                </div>
                {f'<div class="click-hint">Clique para ver o artigo</div>' if is_clickable else ''}
            </div>
        </div>
    </body>
    </html>
    """
    # Calcular altura dinâmica baseada no conteúdo
    content_length = len(description) + len(impact) + len(feasibility) + len(solution)
    dynamic_height = max(360, min(520, 260 + (content_length // 70) * 32))
    # Renderizar usando components.html
    components.html(card_html, height=dynamic_height, scrolling=False)

def render_use_case_card(title: str, description: str, impact: str, feasibility: str, solution: str, icon_name: str, use_metallic: bool = False, section_type: str = "default", article_url: str = "") -> None:
    """
    Renders a use case card with either glass or metallic styling based on the use_metallic parameter.
    
    Args:
        title (str): Title of the use case
        description (str): Description of the use case
        impact (str): Impact description
        feasibility (str): Feasibility assessment
        solution (str): Insight Expert solution description
        icon_name (str): Name of the SVG icon to display
        use_metallic (bool): If True, uses metallic styling; if False, uses glass styling
        section_type (str): Type of the section (default: "default")
    """
    if use_metallic:
        render_use_case_card_metallic_html(title, description, impact, feasibility, solution, icon_name, section_type, article_url)
    else:
        render_use_case_card_glass(title, description, impact, feasibility, solution, icon_name, section_type, article_url)


def render_roi_insight_card(content: str, title: str = "") -> None:
    """
    Renders content inside a glass-effect card specifically designed for ROI insights.
    Uses a fixed height to prevent content from being cut off.
    
    Args:
        content (str): The content of the card (can be HTML).
        title (str): Optional title for the card.
    """
    import streamlit.components.v1 as components
    
    # Fixed height to prevent content cutoff; padding extra na base para linha "Previsão ML (Ensemble)"
    fixed_height = 535

    card_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            .roi-glass-card {{
                background: linear-gradient(135deg, 
                    rgba(30, 41, 59, 0.8) 0%, 
                    rgba(45, 55, 72, 0.9) 50%, 
                    rgba(30, 41, 59, 0.8) 100%);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid rgba(227, 236, 240, 0.25);
                border-radius: 20px;
                padding: 25px;
                padding-bottom: 32px;
                box-shadow: 
                    0 8px 32px rgba(0, 0, 0, 0.3),
                    0 2px 16px rgba(227, 236, 240, 0.1),
                    inset 0 1px 0 rgba(255, 255, 255, 0.1);
                font-family: 'Inter', sans-serif;
                line-height: 1.6;
                color: #cbd5e1;
                overflow: visible;
                margin-bottom: 15px;
                min-height: 400px;
                display: flex;
                flex-direction: column;
            }}
            
            .roi-card-title {{
                color: #e2e8f0;
                font-size: 1.4em;
                font-weight: 600;
                margin-bottom: 20px;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
                flex-shrink: 0;
            }}
            
            .roi-card-content {{
                font-size: 16px;
                line-height: 1.6;
                flex: 1;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                padding-bottom: 18px;
            }}
            
            .roi-card-content h3 {{
                color: #e2e8f0;
                margin-bottom: 15px;
                font-size: 1.4em;
                font-weight: 600;
            }}
            
            .roi-card-content p {{
                margin-bottom: 15px;
                color: #fbbf24;
                font-weight: 600;
            }}
            
            .roi-card-content h4 {{
                color: #e2e8f0;
                margin: 5px 0;
                font-size: 1.1em;
                font-weight: 500;
            }}
            
            .roi-card-content .metric-value {{
                color: #94a3b8;
                font-size: 1.2em;
                margin: 0;
                font-weight: 400;
            }}
            
            .roi-card-content .achievement-item {{
                display: flex;
                align-items: center;
                margin-bottom: 15px;
            }}
            
            .roi-card-content .achievement-item:last-child {{
                margin-bottom: 0;
            }}
            
            .roi-card-content .achievement-icon {{
                font-size: 1.2em;
                margin-right: 10px;
                flex-shrink: 0;
            }}
            
            .roi-card-content .achievement-text {{
                color: #e2e8f0;
                flex: 1;
            }}
            
            .roi-card-content .achievement-text strong {{
                color: #e2e8f0;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="roi-glass-card">
            {f'<div class="roi-card-title">{title}</div>' if title else ''}
            <div class="roi-card-content">
                {content}
            </div>
        </div>
    </body>
    </html>
    """
    
    components.html(card_html, height=fixed_height, scrolling=False)

def render_download_button_with_glass_style(custom_colors: Dict[str, str] = None) -> None:
    """
    Renderiza o estilo CSS glass card metálico para botões de download.
    
    Esta função deve ser chamada antes de qualquer st.download_button() 
    para aplicar o estilo consistente em todo o dashboard.
    
    Args:
        custom_colors (Dict[str, str], optional): Cores customizadas para o botão.
            Chaves disponíveis:
            - 'primary_bg': Cor primária do background
            - 'secondary_bg': Cor secundária do background  
            - 'text_color': Cor do texto
            - 'border_color': Cor da borda
            - 'hover_primary': Cor primária no hover
            - 'hover_secondary': Cor secundária no hover
    
    Usage:
        # Estilo padrão
        render_download_button_with_glass_style()
        
        # Estilo customizado
        custom_colors = {
            'primary_bg': 'rgba(59, 130, 246, 0.9)',
            'secondary_bg': 'rgba(96, 165, 250, 0.95)',
            'text_color': '#ffffff'
        }
        render_download_button_with_glass_style(custom_colors)
        
        st.download_button(
            label="📥 Exportar Dados",
            data=data,
            file_name="dados.xlsx",
            use_container_width=True
        )
    """
    import streamlit as st
    from typing import Dict
    
    # Cores padrão do tema glass card metálico
    default_colors = {
        'primary_bg': 'rgba(30, 41, 59, 0.9)',
        'secondary_bg': 'rgba(45, 55, 72, 0.95)',
        'text_color': '#e3ecf0',
        'border_color': 'rgba(227, 236, 240, 0.3)',
        'hover_primary': 'rgba(45, 55, 72, 0.95)',
        'hover_secondary': 'rgba(30, 41, 59, 0.9)',
        'hover_border': 'rgba(227, 236, 240, 0.5)'
    }
    
    # Mesclar cores customizadas com as padrão
    colors = default_colors.copy()
    if custom_colors:
        colors.update(custom_colors)
    
    st.markdown(f"""
    <style>
    .stDownloadButton > button {{
        background: linear-gradient(135deg, {colors['primary_bg']}, {colors['secondary_bg']}) !important;
        color: {colors['text_color']} !important;
        border: 1px solid {colors['border_color']} !important;
        border-radius: 12px !important;
        padding: 12px 20px !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
        backdrop-filter: blur(16px) !important;
        -webkit-backdrop-filter: blur(16px) !important;
        box-shadow: 
            0 4px 16px rgba(0, 0, 0, 0.3),
            0 1px 8px rgba(227, 236, 240, 0.1),
            inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
        transition: all 0.3s ease !important;
        text-shadow: 0 1px 2px rgba(0, 0, 0, 0.5) !important;
    }}
    
    .stDownloadButton > button:hover {{
        background: linear-gradient(135deg, {colors['hover_primary']}, {colors['hover_secondary']}) !important;
        border-color: {colors.get('hover_border', colors['border_color'])} !important;
        box-shadow: 
            0 6px 24px rgba(0, 0, 0, 0.4),
            0 2px 12px rgba(227, 236, 240, 0.2),
            inset 0 1px 0 rgba(255, 255, 255, 0.2) !important;
        transform: translateY(-2px) !important;
    }}
    
    .stDownloadButton > button:active {{
        transform: translateY(0px) !important;
        box-shadow: 
            0 2px 8px rgba(0, 0, 0, 0.3),
            0 1px 4px rgba(227, 236, 240, 0.1) !important;
    }}
    </style>
    """, unsafe_allow_html=True)

def render_glass_dataframe(
    df: pd.DataFrame,
    height: Optional[int] = 400,
    use_container_width: bool = True,
    **kwargs: Any,
) -> None:
    """
    Renderiza um DataFrame com o tema glass do app (container já estilizado pelo theme_manager).
    Mantém usabilidade total (scroll, ordenação, seleção) e visual premium.
    """
    if df is None or df.empty:
        st.caption("Nenhum dado para exibir.")
        return
    st.dataframe(
        df,
        use_container_width=use_container_width,
        height=height,
        **kwargs,
    )


def create_styled_download_button(
    label: str,
    data: bytes,
    file_name: str,
    mime: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    custom_colors: Dict[str, str] = None,
    use_container_width: bool = True
) -> None:
    """
    Cria um botão de download com estilo glass card metálico aplicado automaticamente.
    
    Esta função combina a aplicação do estilo e a criação do botão em uma única chamada,
    melhorando ainda mais a reprodutibilidade do código.
    
    Args:
        label (str): Texto do botão de download
        data (bytes): Dados para download
        file_name (str): Nome do arquivo
        mime (str): Tipo MIME do arquivo
        custom_colors (Dict[str, str], optional): Cores customizadas
        use_container_width (bool): Se deve usar a largura total do container
    
    Usage:
        # Uso básico
        create_styled_download_button(
            label="📥 Exportar Dados",
            data=excel_data,
            file_name="dados.xlsx"
        )
        
        # Com cores customizadas
        blue_theme = {
            'primary_bg': 'rgba(59, 130, 246, 0.9)',
            'secondary_bg': 'rgba(96, 165, 250, 0.95)'
        }
        create_styled_download_button(
            label="📥 Exportar Relatório",
            data=report_data,
            file_name="relatorio.xlsx",
            custom_colors=blue_theme
        )
    """
    import streamlit as st
    
    # Aplicar o estilo antes de criar o botão
    render_download_button_with_glass_style(custom_colors)
    
    # Criar o botão de download
    st.download_button(
        label=label,
        data=data,
        file_name=file_name,
        mime=mime,
        use_container_width=use_container_width
    )

# -----------------------------------------------------------------------------
# NOVAS IMPLEMENTAÇÕES - BCG PORTFOLIO CARDS
# -----------------------------------------------------------------------------

def get_bcg_styles() -> str:
    """
    Retorna o CSS consolidado para os cards BCG baseado nos templates fornecidos.
    """
    return """
<style>
    /* Base Card Styles */
    .glass-bcg-card {
        background: linear-gradient(135deg, rgba(32, 42, 60, 0.9), rgba(55, 64, 80, 0.7));
        backdrop-filter: blur(10px);
        border: 1px solid rgba(203, 210, 220, 0.1);
        border-radius: 16px;
        padding: 24px;
        position: relative;
        overflow: visible; /* Importante para tooltips */
        transition: all 0.3s ease;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        margin-bottom: 20px;
        color: #cbd2dc;
        font-family: 'Segoe UI', sans-serif;
    }

    .glass-bcg-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, transparent, rgba(203, 210, 220, 0.3), transparent);
        border-top-left-radius: 16px;
        border-top-right-radius: 16px;
    }

    .glass-bcg-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 48px rgba(0, 0, 0, 0.5);
        border-color: rgba(203, 210, 220, 0.2);
        z-index: 2;
    }

    /* Tipografia e Layout */
    .bcg-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 20px;
    }

    .bcg-title {
        font-size: 20px;
        font-weight: 600;
        color: #cbd2dc;
        margin-bottom: 8px;
    }
    
    .bcg-quadrant-title {
        font-size: 28px;
        font-weight: 700;
        color: #cbd2dc;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    
    .bcg-quadrant-icon {
        text-shadow: 0 0 20px currentColor;
    }

    .bcg-badge {
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        white-space: nowrap;
    }

    /* Glow visível para SVGs (ícone dentro do badge e no título do quadrante) */
    .bcg-badge svg,
    .bcg-quadrant-icon svg {
        display: inline-block;
        vertical-align: middle;
        overflow: visible;
    }

    .quadrant-estrela .bcg-badge svg,
    .quadrant-estrela .bcg-quadrant-icon svg {
        filter: drop-shadow(0 0 10px rgba(227, 188, 68, 0.65))
                drop-shadow(0 0 22px rgba(227, 188, 68, 0.30));
    }

    .quadrant-vaca .bcg-badge svg,
    .quadrant-vaca .bcg-quadrant-icon svg {
        filter: drop-shadow(0 0 10px rgba(125, 193, 129, 0.60))
                drop-shadow(0 0 22px rgba(125, 193, 129, 0.28));
    }

    .quadrant-interrogacao .bcg-badge svg,
    .quadrant-interrogacao .bcg-quadrant-icon svg {
        filter: drop-shadow(0 0 10px rgba(93, 173, 226, 0.62))
                drop-shadow(0 0 22px rgba(93, 173, 226, 0.28));
    }

    .quadrant-abacaxi .bcg-badge svg,
    .quadrant-abacaxi .bcg-quadrant-icon svg {
        filter: drop-shadow(0 0 10px rgba(195, 68, 68, 0.62))
                drop-shadow(0 0 22px rgba(195, 68, 68, 0.28));
    }

    .bcg-score-badge {
        display: inline-block;
        background: linear-gradient(135deg, rgba(55, 64, 80, 0.5), rgba(32, 42, 60, 0.5));
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 14px;
        border: 1px solid rgba(203, 210, 220, 0.1);
        color: #cbd2dc;
    }

    /* Métricas */
    .bcg-metrics-grid {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 12px;
        margin: 20px 0;
    }
    
    .bcg-stats-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 16px;
        margin: 20px 0;
    }

    .bcg-metric-item, .bcg-stat-box {
        background: rgba(14, 20, 36, 0.4);
        padding: 12px;
        border-radius: 8px;
        border: 1px solid rgba(203, 210, 220, 0.05);
        text-align: center;
    }
    
    .bcg-metric-item { text-align: left; }

    /* Métrica de Avaliação com Estrelas */
    .bcg-metric-item.rating {
        grid-column: 1 / -1;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .rating-content {
        display: flex;
        align-items: center;
        gap: 12px;
        width: 100%;
    }

    .rating-stars {
        display: flex;
        align-items: center;
        gap: 2px;
    }

    .star {
        color: #ffd54f;
        font-size: 16px;
    }

    .star.empty {
        color: rgba(203, 210, 220, 0.2);
    }

    .rating-value {
        font-size: 18px;
        font-weight: 700;
        color: #cbd2dc;
    }

    .rating-count {
        font-size: 12px;
        color: rgba(203, 210, 220, 0.5);
        margin-left: auto;
    }

    .bcg-metric-label, .bcg-stat-label {
        font-size: 11px;
        color: rgba(203, 210, 220, 0.6);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 6px;
    }

    .bcg-metric-value, .bcg-stat-value {
        font-size: 18px;
        font-weight: 700;
        color: #cbd2dc;
    }
    
    .bcg-stat-value { font-size: 24px; }
    .bcg-metric-value.small { font-size: 16px; }

    /* Variações de Cores por Quadrante */
    
    /* Abacaxi (Red) */
    .quadrant-abacaxi .bcg-badge {
        background: linear-gradient(135deg, rgba(244, 67, 54, 0.2), rgba(211, 47, 47, 0.15));
        border: 1px solid rgba(244, 67, 54, 0.5);
        color: #ef5350;
        box-shadow: 0 0 12px rgba(244, 67, 54, 0.25);
    }
    .quadrant-abacaxi.glass-bcg-card.quadrant-card {
        background: linear-gradient(135deg, rgba(69, 20, 18, 0.5), rgba(56, 16, 14, 0.6));
        border: 1px solid rgba(244, 67, 54, 0.3);
    }
    .quadrant-abacaxi .bcg-insight-box {
        background: linear-gradient(135deg, rgba(244, 67, 54, 0.08), rgba(211, 47, 47, 0.05));
        border-left: 3px solid #e53935;
    }
    .quadrant-abacaxi .bcg-insight-label, .quadrant-abacaxi .bcg-subtitle { color: #ef5350; }
    .quadrant-abacaxi .bcg-action-btn {
        background: linear-gradient(135deg, rgba(244, 67, 54, 0.15), rgba(211, 47, 47, 0.1));
        border: 1px solid rgba(244, 67, 54, 0.5);
        color: #ef5350;
    }
    .quadrant-abacaxi .bcg-stat-box {
        background: rgba(56, 16, 14, 0.4);
        border: 1px solid rgba(244, 67, 54, 0.25);
    }

    /* Vaca Leiteira (Green) */
    .quadrant-vaca .bcg-badge {
        background: linear-gradient(135deg, rgba(76, 175, 80, 0.2), rgba(56, 142, 60, 0.15));
        border: 1px solid rgba(76, 175, 80, 0.45);
        color: #81c784;
        box-shadow: 0 0 12px rgba(76, 175, 80, 0.2);
    }
    .quadrant-vaca.glass-bcg-card.quadrant-card {
        background: linear-gradient(135deg, rgba(33, 64, 35, 0.5), rgba(27, 52, 28, 0.6));
        border: 1px solid rgba(76, 175, 80, 0.25);
    }
    .quadrant-vaca .bcg-insight-box {
        background: linear-gradient(135deg, rgba(76, 175, 80, 0.08), rgba(56, 142, 60, 0.05));
        border-left: 3px solid #66bb6a;
    }
    .quadrant-vaca .bcg-insight-label, .quadrant-vaca .bcg-subtitle { color: #81c784; }
    .quadrant-vaca .bcg-action-btn {
        background: linear-gradient(135deg, rgba(76, 175, 80, 0.15), rgba(56, 142, 60, 0.1));
        border: 1px solid rgba(76, 175, 80, 0.45);
        color: #81c784;
    }
    .quadrant-vaca .bcg-stat-box {
        background: rgba(27, 52, 28, 0.4);
        border: 1px solid rgba(76, 175, 80, 0.2);
    }

    /* Estrela Digital (Yellow/Gold) */
    .quadrant-estrela .bcg-badge {
        background: linear-gradient(135deg, rgba(255, 193, 7, 0.2), rgba(255, 160, 0, 0.15));
        border: 1px solid rgba(255, 193, 7, 0.45);
        color: #ffd54f;
        box-shadow: 0 0 12px rgba(255, 193, 7, 0.2);
    }
    .quadrant-estrela.glass-bcg-card.quadrant-card {
        background: linear-gradient(135deg, rgba(92, 70, 15, 0.5), rgba(77, 57, 10, 0.6));
        border: 1px solid rgba(255, 193, 7, 0.25);
    }
    .quadrant-estrela .bcg-insight-box {
        background: linear-gradient(135deg, rgba(255, 193, 7, 0.08), rgba(255, 160, 0, 0.05));
        border-left: 3px solid #ffb300;
    }
    .quadrant-estrela .bcg-insight-label, .quadrant-estrela .bcg-subtitle { color: #ffd54f; }
    .quadrant-estrela .bcg-action-btn {
        background: linear-gradient(135deg, rgba(255, 193, 7, 0.15), rgba(255, 160, 0, 0.1));
        border: 1px solid rgba(255, 193, 7, 0.45);
        color: #ffd54f;
    }
    .quadrant-estrela .bcg-stat-box {
        background: rgba(77, 57, 10, 0.4);
        border: 1px solid rgba(255, 193, 7, 0.2);
    }

    /* Interrogação (Blue) */
    .quadrant-interrogacao .bcg-badge {
        background: linear-gradient(135deg, rgba(66, 153, 225, 0.2), rgba(49, 130, 206, 0.15));
        border: 1px solid rgba(66, 153, 225, 0.4);
        color: #5dade2;
        box-shadow: 0 0 12px rgba(66, 153, 225, 0.15);
    }
    .quadrant-interrogacao.glass-bcg-card.quadrant-card {
        background: linear-gradient(135deg, rgba(41, 72, 107, 0.5), rgba(32, 54, 82, 0.6));
        border: 1px solid rgba(66, 153, 225, 0.2);
    }
    .quadrant-interrogacao .bcg-insight-box {
        background: linear-gradient(135deg, rgba(66, 153, 225, 0.12), rgba(49, 130, 206, 0.08));
        border-left: 3px solid #5dade2;
    }
    .quadrant-interrogacao .bcg-insight-label, .quadrant-interrogacao .bcg-subtitle { color: #5dade2; }
    .quadrant-interrogacao .bcg-action-btn {
        background: linear-gradient(135deg, rgba(66, 153, 225, 0.15), rgba(49, 130, 206, 0.1));
        border: 1px solid rgba(66, 153, 225, 0.4);
        color: #5dade2;
    }
    .quadrant-interrogacao .bcg-stat-box {
        background: rgba(23, 47, 72, 0.4);
        border: 1px solid rgba(66, 153, 225, 0.15);
    }

    /* Elementos Comuns */
    .bcg-insight-box {
        padding: 14px;
        border-radius: 8px;
        margin-top: 20px;
    }

    .bcg-insight-label {
        font-size: 11px;
        font-weight: 600;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    .bcg-insight-text {
        font-size: 13px;
        color: rgba(203, 210, 220, 0.9);
        line-height: 1.5;
    }

    .bcg-action-btn {
        width: 100%;
        padding: 12px;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        text-align: center;
        display: block;
        text-decoration: none;
    }
    .bcg-action-btn:hover { filter: brightness(1.2); }

    .bcg-divider { height: 1px; margin: 20px 0; background: rgba(203, 210, 220, 0.1); }
    .quadrant-abacaxi .bcg-divider { background: linear-gradient(90deg, transparent, rgba(244, 67, 54, 0.3), transparent); }
    .quadrant-vaca .bcg-divider { background: linear-gradient(90deg, transparent, rgba(76, 175, 80, 0.25), transparent); }
    .quadrant-estrela .bcg-divider { background: linear-gradient(90deg, transparent, rgba(255, 193, 7, 0.25), transparent); }
    .quadrant-interrogacao .bcg-divider { background: linear-gradient(90deg, transparent, rgba(66, 153, 225, 0.2), transparent); }

    /* Tooltip */
    .bcg-tooltip {
        position: absolute;
        top: 24px;
        right: 24px;
        padding: 12px 16px;
        border-radius: 8px;
        opacity: 0;
        pointer-events: none;
        transition: opacity 0.3s ease;
        z-index: 100;
        background: rgba(14, 20, 36, 0.95);
        border: 1px solid rgba(255,255,255,0.1);
        backdrop-filter: blur(4px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    }
    
    /* Semantic Tooltip Backgrounds */
    .bcg-tooltip.quadrant-estrela {
        background: linear-gradient(135deg, rgba(92, 70, 15, 0.98), rgba(77, 57, 10, 0.95));
        border: 1px solid rgba(255, 193, 7, 0.35);
    }
    .bcg-tooltip.quadrant-vaca {
        background: linear-gradient(135deg, rgba(33, 64, 35, 0.98), rgba(27, 52, 28, 0.95));
        border: 1px solid rgba(76, 175, 80, 0.35);
    }
    .bcg-tooltip.quadrant-interrogacao {
        background: linear-gradient(135deg, rgba(41, 72, 107, 0.98), rgba(32, 54, 82, 0.95));
        border: 1px solid rgba(66, 153, 225, 0.35);
    }
    .bcg-tooltip.quadrant-abacaxi {
        background: linear-gradient(135deg, rgba(69, 20, 18, 0.98), rgba(56, 16, 14, 0.95));
        border: 1px solid rgba(244, 67, 54, 0.35);
    }

    .glass-bcg-card:hover .bcg-tooltip { opacity: 1; }
    
    .bcg-tooltip-line {
        font-size: 12px;
        color: #cbd2dc;
        white-space: nowrap;
        margin: 4px 0;
    }
    
    /* Tooltip Highlight Color */
    .bcg-tooltip.quadrant-estrela .bcg-tooltip-line strong { color: #ffd54f; }
    .bcg-tooltip.quadrant-vaca .bcg-tooltip-line strong { color: #81c784; }
    .bcg-tooltip.quadrant-interrogacao .bcg-tooltip-line strong { color: #5dade2; }
    .bcg-tooltip.quadrant-abacaxi .bcg-tooltip-line strong { color: #ef5350; }
    
    .critical-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(244, 67, 54, 0.15);
        border: 1px solid rgba(244, 67, 54, 0.4);
        color: #ef5350;
        padding: 8px 12px;
        border-radius: 8px;
        font-size: 12px;
        font-weight: 600;
        margin-top: 12px;
        width: 100%;
        justify-content: center;
    }

    /* Utilitários de Cor de Texto */
    .text-critical { color: #ef5350; font-weight: 800; }
    .text-warning { color: #ff9800; font-weight: 800; }
    .text-success { color: #4caf50; font-weight: 800; }
    .text-hyper { color: #ff9800; font-weight: 800; }
    
</style>
"""

def _get_quadrant_class(quadrant_name: str) -> str:
    mapping = {
        'Estrela Digital': 'quadrant-estrela',
        'Vaca Leiteira': 'quadrant-vaca',
        'Interrogação': 'quadrant-interrogacao',
        'Abacaxi': 'quadrant-abacaxi'
    }
    return mapping.get(quadrant_name, '')

def render_bcg_product_card(
    title: str,
    score: float,
    quadrant: str,
    metrics: Dict[str, Any],
    insight_title: str,
    insight_text: str,
    stock_info: Dict[str, Any] = None,
    badge_info: Dict[str, str] = None,
    download_html: str = "",
    rating_data: Dict[str, Any] = None,
    detailed_plans: Dict[str, List[str]] = None
) -> str:
    """
    Renderiza o HTML de um card de produto (individual) estilo Glassmorphism.
    """
    quadrant_class = _get_quadrant_class(quadrant)
    
    # Mapeamento de ícones para o badge
    quadrant_icons = {
        'Estrela Digital': get_svg_icon('estrela_digital', size=20),
        'Vaca Leiteira': get_svg_icon('vaca_leiteira', size=20),
        'Interrogação': get_svg_icon('interrogacao', size=20),
        'Abacaxi': get_svg_icon('abacaxi', size=20)
    }
    quadrant_icon = quadrant_icons.get(quadrant, '')
    
    # Formatação de métricas e classes condicionais
    growth_class = ""
    growth_val = float(metrics.get('growth_val', 0))
    if growth_val > 100: growth_class = "text-hyper"
    elif growth_val > 0: growth_class = "text-success"
    elif growth_val < -25: growth_class = "text-critical"
    elif growth_val < 0: growth_class = "text-warning"
    
    # Tooltip de estoque com classe semântica
    tooltip_html = ""
    if stock_info:
        tooltip_html = f"""<div class="bcg-tooltip stock-tooltip {quadrant_class}">
<div class="bcg-tooltip-line"><strong>{stock_info.get('units', 0)} unid.</strong> em estoque</div>
<div class="bcg-tooltip-line">Valor: <strong>{stock_info.get('value_fmt', 'R$ 0,00')}</strong></div>
</div>"""

    # Badge crítico removido conforme solicitação
    critical_badge_html = ""
    
    # Renderização de Avaliação (Estrelas)
    rating_html = ""
    if rating_data:
        score_val = rating_data.get('score', 0.0)
        count_val = rating_data.get('count', 0)
        
        # Se houver score, renderizar estrelas
        if score_val > 0:
            stars_html = ""
            for i in range(1, 6):
                if i <= round(score_val):
                    stars_html += '<span class="star">★</span>'
                else:
                    stars_html += '<span class="star empty">★</span>'
                    
            rating_html = f"""<div class="bcg-metric-item rating">
<div class="rating-content">
<div>
<div class="bcg-metric-label">Avaliação</div>
<div class="rating-stars">
{stars_html}
</div>
</div>
<div class="rating-value">{score_val:.1f}</div>
<div class="rating-count">({count_val} avaliações)</div>
</div>
</div>"""
        else: # Variante sem avaliações
            rating_html = f"""<div class="bcg-metric-item rating">
<div class="rating-content">
<div>
<div class="bcg-metric-label">Avaliação</div>
<div class="rating-count" style="margin-left:0; font-style:italic;">Sem avaliações disponíveis</div>
</div>
</div>
</div>"""

    # Renderizar Planos Detalhados se houver
    plans_html = ""
    if detailed_plans:
        # Helper para renderizar lista
        def _render_plan_list(label, items):
            if not items:
                return ""
            lis = "".join([f"<li style='margin-bottom:4px;'>{item}</li>" for item in items])
            return (
                f"<div style=\"margin-top:12px;\">"
                f"<div class=\"bcg-insight-label\" style=\"opacity:0.8; font-size:10px;\">{label}</div>"
                f"<ul style=\"margin:0; padding-left:16px; font-size:12px; color:rgba(203, 210, 220, 0.8); list-style-type:disc;\">"
                f"{lis}"
                f"</ul>"
                f"</div>"
            )
        
        capital = _render_plan_list("Capital & Investimento", detailed_plans.get('plano_capital'))
        ops = _render_plan_list("Operacional", detailed_plans.get('plano_operacional'))
        mkt = _render_plan_list("Mercado & Growth", detailed_plans.get('plano_mercado'))
        
        if capital or ops or mkt:
            plans_html = (
                "<div style=\"margin-top:16px; padding-top:12px; border-top:1px solid rgba(255,255,255,0.05);\">"
                f"{capital}{ops}{mkt}"
                "</div>"
            )

    # Nota: A div principal NÃO recebe a quadrant_class para manter o fundo cinza,
    # mas passamos a classe para os filhos (badges, insights) através da quadrant_class no wrapper se necessário,
    # mas aqui aplicamos manualmente nos componentes internos via a classe `quadrant_class`
    
    html = f"""<div class="glass-bcg-card {quadrant_class}">
{tooltip_html}
<div class="bcg-header">
<div>
<div class="bcg-title">{title}</div>
<div class="bcg-score-badge">Score: {score:.3f}</div>
</div>
<div class="bcg-badge">{quadrant_icon} {quadrant}</div>
</div>
<div class="bcg-metrics-grid">
<div class="bcg-metric-item">
<div class="bcg-metric-label">Market Share</div>
<div class="bcg-metric-value small">{metrics.get('share', '0.0%')}</div>
</div>
<div class="bcg-metric-item">
<div class="bcg-metric-label">Crescimento</div>
<div class="bcg-metric-value small {growth_class}">{metrics.get('growth', '0.0%')}</div>
</div>
<div class="bcg-metric-item">
<div class="bcg-metric-label">Preço Médio</div>
<div class="bcg-metric-value small">{metrics.get('price', 'R$ 0,00')}</div>
</div>
<div class="bcg-metric-item">
<div class="bcg-metric-label">Vendas</div>
<div class="bcg-metric-value small">{metrics.get('sales', '0')}</div>
</div>
</div>
{rating_html}
<div class="bcg-insight-box">
<div class="bcg-insight-label">{get_svg_icon('insights', size=16, color='currentColor')} {insight_title}</div>
<div class="bcg-insight-text">
{insight_text}
</div>
{plans_html}
</div>
{critical_badge_html}
{download_html}
</div>"""
    return html

def render_bcg_quadrant_card(
    quadrant: str,
    subtitle: str,
    icon: str,
    stats: Dict[str, Any],
    insight_text: str,
    stock_total: Dict[str, str],
    action_text: str = None,
    action_href: str = None
) -> str:
    """
    Renderiza o HTML do card de resumo do quadrante (Quadrante Grande).
    """
    quadrant_class = _get_quadrant_class(quadrant)
    
    # Adicionamos a classe 'quadrant-card' para ativar o background colorido específico
    action_html = ""
    if action_text and action_href:
        action_html = f"""<div style="margin-top: 20px;"><a class="bcg-action-btn" href="{action_href}" target="_self">{action_text}</a></div>"""
    elif action_text:
        action_html = f"""<div style="margin-top: 20px;"><button class="bcg-action-btn">{action_text}</button></div>"""

    html = f"""<div class="glass-bcg-card quadrant-card {quadrant_class}">
<div class="bcg-tooltip stock-tooltip {quadrant_class}">
<div class="bcg-tooltip-line"><strong>{stock_total.get('units', '0')} unid.</strong> em estoque</div>
<div class="bcg-tooltip-line">Valor: <strong>{stock_total.get('value', 'R$ 0,00')}</strong></div>
</div>
<div class="bcg-header" style="display:block;">
<div class="bcg-quadrant-title">
<span class="bcg-quadrant-icon" style="font-size:32px;">{icon}</span>
{quadrant}
</div>
<div class="bcg-subtitle" style="font-weight:600; text-transform:uppercase; letter-spacing:1px;">{subtitle}</div>
</div>
<div class="bcg-stats-grid">
<div class="bcg-stat-box">
<div class="bcg-stat-value">{stats.get('categories', 0)}</div>
<div class="bcg-stat-label">Categorias</div>
</div>
<div class="bcg-stat-box">
<div class="bcg-stat-value">{stats.get('units', 0)}</div>
<div class="bcg-stat-label">Unidades</div>
</div>
<div class="bcg-stat-box">
<div class="bcg-stat-value">{stats.get('avg_score', '0.00')}</div>
<div class="bcg-stat-label">Score Médio</div>
</div>
</div>
<div class="bcg-divider"></div>
<div class="bcg-metric-item" style="margin-bottom: 20px;">
<div class="bcg-metric-label">Valor Total em Estoque</div>
<div class="bcg-metric-value">{stock_total.get('value', 'R$ 0,00')}</div>
</div>
<div class="bcg-insight-box">
<div class="bcg-insight-label">{get_svg_icon('insights', size=16, color='currentColor')} Estratégia Recomendada</div>
<div class="bcg-insight-text">
{insight_text}
</div>
</div>
{action_html}
</div>"""
    return html
