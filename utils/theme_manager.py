import streamlit as st
import toml
import os
from pathlib import Path
from typing import Dict, Any, Optional
 

class ThemeManager:
    _instance: Optional["ThemeManager"] = None
    _config: Dict[str, Any] = {}
    _custom_config: Dict[str, Any] = {}
    
    def __new__(cls) -> "ThemeManager":
        if cls._instance is None:
            cls._instance = super(ThemeManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self) -> None:
        """Carrega as configurações do tema."""
        # Inicializar com valores padrão
        self._config = {}
        self._custom_config = {}
        
        try:
            # Carrega configurações do Streamlit
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.streamlit', 'config.toml')
            if os.path.exists(config_path):
                self._config = toml.load(config_path)
            else:
                self._config = {
                    'theme': {
                        'primaryColor': '#6366f1',
                        'backgroundColor': '#020617',
                        'secondaryBackgroundColor': '#0f172a',
                        'textColor': '#e2e8f0',
                        'font': 'Inter'
                    }
                }

            # Carrega configurações personalizadas
            custom_config_path = os.path.join(os.path.dirname(__file__), 'custom_theme.toml')
            if os.path.exists(custom_config_path):
                self._custom_config = toml.load(custom_config_path)
            else:
                self._custom_config = {
                    'glass_theme': {
                        'cardBackground': 'rgba(15, 23, 42, 0.6)',
                        'cardBorder': 'rgba(99, 102, 241, 0.5)',
                        'cardShadow': 'rgba(0, 0, 0, 0.3)',
                        'cardBlur': '16px',
                        'cardBorderRadius': '20px',
                        'cardPadding': '25px'
                    },
                    'background_theme': {
                        'radialGradient': 'radial-gradient(ellipse at center, rgba(15, 23, 42, 0.95) 0%, rgba(10, 14, 26, 0.98) 40%, rgba(5, 8, 15, 1) 100%)',
                        'diagonalPattern': 'repeating-linear-gradient(45deg, transparent, transparent 2px, rgba(30, 41, 59, 0.15) 2px, rgba(30, 41, 59, 0.15) 4px)',
                        'metallicOverlay': 'linear-gradient(135deg, rgba(99, 102, 241, 0.03) 0%, rgba(139, 92, 246, 0.02) 25%, rgba(59, 130, 246, 0.03) 50%, rgba(99, 102, 241, 0.02) 75%, rgba(139, 92, 246, 0.03) 100%)',
                        'texturePattern': 'repeating-linear-gradient(-45deg, transparent, transparent 1px, rgba(255, 255, 255, 0.01) 1px, rgba(255, 255, 255, 0.01) 2px)'
                    },
                    'colors': {
                        'primary': '#6366f1',
                        'success': '#10b981',
                        'warning': '#f59e0b',
                        'danger': '#ef4444',
                        'info': '#3b82f6'
                    }
                }
        except Exception as e:
            print(f"Erro ao carregar configurações: {e}")
            # Garantir que sempre tenhamos dicionários válidos
            if not self._config:
                self._config = {
                    'theme': {
                        'primaryColor': '#6366f1',
                        'backgroundColor': '#020617',
                        'secondaryBackgroundColor': '#0f172a',
                        'textColor': '#e2e8f0',
                        'font': 'Inter'
                    }
                }
            if not self._custom_config:
                self._custom_config = {
                    'glass_theme': {
                        'cardBackground': 'rgba(15, 23, 42, 0.6)',
                        'cardBorder': 'rgba(99, 102, 241, 0.5)',
                        'cardShadow': 'rgba(0, 0, 0, 0.3)',
                        'cardBlur': '16px',
                        'cardBorderRadius': '20px',
                        'cardPadding': '25px'
                    },
                    'background_theme': {
                        'radialGradient': 'radial-gradient(ellipse at center, rgba(15, 23, 42, 0.95) 0%, rgba(10, 14, 26, 0.98) 40%, rgba(5, 8, 15, 1) 100%)',
                        'diagonalPattern': 'repeating-linear-gradient(45deg, transparent, transparent 2px, rgba(30, 41, 59, 0.15) 2px, rgba(30, 41, 59, 0.15) 4px)',
                        'metallicOverlay': 'linear-gradient(135deg, rgba(99, 102, 241, 0.03) 0%, rgba(139, 92, 246, 0.02) 25%, rgba(59, 130, 246, 0.03) 50%, rgba(99, 102, 241, 0.02) 75%, rgba(139, 92, 246, 0.03) 100%)',
                        'texturePattern': 'repeating-linear-gradient(-45deg, transparent, transparent 1px, rgba(255, 255, 255, 0.01) 1px, rgba(255, 255, 255, 0.01) 2px)'
                    },
                    'colors': {
                        'primary': '#6366f1',
                        'success': '#10b981',
                        'warning': '#f59e0b',
                        'danger': '#ef4444',
                        'info': '#3b82f6'
                    }
                }
    
    def get_theme(self) -> Dict[str, Any]:
        """Retorna as configurações do tema"""
        return self._config.get("theme", {}) or {}
    
    def get_glass_theme(self) -> Dict[str, Any]:
        """Retorna as configurações do tema glass"""
        return self._custom_config.get("glass_theme", {}) or {}
    
    def get_colors(self) -> Dict[str, Any]:
        """Retorna as cores do tema"""
        return self._custom_config.get("colors", {}) or {}
    
    def get_background_theme(self) -> Dict[str, Any]:
        """Retorna as configurações do tema de fundo"""
        return self._custom_config.get("background_theme", {}) or {} 
    
    def apply_theme(self) -> Dict[str, Any]:
        """Aplica o tema ao Streamlit."""
        theme = self.get_theme()
        glass = self.get_glass_theme()
        colors = self.get_colors()
        background = self.get_background_theme()
        
        st.markdown(f"""
        <style>
            /* Estilos globais com fundo radial e padrões metálicos (sem pseudo-elementos) */
            .stApp {{
                /* Camadas: overlay metálico, textura fina, linhas diagonais, gradiente radial */
                background-image:
                    {background.get('metallicOverlay', 'linear-gradient(135deg, rgba(99,102,241,0.03) 0%, rgba(139,92,246,0.02) 25%, rgba(59,130,246,0.03) 50%, rgba(99,102,241,0.02) 75%, rgba(139,92,246,0.03) 100%)')},
                    {background.get('texturePattern', 'repeating-linear-gradient(-45deg, transparent, transparent 1px, rgba(255,255,255,0.01) 1px, rgba(255,255,255,0.01) 2px)')},
                    {background.get('diagonalPattern', 'repeating-linear-gradient(45deg, transparent, transparent 2px, rgba(30,41,59,0.15) 2px, rgba(30,41,59,0.15) 4px)')},
                    {background.get('radialGradient', 'radial-gradient(ellipse at center, rgba(15,23,42,0.95) 0%, rgba(10,14,26,0.98) 40%, rgba(5,8,15,1) 100%)')};
                background-attachment: fixed, fixed, fixed, fixed;
                background-size: cover, cover, cover, cover;
                background-repeat: no-repeat, repeat, repeat, no-repeat;
                color: {theme.get('textColor', '#e2e8f0')};
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}
            
            /* Estilos dos cards */
            .stCard {{
                background: {glass.get('cardBackground', 'rgba(30, 41, 59, 0.7)')};
                backdrop-filter: blur({glass.get('cardBlur', '16px')});
                border: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
                border-radius: {glass.get('cardBorderRadius', '20px')};
                padding: {glass.get('cardPadding', '25px')};
                box-shadow: 0 8px 32px {glass.get('cardShadow', 'rgba(0, 0, 0, 0.3)')};
            }}
            
            /* Estilos dos títulos */
            h1, h2, h3 {{
                color: {theme.get('textColor', '#e2e8f0')};
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}

            /* Estilos dos textos */
            p, li, td, th {{
                color: {theme.get('textColor', '#e2e8f0')};
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}

            /* Estilos dos botões */
            .stButton button {{
                background-color: {theme.get('primaryColor', '#6366f1')};
                color: white;
                border: none;
                border-radius: 8px;
                padding: 8px 16px;
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}
            
            /* Estilos dos inputs */
            .stTextInput input, .stNumberInput input {{
                background-color: {theme.get('secondaryBackgroundColor', '#1e293b')};
                color: {theme.get('textColor', '#e2e8f0')};
                border: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
                border-radius: 8px;
                padding: 8px 12px;
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}
            
            /* Estilos dos selects */
            .stSelectbox select {{
                background-color: {theme.get('secondaryBackgroundColor', '#1e293b')};
                color: {theme.get('textColor', '#e2e8f0')};
                border: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
                border-radius: 8px;
                padding: 8px 12px;
                font-family: {theme.get('font', 'Inter')}, sans-serif;
            }}

            /* Tabelas / DataFrames – tema glass premium (sem perder usabilidade) */
            .stDataFrame,
            div[data-testid="stDataFrame"] {{
                background: linear-gradient(135deg, rgba(30, 41, 59, 0.92), rgba(51, 65, 85, 0.9) 50%, rgba(30, 41, 59, 0.92)) !important;
                border: 1px solid rgba(227, 236, 240, 0.25) !important;
                border-radius: 16px !important;
                box-shadow: 0 10px 28px rgba(0, 0, 0, 0.35), 0 2px 12px rgba(227, 236, 240, 0.1), inset 0 1px 0 rgba(255, 255, 255, 0.08) !important;
                overflow: hidden !important;
                font-family: {theme.get('font', 'Inter')}, sans-serif !important;
            }}
            .stDataFrame > div,
            div[data-testid="stDataFrame"] > div {{
                border-radius: 16px !important;
                overflow: hidden !important;
            }}

            /* Estilos dos gráficos - Glass Morphism Metálico Escuro */
            .stPlotlyChart {{
                background: linear-gradient(135deg, 
                    rgba(15, 23, 42, 0.95) 0%, 
                    rgba(30, 41, 59, 0.9) 50%, 
                    rgba(15, 23, 42, 0.95) 100%);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid rgba(99, 102, 241, 0.3);
                border-radius: 18px;
                padding: 24px;
                box-shadow: 
                    0 8px 32px rgba(0, 0, 0, 0.4),
                    0 2px 16px rgba(99, 102, 241, 0.1),
                    inset 0 1px 0 rgba(255, 255, 255, 0.1);
                position: relative;
                overflow: hidden;
            }}
            
            /* Efeito metálico sutil nos gráficos */
            .stPlotlyChart::before {{
                content: '';
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
                animation: metallic-shine 8s infinite linear;
                pointer-events: none;
            }}
            
            @keyframes metallic-shine {{
                0% {{ transform: translateX(-50%); }}
                100% {{ transform: translateX(50%); }}
            }}
            
            /* Garantir que o conteúdo do gráfico fique acima do efeito */
            .stPlotlyChart > div {{
                position: relative;
                z-index: 2;
            }}
            
            /* Estilos da Sidebar - Black Metallic Light Gradient */
            /* Compatibilidade: aplica a ambos os seletores usados pelo Streamlit */
            [data-testid="stSidebar"], .stSidebar {{
                position: relative;
                overflow: hidden;
            }}

            [data-testid="stSidebar"] > div, .stSidebar {{
                background-image:
                    /* base: preto com leve tonalidade azulada */
                    linear-gradient(135deg,
                        rgba(3, 6, 12, 0.98) 0%,
                        rgba(12, 18, 32, 0.98) 45%,
                        rgba(3, 6, 12, 0.98) 100%),
                    /* leve matiz metálica azul (sutil) */
                    linear-gradient(180deg,
                        rgba(59, 130, 246, 0.06) 0%,
                        rgba(99, 102, 241, 0.04) 100%),
                    /* textura metálica sutil */
                    repeating-linear-gradient(-45deg,
                        rgba(200, 220, 255, 0.03) 0px,
                        rgba(200, 220, 255, 0.03) 1px,
                        transparent 1px,
                        transparent 3px);
                background-color: #070b12;
                border-right: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
            }}

            /* Brilho metálico suave em varredura */
            [data-testid="stSidebar"]::before, .stSidebar::before {{
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 200%;
                height: 100%;
                background: linear-gradient(90deg,
                    transparent 0%,
                    rgba(170, 190, 255, 0.06) 45%,
                    rgba(170, 190, 255, 0.16) 50%,
                    rgba(170, 190, 255, 0.06) 55%,
                    transparent 100%);
                animation: metallic-shine 10s infinite linear;
                pointer-events: none;
            }}
            
            /* Estilos dos elementos da sidebar */
            [data-testid="stSidebar"] .stMarkdown, .stSidebar .stMarkdown {{
                color: {theme.get('textColor', '#e2e8f0')};
            }}
            
            [data-testid="stSidebar"] .stSelectbox > div > div, .stSidebar .stSelectbox > div > div {{
                background-color: {theme.get('secondaryBackgroundColor', '#1e293b')};
                color: {theme.get('textColor', '#e2e8f0')};
                border: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
            }}
            
            [data-testid="stSidebar"] .stNumberInput > div > div > input, .stSidebar .stNumberInput > div > div > input {{
                background-color: {theme.get('secondaryBackgroundColor', '#1e293b')};
                color: {theme.get('textColor', '#e2e8f0')};
                border: 1px solid {glass.get('cardBorder', 'rgba(99, 102, 241, 0.3)')};
            }}
            
            [data-testid="stSidebar"] .stRadio > div, .stSidebar .stRadio > div {{
                background-color: {theme.get('secondaryBackgroundColor', '#1e293b')};
                color: {theme.get('textColor', '#e2e8f0')};
            }}
        </style>
        """, unsafe_allow_html=True)
        return {
            "theme": theme,
            "glass": glass,
            "colors": colors,
            "background": background
        }
    
    def get_glass_card_style(self, custom_style: str = "") -> str:
        """Retorna o estilo CSS para um card com efeito glass"""
        glass = self.get_glass_theme()
        base_style = f"""
            background: {glass.get('cardBackground', 'rgba(255, 255, 255, 0.1)')};
            border: 1px solid {glass.get('cardBorder', 'rgba(255, 255, 255, 0.3)')};
            box-shadow: 0 8px 32px {glass.get('cardShadow', 'rgba(0, 0, 0, 0.2)')};
            backdrop-filter: blur({glass.get('cardBlur', '16px')});
            border-radius: {glass.get('cardBorderRadius', '20px')};
            padding: {glass.get('cardPadding', '25px')};
        """
        return base_style + custom_style

# Função helper para obter a instância do ThemeManager
def get_theme_manager() -> ThemeManager:
    return ThemeManager() 