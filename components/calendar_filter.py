"""
Componente de Calendário Personalizado para Dashboard Streamlit
Estilo: Dark Metallic Glassmorphism

Este módulo fornece um calendário estilizado que segue o tema visual do painel,
mantendo compatibilidade com o sistema de filtros existente.
"""
from __future__ import annotations
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Optional, Tuple, List
import json


# Formato de exibição/entrada de datas para melhor usabilidade (DD-MM-YYYY)
DATE_FMT_DISPLAY = "%d-%m-%Y"


def _format_date_dd_mm_yyyy(d: date) -> str:
    """Formata data para DD-MM-YYYY."""
    return d.strftime(DATE_FMT_DISPLAY)


def _parse_dd_mm_yyyy(s: str) -> Optional[date]:
    """Interpreta string no formato DD-MM-YYYY ou DD/MM/YYYY. Retorna None se inválido."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None



def render_glass_calendar(
    key: str = "calendar_filter",
    default_start: Optional[date] = None,
    default_end: Optional[date] = None,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
    preset_periods: bool = True
) -> Tuple[Optional[date], Optional[date]]:
    """
    Renderiza um calendário com estilo dark metallic glassmorphism.
    
    Args:
        key: Chave única para o componente
        default_start: Data inicial padrão
        default_end: Data final padrão
        min_date: Data mínima permitida
        max_date: Data máxima permitida
        preset_periods: Se deve mostrar períodos pré-definidos
        
    Returns:
        Tupla com (data_inicio, data_fim) ou (None, None) se "Todo o período"
    """
    
    # Valores padrão baseados nos dados
    if min_date is None:
        min_date = date(2016, 1, 1)  # Data mínima dos dados Olist
    if max_date is None:
        max_date = date.today()
    if default_start is None:
        default_start = max_date - timedelta(days=365)  # Último ano por padrão
    if default_end is None:
        default_end = max_date
    
    # HTML e CSS para o calendário estilizado
    calendar_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
            
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }}
            
            body {{
                font-family: 'Inter', sans-serif;
                background: transparent;
                color: #e2e8f0;
                padding: 10px;
            }}
            
            .calendar-container {{
                background: linear-gradient(135deg, 
                    rgba(30, 41, 59, 0.9) 0%, 
                    rgba(45, 55, 72, 0.95) 50%, 
                    rgba(30, 41, 59, 0.9) 100%);
                backdrop-filter: blur(16px);
                -webkit-backdrop-filter: blur(16px);
                border: 1px solid rgba(227, 236, 240, 0.3);
                border-radius: 16px;
                padding: 20px;
                box-shadow: 
                    0 8px 32px rgba(0, 0, 0, 0.4),
                    0 2px 16px rgba(227, 236, 240, 0.1),
                    inset 0 1px 0 rgba(255, 255, 255, 0.1);
                position: relative;
                overflow: hidden;
            }}
            
            .calendar-container::before {{
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
                animation: shimmer 6s infinite linear;
            }}
            
            @keyframes shimmer {{
                0% {{ transform: translateX(-50%); }}
                100% {{ transform: translateX(50%); }}
            }}
            
            .calendar-content {{
                position: relative;
                z-index: 2;
            }}
            
            .calendar-header {{
                text-align: center;
                margin-bottom: 20px;
                padding-bottom: 15px;
                border-bottom: 1px solid rgba(227, 236, 240, 0.2);
            }}
            
            .calendar-title {{
                font-size: 1.4em;
                font-weight: 700;
                color: #e3ecf0;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
                margin-bottom: 8px;
            }}
            
            .calendar-subtitle {{
                font-size: 0.9em;
                color: #94a3b8;
                font-weight: 500;
            }}
            
            .preset-buttons {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 8px;
                margin-bottom: 20px;
            }}
            
            .preset-btn {{
                background: linear-gradient(135deg, 
                    rgba(52, 63, 82, 0.8) 0%, 
                    rgba(28, 36, 48, 0.9) 100%);
                border: 1px solid rgba(227, 236, 240, 0.25);
                border-radius: 8px;
                padding: 8px 12px;
                color: #cbd5e1;
                font-size: 0.85em;
                font-weight: 500;
                cursor: pointer;
                transition: all 0.3s ease;
                text-align: center;
                backdrop-filter: blur(8px);
                -webkit-backdrop-filter: blur(8px);
            }}
            
            .preset-btn:hover {{
                background: linear-gradient(135deg, 
                    rgba(59, 130, 246, 0.3) 0%, 
                    rgba(99, 102, 241, 0.4) 100%);
                border-color: rgba(99, 102, 241, 0.6);
                color: #e3ecf0;
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
            }}
            
            .preset-btn.active {{
                background: linear-gradient(135deg, 
                    rgba(99, 102, 241, 0.6) 0%, 
                    rgba(139, 92, 246, 0.7) 100%);
                border-color: rgba(99, 102, 241, 0.8);
                color: #ffffff;
                box-shadow: 0 4px 16px rgba(99, 102, 241, 0.3);
            }}
            
            .date-inputs {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 12px;
                margin-bottom: 15px;
            }}
            
            .date-input-group {{
                display: flex;
                flex-direction: column;
            }}
            
            .date-label {{
                font-size: 0.85em;
                font-weight: 600;
                color: #94a3b8;
                margin-bottom: 6px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            
            .date-input {{
                background: linear-gradient(135deg, 
                    rgba(15, 23, 42, 0.8) 0%, 
                    rgba(30, 41, 59, 0.9) 100%);
                border: 1px solid rgba(227, 236, 240, 0.3);
                border-radius: 8px;
                padding: 10px 12px;
                color: #e3ecf0;
                font-size: 0.9em;
                font-weight: 500;
                backdrop-filter: blur(8px);
                -webkit-backdrop-filter: blur(8px);
                transition: all 0.3s ease;
            }}
            
            .date-input:focus {{
                outline: none;
                border-color: rgba(99, 102, 241, 0.6);
                box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
                background: linear-gradient(135deg, 
                    rgba(15, 23, 42, 0.9) 0%, 
                    rgba(30, 41, 59, 0.95) 100%);
            }}
            
            .date-input::-webkit-calendar-picker-indicator {{
                filter: invert(1);
                opacity: 0.7;
                cursor: pointer;
            }}
            
            .date-input::-webkit-calendar-picker-indicator:hover {{
                opacity: 1;
            }}
            
            .apply-button {{
                width: 100%;
                background: linear-gradient(135deg, 
                    rgba(59, 130, 246, 0.8) 0%, 
                    rgba(99, 102, 241, 0.9) 100%);
                border: 1px solid rgba(99, 102, 241, 0.6);
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 0.95em;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                backdrop-filter: blur(8px);
                -webkit-backdrop-filter: blur(8px);
                box-shadow: 0 4px 16px rgba(99, 102, 241, 0.2);
            }}
            
            .apply-button:hover {{
                background: linear-gradient(135deg, 
                    rgba(59, 130, 246, 0.9) 0%, 
                    rgba(99, 102, 241, 1.0) 100%);
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(99, 102, 241, 0.3);
            }}
            
            .apply-button:active {{
                transform: translateY(0);
            }}
            
            .period-info {{
                margin-top: 12px;
                padding: 10px;
                background: rgba(15, 23, 42, 0.6);
                border-radius: 8px;
                border-left: 3px solid rgba(99, 102, 241, 0.6);
                font-size: 0.85em;
                color: #cbd5e1;
                line-height: 1.4;
            }}
            
            .period-info strong {{
                color: #e3ecf0;
            }}
        </style>
    </head>
    <body>
        <div class="calendar-container">
            <div class="calendar-content">
                <div class="calendar-header">
                    <div class="calendar-title">📅 Período de Análise</div>
                    <div class="calendar-subtitle">Selecione o intervalo de datas</div>
                </div>
                
                <div class="preset-buttons">
                    <button class="preset-btn" onclick="setPreset('all')">Todo o Período</button>
                    <button class="preset-btn" onclick="setPreset('month')">Último Mês</button>
                    <button class="preset-btn" onclick="setPreset('quarter')">Último Trimestre</button>
                    <button class="preset-btn" onclick="setPreset('year')">Último Ano</button>
                    <button class="preset-btn" onclick="setPreset('ytd')">Ano Atual</button>
                    <button class="preset-btn" onclick="setPreset('custom')">Personalizado</button>
                </div>
                
                <div class="date-inputs" id="dateInputs">
                    <div class="date-input-group">
                        <label class="date-label">Data Inicial</label>
                        <input type="date" class="date-input" id="startDate" 
                               value="{default_start.strftime('%Y-%m-%d')}"
                               min="{min_date.strftime('%Y-%m-%d')}"
                               max="{max_date.strftime('%Y-%m-%d')}">
                    </div>
                    <div class="date-input-group">
                        <label class="date-label">Data Final</label>
                        <input type="date" class="date-input" id="endDate" 
                               value="{default_end.strftime('%Y-%m-%d')}"
                               min="{min_date.strftime('%Y-%m-%d')}"
                               max="{max_date.strftime('%Y-%m-%d')}">
                    </div>
                </div>
                
                <button class="apply-button" onclick="applyDateRange()">
                    ✨ Aplicar Período
                </button>
                
                <div class="period-info" id="periodInfo">
                    <strong>Período Selecionado:</strong><br>
                    <span id="periodText">Carregando...</span>
                </div>
            </div>
        </div>
        
        <script>
            let currentPreset = 'year';
            const maxDate = new Date('{max_date.strftime('%Y-%m-%d')}');
            const minDate = new Date('{min_date.strftime('%Y-%m-%d')}');
            
            function formatDate(date) {{
                return date.toLocaleDateString('pt-BR', {{
                    day: '2-digit',
                    month: '2-digit', 
                    year: 'numeric'
                }});
            }}
            
            function calculateDaysDifference(start, end) {{
                const diffTime = Math.abs(end - start);
                return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            }}
            
            function updatePeriodInfo() {{
                const startDate = document.getElementById('startDate').value;
                const endDate = document.getElementById('endDate').value;
                const periodText = document.getElementById('periodText');
                
                if (currentPreset === 'all') {{
                    periodText.innerHTML = 'Todo o período disponível nos dados';
                }} else if (startDate && endDate) {{
                    const start = new Date(startDate);
                    const end = new Date(endDate);
                    const days = calculateDaysDifference(start, end);
                    
                    periodText.innerHTML = `${{formatDate(start)}} até ${{formatDate(end)}}<br>
                                          <small>(${{days}} dias de análise)</small>`;
                }} else {{
                    periodText.innerHTML = 'Selecione as datas para ver o resumo';
                }}
            }}
            
            function setPreset(preset) {{
                currentPreset = preset;
                const today = new Date();
                const startInput = document.getElementById('startDate');
                const endInput = document.getElementById('endDate');
                const dateInputs = document.getElementById('dateInputs');
                
                // Remover classe active de todos os botões
                document.querySelectorAll('.preset-btn').forEach(btn => {{
                    btn.classList.remove('active');
                }});
                
                // Adicionar classe active ao botão clicado
                event.target.classList.add('active');
                
                let startDate, endDate;
                
                switch(preset) {{
                    case 'all':
                        dateInputs.style.display = 'none';
                        updatePeriodInfo();
                        sendToStreamlit(null, null, 'Todo o período');
                        return;
                        
                    case 'month':
                        startDate = new Date(today);
                        startDate.setMonth(startDate.getMonth() - 1);
                        endDate = new Date(today);
                        break;
                        
                    case 'quarter':
                        startDate = new Date(today);
                        startDate.setMonth(startDate.getMonth() - 3);
                        endDate = new Date(today);
                        break;
                        
                    case 'year':
                        startDate = new Date(today);
                        startDate.setFullYear(startDate.getFullYear() - 1);
                        endDate = new Date(today);
                        break;
                        
                    case 'ytd':
                        startDate = new Date(today.getFullYear(), 0, 1);
                        endDate = new Date(today);
                        break;
                        
                    case 'custom':
                        dateInputs.style.display = 'grid';
                        updatePeriodInfo();
                        return;
                }}
                
                // Garantir que as datas estão dentro dos limites
                if (startDate < minDate) startDate = minDate;
                if (endDate > maxDate) endDate = maxDate;
                
                startInput.value = startDate.toISOString().split('T')[0];
                endInput.value = endDate.toISOString().split('T')[0];
                
                dateInputs.style.display = 'grid';
                updatePeriodInfo();
            }}
            
            function applyDateRange() {{
                const startDate = document.getElementById('startDate').value;
                const endDate = document.getElementById('endDate').value;
                
                if (currentPreset === 'all') {{
                    sendToStreamlit(null, null, 'Todo o período');
                }} else if (startDate && endDate) {{
                    sendToStreamlit(startDate, endDate, 'Período personalizado');
                }} else {{
                    alert('Por favor, selecione ambas as datas.');
                }}
            }}
            
            function sendToStreamlit(startDate, endDate, periodType) {{
                const data = {{
                    start_date: startDate,
                    end_date: endDate,
                    period_type: periodType,
                    preset: currentPreset
                }};
                
                // Enviar dados para o Streamlit
                window.parent.postMessage({{
                    type: 'calendar_update',
                    key: '{key}',
                    data: data
                }}, '*');
            }}
            
            // Event listeners para mudanças nas datas
            document.getElementById('startDate').addEventListener('change', function() {{
                const endDate = document.getElementById('endDate');
                if (this.value > endDate.value) {{
                    endDate.value = this.value;
                }}
                updatePeriodInfo();
            }});
            
            document.getElementById('endDate').addEventListener('change', function() {{
                const startDate = document.getElementById('startDate');
                if (this.value < startDate.value) {{
                    startDate.value = this.value;
                }}
                updatePeriodInfo();
            }});
            
            // Inicializar com último ano selecionado
            document.addEventListener('DOMContentLoaded', function() {{
                const yearBtn = document.querySelectorAll('.preset-btn')[3]; // "Último Ano"
                yearBtn.classList.add('active');
                updatePeriodInfo();
            }});
        </script>
    </body>
    </html>
    """
    
    # Renderizar o componente com comunicação bidirecional
    result = components.html(calendar_html, height=420, scrolling=False, key=key)
    
    # Inicializar estado se não existir
    if f"{key}_state" not in st.session_state:
        st.session_state[f"{key}_state"] = {
            'start_date': default_start,
            'end_date': default_end,
            'period_type': 'Último ano'
        }
    
    # Verificar se houve mudança via JavaScript (simulado por enquanto)
    # Em uma implementação real, isso seria feito via callback do componente
    current_state = st.session_state[f"{key}_state"]
    
    return current_state.get('start_date', default_start), current_state.get('end_date', default_end)

def _first_day_prev_month(t: date) -> date:
    """Primeiro dia do mês anterior."""
    first_this = date(t.year, t.month, 1)
    last_prev = first_this - timedelta(days=1)
    return date(last_prev.year, last_prev.month, 1)


def _last_day_prev_month(t: date) -> date:
    """Último dia do mês anterior."""
    first_this = date(t.year, t.month, 1)
    return first_this - timedelta(days=1)


def convert_calendar_to_period_filter(
    start_date: Optional[date], 
    end_date: Optional[date]
) -> str:
    """
    Converte datas do calendário para o formato de período usado pelo sistema existente.
    
    Args:
        start_date: Data inicial selecionada
        end_date: Data final selecionada
        
    Returns:
        String do período no formato esperado pelo sistema
    """
    if start_date is None or end_date is None:
        return "Todo o período"
    
    today = date.today()
    prev_month_start = _first_day_prev_month(today)
    prev_month_end = _last_day_prev_month(today)
    this_month_start = date(today.year, today.month, 1)
    
    if start_date == today - timedelta(days=6) and end_date == today:
        return "Últimos 7 dias"
    if start_date == prev_month_start and end_date == prev_month_end:
        return "Último mês"
    if start_date == this_month_start and end_date == today:
        return "Mês Atual"
    if start_date == today - timedelta(days=60) and end_date == today:
        return "Últimos 2 meses"
    if start_date == today - timedelta(days=90) and end_date == today:
        return "Último trimestre"
    if start_date == today - timedelta(days=180) and end_date == today:
        return "Último semestre"
    if start_date == date(today.year - 1, 1, 1) and end_date == date(today.year - 1, 12, 31):
        return "Último ano"
    if start_date == date(today.year - 2, 1, 1) and end_date == date(today.year - 1, 12, 31):
        return "Últimos 2 anos"
    return "Período personalizado"

def get_date_range_from_calendar(
    start_date: Optional[date], 
    end_date: Optional[date]
) -> Optional[List[datetime]]:
    """
    Converte datas do calendário para o formato de date_range usado pelo sistema.
    
    Args:
        start_date: Data inicial selecionada
        end_date: Data final selecionada
        
    Returns:
        Lista com [data_inicio, data_fim] como datetime ou None para todo o período
    """
    if start_date is None or end_date is None:
        return None
    
    return [
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.max.time())
    ]

def render_simple_date_picker() -> Tuple[Optional[date], Optional[date]]:
    """
    Renderiza um seletor de datas simples usando componentes nativos do Streamlit.
    
    Returns:
        Tupla com (data_inicio, data_fim)
    """
    st.sidebar.markdown("---")
    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today
    
    custom_start = st.session_state.get("custom_start_date")
    custom_end = st.session_state.get("custom_end_date")
    if custom_start and custom_end:
        default_start = custom_start
        default_end = custom_end
    
    prev_month_start = _first_day_prev_month(today)
    prev_month_end = _last_day_prev_month(today)
    this_month_start = date(today.year, today.month, 1)
    last_year_start = date(today.year - 1, 1, 1)
    last_year_end = date(today.year - 1, 12, 31)
    two_years_start = date(today.year - 2, 1, 1)
    
    period_options = {
        "Todo o período": (None, None),
        "Últimos 7 dias": (today - timedelta(days=6), today),
        "Último mês": (prev_month_start, prev_month_end),
        "Mês Atual": (this_month_start, today),
        "Últimos 2 meses": (today - timedelta(days=60), today),
        "Último trimestre": (today - timedelta(days=90), today),
        "Último semestre": (today - timedelta(days=180), today),
        "Último ano": (last_year_start, last_year_end),
        "Últimos 2 anos": (two_years_start, last_year_end),
        "Personalizado": ("custom", "custom")
    }
    
    current_period = st.session_state.get('periodo_analise', 'Último ano')
    period_mapping = {
        "Todo o período": "Todo o período",
        "Últimos 7 dias": "Últimos 7 dias",
        "Último mês": "Último mês",
        "Mês Atual": "Mês Atual",
        "Últimos 2 meses": "Últimos 2 meses",
        "Último trimestre": "Último trimestre",
        "Último semestre": "Último semestre",
        "Último ano": "Último ano",
        "Últimos 2 anos": "Últimos 2 anos",
        "Período personalizado": "Personalizado"
    }
    selectbox_option = period_mapping.get(current_period, "Último ano")
    try:
        default_index = list(period_options.keys()).index(selectbox_option)
    except ValueError:
        default_index = 3
    
    # Selectbox para períodos pré-definidos
    selected_period = st.sidebar.selectbox(
        "Período pré-definido:",
        options=list(period_options.keys()),
        index=default_index,
        key="simple_period_select"
    )
    
    start_date, end_date = period_options[selected_period]
    
    # Personalizado: inputs em DD-MM-YYYY para o usuário enxergar dia/mês/ano
    if selected_period == "Personalizado":
        actual_start = st.session_state.get("custom_start_date") or default_start
        actual_end = st.session_state.get("custom_end_date") or default_end
        if "custom_start_date_str" not in st.session_state:
            st.session_state.custom_start_date_str = _format_date_dd_mm_yyyy(actual_start)
            st.session_state.custom_end_date_str = _format_date_dd_mm_yyyy(actual_end)
        
        st.sidebar.caption("Formato: **DD-MM-AAAA** (ex: 25-02-2026)")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_str = st.text_input(
                "Data inicial:",
                key="custom_start_date_str",
                placeholder="dd-mm-aaaa",
                label_visibility="visible"
            )
        with col2:
            end_str = st.text_input(
                "Data final:",
                key="custom_end_date_str",
                placeholder="dd-mm-aaaa",
                label_visibility="visible"
            )
        
        start_parsed = _parse_dd_mm_yyyy(start_str)
        end_parsed = _parse_dd_mm_yyyy(end_str)
        if start_parsed is None or end_parsed is None:
            st.sidebar.error("Use o formato DD-MM-AAAA (ex: 25-02-2026).")
            start_date, end_date = actual_start, actual_end
        elif start_parsed > end_parsed:
            st.sidebar.error("Data inicial deve ser anterior à data final!")
            start_date, end_date = actual_start, actual_end
        else:
            start_date, end_date = start_parsed, end_parsed
        # Não alterar custom_*_date_str aqui: a chave pertence ao widget após st.text_input()
        
        st.session_state.custom_start_date = start_date
        st.session_state.custom_end_date = end_date
    
    return start_date, end_date

def render_calendar_sidebar_section() -> Tuple[str, Optional[List[datetime]]]:
    """
    Renderiza a seção do calendário na sidebar e retorna os valores para compatibilidade.
    
    Returns:
        Tupla com (periodo_string, date_range) para compatibilidade com sistema existente
    """
    # Renderizar seletor simples (mais rápido e eficiente)
    start_date, end_date = render_simple_date_picker()
    
    # Converter para formato compatível
    periodo_string = convert_calendar_to_period_filter(start_date, end_date)
    date_range = get_date_range_from_calendar(start_date, end_date)
    
    # Verificar se houve mudança no período para atualizar URL
    previous_period = st.session_state.get('periodo_analise', 'Todo o período')
    previous_start = st.session_state.get('custom_start_date')
    previous_end = st.session_state.get('custom_end_date')
    
    # Atualizar session_state para compatibilidade
    st.session_state.periodo_analise = periodo_string
    st.session_state.filter_period = periodo_string
    
    # Verificar se houve mudança no período ou nas datas personalizadas
    period_changed = previous_period != periodo_string
    dates_changed = False
    
    if periodo_string == "Período personalizado":
        current_start = st.session_state.get('custom_start_date')
        current_end = st.session_state.get('custom_end_date')
        dates_changed = (previous_start != current_start) or (previous_end != current_end)
    
    # Atualizar URL se houve mudança
    if period_changed or dates_changed:
        from utils.filtros import update_url_with_filters
        update_url_with_filters()
    
    # Mostrar resumo do período selecionado
    if start_date and end_date:
        days_diff = (end_date - start_date).days
        st.sidebar.success(
            f"📈 **Duração:** {days_diff} dias"
        )
   
    return periodo_string, date_range
