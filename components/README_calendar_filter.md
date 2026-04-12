# 📅 Calendário de Filtros Estilizado

## Visão Geral

O novo componente de calendário substitui o filtro de período tradicional (selectbox) por uma interface mais moderna e intuitiva, mantendo total compatibilidade com o sistema existente.

## Características

### 🎨 Visual
- **Dark Metallic Glassmorphism**: Segue o tema visual do painel
- **Animações suaves**: Efeitos de shimmer e transições
- **Responsivo**: Adapta-se ao tamanho da sidebar
- **Acessível**: Suporte a teclado e leitores de tela

### ⚡ Funcionalidade
- **Períodos pré-definidos**: Último mês, trimestre, ano, etc.
- **Seleção personalizada**: Calendário interativo para datas específicas
- **Validação automática**: Impede seleções inválidas
- **Compatibilidade total**: Funciona com o sistema de filtros existente

### 🔧 Modos de Uso

1. **Calendário Completo**: Interface estilizada com HTML/CSS/JS
2. **Seletor Simples**: Componentes nativos do Streamlit (mais rápido)
3. **Toggle Automático**: Usuário escolhe entre os dois modos

## Como Usar

### Integração Básica

```python
from components.calendar_filter import render_calendar_sidebar_section

# Substituir o filtro tradicional
periodo_string, date_range = render_calendar_sidebar_section()

# Usar os valores retornados normalmente
filtered_df = apply_date_filter(df, date_range)
```

### Uso Avançado

```python
from components.calendar_filter import (
    render_glass_calendar,
    convert_calendar_to_period_filter,
    get_date_range_from_calendar
)

# Calendário personalizado
start_date, end_date = render_glass_calendar(
    key="my_calendar",
    default_start=date(2023, 1, 1),
    default_end=date.today(),
    min_date=date(2020, 1, 1),
    max_date=date.today()
)

# Converter para formato do sistema
periodo = convert_calendar_to_period_filter(start_date, end_date)
date_range = get_date_range_from_calendar(start_date, end_date)
```

## Compatibilidade

### Session State
O calendário atualiza automaticamente as seguintes variáveis:
- `st.session_state.periodo_analise`
- `st.session_state.filter_period`

### URL Persistence
Funciona com o sistema de URL persistence existente através dos callbacks de filtro.

### Formato de Retorno
Retorna os mesmos formatos esperados pelo sistema:
- `periodo_string`: String compatível com `PERIOD_OPTIONS`
- `date_range`: Lista `[datetime_inicio, datetime_fim]` ou `None`

## Configuração

### No arquivo `utils/filtros.py`

O sistema detecta automaticamente se deve usar o novo calendário:

```python
# Toggle para escolher tipo de filtro
use_calendar = st.session_state.get("use_calendar_filter", True)

if use_calendar:
    # Novo calendário
    periodo, date_range = render_calendar_sidebar_section()
else:
    # Filtro tradicional
    periodo = st.sidebar.selectbox(...)
```

### Personalização de Cores

O calendário usa as cores do tema existente automaticamente, mas pode ser personalizado:

```css
/* Cores principais */
--primary-bg: rgba(30, 41, 59, 0.9);
--secondary-bg: rgba(45, 55, 72, 0.95);
--accent-color: rgba(99, 102, 241, 0.6);
--text-color: #e3ecf0;
```

## Performance

### Calendário Completo
- **Prós**: Visual aprimorado, mais funcionalidades
- **Contras**: Pode ser mais lento em conexões lentas
- **Recomendado**: Para usuários que valorizam UX

### Seletor Simples
- **Prós**: Rápido, componentes nativos
- **Contras**: Visual mais básico
- **Recomendado**: Para máxima performance

## Teste

Execute o demo para testar todas as funcionalidades:

```bash
streamlit run test_calendar_demo.py
```

O demo inclui:
- Teste do calendário completo
- Teste do seletor simples
- Integração com dados simulados
- Verificação de compatibilidade

## Migração

### Passo 1: Ativar o novo calendário
```python
# Em utils/filtros.py, a integração já está pronta
# Basta definir use_calendar_filter = True
```

### Passo 2: Testar compatibilidade
```python
# Verificar se os valores retornados estão corretos
periodo, date_range = render_calendar_sidebar_section()
assert periodo in PERIOD_OPTIONS or periodo == "Período personalizado"
```

### Passo 3: Rollback (se necessário)
```python
# Desativar o calendário
st.session_state.use_calendar_filter = False
```

## Troubleshooting

### Problema: Calendário não aparece
**Solução**: Verificar se `streamlit.components.v1` está disponível

### Problema: Datas não são salvas
**Solução**: Verificar se a key do componente é única

### Problema: Performance lenta
**Solução**: Usar o seletor simples ou desativar animações

### Problema: Incompatibilidade com filtros existentes
**Solução**: Verificar se `periodo_analise` está sendo atualizado corretamente

## Roadmap

### Versão Atual (v1.0)
- ✅ Calendário estilizado
- ✅ Seletor simples
- ✅ Compatibilidade total
- ✅ Toggle entre modos

### Próximas Versões
- 🔄 Comunicação bidirecional JavaScript ↔ Streamlit
- 🔄 Preset periods personalizáveis
- 🔄 Integração com timezone
- 🔄 Suporte a múltiplos idiomas

## Suporte

Para dúvidas ou problemas:
1. Verificar este README
2. Executar o demo de teste
3. Consultar os logs do Streamlit
4. Reportar issues com detalhes da configuração



