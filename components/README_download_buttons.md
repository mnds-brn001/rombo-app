# 🎨 Glass Card Download Buttons - Guia de Uso

Este documento explica como usar as funções abstratas para criar botões de download com estilo glass card metálico consistente em todo o dashboard.

## 📋 Funções Disponíveis

### 1. `render_download_button_with_glass_style()`

Aplica apenas o estilo CSS aos botões de download. Use quando você quer controle total sobre o botão.

```python
from components.glass_card import render_download_button_with_glass_style

# Aplicar estilo padrão
render_download_button_with_glass_style()

# Aplicar estilo customizado
custom_colors = {
    'primary_bg': 'rgba(59, 130, 246, 0.9)',
    'secondary_bg': 'rgba(96, 165, 250, 0.95)',
    'text_color': '#ffffff'
}
render_download_button_with_glass_style(custom_colors)

# Depois criar o botão manualmente
st.download_button(
    label="📥 Exportar Dados",
    data=data,
    file_name="dados.xlsx",
    use_container_width=True
)
```

### 2. `create_styled_download_button()` ⭐ **RECOMENDADO**

Função mais avançada que combina estilo + criação do botão em uma única chamada.

```python
from components.glass_card import create_styled_download_button

# Uso básico (mais simples)
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
```

## 🎨 Cores Customizáveis

Você pode personalizar as seguintes propriedades:

| Propriedade | Descrição | Valor Padrão |
|-------------|-----------|--------------|
| `primary_bg` | Cor primária do background | `rgba(30, 41, 59, 0.9)` |
| `secondary_bg` | Cor secundária do background | `rgba(45, 55, 72, 0.95)` |
| `text_color` | Cor do texto | `#e3ecf0` |
| `border_color` | Cor da borda | `rgba(227, 236, 240, 0.3)` |
| `hover_primary` | Cor primária no hover | `rgba(45, 55, 72, 0.95)` |
| `hover_secondary` | Cor secundária no hover | `rgba(30, 41, 59, 0.9)` |
| `hover_border` | Cor da borda no hover | `rgba(227, 236, 240, 0.5)` |

## 🚀 Exemplos de Temas

### Tema Azul (Corporativo)
```python
blue_theme = {
    'primary_bg': 'rgba(59, 130, 246, 0.9)',
    'secondary_bg': 'rgba(96, 165, 250, 0.95)',
    'hover_primary': 'rgba(96, 165, 250, 0.95)',
    'hover_secondary': 'rgba(59, 130, 246, 0.9)'
}
```

### Tema Verde (Sucesso)
```python
green_theme = {
    'primary_bg': 'rgba(34, 197, 94, 0.9)',
    'secondary_bg': 'rgba(74, 222, 128, 0.95)',
    'hover_primary': 'rgba(74, 222, 128, 0.95)',
    'hover_secondary': 'rgba(34, 197, 94, 0.9)'
}
```

### Tema Roxo (Premium)
```python
purple_theme = {
    'primary_bg': 'rgba(139, 92, 246, 0.9)',
    'secondary_bg': 'rgba(167, 139, 250, 0.95)',
    'hover_primary': 'rgba(167, 139, 250, 0.95)',
    'hover_secondary': 'rgba(139, 92, 246, 0.9)'
}
```

## 📝 Migração do Código Antigo

### Antes (código duplicado):
```python
# Código CSS inline repetido em cada página
st.markdown("""
<style>
.stDownloadButton > button {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(45, 55, 72, 0.95)) !important;
    color: #e3ecf0 !important;
    # ... 30+ linhas de CSS ...
}
</style>
""", unsafe_allow_html=True)

st.download_button(
    label="📥 Exportar Dados",
    data=data,
    file_name="dados.xlsx",
    use_container_width=True
)
```

### Depois (código limpo e reutilizável):
```python
# Uma única linha!
create_styled_download_button(
    label="📥 Exportar Dados",
    data=data,
    file_name="dados.xlsx"
)
```

## ✅ Benefícios da Abstração

1. **🔄 Reprodutibilidade**: Código consistente em todas as páginas
2. **🛠️ Manutenibilidade**: Mudanças centralizadas em um local
3. **🎨 Flexibilidade**: Cores customizáveis por contexto
4. **📏 Menos Código**: Redução de 30+ linhas para 1 linha
5. **🐛 Menos Bugs**: Elimina duplicação de CSS
6. **⚡ Performance**: CSS aplicado apenas quando necessário

## 📂 Estrutura de Arquivos

```
components/
├── glass_card.py                    # Funções abstratas aqui
└── README_download_buttons.md       # Este documento

paginas/
├── analise_categorias.py           # Exemplo de uso
├── aquisicao_retencao.py           # Exemplo de uso
└── comportamento_cliente.py        # Exemplo de uso
```

## 🔧 Troubleshooting

### Problema: Estilo não aplicado
**Solução**: Certifique-se de chamar a função antes do `st.download_button()`

### Problema: Cores não funcionam
**Solução**: Verifique se as cores estão no formato correto (rgba ou hex)

### Problema: Import não encontrado
**Solução**: Verifique se está importando de `components.glass_card`

---

**💡 Dica**: Use sempre `create_styled_download_button()` para novos botões - é mais simples e mantém o código limpo!

