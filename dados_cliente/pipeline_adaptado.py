"""
Pipeline de Dados Adaptado para o Cliente
=================================================

Este módulo contém as adaptações específicas do pipeline Insight Expert para o Cliente,
incluindo mapeamentos de campos, regras de negócio e métricas específicas para
e-commerce de moda.

Autor: Insight Expert Team
Versão: 1.0.0
Data: Janeiro 2025
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import re
import logging
from functools import wraps
import time

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Aliases Específicos ANJUSS
# ----------------------------

ANJUSS_ALIAS_MAP: Dict[str, set] = {
    # Identificadores básicos
    "order_id": {
        "pedido_id", "numero_pedido", "codigo_venda", "id_pedido",
        "order_id", "order_number", "order_code"
    },
    "customer_id": {
        "cliente_id", "codigo_cliente", "id_cliente",
        "customer_id", "client_id", "buyer_id"
    },
    "product_id": {
        "produto_id", "sku", "codigo_produto", "id_produto",
        "product_id", "item_id", "product_sku"
    },
    
    # Categorias específicas de moda
    "product_category_name": {
        "categoria", "categoria_produto", "linha_produto", 
        "colecao", "temporada", "tipo_produto", "categoria_moda",
        "linha", "familia", "grupo_produto"
    },
    "product_subcategory": {
        "subcategoria", "subcategoria_produto", "subcategoria_moda",
        "linha", "familia_produto", "grupo_produto", "tipo",
        "variacao", "modelo"
    },
    
    # Dados específicos de moda
    "product_name": {
        "nome_produto", "descricao_produto", "produto",
        "product_name", "item_name", "descricao"
    },
    "collection": {
        "colecao", "linha", "temporada", "season",
        "campanha_colecao", "nova_colecao", "colecao_ano",
        "linha_temporada", "temporada_ano"
    },
    "size": {
        "tamanho", "num_tamanho", "grade", "medida",
        "tamanho_produto", "num", "tamanho_roupa"
    },
    "color": {
        "cor", "coloracao", "variacao_cor", "cor_produto",
        "tonalidade", "matiz", "variacao"
    },
    "brand": {
        "marca", "fabricante", "marca_produto", "label",
        "nome_marca", "brand_name"
    },
    
    # Campanhas e marketing específicos
    "campaign_id": {
        "campanha", "codigo_campanha", "utm_campaign",
        "promocao", "acao_marketing", "evento", "campanha_moda",
        "lançamento", "promocao_colecao", "campaign_id"
    },
    "marketing_channel": {
        "canal", "fonte", "origem", "utm_source",
        "utm_medium", "canal_marketing", "canal_venda",
        "plataforma", "rede_social"
    },
    "influencer": {
        "influenciador", "influencer", "parceiro",
        "embaixador", "afiliado", "colaborador"
    },
    
    # Dados financeiros específicos
    "price": {
        "valor_total", "preco", "valor_venda", "total",
        "price", "amount", "value", "order_value"
    },
    "cost_price": {
        "preco_custo", "custo_produto", "custo_unitario",
        "preco_compra", "custo_mercadoria", "cmv"
    },
    "margin": {
        "margem", "margem_lucro", "lucro", "margem_produto",
        "margem_bruta", "lucro_bruto"
    },
    "discount_percentage": {
        "desconto_percentual", "percentual_desconto", "desconto_pct",
        "desconto_porcentagem", "pct_desconto"
    },
    
    # Dados de estoque
    "stock_quantity": {
        "estoque", "quantidade_estoque", "qtd_estoque",
        "saldo_estoque", "estoque_disponivel", "qtd_disponivel"
    },
    "reorder_point": {
        "ponto_reposicao", "estoque_minimo", "estoque_min",
        "nivel_reposicao", "ponto_compra"
    },
    
    # Dados de cliente específicos
    "customer_segment": {
        "segmento_cliente", "perfil_cliente", "tipo_cliente",
        "categoria_cliente", "segmento", "perfil"
    },
    "customer_lifetime_value": {
        "ltv", "valor_vida_cliente", "valor_total_cliente",
        "receita_cliente", "valor_acumulado"
    }
}

# ----------------------------
# Categorias de Moda Cliente
# ----------------------------

FASHION_CATEGORIES = {
    "Vestidos": {
        "keywords": ["vestido", "dress", "maxi", "midi", "mini", "longo", "curto"],
        "subcategories": ["Vestidos de Festa", "Vestidos Casuais", "Vestidos de Noite", "Vestidos de Praia"]
    },
    "Blusas": {
        "keywords": ["blusa", "camisa", "camiseta", "top", "regata", "manga", "gola"],
        "subcategories": ["Blusas Sociais", "Blusas Casuais", "Tops", "Camisetas"]
    },
    "Calças": {
        "keywords": ["calça", "jeans", "legging", "pantalon", "bermuda", "short"],
        "subcategories": ["Calças Jeans", "Calças Sociais", "Leggings", "Bermudas"]
    },
    "Acessórios": {
        "keywords": ["bolsa", "cinto", "óculos", "joia", "colar", "brinco", "pulseira"],
        "subcategories": ["Bolsas", "Cintos", "Joias", "Óculos", "Relógios"]
    },
    "Sapatos": {
        "keywords": ["sapato", "sandália", "bota", "tênis", "salto", "rasteira"],
        "subcategories": ["Sapatos Sociais", "Sandálias", "Tênis", "Botas"]
    },
    "Casacos": {
        "keywords": ["casaco", "jaqueta", "blazer", "cardigan", "moletom"],
        "subcategories": ["Casacos de Inverno", "Jaquetas", "Blazers", "Cardigans"]
    }
}

# ----------------------------
# Temporadas e Sazonalidade
# ----------------------------

SEASONS_MAPPING = {
    12: 'Verão', 1: 'Verão', 2: 'Verão',
    3: 'Outono', 4: 'Outono', 5: 'Outono', 
    6: 'Inverno', 7: 'Inverno', 8: 'Inverno',
    9: 'Primavera', 10: 'Primavera', 11: 'Primavera'
}

SEASONALITY_PATTERNS = {
    "Vestidos": {
        "Verão": 1.5,  # 50% mais vendas no verão
        "Primavera": 1.2,
        "Outono": 0.8,
        "Inverno": 0.5
    },
    "Blusas": {
        "Verão": 1.3,
        "Primavera": 1.1,
        "Outono": 0.9,
        "Inverno": 0.7
    },
    "Calças": {
        "Inverno": 1.4,
        "Outono": 1.2,
        "Primavera": 0.9,
        "Verão": 0.5
    },
    "Casacos": {
        "Inverno": 2.0,
        "Outono": 1.3,
        "Primavera": 0.6,
        "Verão": 0.1
    }
}

# ----------------------------
# Utilitários de Validação e Sanitização
# ----------------------------

def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Valida qualidade dos dados antes de processar.
    
    Args:
        df: DataFrame para validar
        
    Returns:
        Dicionário com issues encontrados e score de qualidade
    """
    issues = []
    warnings = []
    
    # Verificar se DataFrame não está vazio
    if df.empty:
        issues.append("DataFrame está vazio")
        return {'issues': issues, 'warnings': warnings, 'quality_score': 0}
    
    # Verificar valores absurdos em preços
    if 'price' in df.columns:
        price_col = pd.to_numeric(df['price'], errors='coerce')
        
        # Preços negativos
        negative_prices = (price_col < 0).sum()
        if negative_prices > 0:
            issues.append(f"{negative_prices} preços negativos encontrados")
        
        # Preços muito altos (outliers extremos)
        if not price_col.empty:
            q99 = price_col.quantile(0.99)
            outliers = (price_col > q99 * 10).sum()  # 10x o percentil 99
            if outliers > 0:
                warnings.append(f"{outliers} preços suspeitos (muito altos) encontrados")
        
        # Preços zerados
        zero_prices = (price_col == 0).sum()
        if zero_prices > len(df) * 0.05:  # Mais de 5% com preço zero
            warnings.append(f"{zero_prices} produtos com preço zero ({zero_prices/len(df)*100:.1f}%)")
    
    # Verificar encoding em colunas de texto
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        if col in df.columns:
            # Verificar caracteres não-ASCII
            non_ascii = df[col].astype(str).str.contains(r'[^\x00-\x7F]', na=False).sum()
            if non_ascii > len(df) * 0.3:  # Mais de 30% com chars especiais
                warnings.append(f"Coluna '{col}' com muitos caracteres especiais ({non_ascii/len(df)*100:.1f}%)")
            
            # Verificar valores muito longos
            if not df[col].empty:
                max_length = df[col].astype(str).str.len().max()
                if max_length > 500:  # Strings muito longas
                    warnings.append(f"Coluna '{col}' tem valores muito longos (max: {max_length} chars)")
    
    # Verificar duplicatas
    if 'order_id' in df.columns:
        duplicates = df['order_id'].duplicated().sum()
        if duplicates > 0:
            warnings.append(f"{duplicates} order_ids duplicados encontrados")
    
    # Verificar datas inválidas
    date_cols = ['order_purchase_timestamp', 'order_date', 'date']
    for col in date_cols:
        if col in df.columns:
            try:
                date_series = pd.to_datetime(df[col], errors='coerce')
                invalid_dates = date_series.isna().sum()
                if invalid_dates > 0:
                    warnings.append(f"{invalid_dates} datas inválidas na coluna '{col}'")
                
                # Verificar datas muito antigas ou futuras
                if not date_series.empty:
                    min_date = date_series.min()
                    max_date = date_series.max()
                    current_year = datetime.now().year
                    
                    if min_date.year < 2000:
                        warnings.append(f"Datas muito antigas encontradas (mínima: {min_date.year})")
                    if max_date.year > current_year + 1:
                        warnings.append(f"Datas futuras encontradas (máxima: {max_date.year})")
            except:
                issues.append(f"Erro ao processar datas na coluna '{col}'")
    
    # Calcular score de qualidade
    quality_score = max(0, 100 - len(issues) * 20 - len(warnings) * 5)
    
    return {
        'issues': issues,
        'warnings': warnings, 
        'quality_score': quality_score,
        'total_records': len(df)
    }

def sanitize_text_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitiza dados de texto para evitar problemas de encoding e regex.
    
    Args:
        df: DataFrame para sanitizar
        
    Returns:
        DataFrame com dados sanitizados
    """
    df_clean = df.copy()
    
    # Colunas de texto para sanitizar
    text_cols = df_clean.select_dtypes(include=['object']).columns
    
    for col in text_cols:
        if col in df_clean.columns:
            try:
                # Converter para string e tratar NaN
                df_clean[col] = df_clean[col].astype(str).fillna('')
                
                # Normalizar encoding (remover acentos se necessário)
                df_clean[col] = df_clean[col].str.normalize('NFKD')
                
                # Remover caracteres de controle
                df_clean[col] = df_clean[col].str.replace(r'[\\x00-\\x1f\\x7f-\\x9f]', '', regex=True)
                
                # Limitar tamanho máximo
                df_clean[col] = df_clean[col].str[:500]
                
                # Remover espaços extras
                df_clean[col] = df_clean[col].str.strip().str.replace(r'\s+', ' ', regex=True)
                
            except Exception as e:
                logger.warning(f"Erro ao sanitizar coluna '{col}': {e}")
    
    return df_clean

def handle_outliers(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Trata outliers em colunas numéricas.
    
    Args:
        df: DataFrame para processar
        columns: Lista de colunas para tratar (None = todas numéricas)
        
    Returns:
        DataFrame com outliers tratados
    """
    df_clean = df.copy()
    
    if columns is None:
        columns = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in columns:
        if col in df_clean.columns:
            try:
                # Converter para numérico
                numeric_col = pd.to_numeric(df_clean[col], errors='coerce')
                
                # Calcular quartis
                Q1 = numeric_col.quantile(0.25)
                Q3 = numeric_col.quantile(0.75)
                IQR = Q3 - Q1
                
                # Definir limites (método IQR)
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Para preços, não permitir valores negativos
                if col in ['price', 'cost_price', 'margin']:
                    lower_bound = max(0, lower_bound)
                
                # Aplicar limites (cap outliers em vez de remover)
                df_clean[col] = numeric_col.clip(lower=lower_bound, upper=upper_bound)
                
                outliers_count = ((numeric_col < lower_bound) | (numeric_col > upper_bound)).sum()
                if outliers_count > 0:
                    logger.info(f"Tratados {outliers_count} outliers na coluna '{col}'")
                    
            except Exception as e:
                logger.warning(f"Erro ao tratar outliers na coluna '{col}': {e}")
    
    return df_clean

# ----------------------------
# Funções de Adaptação
# ----------------------------

def apply_anjuss_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica mapeamento de aliases específicos da Cliente ao DataFrame.
    
    Args:
        df: DataFrame com dados brutos da Cliente
        
    Returns:
        DataFrame com colunas renomeadas usando aliases Cliente
    """
    df_adapted = df.copy()
    
    # Criar mapa reverso para busca eficiente
    alias_reverse_map: Dict[str, str] = {}
    for canonical, aliases in ANJUSS_ALIAS_MAP.items():
        for alias in aliases:
            alias_reverse_map[alias.lower()] = canonical
    
    # Renomear colunas
    rename_map = {}
    for col in df_adapted.columns:
        col_normalized = col.lower().strip()
        canonical_name = alias_reverse_map.get(col_normalized, col)
        if canonical_name != col:
            rename_map[col] = canonical_name
    
    if rename_map:
        df_adapted = df_adapted.rename(columns=rename_map)
        print(f"ANJUSS: {len(rename_map)} colunas renomeadas usando aliases específicos")
    
    return df_adapted

def categorize_fashion_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categoriza produtos automaticamente em categorias de moda.
    
    Args:
        df: DataFrame com dados de produtos
        
    Returns:
        DataFrame com coluna 'fashion_category' adicionada
    """
    df_categorized = df.copy()
    
    # Verificar se coluna product_name existe
    if 'product_name' not in df_categorized.columns:
        logger.warning("Coluna 'product_name' não encontrada. Pulando categorização.")
        df_categorized['fashion_category'] = 'Outros'
        df_categorized['fashion_subcategory'] = 'Não categorizado'
        return df_categorized
    
    # Sanitizar dados de produto antes da categorização
    df_categorized = sanitize_text_data(df_categorized)
    
    # Inicializar coluna de categoria
    df_categorized['fashion_category'] = 'Outros'
    df_categorized['fashion_subcategory'] = 'Não categorizado'
    
    # Aplicar categorização baseada em palavras-chave
    for category, config in FASHION_CATEGORIES.items():
        try:
            keywords = config['keywords']
            subcategories = config['subcategories']
            
            # Escapar caracteres especiais nas keywords
            escaped_keywords = [re.escape(keyword) for keyword in keywords]
            pattern = '|'.join(escaped_keywords)
            
            # Aplicar máscara para categoria principal com tratamento de erro
            try:
                mask = df_categorized['product_name'].str.contains(
                    pattern, case=False, na=False, regex=True
                )
                df_categorized.loc[mask, 'fashion_category'] = category
                
                # Tentar categorizar subcategoria (lógica simplificada)
                for subcat in subcategories:
                    subcat_keywords = subcat.lower().split()
                    escaped_subcat_keywords = [re.escape(kw) for kw in subcat_keywords]
                    subcat_pattern = '|'.join(escaped_subcat_keywords)
                    
                    try:
                        subcat_mask = (
                            mask & 
                            df_categorized['product_name'].str.contains(
                                subcat_pattern, case=False, na=False, regex=True
                            )
                        )
                        df_categorized.loc[subcat_mask, 'fashion_subcategory'] = subcat
                    except Exception as e:
                        logger.warning(f"Erro ao categorizar subcategoria '{subcat}': {e}")
                        
            except Exception as e:
                logger.warning(f"Erro ao aplicar pattern para categoria '{category}': {e}")
                continue
                
        except Exception as e:
            logger.error(f"Erro ao processar categoria '{category}': {e}")
            continue
    
    # Log de resultados
    category_counts = df_categorized['fashion_category'].value_counts()
    logger.info(f"Categorização concluída: {dict(category_counts)}")
    
    return df_categorized

def add_seasonal_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona análise sazonal aos dados.
    
    Args:
        df: DataFrame com dados de pedidos
        
    Returns:
        DataFrame com colunas sazonais adicionadas
    """
    df_seasonal = df.copy()
    
    # Converter data se necessário
    if 'order_purchase_timestamp' in df_seasonal.columns:
        df_seasonal['order_purchase_timestamp'] = pd.to_datetime(
            df_seasonal['order_purchase_timestamp'], errors='coerce'
        )
        
        # Adicionar colunas sazonais
        df_seasonal['month'] = df_seasonal['order_purchase_timestamp'].dt.month
        df_seasonal['season'] = df_seasonal['month'].map(SEASONS_MAPPING)
        df_seasonal['quarter'] = df_seasonal['order_purchase_timestamp'].dt.quarter
        df_seasonal['year'] = df_seasonal['order_purchase_timestamp'].dt.year
        
        # Adicionar indicador de sazonalidade
        df_seasonal['seasonal_index'] = 1.0
        
        # Aplicar padrões sazonais por categoria
        for category, patterns in SEASONALITY_PATTERNS.items():
            for season, multiplier in patterns.items():
                mask = (
                    (df_seasonal['fashion_category'] == category) & 
                    (df_seasonal['season'] == season)
                )
                df_seasonal.loc[mask, 'seasonal_index'] = multiplier
    
    return df_seasonal

def calculate_fashion_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula métricas específicas para e-commerce de moda.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        Dicionário com métricas específicas de moda
    """
    metrics = {}
    
    # Garantir que temos as colunas necessárias
    required_cols = ['price', 'fashion_category', 'order_id']
    if not all(col in df.columns for col in required_cols):
        print("AVISO: Colunas necessárias não encontradas para cálculo de métricas de moda")
        return metrics
    
    # Performance por categoria de moda
    category_metrics = df.groupby('fashion_category').agg({
        'price': ['sum', 'mean', 'count'],
        'order_id': 'nunique',
        'customer_id': 'nunique' if 'customer_id' in df.columns else lambda x: 0
    }).round(2)
    
    # Flatten column names
    category_metrics.columns = ['_'.join(col).strip() for col in category_metrics.columns]
    metrics['category_performance'] = category_metrics
    
    # Análise sazonal
    if 'season' in df.columns:
        seasonality = df.groupby(['fashion_category', 'season']).agg({
            'price': 'sum',
            'order_id': 'nunique'
        }).round(2)
        metrics['seasonality_analysis'] = seasonality
    
    # Performance por coleção
    if 'collection' in df.columns:
        collection_perf = df.groupby('collection').agg({
            'price': 'sum',
            'order_id': 'nunique',
            'customer_id': 'nunique' if 'customer_id' in df.columns else lambda x: 0
        }).round(2)
        metrics['collection_performance'] = collection_perf
    
    # Análise de tamanhos
    if 'size' in df.columns:
        size_analysis = df.groupby(['fashion_category', 'size']).agg({
            'order_id': 'count',
            'price': 'sum'
        }).round(2)
        metrics['size_analysis'] = size_analysis
    
    # Análise de cores
    if 'color' in df.columns:
        color_analysis = df.groupby(['fashion_category', 'color']).agg({
            'order_id': 'count',
            'price': 'sum'
        }).round(2)
        metrics['color_analysis'] = color_analysis
    
    # Métricas de margem
    if 'cost_price' in df.columns:
        df['margin'] = df['price'] - df['cost_price']
        df['margin_percentage'] = (df['margin'] / df['price'] * 100).round(2)
        
        margin_analysis = df.groupby('fashion_category').agg({
            'margin': ['sum', 'mean'],
            'margin_percentage': 'mean'
        }).round(2)
        metrics['margin_analysis'] = margin_analysis
    
    return metrics

def apply_anjuss_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica regras de negócio específicas da ANJUSS.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        DataFrame com regras de negócio aplicadas
    """
    logger.info(f"Aplicando regras de negócio em {len(df)} registros...")
    
    # Validar qualidade dos dados primeiro
    quality_report = validate_data_quality(df)
    logger.info(f"Qualidade dos dados: {quality_report['quality_score']}/100")
    
    if quality_report['issues']:
        logger.error(f"Issues críticos encontrados: {quality_report['issues']}")
        for issue in quality_report['issues']:
            logger.error(f"  - {issue}")
    
    if quality_report['warnings']:
        logger.warning("Warnings encontrados:")
        for warning in quality_report['warnings']:
            logger.warning(f"  - {warning}")
    
    df_rules = df.copy()
    
    # Tratar outliers em colunas numéricas importantes
    numeric_cols = ['price', 'cost_price'] if 'cost_price' in df_rules.columns else ['price']
    df_rules = handle_outliers(df_rules, numeric_cols)
    
    # Regra 1: Categorização automática de produtos
    try:
        df_rules = categorize_fashion_products(df_rules)
        logger.info("Categorização de produtos concluída")
    except Exception as e:
        logger.error(f"Erro na categorização de produtos: {e}")
    
    # Regra 2: Análise sazonal
    try:
        df_rules = add_seasonal_analysis(df_rules)
        logger.info("Análise sazonal concluída")
    except Exception as e:
        logger.error(f"Erro na análise sazonal: {e}")
    
    # Regra 3: Cálculo de margem se dados de custo disponíveis
    if 'cost_price' in df_rules.columns and 'price' in df_rules.columns:
        try:
            # Converter para numérico e tratar valores inválidos
            df_rules['cost_price'] = pd.to_numeric(df_rules['cost_price'], errors='coerce').fillna(0)
            df_rules['price'] = pd.to_numeric(df_rules['price'], errors='coerce').fillna(0)
            
            df_rules['margin'] = df_rules['price'] - df_rules['cost_price']
            
            # Evitar divisão por zero
            df_rules['margin_percentage'] = np.where(
                df_rules['price'] > 0,
                (df_rules['margin'] / df_rules['price'] * 100).round(2),
                0
            )
            logger.info("Cálculo de margem concluído")
        except Exception as e:
            logger.error(f"Erro no cálculo de margem: {e}")
    
    # Regra 4: Identificação de produtos estrela
    if 'order_id' in df_rules.columns and 'product_id' in df_rules.columns:
        try:
            product_sales = df_rules.groupby('product_id')['order_id'].count()
            if len(product_sales) > 0:
                high_sales_threshold = product_sales.quantile(0.8)
                df_rules['is_star_product'] = df_rules['product_id'].map(
                    lambda x: product_sales.get(x, 0) >= high_sales_threshold
                )
                star_count = df_rules['is_star_product'].sum()
                logger.info(f"Identificados {star_count} produtos estrela")
            else:
                df_rules['is_star_product'] = False
        except Exception as e:
            logger.error(f"Erro na identificação de produtos estrela: {e}")
            df_rules['is_star_product'] = False
    
    # Regra 5: Segmentação de clientes por valor
    if 'customer_id' in df_rules.columns and 'price' in df_rules.columns:
        try:
            customer_value = df_rules.groupby('customer_id')['price'].sum()
            if len(customer_value) > 0:
                df_rules['customer_segment'] = pd.cut(
                    df_rules['customer_id'].map(customer_value),
                    bins=[0, 100, 500, 1000, float('inf')],
                    labels=['Bronze', 'Prata', 'Ouro', 'Diamante'],
                    include_lowest=True
                )
                segment_counts = df_rules['customer_segment'].value_counts()
                logger.info(f"Segmentação de clientes: {dict(segment_counts)}")
            else:
                df_rules['customer_segment'] = 'Bronze'
        except Exception as e:
            logger.error(f"Erro na segmentação de clientes: {e}")
            df_rules['customer_segment'] = 'Bronze'
    
    # Regra 6: Identificação de campanhas de lançamento
    if 'campaign_id' in df_rules.columns:
        try:
            launch_keywords = ['lançamento', 'nova', 'coleção', 'launch', 'new']
            escaped_keywords = [re.escape(kw) for kw in launch_keywords]
            pattern = '|'.join(escaped_keywords)
            
            df_rules['is_launch_campaign'] = df_rules['campaign_id'].str.contains(
                pattern, case=False, na=False, regex=True
            )
            launch_count = df_rules['is_launch_campaign'].sum()
            logger.info(f"Identificadas {launch_count} campanhas de lançamento")
        except Exception as e:
            logger.error(f"Erro na identificação de campanhas: {e}")
            df_rules['is_launch_campaign'] = False
    
    logger.info(f"Regras de negócio aplicadas com sucesso em {len(df_rules)} registros")
    return df_rules

def generate_anjuss_insights(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Gera insights específicos para a ANJUSS.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        Dicionário com insights específicos
    """
    insights = {}
    
    # Insight 1: Top categorias de moda
    if 'fashion_category' in df.columns:
        top_categories = df.groupby('fashion_category')['price'].sum().sort_values(ascending=False)
        insights['top_fashion_categories'] = top_categories.head(5).to_dict()
    
    # Insight 2: Sazonalidade por categoria
    if 'season' in df.columns and 'fashion_category' in df.columns:
        seasonal_performance = df.groupby(['fashion_category', 'season'])['price'].sum().unstack(fill_value=0)
        insights['seasonal_performance'] = seasonal_performance.to_dict()
    
    # Insight 3: Performance de campanhas
    if 'campaign_id' in df.columns:
        campaign_performance = df.groupby('campaign_id').agg({
            'price': 'sum',
            'order_id': 'nunique',
            'customer_id': 'nunique' if 'customer_id' in df.columns else lambda x: 0
        }).sort_values('price', ascending=False)
        insights['top_campaigns'] = campaign_performance.head(10).to_dict()
    
    # Insight 4: Análise de margem por categoria
    if 'margin_percentage' in df.columns and 'fashion_category' in df.columns:
        margin_by_category = df.groupby('fashion_category')['margin_percentage'].mean().sort_values(ascending=False)
        insights['margin_by_category'] = margin_by_category.to_dict()
    
    # Insight 5: Produtos estrela
    if 'is_star_product' in df.columns:
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
        
        star_products = df[df['is_star_product'] == True][product_col].nunique()
        total_products = df[product_col].nunique()
        insights['star_products_percentage'] = (star_products / total_products * 100) if total_products > 0 else 0
    
    return insights

# ----------------------------
# Função Principal de Adaptação
# ----------------------------

def adapt_pipeline_for_anjuss(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Função principal que adapta o pipeline Insight Expert para a ANJUSS.
    
    Args:
        df: DataFrame com dados brutos da ANJUSS
        
    Returns:
        Tupla com (DataFrame processado, métricas específicas)
    """
    print("ANJUSS: Iniciando adaptação do pipeline...")
    
    # Passo 1: Aplicar aliases específicos
    df_adapted = apply_anjuss_aliases(df)
    
    # Passo 2: Aplicar regras de negócio
    df_processed = apply_anjuss_business_rules(df_adapted)
    
    # Passo 3: Calcular métricas específicas
    fashion_metrics = calculate_fashion_metrics(df_processed)
    
    # Passo 4: Gerar insights
    insights = generate_anjuss_insights(df_processed)
    
    print(f"ANJUSS: Pipeline adaptado com sucesso - {len(df_processed)} registros processados")
    
    return df_processed, {**fashion_metrics, **insights}

# ----------------------------
# Exemplo de Uso
# ----------------------------

if __name__ == "__main__":
    """
    Exemplo de uso das adaptações para Cliente
    """
    print("Pipeline ANJUSS - Adaptações Específicas")
    print("=" * 50)
    print("Para usar este módulo:")
    print("   from dados_cliente.anjuss_pipeline_adaptations import adapt_pipeline_for_anjuss")
    print("   df_processed, metrics = adapt_pipeline_for_anjuss(df_raw)")
    print("=" * 50)
