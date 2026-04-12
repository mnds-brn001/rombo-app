"""
Sistema de Enriquecimento de Dados de Estoque
============================================

Cruza dados de estoque (stock_level, cost_price) com dados de vendas
para calcular:
- Preço médio de venda por produto
- Capital imobilizado por categoria
- Valor de venda potencial
- Métricas de giro de estoque
- Dias de cobertura

Autor: Insight Expert Team
Data: Dezembro 2024
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class StockEnricher:
    """
    Enriquece dados de estoque com informações de vendas e calcula métricas financeiras.
    """
    
    def __init__(self, stock_df: pd.DataFrame, orders_df: pd.DataFrame):
        """
        Inicializa o enriquecedor de estoque.
        
        Args:
            stock_df: DataFrame com dados de estoque (product_id, stock_level, cost_price)
            orders_df: DataFrame com dados de pedidos (product_id, price, order_purchase_timestamp)
        """
        self.stock_df = stock_df.copy()
        self.orders_df = orders_df.copy()
        self.enriched_stock = None
        
        # Validar colunas obrigatórias
        self._validate_dataframes()
    
    def _validate_dataframes(self):
        """Valida se as colunas necessárias existem."""
        
        # Stock mínimo: product_id ou product_sku, stock_level
        stock_id_cols = ['product_id', 'product_sku']
        if not any(col in self.stock_df.columns for col in stock_id_cols):
            raise ValueError(f"Stock DataFrame must have one of: {stock_id_cols}")
        
        if 'stock_level' not in self.stock_df.columns:
            logger.warning("'stock_level' not found in stock_df, assuming 0")
            self.stock_df['stock_level'] = 0
        
        # Orders mínimo: product_id ou product_sku, price
        orders_id_cols = ['product_id', 'product_sku']
        if not any(col in self.orders_df.columns for col in orders_id_cols):
            raise ValueError(f"Orders DataFrame must have one of: {orders_id_cols}")
        
        if 'price' not in self.orders_df.columns:
            # Tentar valorTotalFinal ou ticket_liquido_linha
            if 'valorTotalFinal' in self.orders_df.columns:
                self.orders_df['price'] = pd.to_numeric(self.orders_df['valorTotalFinal'], errors='coerce').fillna(0)
            elif 'ticket_liquido_linha' in self.orders_df.columns:
                self.orders_df['price'] = pd.to_numeric(self.orders_df['ticket_liquido_linha'], errors='coerce').fillna(0)
            else:
                logger.warning("No price column found in orders, using 0")
                self.orders_df['price'] = 0
    
    def calculate_avg_selling_price(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        Calcula preço médio de venda por produto baseado em histórico recente.
        
        Args:
            lookback_days: Número de dias para considerar no cálculo (padrão: 90)
            
        Returns:
            DataFrame com product_id, avg_selling_price, sales_count, last_sale_date
        """
        orders_work = self.orders_df.copy()
        
        # Filtrar por período recente
        if 'order_purchase_timestamp' in orders_work.columns:
            orders_work['order_purchase_timestamp'] = pd.to_datetime(
                orders_work['order_purchase_timestamp'], errors='coerce'
            )
            cutoff_date = orders_work['order_purchase_timestamp'].max() - pd.Timedelta(days=lookback_days)
            orders_work = orders_work[orders_work['order_purchase_timestamp'] >= cutoff_date]
        
        # Determinar coluna de ID do produto
        product_id_col = 'product_id' if 'product_id' in orders_work.columns else 'product_sku'
        
        # Remover cancelados
        if 'pedido_cancelado' in orders_work.columns:
            orders_work = orders_work[orders_work['pedido_cancelado'] != 1]
        elif 'funnel_cancelled' in orders_work.columns:
            orders_work = orders_work[orders_work['funnel_cancelled'] != 1]
        
        # Calcular preço médio, quantidade vendida e última venda
        price_stats = orders_work.groupby(product_id_col).agg({
            'price': ['mean', 'median', 'std', 'count'],
            'order_purchase_timestamp': 'max' if 'order_purchase_timestamp' in orders_work.columns else 'count'
        }).reset_index()
        
        # Flatten multi-level columns
        price_stats.columns = [
            product_id_col, 
            'avg_selling_price', 
            'median_selling_price', 
            'price_std',
            'sales_count',
            'last_sale_date'
        ]
        
        return price_stats
    
    def calculate_category_aggregates(self) -> pd.DataFrame:
        """
        Agrega métricas por categoria.
        
        Returns:
            DataFrame com métricas agregadas por product_category_name
        """
        if self.enriched_stock is None:
            raise RuntimeError("Run enrich_stock() first")
        
        # Garantir coluna de categoria
        if 'product_category_name' not in self.enriched_stock.columns:
            logger.warning("No category column, using 'Sem Categoria'")
            self.enriched_stock['product_category_name'] = 'Sem Categoria'
        
        category_metrics = self.enriched_stock.groupby('product_category_name').agg({
            'stock_level': 'sum',
            'capital_imobilizado': 'sum',
            'valor_venda_potencial': 'sum',
            'margem_potencial': 'sum',
            'sales_count': 'sum',
            'days_of_coverage': 'mean',
            'inventory_turnover_annual': 'mean',
            'product_id': 'count'  # Contagem de SKUs
        }).reset_index()
        
        category_metrics.columns = [
            'category',
            'total_stock_units',
            'capital_imobilizado',
            'valor_venda_potencial',
            'margem_potencial',
            'total_sales_90d',
            'avg_days_coverage',
            'avg_turnover_annual',
            'unique_skus'
        ]
        
        # Calcular métricas derivadas
        category_metrics['capital_efficiency'] = (
            category_metrics['margem_potencial'] / category_metrics['capital_imobilizado']
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        category_metrics['sell_through_rate'] = (
            category_metrics['total_sales_90d'] / category_metrics['total_stock_units']
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        return category_metrics
    
    def enrich_stock(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        Enriquece dados de estoque com preços de venda e métricas financeiras.
        
        Args:
            lookback_days: Dias de histórico para calcular preço médio
            
        Returns:
            DataFrame enriquecido com métricas de capital e giro
        """
        logger.info("Iniciando enriquecimento de estoque...")
        
        # 1. Calcular preços médios de venda
        price_stats = self.calculate_avg_selling_price(lookback_days)
        logger.info(f"Preços calculados para {len(price_stats)} produtos")
        
        # 2. Merge stock com price stats
        product_id_col = 'product_id' if 'product_id' in self.stock_df.columns else 'product_sku'
        enriched = self.stock_df.merge(
            price_stats,
            on=product_id_col,
            how='left'
        )
        
        # 3. Garantir tipos numéricos
        numeric_cols = ['stock_level', 'cost_price', 'avg_selling_price', 'median_selling_price', 'sales_count']
        for col in numeric_cols:
            if col in enriched.columns:
                enriched[col] = pd.to_numeric(enriched[col], errors='coerce').fillna(0)
        
        # 4. Calcular métricas de capital
        enriched['capital_imobilizado'] = enriched['stock_level'] * enriched.get('cost_price', 0)
        enriched['valor_venda_potencial'] = enriched['stock_level'] * enriched['avg_selling_price']
        enriched['margem_potencial'] = enriched['valor_venda_potencial'] - enriched['capital_imobilizado']
        enriched['margem_percentual'] = (
            (enriched['margem_potencial'] / enriched['capital_imobilizado']) * 100
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        # 5. Calcular dias de cobertura e giro
        # Dias de cobertura = estoque / (vendas_90d / 90)
        enriched['daily_sales_rate'] = enriched['sales_count'] / lookback_days
        enriched['days_of_coverage'] = (
            enriched['stock_level'] / enriched['daily_sales_rate']
        ).replace([np.inf, -np.inf], 999).fillna(999)
        
        # Giro anual = (vendas_90d × 4) / estoque_médio
        # Aproximação: usar estoque atual como média
        enriched['inventory_turnover_annual'] = (
            (enriched['sales_count'] * (365 / lookback_days)) / enriched['stock_level']
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        # 6. Classificar situação do estoque
        def classify_stock_situation(row):
            days_coverage = row['days_of_coverage']
            turnover = row['inventory_turnover_annual']
            
            if days_coverage > 180:
                return 'Excesso Crítico'
            elif days_coverage > 90:
                return 'Excesso'
            elif days_coverage > 60:
                return 'Alto'
            elif days_coverage > 30:
                return 'Adequado'
            elif days_coverage > 15:
                return 'Baixo'
            else:
                return 'Risco de Ruptura'
        
        enriched['stock_situation'] = enriched.apply(classify_stock_situation, axis=1)
        
        # 7. Salvar resultado
        self.enriched_stock = enriched
        logger.info(f"Estoque enriquecido: {len(enriched)} produtos")
        
        return enriched


def load_and_enrich_stock(
    stock_path: str = "data/raw/magazord_stock_raw.parquet",
    orders_path: str = "data/processed/pedidos.parquet",
    lookback_days: int = 90
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Função conveniente para carregar e enriquecer estoque.
    
    Args:
        stock_path: Caminho para arquivo de estoque
        orders_path: Caminho para arquivo de pedidos
        lookback_days: Dias de histórico para preços
        
    Returns:
        Tupla com (stock_enriquecido, category_aggregates)
    """
    # Carregar dados
    stock_df = pd.read_parquet(stock_path)
    orders_df = pd.read_parquet(orders_path)
    
    # Enriquecer
    enricher = StockEnricher(stock_df, orders_df)
    enriched_stock = enricher.enrich_stock(lookback_days)
    category_agg = enricher.calculate_category_aggregates()
    
    return enriched_stock, category_agg


if __name__ == "__main__":
    # Teste
    enriched, categories = load_and_enrich_stock()
    print("📦 Estoque Enriquecido:")
    print(enriched.head())
    print("\n📊 Agregados por Categoria:")
    print(categories.head())

