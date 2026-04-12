"""
Cálculo Dinâmico de CAC e LTV
============================

Este módulo implementa cálculos dinâmicos de CAC (Customer Acquisition Cost) 
e LTV (Customer Lifetime Value) usando dados reais de múltiplas fontes:
- Meta Ads (Facebook/Instagram)
- Google Ads
- Google Analytics 4
- Dados de vendas do ERP

Autor: Dashboard E-commerce Project
Versão: 1.0.0
Data: Janeiro 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class DynamicCACLTVCalculator:
    """
    Calculadora dinâmica de CAC e LTV usando dados de múltiplas fontes.
    """
    
    def __init__(self):
        self.marketing_data = {}
        self.sales_data = pd.DataFrame()
        self.ga4_data = pd.DataFrame()
        
    def load_marketing_data(self, source: str, data: pd.DataFrame) -> None:
        """
        Carrega dados de marketing de uma fonte específica.
        
        Args:
            source: Nome da fonte ('meta_ads', 'google_ads', etc.)
            data: DataFrame com dados de marketing
        """
        self.marketing_data[source] = data
        logger.info(f"Carregados {len(data)} registros de {source}")
        
    def load_sales_data(self, data: pd.DataFrame) -> None:
        """
        Carrega dados de vendas/pedidos.
        
        Args:
            data: DataFrame com dados de vendas
        """
        self.sales_data = data
        logger.info(f"Carregados {len(data)} registros de vendas")
        
    def load_ga4_data(self, data: pd.DataFrame) -> None:
        """
        Carrega dados do Google Analytics 4.
        
        Args:
            data: DataFrame com dados do GA4
        """
        self.ga4_data = data
        logger.info(f"Carregados {len(data)} registros do GA4")
        
    def calculate_total_marketing_spend(self, start_date: datetime, end_date: datetime) -> float:
        """
        Calcula o gasto total de marketing no período.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Gasto total de marketing
        """
        total_spend = 0.0
        
        for source, data in self.marketing_data.items():
            if 'date' in data.columns and 'spend' in data.columns:
                # Filtrar por período
                data['date'] = pd.to_datetime(data['date'])
                period_data = data[
                    (data['date'] >= start_date) & 
                    (data['date'] <= end_date)
                ]
                
                source_spend = period_data['spend'].sum()
                total_spend += source_spend
                
                logger.info(f"{source}: R$ {source_spend:,.2f}")
        
        logger.info(f"Gasto total de marketing: R$ {total_spend:,.2f}")
        return total_spend
        
    def calculate_new_customers(self, start_date: datetime, end_date: datetime) -> int:
        """
        Calcula o número de novos clientes no período.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Número de novos clientes
        """
        if self.sales_data.empty:
            return 0
            
        # Filtrar vendas do período (garantindo TZ naive)
        sales_ts = pd.to_datetime(self.sales_data['order_purchase_timestamp'], utc=True).dt.tz_localize(None)
        
        sales_period = self.sales_data[
            (sales_ts >= start_date) &
            (sales_ts <= end_date)
        ]
        
        # Identificar clientes que fizeram primeira compra no período
        customer_first_purchase = self.sales_data.groupby('customer_unique_id')['order_purchase_timestamp'].min()
        customer_first_purchase = pd.to_datetime(customer_first_purchase, utc=True).dt.tz_localize(None)
        
        new_customers = customer_first_purchase[
            (customer_first_purchase >= start_date) & 
            (customer_first_purchase <= end_date)
        ]
        
        return len(new_customers)
        
    def calculate_dynamic_cac(self, start_date: datetime, end_date: datetime) -> float:
        """
        Calcula CAC dinâmico baseado em dados reais de marketing.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            CAC (Customer Acquisition Cost)
        """
        total_spend = self.calculate_total_marketing_spend(start_date, end_date)
        new_customers = self.calculate_new_customers(start_date, end_date)
        
        if new_customers == 0:
            logger.warning("Nenhum novo cliente encontrado no período")
            return 0.0
            
        cac = total_spend / new_customers
        logger.info(f"CAC calculado: R$ {cac:.2f} ({total_spend:.2f} / {new_customers})")
        
        return cac
        
    def calculate_customer_ltv(self, customer_id: str) -> float:
        """
        Calcula LTV de um cliente específico.
        
        Args:
            customer_id: ID do cliente
            
        Returns:
            LTV do cliente
        """
        if self.sales_data.empty:
            return 0.0
            
        customer_orders = self.sales_data[
            self.sales_data['customer_unique_id'] == customer_id
        ]
        
        if customer_orders.empty:
            return 0.0
            
        # Filtrar pedidos não cancelados
        valid_orders = customer_orders[customer_orders['pedido_cancelado'] == 0]
        
        # Calcular receita total do cliente
        total_revenue = valid_orders['price'].sum()
        
        return total_revenue
        
    def calculate_average_ltv(self, start_date: datetime, end_date: datetime) -> float:
        """
        Calcula LTV médio dos clientes que fizeram primeira compra no período.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            LTV médio
        """
        if self.sales_data.empty:
            return 0.0
            
        # Identificar clientes que fizeram primeira compra no período
        customer_first_purchase = self.sales_data.groupby('customer_unique_id')['order_purchase_timestamp'].min()
        customer_first_purchase = pd.to_datetime(customer_first_purchase, utc=True).dt.tz_localize(None)
        
        new_customers = customer_first_purchase[
            (customer_first_purchase >= start_date) & 
            (customer_first_purchase <= end_date)
        ].index.tolist()
        
        if not new_customers:
            return 0.0
            
        # Calcular LTV de cada novo cliente
        ltvs = []
        for customer_id in new_customers:
            ltv = self.calculate_customer_ltv(customer_id)
            if ltv > 0:
                ltvs.append(ltv)
        
        if not ltvs:
            return 0.0
            
        average_ltv = np.mean(ltvs)
        logger.info(f"LTV médio calculado: R$ {average_ltv:.2f} ({len(ltvs)} clientes)")
        
        return average_ltv
        
    def calculate_ltv_cac_ratio(self, start_date: datetime, end_date: datetime) -> Dict[str, float]:
        """
        Calcula métricas completas de LTV/CAC.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Dicionário com métricas calculadas
        """
        cac = self.calculate_dynamic_cac(start_date, end_date)
        ltv = self.calculate_average_ltv(start_date, end_date)
        
        ratio = ltv / cac if cac > 0 else 0
        
        # Métricas adicionais
        total_spend = self.calculate_total_marketing_spend(start_date, end_date)
        new_customers = self.calculate_new_customers(start_date, end_date)
        
        # Calcular ROI de marketing
        total_revenue_new_customers = ltv * new_customers if new_customers > 0 else 0
        marketing_roi = (total_revenue_new_customers - total_spend) / total_spend if total_spend > 0 else 0
        
        return {
            'cac': cac,
            'ltv': ltv,
            'ltv_cac_ratio': ratio,
            'total_marketing_spend': total_spend,
            'new_customers': new_customers,
            'marketing_roi': marketing_roi,
            'total_revenue_new_customers': total_revenue_new_customers
        }
        
    def get_channel_performance(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Analisa performance por canal de marketing.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com performance por canal
        """
        channel_metrics = []
        
        for source, data in self.marketing_data.items():
            if 'date' in data.columns and 'spend' in data.columns:
                # Filtrar por período
                data['date'] = pd.to_datetime(data['date'])
                period_data = data[
                    (data['date'] >= start_date) & 
                    (data['date'] <= end_date)
                ]
                
                total_spend = period_data['spend'].sum()
                total_conversions = period_data.get('conversions', pd.Series([0])).sum()
                total_revenue = period_data.get('purchase_value', pd.Series([0])).sum()
                
                cac_channel = total_spend / total_conversions if total_conversions > 0 else 0
                roas = total_revenue / total_spend if total_spend > 0 else 0
                
                channel_metrics.append({
                    'channel': source,
                    'spend': total_spend,
                    'conversions': total_conversions,
                    'revenue': total_revenue,
                    'cac': cac_channel,
                    'roas': roas,
                    'ctr': period_data.get('ctr', pd.Series([0])).mean(),
                    'cpc': period_data.get('cpc', pd.Series([0])).mean()
                })
        
        return pd.DataFrame(channel_metrics)
        
    def get_monthly_trends(self, months_back: int = 6) -> pd.DataFrame:
        """
        Calcula tendências mensais de CAC e LTV.
        
        Args:
            months_back: Número de meses para analisar
            
        Returns:
            DataFrame com tendências mensais
        """
        end_date = datetime.now()
        trends = []
        
        for i in range(months_back):
            month_end = end_date.replace(day=1) - timedelta(days=i*30)
            month_start = month_end.replace(day=1)
            month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            
            metrics = self.calculate_ltv_cac_ratio(month_start, month_end)
            metrics['month'] = month_start.strftime('%Y-%m')
            trends.append(metrics)
        
        return pd.DataFrame(trends).sort_values('month')

def integrate_with_acquisition_page(
    meta_ads_data: Optional[pd.DataFrame] = None,
    google_ads_data: Optional[pd.DataFrame] = None,
    ga4_data: Optional[pd.DataFrame] = None,
    sales_data: Optional[pd.DataFrame] = None,
    start_date: datetime = None,
    end_date: datetime = None
) -> Dict[str, float]:
    """
    Integra com a página de aquisição e retenção.
    
    Args:
        meta_ads_data: Dados do Meta Ads
        google_ads_data: Dados do Google Ads
        ga4_data: Dados do GA4
        sales_data: Dados de vendas
        start_date: Data inicial
        end_date: Data final
        
    Returns:
        Métricas calculadas para usar na página
    """
    calculator = DynamicCACLTVCalculator()
    
    # Carregar dados disponíveis
    if meta_ads_data is not None:
        calculator.load_marketing_data('meta_ads', meta_ads_data)
        
    if google_ads_data is not None:
        calculator.load_marketing_data('google_ads', google_ads_data)
        
    if ga4_data is not None:
        calculator.load_ga4_data(ga4_data)
        
    if sales_data is not None:
        calculator.load_sales_data(sales_data)
    
    # Usar período padrão se não fornecido
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=30)
    
    # Calcular métricas
    return calculator.calculate_ltv_cac_ratio(start_date, end_date)

# Exemplo de uso
if __name__ == "__main__":
    """
    Exemplo de uso da calculadora dinâmica
    """
    
    # Dados simulados do Meta Ads
    meta_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=30),
        'spend': np.random.uniform(100, 500, 30),
        'conversions': np.random.randint(5, 25, 30),
        'purchase_value': np.random.uniform(1000, 5000, 30)
    })
    
    # Dados simulados de vendas
    sales_data = pd.DataFrame({
        'customer_unique_id': [f'customer_{i}' for i in range(100)],
        'order_purchase_timestamp': pd.date_range('2024-01-01', periods=100, freq='D'),
        'price': np.random.uniform(50, 500, 100),
        'pedido_cancelado': np.random.choice([0, 1], 100, p=[0.9, 0.1])
    })
    
    # Calcular métricas
    metrics = integrate_with_acquisition_page(
        meta_ads_data=meta_data,
        sales_data=sales_data,
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 31)
    )
    
    print("Métricas calculadas:")
    for key, value in metrics.items():
        print(f"- {key}: {value:.2f}")
