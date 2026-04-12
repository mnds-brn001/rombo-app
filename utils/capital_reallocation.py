"""
Sistema de Realocação Estratégica de Capital
==========================================

Integra:
- Estoque atual (níveis + custos)
- BCG Matrix (classificação de categorias)
- Previsões ML (tendências estruturais)
- Análise de giro e performance

Para recomendar:
- Categorias para LIQUIDAR (liberar capital)
- Categorias para REDUZIR (otimizar)
- Categorias para AUMENTAR (investir)
- Montante de capital a ser realocado

Autor: Insight Expert Team
Data: Dezembro 2024
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class CapitalReallocationAnalyzer:
    """
    Analisador de realocação de capital baseado em estoque, performance e previsões.
    """
    
    def __init__(
        self,
        enriched_stock: pd.DataFrame,
        category_performance: pd.DataFrame,
        forecast_results: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa o analisador.
        
        Args:
            enriched_stock: DataFrame de estoque enriquecido (de StockEnricher)
            category_performance: DataFrame com métricas BCG (de analyze_category_performance)
            forecast_results: Dicionário com previsões por categoria (opcional)
        """
        self.enriched_stock = enriched_stock.copy()
        self.category_performance = category_performance.copy()
        self.forecast_results = forecast_results or {}
        
        # Garantir coluna de categoria no estoque
        if 'product_category_name' not in self.enriched_stock.columns and 'category' in self.enriched_stock.columns:
            self.enriched_stock['product_category_name'] = self.enriched_stock['category']
        
        # Garantir coluna de categoria na performance
        if 'category' not in self.category_performance.columns and 'product_category_name' in self.category_performance.columns:
            self.category_performance['category'] = self.category_performance['product_category_name']
    
    def classify_reallocation_action(self, row: pd.Series) -> str:
        """
        Classifica ação de realocação baseada em BCG + Forecast + Estoque.
        
        Args:
            row: Linha do DataFrame com métricas da categoria
            
        Returns:
            Ação: 'LIQUIDAR', 'REDUZIR', 'MANTER', 'AUMENTAR', 'ESTRELA'
        """
        bcg_quadrant = row.get('bcg_quadrant', 'Indefinido')
        forecast_variation = row.get('forecast_variation', 0)
        days_coverage = row.get('avg_days_coverage', 60)
        turnover = row.get('avg_turnover_annual', 2)
        composite_score = row.get('composite_score', 0.5)
        
        # LIQUIDAR: Abacaxi + Declínio previsto + Excesso de estoque
        if bcg_quadrant == 'Abacaxi':
            if forecast_variation < -15 and days_coverage > 120:
                return 'LIQUIDAR'
            elif forecast_variation < -10 or days_coverage > 180:
                return 'LIQUIDAR'
            else:
                return 'REDUZIR'
        
        # ESTRELA: Investir pesado
        if bcg_quadrant == 'Estrela Digital':
            if forecast_variation > 10:
                return 'ESTRELA'  # Aumentar agressivamente
            else:
                return 'AUMENTAR'
        
        # VACA LEITEIRA: Otimizar
        if bcg_quadrant == 'Vaca Leiteira':
            if days_coverage > 90:
                return 'REDUZIR'  # Excesso de estoque
            elif turnover < 2:
                return 'REDUZIR'  # Giro baixo
            else:
                return 'MANTER'
        
        # INTERROGAÇÃO: Analisar forecast
        if bcg_quadrant == 'Interrogação':
            if forecast_variation > 15:
                return 'AUMENTAR'  # Aposta no crescimento
            elif forecast_variation < -10:
                return 'REDUZIR'  # Tendência negativa
            else:
                return 'MANTER'  # Aguardar sinais
        
        # Default: baseado em composite_score
        if composite_score < 0.3:
            return 'REDUZIR'
        elif composite_score > 0.7:
            return 'AUMENTAR'
        else:
            return 'MANTER'
    
    def calculate_reallocation_recommendations(
        self,
        min_capital_threshold: float = 5000,
        target_turnover: float = 4.0
    ) -> pd.DataFrame:
        """
        Calcula recomendações detalhadas de realocação de capital.
        
        Args:
            min_capital_threshold: Capital mínimo para considerar categoria
            target_turnover: Giro anual alvo
            
        Returns:
            DataFrame com recomendações por categoria
        """
        logger.info("Calculando recomendações de realocação...")
        
        # 1. Agregar estoque por categoria
        stock_by_category = self.enriched_stock.groupby('product_category_name').agg({
            'capital_imobilizado': 'sum',
            'valor_venda_potencial': 'sum',
            'margem_potencial': 'sum',
            'stock_level': 'sum',
            'sales_count': 'sum',
            'days_of_coverage': 'mean',
            'inventory_turnover_annual': 'mean',
            'product_id': 'count'
        }).reset_index()
        
        stock_by_category.columns = [
            'category',
            'capital_imobilizado',
            'valor_venda_potencial',
            'margem_potencial',
            'total_stock_units',
            'sales_90d',
            'avg_days_coverage',
            'avg_turnover_annual',
            'unique_skus'
        ]
        
        # 2. Merge com performance BCG
        recommendations = stock_by_category.merge(
            self.category_performance[[
                'category', 'bcg_quadrant', 'composite_score', 
                'growth_rate', 'market_share'
            ]],
            on='category',
            how='left'
        )
        
        # 3. Adicionar previsões de forecast
        if self.forecast_results:
            forecast_df = pd.DataFrame([
                {'category': cat, 'forecast_variation': data.get('variation', 0)}
                for cat, data in self.forecast_results.items()
            ])
            recommendations = recommendations.merge(forecast_df, on='category', how='left')
        else:
            recommendations['forecast_variation'] = 0
        
        # 4. Filtrar por capital mínimo
        recommendations = recommendations[
            recommendations['capital_imobilizado'] >= min_capital_threshold
        ].copy()
        
        # 5. Classificar ação de realocação
        recommendations['reallocation_action'] = recommendations.apply(
            self.classify_reallocation_action, axis=1
        )
        
        # 6. Calcular capital a liberar/alocar
        def calculate_capital_change(row):
            action = row['reallocation_action']
            current_capital = row['capital_imobilizado']
            current_turnover = row['avg_turnover_annual']
            
            if action == 'LIQUIDAR':
                # Liquidar 80-100% do estoque
                return -current_capital * 0.90
            
            elif action == 'REDUZIR':
                # Reduzir para atingir giro alvo
                if current_turnover < target_turnover:
                    target_stock = row['sales_90d'] * (365 / 90) / target_turnover
                    target_capital = target_stock * (current_capital / row['total_stock_units']) if row['total_stock_units'] > 0 else current_capital
                    reduction = min(current_capital - target_capital, current_capital * 0.5)  # Max 50% redução
                    return -abs(reduction)
                else:
                    return -current_capital * 0.20  # Redução moderada
            
            elif action == 'MANTER':
                return 0
            
            elif action == 'AUMENTAR':
                # Aumentar 30-50% baseado em forecast
                variation_factor = row.get('forecast_variation', 0) / 100
                increase = current_capital * min(0.5, max(0.3, 0.3 + variation_factor))
                return increase
            
            elif action == 'ESTRELA':
                # Aumentar agressivamente: 50-100%
                variation_factor = row.get('forecast_variation', 0) / 100
                increase = current_capital * min(1.0, max(0.5, 0.5 + variation_factor))
                return increase
            
            return 0
        
        recommendations['capital_change'] = recommendations.apply(calculate_capital_change, axis=1)
        recommendations['new_capital_allocation'] = (
            recommendations['capital_imobilizado'] + recommendations['capital_change']
        )
        
        # 7. Calcular impacto esperado
        recommendations['expected_turnover_improvement'] = (
            recommendations.apply(
                lambda r: target_turnover - r['avg_turnover_annual'] 
                if r['reallocation_action'] in ['REDUZIR', 'AUMENTAR', 'ESTRELA'] 
                else 0,
                axis=1
            )
        )
        
        recommendations['expected_roi_improvement'] = (
            (recommendations['expected_turnover_improvement'] / recommendations['avg_turnover_annual']) * 100
        ).replace([np.inf, -np.inf], 0).fillna(0)
        
        # 8. Priorizar por impacto financeiro
        recommendations['priority_score'] = (
            abs(recommendations['capital_change']) * 0.4 +
            recommendations['capital_imobilizado'] * 0.3 +
            abs(recommendations['forecast_variation']) * 0.2 +
            recommendations['composite_score'] * 0.1
        )
        
        # Ordenar por prioridade
        recommendations = recommendations.sort_values('priority_score', ascending=False)
        
        logger.info(f"Recomendações calculadas para {len(recommendations)} categorias")
        
        return recommendations
    
    def generate_reallocation_summary(self, recommendations: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera resumo executivo da realocação.
        
        Args:
            recommendations: DataFrame com recomendações
            
        Returns:
            Dicionário com resumo executivo
        """
        summary = {}
        
        # Capital a ser liberado (negativo)
        capital_to_free = recommendations[
            recommendations['capital_change'] < 0
        ]['capital_change'].sum()
        
        # Capital a ser alocado (positivo)
        capital_to_allocate = recommendations[
            recommendations['capital_change'] > 0
        ]['capital_change'].sum()
        
        # Balanço líquido
        net_capital_flow = capital_to_free + capital_to_allocate
        
        summary['capital_to_free'] = abs(capital_to_free)
        summary['capital_to_allocate'] = capital_to_allocate
        summary['net_capital_flow'] = net_capital_flow
        
        # Categorias por ação
        for action in ['LIQUIDAR', 'REDUZIR', 'MANTER', 'AUMENTAR', 'ESTRELA']:
            action_data = recommendations[recommendations['reallocation_action'] == action]
            summary[f'{action.lower()}_count'] = len(action_data)
            summary[f'{action.lower()}_capital'] = action_data['capital_imobilizado'].sum()
        
        # Capital total atual
        summary['total_current_capital'] = recommendations['capital_imobilizado'].sum()
        summary['total_new_capital'] = recommendations['new_capital_allocation'].sum()
        
        # ROI esperado
        weighted_roi_improvement = (
            (recommendations['expected_roi_improvement'] * recommendations['capital_imobilizado']).sum() /
            summary['total_current_capital']
        ) if summary['total_current_capital'] > 0 else 0
        summary['expected_roi_improvement'] = weighted_roi_improvement
        
        # Top 5 categorias para liquidar
        top_liquidate = recommendations[
            recommendations['reallocation_action'] == 'LIQUIDAR'
        ].nlargest(5, 'capital_imobilizado')
        summary['top_liquidate'] = top_liquidate[['category', 'capital_imobilizado', 'capital_change']].to_dict('records')
        
        # Top 5 categorias para investir
        top_invest = recommendations[
            recommendations['reallocation_action'].isin(['ESTRELA', 'AUMENTAR'])
        ].nlargest(5, 'capital_change')
        summary['top_invest'] = top_invest[['category', 'capital_imobilizado', 'capital_change']].to_dict('records')
        
        return summary
    
    def generate_actionable_plan(
        self,
        recommendations: pd.DataFrame,
        timeline_months: int = 6
    ) -> List[Dict[str, Any]]:
        """
        Gera plano de ação detalhado com timeline.
        
        Args:
            recommendations: DataFrame com recomendações
            timeline_months: Meses para implementar mudanças
            
        Returns:
            Lista de ações priorizadas com timeline
        """
        action_plan = []
        
        # Prioridade 1: LIQUIDAR (imediato - mês 1-2)
        liquidate_categories = recommendations[
            recommendations['reallocation_action'] == 'LIQUIDAR'
        ].nlargest(10, 'capital_imobilizado')
        
        for _, row in liquidate_categories.iterrows():
            action_plan.append({
                'priority': 1,
                'timeline': 'Mês 1-2 (Imediato)',
                'action': 'LIQUIDAR',
                'category': row['category'],
                'current_capital': row['capital_imobilizado'],
                'capital_to_free': abs(row['capital_change']),
                'reason': f"BCG: {row['bcg_quadrant']} | Forecast: {row['forecast_variation']:.1f}% | Cobertura: {row['avg_days_coverage']:.0f} dias",
                'tactics': [
                    f"Promoção agressiva (desconto 40-60%)",
                    f"Bundle com produtos Estrela",
                    f"Clearance sale dedicado",
                    f"Oferecer para parceiros/distribuidores"
                ]
            })
        
        # Prioridade 2: REDUZIR (curto prazo - mês 2-3)
        reduce_categories = recommendations[
            recommendations['reallocation_action'] == 'REDUZIR'
        ].nlargest(10, 'capital_imobilizado')
        
        for _, row in reduce_categories.iterrows():
            action_plan.append({
                'priority': 2,
                'timeline': 'Mês 2-3 (Curto Prazo)',
                'action': 'REDUZIR',
                'category': row['category'],
                'current_capital': row['capital_imobilizado'],
                'capital_to_free': abs(row['capital_change']),
                'reason': f"Giro: {row['avg_turnover_annual']:.1f}x/ano | Cobertura: {row['avg_days_coverage']:.0f} dias",
                'tactics': [
                    f"Reduzir pedido de reposição em 30-50%",
                    f"Promoção moderada (desconto 20-30%)",
                    f"Focar em produtos de maior giro da categoria"
                ]
            })
        
        # Prioridade 3: ESTRELA (médio prazo - mês 3-4)
        star_categories = recommendations[
            recommendations['reallocation_action'] == 'ESTRELA'
        ].nlargest(10, 'capital_change')
        
        for _, row in star_categories.iterrows():
            action_plan.append({
                'priority': 3,
                'timeline': 'Mês 3-4 (Médio Prazo)',
                'action': 'ESTRELA',
                'category': row['category'],
                'current_capital': row['capital_imobilizado'],
                'capital_to_allocate': row['capital_change'],
                'reason': f"BCG: {row['bcg_quadrant']} | Forecast: +{row['forecast_variation']:.1f}% | Score: {row['composite_score']:.2f}",
                'tactics': [
                    f"Aumentar estoque em 50-100%",
                    f"Ampliar mix de produtos",
                    f"Negociar melhores condições com fornecedor",
                    f"Investir em marketing da categoria"
                ]
            })
        
        # Prioridade 4: AUMENTAR (longo prazo - mês 4-6)
        increase_categories = recommendations[
            recommendations['reallocation_action'] == 'AUMENTAR'
        ].nlargest(10, 'capital_change')
        
        for _, row in increase_categories.iterrows():
            action_plan.append({
                'priority': 4,
                'timeline': 'Mês 4-6 (Longo Prazo)',
                'action': 'AUMENTAR',
                'category': row['category'],
                'current_capital': row['capital_imobilizado'],
                'capital_to_allocate': row['capital_change'],
                'reason': f"Forecast: +{row['forecast_variation']:.1f}% | Giro: {row['avg_turnover_annual']:.1f}x/ano",
                'tactics': [
                    f"Aumentar estoque gradualmente (30-50%)",
                    f"Testar novos produtos da categoria",
                    f"Monitorar resposta do mercado"
                ]
            })
        
        # Ordenar por prioridade e impacto
        action_plan.sort(key=lambda x: (x['priority'], -x.get('capital_to_free', x.get('capital_to_allocate', 0))))
        
        return action_plan


def analyze_capital_reallocation(
    enriched_stock: pd.DataFrame,
    category_performance: pd.DataFrame,
    forecast_results: Optional[Dict[str, Any]] = None,
    min_capital_threshold: float = 5000,
    target_turnover: float = 4.0
) -> Tuple[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]:
    """
    Função conveniente para análise completa de realocação.
    
    Args:
        enriched_stock: Estoque enriquecido
        category_performance: Performance BCG
        forecast_results: Previsões ML
        min_capital_threshold: Capital mínimo
        target_turnover: Giro alvo
        
    Returns:
        Tupla com (recommendations, summary, action_plan)
    """
    analyzer = CapitalReallocationAnalyzer(
        enriched_stock,
        category_performance,
        forecast_results
    )
    
    recommendations = analyzer.calculate_reallocation_recommendations(
        min_capital_threshold,
        target_turnover
    )
    
    summary = analyzer.generate_reallocation_summary(recommendations)
    action_plan = analyzer.generate_actionable_plan(recommendations)
    
    return recommendations, summary, action_plan


if __name__ == "__main__":
    # Teste
    print("📊 Capital Reallocation Analyzer initialized")

