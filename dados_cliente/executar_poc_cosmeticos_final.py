#!/usr/bin/env python3
"""
Script Executável para PoC - Cliente Distribuidora de Cosméticos
================================================================

Script completo e otimizado para processar dados do cliente de cosméticos
e gerar uma Prova de Conceito (PoC) completa com dashboards e insights.

Uso:
    python executar_poc_cosmeticos_final.py <caminho_pedidos> [--estoque <caminho_estoque>] [--output <diretorio_saida>]

Exemplo:
    python executar_poc_cosmeticos_final.py "Consulta de Pedidos.csv" --estoque "Consulta Estoque.csv" --output "poc_results"

Autor: Insight Expert Team
Data: Outubro 2024
"""

import sys
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
import time

# Importar módulos do projeto
sys.path.append(str(Path(__file__).parent.parent))

from dados_cliente.adaptador_cosmeticos import process_cosmeticos_data, process_stock_data
from utils.KPIs import calculate_kpis, calculate_product_metrics
from utils.insights import (
    calculate_revenue_insights, 
    generate_category_recommendations,
    analyze_category_performance
)

def parse_arguments():
    """Parse argumentos da linha de comando"""
    parser = argparse.ArgumentParser(
        description='Executar PoC para cliente de distribuidora de cosméticos',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python executar_poc_cosmeticos_final.py "Consulta de Pedidos.csv"
  python executar_poc_cosmeticos_final.py "pedidos.csv" --estoque "estoque.csv"
  python executar_poc_cosmeticos_final.py "pedidos.csv" --output "resultados_poc"
        """
    )
    
    parser.add_argument(
        'pedidos_file',
        help='Caminho para arquivo de pedidos (CSV)'
    )
    
    parser.add_argument(
        '--estoque',
        help='Caminho para arquivo de estoque (CSV, opcional)'
    )
    
    parser.add_argument(
        '--output',
        default='poc_cosmeticos_results',
        help='Diretório de saída para resultados (padrão: poc_cosmeticos_results)'
    )
    
    parser.add_argument(
        '--marketing-spend',
        type=float,
        default=25000.0,
        help='Gasto mensal com marketing em R$ (padrão: 25000)'
    )
    
    return parser.parse_args()

def setup_output_directory(output_dir: str) -> Path:
    """Configura diretório de saída"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Criar subdiretórios
    (output_path / "data").mkdir(exist_ok=True)
    (output_path / "reports").mkdir(exist_ok=True)
    (output_path / "charts").mkdir(exist_ok=True)
    
    return output_path

def validate_input_files(pedidos_file: str, estoque_file: str = None) -> bool:
    """Valida se os arquivos de entrada existem"""
    if not Path(pedidos_file).exists():
        print(f"❌ Erro: Arquivo de pedidos não encontrado: {pedidos_file}")
        return False
    
    if estoque_file and not Path(estoque_file).exists():
        print(f"⚠️  Aviso: Arquivo de estoque não encontrado: {estoque_file}")
        print("   Continuando sem dados de estoque...")
    
    return True

def generate_executive_summary(df: pd.DataFrame, metrics: dict, kpis: dict, output_path: Path):
    """Gera resumo executivo da PoC"""
    
    summary = {
        "poc_info": {
            "cliente": "Distribuidora de Cosméticos",
            "data_processamento": datetime.now().isoformat(),
            "periodo_dados": {
                "inicio": str(df['order_purchase_timestamp'].min().strftime('%Y-%m-%d')),
                "fim": str(df['order_purchase_timestamp'].max().strftime('%Y-%m-%d')),
                "meses": int((df['order_purchase_timestamp'].max() - df['order_purchase_timestamp'].min()).days // 30)
            }
        },
        "metricas_processamento": metrics,
        "kpis_principais": {
            "receita_total": f"R$ {kpis.get('total_revenue', 0):,.2f}",
            "pedidos_total": f"{kpis.get('total_orders', 0):,}",
            "clientes_unicos": f"{kpis.get('total_customers', 0):,}",
            "ticket_medio": f"R$ {kpis.get('average_order_value', 0):,.2f}",
            "taxa_cancelamento": f"{kpis.get('abandonment_rate', 0):.1%}",
            "satisfacao_media": f"{kpis.get('csat', 0):.1f}/5.0"
        },
        "insights_principais": [
            f"📈 Faturamento de R$ {kpis.get('total_revenue', 0):,.2f} em {metrics.get('final_records', 0):,} pedidos",
            f"👥 Base de {kpis.get('total_customers', 0):,} clientes únicos",
            f"🛍️ Ticket médio de R$ {kpis.get('average_order_value', 0):,.2f}",
            f"⭐ Satisfação média de {kpis.get('csat', 0):.1f}/5.0",
            f"📦 Portfólio com {metrics.get('unique_products', 0):,} produtos únicos",
            f"🏷️ {metrics.get('categories_found', 0)} categorias identificadas automaticamente"
        ],
        "recomendacoes_imediatas": [
            "🎯 Implementar sistema de forecasting para otimizar estoque",
            "📊 Configurar dashboards em tempo real para acompanhamento",
            "🤖 Ativar alertas automáticos para produtos em baixa",
            "📈 Implementar análise de sazonalidade para campanhas",
            "💡 Usar insights de categoria para cross-selling"
        ]
    }
    
    # Salvar resumo
    with open(output_path / "reports" / "resumo_executivo.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    
    return summary

def generate_detailed_report(df: pd.DataFrame, kpis: dict, insights: dict, output_path: Path):
    """Gera relatório detalhado em markdown"""
    
    report_content = f"""# 📊 RELATÓRIO COMPLETO - POC DISTRIBUIDORA DE COSMÉTICOS

## 🎯 Resumo Executivo

**Período Analisado**: {df['order_purchase_timestamp'].min().strftime('%d/%m/%Y')} a {df['order_purchase_timestamp'].max().strftime('%d/%m/%Y')}
**Total de Registros**: {len(df):,}
**Processamento**: {datetime.now().strftime('%d/%m/%Y às %H:%M')}

---

## 📈 KPIs Principais

| Métrica | Valor | Observação |
|---------|-------|------------|
| **Receita Total** | R$ {kpis.get('total_revenue', 0):,.2f} | Faturamento bruto do período |
| **Total de Pedidos** | {kpis.get('total_orders', 0):,} | Pedidos únicos processados |
| **Clientes Únicos** | {kpis.get('total_customers', 0):,} | Base de clientes ativa |
| **Ticket Médio** | R$ {kpis.get('average_order_value', 0):,.2f} | Valor médio por pedido |
| **Taxa de Cancelamento** | {kpis.get('abandonment_rate', 0):.1%} | Pedidos cancelados/devolvidos |
| **Satisfação (CSAT)** | {kpis.get('csat', 0):.1f}/5.0 | Nota média de satisfação |

---

## 🏷️ Análise por Categorias

### Top 5 Categorias por Receita
"""
    
    # Adicionar análise de categorias
    if 'product_category_name' in df.columns:
        category_revenue = df.groupby('product_category_name')['price'].sum().sort_values(ascending=False).head(5)
        for i, (category, revenue) in enumerate(category_revenue.items(), 1):
            report_content += f"\n{i}. **{category}**: R$ {revenue:,.2f}"
    
    report_content += f"""

---

## 💡 Insights Principais

### Receita e Performance
{insights.get('revenue', {}).get('summary', 'Análise de receita não disponível')}

### Comportamento do Cliente
- **Clientes Recorrentes**: {kpis.get('repurchase_rate', 0):.1%}
- **Tempo Médio entre Compras**: {kpis.get('avg_time_to_second', 0):.0f} dias
- **LTV Médio**: R$ {kpis.get('ltv', 0):,.2f}
- **CAC**: R$ {kpis.get('cac', 0):,.2f}

### Oportunidades Identificadas
1. **Otimização de Estoque**: Produtos com alta rotatividade precisam de reposição mais frequente
2. **Campanhas Sazonais**: Identificar picos de venda para planejamento de marketing
3. **Cross-selling**: Produtos complementares podem aumentar ticket médio
4. **Retenção**: Programas de fidelidade para aumentar recorrência

---

## 🎯 Recomendações Estratégicas

### Curto Prazo (1-3 meses)
- [ ] Implementar sistema de alertas de estoque baixo
- [ ] Configurar dashboards de acompanhamento diário
- [ ] Criar campanhas para produtos de alta margem
- [ ] Otimizar mix de produtos por categoria

### Médio Prazo (3-6 meses)
- [ ] Implementar forecasting automatizado
- [ ] Desenvolver programa de fidelidade
- [ ] Análise de sazonalidade para planejamento
- [ ] Automação de reposição de estoque

### Longo Prazo (6+ meses)
- [ ] Expansão de categorias baseada em dados
- [ ] Personalização de ofertas por cliente
- [ ] Integração com fornecedores para just-in-time
- [ ] Análise preditiva de churn de clientes

---

## 📊 Próximos Passos

1. **Validação dos Resultados**: Revisar insights com equipe comercial
2. **Implementação Gradual**: Começar com dashboards básicos
3. **Treinamento da Equipe**: Capacitar usuários no sistema
4. **Monitoramento Contínuo**: Acompanhar KPIs semanalmente
5. **Expansão**: Adicionar novas fontes de dados conforme necessário

---

*Relatório gerado automaticamente pelo sistema Insight Expert*
*Data: {datetime.now().strftime('%d/%m/%Y às %H:%M')}*
"""
    
    # Salvar relatório
    with open(output_path / "reports" / "relatorio_detalhado.md", 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"📄 Relatório detalhado salvo em: {output_path / 'reports' / 'relatorio_detalhado.md'}")

def main():
    """Função principal"""
    print("🚀 INICIANDO POC - DISTRIBUIDORA DE COSMÉTICOS")
    print("=" * 60)
    
    # Parse argumentos
    args = parse_arguments()
    
    # Validar arquivos de entrada
    if not validate_input_files(args.pedidos_file, args.estoque):
        sys.exit(1)
    
    # Configurar diretório de saída
    output_path = setup_output_directory(args.output)
    print(f"📁 Diretório de saída: {output_path.absolute()}")
    
    try:
        # ETAPA 1: Processar dados do cliente
        print(f"\n📊 ETAPA 1: Processando dados do cliente...")
        start_time = time.time()
        
        df_processed, processing_metrics = process_cosmeticos_data(
            pedidos_path=args.pedidos_file,
            estoque_path=args.estoque
        )
        
        processing_time = time.time() - start_time
        print(f"   ✅ Dados processados em {processing_time:.2f}s")
        print(f"   📊 {processing_metrics['final_records']:,} registros finais")
        
        # Salvar dados processados
        data_output = output_path / "data" / "dados_processados.parquet"
        df_processed.to_parquet(data_output, index=False)
        print(f"   💾 Dados salvos em: {data_output}")
        
        # ETAPA 1.5: Processar dados de estoque (se disponível)
        df_stock_processed = None
        stock_metrics = {}
        if args.estoque and Path(args.estoque).exists():
            print(f"\n📦 ETAPA 1.5: Processando dados de estoque...")
            start_time = time.time()
            
            df_stock_processed, stock_metrics = process_stock_data(args.estoque)
            
            stock_processing_time = time.time() - start_time
            print(f"   ✅ Dados de estoque processados em {stock_processing_time:.2f}s")
            print(f"   📊 {stock_metrics['final_records']:,} produtos no estoque")
            print(f"   🏷️ {stock_metrics['total_products']:,} SKUs únicos")
            print(f"   📦 {stock_metrics['total_stock_value']:,} unidades em estoque")
            
            # Salvar dados de estoque processados
            stock_output = output_path / "data" / "dados_estoque_processados.parquet"
            df_stock_processed.to_parquet(stock_output, index=False)
            print(f"   💾 Dados de estoque salvos em: {stock_output}")
        
        # ETAPA 2: Calcular KPIs
        print(f"\n📈 ETAPA 2: Calculando KPIs...")
        start_time = time.time()
        
        kpis = calculate_kpis(df_processed, marketing_spend=args.marketing_spend)
        
        kpi_time = time.time() - start_time
        print(f"   ✅ KPIs calculados em {kpi_time:.2f}s")
        print(f"   💰 Receita Total: R$ {kpis.get('total_revenue', 0):,.2f}")
        
        # ETAPA 3: Gerar insights
        print(f"\n💡 ETAPA 3: Gerando insights...")
        start_time = time.time()
        
        insights = {
            'revenue': calculate_revenue_insights(df_processed),
            'categories': analyze_category_performance(df_processed)
        }
        
        insights_time = time.time() - start_time
        print(f"   ✅ Insights gerados em {insights_time:.2f}s")
        
        # ETAPA 4: Gerar relatórios
        print(f"\n📄 ETAPA 4: Gerando relatórios...")
        start_time = time.time()
        
        # Resumo executivo
        summary = generate_executive_summary(df_processed, processing_metrics, kpis, output_path)
        
        # Relatório detalhado
        generate_detailed_report(df_processed, kpis, insights, output_path)
        
        # Salvar KPIs em JSON
        with open(output_path / "reports" / "kpis.json", 'w', encoding='utf-8') as f:
            json.dump(kpis, f, indent=2, ensure_ascii=False, default=str)
        
        reports_time = time.time() - start_time
        print(f"   ✅ Relatórios gerados em {reports_time:.2f}s")
        
        # RESUMO FINAL
        total_time = processing_time + kpi_time + insights_time + reports_time
        
        print(f"\n🎉 POC CONCLUÍDA COM SUCESSO!")
        print("=" * 60)
        print(f"⏱️  Tempo Total: {total_time:.2f}s")
        print(f"📊 Registros Processados: {processing_metrics['final_records']:,}")
        print(f"💰 Receita Analisada: R$ {kpis.get('total_revenue', 0):,.2f}")
        print(f"👥 Clientes Únicos: {kpis.get('total_customers', 0):,}")
        print(f"🏷️ Categorias Identificadas: {processing_metrics['categories_found']}")
        
        # Informações sobre estoque se processado
        if df_stock_processed is not None:
            print(f"📦 Produtos em Estoque: {stock_metrics['total_products']:,}")
            print(f"🏷️ SKUs Únicos: {stock_metrics['total_products']:,}")
            print(f"📊 Unidades em Estoque: {stock_metrics['total_stock_value']:,}")
        
        print(f"\n📁 ARQUIVOS GERADOS:")
        print(f"   📊 Dados: {output_path / 'data' / 'dados_processados.parquet'}")
        if df_stock_processed is not None:
            print(f"   📦 Estoque: {output_path / 'data' / 'dados_estoque_processados.parquet'}")
        print(f"   📋 Resumo: {output_path / 'reports' / 'resumo_executivo.json'}")
        print(f"   📄 Relatório: {output_path / 'reports' / 'relatorio_detalhado.md'}")
        print(f"   📈 KPIs: {output_path / 'reports' / 'kpis.json'}")
        
        print(f"\n🚀 PRÓXIMOS PASSOS:")
        print(f"   1. Revisar relatório detalhado")
        print(f"   2. Validar insights com equipe comercial")
        print(f"   3. Configurar dashboards no sistema")
        print(f"   4. Agendar apresentação dos resultados")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERRO na execução da PoC: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
