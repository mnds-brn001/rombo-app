"""
Processador ETL Simplificado para Upload de Arquivos
Processa arquivos CSV usando o adaptador de cosméticos sem dependências complexas
"""
import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

# Configurar logging para evitar problemas de encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def process_uploaded_file(input_file: str, output_dir: str) -> tuple[bool, str, str]:
    """
    Processa arquivo carregado usando o adaptador de cosméticos
    
    Args:
        input_file: Caminho para arquivo CSV de entrada
        output_dir: Diretório para salvar arquivo processado
        
    Returns:
        (success, message, output_file_path)
    """
    try:
        # Importar funções do adaptador diretamente
        sys.path.append(str(Path(__file__).parent.parent))
        
        from dados_cliente.adaptador_cosmeticos import (
            normalize_column_names,
            apply_cosmeticos_aliases,
            process_dates,
            process_monetary_values,
            clean_marketplace_names,
            categorize_cosmetics_products,
            generate_synthetic_customer_states,
            calculate_derived_fields,
            map_order_status_to_funnel_stages,
            apply_business_rules
        )
        
        logger.info(f"Iniciando processamento: {input_file}")
        
        # 1. Carregar dados
        encodings = ['utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1', 'utf-8']
        df = None
        used_encoding = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(input_file, sep=';', encoding=encoding)
                used_encoding = encoding
                logger.info(f"Arquivo carregado com encoding: {encoding}")
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        
        if df is None:
            return False, "Erro: Não foi possível ler o arquivo", ""
        
        logger.info(f"Dados carregados: {len(df):,} registros")
        
        # 2. Aplicar pipeline de processamento
        logger.info("Aplicando pipeline de processamento...")
        
        # Normalizar nomes de colunas
        df = normalize_column_names(df)
        
        # Aplicar aliases
        df = apply_cosmeticos_aliases(df)
        
        # Processar datas
        df = process_dates(df)
        
        # Processar valores monetários
        df = process_monetary_values(df)
        
        # Limpar nomes de marketplaces
        df = clean_marketplace_names(df)
        
        # Categorizar produtos
        df = categorize_cosmetics_products(df)
        
        # Gerar estados sintéticos
        df = generate_synthetic_customer_states(df)
        
        # Calcular campos derivados
        df = calculate_derived_fields(df)
        
        # Mapear status para funil
        df = map_order_status_to_funnel_stages(df)
        
        # Aplicar regras de negócio
        df = apply_business_rules(df)
        
        logger.info(f"Pipeline concluído: {len(df):,} registros finais")
        
        # 3. Salvar arquivo processado
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"dados_processados_{timestamp}.parquet"
        
        df.to_parquet(output_file, index=False)
        
        logger.info(f"Arquivo salvo: {output_file}")
        
        # 4. Gerar resumo
        summary = {
            'registros_iniciais': len(df),
            'registros_finais': len(df),
            'colunas': len(df.columns),
            'periodo': {
                'inicio': df['order_purchase_timestamp'].min() if 'order_purchase_timestamp' in df.columns else None,
                'fim': df['order_purchase_timestamp'].max() if 'order_purchase_timestamp' in df.columns else None
            },
            'receita_total': df['price'].sum() if 'price' in df.columns else 0,
            'clientes_unicos': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
            'categorias': df['product_category_name'].nunique() if 'product_category_name' in df.columns else 0
        }
        
        success_msg = f"Processamento concluído com sucesso!\n"
        success_msg += f"• Registros: {summary['registros_finais']:,}\n"
        success_msg += f"• Receita: R$ {summary['receita_total']:,.2f}\n"
        success_msg += f"• Clientes: {summary['clientes_unicos']:,}\n"
        success_msg += f"• Categorias: {summary['categorias']}"
        
        return True, success_msg, str(output_file)
        
    except Exception as e:
        error_msg = f"Erro no processamento: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, ""

def main():
    """Função principal para teste"""
    if len(sys.argv) < 3:
        print("Uso: python simple_etl_processor.py <arquivo_entrada> <diretorio_saida>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    success, message, output_file = process_uploaded_file(input_file, output_dir)
    
    if success:
        print(f"✅ {message}")
        print(f"📄 Arquivo salvo: {output_file}")
    else:
        print(f"❌ {message}")
        sys.exit(1)

if __name__ == "__main__":
    main()
