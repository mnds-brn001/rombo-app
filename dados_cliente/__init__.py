"""
Pipeline de Dados E-commerce - Módulo Cliente
=============================================

Este módulo fornece acesso unificado aos pipelines de dados, garantindo
compatibilidade total entre versões legado e nova.

Uso Recomendado:
---------------
from dados_cliente import run_pipeline_safe

# Para CSV (compatível com ambas versões)
stats = run_pipeline_safe('dados.csv', 'output.parquet')

# Para Google Analytics (apenas novo pipeline)
stats = run_pipeline_safe('', 'ga_data.parquet', 'google_analytics',
                         property_id='properties/123456789',
                         start_date='2024-01-01',
                         end_date='2024-01-31')
"""

# Importar função principal de compatibilidade
try:
    from .migration_helper import run_pipeline_safe, detect_pipeline_version, get_available_sources
    __all__ = ['run_pipeline_safe', 'detect_pipeline_version', 'get_available_sources']
except ImportError:
    # Fallback se migration_helper não estiver disponível
    try:
        from .cliente_pipeline import run_pipeline
        
        def run_pipeline_safe(source_or_path, output_path, source_type="csv", **kwargs):
            """Wrapper de compatibilidade simples."""
            if source_type == "csv":
                return run_pipeline(source_type, output_path, path=str(source_or_path), **kwargs)
            else:
                return run_pipeline(source_type, output_path, **kwargs)
        
        __all__ = ['run_pipeline_safe', 'run_pipeline']
    except ImportError:
        # Último fallback: não quebrar import do pacote se módulos legados não existirem.
        # Mantemos uma API mínima com erro explícito quando chamada.
        def run_pipeline_safe(source_or_path, output_path, source_type="csv", **kwargs):
            raise ImportError(
                "dados_cliente: migration_helper/cliente_pipeline indisponíveis e cliente_pipeline_legacy não encontrado. "
                "Instale dependências completas ou utilize diretamente dados_cliente/sistema_conectores.py."
            )
        __all__ = ['run_pipeline_safe']

# Informações do módulo
__version__ = "2.0.0"
__author__ = "Dashboard E-commerce Project"