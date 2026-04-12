"""
Sistema de Adaptadores de Dados para Clientes Reais
==================================================

Adapta dados de diferentes formatos e sistemas para o formato padrão
do sistema de forecasting, facilitando a integração com novos clientes.

Características:
- Mapeamento flexível de colunas
- Transformações automáticas de dados
- Validação de integridade
- Suporte a múltiplos formatos
- Configuração por cliente
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import json
import yaml
from pathlib import Path

class DataFormat(Enum):
    """Formatos de dados suportados."""
    PARQUET = "parquet"
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    DATABASE = "database"

@dataclass
class ColumnMapping:
    """Mapeamento de colunas do cliente para formato padrão."""
    client_column: str
    standard_column: str
    data_type: str
    transformation: Optional[str] = None
    required: bool = True
    default_value: Any = None

@dataclass
class ClientConfig:
    """Configuração específica do cliente."""
    client_id: str
    data_format: DataFormat
    column_mappings: List[ColumnMapping]
    date_format: str = "%Y-%m-%d"
    timezone: str = "America/Sao_Paulo"
    currency: str = "BRL"
    decimal_separator: str = "."
    business_rules: Dict[str, Any] = None

class DataAdapter:
    """
    Adaptador principal para dados de clientes reais.
    """
    
    def __init__(self, config: ClientConfig):
        self.config = config
        self.standard_columns = {
            'order_id': 'order_id',
            'customer_id': 'customer_id',
            'product_id': 'product_id',
            'product_category_name': 'product_category_name',
            'order_purchase_timestamp': 'order_purchase_timestamp',
            'price': 'price',
            'freight_value': 'freight_value',
            'order_status': 'order_status',
            'review_score': 'review_score'
        }
    
    def adapt_data(self, data_source: Union[str, pd.DataFrame, Dict]) -> pd.DataFrame:
        """
        Adapta dados do cliente para formato padrão.
        
        Args:
            data_source: Caminho do arquivo, DataFrame ou dicionário com dados
            
        Returns:
            DataFrame no formato padrão
        """
        # 1. Carregar dados
        df = self._load_data(data_source)
        
        # 2. Aplicar mapeamento de colunas
        df = self._apply_column_mapping(df)
        
        # 3. Aplicar transformações
        df = self._apply_transformations(df)
        
        # 4. Validar integridade
        df = self._validate_integrity(df)
        
        # 5. Aplicar regras de negócio
        df = self._apply_business_rules(df)
        
        return df
    
    def _load_data(self, data_source: Union[str, pd.DataFrame, Dict]) -> pd.DataFrame:
        """Carrega dados de diferentes fontes."""
        if isinstance(data_source, pd.DataFrame):
            return data_source.copy()
        
        if isinstance(data_source, dict):
            return pd.DataFrame(data_source)
        
        if isinstance(data_source, str):
            path = Path(data_source)
            if not path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {data_source}")
            
            if self.config.data_format == DataFormat.PARQUET:
                return pd.read_parquet(data_source)
            elif self.config.data_format == DataFormat.CSV:
                return pd.read_csv(data_source)
            elif self.config.data_format == DataFormat.EXCEL:
                return pd.read_excel(data_source)
            elif self.config.data_format == DataFormat.JSON:
                return pd.read_json(data_source)
            else:
                raise ValueError(f"Formato não suportado: {self.config.data_format}")
        
        raise ValueError("Fonte de dados não suportada")
    
    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica mapeamento de colunas do cliente para formato padrão."""
        df_adapted = pd.DataFrame()
        
        for mapping in self.config.column_mappings:
            client_col = mapping.client_column
            standard_col = mapping.standard_column
            
            if client_col in df.columns:
                df_adapted[standard_col] = df[client_col]
            elif mapping.required:
                if mapping.default_value is not None:
                    df_adapted[standard_col] = mapping.default_value
                else:
                    raise ValueError(f"Coluna obrigatória não encontrada: {client_col}")
            else:
                # Coluna opcional não encontrada - usar valor padrão
                df_adapted[standard_col] = mapping.default_value
        
        return df_adapted
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações específicas do cliente."""
        df_transformed = df.copy()
        
        for mapping in self.config.column_mappings:
            if mapping.transformation:
                standard_col = mapping.standard_column
                if standard_col in df_transformed.columns:
                    df_transformed[standard_col] = self._apply_transformation(
                        df_transformed[standard_col], 
                        mapping.transformation
                    )
        
        return df_transformed
    
    def _apply_transformation(self, series: pd.Series, transformation: str) -> pd.Series:
        """Aplica transformação específica a uma série."""
        if transformation == "to_datetime":
            return pd.to_datetime(series, format=self.config.date_format)
        elif transformation == "to_numeric":
            return pd.to_numeric(series, errors='coerce')
        elif transformation == "to_string":
            return series.astype(str)
        elif transformation == "currency_to_float":
            return self._currency_to_float(series)
        elif transformation == "date_parse_flexible":
            return self._parse_date_flexible(series)
        else:
            return series
    
    def _currency_to_float(self, series: pd.Series) -> pd.Series:
        """Converte valores monetários para float."""
        if self.config.decimal_separator == ",":
            # Formato brasileiro: 1.234,56
            return series.str.replace(".", "").str.replace(",", ".").astype(float)
        else:
            # Formato americano: 1,234.56
            return series.str.replace(",", "").astype(float)
    
    def _parse_date_flexible(self, series: pd.Series) -> pd.Series:
        """Parse de datas flexível para diferentes formatos."""
        try:
            return pd.to_datetime(series, format=self.config.date_format)
        except:
            # Tentar parse automático
            return pd.to_datetime(series)  # infer_datetime_format é padrão agora
    
    def _validate_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida integridade dos dados adaptados."""
        # Verificar colunas obrigatórias
        required_columns = [mapping.standard_column for mapping in self.config.column_mappings 
                          if mapping.required]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias ausentes após adaptação: {missing_columns}")
        
        # Verificar tipos de dados
        for mapping in self.config.column_mappings:
            if mapping.data_type == "datetime" and mapping.standard_column in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df[mapping.standard_column]):
                    raise ValueError(f"Coluna {mapping.standard_column} não é do tipo datetime")
            elif mapping.data_type == "numeric" and mapping.standard_column in df.columns:
                if not pd.api.types.is_numeric_dtype(df[mapping.standard_column]):
                    raise ValueError(f"Coluna {mapping.standard_column} não é numérica")
        
        return df
    
    def _apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de negócio específicas do cliente."""
        if not self.config.business_rules:
            return df
        
        df_rules = df.copy()
        
        # Filtrar por status de pedido se especificado
        if 'valid_order_statuses' in self.config.business_rules:
            valid_statuses = self.config.business_rules['valid_order_statuses']
            df_rules = df_rules[df_rules['order_status'].isin(valid_statuses)]
        
        # Filtrar por faixa de preço se especificado
        if 'min_price' in self.config.business_rules:
            df_rules = df_rules[df_rules['price'] >= self.config.business_rules['min_price']]
        
        if 'max_price' in self.config.business_rules:
            df_rules = df_rules[df_rules['price'] <= self.config.business_rules['max_price']]
        
        # Filtrar por período se especificado
        if 'start_date' in self.config.business_rules:
            start_date = pd.to_datetime(self.config.business_rules['start_date'])
            df_rules = df_rules[df_rules['order_purchase_timestamp'] >= start_date]
        
        if 'end_date' in self.config.business_rules:
            end_date = pd.to_datetime(self.config.business_rules['end_date'])
            df_rules = df_rules[df_rules['order_purchase_timestamp'] <= end_date]
        
        return df_rules

class ClientConfigManager:
    """
    Gerenciador de configurações de clientes.
    """
    
    def __init__(self, config_dir: str = "client_configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
    
    def save_config(self, client_id: str, config: ClientConfig):
        """Salva configuração do cliente."""
        config_file = self.config_dir / f"{client_id}_config.yaml"
        
        config_dict = {
            'client_id': config.client_id,
            'data_format': config.data_format.value,
            'date_format': config.date_format,
            'timezone': config.timezone,
            'currency': config.currency,
            'decimal_separator': config.decimal_separator,
            'column_mappings': [
                {
                    'client_column': mapping.client_column,
                    'standard_column': mapping.standard_column,
                    'data_type': mapping.data_type,
                    'transformation': mapping.transformation,
                    'required': mapping.required,
                    'default_value': mapping.default_value
                }
                for mapping in config.column_mappings
            ],
            'business_rules': config.business_rules or {}
        }
        
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
    
    def load_config(self, client_id: str) -> ClientConfig:
        """Carrega configuração do cliente."""
        config_file = self.config_dir / f"{client_id}_config.yaml"
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuração não encontrada para cliente: {client_id}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        
        # Converter mapeamentos de colunas
        column_mappings = [
            ColumnMapping(
                client_column=mapping['client_column'],
                standard_column=mapping['standard_column'],
                data_type=mapping['data_type'],
                transformation=mapping.get('transformation'),
                required=mapping.get('required', True),
                default_value=mapping.get('default_value')
            )
            for mapping in config_dict['column_mappings']
        ]
        
        return ClientConfig(
            client_id=config_dict['client_id'],
            data_format=DataFormat(config_dict['data_format']),
            column_mappings=column_mappings,
            date_format=config_dict.get('date_format', '%Y-%m-%d'),
            timezone=config_dict.get('timezone', 'America/Sao_Paulo'),
            currency=config_dict.get('currency', 'BRL'),
            decimal_separator=config_dict.get('decimal_separator', '.'),
            business_rules=config_dict.get('business_rules', {})
        )
    
    def create_default_config(self, client_id: str, sample_data: pd.DataFrame) -> ClientConfig:
        """Cria configuração padrão baseada em dados de exemplo."""
        # Detectar colunas automaticamente
        column_mappings = []
        
        # Mapear colunas comuns
        common_mappings = {
            'order_id': ['order_id', 'id_pedido', 'pedido_id', 'order_number'],
            'customer_id': ['customer_id', 'cliente_id', 'customer_unique_id'],
            'product_id': ['product_id', 'produto_id', 'product_sku'],
            'product_category_name': ['product_category_name', 'categoria', 'category', 'categoria_produto'],
            'order_purchase_timestamp': ['order_purchase_timestamp', 'data_pedido', 'created_at', 'order_date'],
            'price': ['price', 'preco', 'valor', 'order_value', 'total_value'],
            'freight_value': ['freight_value', 'frete', 'shipping_cost'],
            'order_status': ['order_status', 'status', 'status_pedido'],
            'review_score': ['review_score', 'avaliacao', 'rating', 'score']
        }
        
        for standard_col, possible_names in common_mappings.items():
            found_col = None
            for possible_name in possible_names:
                if possible_name in sample_data.columns:
                    found_col = possible_name
                    break
            
            if found_col:
                # Detectar tipo de dados
                data_type = self._detect_data_type(sample_data[found_col])
                
                # Detectar transformação necessária
                transformation = self._detect_transformation(sample_data[found_col], data_type)
                
                column_mappings.append(ColumnMapping(
                    client_column=found_col,
                    standard_column=standard_col,
                    data_type=data_type,
                    transformation=transformation,
                    required=True
                ))
        
        return ClientConfig(
            client_id=client_id,
            data_format=DataFormat.PARQUET,  # Padrão
            column_mappings=column_mappings,
            business_rules={}
        )
    
    def _detect_data_type(self, series: pd.Series) -> str:
        """Detecta tipo de dados de uma série."""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        elif pd.api.types.is_numeric_dtype(series):
            return "numeric"
        else:
            return "string"
    
    def _detect_transformation(self, series: pd.Series, data_type: str) -> Optional[str]:
        """Detecta transformação necessária para uma série."""
        if data_type == "string":
            # Verificar se parece com data
            if series.str.contains(r'\d{4}-\d{2}-\d{2}').any():
                return "to_datetime"
            # Verificar se parece com número
            elif series.str.match(r'^\d+\.?\d*$').any():
                return "to_numeric"
            # Verificar se parece com moeda
            elif series.str.contains(r'[R$]|[,\.]\d{2}$').any():
                return "currency_to_float"
        elif data_type == "numeric":
            # Verificar se precisa de conversão de moeda
            if series.max() > 10000:  # Valores muito altos podem ser em centavos
                return "currency_to_float"
        
        return None

# Funções de conveniência
def adapt_client_data(data_source: Union[str, pd.DataFrame], 
                     client_id: str,
                     config_manager: Optional[ClientConfigManager] = None) -> pd.DataFrame:
    """
    Função de conveniência para adaptar dados de cliente.
    
    Args:
        data_source: Fonte de dados do cliente
        client_id: ID do cliente
        config_manager: Gerenciador de configurações (opcional)
        
    Returns:
        DataFrame adaptado para formato padrão
    """
    if config_manager is None:
        config_manager = ClientConfigManager()
    
    try:
        config = config_manager.load_config(client_id)
    except FileNotFoundError:
        # Criar configuração padrão se não existir
        if isinstance(data_source, pd.DataFrame):
            sample_data = data_source
        else:
            sample_data = pd.read_parquet(data_source) if str(data_source).endswith('.parquet') else pd.read_csv(data_source)
        
        config = config_manager.create_default_config(client_id, sample_data)
        config_manager.save_config(client_id, config)
    
    adapter = DataAdapter(config)
    return adapter.adapt_data(data_source)

