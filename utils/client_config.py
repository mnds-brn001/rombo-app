"""
Sistema de Configuração Flexível por Cliente
============================================

Permite configuração personalizada do sistema de forecasting para cada cliente,
incluindo thresholds, modelos, horizontes e regras de negócio específicas.

Características:
- Configuração por cliente
- Thresholds personalizáveis
- Regras de negócio específicas
- Validação de configurações
- Interface de configuração
"""

import json
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd

class BusinessType(Enum):
    """Tipos de negócio suportados."""
    ECOMMERCE = "ecommerce"
    RETAIL = "retail"
    B2B = "b2b"
    MARKETPLACE = "marketplace"
    SAAS = "saas"

class ForecastHorizon(Enum):
    """Horizontes de previsão disponíveis."""
    ULTRA_SHORT = 1
    SHORT = 7
    MEDIUM = 14
    LONG = 21
    EXTRA_LONG = 30

@dataclass
class ModelConfig:
    """Configuração de um modelo específico."""
    name: str
    enabled: bool = True
    max_forecast_days: int = 30
    min_data_points: int = 30
    params: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.params is None:
            self.params = {}

@dataclass
class ValidationConfig:
    """Configuração de validação de dados."""
    level: str = "standard"  # basic, standard, strict
    min_days: int = 30
    max_gap_days: int = 14
    min_orders_per_day: int = 2
    outlier_threshold: float = 3.0
    seasonality_min_periods: int = 14

@dataclass
class ForecastingConfig:
    """Configuração de forecasting."""
    mape_threshold: float = 18.0
    confidence_threshold: float = 0.75
    min_forecast_days: int = 7
    max_forecast_days: int = 30
    default_horizon: int = 14
    ensemble_strategy: str = "hybrid"  # individual, ensemble, hybrid
    use_temporal_validation: bool = True

@dataclass
class BusinessRules:
    """Regras de negócio específicas do cliente."""
    valid_order_statuses: List[str] = None
    min_price: float = 0.0
    max_price: float = float('inf')
    start_date: str = None
    end_date: str = None
    exclude_categories: List[str] = None
    include_categories: List[str] = None
    seasonal_adjustments: Dict[str, float] = None
    
    def __post_init__(self):
        if self.valid_order_statuses is None:
            self.valid_order_statuses = ['delivered', 'shipped', 'approved']
        if self.exclude_categories is None:
            self.exclude_categories = []
        if self.include_categories is None:
            self.include_categories = []
        if self.seasonal_adjustments is None:
            self.seasonal_adjustments = {}

@dataclass
class ClientConfig:
    """Configuração completa do cliente."""
    client_id: str
    client_name: str
    business_type: BusinessType
    data_source: str
    models: List[ModelConfig]
    validation: ValidationConfig
    forecasting: ForecastingConfig
    business_rules: BusinessRules
    created_at: str = None
    updated_at: str = None
    version: str = "1.0"
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = pd.Timestamp.now().isoformat()
        if self.updated_at is None:
            self.updated_at = pd.Timestamp.now().isoformat()

class ClientConfigManager:
    """
    Gerenciador de configurações de clientes.
    """
    
    def __init__(self, config_dir: str = "client_configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
    
    def save_config(self, config: ClientConfig) -> bool:
        """Salva configuração do cliente."""
        try:
            config.updated_at = pd.Timestamp.now().isoformat()
            config_file = self.config_dir / f"{config.client_id}_config.yaml"
            
            config_dict = self._config_to_dict(config)
            
            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
            
            return True
        except Exception as e:
            print(f"Erro ao salvar configuração: {e}")
            return False
    
    def load_config(self, client_id: str) -> Optional[ClientConfig]:
        """Carrega configuração do cliente."""
        try:
            config_file = self.config_dir / f"{client_id}_config.yaml"
            
            if not config_file.exists():
                return None
            
            with open(config_file, 'r', encoding='utf-8') as f:
                config_dict = yaml.safe_load(f)
            
            return self._dict_to_config(config_dict)
        except Exception as e:
            print(f"Erro ao carregar configuração: {e}")
            return None
    
    def list_clients(self) -> List[str]:
        """Lista todos os clientes com configuração."""
        config_files = list(self.config_dir.glob("*_config.yaml"))
        return [f.stem.replace("_config", "") for f in config_files]
    
    def create_default_config(self, client_id: str, client_name: str, 
                            business_type: BusinessType) -> ClientConfig:
        """Cria configuração padrão para novo cliente."""
        # Modelos padrão
        models = [
            ModelConfig(name="baseline", enabled=True, max_forecast_days=30, min_data_points=7),
            ModelConfig(name="arima", enabled=True, max_forecast_days=30, min_data_points=30),
            ModelConfig(name="prophet", enabled=True, max_forecast_days=30, min_data_points=30),
            ModelConfig(name="xgboost", enabled=True, max_forecast_days=30, min_data_points=60),
            ModelConfig(name="lightgbm", enabled=True, max_forecast_days=30, min_data_points=60),
            ModelConfig(name="lstm", enabled=True, max_forecast_days=30, min_data_points=90)
        ]
        
        # Configuração de validação baseada no tipo de negócio
        validation_config = self._get_default_validation_config(business_type)
        
        # Configuração de forecasting baseada no tipo de negócio
        forecasting_config = self._get_default_forecasting_config(business_type)
        
        # Regras de negócio baseadas no tipo
        business_rules = self._get_default_business_rules(business_type)
        
        return ClientConfig(
            client_id=client_id,
            client_name=client_name,
            business_type=business_type,
            data_source="",  # Será preenchido quando dados forem carregados
            models=models,
            validation=validation_config,
            forecasting=forecasting_config,
            business_rules=business_rules
        )
    
    def _get_default_validation_config(self, business_type: BusinessType) -> ValidationConfig:
        """Retorna configuração de validação padrão baseada no tipo de negócio."""
        configs = {
            BusinessType.ECOMMERCE: ValidationConfig(
                level="standard",
                min_days=30,
                max_gap_days=14,
                min_orders_per_day=2,
                outlier_threshold=3.0,
                seasonality_min_periods=14
            ),
            BusinessType.RETAIL: ValidationConfig(
                level="strict",
                min_days=60,
                max_gap_days=7,
                min_orders_per_day=5,
                outlier_threshold=2.0,
                seasonality_min_periods=21
            ),
            BusinessType.B2B: ValidationConfig(
                level="basic",
                min_days=14,
                max_gap_days=30,
                min_orders_per_day=1,
                outlier_threshold=5.0,
                seasonality_min_periods=7
            ),
            BusinessType.MARKETPLACE: ValidationConfig(
                level="standard",
                min_days=45,
                max_gap_days=10,
                min_orders_per_day=3,
                outlier_threshold=2.5,
                seasonality_min_periods=21
            ),
            BusinessType.SAAS: ValidationConfig(
                level="strict",
                min_days=90,
                max_gap_days=5,
                min_orders_per_day=10,
                outlier_threshold=1.5,
                seasonality_min_periods=30
            )
        }
        return configs.get(business_type, configs[BusinessType.ECOMMERCE])
    
    def _get_default_forecasting_config(self, business_type: BusinessType) -> ForecastingConfig:
        """Retorna configuração de forecasting padrão baseada no tipo de negócio."""
        configs = {
            BusinessType.ECOMMERCE: ForecastingConfig(
                mape_threshold=18.0,
                confidence_threshold=0.75,
                min_forecast_days=7,
                max_forecast_days=21,
                default_horizon=14,
                ensemble_strategy="hybrid",
                use_temporal_validation=True
            ),
            BusinessType.RETAIL: ForecastingConfig(
                mape_threshold=15.0,
                confidence_threshold=0.80,
                min_forecast_days=7,
                max_forecast_days=14,
                default_horizon=7,
                ensemble_strategy="ensemble",
                use_temporal_validation=True
            ),
            BusinessType.B2B: ForecastingConfig(
                mape_threshold=25.0,
                confidence_threshold=0.70,
                min_forecast_days=7,
                max_forecast_days=30,
                default_horizon=21,
                ensemble_strategy="individual",
                use_temporal_validation=False
            ),
            BusinessType.MARKETPLACE: ForecastingConfig(
                mape_threshold=20.0,
                confidence_threshold=0.75,
                min_forecast_days=7,
                max_forecast_days=21,
                default_horizon=14,
                ensemble_strategy="hybrid",
                use_temporal_validation=True
            ),
            BusinessType.SAAS: ForecastingConfig(
                mape_threshold=12.0,
                confidence_threshold=0.85,
                min_forecast_days=7,
                max_forecast_days=14,
                default_horizon=7,
                ensemble_strategy="ensemble",
                use_temporal_validation=True
            )
        }
        return configs.get(business_type, configs[BusinessType.ECOMMERCE])
    
    def _get_default_business_rules(self, business_type: BusinessType) -> BusinessRules:
        """Retorna regras de negócio padrão baseadas no tipo de negócio."""
        rules = {
            BusinessType.ECOMMERCE: BusinessRules(
                valid_order_statuses=['delivered', 'shipped', 'approved'],
                min_price=0.01,
                max_price=10000.0,
                exclude_categories=['test', 'sample'],
                seasonal_adjustments={
                    'black_friday': 2.5,
                    'christmas': 3.0,
                    'mothers_day': 1.8,
                    'valentines_day': 1.5
                }
            ),
            BusinessType.RETAIL: BusinessRules(
                valid_order_statuses=['delivered', 'shipped'],
                min_price=1.0,
                max_price=5000.0,
                exclude_categories=['damaged', 'returned'],
                seasonal_adjustments={
                    'back_to_school': 1.5,
                    'holiday_season': 2.0
                }
            ),
            BusinessType.B2B: BusinessRules(
                valid_order_statuses=['delivered', 'shipped', 'approved', 'pending'],
                min_price=10.0,
                max_price=100000.0,
                exclude_categories=[],
                seasonal_adjustments={}
            ),
            BusinessType.MARKETPLACE: BusinessRules(
                valid_order_statuses=['delivered', 'shipped', 'approved'],
                min_price=0.01,
                max_price=50000.0,
                exclude_categories=['fraud', 'cancelled'],
                seasonal_adjustments={
                    'prime_day': 2.0,
                    'cyber_monday': 2.5
                }
            ),
            BusinessType.SAAS: BusinessRules(
                valid_order_statuses=['active', 'trial', 'converted'],
                min_price=0.0,
                max_price=10000.0,
                exclude_categories=[],
                seasonal_adjustments={
                    'new_year': 0.8,
                    'summer': 0.9
                }
            )
        }
        return rules.get(business_type, rules[BusinessType.ECOMMERCE])
    
    def _config_to_dict(self, config: ClientConfig) -> Dict[str, Any]:
        """Converte configuração para dicionário."""
        config_dict = asdict(config)
        
        # Converter enums para strings
        config_dict['business_type'] = config.business_type.value
        
        # Converter modelos
        config_dict['models'] = [asdict(model) for model in config.models]
        
        return config_dict
    
    def _dict_to_config(self, config_dict: Dict[str, Any]) -> ClientConfig:
        """Converte dicionário para configuração."""
        # Converter string para enum
        config_dict['business_type'] = BusinessType(config_dict['business_type'])
        
        # Converter modelos
        config_dict['models'] = [ModelConfig(**model) for model in config_dict['models']]
        
        # Converter subconfigurações
        config_dict['validation'] = ValidationConfig(**config_dict['validation'])
        config_dict['forecasting'] = ForecastingConfig(**config_dict['forecasting'])
        config_dict['business_rules'] = BusinessRules(**config_dict['business_rules'])
        
        return ClientConfig(**config_dict)
    
    def validate_config(self, config: ClientConfig) -> List[str]:
        """Valida configuração do cliente."""
        errors = []
        
        # Validar IDs
        if not config.client_id or not config.client_id.strip():
            errors.append("Client ID não pode estar vazio")
        
        if not config.client_name or not config.client_name.strip():
            errors.append("Client Name não pode estar vazio")
        
        # Validar modelos
        if not config.models:
            errors.append("Pelo menos um modelo deve estar habilitado")
        
        enabled_models = [m for m in config.models if m.enabled]
        if not enabled_models:
            errors.append("Pelo menos um modelo deve estar habilitado")
        
        # Validar thresholds
        if config.forecasting.mape_threshold < 0 or config.forecasting.mape_threshold > 100:
            errors.append("MAPE threshold deve estar entre 0 e 100")
        
        if config.forecasting.confidence_threshold < 0 or config.forecasting.confidence_threshold > 1:
            errors.append("Confidence threshold deve estar entre 0 e 1")
        
        if config.forecasting.min_forecast_days > config.forecasting.max_forecast_days:
            errors.append("Min forecast days não pode ser maior que max forecast days")
        
        # Validar regras de negócio
        if config.business_rules.min_price < 0:
            errors.append("Min price não pode ser negativo")
        
        if config.business_rules.max_price <= config.business_rules.min_price:
            errors.append("Max price deve ser maior que min price")
        
        return errors

# Funções de conveniência
def get_client_config(client_id: str, config_manager: Optional[ClientConfigManager] = None) -> Optional[ClientConfig]:
    """Função de conveniência para obter configuração do cliente."""
    if config_manager is None:
        config_manager = ClientConfigManager()
    return config_manager.load_config(client_id)

def create_new_client_config(client_id: str, client_name: str, business_type: str) -> ClientConfig:
    """Função de conveniência para criar nova configuração."""
    config_manager = ClientConfigManager()
    business_type_enum = BusinessType(business_type.lower())
    return config_manager.create_default_config(client_id, client_name, business_type_enum)

