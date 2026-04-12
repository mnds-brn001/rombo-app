"""
Conector Cliente - Implementação Específica
==========================================

Este módulo implementa conectores específicos para os sistemas da Cliente,
incluindo adaptações para diferentes tipos de ERP/CRM e marketplaces.

Autor: Insight Expert Team
Versão: 1.0.0
Data: Janeiro 2025
"""

import pandas as pd
import requests
import json
import base64
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # carrega variáveis do arquivo .env, se existir
except Exception:
    pass

# SQLAlchemy is optional for tests that don't touch DB paths. Provide a shim when absent.
try:
    import sqlalchemy as sa
except ModuleNotFoundError:
    class _SaShim:  # minimal shim to satisfy references when SQLAlchemy isn't installed
        class exc:
            class SQLAlchemyError(Exception):
                pass

        @staticmethod
        def text(sql: str):
            return sql

        @staticmethod
        def create_engine(*args, **kwargs):
            raise ModuleNotFoundError("sqlalchemy is required for database connections")

    sa = _SaShim()

from pathlib import Path
import logging
import time
from functools import wraps
import threading
from collections import defaultdict

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Utilitários de Retry e Rate Limiting
# ----------------------------

class RateLimiter:
    """Rate limiter thread-safe para controlar chamadas de API"""
    
    def __init__(self, max_calls: int, time_window: int):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = defaultdict(list)
        self.lock = threading.Lock()
    
    def wait_if_needed(self, key: str = 'default'):
        """Espera se necessário para respeitar rate limit"""
        with self.lock:
            now = time.time()
            
            # Remove chamadas antigas
            self.calls[key] = [call_time for call_time in self.calls[key] 
                              if now - call_time < self.time_window]
            
            # Verifica se precisa esperar
            if len(self.calls[key]) >= self.max_calls:
                oldest_call = min(self.calls[key])
                wait_time = self.time_window - (now - oldest_call)
                if wait_time > 0:
                    # Adiciona um buffer de segurança de 5%
                    wait_time = wait_time * 1.05
                    logger.info(f"Rate limit preventivo ({key}). Aguardando {wait_time:.2f}s...")
                    time.sleep(wait_time)
            
            # Registra a nova chamada
            self.calls[key].append(now)

def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 2, 
                      exceptions: tuple = (Exception,)):
    """Decorator para retry com backoff exponencial e tratamento especial para 429"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # Checar se é erro 429 (Too Many Requests)
                    response = getattr(e, 'response', None)
                    status_code = getattr(response, 'status_code', None)
                    
                    if status_code == 429:
                        # Espera agressiva para 429: 60s, 120s, etc.
                        wait_time = 60 * (attempt + 1)
                        logger.warning(f"⛔ Bloqueio 429 detectado! Pausando operação por {wait_time}s para esfriar API...")
                        time.sleep(wait_time)
                        continue
                    
                    if attempt == max_retries:
                        logger.error(f"Falha após {max_retries} tentativas: {e}")
                        raise e
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Tentativa {attempt + 1} falhou: {e}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
            
            raise last_exception
        return wrapper
    return decorator

def timeout_handler(timeout_seconds: int = 30):
    """Decorator para timeout em operações"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import signal
            
            def timeout_signal(signum, frame):
                raise TimeoutError(f"Operação excedeu {timeout_seconds}s")
            
            # Configurar timeout (apenas em sistemas Unix)
            try:
                signal.signal(signal.SIGALRM, timeout_signal)
                signal.alarm(timeout_seconds)
                result = func(*args, **kwargs)
                signal.alarm(0)  # Cancelar timeout
                return result
            except AttributeError:
                # Windows não suporta SIGALRM, executar sem timeout
                return func(*args, **kwargs)
            except TimeoutError:
                logger.error(f"Timeout na operação {func.__name__}")
                raise
        return wrapper
    return decorator

# ----------------------------
# Configurações Flexíveis de Queries
# ----------------------------

TOTVS_QUERY_TEMPLATES = {
    'standard': {
        'orders': """
            SELECT 
                C5_NUM as order_id,
                C5_CLIENTE as customer_id,
                C5_EMISSAO as order_date,
                C5_VALOR as total_value,
                C5_STATUS as order_status,
                C5_TIPO as order_type
            FROM SC5010 
            WHERE C5_EMISSAO BETWEEN :start_date AND :end_date
            AND C5_FILIAL = :filial
            AND D_E_L_E_T_ = ' '
        """,
        'products': """
            SELECT 
                B1_COD as product_id,
                B1_DESC as product_name,
                B1_TIPO as product_type,
                B1_UM as unit_measure,
                B1_CUSTD as cost_price
            FROM SB1010
            WHERE D_E_L_E_T_ = ' '
            AND B1_FILIAL = :filial
        """,
        'customers': """
            SELECT 
                A1_COD as customer_id,
                A1_NOME as customer_name,
                A1_EMAIL as email,
                A1_DDD + A1_TEL as phone,
                A1_EST as state,
                A1_MUN as city
            FROM SA1010
            WHERE D_E_L_E_T_ = ' '
            AND A1_FILIAL = :filial
        """
    },
    'customized_v1': {
        'orders': """
            SELECT 
                PEDIDO as order_id,
                CLIENTE as customer_id,
                DATA_PEDIDO as order_date,
                VALOR_TOTAL as total_value,
                STATUS as order_status
            FROM VENDAS
            WHERE DATA_PEDIDO BETWEEN :start_date AND :end_date
        """,
        'products': """
            SELECT 
                CODIGO as product_id,
                DESCRICAO as product_name,
                CATEGORIA as product_category,
                PRECO_CUSTO as cost_price
            FROM PRODUTOS
            WHERE ATIVO = 'S'
        """
    },
    'customized_v2': {
        'orders': """
            SELECT 
                NR_PEDIDO as order_id,
                CD_CLIENTE as customer_id,
                DT_PEDIDO as order_date,
                VL_TOTAL as total_value,
                ST_PEDIDO as order_status
            FROM TB_PEDIDOS
            WHERE DT_PEDIDO BETWEEN :start_date AND :end_date
            AND ST_PEDIDO IN ('FATURADO', 'ENTREGUE')
        """
    }
}

# ----------------------------
# Configurações Cliente
# ----------------------------

class ClienteConfig:
    """Configurações específicas para Cliente"""
    
    def __init__(self):
        # Configurações de sistema (serão preenchidas durante implementação)
        self.erp_type = None  # TOTVS, SAP, Linx, Magazord etc.
        self.erp_config = {}
        self.marketplace_configs = {}
        self.ga4_config = {}
        self.margin_mappings = {}
        
        # Configurações de rate limiting
        self.rate_limits = {
            'totvs_api': {'max_calls': 100, 'time_window': 60},  # 100 calls/min
            'mercado_livre': {'max_calls': 1000, 'time_window': 3600},  # 1000 calls/hour
            'amazon': {'max_calls': 200, 'time_window': 60},  # 200 calls/min
            'ga4': {'max_calls': 100, 'time_window': 60},  # 100 calls/min
            'meta_ads': {'max_calls': 200, 'time_window': 3600},  # 200 calls/hour
            'magazord_api': {'max_calls': 100, 'time_window': 60},  # 100 calls/min
            # VTEX (OMS/Logistics) tende a ser bem estável, mas protegemos com um limite conservador.
            'vtex_api': {'max_calls': 120, 'time_window': 60},  # 120 calls/min
        }
        
        # Template de queries personalizado
        self.query_template = 'standard'
        
    def set_erp_config(self, erp_type: str, config: Dict[str, Any]):
        """Define configuração do ERP"""
        self.erp_type = erp_type
        self.erp_config = config
        
    def set_marketplace_config(self, marketplace: str, config: Dict[str, Any]):
        """Define configuração de marketplace"""
        self.marketplace_configs[marketplace] = config
        
    def set_ga4_config(self, config: Dict[str, Any]):
        """Define configuração do Google Analytics"""
        self.ga4_config = config
        
    def set_rate_limits(self, service: str, max_calls: int, time_window: int):
        """Define rate limits para um serviço específico"""
        self.rate_limits[service] = {'max_calls': max_calls, 'time_window': time_window}
        
    def set_query_template(self, template_name: str):
        """Define template de queries SQL a ser usado"""
        if template_name in TOTVS_QUERY_TEMPLATES:
            self.query_template = template_name
        else:
            logger.warning(f"Template '{template_name}' não encontrado. Usando 'standard'.")
            self.query_template = 'standard'

    def set_margin_mapping(self, scope: str, mapping: Dict[str, Any]) -> None:
        """Define mapeamento de margens por escopo (ex.: 'erp', 'marketplace:mercado_livre')."""
        self.margin_mappings[scope] = mapping

    def get_margin_mapping(self, scope: str) -> Dict[str, Any]:
        """Recupera mapeamento de margens; retorna {} se não existir."""
        return self.margin_mappings.get(scope, {})

# Instância global de configuração
cliente_config = ClienteConfig()

# ----------------------------
# Mapeamento de Margens
# ----------------------------

# Colunas alvo para margens no dataset padronizado
MARGIN_TARGET_COLUMNS: Tuple[str, ...] = (
    "product_cost",
    "marketplace_commission",
    "payment_gateway_fee",
    "tax_amount",
    "packaging_cost",
)

def apply_margin_mapping(df: pd.DataFrame, mapping: Optional[Dict[str, Any]]) -> pd.DataFrame:
    """
    Aplica mapeamento de margens para preencher colunas alvo quando ausentes.

    mapping esperado:
        {
          "value_field": "total_value" | "price",
          "cost_field": "cost_price",
          "cost_ratio": 0.58,
          "commission_rate": 0.12,
          "gateway_rate": 0.025,
          "tax_rate": 0.17,
          "packaging_value": 1.20
        }
    """
    if df is None or df.empty:
        return df
    if not mapping:
        return df

    df_out = df.copy()

    value_field = mapping.get("value_field", "price")
    base_price = pd.to_numeric(df_out.get(value_field, df_out.get("price", 0.0)), errors="coerce").fillna(0.0)

    # product_cost
    if "product_cost" not in df_out.columns or df_out["product_cost"].isna().all():
        if "cost_field" in mapping and mapping["cost_field"] in df_out.columns:
            df_out["product_cost"] = pd.to_numeric(df_out[mapping["cost_field"]], errors="coerce").fillna(
                base_price * float(mapping.get("cost_ratio", 0.60))
            )
        else:
            df_out["product_cost"] = base_price * float(mapping.get("cost_ratio", 0.60))

    # marketplace_commission
    if "marketplace_commission" not in df_out.columns or df_out["marketplace_commission"].isna().all():
        df_out["marketplace_commission"] = base_price * float(mapping.get("commission_rate", 0.0))

    # payment_gateway_fee
    if "payment_gateway_fee" not in df_out.columns or df_out["payment_gateway_fee"].isna().all():
        df_out["payment_gateway_fee"] = base_price * float(mapping.get("gateway_rate", 0.0))

    # tax_amount
    if "tax_amount" not in df_out.columns or df_out["tax_amount"].isna().all():
        df_out["tax_amount"] = base_price * float(mapping.get("tax_rate", 0.0))

    # packaging_cost
    if "packaging_cost" not in df_out.columns or df_out["packaging_cost"].isna().all():
        df_out["packaging_cost"] = float(mapping.get("packaging_value", 0.0))

    return df_out

# ----------------------------
# Conectores ERP
# ----------------------------

class ERPConnector:
    """Classe base para conectores ERP"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    def connect(self) -> bool:
        """Estabelece conexão com o ERP"""
        raise NotImplementedError
        
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos no período especificado"""
        raise NotImplementedError
        
    def get_products(self) -> pd.DataFrame:
        """Busca dados de produtos"""
        raise NotImplementedError
        
    def get_customers(self) -> pd.DataFrame:
        """Busca dados de clientes"""
        raise NotImplementedError

class TOTVSConnector(ERPConnector):
    """Conector para TOTVS Protheus"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Configurar rate limiter
        rate_config = cliente_config.rate_limits.get('totvs_api', {'max_calls': 100, 'time_window': 60})
        self.rate_limiter = RateLimiter(rate_config['max_calls'], rate_config['time_window'])
        self.query_template = cliente_config.query_template
    
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException, ConnectionError))
    def connect(self) -> bool:
        """Conecta ao TOTVS Protheus via API ou banco"""
        try:
            if 'api_url' in self.config:
                # Conexão via API REST
                self.connection = requests.Session()
                self.connection.headers.update({
                    'Authorization': f"Bearer {self.config['api_token']}",
                    'Content-Type': 'application/json'
                })
                
                # Testar conexão
                test_url = f"{self.config['api_url']}/health"
                response = self.connection.get(test_url, timeout=10)
                response.raise_for_status()
                
                logger.info("Conectado ao TOTVS via API")
                return True
                
            elif 'database_url' in self.config:
                # Conexão direta ao banco
                self.connection = sa.create_engine(
                    self.config['database_url'],
                    pool_pre_ping=True,  # Verificar conexão antes de usar
                    pool_recycle=3600,   # Reciclar conexões a cada hora
                    connect_args={'timeout': 30}  # Timeout de conexão
                )
                
                # Testar conexão
                with self.connection.connect() as conn:
                    conn.execute(sa.text("SELECT 1"))
                
                logger.info("Conectado ao TOTVS via banco de dados")
                return True
                
        except Exception as e:
            logger.error(f"Erro ao conectar TOTVS: {e}")
            return False
            
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do TOTVS"""
        if 'api_url' in self.config:
            return self._get_orders_api(start_date, end_date)
        else:
            return self._get_orders_db(start_date, end_date)
            
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def _get_orders_api(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos via API TOTVS"""
        # Aplicar rate limiting
        self.rate_limiter.wait_if_needed('totvs_orders')
        
        url = f"{self.config['api_url']}/pedidos"
        params = {
            'data_inicio': start_date.strftime('%Y-%m-%d'),
            'data_fim': end_date.strftime('%Y-%m-%d'),
            'status': 'FATURADO,ENTREGUE',
            'limit': 1000  # Limitar registros por página
        }
        
        all_orders = []
        page = 1
        
        while True:
            params['page'] = page
            
            try:
                response = self.connection.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                orders = data.get('pedidos', [])
                
                if not orders:
                    break
                    
                all_orders.extend(orders)
                
                # Verificar se há mais páginas
                if len(orders) < params['limit']:
                    break
                    
                page += 1
                
                # Rate limiting entre páginas
                if page > 1:
                    self.rate_limiter.wait_if_needed('totvs_orders')
                    
            except requests.RequestException as e:
                logger.error(f"Erro na página {page}: {e}")
                if page == 1:  # Se primeira página falhou, re-raise
                    raise
                break  # Se páginas subsequentes falharam, continuar com o que temos
        
        logger.info(f"Coletados {len(all_orders)} pedidos via API TOTVS")
        return pd.DataFrame(all_orders)
        
    @retry_with_backoff(max_retries=2, exceptions=(sa.exc.SQLAlchemyError,))
    def _get_orders_db(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos via banco de dados TOTVS"""
        # Usar template de query configurado
        query_templates = TOTVS_QUERY_TEMPLATES.get(self.query_template, TOTVS_QUERY_TEMPLATES['standard'])
        query = query_templates.get('orders')
        
        if not query:
            logger.error(f"Query template '{self.query_template}' não possui query de pedidos")
            raise ValueError(f"Query de pedidos não encontrada para template '{self.query_template}'")
        
        try:
            df = pd.read_sql(
                query, 
                self.connection,
                params={
                    'start_date': start_date,
                    'end_date': end_date,
                    'filial': self.config.get('filial', '01')
                }
            )
            
            logger.info(f"Coletados {len(df)} pedidos via banco TOTVS (template: {self.query_template})")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao executar query de pedidos: {e}")
            logger.error(f"Query utilizada: {query}")
            raise

class SAPConnector(ERPConnector):
    """Conector para SAP Business One"""
    
    def connect(self) -> bool:
        """Conecta ao SAP Business One"""
        try:
            # SAP Business One Service Layer
            self.connection = requests.Session()
            self.connection.headers.update({
                'Authorization': f"Basic {self.config['auth_token']}",
                'Content-Type': 'application/json'
            })
            logger.info("Conectado ao SAP Business One")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar SAP: {e}")
            return False
            
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do SAP"""
        url = f"{self.config['base_url']}/Orders"
        params = {
            '$filter': f"DocDate ge {start_date.strftime('%Y-%m-%d')} and DocDate le {end_date.strftime('%Y-%m-%d')}",
            '$select': 'DocEntry,CardCode,DocDate,DocTotal,DocStatus'
        }
        
        response = self.connection.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        orders = []
        
        for order in data['value']:
            orders.append({
                'order_id': order['DocEntry'],
                'customer_id': order['CardCode'],
                'order_date': order['DocDate'],
                'total_value': order['DocTotal'],
                'order_status': order['DocStatus']
            })
            
        return pd.DataFrame(orders)

class LinxConnector(ERPConnector):
    """Conector para Linx Commerce"""
    
    def connect(self) -> bool:
        """Conecta ao Linx Commerce"""
        try:
            self.connection = requests.Session()
            self.connection.headers.update({
                'Authorization': f"Bearer {self.config['api_token']}",
                'Content-Type': 'application/json'
            })
            logger.info("Conectado ao Linx Commerce")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Linx: {e}")
            return False
            
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do Linx"""
        url = f"{self.config['base_url']}/api/v1/orders"
        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'status': 'completed,shipped'
        }
        
        response = self.connection.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        return pd.DataFrame(data['orders'])

class MagazordConnector(ERPConnector):
    """Conector para Magazord API v2 (site/pedido). Autenticação Basic."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        rl = cliente_config.rate_limits.get('magazord_api', {'max_calls': 100, 'time_window': 60})
        self.rate_limiter = RateLimiter(rl['max_calls'], rl['time_window'])
        self.session: Optional[requests.Session] = None
        self.base_url: Optional[str] = None
        self.detected_date_field: Optional[str] = None
    
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def connect(self) -> bool:
        """Inicializa sessão HTTP com autenticação Basic (Authorization header)."""
        try:
            self.session = requests.Session()
            self.session.headers.update({'Accept': 'application/json'})
            
            # Resolver URL base
            self.base_url = (
                self.config.get('api_url')
                or os.getenv('MAGAZORD_API_URL')
                or 'https://homologacaodicademadame.painel.magazord.com.br/api/v2'
            )
            
            # Montar Authorization Basic
            basic_token = self.config.get('basic_token') or os.getenv('MAGAZORD_BASIC_TOKEN')
            if not basic_token:
                username = self.config.get('username') or os.getenv('MAGAZORD_USER', '')
                password = self.config.get('password') or os.getenv('MAGAZORD_PASS', '')
                token_bytes = f"{username}:{password}".encode('utf-8')
                basic_token = base64.b64encode(token_bytes).decode('utf-8')
            else:
                # evitar "Basic Basic ..." caso o token já venha com prefixo
                if basic_token.lower().startswith('basic '):
                    basic_token = basic_token.split(' ', 1)[1]
            self.session.headers.update({'Authorization': f'Basic {basic_token}'})
            
            # Testar conexão: buscar uma página
            test_url = f"{self.base_url}/site/pedido"
            resp = self.session.get(test_url, params={'limit': 1, 'page': 1}, timeout=15)
            resp.raise_for_status()
            logger.info("Conectado ao Magazord API")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Magazord: {e}")
            return False
    
    def _detect_date_field(self) -> str:
        """Detecta automaticamente qual campo de data está disponível no endpoint de pedidos."""
        if self.detected_date_field:
            return self.detected_date_field
        if not self.session or not self.base_url:
            self.detected_date_field = "dataHora"
            return self.detected_date_field
        possible_fields = [
            "dataHoraUltimaAlteracaoSituacao",
            "dataHoraUltimaAlteracao",
            "dataAtualizacao",
            "dataAlteracao",
            "dataHora",
        ]
        try:
            self.rate_limiter.wait_if_needed('magazord_detect')
            url = f"{self.base_url}/site/pedido"
            resp = self.session.get(url, params={"limit": 1, "page": 1}, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("data", {}).get("items", [])
            if not items:
                self.detected_date_field = "dataHora"
                return self.detected_date_field
            sample = items[0]
            for f in possible_fields:
                if f in sample and sample[f]:
                    logger.info(f"Campo de data detectado automaticamente: {f}")
                    self.detected_date_field = f
                    return f
        except Exception:
            pass
        logger.warning("Nenhum campo de data detectado. Usando 'dataHora'.")
        self.detected_date_field = "dataHora"
        return self.detected_date_field
    
    def _normalize_order_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Mapeia campos Magazord para colunas canônicas do pipeline."""
        # Marketplace real (quando houver) separado de forma de pagamento / gateway
        marketplace_name = item.get('marketplaceNome') or item.get('lojaDoMarketplaceNome')
        if not marketplace_name:
            marketplace_name = 'Site Próprio'

        return {
            'order_id': item.get('codigo') or str(item.get('id')),
            'customer_id': item.get('pessoaId'),
            'customer_unique_id': item.get('pessoaCpfCnpj') or item.get('pessoaEmail') or '',
            'order_purchase_timestamp': item.get('dataHora'),
            'price': item.get('valorTotal'),
            'valorProduto': item.get('valorProduto'),
            'valorFrete': item.get('valorFrete'),
            'valorDesconto': item.get('valorDesconto'),
            'valorAcrescimo': item.get('valorAcrescimo'),
            'valorTotal': item.get('valorTotal'),
            'valorTotalFinal': item.get('valorTotalFinal'),
            'freight_value': item.get('valorFrete'),
            'discount_value': item.get('valorDesconto'),
            'payment_type': item.get('formaPagamentoNome'),
            'payment_gateway': item.get('formaRecebimentoNome'),
            'marketplace': marketplace_name,
            'order_status': item.get('pedidoSituacaoDescricao'),
            'order_status_code': item.get('pedidoSituacao'),
            # extras úteis
            'marketplace_date': item.get('dataHora'),
        }
    
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def get_orders(self, start_date: datetime, end_date: datetime, parallel: bool = False) -> pd.DataFrame:
        """Coleta pedidos paginados e retorna DataFrame normalizado."""
        if not self.session or not self.base_url:
            raise RuntimeError("MagazordConnector não conectado. Chame connect() antes.")
        
        url = f"{self.base_url}/site/pedido"
        page = 1
        limit = 100
        all_rows: List[Dict[str, Any]] = []
        # Estratégia de filtro: auto-detectar campo de data, usar apenas [gte] e filtrar preciso no Python.
        date_field = (
            self.config.get('date_field')
            or os.getenv('MAGAZORD_DATE_FIELD')
            or self._detect_date_field()
        )
        base_params = {
            'limit': limit,
            'orderDirection': 'desc',
            f'{date_field}[gte]': start_date.strftime('%Y-%m-%d'),
        }
        tried_alternate_field = False
        
        while True:
            self.rate_limiter.wait_if_needed('magazord_orders')
            params = dict(base_params, page=page)
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                data = payload.get('data', {})
                items = data.get('items', []) if isinstance(data, dict) else []
                if not items:
                    break
                
                # Normalizar e preservar timestamp de referência para filtro preciso
                for it in items:
                    row = self._normalize_order_item(it)
                    # Guardar timestamp bruto usado para filtro (magazord_timestamp)
                    ts_raw = it.get(date_field) or it.get('dataHora')
                    row['magazord_timestamp'] = ts_raw
                    all_rows.append(row)
                
                has_more = bool(data.get('has_more')) if isinstance(data, dict) else False
                total_pages = data.get('total_pages') if isinstance(data, dict) else None
                
                if not has_more or (total_pages and page >= int(total_pages)):
                    break
                page += 1
            except requests.RequestException as e:
                logger.error(f"Erro Magazord na página {page}: {e}")
                # Fallback: alguns ambientes rejeitam dataHoraUltimaAlteracaoSituacao; tentar 'dataHora' com o mesmo formato YYYY-MM-DD
                resp_obj = getattr(e, 'response', None)
                status_code = getattr(resp_obj, 'status_code', None)
                if page == 1 and status_code == 400 and not tried_alternate_field and date_field != 'dataHora':
                    tried_alternate_field = True
                    date_field = 'dataHora'
                    # Reconstroi base_params com o campo alternativo
                    base_params = {
                        'limit': limit,
                        'orderDirection': 'desc',
                        f'{date_field}[gte]': start_date.strftime('%Y-%m-%d'),
                    }
                    # reiniciar leitura a partir da página 1
                    page = 1
                    continue
                if page == 1:
                    raise
                break
        
        
        # Fallbacks quando a API retorna vazio mesmo com [gte]
        if not all_rows:
            # 1) tentar com campo 'dataHora' se ainda não usamos
            if date_field != 'dataHora':
                try:
                    page = 1
                    alt_field = 'dataHora'
                    alt_params = {
                        'limit': limit,
                        'orderDirection': 'desc',
                        f'{alt_field}[gte]': start_date.strftime('%Y-%m-%d'),
                    }
                    while True:
                        self.rate_limiter.wait_if_needed('magazord_orders')
                        params = dict(alt_params, page=page)
                        resp = self.session.get(url, params=params, timeout=30)
                        resp.raise_for_status()
                        payload = resp.json()
                        data = payload.get('data', {})
                        items = data.get('items', []) if isinstance(data, dict) else []
                        if not items:
                            break
                        for it in items:
                            row = self._normalize_order_item(it)
                            ts_raw = it.get(alt_field) or it.get('dataHora')
                            row['magazord_timestamp'] = ts_raw
                            all_rows.append(row)
                        has_more = bool(data.get('has_more')) if isinstance(data, dict) else False
                        total_pages = data.get('total_pages') if isinstance(data, dict) else None
                        if not has_more or (total_pages and page >= int(total_pages)):
                            break
                        page += 1
                except Exception as _e:
                    logger.warning(f"Fallback dataHora falhou: {_e}")
        
        if not all_rows:
            # 2) fallback por dia usando 'dataHora=YYYY-MM-DD' (sem range)
            try:
                current_day = pd.to_datetime(start_date).normalize()
                last_day = pd.to_datetime(end_date).normalize()
                while current_day <= last_day:
                    page = 1
                    day_params = {
                        'limit': limit,
                        'orderDirection': 'desc',
                        'dataHora': current_day.strftime('%Y-%m-%d'),
                    }
                    while True:
                        self.rate_limiter.wait_if_needed('magazord_orders')
                        params = dict(day_params, page=page)
                        resp = self.session.get(url, params=params, timeout=30)
                        resp.raise_for_status()
                        payload = resp.json()
                        data = payload.get('data', {})
                        items = data.get('items', []) if isinstance(data, dict) else []
                        if not items:
                            break
                        for it in items:
                            row = self._normalize_order_item(it)
                            ts_raw = it.get('dataHora') or it.get(date_field)
                            row['magazord_timestamp'] = ts_raw
                            all_rows.append(row)
                        has_more = bool(data.get('has_more')) if isinstance(data, dict) else False
                        if not has_more:
                            break
                        page += 1
                    current_day += pd.Timedelta(days=1)
            except Exception as _e:
                logger.warning(f"Fallback por dia falhou: {_e}")

        if not all_rows:
            # 3) último fallback: sem filtro de data no servidor, limitar páginas e filtrar no cliente
            try:
                max_pages_fallback = int(os.getenv('MAGAZORD_FALLBACK_MAX_PAGES', '5'))
                page = 1
                base_no_filter = {
                    'limit': limit,
                    'orderDirection': 'desc',
                }
                while page <= max_pages_fallback:
                    self.rate_limiter.wait_if_needed('magazord_orders')
                    params = dict(base_no_filter, page=page)
                    resp = self.session.get(url, params=params, timeout=30)
                    resp.raise_for_status()
                    payload = resp.json()
                    data = payload.get('data', {})
                    items = data.get('items', []) if isinstance(data, dict) else []
                    if not items:
                        break
                    for it in items:
                        row = self._normalize_order_item(it)
                        ts_raw = it.get(date_field) or it.get('dataHora')
                        row['magazord_timestamp'] = ts_raw
                        all_rows.append(row)
                    has_more = bool(data.get('has_more')) if isinstance(data, dict) else False
                    if not has_more:
                        break
                    page += 1
            except Exception as _e:
                logger.warning(f"Fallback sem filtro falhou: {_e}")

        # Filtragem final precisa pela janela de datas (sem confiar na ordenação da API)
        if not all_rows:
            return pd.DataFrame()
        
        # Comparação consciente de timezone: converter tudo para timestamp aware ou naive consistentemente
        start_ts = pd.to_datetime(start_date, utc=True).tz_localize(None)
        end_ts = pd.to_datetime(end_date, utc=True).tz_localize(None)
        
        filtered_rows: List[Dict[str, Any]] = []
        
        # Otimização: Coletar detalhes apenas dos pedidos que passaram no filtro de data
        orders_to_fetch_details = []
        
        for r in all_rows:
            ts_raw = r.get('magazord_timestamp') or r.get('order_purchase_timestamp')
            ts_parsed = pd.to_datetime(ts_raw, errors='coerce', utc=True)
            if pd.notna(ts_parsed):
                ts_parsed_naive = ts_parsed.tz_localize(None)
                if start_ts <= ts_parsed_naive <= end_ts:
                    orders_to_fetch_details.append(r)
        
        logger.info(f"Buscando detalhes de itens para {len(orders_to_fetch_details)} pedidos... (Parallel={parallel})")
        
        detailed_rows = []

        def _fetch_single_order_details(order_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            """Função auxiliar para buscar detalhes de um único pedido (usada em loop ou threads)"""
            order_id = order_data.get('order_id') or order_data.get('codigo')
            
            # Rate limit check (thread-safe)
            # No modo paralelo, isso pode bloquear a thread até liberar o slot
            self.rate_limiter.wait_if_needed('magazord_details')
            
            result_rows = []
            try:
                detail_url = f"{self.base_url}/site/pedido/{order_id}"
                resp = self.session.get(detail_url, timeout=15)
                
                # Tratamento especial para 429 dentro da thread
                if resp.status_code == 429:
                    logger.warning(f"⚠️ 429 no pedido {order_id}. Dormindo 30s...")
                    time.sleep(30)
                    # Tentar mais uma vez
                    self.rate_limiter.wait_if_needed('magazord_details')
                    resp = self.session.get(detail_url, timeout=15)

                if resp.status_code == 200:
                    payload = resp.json()
                    data = payload.get('data', {})
                    
                    # Geo do cliente
                    estado_sigla = data.get('estadoSigla')
                    cidade_nome = data.get('cidadeNome')
                    
                    # Data de entrega efetiva
                    delivery_ts = None
                    try:
                        hist = data.get('pedidoHistorico', [])
                        for h in hist:
                            situ = h.get('pedidoSituacao')
                            desc = str(h.get('pedidoSituacaoDescricaoDetalhada') or '').lower()
                            if situ == 8 or 'entreg' in desc:
                                delivery_ts = h.get('dataHora')
                    except Exception:
                        delivery_ts = None
                    
                    # Extrair itens
                    rastreios = data.get('arrayPedidoRastreio', [])
                    items_found = False
                    
                    for rastreio in rastreios:
                        for item in rastreio.get('pedidoItem', []):
                            item_row = order_data.copy()
                            item_value_net = item.get('valorItem', 0.0)
                            item_qty = item.get('quantidade', 1)
                            price_unit_net = float(item_value_net / item_qty) if item_qty > 0 else 0.0
                            
                            item_row.update({
                                'product_id': str(item.get('produtoDerivacaoCodigo') or item.get('produtoId')),
                                'product_sku': str(item.get('produtoDerivacaoCodigo') or ''),
                                'product_name': item.get('produtoNome') or item.get('descricao'),
                                'product_qty': item_qty,
                                'price': price_unit_net,
                                'price_gross': item.get('valorUnitario', 0.0),
                                'total_item_value': item_value_net,
                                'freight_value': item.get('valorFrete', 0.0),
                                'discount_value': item.get('valorDesconto', 0.0),
                                # Preservar total do pedido (valorTotal) para KPIs
                                'valorProduto': data.get('valorProduto', item_row.get('valorProduto')),
                                'valorFrete': data.get('valorFrete', item_row.get('valorFrete')),
                                'valorDesconto': data.get('valorDesconto', item_row.get('valorDesconto')),
                                'valorAcrescimo': data.get('valorAcrescimo', item_row.get('valorAcrescimo')),
                                'valorTotal': data.get('valorTotal', item_row.get('valorTotal')),
                                'valorTotalFinal': data.get('valorTotalFinal', item_row.get('valorTotalFinal')),
                                'category_name': item.get('categoria'),
                                'brand': item.get('marcaNome'),
                                'ean': item.get('ean'),
                                'carrier_name': rastreio.get('transportadoraNome'),
                                'customer_state': estado_sigla,
                                'customer_city': cidade_nome,
                                'order_delivered_customer_date': delivery_ts,
                            })
                            result_rows.append(item_row)
                            items_found = True
                    
                    if not items_found:
                        order_with_geo = order_data.copy()
                        order_with_geo['carrier_name'] = None
                        order_with_geo['customer_state'] = estado_sigla
                        order_with_geo['customer_city'] = cidade_nome
                        order_with_geo['order_delivered_customer_date'] = delivery_ts
                        result_rows.append(order_with_geo)
                else:
                    logger.warning(f"Erro ao buscar detalhes do pedido {order_id}: {resp.status_code}")
                    result_rows.append(order_data)
                    
            except Exception as e:
                logger.error(f"Falha ao buscar detalhes pedido {order_id}: {e}")
                result_rows.append(order_data)
                
            return result_rows

        if parallel and len(orders_to_fetch_details) > 5:
            # Modo Paralelo
            import concurrent.futures
            # Limitar threads conservadoramente (ex: 5-8 workers) para não sobrecarregar
            # O rate limiter ainda vai ditar o ritmo global (ex: 100/min), mas as threads
            # ajudam a esconder a latência de cada request.
            max_workers = int(os.getenv('MAGAZORD_THREADS', '6'))
            logger.info(f"Iniciando ThreadPoolExecutor com {max_workers} workers...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_fetch_single_order_details, order): order for order in orders_to_fetch_details}
                
                completed_count = 0
                total_count = len(orders_to_fetch_details)
                
                for future in concurrent.futures.as_completed(futures):
                    completed_count += 1
                    if completed_count % 50 == 0:
                         logger.info(f"Progresso: {completed_count}/{total_count} pedidos processados...")
                    
                    try:
                        rows = future.result()
                        detailed_rows.extend(rows)
                    except Exception as exc:
                        logger.error(f"Worker exception: {exc}")
        else:
            # Modo Sequencial (Legado)
            for i, order in enumerate(orders_to_fetch_details):
                if i % 10 == 0:
                    logger.info(f"Processando detalhe {i+1}/{len(orders_to_fetch_details)}...")
                
                # Pequena pausa apenas para não ser muito agressivo no loop sequencial
                # (O rate limiter dentro da função auxiliar cuidará do resto)
                # time.sleep(0.1) 
                
                rows = _fetch_single_order_details(order)
                detailed_rows.extend(rows)

        df = pd.DataFrame(detailed_rows)
        logger.info(f"Coletados {len(df)} itens de pedidos via Magazord (após expansão)")
        return df

    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def get_carts(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Busca carrinhos abandonados no endpoint /site/carrinho.
        """
        if not self.session or not self.base_url:
            raise RuntimeError("MagazordConnector não conectado. Chame connect() antes.")
            
        url = f"{self.base_url}/site/carrinho"
        page = 1
        limit = 100
        all_carts = []
        
        # Formatar datas
        start_str = start_date.strftime('%Y-%m-%d')
        
        # Parâmetros base (filtrando por data de atualização >= start_date)
        base_params = {
            'limit': limit,
            'dataAtualizacao[gte]': start_str,
            'status': 0, # 0 geralmente indica carrinho aberto/abandonado
        }
        
        while True:
            self.rate_limiter.wait_if_needed('magazord_carts')
            params = dict(base_params, page=page)
            
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                data = payload.get('data', {})
                items = data.get('items', []) if isinstance(data, dict) else []
                
                if not items:
                    break
                    
                for item in items:
                    # Extrair dados relevantes do carrinho
                    all_carts.append({
                        'cart_id': str(item.get('id')),
                        'created_at': item.get('dataInicio'),
                        'updated_at': item.get('dataAtualizacao'),
                        'customer_id': str(item.get('pedido', {}).get('id') or ''),
                        'total_value': 0.0, # Carrinhos geralmente não trazem total no header da lista
                        'status': 'abandoned',
                        'carrinho_abandonado': 1
                    })
                
                has_more = bool(data.get('has_more')) if isinstance(data, dict) else False
                total_pages = data.get('total_pages') if isinstance(data, dict) else None
                
                if not has_more or (total_pages and page >= int(total_pages)):
                    break
                page += 1
                
            except Exception as e:
                logger.error(f"Erro ao buscar carrinhos página {page}: {e}")
                break
                
        df = pd.DataFrame(all_carts)
        logger.info(f"Coletados {len(df)} carrinhos abandonados")
        return df

    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def get_products(self) -> pd.DataFrame:
        """
        Busca produtos e estoque.
        Usa endpoint V1 (/listEstoque) pois a V2 (/site/produto) retorna 403 Forbidden.
        """
        if not self.session or not self.base_url:
            raise RuntimeError("MagazordConnector não conectado. Chame connect() antes.")

        # Forçar uso da V1 para estoque, pois V2 é bloqueada
        base_v1 = self.base_url.replace('/v2', '/v1')
        url = f"{base_v1}/listEstoque"
        
        logger.info(f"Buscando produtos/estoque (V1) em: {url}")
        
        all_products = []
        
        try:
            self.rate_limiter.wait_if_needed('magazord_products')
            # Endpoint V1 /listEstoque retorna array direto em 'data' e não parece paginar da mesma forma
            # Vamos tentar uma única chamada (geralmente traz tudo ou tem limite alto)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            
            items = payload.get('data', [])
            if isinstance(items, list):
                for item in items:
                    # Mapear campos do endpoint listEstoque V1
                    # Ex: {"produto": "9999-A", "descricaoProduto": "...", "quantidadeDisponivelVenda": 5}
                    all_products.append({
                        'product_id': str(item.get('produto') or ''), # SKU
                        'product_sku': str(item.get('produto') or ''),
                        'product_name': item.get('descricaoProduto'),
                        'price': 0.0, # V1 estoque não traz preço de venda, apenas custo
                        'cost_price': item.get('custoMedio') or item.get('custoVirtual') or 0.0,
                        'stock_level': item.get('quantidadeDisponivelVenda') or item.get('quantidadeFisica') or 0,
                        'last_update': item.get('dataHoraAtualizacao'),
                        'active': item.get('ativo', True)
                    })
            
            logger.info(f"Coletados {len(all_products)} produtos via Magazord (V1)")
            
        except Exception as e:
            logger.error(f"Erro ao buscar estoque V1: {e}")
            
        return pd.DataFrame(all_products)

    def get_stock(self) -> pd.DataFrame:
        """Retorna DataFrame simplificado com níveis de estoque."""
        df = self.get_products()
        if df.empty:
            return pd.DataFrame(columns=['product_id', 'stock_level'])
        return df[['product_id', 'stock_level', 'product_name', 'product_sku']]

    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def get_stock_movements(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 100,
        *,
        origins: Optional[List[int]] = None,
        origin: Optional[int] = None,
        deposito: Optional[int] = None,
        produto: Optional[str] = None,
        ignore_movimentacao_webservice: Optional[bool] = None,
        order: str = "id",
    ) -> pd.DataFrame:
        """
        Busca movimentações de estoque (V1).
        Payload exemplo:
        {
          "movimentacao": 0, "deposito": 0, "produto": "string", "valorMovimentacao": null,
          "dataHoraMovimentacao": "...", "quantidade": 0, "tipoOperacao": null, "tipo": 0,
          "origem": 0, "pedidoId": 0, "pedidoCodigo": 0, ...
        }
        """
        if not self.session or not self.base_url:
            raise RuntimeError("MagazordConnector não conectado. Chame connect() antes.")

        # V1 endpoint
        base_v1 = self.base_url.replace('/v2', '/v1')
        url = f"{base_v1}/listMovimentacaoEstoque"
        
        # Formatar datas para ATOM/ISO
        # API V1 geralmente espera YYYY-MM-DDTHH:mm:ss com timezone ou sem
        # Para Magazord V1, o formato aceito é YYYY-MM-DDTHH:mm:ss-03:00 (ATOM)
        start_str = start_date.strftime('%Y-%m-%dT00:00:00-03:00')
        end_str = end_date.strftime('%Y-%m-%dT23:59:59-03:00')
        
        def _fetch_for_origin(origin_filter: Optional[int]) -> List[Dict[str, Any]]:
            offset = 0
            all_movements: List[Dict[str, Any]] = []
            origin_txt = f" | origem={origin_filter}" if origin_filter is not None else ""
            logger.info(f"Buscando movimentações de {start_str} a {end_str}{origin_txt}")

            while True:
                self.rate_limiter.wait_if_needed('magazord_stock_movements')
                params: Dict[str, Any] = {
                    "limit": limit,
                    "offset": offset,
                    "order": order,
                    "dataHoraMovimentacaoInicial": start_str,
                    "dataHoraMovimentacaoFinal": end_str,
                }
                if origin_filter is not None:
                    params["origem"] = int(origin_filter)
                if deposito is not None:
                    params["deposito"] = int(deposito)
                if produto is not None:
                    params["produto"] = str(produto)
                if ignore_movimentacao_webservice is not None:
                    params["ignoreMovimentacaoWebService"] = bool(ignore_movimentacao_webservice)

                # Log de progresso a cada X páginas
                if (offset // limit) % 5 == 0:
                    logger.info(f"⏳ Coletando offset {offset} (Já coletados: {len(all_movements)}){origin_txt}...")

                try:
                    resp = self.session.get(url, params=params, timeout=60)
                    resp.raise_for_status()
                    payload = resp.json()

                    items = payload.get("data", [])
                    if not items:
                        logger.info(f"Fim da paginação no offset {offset}{origin_txt}.")
                        break

                    for item in items:
                        all_movements.append({
                            "movement_id": str(item.get("movimentacao")),
                            "deposito": item.get("deposito"),
                            "product_id": str(item.get("produto")),
                            "movement_value": item.get("valorMovimentacao"),
                            "date": item.get("dataHoraMovimentacao"),
                            "created_at": item.get("dataHoraInclusao"),
                            "qty": item.get("quantidade"),
                            "operation_type": item.get("tipoOperacao"),
                            "type": item.get("tipo"),
                            "origin": item.get("origem"),
                            "order_id": str(item.get("pedidoId") or ""),
                            "order_code": str(item.get("pedidoCodigo") or ""),
                            "invoice_id": str(item.get("notaFiscalId") or ""),
                            "invoice_number": str(item.get("notaFiscalNumero") or ""),
                            "observation": item.get("observacao"),
                            "serial_number": item.get("numero_serie") or item.get("numeroSerie"),
                        })

                    total = payload.get("total", 0)

                    # Log final se total disponível
                    if total > 0 and len(all_movements) >= total:
                        logger.info(f"Total atingido: {len(all_movements)}/{total}{origin_txt}")
                        break

                    # Verificar se há dados fora do range de data solicitado
                    # Se sim, pode ser que a API esteja retornando dados fora do filtro
                    if items:
                        sample_date = items[0].get("dataHoraMovimentacao")
                        if sample_date:
                            try:
                                from datetime import datetime as dt
                                item_date = pd.to_datetime(sample_date)
                                if item_date < pd.to_datetime(start_str) or item_date > pd.to_datetime(end_str):
                                    logger.warning(f"⚠️ API retornou data fora do range: {item_date} (range: {start_str} -> {end_str})")
                            except:
                                pass

                    # Alguns endpoints V1 retornam total, outros não.
                    # Se items vier vazio ou menor que limit, paramos.
                    if len(items) < limit:
                        logger.info(f"Página incompleta ({len(items)} < {limit}), fim da coleta{origin_txt}.")
                        # Verificar se realmente não há mais dados ou se é um limite da API
                        if len(all_movements) > 0:
                            last_date = max([pd.to_datetime(m.get("date", ""), errors="coerce") for m in all_movements if m.get("date")], default=None)
                            if last_date and pd.notna(last_date):
                                logger.info(f"Última data coletada: {last_date} (range solicitado: {start_str} -> {end_str})")
                                if last_date < pd.to_datetime(end_str):
                                    logger.warning(f"⚠️ Coleta parou antes do fim do range! Última data: {last_date}, Fim do range: {end_str}")
                        break

                    offset += limit

                except requests.HTTPError as e:
                    # Tratamento especial para 429 - propagar para o decorator @retry_with_backoff
                    if e.response is not None and e.response.status_code == 429:
                        logger.warning(f"⚠️ 429 Too Many Requests (offset={offset}){origin_txt}. Propagando para retry...")
                        raise  # Propaga para o decorator @retry_with_backoff tratar
                    logger.error(f"Erro HTTP ao buscar movimentações (offset={offset}){origin_txt}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Erro ao buscar movimentações (offset={offset}){origin_txt}: {e}")
                    break

            return all_movements

        # Compatibilidade:
        # - `origin` permite um único filtro
        # - `origins` permite múltiplas origens (faz várias coletas e concatena)
        origin_list: Optional[List[int]] = None
        if origins is not None:
            origin_list = [int(o) for o in origins if o is not None]
        elif origin is not None:
            origin_list = [int(origin)]

        if origin_list:
            all_rows: List[Dict[str, Any]] = []
            for o in origin_list:
                all_rows.extend(_fetch_for_origin(o))
            return pd.DataFrame(all_rows)

        return pd.DataFrame(_fetch_for_origin(None))


class VTEXConnector(ERPConnector):
    """
    Conector para VTEX (OMS + Logistics + Reviews).

    Objetivo: gerar um DataFrame de "orders_enriched" (1 linha por item) compatível com o pipeline
    atual do Insight Expert (mesmas chaves e campos principais do MagazordConnector).

    Config esperada (via erp_config ou variáveis de ambiente):
      - VTEX_ACCOUNT_NAME / account_name
      - VTEX_ENVIRONMENT / environment (default: vtexcommercestable)
      - VTEX_DOMAIN / domain (default: com.br)
      - VTEX_APP_KEY / app_key
      - VTEX_APP_TOKEN / app_token
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        rl = cliente_config.rate_limits.get('vtex_api', {'max_calls': 120, 'time_window': 60})
        self.rate_limiter = RateLimiter(rl['max_calls'], rl['time_window'])
        self.session: Optional[requests.Session] = None
        self.base_url: Optional[str] = None
        self.reviews_base_url: Optional[str] = None

    def _cfg(self, key: str, env: str, default: Optional[str] = None) -> Optional[str]:
        v = self.config.get(key) if isinstance(self.config, dict) else None
        if v is None or str(v).strip() == "":
            v = os.getenv(env, default)
        return None if v is None else str(v).strip()

    def _build_urls(self) -> None:
        base_override = self._cfg("base_url", "VTEX_BASE_URL")
        if base_override:
            self.base_url = base_override.rstrip("/")
        else:
            account = self._cfg("account_name", "VTEX_ACCOUNT_NAME")
            env_name = self._cfg("environment", "VTEX_ENVIRONMENT", "vtexcommercestable")
            domain = self._cfg("domain", "VTEX_DOMAIN", "com.br")
            if not account:
                raise RuntimeError("VTEX_ACCOUNT_NAME/account_name é obrigatório.")
            self.base_url = f"https://{account}.{env_name}.{domain}".rstrip("/")

        reviews_override = self._cfg("reviews_base_url", "VTEX_REVIEWS_BASE_URL")
        if reviews_override:
            self.reviews_base_url = reviews_override.rstrip("/")
        else:
            account = self._cfg("account_name", "VTEX_ACCOUNT_NAME")
            if not account:
                raise RuntimeError("VTEX_ACCOUNT_NAME/account_name é obrigatório.")
            self.reviews_base_url = f"https://{account}.myvtex.com/reviews-and-ratings/api".rstrip("/")

    @staticmethod
    def _cents_to_brl(v: Any) -> float:
        try:
            if v is None:
                return 0.0
            n = float(v)
            # VTEX costuma retornar valores em centavos
            return n / 100.0
        except Exception:
            return 0.0

    @staticmethod
    def _safe_dt(v: Any) -> Optional[str]:
        if v is None:
            return None
        try:
            # manter ISO string (pipeline normaliza depois)
            return str(v)
        except Exception:
            return None

    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def connect(self) -> bool:
        try:
            self._build_urls()
            app_key = self._cfg("app_key", "VTEX_APP_KEY")
            app_token = self._cfg("app_token", "VTEX_APP_TOKEN")
            if not app_key or not app_token:
                raise RuntimeError("VTEX_APP_KEY e VTEX_APP_TOKEN são obrigatórios.")

            self.session = requests.Session()
            self.session.headers.update(
                {
                    "Accept": "application/json",
                    "X-VTEX-API-AppKey": app_key,
                    "X-VTEX-API-AppToken": app_token,
                }
            )

            # Teste simples (OMS list)
            test_url = f"{self.base_url}/api/oms/pvt/orders"
            self.rate_limiter.wait_if_needed("vtex_oms")
            resp = self.session.get(test_url, params={"page": 1, "per_page": 1}, timeout=30)
            if resp.status_code in (401, 403):
                resp.raise_for_status()
            logger.info("Conectado ao VTEX (OMS/Logistics)")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar VTEX: {e}")
            return False

    def _list_orders(self, start_date: datetime, end_date: datetime) -> list[dict]:
        if not self.session or not self.base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")

        url = f"{self.base_url}/api/oms/pvt/orders"
        page = 1
        per_page = int(self._cfg("per_page", "VTEX_ORDERS_PER_PAGE", "50") or 50)
        max_pages_fallback = int(self._cfg("fallback_max_pages", "VTEX_FALLBACK_MAX_PAGES", "10") or 10)

        # Tentar filtro por creationDate (se suportado); se 400, cair no fallback sem filtro.
        start_iso = pd.to_datetime(start_date, utc=True).strftime("%Y-%m-%dT00:00:00.000Z")
        end_iso = pd.to_datetime(end_date, utc=True).strftime("%Y-%m-%dT23:59:59.999Z")
        f_creation = f"creationDate:[{start_iso} TO {end_iso}]"

        all_items: list[dict] = []
        tried_no_filter = False

        while True:
            self.rate_limiter.wait_if_needed("vtex_oms")
            params: Dict[str, Any] = {"page": page, "per_page": per_page}
            if not tried_no_filter:
                params["f_creationDate"] = f_creation
            try:
                resp = self.session.get(url, params=params, timeout=45)
                if resp.status_code == 400 and not tried_no_filter:
                    tried_no_filter = True
                    page = 1
                    continue
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException as e:
                logger.error(f"Erro VTEX ao listar pedidos (page={page}): {e}")
                break

            items = payload.get("list") if isinstance(payload, dict) else None
            if not isinstance(items, list) or not items:
                break
            all_items.extend(items)

            paging = payload.get("paging") if isinstance(payload, dict) else {}
            total_pages = None
            try:
                total_pages = int((paging or {}).get("pages")) if (paging or {}).get("pages") is not None else None
            except Exception:
                total_pages = None

            if total_pages is not None:
                if page >= total_pages:
                    break
            else:
                # fallback: limitar páginas quando não há paging confiável
                if page >= max_pages_fallback:
                    break
            page += 1

        # Filtro final no cliente (para o fallback sem filtro)
        start_ts = pd.to_datetime(start_date, utc=True)
        end_ts = pd.to_datetime(end_date, utc=True)
        filtered: list[dict] = []
        for it in all_items:
            dt_raw = it.get("creationDate") or it.get("authorizedDate") or it.get("lastChange")
            ts = pd.to_datetime(dt_raw, errors="coerce", utc=True)
            if pd.notna(ts) and start_ts <= ts <= end_ts:
                filtered.append(it)
        return filtered

    def _get_order_detail(self, order_id: str) -> Optional[dict]:
        if not self.session or not self.base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")
        url = f"{self.base_url}/api/oms/pvt/orders/{order_id}"
        self.rate_limiter.wait_if_needed("vtex_oms_details")
        resp = self.session.get(url, timeout=45)
        if resp.status_code == 429:
            raise requests.RequestException("429 Too Many Requests", response=resp)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def _normalize_order_item(self, order_list_item: Dict[str, Any]) -> Dict[str, Any]:
        origin = order_list_item.get("origin") or ""
        affiliate = order_list_item.get("affiliateId") or ""
        marketplace = origin if not affiliate else f"{origin}:{affiliate}"

        return {
            "order_id": order_list_item.get("orderId") or "",
            "order_purchase_timestamp": order_list_item.get("creationDate"),
            "marketplace_date": order_list_item.get("creationDate"),
            "marketplace": marketplace or "VTEX",
            "order_status": order_list_item.get("statusDescription") or order_list_item.get("status") or "",
            "order_status_code": order_list_item.get("status") or "",
            "payment_type": order_list_item.get("paymentNames") or "",
        }

    def get_orders(self, start_date: datetime, end_date: datetime, parallel: bool = False) -> pd.DataFrame:
        """
        Retorna DataFrame item-level (equivalente ao MagazordConnector.get_orders()).
        Cada linha representa um item do pedido com:
          - product_qty
          - price (unitário líquido)
          - ean/brand/category quando disponíveis
          - valores totais do pedido (valorTotal etc.) repetidos por item (compatibilidade)
        """
        if not self.session or not self.base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")

        orders = self._list_orders(start_date, end_date)
        if not orders:
            return pd.DataFrame()

        logger.info(f"Buscando detalhes de itens para {len(orders)} pedidos VTEX... (Parallel={parallel})")

        detailed_rows: list[dict] = []

        def _extract_rows(order_list_item: Dict[str, Any]) -> list[dict]:
            base = self._normalize_order_item(order_list_item)
            order_id = str(base.get("order_id") or "").strip()
            if not order_id:
                return []
            detail = self._get_order_detail(order_id)
            if not isinstance(detail, dict):
                return [base]

            # Totais do pedido (centavos -> BRL)
            totals = {t.get("id"): t.get("value") for t in (detail.get("totals") or []) if isinstance(t, dict)}
            valor_produto = self._cents_to_brl(totals.get("Items"))
            valor_frete = self._cents_to_brl(totals.get("Shipping"))
            # Discounts pode vir negativo
            valor_desconto = abs(self._cents_to_brl(totals.get("Discounts")))
            valor_tax = self._cents_to_brl(totals.get("Tax"))
            valor_total = self._cents_to_brl(detail.get("value")) or (valor_produto + valor_frete + valor_tax - valor_desconto)

            # Geo / shipping
            ship = detail.get("shippingData") or {}
            address = (ship.get("address") or {}) if isinstance(ship, dict) else {}
            customer_state = address.get("state")
            customer_city = address.get("city")
            carrier_name = None
            try:
                li = (ship.get("logisticsInfo") or [])
                if isinstance(li, list) and li:
                    carrier_name = (li[0] or {}).get("deliveryCompany")
            except Exception:
                carrier_name = None

            # Customer unique id
            client_profile = detail.get("clientProfileData") or {}
            customer_unique_id = (
                (client_profile.get("email") or "").strip()
                or (client_profile.get("document") or "").strip()
                or ""
            )

            # Delivery date (best-effort)
            delivered_ts = None
            try:
                packages = (((detail.get("packageAttachment") or {}).get("packages")) or [])
                if isinstance(packages, list) and packages:
                    courier_status = (packages[0] or {}).get("courierStatus") or {}
                    delivered_ts = courier_status.get("deliveredDate")
            except Exception:
                delivered_ts = None

            rows: list[dict] = []
            items = detail.get("items") or []
            if not isinstance(items, list) or not items:
                base2 = base.copy()
                base2.update(
                    {
                        "customer_unique_id": customer_unique_id,
                        "customer_state": customer_state,
                        "customer_city": customer_city,
                        "carrier_name": carrier_name,
                        "order_delivered_customer_date": delivered_ts,
                        "valorProduto": valor_produto,
                        "valorFrete": valor_frete,
                        "valorDesconto": valor_desconto,
                        "valorTotal": valor_total,
                        "valorTotalFinal": valor_total,
                    }
                )
                return [base2]

            for item in items:
                if not isinstance(item, dict):
                    continue
                qty = int(pd.to_numeric(item.get("quantity", 1), errors="coerce") or 1)

                selling_price = item.get("sellingPrice")
                list_price = item.get("listPrice")
                unit_price = self._cents_to_brl(selling_price)
                unit_price_gross = self._cents_to_brl(list_price)

                product_id = str(item.get("sellerSku") or item.get("id") or "").strip()
                product_sku = str(item.get("sellerSku") or item.get("id") or "").strip()

                add_info = item.get("additionalInfo") or {}
                brand = add_info.get("brandName") if isinstance(add_info, dict) else None
                ean = item.get("ean")

                row = base.copy()
                row.update(
                    {
                        "customer_unique_id": customer_unique_id,
                        "customer_state": customer_state,
                        "customer_city": customer_city,
                        "carrier_name": carrier_name,
                        "order_delivered_customer_date": delivered_ts,
                        "product_id": product_id,
                        "product_sku": product_sku,
                        "product_name": item.get("name"),
                        "product_qty": qty,
                        "price": unit_price,
                        "price_gross": unit_price_gross,
                        "total_item_value": unit_price * qty,
                        "freight_value": 0.0,
                        "discount_value": 0.0,
                        "valorProduto": valor_produto,
                        "valorFrete": valor_frete,
                        "valorDesconto": valor_desconto,
                        "valorTotal": valor_total,
                        "valorTotalFinal": valor_total,
                        "brand": brand,
                        # categoria pode vir do catalog (não garantido no OMS payload)
                        "category_name": None,
                    }
                )
                rows.append(row)

            return rows

        if parallel and len(orders) > 20:
            import concurrent.futures

            max_workers = int(os.getenv("VTEX_THREADS", "6"))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(_extract_rows, o) for o in orders]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        detailed_rows.extend(f.result() or [])
                    except Exception as e:
                        logger.error(f"Worker exception (VTEX): {e}")
        else:
            for i, o in enumerate(orders):
                if i % 50 == 0:
                    logger.info(f"Processando detalhe {i+1}/{len(orders)} (VTEX)...")
                try:
                    detailed_rows.extend(_extract_rows(o))
                except Exception as e:
                    logger.error(f"Erro ao buscar detalhes VTEX ({o.get('orderId')}): {e}")
                    detailed_rows.append(self._normalize_order_item(o))

        df = pd.DataFrame(detailed_rows)
        logger.info(f"Coletados {len(df)} itens de pedidos via VTEX (após expansão)")
        return df

    def get_products(self) -> pd.DataFrame:
        """
        No VTEX, o catálogo é separado do OMS/Logistics.
        Por enquanto, este conector suporta 'sku ids' via VTEX_SKU_IDS para bootstrap de estoque.
        """
        sku_ids = (self._cfg("sku_ids", "VTEX_SKU_IDS") or "").strip()
        if not sku_ids:
            return pd.DataFrame()
        ids = [s.strip() for s in sku_ids.split(",") if s.strip()]
        return pd.DataFrame([{"product_id": i, "product_sku": i, "product_name": None} for i in ids])

    def get_customers(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_stock(self) -> pd.DataFrame:
        """
        Snapshot de estoque (Logistics API) baseado em skuIds fornecidos em VTEX_SKU_IDS.
        Retorna DataFrame com colunas compatíveis com o pipeline de estoque simplificado:
          - product_id, stock_level, reserved_quantity, warehouse_id, warehouse_name
        """
        if not self.session or not self.base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")
        df_skus = self.get_products()
        if df_skus.empty:
            return pd.DataFrame()

        out_rows: list[dict] = []
        for sku in df_skus["product_id"].astype(str).tolist():
            url = f"{self.base_url}/api/logistics/pvt/inventory/skus/{sku}"
            self.rate_limiter.wait_if_needed("vtex_logistics")
            resp = self.session.get(url, timeout=45)
            if resp.status_code in (404,):
                continue
            resp.raise_for_status()
            payload = resp.json()
            balances = payload.get("balance") if isinstance(payload, dict) else None
            if not isinstance(balances, list):
                continue
            for b in balances:
                if not isinstance(b, dict):
                    continue
                total_q = pd.to_numeric(b.get("totalQuantity"), errors="coerce")
                reserved_q = pd.to_numeric(b.get("reservedQuantity"), errors="coerce")
                has_unlimited = bool(b.get("hasUnlimitedQuantity"))
                stock_level = None if has_unlimited else float((total_q if pd.notna(total_q) else 0) - (reserved_q if pd.notna(reserved_q) else 0))
                out_rows.append(
                    {
                        "snapshot_date": datetime.utcnow().date().isoformat(),
                        "product_id": str(payload.get("skuId") or sku),
                        "product_sku": str(payload.get("skuId") or sku),
                        "warehouseId": b.get("warehouseId"),
                        "warehouseName": b.get("warehouseName"),
                        "stock_level": stock_level,
                        "reserved_quantity": float(reserved_q) if pd.notna(reserved_q) else 0.0,
                        "has_unlimited_quantity": has_unlimited,
                        "timeToRefill": b.get("timeToRefill"),
                        "dateOfSupplyUtc": b.get("dateOfSupplyUtc"),
                        "leadTime": b.get("leadTime"),
                    }
                )
        return pd.DataFrame(out_rows)

    def get_product_rating(self, product_id: str) -> Optional[dict]:
        if not self.session or not self.reviews_base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")
        url = f"{self.reviews_base_url}/rating/{product_id}"
        self.rate_limiter.wait_if_needed("vtex_reviews")
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def list_reviews(self, *, page: int = 1, page_size: int = 50) -> pd.DataFrame:
        if not self.session or not self.reviews_base_url:
            raise RuntimeError("VTEXConnector não conectado. Chame connect() antes.")
        url = f"{self.reviews_base_url}/reviews"
        self.rate_limiter.wait_if_needed("vtex_reviews")
        resp = self.session.get(url, params={"page": page, "pageSize": page_size}, timeout=45)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        return pd.DataFrame(data or [])

# ----------------------------
# Conectores Marketplace
# ----------------------------

class MarketplaceConnector:
    """Classe base para conectores de marketplace"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    def connect(self) -> bool:
        """Estabelece conexão com o marketplace"""
        raise NotImplementedError
        
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do marketplace"""
        raise NotImplementedError

class MercadoLivreConnector(MarketplaceConnector):
    """Conector para Mercado Livre"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Configurar rate limiter
        rate_config = cliente_config.rate_limits.get('mercado_livre', {'max_calls': 1000, 'time_window': 3600})
        self.rate_limiter = RateLimiter(rate_config['max_calls'], rate_config['time_window'])
    
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def connect(self) -> bool:
        """Conecta ao Mercado Livre"""
        try:
            self.connection = requests.Session()
            self.connection.headers.update({
                'Authorization': f"Bearer {self.config['access_token']}",
                'Content-Type': 'application/json'
            })
            
            # Testar conexão
            test_url = "https://api.mercadolibre.com/users/me"
            response = self.connection.get(test_url, timeout=10)
            response.raise_for_status()
            
            logger.info("Conectado ao Mercado Livre")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Mercado Livre: {e}")
            return False
            
    @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do Mercado Livre"""
        url = "https://api.mercadolibre.com/orders/search"
        params = {
            'seller': self.config['user_id'],
            'order.status': 'paid',
            'offset': 0,
            'limit': 50
        }
        
        all_orders = []
        offset = 0
        max_requests = 100  # Limite de segurança
        request_count = 0
        
        while request_count < max_requests:
            # Aplicar rate limiting
            self.rate_limiter.wait_if_needed('ml_orders')
            
            params['offset'] = offset
            
            try:
                response = self.connection.get(url, params=params, timeout=30)
                response.raise_for_status()
                request_count += 1
                
                data = response.json()
                orders = data.get('results', [])
                
                if not orders:
                    break
                    
                # Filtrar por data
                filtered_orders = []
                for order in orders:
                    try:
                        order_date = datetime.fromisoformat(order['date_created'].replace('Z', '+00:00'))
                        if start_date <= order_date <= end_date:
                            filtered_orders.append({
                                'order_id': f"ML_{order['id']}",
                                'customer_id': order['buyer']['id'],
                                'order_date': order_date,
                                'total_value': order['total_amount'],
                                'marketplace': 'Mercado Livre',
                                'order_status': order['status']
                            })
                    except (KeyError, ValueError) as e:
                        logger.warning(f"Erro ao processar pedido ML: {e}")
                        continue
                        
                all_orders.extend(filtered_orders)
                offset += 50
                
                if len(orders) < 50:  # Última página
                    break
                    
            except requests.RequestException as e:
                logger.error(f"Erro na requisição ML (offset {offset}): {e}")
                if request_count == 1:  # Se primeira requisição falhou, re-raise
                    raise
                break  # Se requisições subsequentes falharam, continuar com o que temos
        
        if request_count >= max_requests:
            logger.warning(f"Limite de {max_requests} requisições atingido para ML")
        
        logger.info(f"Coletados {len(all_orders)} pedidos do Mercado Livre")
        return pd.DataFrame(all_orders)

class AmazonConnector(MarketplaceConnector):
    """Conector para Amazon SP-API"""
    
    def connect(self) -> bool:
        """Conecta ao Amazon SP-API"""
        try:
            # Implementar autenticação OAuth2 para SP-API
            self.connection = requests.Session()
            logger.info("Conectado ao Amazon SP-API")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Amazon: {e}")
            return False
            
    def get_orders(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca pedidos do Amazon"""
        # Implementar busca de pedidos via SP-API
        # Por enquanto, retornar DataFrame vazio
        return pd.DataFrame()

# ----------------------------
# Conector Google Analytics
# ----------------------------

class GA4Connector:
    """Conector para Google Analytics 4"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        
    def connect(self) -> bool:
        """Conecta ao Google Analytics 4"""
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.oauth2.service_account import Credentials
            
            if 'credentials_path' in self.config:
                credentials = Credentials.from_service_account_file(
                    self.config['credentials_path']
                )
                self.client = BetaAnalyticsDataClient(credentials=credentials)
            else:
                self.client = BetaAnalyticsDataClient()
                
            logger.info("Conectado ao Google Analytics 4")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar GA4: {e}")
            return False
            
    def get_ecommerce_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca dados de e-commerce do GA4"""
        from google.analytics.data_v1beta.types import (
            RunReportRequest, Dimension, Metric, DateRange
        )
        
        request = RunReportRequest(
            property=self.config['property_id'],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="deviceCategory"),
                Dimension(name="customEvent:product_category"),
                Dimension(name="customEvent:campaign_name")
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="newUsers"),
                Metric(name="screenPageViews"),
                Metric(name="purchaseRevenue"),
                Metric(name="purchases"),
                Metric(name="addToCarts")
            ],
            date_ranges=[DateRange(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )]
        )
        
        response = self.client.run_report(request=request)
        
        data = []
        for row in response.rows:
            row_data = {}
            
            # Adicionar dimensões
            for i, dimension_value in enumerate(row.dimension_values):
                dimension_name = request.dimensions[i].name
                row_data[dimension_name] = dimension_value.value
                
            # Adicionar métricas
            for i, metric_value in enumerate(row.metric_values):
                metric_name = request.metrics[i].name
                row_data[metric_name] = float(metric_value.value)
                
            data.append(row_data)
            
        return pd.DataFrame(data)

# ----------------------------
# Classe Principal de Integração
# ----------------------------

class ClienteDataConnector:
    """Classe principal para integração de dados da ANJUSS"""
    
    def __init__(self, config: ClienteConfig):
        self.config = config
        self.erp_connector = None
        self.marketplace_connectors = {}
        self.ga4_connector = None
        
    def setup_erp_connection(self) -> bool:
        """Configura conexão com ERP"""
        if not self.config.erp_type or not self.config.erp_config:
            logger.error("Configuração de ERP não definida")
            return False
            
        # Instanciar conector baseado no tipo
        if self.config.erp_type.lower() == 'totvs':
            self.erp_connector = TOTVSConnector(self.config.erp_config)
        elif self.config.erp_type.lower() == 'sap':
            self.erp_connector = SAPConnector(self.config.erp_config)
        elif self.config.erp_type.lower() == 'linx':
            self.erp_connector = LinxConnector(self.config.erp_config)
        elif self.config.erp_type.lower() == 'magazord':
            self.erp_connector = MagazordConnector(self.config.erp_config)
        elif self.config.erp_type.lower() == 'vtex':
            self.erp_connector = VTEXConnector(self.config.erp_config)
        else:
            logger.error(f"Tipo de ERP não suportado: {self.config.erp_type}")
            return False
            
        return self.erp_connector.connect()
        
    def setup_marketplace_connections(self) -> Dict[str, bool]:
        """Configura conexões com marketplaces"""
        results = {}
        
        for marketplace, config in self.config.marketplace_configs.items():
            try:
                if marketplace.lower() == 'mercado_livre':
                    connector = MercadoLivreConnector(config)
                elif marketplace.lower() == 'amazon':
                    connector = AmazonConnector(config)
                else:
                    logger.warning(f"Marketplace não suportado: {marketplace}")
                    continue
                    
                results[marketplace] = connector.connect()
                if results[marketplace]:
                    self.marketplace_connectors[marketplace] = connector
                    
            except Exception as e:
                logger.error(f"Erro ao configurar {marketplace}: {e}")
                results[marketplace] = False
                
        return results
        
    def setup_ga4_connection(self) -> bool:
        """Configura conexão com Google Analytics 4"""
        if not self.config.ga4_config:
            logger.warning("Configuração GA4 não definida")
            return False
            
        self.ga4_connector = GA4Connector(self.config.ga4_config)
        return self.ga4_connector.connect()
        
    def collect_all_data(self, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Coleta dados de todas as fontes configuradas"""
        data = {}
        
        # Coletar dados do ERP
        if self.erp_connector:
            try:
                logger.info("Coletando dados do ERP...")
                # data['erp_orders'] = self.erp_connector.get_orders(start_date, end_date)
                
                df_orders = self.erp_connector.get_orders(start_date, end_date)
                
                # Aplicar mapeamento de margens se configurado
                erp_mapping = (
                    self.config.erp_config.get('margin_mapping')
                    if isinstance(self.config.erp_config, dict) else {}
                )
                if not erp_mapping:
                    erp_mapping = cliente_config.get_margin_mapping('erp')
                    
                data['erp_orders'] = apply_margin_mapping(df_orders, erp_mapping)
                
                logger.info(f"Coletados {len(data['erp_orders'])} pedidos do ERP")
            except Exception as e:
                logger.error(f"Erro ao coletar dados do ERP: {e}")
                import traceback
                logger.error(traceback.format_exc())
                
        # Coletar dados dos marketplaces
        for marketplace, connector in self.marketplace_connectors.items():
            try:
                logger.info(f"Coletando dados do {marketplace}...")
                marketplace_data = connector.get_orders(start_date, end_date)
                
                # Aplicar mapeamento de margens específico do marketplace
                scope = f"marketplace:{marketplace.lower()}"
                mp_cfg = self.config.marketplace_configs.get(marketplace, {}) if isinstance(self.config.marketplace_configs, dict) else {}
                mp_mapping = mp_cfg.get('margin_mapping') if isinstance(mp_cfg, dict) else {}
                if not mp_mapping:
                    mp_mapping = cliente_config.get_margin_mapping(scope)
                
                data[f'{marketplace}_orders'] = apply_margin_mapping(marketplace_data, mp_mapping)
                
                logger.info(f"Coletados {len(data[f'{marketplace}_orders'])} pedidos do {marketplace}")
            except Exception as e:
                logger.error(f"Erro ao coletar dados do {marketplace}: {e}")
                
        # Coletar dados do GA4
        if self.ga4_connector:
            try:
                logger.info("Coletando dados do Google Analytics...")
                data['ga4_data'] = self.ga4_connector.get_ecommerce_data(start_date, end_date)
                logger.info(f"Coletados {len(data['ga4_data'])} registros do GA4")
            except Exception as e:
                logger.error(f"Erro ao coletar dados do GA4: {e}")
                
        return data

# ----------------------------
# Função de Configuração Rápida
# ----------------------------

def setup_cliente_connector(
    erp_type: str,
    erp_config: Dict[str, Any],
    marketplace_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ga4_config: Optional[Dict[str, Any]] = None
) -> ClienteDataConnector:
    """
    Configura rapidamente o conector Cliente
    
    Args:
        erp_type: Tipo do ERP (totvs, sap, linx, magazord, vtex)
        erp_config: Configuração do ERP
        marketplace_configs: Configurações dos marketplaces
        ga4_config: Configuração do Google Analytics
        
    Returns:
        ClienteDataConnector configurado
    """
    # Configurar Cliente
    cliente_config.set_erp_config(erp_type, erp_config)
    
    if marketplace_configs:
        for marketplace, config in marketplace_configs.items():
            cliente_config.set_marketplace_config(marketplace, config)
            
    if ga4_config:
        cliente_config.set_ga4_config(ga4_config)
        
    # Criar conector
    connector = ClienteDataConnector(cliente_config)
    
    # Configurar conexões
    connector.setup_erp_connection()
    connector.setup_marketplace_connections()
    connector.setup_ga4_connection()
    
    return connector

# ----------------------------
# Conector Meta Ads (Facebook/Instagram)
# ----------------------------

class MetaAdsConnector:
    """Conector para Meta Ads (Facebook/Instagram)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    def connect(self) -> bool:
        """Conecta ao Meta Ads API"""
        try:
            self.connection = requests.Session()
            self.connection.headers.update({
                'Authorization': f"Bearer {self.config['access_token']}",
                'Content-Type': 'application/json'
            })
            logger.info("Conectado ao Meta Ads API")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Meta Ads: {e}")
            return False
            
    def get_campaign_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca dados de campanhas do Meta Ads"""
        url = f"https://graph.facebook.com/v18.0/act_{self.config['ad_account_id']}/insights"
        
        params = {
            'time_range': {
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
            },
            'fields': [
                'campaign_name',
                'spend',           # Gasto total - para CAC
                'impressions',     # Impressões
                'clicks',          # Cliques
                'ctr',            # CTR
                'cpc',            # Custo por clique
                'conversions',     # Conversões
                'conversion_values', # Valor das conversões - para LTV
                'cost_per_conversion', # Custo por conversão
                'actions'         # Ações detalhadas
            ],
            'level': 'campaign',
            'breakdowns': ['age', 'gender', 'device_platform'],
            'access_token': self.config['access_token']
        }
        
        response = self.connection.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        campaigns = []
        
        for campaign in data.get('data', []):
            # Processar ações para extrair conversões específicas
            purchase_value = 0
            purchases = 0
            
            if 'actions' in campaign:
                for action in campaign['actions']:
                    if action['action_type'] == 'purchase':
                        purchases = int(action['value'])
                    elif action['action_type'] == 'purchase_value':
                        purchase_value = float(action['value'])
            
            campaigns.append({
                'campaign_name': campaign.get('campaign_name'),
                'date': start_date,  # Simplificado - seria melhor pegar por dia
                'spend': float(campaign.get('spend', 0)),
                'impressions': int(campaign.get('impressions', 0)),
                'clicks': int(campaign.get('clicks', 0)),
                'conversions': int(campaign.get('conversions', 0)),
                'purchases': purchases,
                'purchase_value': purchase_value,
                'cost_per_conversion': float(campaign.get('cost_per_conversion', 0)),
                'marketing_channel': 'Meta Ads',
                'platform': 'Facebook/Instagram'
            })
            
        return pd.DataFrame(campaigns)

class GoogleAdsConnector:
    """Conector para Google Ads"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    def connect(self) -> bool:
        """Conecta ao Google Ads API"""
        try:
            # Implementar autenticação Google Ads API
            logger.info("Conectado ao Google Ads API")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Google Ads: {e}")
            return False
            
    def get_campaign_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Busca dados de campanhas do Google Ads"""
        # Query GAQL (Google Ads Query Language)
        query = f"""
        SELECT 
            campaign.name,
            segments.date,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_per_conversion
        FROM campaign 
        WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' 
        AND '{end_date.strftime('%Y-%m-%d')}'
        """
        
        # Implementar busca via Google Ads API
        # Por enquanto, retornar DataFrame vazio
        return pd.DataFrame()

# ----------------------------
# Exemplo de Uso
# ----------------------------

if __name__ == "__main__":
    """
    Exemplo de configuração e uso do conector Cliente
    """
    
    # Exemplo 1: TOTVS Protheus
    totvs_config = {
        'api_url': 'https://api.anjuss.com.br/totvs',
        'api_token': 'seu_token_aqui',
        'filial': '01'
    }
    
    # Exemplo 2: Mercado Livre
    ml_config = {
        'access_token': 'seu_access_token_ml',
        'user_id': 'seu_user_id_ml'
    }
    
    # Exemplo 3: Google Analytics
    ga4_config = {
        'property_id': 'properties/123456789',
        'credentials_path': 'anjuss-ga4-credentials.json'
    }
    
    # Configurar conector
    connector = setup_cliente_connector(
        erp_type='totvs',
        erp_config=totvs_config,
        marketplace_configs={'mercado_livre': ml_config},
        ga4_config=ga4_config
    )
    
    # Coletar dados dos últimos 30 dias
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    data = connector.collect_all_data(start_date, end_date)
    
    print("Dados coletados:")
    for source, df in data.items():
        print(f"- {source}: {len(df)} registros")
