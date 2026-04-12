"""
Sistema de Ensemble ML para Previsões de Estoque
===============================================

Implementa ensemble inteligente usando XGBoost e LightGBM para previsões
de demanda por categoria, otimizado para recomendações de estoque.

Características:
- Ensemble híbrido XGBoost + LightGBM
- Validação temporal robusta
- Features de engenharia avançada
- Otimização automática de hiperparâmetros
- Cache inteligente para performance
"""

import pandas as pd
import numpy as np
import sys
import os
import time
import io
import contextlib
from typing import Dict, List, Tuple, Any, Optional
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
import warnings
warnings.filterwarnings('ignore')

# Silenciar LightGBM completamente
import os
os.environ['LIGHTGBM_VERBOSE'] = '0'
os.environ['LIGHTGBM_LOG_LEVEL'] = 'FATAL'

# Silenciar warnings específicos do LightGBM
warnings.filterwarnings('ignore', category=UserWarning, module='lightgbm')
warnings.filterwarnings('ignore', category=FutureWarning, module='lightgbm')
warnings.filterwarnings('ignore', category=DeprecationWarning, module='lightgbm')


# Adicionar o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from xgboost import XGBRegressor
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

try:
    from lightgbm import LGBMRegressor
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False

class MLFeatureEngineer:
    """
    Engenheiro de features para modelos ML de séries temporais.
    """
    
    def __init__(self, lag_days: int = 14, seasonal_periods: int = 7):
        self.lag_days = lag_days
        self.seasonal_periods = seasonal_periods
        # Cache leve em memória (TTL) para evitar recomputo caro de features
        self.cache_enabled = True
        self.cache_ttl_seconds = 3600  # 1h por padrão
        # key -> (timestamp, features_df)
        self._cache: Dict[str, Tuple[float, pd.DataFrame]] = {}

    def _hash_dataframe_content(self, df: pd.DataFrame, date_col: str, target_col: str) -> str:
        # Usa colunas relevantes; ordena para estabilidade
        extra_cols = [
            'order_id',
            'product_category_name',
            'marketplaceNome',
            'customer_state',
            'carrier_name',
            'funnel_cancelled',
            'funnel_problem',
            'lead_time_delivery_days',
            'delivery_delay_days',
        ]
        cols = [date_col]
        if target_col in df.columns:
            cols.append(target_col)
        for c in extra_cols:
            if c in df.columns:
                cols.append(c)
        df_sub = df[cols].copy()
        # Normaliza datas e ordena
        df_sub[date_col] = pd.to_datetime(df_sub[date_col])
        df_sub = df_sub.sort_values(by=[date_col]).reset_index(drop=True)
        # Converte para string compacta
        data_str = df_sub.to_csv(index=False)
        # Hash rápido
        try:
            import hashlib
            # MD5 usado apenas para cache de hash, não para segurança
            return hashlib.md5(data_str.encode('utf-8')).hexdigest()  # nosec B324
        except Exception:
            # Fallback simples
            return str(abs(hash(data_str)))

    def _make_cache_key(self, df: pd.DataFrame, date_col: str, target_col: str) -> str:
        df_hash = self._hash_dataframe_content(df, date_col, target_col)
        return f"{df_hash}|{date_col}|{target_col}|lags={self.lag_days}|season={self.seasonal_periods}"
        
    def create_features(
        self,
        df: pd.DataFrame,
        date_col: str,
        target_col: str,
        use_cache: bool = True,
        drop_cancelled: bool = True,
        revenue_col: str = "valorTotalFinal",
        revenue_fallbacks: Optional[List[str]] = None,
        top_n_share: int = 10,
        include_mix_features: bool = False,
        include_exogenous_features: bool = False,
    ) -> pd.DataFrame:
        """
        Cria features avançadas para modelos ML.
        
        Args:
            df: DataFrame com dados históricos
            date_col: Nome da coluna de data
            target_col: Nome da coluna de valor
            use_cache: Se verdadeiro, reutiliza features cacheadas quando possível
            include_mix_features: Se deve incluir features de mix (share).
                                  ATENÇÃO: Forecast recursivo (predict) não suporta mix features ainda.
            include_exogenous_features: Se deve incluir features exógenas (taxas de cancel/problem e logística).
            
        Returns:
            DataFrame com features criadas
        """
        revenue_fallbacks = revenue_fallbacks or ["ticket_liquido_linha", "price"]

        # Tentar cache
        if self.cache_enabled and use_cache:
            try:
                cache_key = self._make_cache_key(df, date_col, target_col)
                # Adicionar flag ao cache key para evitar colisão
                cache_key += f"|mix={include_mix_features}|exo={include_exogenous_features}"
                cached = self._cache.get(cache_key)
                if cached:
                    ts, features_df_cached = cached
                    if (time.time() - ts) <= self.cache_ttl_seconds:
                        return features_df_cached.copy()
            except Exception:
                # Não bloquear fluxo em caso de qualquer falha no cache
                pass

        df_work = df.copy()

        # Filtrar cancelados para demanda/receita líquida
        if drop_cancelled:
            if "funnel_cancelled" in df_work.columns:
                df_work = df_work[df_work["funnel_cancelled"] != 1]
            elif "pedido_cancelado" in df_work.columns:
                df_work = df_work[df_work["pedido_cancelado"] != 1]

        df_work['date'] = pd.to_datetime(df_work[date_col])
        df_work = df_work.sort_values('date').reset_index(drop=True)
        
        # Determinar coluna de valor conforme objetivo
        agg_col = target_col
        if target_col == "price":
            # Tentar usar revenue_col (valorTotalFinal) se tiver dados
            used_revenue = False
            if revenue_col in df_work.columns:
                candidate = pd.to_numeric(df_work[revenue_col], errors='coerce').fillna(0)
                if candidate.sum() > 0:
                    df_work['price'] = candidate
                    agg_col = revenue_col # Apenas indicativo, já substituímos 'price'
                    used_revenue = True
            
            # Se não usou revenue_col (ou estava zerada), tentar fallbacks
            if not used_revenue:
                for fb in revenue_fallbacks:
                    if fb in df_work.columns:
                        candidate = pd.to_numeric(df_work[fb], errors='coerce').fillna(0)
                        if candidate.sum() > 0:
                            df_work['price'] = candidate
                            used_revenue = True
                            break
            
            # Se ainda assim nada funcionou (tudo zero ou ausente), mantém 'price' original se existir
            if not used_revenue and 'price' in df_work.columns:
                 df_work['price'] = pd.to_numeric(df_work['price'], errors='coerce').fillna(0)
        
        # Agregar por dia com base no target_col (ex.: 'price' para receita, 'order_id' para quantidade)
        if agg_col == 'order_id':
            daily_data = df_work.groupby(df_work['date'].dt.date)['order_id'].nunique().reset_index()
        else:
            daily_data = df_work.groupby(df_work['date'].dt.date)[agg_col].sum().reset_index()
        daily_data.columns = ['date', 'value']
        daily_data['date'] = pd.to_datetime(daily_data['date'])
        daily_data = daily_data.sort_values('date').reset_index(drop=True)

        # ---- Covariáveis adicionais (mix, taxas, logística) ----
        def _safe_numeric(series):
            return pd.to_numeric(series, errors='coerce')

        # Taxas de cancelamento / problema e logística (opcional, pois aumentam colunas)
        if include_exogenous_features:
            if 'order_id' in df_work.columns:
                orders_per_day = df_work.groupby(df_work['date'].dt.date)['order_id'].nunique()
                if 'funnel_cancelled' in df_work.columns:
                    cancels = df_work[df_work['funnel_cancelled'] == 1].groupby(df_work['date'].dt.date)['order_id'].nunique()
                    daily_data['cancel_rate'] = daily_data['date'].dt.date.map(cancels / orders_per_day).fillna(0)
                elif 'pedido_cancelado' in df_work.columns:
                    cancels = df_work[df_work['pedido_cancelado'] == 1].groupby(df_work['date'].dt.date)['order_id'].nunique()
                    daily_data['cancel_rate'] = daily_data['date'].dt.date.map(cancels / orders_per_day).fillna(0)
                if 'funnel_problem' in df_work.columns:
                    problems = df_work[df_work['funnel_problem'] == 1].groupby(df_work['date'].dt.date)['order_id'].nunique()
                    daily_data['problem_rate'] = daily_data['date'].dt.date.map(problems / orders_per_day).fillna(0)

            if 'lead_time_delivery_days' in df_work.columns:
                tmp_lead = df_work.copy()
                tmp_lead['lead_time_delivery_days'] = _safe_numeric(tmp_lead['lead_time_delivery_days'])
                lead_mean = tmp_lead.groupby(tmp_lead['date'].dt.date)['lead_time_delivery_days'].mean()
                daily_data['lead_time_mean'] = daily_data['date'].dt.date.map(lead_mean).fillna(0)
            if 'delivery_delay_days' in df_work.columns:
                tmp_delay = df_work.copy()
                tmp_delay['delivery_delay_days'] = _safe_numeric(tmp_delay['delivery_delay_days'])
                delay_mean = tmp_delay.groupby(tmp_delay['date'].dt.date)['delivery_delay_days'].mean()
                daily_data['delay_mean'] = daily_data['date'].dt.date.map(delay_mean).fillna(0)

        # Mix shares helper
        def _add_share(df_source: pd.DataFrame, key: str, prefix: str):
            nonlocal daily_data
            if key not in df_source.columns or 'order_id' not in df_source.columns:
                return
            tmp = df_source[['date', 'order_id', key]].dropna()
            if tmp.empty:
                return
            top_keys = (
                tmp.groupby(key)['order_id']
                .nunique()
                .sort_values(ascending=False)
                .head(top_n_share)
                .index
            )
            tmp = tmp[tmp[key].isin(top_keys)]
            mat = (
                tmp.groupby([tmp['date'].dt.date, key])['order_id']
                .nunique()
                .reset_index()
            )
            pivot = mat.pivot(index='date', columns=key, values='order_id').fillna(0)
            pivot = pivot.div(pivot.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)
            pivot.columns = [f"{prefix}{c}" for c in pivot.columns]
            pivot = pivot.reset_index()
            pivot['date'] = pd.to_datetime(pivot['date'])
            daily_data = daily_data.merge(pivot, on='date', how='left')

        if include_mix_features:
            _add_share(df_work, 'product_category_name', 'share_cat_')
            _add_share(df_work, 'marketplaceNome', 'share_channel_')
            _add_share(df_work, 'customer_state', 'share_state_')
            _add_share(df_work, 'carrier_name', 'share_carrier_')
        
        # Features de lag
        for i in range(1, self.lag_days + 1):
            daily_data[f'lag_{i}'] = daily_data['value'].shift(i)
        
        # Features de média móvel
        for window in [3, 7, 14, 30]:
            daily_data[f'ma_{window}'] = daily_data['value'].rolling(window=window).mean()
            daily_data[f'ma_{window}_std'] = daily_data['value'].rolling(window=window).std()
        
        # Features de sazonalidade
        daily_data['day_of_week'] = daily_data['date'].dt.dayofweek
        daily_data['day_of_month'] = daily_data['date'].dt.day
        daily_data['month'] = daily_data['date'].dt.month
        daily_data['quarter'] = daily_data['date'].dt.quarter
        daily_data['is_weekend'] = daily_data['day_of_week'].isin([5, 6]).astype(int)
        
        # Features de tendência
        daily_data['trend'] = range(len(daily_data))
        daily_data['trend_squared'] = daily_data['trend'] ** 2
        
        # ---- Features de Datas Especiais (Feriados e Shopee Dates) ----
        # Datas Duplas (1.1, 2.2, ..., 11.11, 12.12)
        daily_data['is_double_date'] = (daily_data['month'] == daily_data['day_of_month']).astype(int)
        
        # Feriados Fixos Nacionais
        # 1/1, 21/4, 1/5, 7/9, 12/10, 2/11, 15/11, 25/12
        fixed_holidays = {
            (1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)
        }
        daily_data['is_holiday'] = daily_data['date'].apply(
            lambda x: 1 if (x.month, x.day) in fixed_holidays else 0
        )
        
        # Black Friday (4ª sexta de novembro) - Aproximação simplificada para semana
        # Identifica se é novembro e sexta-feira, depois filtra as datas
        def is_bf(d):
            if d.month == 11 and d.dayofweek == 4 and d.day >= 23 and d.day <= 29:
                return 1
            return 0
        daily_data['is_black_friday'] = daily_data['date'].apply(is_bf)
        
        # Features de volatilidade
        daily_data['volatility_7'] = daily_data['value'].rolling(window=7).std()
        daily_data['volatility_14'] = daily_data['value'].rolling(window=14).std()
        
        # Features de crescimento
        daily_data['growth_7'] = daily_data['value'].pct_change(7)
        daily_data['growth_14'] = daily_data['value'].pct_change(14)
        
        # Features de sazonalidade semanal
        for i in range(7):
            daily_data[f'weekday_{i}'] = (daily_data['day_of_week'] == i).astype(int)
        
        # Features de sazonalidade mensal
        for i in range(1, 13):
            daily_data[f'month_{i}'] = (daily_data['month'] == i).astype(int)
        
        # Features de interação
        daily_data['weekend_volatility'] = daily_data['is_weekend'] * daily_data['volatility_7']
        daily_data['trend_volatility'] = daily_data['trend'] * daily_data['volatility_7']
        
        # Salvar no cache
        if self.cache_enabled and use_cache:
            try:
                self._cache[cache_key] = (time.time(), daily_data.copy())
            except Exception:
                pass

        return daily_data

class MLEnsembleForecast:
    """
    Sistema de ensemble ML usando XGBoost e LightGBM.
    """
    
    def __init__(self, 
                 xgb_params: Optional[Dict] = None,
                 lgb_params: Optional[Dict] = None,
                 ensemble_weights: Optional[Dict] = None,
                 feature_engineer: Optional[MLFeatureEngineer] = None):
        """
        Inicializa o ensemble ML.
        
        Args:
            xgb_params: Parâmetros do XGBoost
            lgb_params: Parâmetros do LightGBM
            ensemble_weights: Pesos do ensemble
            feature_engineer: Engenheiro de features
        """
        self.xgb_params = xgb_params or self._get_default_xgb_params()
        self.lgb_params = lgb_params or self._get_default_lgb_params()
        self.ensemble_weights = ensemble_weights or {'xgb': 0.6, 'lgb': 0.4}
        self.feature_engineer = feature_engineer or MLFeatureEngineer()
        
        # Inicializar modelos
        self.xgb_model = None
        self.lgb_model = None
        self.feature_columns = None
        self.is_trained = False
        
        # Validação de dependências
        if not XGBOOST_AVAILABLE:
            raise ImportError("XGBoost não está instalado. Use 'pip install xgboost'.")
        if not LIGHTGBM_AVAILABLE:
            raise ImportError("LightGBM não está instalado. Use 'pip install lightgbm'.")
    
    def _get_default_xgb_params(self) -> Dict:
        """Retorna parâmetros padrão do XGBoost."""
        return {
            'n_estimators': 100,
            'max_depth': 4,
            'learning_rate': 0.05,
            'subsample': 0.9,
            'colsample_bytree': 0.9,
            'min_child_weight': 3,
            'reg_alpha': 0.1,
            'reg_lambda': 0.1,
            'random_state': 42,
            'n_jobs': -1,
            'verbosity': 0
        }
    
    def _get_default_lgb_params(self) -> Dict:
        """Retorna parâmetros padrão do LightGBM."""
        return {
            'n_estimators': 100,
            'max_depth': 4,
            'learning_rate': 0.05,
            'subsample': 0.9,
            'colsample_bytree': 0.9,
            'min_child_samples': 10,
            'reg_alpha': 0.1,
            'reg_lambda': 0.1,
            'random_state': 42,
            'n_jobs': -1,
            'verbosity': -1,
            'verbose': -1
        }

    def _finalize_lgb_params(self, params: Dict) -> Dict:
        """Ajusta parâmetros do LGBM para evitar warnings (num_leaves vs max_depth) e reduzir logs."""
        final = dict(params or {})
        max_depth = final.get('max_depth', -1)
        if max_depth is not None and max_depth > 0 and 'num_leaves' not in final:
            # Garante relação consistente: num_leaves <= 2^max_depth
            final['num_leaves'] = int(2 ** max_depth)
        # Força silêncio de logs em diferentes versões
        final['verbosity'] = -1
        final['verbose'] = -1
        return final

    def _silent_fit(self, model, X, y):
        """Suprime stdout/stderr de libs nativas durante o fit."""
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            model.fit(X, y)
    
    def fit(self, df: pd.DataFrame, date_col: str, target_col: str):
        """
        Treina o ensemble ML.
        
        Args:
            df: DataFrame com dados históricos
            date_col: Nome da coluna de data
            target_col: Nome da coluna de valor
        """
        # Criar features (desativar mix e exogenous features para compatibilidade com predict recursivo)
        features_df = self.feature_engineer.create_features(
            df, date_col, target_col, include_mix_features=False, include_exogenous_features=False
        )
        
        # Remover linhas com NaN
        features_df = features_df.dropna()
        
        # Relaxar limite mínimo para lidar com datasets menores
        min_required = 14
        if len(features_df) < min_required:
            raise ValueError(f"Dados insuficientes para treinamento (mínimo {min_required} dias, atual {len(features_df)})")
        
        # Preparar features e target
        feature_columns = [col for col in features_df.columns 
                          if col not in ['date', 'value']]
        self.feature_columns = feature_columns
        
        X = features_df[feature_columns]
        y = features_df['value']
        
        # Treinar XGBoost
        self.xgb_model = XGBRegressor(**self.xgb_params)
        self._silent_fit(self.xgb_model, X, y)
        
        # Treinar LightGBM
        lgb_final_params = self._finalize_lgb_params(self.lgb_params)
        self.lgb_model = LGBMRegressor(**lgb_final_params)
        self._silent_fit(self.lgb_model, X, y)
        
        self.is_trained = True
        
        # Salvar dados para predição
        self.last_date = features_df['date'].max()
        self.last_values = features_df['value'].tail(self.feature_engineer.lag_days).values
    
    def predict(self, periods: int) -> pd.DataFrame:
        """
        Gera previsão usando ensemble ML.
        
        Args:
            periods: Número de períodos a prever
            
        Returns:
            DataFrame com previsões
        """
        if not self.is_trained:
            raise RuntimeError("Modelo deve ser treinado antes de prever")
        
        # Criar datas futuras
        forecast_dates = pd.date_range(
            start=self.last_date + pd.Timedelta(days=1), 
            periods=periods, 
            freq='D'
        )
        
        # Inicializar histórico
        history = list(self.last_values)
        forecasts = []
        
        for i, date in enumerate(forecast_dates):
            # Criar features para esta predição
            features = self._create_prediction_features(history, date, i)
            
            # Fazer predições individuais
            xgb_pred = self.xgb_model.predict([features])[0]
            lgb_pred = self.lgb_model.predict([features])[0]
            
            # Ensemble ponderado
            ensemble_pred = (xgb_pred * self.ensemble_weights['xgb'] + 
                           lgb_pred * self.ensemble_weights['lgb'])
            
            # Validar predição
            if pd.isna(ensemble_pred) or ensemble_pred < 0:
                ensemble_pred = history[-1] if len(history) > 0 else 0
            
            forecasts.append(ensemble_pred)
            history.append(ensemble_pred)
        
        # Criar DataFrame de resultado
        forecast_df = pd.DataFrame({
            'date': forecast_dates,
            'forecast': forecasts
        })
        
        # Calcular intervalos de confiança
        self._add_confidence_intervals(forecast_df)
        
        return forecast_df
    
    def _create_prediction_features(self, history: List[float], date: pd.Timestamp, period: int) -> List[float]:
        """Cria features para uma predição específica."""
        features = []
        
        # Features de lag
        for i in range(1, self.feature_engineer.lag_days + 1):
            if len(history) >= i:
                features.append(history[-i])
            else:
                features.append(history[0] if len(history) > 0 else 0)
        
        # Features de média móvel
        for window in [3, 7, 14, 30]:
            if len(history) >= window:
                ma = np.mean(history[-window:])
                ma_std = np.std(history[-window:])
            else:
                ma = np.mean(history) if len(history) > 0 else 0
                ma_std = np.std(history) if len(history) > 1 else 0
            
            features.extend([ma, ma_std])
        
        # Features de sazonalidade
        features.append(date.dayofweek)  # day_of_week
        features.append(date.day)        # day_of_month
        features.append(date.month)      # month
        features.append(date.quarter)    # quarter
        features.append(1 if date.dayofweek in [5, 6] else 0)  # is_weekend
        
        # Features de tendência
        features.append(len(history) + period)  # trend
        features.append((len(history) + period) ** 2)  # trend_squared
        
        # ---- Features de Datas Especiais (Predict) ----
        # is_double_date
        features.append(1 if date.month == date.day else 0)
        
        # is_holiday
        fixed_holidays = {(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)}
        features.append(1 if (date.month, date.day) in fixed_holidays else 0)
        
        # is_black_friday
        is_bf = 1 if (date.month == 11 and date.dayofweek == 4 and date.day >= 23 and date.day <= 29) else 0
        features.append(is_bf)
        
        # Features de volatilidade
        if len(history) >= 7:
            vol_7 = np.std(history[-7:])
        else:
            vol_7 = np.std(history) if len(history) > 1 else 0
        
        if len(history) >= 14:
            vol_14 = np.std(history[-14:])
        else:
            vol_14 = np.std(history) if len(history) > 1 else 0
        
        features.extend([vol_7, vol_14])
        
        # Features de crescimento
        if len(history) >= 7:
            growth_7 = (history[-1] - history[-7]) / history[-7] if history[-7] != 0 else 0
        else:
            growth_7 = 0
        
        if len(history) >= 14:
            growth_14 = (history[-1] - history[-14]) / history[-14] if history[-14] != 0 else 0
        else:
            growth_14 = 0
        
        features.extend([growth_7, growth_14])
        
        # Features de sazonalidade semanal
        for i in range(7):
            features.append(1 if date.dayofweek == i else 0)
        
        # Features de sazonalidade mensal
        for i in range(1, 13):
            features.append(1 if date.month == i else 0)
        
        # Features de interação (usar variáveis locais explícitas)
        is_weekend_flag = 1 if date.dayofweek in [5, 6] else 0
        trend_value = len(history) + period
        features.append(is_weekend_flag * vol_7)  # weekend_volatility
        features.append(trend_value * vol_7)      # trend_volatility
        
        return features
    
    def _add_confidence_intervals(self, forecast_df: pd.DataFrame):
        """Adiciona intervalos de confiança às previsões."""
        # Calcular desvio padrão das previsões
        forecast_std = forecast_df['forecast'].std()
        
        # Intervalo de confiança de 95%
        confidence_factor = 1.96
        forecast_df['lower_bound'] = forecast_df['forecast'] - (confidence_factor * forecast_std)
        forecast_df['upper_bound'] = forecast_df['forecast'] + (confidence_factor * forecast_std)
        
        # Garantir que lower_bound não seja negativo
        forecast_df['lower_bound'] = forecast_df['lower_bound'].clip(lower=0)
    
    def evaluate(self, df: pd.DataFrame, date_col: str, target_col: str, 
                test_size_days: int = 14) -> Dict[str, float]:
        """
        Avalia o modelo usando validação temporal.
        
        Args:
            df: DataFrame com dados históricos
            date_col: Nome da coluna de data
            target_col: Nome da coluna de valor
            test_size_days: Número de dias para teste
            
        Returns:
            Dicionário com métricas de avaliação
        """
        # Criar features (sem mix e sem exogenous features)
        features_df = self.feature_engineer.create_features(
            df, date_col, target_col, include_mix_features=False, include_exogenous_features=False
        )
        features_df = features_df.dropna()
        
        min_required = test_size_days + 14 # Relaxado de 30 para 14
        if len(features_df) < min_required:
            raise ValueError(f"Dados insuficientes para validação (tem {len(features_df)}, precisa {min_required})")
        
        # Dividir dados
        train_data = features_df[:-test_size_days]
        test_data = features_df[-test_size_days:]
        
        # Treinar modelo
        X_train = train_data[self.feature_columns]
        y_train = train_data['value']
        
        xgb_model = XGBRegressor(**self.xgb_params)
        lgb_model = LGBMRegressor(**self._finalize_lgb_params(self.lgb_params))
        
        self._silent_fit(xgb_model, X_train, y_train)
        self._silent_fit(lgb_model, X_train, y_train)
        
        # Fazer previsões
        X_test = test_data[self.feature_columns]
        xgb_pred = xgb_model.predict(X_test)
        lgb_pred = lgb_model.predict(X_test)
        
        # Ensemble
        ensemble_pred = (xgb_pred * self.ensemble_weights['xgb'] + 
                        lgb_pred * self.ensemble_weights['lgb'])
        
        # Calcular métricas
        y_true = test_data['value'].values
        
        rmse = np.sqrt(mean_squared_error(y_true, ensemble_pred))
        mae = mean_absolute_error(y_true, ensemble_pred)
        
        # MAPE
        mape_mask = y_true != 0
        if mape_mask.sum() > 0:
            mape = np.mean(np.abs((y_true[mape_mask] - ensemble_pred[mape_mask]) / y_true[mape_mask])) * 100
        else:
            mape = np.nan
        
        return {
            'RMSE': rmse,
            'MAE': mae,
            'MAPE': mape
        }

    def evaluate_multiple_horizons(
        self,
        df: pd.DataFrame,
        date_col: str,
        target_col: str,
        horizons: List[int] = None,
        min_train_days: int = 30
    ) -> Dict[int, Dict[str, float]]:
        """
        Treina UMA vez usando o maior horizonte e calcula métricas para todos os
        horizontes truncando o conjunto de teste (sem retreinar).

        Returns:
            Dict[horizon] -> {'RMSE': float, 'MAE': float, 'MAPE': float}
        """
        if horizons is None:
            horizons = [21, 14, 7]
        horizons = sorted(set(horizons), reverse=True)
        max_h = max(horizons)

        # Criar features (com cache e SEM mix/exogenous features para evitar mismatch) e validar tamanho
        features_df = self.feature_engineer.create_features(
            df, date_col, target_col, use_cache=True, include_mix_features=False, include_exogenous_features=False
        )
        features_df = features_df.dropna()

        # Relaxar validação para dataset pequeno
        actual_min_train = max(14, min_train_days)
        if len(features_df) < (actual_min_train + max_h):
            # Tentar reduzir horizonte máximo se possível
            if len(features_df) > (actual_min_train + 7):
                 max_h = 7
                 horizons = [h for h in horizons if h <= 7]
                 if not horizons:
                     horizons = [7]
            else:
                raise ValueError(f"Dados insuficientes para validação multi-horizonte (tem {len(features_df)}, precisa {actual_min_train + max_h})")

        feature_columns = [c for c in features_df.columns if c not in ['date', 'value']]

        # Split único: treino até -max_h, teste de tamanho max_h
        train_df = features_df[:-max_h]
        test_df_full = features_df[-max_h:]

        X_train = train_df[feature_columns]
        y_train = train_df['value']
        X_test_full = test_df_full[feature_columns]
        y_test_full = test_df_full['value'].values

        # Fit único
        xgb_model = XGBRegressor(**self.xgb_params)
        lgb_model = LGBMRegressor(**self._finalize_lgb_params(self.lgb_params))
        self._silent_fit(xgb_model, X_train, y_train)
        self._silent_fit(lgb_model, X_train, y_train)

        # Predição para janela completa (max_h)
        xgb_pred_full = xgb_model.predict(X_test_full)
        lgb_pred_full = lgb_model.predict(X_test_full)
        ens_pred_full = (xgb_pred_full * self.ensemble_weights['xgb'] +
                         lgb_pred_full * self.ensemble_weights['lgb'])

        results: Dict[int, Dict[str, float]] = {}
        for h in horizons:
            y_true = y_test_full[-h:]
            y_pred = ens_pred_full[-h:]

            rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
            mae = float(mean_absolute_error(y_true, y_pred))
            mape_mask = y_true != 0
            if mape_mask.sum() > 0:
                mape = float(np.mean(np.abs((y_true[mape_mask] - y_pred[mape_mask]) / y_true[mape_mask])) * 100)
            else:
                mape = float('nan')

            results[h] = {'RMSE': rmse, 'MAE': mae, 'MAPE': mape}

        return results

class MLStockRecommendationSystem:
    """
    Sistema de recomendações de estoque baseado em ensemble ML com seleção dinâmica de horizonte.
    """
    
    def __init__(self, 
                 min_revenue: float = 15000,
                 min_data_points: int = 30,
                 mape_threshold: float = 40.0,
                 use_dynamic_horizon: bool = True):
        """
        Inicializa o sistema de recomendações.
        
        Args:
            min_revenue: Receita mínima para considerar categoria
            min_data_points: Mínimo de pontos de dados
            mape_threshold: Limite máximo de MAPE aceitável
            use_dynamic_horizon: Se deve usar seleção dinâmica de horizonte baseada no MAPE
        """
        self.min_revenue = min_revenue
        self.min_data_points = min_data_points
        self.mape_threshold = mape_threshold
        self.use_dynamic_horizon = use_dynamic_horizon
        
        # Horizontes disponíveis para teste
        self.available_horizons = [7, 14, 21]
        
        # Limites de MAPE por horizonte (baseados em benchmarks de mercado)
        self.horizon_mape_limits = {
            7: 25.0,   # Curto prazo - mais preciso
            14: 35.0,  # Médio prazo - balanceado
            21: 45.0   # Longo prazo - menos preciso
        }
        
        # Configurações do ensemble
        self.ensemble_configs = {
            'conservative': {
                'xgb_params': {'n_estimators': 50, 'max_depth': 3, 'learning_rate': 0.03, 'min_child_weight': 5},
                'lgb_params': {'n_estimators': 50, 'max_depth': 3, 'learning_rate': 0.03, 'min_child_samples': 15},
                'weights': {'xgb': 0.5, 'lgb': 0.5}
            },
            'balanced': {
                'xgb_params': {'n_estimators': 100, 'max_depth': 4, 'learning_rate': 0.05, 'min_child_weight': 3},
                'lgb_params': {'n_estimators': 100, 'max_depth': 4, 'learning_rate': 0.05, 'min_child_samples': 10},
                'weights': {'xgb': 0.6, 'lgb': 0.4}
            },
            'aggressive': {
                'xgb_params': {'n_estimators': 150, 'max_depth': 5, 'learning_rate': 0.08, 'min_child_weight': 2},
                'lgb_params': {'n_estimators': 150, 'max_depth': 5, 'learning_rate': 0.08, 'min_child_samples': 5},
                'weights': {'xgb': 0.7, 'lgb': 0.3}
            }
        }
    
    def _select_optimal_horizon(self, category_data: pd.DataFrame, category: str) -> Tuple[int, float, str]:
        """
        Seleciona o horizonte ótimo baseado no MAPE para uma categoria específica.
        
        Args:
            category_data: Dados da categoria
            category: Nome da categoria
            
        Returns:
            Tupla com (horizonte_ótimo, mape_ótimo, configuração_ótima)
        """
        # Mantido para compatibilidade; método antigo (custo alto). Preferir _select_optimal_horizon_optimized.
        if not self.use_dynamic_horizon:
            # Usar horizonte fixo de 14 dias se seleção dinâmica estiver desabilitada
            return 14, 0.0, 'fixed'
        
        print(f"   🔍 Testando horizontes dinâmicos para {category}...")
        
        best_horizon = 7  # Começar com o mais conservador
        best_mape = float('inf')
        best_config = 'conservative'
        
        # Testar cada horizonte disponível
        for horizon in self.available_horizons:
            print(f"     📅 Testando horizonte: {horizon} dias")
            
            # Selecionar configuração baseada no horizonte
            if horizon <= 7:
                config_name = 'conservative'
            elif horizon <= 14:
                config_name = 'balanced'
            else:
                config_name = 'aggressive'
            
            try:
                # Criar ensemble com configuração apropriada
                config = self.ensemble_configs[config_name]
                ensemble = MLEnsembleForecast(
                    xgb_params=config['xgb_params'],
                    lgb_params=config['lgb_params'],
                    ensemble_weights=config['weights']
                )
                
                # Treinar modelo usando volume de pedidos
                ensemble.fit(category_data, 'order_purchase_timestamp', 'order_id')
                
                # Avaliar modelo no mesmo alvo (volume)
                metrics = ensemble.evaluate(category_data, 'order_purchase_timestamp', 'order_id')
                mape = metrics['MAPE']
                
                print(f"       📈 MAPE: {mape:.2f}% (limite: {self.horizon_mape_limits[horizon]:.1f}%)")
                
                # Verificar se o MAPE está dentro do limite para este horizonte
                if mape <= self.horizon_mape_limits[horizon]:
                    # Se for melhor que o atual, atualizar
                    if mape < best_mape:
                        best_mape = mape
                        best_horizon = horizon
                        best_config = config_name
                        print(f"       ✅ Novo melhor: {horizon}d com MAPE {mape:.2f}%")
                else:
                    print(f"       ❌ MAPE muito alto para {horizon}d")
                    
            except Exception as e:
                print(f"       ⚠️ Erro no horizonte {horizon}d: {e}")
                continue
        
        # Se nenhum horizonte passou no teste, usar o mais conservador
        if best_mape == float('inf'):
            print(f"     ⚠️ Nenhum horizonte passou no teste, usando 7d como fallback")
            best_horizon = 7
            best_mape = 0.0
            best_config = 'conservative'
        
        print(f"     🎯 Horizonte selecionado: {best_horizon}d (MAPE: {best_mape:.2f}%, config: {best_config})")
        return best_horizon, best_mape, best_config

    def _select_optimal_horizon_optimized(self, category_data: pd.DataFrame, category: str) -> Tuple[int, float, str]:
        """
        Versão otimizada: treino único (max_h=21) e avaliação multi-horizonte via truncamento.
        Retorna (horizon, mape, config_name).
        """
        if not self.use_dynamic_horizon:
            return 14, 0.0, 'fixed'

        print(f"   🔍 [Fast] Avaliando horizontes para {category} com treino único...")

        # Treinar UMA vez com configuração voltada a horizonte longo
        config_name_for_eval = 'aggressive'
        config_eval = self.ensemble_configs[config_name_for_eval]
        temp_ensemble = MLEnsembleForecast(
            xgb_params=config_eval['xgb_params'],
            lgb_params=config_eval['lgb_params'],
            ensemble_weights=config_eval['weights']
        )

        try:
            metrics_by_h = temp_ensemble.evaluate_multiple_horizons(
                category_data,
                date_col='order_purchase_timestamp',
                target_col='order_id',
                horizons=[21, 14, 7],
                min_train_days=self.min_data_points
            )
        except Exception as e:
            print(f"     ⚠️ Falha na avaliação rápida: {e}. Fallback para 7 dias")
            return 7, 0.0, 'conservative'

        # Escolher melhor horizonte respeitando limites
        best_horizon = 7
        best_mape = float('inf')
        best_config = 'conservative'
        # Prioridade: maiores horizontes primeiro, se dentro do limite e com MAPE menor
        for h in [21, 14, 7]:
            m = metrics_by_h.get(h, {})
            mape_val = m.get('MAPE', float('inf'))
            if np.isnan(mape_val):
                continue
            limit = self.horizon_mape_limits.get(h, float('inf'))
            if mape_val <= limit and mape_val < best_mape:
                best_horizon = h
                best_mape = mape_val
                best_config = 'aggressive' if h == 21 else ('balanced' if h == 14 else 'conservative')

        # Se nada passou no limite, pegar o menor MAPE mesmo acima do limite
        if best_mape == float('inf'):
            # Seleciona pelo menor MAPE absoluto
            best_horizon, best_mape = min(
                ((h, v.get('MAPE', float('inf'))) for h, v in metrics_by_h.items() if not np.isnan(v.get('MAPE', float('nan')))),
                key=lambda x: x[1],
                default=(7, 0.0)
            )
            best_config = 'aggressive' if best_horizon == 21 else ('balanced' if best_horizon == 14 else 'conservative')

        print(f"     🎯 [Fast] Selecionado: {best_horizon}d (MAPE: {best_mape:.2f}%, config: {best_config})")
        return best_horizon, float(best_mape), best_config
    
    def _calculate_category_lead_time(self, category: str, stock_movements: pd.DataFrame, df_sales: pd.DataFrame) -> Tuple[float, str]:
        """
        Calcula o lead time (ciclo de reposição) para a categoria usando dados de movimentação.
        Retorna (dias, método_usado).
        """
        default_lead_time = 15.0
        
        if stock_movements is None or stock_movements.empty:
            return default_lead_time, "fixed"

        # Tentar cruzar product_id -> categoria se stock_movements não tiver
        # Mas para simplificar, vamos assumir que o usuário filtra ou que mapeamos antes
        # Aqui vamos mapear product_ids da categoria
        
        try:
            # Pegar produtos da categoria
            cat_products = df_sales[df_sales['product_category_name'] == category]['product_id'].unique()
            if len(cat_products) == 0:
                return default_lead_time, "fixed"
                
            # Converter para string para garantir match
            cat_products = [str(p) for p in cat_products]
            
            # Filtrar movimentos desses produtos
            # Assumindo coluna product_id em stock_movements
            if 'product_id' not in stock_movements.columns:
                 return default_lead_time, "fixed"
                 
            # Garantir tipos
            sm_subset = stock_movements[stock_movements['product_id'].astype(str).isin(cat_products)].copy()
            
            if sm_subset.empty:
                return default_lead_time, "fixed"
            
            # Identificar Entradas: qty > 0 e não é estorno/saída
            # Se tiver 'type' (1=entrada, 2=saída geralmente), usar. Se não, qty positivo.
            # No Magazord V1: qty vem sempre positivo? type define?
            # Vamos assumir:
            # Se houver coluna 'type', vamos filtrar type == 1 ou algo assim. 
            # Mas como não temos certeza do dicionário, vamos usar a heurística de diff de saldo ou qty se possível.
            # O código de coleta: 'qty': item.get('quantidade').
            # Vamos tentar inferir entradas. Se não tivermos certeza, usamos fallback.
            
            # Heurística Magazord: 'tipoOperacao' costuma ter descrição "ENTRADA DE NOTA", "COMPRA", etc.
            # Ou 'tipo' numérico.
            # Vamos usar uma abordagem genérica: agrupar por data e ver dias com saldo líquido positivo significativo
            
            # Converter datas
            sm_subset['date'] = pd.to_datetime(sm_subset['date'])
            sm_subset = sm_subset.sort_values('date')
            
            # Agrupar entradas por dia (considerando entradas aquelas com observação ou tipo indicativo)
            # Simplificação: considerar dias onde houve movimentação significativa como "dias de gestão de estoque"
            # Melhor: calcular média de dias entre movimentações de entrada.
            
            # Se tivermos 'operation_type', tente filtrar 'Compra' ou 'Entrada'
            # Se não, vamos considerar todas as datas únicas de movimento como pontos de contato logístico
            # Isso é uma proxy fraca, mas melhor que nada.
            
            # Tentar ser mais específico se possível:
            entry_keywords = ['COMPRA', 'ENTRADA', 'NF', 'FORNECEDOR']
            
            # Se 'observation' ou 'operation_type' strings existirem
            is_entry = pd.Series(False, index=sm_subset.index)
            
            if 'operation_type' in sm_subset.columns:
                 # Check string contains (case insensitive)
                 is_entry |= sm_subset['operation_type'].astype(str).str.upper().str.contains('|'.join(entry_keywords))
            
            if 'observation' in sm_subset.columns:
                 is_entry |= sm_subset['observation'].astype(str).str.upper().str.contains('|'.join(entry_keywords))
            
            # Se não detectou nada por texto, use a coluna 'type' se existir (geralmente 0 ou 1)
            # Sem documentação, é arriscado. Vamos assumir que se detectou entries por texto, usamos.
            # Se não, fallback.
            
            entries = sm_subset[is_entry]
            
            if len(entries) < 2:
                # Menos de 2 entradas, impossível calcular ciclo
                return default_lead_time, "fixed_insufficient_data"
                
            # Datas únicas de entrada
            entry_dates = entries['date'].dt.date.unique()
            entry_dates.sort()
            
            if len(entry_dates) < 2:
                 return default_lead_time, "fixed_insufficient_data"
            
            # Calcular diff em dias
            diffs = np.diff(entry_dates)
            avg_days = np.mean([d.days for d in diffs])
            
            # Limites sanitários (lead time entre 5 e 90 dias)
            if 5 <= avg_days <= 90:
                return float(avg_days), "dynamic_movements"
            else:
                return default_lead_time, "fixed_out_of_bounds"

        except Exception as e:
            print(f"       ⚠️ Erro calculando lead time dinâmico: {e}")
            return default_lead_time, "fixed_error"

    def generate_recommendations(self, df: pd.DataFrame, stock_movements: pd.DataFrame = None, current_stock: pd.DataFrame = None) -> List[Dict[str, Any]]:
        """
        Gera recomendações de estoque usando ensemble ML.
        
        Args:
            df: DataFrame com dados filtrados
            stock_movements: DataFrame opcional com histórico de movimentações (listMovimentacaoEstoque)
            current_stock: DataFrame opcional com estoque atual (listEstoque)
            
        Returns:
            Lista de recomendações por categoria
        """
        print("🤖 GERANDO RECOMENDAÇÕES COM ENSEMBLE ML")
        print("=" * 50)
        
        has_stock = current_stock is not None and not current_stock.empty
        has_movements = stock_movements is not None and not stock_movements.empty
        
        print(f"📡 Fontes de Dados Conectadas:")
        print(f"   1. Vendas/Pedidos: ✅ ({len(df)} registros)")
        print(f"   2. Estoque Atual (listEstoque): {'✅' if has_stock else '❌'} ({len(current_stock) if has_stock else 0} produtos)")
        print(f"   3. Movimentações (listMovimentacaoEstoque): {'✅' if has_movements else '❌'} ({len(stock_movements) if has_movements else 0} registros)")
        print("=" * 50)

        # ----------------------------------------------------
        # Snapshot freshness (evita usar gap com estoque antigo)
        # ----------------------------------------------------
        stock_asof_ts: Optional[pd.Timestamp] = None
        stock_age_days: Optional[int] = None
        stock_is_stale: bool = False
        STALE_DAYS_THRESHOLD = 7  # se o snapshot tiver mais de X dias, não usamos gap (apenas tendência)

        if has_stock:
            try:
                for col in ["snapshot_timestamp", "data_hora_atualizacao", "dataHoraAtualizacao", "last_update"]:
                    if col in current_stock.columns:
                        s = pd.to_datetime(current_stock[col], errors="coerce")
                        if s.notna().any():
                            cand = s.max()
                            stock_asof_ts = cand if stock_asof_ts is None else max(stock_asof_ts, cand)
                if stock_asof_ts is not None and pd.notna(stock_asof_ts):
                    stock_age_days = int((pd.Timestamp.now() - stock_asof_ts).days)
                    stock_is_stale = bool(stock_age_days > STALE_DAYS_THRESHOLD)
            except Exception:
                pass

        if has_stock and stock_asof_ts is not None:
            stale_txt = " (STALE)" if stock_is_stale else ""
            print(f"🧾 Estoque as-of: {stock_asof_ts.strftime('%Y-%m-%d %H:%M:%S')} | idade: {stock_age_days} dias{stale_txt}")
        elif has_stock:
            print("🧾 Estoque as-of: desconhecido (sem coluna de timestamp detectável)")
        
        # Tratamento de Receita: fallback inteligente para valorTotalFinal -> price
        df_work = df.copy()
        if 'valorTotalFinal' in df_work.columns:
            vtf = pd.to_numeric(df_work['valorTotalFinal'], errors='coerce').fillna(0)
            if vtf.sum() > 0:
                df_work['price'] = vtf
        
        # Obter categorias válidas
        category_volumes = df_work.groupby('product_category_name')['price'].sum().sort_values(ascending=False)
        valid_categories = category_volumes[category_volumes >= self.min_revenue].index.tolist()
        
        print(f"📊 Categorias válidas: {len(valid_categories)}")
        print(f"💰 Receita mínima: R$ {self.min_revenue:,.2f}")
        print(f"🎯 Limite MAPE: {self.mape_threshold}%")
        
        # Preparar lookup de estoque atual por categoria se disponível
        category_current_stock = {}
        if has_stock:
            # Assumindo colunas padronizadas ou fazendo inferência básica
            # listEstoque geralmente traz: produto/sku, quantidadeDisponivelVenda
            # Precisamos cruzar com categoria. O current_stock pode não ter categoria.
            # Vamos tentar usar o df de vendas para mapear produto -> categoria
            try:
                # Caso 1: snapshot já vem com categoria -> agrega direto
                temp_stock = current_stock.copy()

                # Normalizar possíveis nomes de colunas do snapshot
                if "produto_id" in temp_stock.columns and "product_id" not in temp_stock.columns:
                    temp_stock["product_id"] = temp_stock["produto_id"]
                if "quantidade_disponivel_venda" in temp_stock.columns and "stock_level" not in temp_stock.columns:
                    temp_stock["stock_level"] = temp_stock["quantidade_disponivel_venda"]

                # Se já tiver categoria no estoque, agrega direto
                if "product_category_name" in temp_stock.columns and "stock_level" in temp_stock.columns:
                    temp_stock["product_category_name"] = temp_stock["product_category_name"].fillna("Sem Categoria")
                    grp = temp_stock.groupby("product_category_name")["stock_level"].sum()
                    category_current_stock = grp.to_dict()
                else:
                    # Caso 2: mapear produto -> categoria via vendas
                    # Normalizar chaves para evitar mismatch (ex: BC02 vs bc02)
                    tmp_sales = df_work[["product_id", "product_category_name"]].dropna(subset=["product_id", "product_category_name"]).copy()
                    tmp_sales["product_id"] = tmp_sales["product_id"].astype(str).str.strip().str.upper()
                    product_cat_map = tmp_sales.drop_duplicates(subset=["product_id"]).set_index("product_id")["product_category_name"].to_dict()

                    # Tentar identificar coluna de ID do produto no estoque
                    prod_col = next((c for c in ["product_id", "produto_id", "produto", "sku", "codigo"] if c in temp_stock.columns), None)
                    qty_col = next((c for c in ["stock_level", "quantidade_disponivel_venda", "quantidadeDisponivelVenda", "saldo", "quantidade"] if c in temp_stock.columns), None)

                    if prod_col and qty_col:
                        temp_stock["_prod_key"] = temp_stock[prod_col].astype(str).str.strip().str.upper()
                        temp_stock["cat_temp"] = temp_stock["_prod_key"].map(product_cat_map)
                        grp = temp_stock.groupby("cat_temp")[qty_col].sum()
                        category_current_stock = grp.to_dict()

                        # Diagnóstico de match
                        try:
                            matched = int(temp_stock["cat_temp"].notna().sum())
                            total = int(len(temp_stock))
                            if total > 0:
                                print(f"   [ML] Stock mapping match: {matched}/{total} ({(matched/total):.1%}) using prod_col='{prod_col}', qty_col='{qty_col}'.")
                        except Exception:
                            pass
            except Exception as e:
                print(f"⚠️ Erro ao mapear estoque por categoria: {e}")

        recommendations = []
        
        for category in valid_categories:
            print(f"\n🔍 Processando: {category}")
            
            try:
                # Filtrar dados da categoria
                category_data = df[df['product_category_name'] == category].copy()
                
                if len(category_data) < self.min_data_points:
                    print(f"   ⚠️ Dados insuficientes: {len(category_data)} < {self.min_data_points}")
                    continue
                
                # Exibir estoque atual se disponível (somente se snapshot estiver fresco)
                if has_stock:
                    curr_qty = category_current_stock.get(category, 0)
                    if stock_is_stale:
                        print(f"   📦 Estoque Atual: N/A (snapshot stale)")
                    else:
                        print(f"   📦 Estoque Atual (snapshot): {int(curr_qty)} unidades")

                # Selecionar horizonte ótimo dinamicamente
                # Versão otimizada (treino único + avaliação truncada)
                optimal_horizon, optimal_mape, config_name = self._select_optimal_horizon_optimized(category_data, category)
                
                if optimal_mape > self.mape_threshold:
                    print(f"   ❌ MAPE muito alto: {optimal_mape:.2f}% > {self.mape_threshold}%")
                    continue
                
                # Criar ensemble com configuração ótima
                config = self.ensemble_configs[config_name]
                ensemble = MLEnsembleForecast(
                    xgb_params=config['xgb_params'],
                    lgb_params=config['lgb_params'],
                    ensemble_weights=config['weights']
                )
                
                # Treinar modelo para prever volume de pedidos
                ensemble.fit(category_data, 'order_purchase_timestamp', 'order_id')
                
                # Gerar previsão com horizonte ótimo
                forecast_df = ensemble.predict(periods=optimal_horizon)
                
                if forecast_df.empty:
                    print(f"   ❌ Previsão vazia")
                    continue
                
                # Calcular métricas de estoque
                monthly_orders = category_data.groupby(
                    pd.to_datetime(category_data['order_purchase_timestamp']).dt.to_period('M')
                )['order_id'].nunique()
                
                avg_monthly_orders = monthly_orders.mean() if not monthly_orders.empty else 0
                inventory_turnover = avg_monthly_orders / 30
                
                # Calcular variação prevista
                recent_days = category_data.groupby(
                    pd.to_datetime(category_data['order_purchase_timestamp']).dt.date
                )['order_id'].nunique().tail(optimal_horizon).sum()
                
                forecast_days = forecast_df['forecast'].sum()
                variation = (forecast_days - recent_days) / recent_days * 100 if recent_days > 0 else 0
                
                # Calcular estoque ideal usando a mesma lógica da planilha
                # 1. Demanda diária ajustada pela variação ML
                avg_daily_orders = avg_monthly_orders / 30
                adjusted_daily_orders = avg_daily_orders * (1 + variation/100)
                
                # 2. Estoque de segurança baseado em volatilidade (95% nível de serviço)
                # Usar coeficiente de variação da categoria (proxy: abs(variation)/100)
                coefficient_variation = min(abs(variation) / 100, 2.0)  # Limitar entre 0 e 2
                
                # Calcular Lead Time Dinâmico se possível
                lead_time_days, lead_time_method = self._calculate_category_lead_time(category, stock_movements, df)
                if lead_time_method == 'dynamic_movements':
                    print(f"   ⏱️ Lead Time Dinâmico (listMovimentacaoEstoque): {lead_time_days:.1f} dias")
                elif lead_time_method == 'fixed':
                    print(f"   ⏱️ Lead Time Fixo: {lead_time_days:.1f} dias")
                
                z_score_95 = 1.65
                safety_stock = z_score_95 * adjusted_daily_orders * np.sqrt(lead_time_days) * coefficient_variation
                
                # 3. Estoque ideal = (demanda ajustada * horizonte) + estoque de segurança
                horizon_days = forecast_df.shape[0]  # Usar horizonte real da previsão
                ideal_stock = (adjusted_daily_orders * horizon_days) + safety_stock
                
                # --- Estoque e GAP: só usar quando o snapshot estiver fresco ---
                use_gap = bool(has_stock and (not stock_is_stale))
                current_stock_qty = float(category_current_stock.get(category, 0) or 0) if use_gap else None
                stock_gap = (float(ideal_stock) - float(current_stock_qty or 0)) if use_gap else None

                # Determinar ação:
                # - Se gap disponível: usa gap
                # - Se estoque stale/ausente: usa apenas tendência (variação/giro)
                # Se Gap > 0: Falta estoque -> Comprar
                # Se Gap < 0: Sobra estoque -> Reduzir/Promoção
                
                action = "Manter"
                reason = "Estoque equilibrado"
                
                # Margem de tolerância de 10%
                tolerance = ideal_stock * 0.1
                
                if use_gap and stock_gap is not None:
                    if stock_gap > tolerance:
                        # Falta estoque significativa
                        if stock_gap > (ideal_stock * 0.5):
                            action = "Aumentar significativamente"
                            reason = f"Estoque crítico ({int(current_stock_qty or 0)}) vs Ideal ({int(ideal_stock)}). Risco de ruptura."
                        else:
                            action = "Aumentar moderadamente"
                            reason = f"Estoque abaixo do ideal ({int(current_stock_qty or 0)} < {int(ideal_stock)}). Repor para cobrir demanda."
                    elif stock_gap < -tolerance:
                        # Excesso de estoque significativo
                        if abs(stock_gap) > (ideal_stock * 0.5):
                            action = "Reduzir significativamente"
                            reason = f"Excesso de estoque ({int(current_stock_qty or 0)}). Ideal seria ~{int(ideal_stock)}. Considerar promoção."
                        else:
                            action = "Reduzir moderadamente"
                            reason = f"Estoque levemente acima do ideal ({int(current_stock_qty or 0)} > {int(ideal_stock)}). Monitorar giro."
                    else:
                        # Dentro da tolerância
                        # Refinar com base na tendência de vendas
                        if variation > 20:
                            action = "Manter (Vies Alta)"
                            reason = f"Estoque ok, mas vendas crescendo rápido ({variation:.1f}%). Monitorar reposição."
                        elif variation < -20:
                            action = "Manter (Vies Baixa)"
                            reason = f"Estoque ok, mas vendas caindo ({variation:.1f}%). Evitar recompra."
                        else:
                            action = "Manter"
                            reason = f"Estoque alinhado com a previsão de demanda ({int(current_stock_qty or 0)} ~ {int(ideal_stock)})."
                else:
                    # Modo tendência (estoque não confiável para gap)
                    if stock_is_stale and stock_asof_ts is not None:
                        prefix = f"Estoque não considerado (snapshot desatualizado: {stock_asof_ts.strftime('%Y-%m-%d')}). "
                    else:
                        prefix = "Estoque não considerado (sem snapshot atualizado). "

                    if variation > 10:
                        action = "Aumentar moderadamente"
                        reason = prefix + f"Crescimento previsto ({variation:.1f}%). Ajustar reposição para capturar demanda."
                    elif variation < -10:
                        action = "Reduzir moderadamente"
                        reason = prefix + f"Queda prevista ({variation:.1f}%). Reduzir reposição e evitar capital parado."
                    else:
                        action = "Manter"
                        reason = prefix + "Demanda relativamente estável no horizonte."

                # Montar detalhes padronizados para consumo no card
                details = {
                    'action': action,
                    'variation': variation,
                    'reason': reason,
                    'estoque_sugerido_vendas': int(ideal_stock),
                    'velocidade_vendas': inventory_turnover,
                    'lead_time_used': lead_time_days,
                    'estoque_atual_fisico': int(current_stock_qty) if current_stock_qty is not None else None,
                    'stock_gap': int(stock_gap) if stock_gap is not None else None,
                    'stock_asof': stock_asof_ts.isoformat() if stock_asof_ts is not None else None,
                    'stock_age_days': stock_age_days,
                    'stock_is_stale': bool(stock_is_stale),
                }
                
                # Adicionar cobertura aos detalhes (somente se houver estoque confiável)
                if avg_monthly_orders > 0 and current_stock_qty is not None:
                    coverage = current_stock_qty / avg_monthly_orders
                    details['cobertura_meses'] = coverage
                    print(f"   📊 Cobertura Atual: {coverage:.1f} meses")

                recommendations.append({
                    'category': category,
                    'details': details,
                    'forecast_details': {
                        'model_used': 'ml_ensemble',
                        'horizon_days': optimal_horizon,
                        'mape': optimal_mape,
                        'forecast_period': f'{optimal_horizon} dias (XGBoost + LightGBM)',
                        'ensemble_config': config_name,
                        'xgb_weight': config['weights']['xgb'],
                        'lgb_weight': config['weights']['lgb'],
                        'dynamic_horizon': self.use_dynamic_horizon,
                        'horizon_selection': f'Dinâmico baseado em MAPE' if self.use_dynamic_horizon else 'Fixo 14 dias',
                        'lead_time_method': lead_time_method
                    }
                })
                
                print(f"   ✅ Recomendação: {action}")
                if current_stock_qty is not None and stock_gap is not None:
                    print(f"   📉 Estoque Ideal: {int(ideal_stock)} | Atual: {int(current_stock_qty)} | Gap: {int(stock_gap)}")
                else:
                    print(f"   📉 Estoque Ideal: {int(ideal_stock)} | Atual: N/A (stale/ausente)")
                
            except Exception as e:
                print(f"   ❌ Erro: {e}")
                continue
        
        # Ordenar por prioridade da ação
        priority = {
            'Aumentar significativamente': 5,
            'Reduzir significativamente': 4,
            'Aumentar moderadamente': 3,
            'Reduzir moderadamente': 2,
            'Manter': 1
        }
        recommendations.sort(key=lambda r: priority.get(r.get('details', {}).get('action', 'Manter'), 0), reverse=True)
        
        return self._format_recommendations(recommendations)

    def _determine_stock_action(self, variation: float, inventory_turnover: float) -> Tuple[str, str]:
        """
        Determina ação de estoque baseada na variação e giro.
        
        Args:
            variation: Variação prevista em %
            inventory_turnover: Giro de estoque (pedidos/dia)
            
        Returns:
            Tupla com (ação, razão)
        """
        variation_abs = abs(variation)
        action = "Manter"
        
        if variation > 20:
            if inventory_turnover > 2:
                action = "Aumentar significativamente"
                reason = f"Alto crescimento previsto ({variation_abs:.1f}%) com excelente velocidade de vendas"
            else:
                action = "Aumentar moderadamente"
                reason = f"Alto crescimento previsto ({variation_abs:.1f}%) mas velocidade de vendas moderada"
        
        elif variation > 10:
            if inventory_turnover > 1.5:
                action = "Aumentar moderadamente"
                reason = f"Crescimento moderado ({variation_abs:.1f}%) com bom giro"
            else:
                action = "Manter"
                reason = f"Crescimento moderado ({variation_abs:.1f}%) mas giro baixo"
        
        elif variation < -20:
            if inventory_turnover < 1:
                action = "Reduzir significativamente"
                reason = f"Queda significativa ({variation_abs:.1f}%) com baixo giro"
            else:
                action = "Reduzir moderadamente"
                reason = f"Queda significativa ({variation_abs:.1f}%) mas giro ainda bom"
        
        elif variation < -10:
            if inventory_turnover < 1.5:
                action = "Reduzir moderadamente"
                reason = f"Queda moderada ({variation_abs:.1f}%) com giro baixo"
            else:
                action = "Manter"
                reason = f"Queda moderada ({variation_abs:.1f}%) mas giro ainda adequado"
        
        else:  # -10% a +10%
            if inventory_turnover > 3:
                action = "Aumentar moderadamente"
                reason = f"Demanda estável ({variation_abs:.1f}%) mas velocidade de vendas excelente - oportunidade"
            elif inventory_turnover < 1:
                action = "Reduzir moderadamente"
                reason = f"Demanda estável ({variation_abs:.1f}%) mas velocidade de vendas muito baixo"
            else:
                action = "Manter"
                reason = f"Demanda estável ({variation_abs:.1f}%) com giro adequado"
        
        # Retorna tupla (ação, motivo)
        return action, reason

    def _format_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Formata a lista de recomendações para exibição"""
        formatted_recs = []
        for rec in recommendations:
            details = rec.get('details', {})
            forecast_details = rec.get('forecast_details', {})
            ideal_stock_val = details.get('estoque_sugerido_vendas', 0) or 0
            mape_val = forecast_details.get('mape', 0) or 0
            # Calcular intervalo baseado no MAPE: ideal ± (MAPE%)
            if isinstance(ideal_stock_val, (int, float)) and isinstance(mape_val, (int, float)) and mape_val > 0:
                spread = ideal_stock_val * (mape_val / 100.0)
                low = max(0, int(round(ideal_stock_val - spread)))
                high = int(round(ideal_stock_val + spread))
                estoque_sugerido_text = f"Entre {low}-{high} unidades"
            else:
                estoque_sugerido_text = f"{int(ideal_stock_val)} unidades"
            formatted_recs.append({
                'category': rec['category'],
                'action': details.get('action', 'Manter'),
                'variation': f"{details.get('variation', 0):.2f}%",
                'model': forecast_details.get('model_used', 'N/A'),
                'horizon': f"{forecast_details.get('horizon_days', 'N/A')} dias (MAPE: {forecast_details.get('mape', 0):.1f}%)",
                'reason': details.get('reason', 'Demanda estável'),
                'Estoque Sugerido (Vendas)': estoque_sugerido_text,
                'Velocidade de Vendas': f"{details.get('velocidade_vendas', 0):.2f} vendas/dia",
                # Pass-through para o Streamlit usar no card/Excel (estoque atual, gap, as-of etc.)
                'details': details,
                # Adicionar campos numéricos para compatibilidade com download
                'ideal_stock': ideal_stock_val,
                'inventory_turnover': details.get('velocidade_vendas', 0)
            })
        return formatted_recs

def test_ml_ensemble():
    """Testa o sistema de ensemble ML."""
    print("🧪 TESTE DO SISTEMA DE ENSEMBLE ML")
    print("=" * 40)
    
    # Carregar dados
    try:
        df = pd.read_parquet('dados_consolidados_teste/olist_merged_data.parquet')
        print("✅ Dados carregados")
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return
    
    # Filtrar dados recentes
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    cutoff_date = df['order_purchase_timestamp'].max() - pd.Timedelta(days=180)
    filtered_df = df[df['order_purchase_timestamp'] >= cutoff_date].copy()
    
    print(f"📊 Dados filtrados: {len(filtered_df)} registros")
    
    # Testar sistema
    system = MLStockRecommendationSystem()
    recommendations = system.generate_recommendations(filtered_df)
    
    # Mostrar resultados
    print(f"\n📋 RESULTADOS:")
    for i, rec in enumerate(recommendations[:5], 1):
        print(f"\n{i}. {rec['category']}")
        print(f"   Modelo: {rec['model']}")
        print(f"   Configuração: {rec['ensemble_config']}")
        print(f"   MAPE: {rec['mape']:.2f}%")
        print(f"   Variação: {rec['variation']}")
        print(f"   Ação: {rec['action']}")
        print(f"   Estoque Ideal: {rec['Estoque Sugerido (Vendas)']}")

if __name__ == "__main__":
    test_ml_ensemble()
