"""
Revenue Forecast (SOTA) - 2-Stage Decomposition & Quantile Regression
=====================================================================

Architecture:
1.  **Point Forecast (Mean):** 2-Stage Decomposition
    -   Stage A: Predict Daily Orders (Volume) using LGBM (Poisson/Tweedie objective).
    -   Stage B: Predict Average Ticket (Price) using LGBM (Regression/Gamma).
    -   Result: Revenue = Orders * Ticket.
    -   Why? Decouples the noise of "traffic/conversion" (Orders) from "spending behavior" (Ticket).

2.  **Uncertainty (Intervals):** Direct Quantile Regression
    -   Model Lower: LGBM Revenue Predictor (alpha=0.05)
    -   Model Upper: LGBM Revenue Predictor (alpha=0.95)
    -   Why? Provides calibrated probability bounds (Coverage ~90-95%) without assuming normal distribution.

3.  **Validation:** Rolling Origin Backtest
    -   Validates over multiple historical windows (e.g., last 3 months) to ensure stability.

4.  **Features:**
    -   Lags, Rolling Means/Stds.
    -   Calendar: DayOfWeek, Month, Holidays, "Double Dates" (Shopee/Retail logic), Black Friday.
    -   Business: Freight Ratio, Discount Ratio, Payment Method Mix (if available).
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import timedelta
from joblib import Parallel, delayed
import holidays

# Check for LightGBM
try:
    from lightgbm import LGBMRegressor
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False

# Check for XGBoost (used as secondary/fallback if needed)
try:
    from xgboost import XGBRegressor
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False


@dataclass
class ForecastResult:
    forecast_df: pd.DataFrame
    metrics: Dict[str, Any]
    model_name: str
    reliability: str


def _safe_mape(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    if not mask.any():
        return 0.0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def _safe_rmse(y_true, y_pred):
    return np.sqrt(np.mean((np.array(y_true) - np.array(y_pred))**2))

def _safe_mae(y_true, y_pred):
    return np.mean(np.abs(np.array(y_true) - np.array(y_pred)))

def _safe_r2(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    if ss_tot == 0:
        return 0.0
    return 1 - (ss_res / ss_tot)


class SOTAFeatureEngineer:
    """Advanced Feature Engineering for Daily Retail Data."""
    
    def __init__(self, country='BR'):
        self.country = country
        # Dynamic Holidays using library
        self.br_holidays = holidays.country_holidays(country)

    def _is_black_friday(self, date: pd.Timestamp) -> int:
        # 4th Friday of November
        if date.month == 11 and date.dayofweek == 4 and 23 <= date.day <= 29:
            return 1
        return 0

    def create_features(self, df: pd.DataFrame, target_col: str, lags: List[int] = None, windows: List[int] = None) -> pd.DataFrame:
        """
        Expects df with ['date', target_col] plus optional exogenous cols.
        Returns df with features and target, NaNs dropped.
        """
        if lags is None:
            lags = [1, 7, 14, 21, 28]
        if windows is None:
            windows = [7, 14, 30]

        df_feat = df.copy()
        df_feat = df_feat.sort_values('date').reset_index(drop=True)
        
        # Datetime Features
        df_feat['day_of_week'] = df_feat['date'].dt.dayofweek
        df_feat['day_of_month'] = df_feat['date'].dt.day
        df_feat['month'] = df_feat['date'].dt.month
        df_feat['is_weekend'] = df_feat['day_of_week'].isin([5, 6]).astype(int)
        
        # Retail Specifics
        df_feat['is_double_date'] = (df_feat['month'] == df_feat['day_of_month']).astype(int)
        # Use dynamic holidays check
        df_feat['is_holiday'] = df_feat['date'].apply(lambda x: 1 if x in self.br_holidays else 0)
        df_feat['is_black_friday'] = df_feat['date'].apply(self._is_black_friday)
        
        # Quarter end/start (business spikes)
        df_feat['is_quarter_start'] = df_feat['date'].dt.is_quarter_start.astype(int)
        
        # Lags & Rolling for TARGET
        for lag in lags:
            df_feat[f'lag_{lag}'] = df_feat[target_col].shift(lag)
            
        for win in windows:
            # Shift by 1 to avoid leakage (features must be known at t-1 for t)
            df_feat[f'rolling_mean_{win}'] = df_feat[target_col].shift(1).rolling(window=win).mean()
            df_feat[f'rolling_std_{win}'] = df_feat[target_col].shift(1).rolling(window=win).std()
            
        # Exogenous Features (if available in df)
        # Lag them as well to ensure availability at forecast time
        exo_cols = ['avg_freight_ratio', 'avg_discount_ratio', 'payment_pix_share']
        for col in exo_cols:
            if col in df.columns:
                # Use Lag 1 of exogenous variables (assuming we don't know future values perfectly, or persistence)
                # Or rolling mean of recent history
                df_feat[f'{col}_lag1'] = df_feat[col].shift(1)
                df_feat[f'{col}_mean7'] = df_feat[col].shift(1).rolling(window=7).mean()

        # Drop NaNs created by lags/rolling
        df_feat = df_feat.dropna().reset_index(drop=True)
        
        return df_feat


class SOTARevenueForecaster:
    """
    State-of-the-Art Revenue Forecaster.
    Implements 2-Stage Decomposition (Orders * Ticket) + Quantile Regression for Intervals.
    """
    
    def __init__(self, horizon_days: int = 14):
        self.horizon_days = horizon_days
        self.fe = SOTAFeatureEngineer()
        
        # Model Store
        self.models = {
            'orders': None,
            'ticket': None,
            'rev_lower': None,
            'rev_upper': None
        }
        self.feature_cols: List[str] = []
        self.exo_cols: List[str] = []
        
        if not LIGHTGBM_AVAILABLE:
            raise ImportError("LightGBM is required for SOTA Forecast.")

    def prepare_data(self, df: pd.DataFrame, date_col='order_purchase_timestamp', 
                     price_col='price', order_id_col='order_id') -> pd.DataFrame:
        """
        Aggregates raw sales data into Daily: Revenue, Orders, Ticket + Business Features.
        When val_col is order-level (valorTotalFinal), aggregates by order_id first so
        the same order is not counted N times (once per item). Fills missing days with 0.
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])

        if 'valorTotalFinal' in df.columns:
            val_col = 'valorTotalFinal'
        elif 'valorTotal' in df.columns:
            val_col = 'valorTotal'
        else:
            val_col = price_col
        df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)

        # Receita por pedido: agregar por order_id primeiro (1 valor por pedido)
        order_level = val_col in ('valorTotalFinal', 'valorTotal') and order_id_col in df.columns
        if order_level:
            order_agg = {date_col: 'first', val_col: 'max'}
            if 'freight_value' in df.columns:
                df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce').fillna(0)
                order_agg['freight_value'] = 'max'
            if 'discount_value' in df.columns:
                df['discount_value'] = pd.to_numeric(df['discount_value'], errors='coerce').fillna(0)
                order_agg['discount_value'] = 'max'
            if 'payment_type' in df.columns:
                df['_is_pix'] = df['payment_type'].astype(str).str.lower().str.contains('pix').astype(int)
                order_agg['_is_pix'] = 'max'
            by_order = df.groupby(order_id_col).agg(order_agg).reset_index()
            by_order['orders'] = 1
            if 'payment_type' in df.columns:
                by_order['is_pix'] = by_order['_is_pix']
                by_order = by_order.drop(columns=['_is_pix'])
            df = by_order.drop(columns=[order_id_col])

        has_freight = 'freight_value' in df.columns
        if has_freight and not order_level:
            df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce').fillna(0)
        has_discount = 'discount_value' in df.columns
        if has_discount and not order_level:
            df['discount_value'] = pd.to_numeric(df['discount_value'], errors='coerce').fillna(0)
        has_payment = 'payment_type' in df.columns
        if has_payment and not order_level:
            df['is_pix'] = df['payment_type'].astype(str).str.lower().str.contains('pix').astype(int)

        if order_level:
            agg_dict = {val_col: 'sum', 'orders': 'sum'}
        else:
            agg_dict = {val_col: 'sum', order_id_col: 'nunique'}
        if has_freight:
            agg_dict['freight_value'] = 'sum'
        if has_discount:
            agg_dict['discount_value'] = 'sum'
        if has_payment:
            agg_dict['is_pix'] = 'mean'

        daily = df.groupby(df[date_col].dt.floor('D')).agg(agg_dict).reset_index()

        rename_map = {date_col: 'date', val_col: 'revenue'}
        if not order_level:
            rename_map[order_id_col] = 'orders'
        if has_payment:
            rename_map['is_pix'] = 'payment_pix_share'
        daily = daily.rename(columns=rename_map)
        
        # Fill missing dates
        full_idx = pd.date_range(start=daily['date'].min(), end=daily['date'].max(), freq='D')
        
        # Columns to fill with 0
        fill_cols = {'revenue': 0, 'orders': 0}
        if has_freight: fill_cols['freight_value'] = 0
        if has_discount: fill_cols['discount_value'] = 0
        if has_payment: fill_cols['payment_pix_share'] = 0
        
        daily = daily.set_index('date').reindex(full_idx).fillna(fill_cols).reset_index().rename(columns={'index': 'date'})
        
        # Feature Engineering: Ratios
        if has_freight:
            # Avoid division by zero
            daily['avg_freight_ratio'] = daily['freight_value'] / daily['revenue'].replace(0, np.nan)
            daily['avg_freight_ratio'] = daily['avg_freight_ratio'].fillna(0) # or ffill?
        
        if has_discount:
            daily['avg_discount_ratio'] = daily['discount_value'] / (daily['revenue'] + daily['discount_value']).replace(0, np.nan)
            daily['avg_discount_ratio'] = daily['avg_discount_ratio'].fillna(0)

        # Identify Exo columns available
        self.exo_cols = []
        if has_freight: self.exo_cols.append('avg_freight_ratio')
        if has_discount: self.exo_cols.append('avg_discount_ratio')
        if has_payment: self.exo_cols.append('payment_pix_share')

        # Calculate Ticket (Safe division)
        daily['ticket_raw'] = daily['revenue'] / daily['orders'].replace(0, np.nan)
        daily['ticket'] = daily['ticket_raw'].ffill().bfill().fillna(0)
        
        return daily

    def _train_lgbm(self, X, y, objective='regression', alpha=None):
        """Helper to train LGBM."""
        params = {
            'n_estimators': 500,
            'learning_rate': 0.03,
            'num_leaves': 31,
            'max_depth': -1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'n_jobs': -1,
            'random_state': 42,
            'verbosity': -1
        }
        
        if objective == 'quantile':
            params['objective'] = 'quantile'
            params['alpha'] = alpha
            params['metric'] = 'quantile'
        elif objective == 'poisson':
            params['objective'] = 'poisson'
            params['metric'] = 'rmse' 
        else:
            params['objective'] = 'regression'
            params['metric'] = 'rmse'
            
        model = LGBMRegressor(**params)
        model.fit(X, y)
        return model

    def fit(self, daily_df: pd.DataFrame):
        """Trains the 4 models."""
        self.daily_history = daily_df.sort_values('date').reset_index(drop=True)
        
        # 1. Feature Engineering (Orders)
        # Pass only date, orders AND exo columns to avoid leakage of ticket/revenue
        cols_orders = ['date', 'orders'] + self.exo_cols
        df_orders = self.fe.create_features(daily_df[cols_orders], 'orders')
        feat_cols = [c for c in df_orders.columns if c not in ['date', 'orders']]
        self.feature_cols = feat_cols # Store feature schema
        
        self.models['orders'] = self._train_lgbm(
            df_orders[feat_cols], df_orders['orders'], objective='poisson'
        )
        
        # 2. Feature Engineering (Ticket)
        cols_ticket = ['date', 'ticket'] + self.exo_cols
        df_ticket = self.fe.create_features(daily_df[cols_ticket], 'ticket')
        self.models['ticket'] = self._train_lgbm(
            df_ticket[feat_cols], df_ticket['ticket'], objective='regression'
        )
        
        # 3. Feature Engineering (Revenue) - For Quantiles
        cols_rev = ['date', 'revenue'] + self.exo_cols
        df_rev = self.fe.create_features(daily_df[cols_rev], 'revenue')
        
        self.models['rev_lower'] = self._train_lgbm(
            df_rev[feat_cols], df_rev['revenue'], objective='quantile', alpha=0.05
        )
        
        self.models['rev_upper'] = self._train_lgbm(
            df_rev[feat_cols], df_rev['revenue'], objective='quantile', alpha=0.95
        )
        
        return self

    def predict(self, horizon_days: int = None) -> pd.DataFrame:
        """Generates recursive forecast for the horizon."""
        if horizon_days is None:
            horizon_days = self.horizon_days
            
        # Recursive prediction setup
        last_date = self.daily_history['date'].max()
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=horizon_days)
        
        # Prepare history containers
        curr_orders_hist = self.daily_history[['date', 'orders'] + self.exo_cols].copy()
        curr_ticket_hist = self.daily_history[['date', 'ticket'] + self.exo_cols].copy()
        curr_rev_hist = self.daily_history[['date', 'revenue'] + self.exo_cols].copy()
        
        # Get last known values of Exo variables to persist (naive forecast for exo)
        last_exo_vals = {col: self.daily_history[col].iloc[-1] for col in self.exo_cols}
        
        preds = []
        
        for date in future_dates:
            # Prepare dummy row for T+1 with naive Exo values
            dummy_row_dict = {'date': date, 'orders': 0, 'ticket': 0, 'revenue': 0}
            dummy_row_dict.update(last_exo_vals)
            dummy_row = pd.DataFrame([dummy_row_dict])
            
            # --- 1. Predict Orders ---
            tmp_orders = pd.concat([curr_orders_hist, dummy_row[['date', 'orders'] + self.exo_cols]], ignore_index=True)
            f_orders = self.fe.create_features(tmp_orders, 'orders')
            X_orders = f_orders.iloc[[-1]][self.feature_cols]
            pred_orders = float(self.models['orders'].predict(X_orders)[0])
            pred_orders = max(0, pred_orders) # ReLU
            
            # --- 2. Predict Ticket ---
            tmp_ticket = pd.concat([curr_ticket_hist, dummy_row[['date', 'ticket'] + self.exo_cols]], ignore_index=True)
            f_ticket = self.fe.create_features(tmp_ticket, 'ticket')
            X_ticket = f_ticket.iloc[[-1]][self.feature_cols]
            pred_ticket = float(self.models['ticket'].predict(X_ticket)[0])
            pred_ticket = max(0, pred_ticket)
            
            # --- 3. Derived Mean Revenue ---
            pred_rev_mean = pred_orders * pred_ticket
            
            # --- 4. Revenue Bounds (Direct Quantiles) ---
            # Update dummy row with predicted mean revenue for lag features consistency
            dummy_row_rev = dummy_row[['date', 'revenue'] + self.exo_cols].copy()
            dummy_row_rev['revenue'] = pred_rev_mean
            
            tmp_rev = pd.concat([curr_rev_hist, dummy_row_rev], ignore_index=True)
            f_rev = self.fe.create_features(tmp_rev, 'revenue')
            X_rev = f_rev.iloc[[-1]][self.feature_cols]
            
            pred_lower = float(self.models['rev_lower'].predict(X_rev)[0])
            pred_upper = float(self.models['rev_upper'].predict(X_rev)[0])
            
            # Post-process bounds
            pred_lower = max(0, pred_lower)
            pred_upper = max(pred_lower, pred_upper)
            if pred_rev_mean < pred_lower: pred_lower = pred_rev_mean * 0.95
            if pred_rev_mean > pred_upper: pred_upper = pred_rev_mean * 1.05
            
            preds.append({
                'date': date,
                'forecast': pred_rev_mean,
                'lower_bound': pred_lower,
                'upper_bound': pred_upper,
                'predicted_orders': pred_orders,
                'predicted_ticket': pred_ticket
            })
            
            # --- 5. Update Histories ---
            # Append the predicted step to history for next iteration recursion
            new_row_base = {'date': date}
            new_row_base.update(last_exo_vals)
            
            new_ord = new_row_base.copy(); new_ord['orders'] = pred_orders
            new_tic = new_row_base.copy(); new_tic['ticket'] = pred_ticket
            new_rev = new_row_base.copy(); new_rev['revenue'] = pred_rev_mean
            
            curr_orders_hist = pd.concat([curr_orders_hist, pd.DataFrame([new_ord])], ignore_index=True)
            curr_ticket_hist = pd.concat([curr_ticket_hist, pd.DataFrame([new_tic])], ignore_index=True)
            curr_rev_hist = pd.concat([curr_rev_hist, pd.DataFrame([new_rev])], ignore_index=True)
            
        return pd.DataFrame(preds)

    def rolling_backtest(self, daily_df: pd.DataFrame, n_splits: int = 3, min_train_size: int = 60) -> pd.DataFrame:
        """
        Performs rolling origin cross-validation.
        Walk-forward: Train on [0..t], Predict [t+1..t+h]. Move t.
        """
        if len(daily_df) < min_train_size + self.horizon_days + n_splits:
            return None
            
        total_len = len(daily_df)
        split_indices = []
        for i in range(n_splits):
            cutoff = total_len - self.horizon_days * (i + 1)
            if cutoff > min_train_size:
                split_indices.append(cutoff)
        
        split_indices = sorted(split_indices)
        
        results = []
        
        # Accumulators for Global Metrics (R2)
        all_y_true = []
        all_y_pred = []
        
        for cutoff in split_indices:
            train = daily_df.iloc[:cutoff].copy()
            test = daily_df.iloc[cutoff:cutoff+self.horizon_days].copy()
            
            # Fit
            self.fit(train)
            
            # Predict
            fc = self.predict(self.horizon_days)
            
            # Evaluate
            y_true = test['revenue'].values
            y_pred = fc['forecast'].values
            y_lower = fc['lower_bound'].values
            y_upper = fc['upper_bound'].values
            
            mape = _safe_mape(y_true, y_pred)
            rmse = _safe_rmse(y_true, y_pred)
            mae = _safe_mae(y_true, y_pred)
            
            # Accumulate for global R2
            all_y_true.extend(y_true)
            all_y_pred.extend(y_pred)
            
            # Coverage
            inside = ((y_true >= y_lower) & (y_true <= y_upper)).mean() * 100
            
            results.append({
                'cutoff_date': train['date'].max(),
                'mape': mape,
                'rmse': rmse,
                'mae': mae,
                'coverage': inside
            })
            
        df_res = pd.DataFrame(results)
        
        # Calculate Global R2 over all backtest windows
        if len(all_y_true) > 0:
            global_r2 = _safe_r2(all_y_true, all_y_pred)
            # Add as a constant column or handle in orchestrator
            df_res.attrs['global_r2'] = global_r2
            
        return df_res

def orchestrate_sota_forecast(df_sales: pd.DataFrame, horizon_days: int = 14) -> ForecastResult:
    """
    Main entrypoint for the SOTA pipeline.
    """
    try:
        forecaster = SOTARevenueForecaster(horizon_days=horizon_days)
        
        # 1. Prep Data
        daily = forecaster.prepare_data(df_sales)
        if len(daily) < 30:
             return ForecastResult(pd.DataFrame(), {}, "SOTA-Error", "N/A")

        # 2. Backtest (Rolling) for Metrics
        backtest_res = forecaster.rolling_backtest(daily, n_splits=3)
        
        metrics = {}
        reliability = "Indeterminada"
        
        if backtest_res is not None and not backtest_res.empty:
            metrics = {
                'mape': backtest_res['mape'].mean(),
                'rmse': backtest_res['rmse'].mean(),
                'mae': backtest_res['mae'].mean(),
                'coverage': backtest_res['coverage'].mean(),
                'r_squared': backtest_res.attrs.get('global_r2', 0.0)
            }
            
            # Reliability Logic
            m = metrics['mape']
            if m < 15: reliability = "Alta (85-95%)"
            elif m < 25: reliability = "Boa (75-85%)"
            elif m < 40: reliability = "Média (60-75%)"
            else: reliability = "Baixa (40-60%)"
            
            # Inject Reliability into metrics dict for UI compatibility
            metrics['reliability'] = reliability
        else:
            # Fallback
            reliability = "Dados Insuficientes"
            metrics['reliability'] = reliability

        # 3. Final Retrain & Predict
        forecaster.fit(daily)
        forecast_df = forecaster.predict(horizon_days)
        
        # Add metadata
        metrics['model'] = 'SOTA-2Stage-Quantile'
        
        return ForecastResult(
            forecast_df=forecast_df,
            metrics=metrics,
            model_name="sota_2stage",
            reliability=reliability
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return ForecastResult(pd.DataFrame(), {'error': str(e)}, "error", "N/A")
