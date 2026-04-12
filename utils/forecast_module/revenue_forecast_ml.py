"""
Revenue Forecast (ML) - Specialized Module
=========================================

Goal:
- Forecast DAILY revenue (price / valorTotalFinal) for horizons 7, 14, 21 days
- Provide robust, objective backtesting and a lean model set:
  - Baseline (seasonal naive)
  - LightGBM
  - XGBoost
  - Optional LSTM (experimental; only if TensorFlow available)

Design principles:
- Deterministic, reproducible, no silent magic
- Time-series aware evaluation (walk-forward / rolling origin)
- Robust preprocessing for sparse daily revenue (fill missing days with 0)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import holidays

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    if hasattr(dt, "dt") and dt.dt.tz is not None:
        dt = dt.dt.tz_localize(None)
    return dt


def _infer_revenue_column(df: pd.DataFrame, target_col: str = "price") -> str:
    """Prefer order-level revenue when present and non-zero: valorTotalFinal, then valorTotal, else price."""
    if target_col != "price":
        return target_col
    for col in ("valorTotalFinal", "valorTotal"):
        if col in df.columns:
            v = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if float(v.sum()) > 0:
                return col
    return "price"


@dataclass
class BacktestResult:
    mape: float
    mae: float
    rmse: float
    r2: float
    coverage: float
    n: int


def _mape(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-9) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    denom = np.maximum(np.abs(y_true), eps)
    return float(np.mean(np.abs(y_true - y_pred) / denom) * 100.0)


def _mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float))))


def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    err = np.asarray(y_true, dtype=float) - np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean(err ** 2)))


def _r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    if ss_tot <= 0:
        return 0.0
    return float(1.0 - ss_res / ss_tot)


def prepare_daily_revenue_series(
    df: pd.DataFrame,
    date_col: str = "order_purchase_timestamp",
    target_col: str = "price",
    fill_missing_days: bool = True,
    drop_cancelled: bool = True,
) -> pd.DataFrame:
    """Aggregate to daily revenue series with optional missing-day fill (0 revenue)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "value"])

    dfw = df.copy()

    if drop_cancelled:
        if "funnel_cancelled" in dfw.columns:
            dfw = dfw[dfw["funnel_cancelled"] != 1]
        elif "pedido_cancelado" in dfw.columns:
            dfw = dfw[dfw["pedido_cancelado"] != 1]

    dfw[date_col] = _safe_to_datetime(dfw[date_col])
    dfw = dfw[dfw[date_col].notna()].copy()
    if dfw.empty:
        return pd.DataFrame(columns=["date", "value"])

    revenue_col = _infer_revenue_column(dfw, target_col=target_col)
    dfw[revenue_col] = pd.to_numeric(dfw[revenue_col], errors="coerce").fillna(0.0)

    # Quando a receita é por pedido (valorTotalFinal/valorTotal), agregar por order_id primeiro
    # para não contar o mesmo pedido N vezes (uma por item). Senão a série fica inflada.
    order_level_revenue = (
        revenue_col in ("valorTotalFinal", "valorTotal")
        and "order_id" in dfw.columns
    )
    if order_level_revenue:
        order_agg = {date_col: "first", revenue_col: "max"}
        if "freight_value" in dfw.columns:
            dfw["freight_value"] = pd.to_numeric(dfw["freight_value"], errors="coerce").fillna(0.0)
            order_agg["freight_value"] = "max"
        if "discount_value" in dfw.columns:
            dfw["discount_value"] = pd.to_numeric(dfw["discount_value"], errors="coerce").fillna(0.0)
            order_agg["discount_value"] = "max"
        if "payment_type" in dfw.columns:
            dfw["_is_pix"] = dfw["payment_type"].astype(str).str.lower().str.contains("pix").astype(int)
            order_agg["_is_pix"] = "max"
        by_order = dfw.groupby("order_id").agg(order_agg).reset_index()
        if "payment_type" in dfw.columns:
            by_order["is_pix"] = by_order["_is_pix"]
            by_order = by_order.drop(columns=["_is_pix"])
        dfw = by_order

    has_freight = "freight_value" in dfw.columns
    if has_freight and not order_level_revenue:
        dfw["freight_value"] = pd.to_numeric(dfw["freight_value"], errors="coerce").fillna(0.0)
    has_discount = "discount_value" in dfw.columns
    if has_discount and not order_level_revenue:
        dfw["discount_value"] = pd.to_numeric(dfw["discount_value"], errors="coerce").fillna(0.0)
    has_payment = "is_pix" in dfw.columns or "payment_type" in dfw.columns
    if has_payment and not order_level_revenue:
        dfw["is_pix"] = dfw["payment_type"].astype(str).str.lower().str.contains("pix").astype(int)

    agg_dict = {revenue_col: "sum"}
    if has_freight:
        agg_dict["freight_value"] = "sum"
    if has_discount:
        agg_dict["discount_value"] = "sum"
    if has_payment:
        agg_dict["is_pix"] = "mean"

    daily = (
        dfw.groupby(dfw[date_col].dt.floor("D"))
        .agg(agg_dict)
        .reset_index()
        .rename(columns={date_col: "date", revenue_col: "value"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    
    if has_payment:
        daily = daily.rename(columns={"is_pix": "payment_pix_share"})

    # Ensure continuous daily index (important for seasonal naive and lags)
    if fill_missing_days and not daily.empty:
        full_idx = pd.date_range(start=daily["date"].min(), end=daily["date"].max(), freq="D")
        # Fill dictionary
        fill_vals = {"value": 0.0}
        if has_freight: fill_vals["freight_value"] = 0.0
        if has_discount: fill_vals["discount_value"] = 0.0
        if has_payment: fill_vals["payment_pix_share"] = 0.0
        
        daily = daily.set_index("date").reindex(full_idx).fillna(fill_vals).reset_index()
        daily = daily.rename(columns={"index": "date"})

    daily["value"] = pd.to_numeric(daily["value"], errors="coerce").fillna(0.0).astype(float)
    
    # Calculate Ratios
    if has_freight:
        daily["avg_freight_ratio"] = daily["freight_value"] / daily["value"].replace(0, np.nan)
        daily["avg_freight_ratio"] = daily["avg_freight_ratio"].fillna(0.0)
        
    if has_discount:
        daily["avg_discount_ratio"] = daily["discount_value"] / (daily["value"] + daily["discount_value"]).replace(0, np.nan)
        daily["avg_discount_ratio"] = daily["avg_discount_ratio"].fillna(0.0)
        
    return daily


def prepare_daily_cancel_rate_series(
    df: pd.DataFrame,
    date_col: str = "order_purchase_timestamp",
    fill_missing_days: bool = True,
) -> pd.DataFrame:
    """
    Agrega por dia a taxa de cancelamento (pedidos cancelados / total de pedidos * 100).
    Retorna DataFrame com colunas 'date' e 'value' (value = taxa % 0–100), compatível com
    RevenueForecastModel (e.g. LightGBM/Ensemble com use_log1p=False) para previsão.
    """
    if df is None or df.empty or "order_id" not in df.columns:
        return pd.DataFrame(columns=["date", "value"])

    dfw = df.copy()
    dfw[date_col] = _safe_to_datetime(dfw[date_col])
    dfw = dfw[dfw[date_col].notna()].copy()
    if dfw.empty:
        return pd.DataFrame(columns=["date", "value"])

    if "pedido_cancelado" in dfw.columns:
        dfw["_cancelled"] = (pd.to_numeric(dfw["pedido_cancelado"], errors="coerce").fillna(0) != 0).astype(int)
    elif "order_status" in dfw.columns:
        dfw["_cancelled"] = dfw["order_status"].astype(str).str.lower().str.strip().isin(
            ["cancelado", "canceled", "cancelled"]
        ).astype(int)
    else:
        return pd.DataFrame(columns=["date", "value"])

    by_order = dfw.groupby("order_id").agg(
        date=(date_col, "first"),
        _cancelled=("_cancelled", "max"),
    ).reset_index()
    by_order["date"] = pd.to_datetime(by_order["date"]).dt.floor("D")
    by_day = by_order.groupby("date").agg(
        total=("order_id", "count"),
        cancelled=("_cancelled", "sum"),
    ).reset_index()
    by_day["value"] = np.where(
        by_day["total"] > 0,
        by_day["cancelled"].astype(float) / by_day["total"] * 100.0,
        0.0,
    )
    daily = by_day[["date", "value"]].sort_values("date").reset_index(drop=True)

    if fill_missing_days and not daily.empty:
        full_idx = pd.date_range(start=daily["date"].min(), end=daily["date"].max(), freq="D")
        daily = daily.set_index("date").reindex(full_idx).fillna({"value": 0.0}).reset_index()
        daily = daily.rename(columns={"index": "date"})
    daily["value"] = pd.to_numeric(daily["value"], errors="coerce").fillna(0.0).clip(0.0, 100.0)
    return daily


class RevenueForecastModel:
    key: str = "base"

    def fit(self, daily: pd.DataFrame) -> "RevenueForecastModel":
        raise NotImplementedError

    def predict(self, horizon_days: int) -> pd.DataFrame:
        raise NotImplementedError

    def backtest(self, daily: pd.DataFrame, horizon_days: int, min_train_days: int = 60) -> BacktestResult:
        """Rolling-origin backtest (single split, last horizon) for now (simple + fast)."""
        if daily is None or daily.empty:
            return BacktestResult(mape=float("nan"), mae=float("nan"), rmse=float("nan"), r2=0.0, coverage=float("nan"), n=0)

        daily = daily.sort_values("date").reset_index(drop=True)
        if len(daily) < (min_train_days + horizon_days):
            return BacktestResult(mape=float("nan"), mae=float("nan"), rmse=float("nan"), r2=0.0, coverage=float("nan"), n=int(len(daily)))

        train = daily.iloc[:-horizon_days].copy()
        test = daily.iloc[-horizon_days:].copy()
        self.fit(train)
        pred = self.predict(horizon_days)
        y_true = test["value"].to_numpy()
        y_pred = pred["forecast"].to_numpy()

        coverage = float("nan")
        try:
            if "lower_bound" in pred.columns and "upper_bound" in pred.columns:
                lo = pd.to_numeric(pred["lower_bound"], errors="coerce").to_numpy(dtype=float)
                hi = pd.to_numeric(pred["upper_bound"], errors="coerce").to_numpy(dtype=float)
                yt = np.asarray(y_true, dtype=float)
                ok = np.isfinite(lo) & np.isfinite(hi) & np.isfinite(yt)
                if ok.any():
                    inside = (yt[ok] >= lo[ok]) & (yt[ok] <= hi[ok])
                    coverage = float(np.mean(inside) * 100.0)
        except Exception:
            coverage = float("nan")

        return BacktestResult(
            mape=_mape(y_true, y_pred),
            mae=_mae(y_true, y_pred),
            rmse=_rmse(y_true, y_pred),
            r2=_r2(y_true, y_pred),
            coverage=coverage,
            n=int(len(y_true)),
        )


class SeasonalNaiveBaseline(RevenueForecastModel):
    """Seasonal naive baseline: repeats the last 7 days pattern (better than flat mean)."""

    key = "baseline"

    def __init__(self, season_length: int = 7):
        self.season_length = int(season_length)
        self._train: Optional[pd.DataFrame] = None

    def fit(self, daily: pd.DataFrame) -> "SeasonalNaiveBaseline":
        self._train = daily.sort_values("date").reset_index(drop=True)
        return self

    def predict(self, horizon_days: int) -> pd.DataFrame:
        if self._train is None or self._train.empty:
            return pd.DataFrame(columns=["date", "forecast", "lower_bound", "upper_bound"])
        horizon_days = int(horizon_days)
        last_date = pd.to_datetime(self._train["date"]).max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=horizon_days, freq="D")

        hist = self._train["value"].to_numpy(dtype=float)
        if len(hist) < self.season_length:
            base = float(np.mean(hist)) if len(hist) else 0.0
            fc = np.array([base] * horizon_days, dtype=float)
        else:
            pattern = hist[-self.season_length :]
            reps = int(np.ceil(horizon_days / self.season_length))
            fc = np.tile(pattern, reps)[:horizon_days]

        resid_std = float(np.std(hist[-max(28, self.season_length) :])) if len(hist) else 0.0
        lower = np.clip(fc - 1.96 * resid_std, 0, None)
        upper = fc + 1.96 * resid_std

        return pd.DataFrame({"date": future_dates, "forecast": fc, "lower_bound": lower, "upper_bound": upper})


def _calendar_features(dates: pd.DatetimeIndex) -> pd.DataFrame:
    df = pd.DataFrame({"date": pd.to_datetime(dates)})
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_month"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    # Double date (11/11 etc.)
    df["is_double_date"] = (df["month"] == df["day_of_month"]).astype(int)
    # Black Friday approx: 4th Friday of November (23..29)
    df["is_black_friday"] = ((df["month"] == 11) & (df["day_of_week"] == 4) & (df["day_of_month"].between(23, 29))).astype(int)
    
    # Dynamic Holidays (BR)
    br_holidays = holidays.country_holidays('BR')
    df["is_holiday"] = df["date"].apply(lambda x: 1 if x in br_holidays else 0)
    
    return df


class _TreeGBMRevenueBase(RevenueForecastModel):
    key = "tree"

    def __init__(self, lag: int = 14, use_log1p: bool = True):
        self.lag = int(lag)
        self.use_log1p = bool(use_log1p)
        self.model = None
        self.feature_cols: List[str] = []
        self._train_daily: Optional[pd.DataFrame] = None

    def _build_supervised(self, daily: pd.DataFrame) -> pd.DataFrame:
        df = daily.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        # Target transform
        y = df["value"].astype(float)
        if self.use_log1p:
            y = np.log1p(np.clip(y, 0, None))
        df["_y"] = y

        # Lags + rolling
        for i in range(1, self.lag + 1):
            df[f"lag_{i}"] = df["_y"].shift(i)
        for w in [7, 14, 30]:
            df[f"ma_{w}"] = df["_y"].rolling(w).mean()
            df[f"std_{w}"] = df["_y"].rolling(w).std()

        # Exogenous Business Features (Lagged to avoid leakage)
        exo_cols = ["avg_freight_ratio", "avg_discount_ratio", "payment_pix_share"]
        for col in exo_cols:
            if col in df.columns:
                # Use lag 1 (yesterday's known ratio) as predictor for today
                df[f"{col}_lag1"] = df[col].shift(1)
                # Rolling mean of recent history
                df[f"{col}_mean7"] = df[col].shift(1).rolling(7).mean()

        df["trend"] = np.arange(len(df), dtype=float)
        cal = _calendar_features(pd.DatetimeIndex(df["date"]))
        df = df.merge(cal, on="date", how="left")

        df = df.dropna().reset_index(drop=True)
        return df

    def fit(self, daily: pd.DataFrame) -> "_TreeGBMRevenueBase":
        self._train_daily = daily.sort_values("date").reset_index(drop=True)
        sup = self._build_supervised(self._train_daily)
        if sup.empty or len(sup) < 30:
            raise ValueError("Dados insuficientes para treinar modelo GBM de receita (mínimo ~30 pontos após lags).")

        # Exclude raw exo columns from features (we only want lags)
        raw_exo_cols = {
            "freight_value", "avg_freight_ratio",
            "discount_value", "avg_discount_ratio",
            "payment_pix_share", "is_pix"
        }
        
        # Features: all columns except date, target, and raw exo cols
        self.feature_cols = [
            c for c in sup.columns 
            if c not in {"date", "value", "_y"} and c not in raw_exo_cols
        ]
        
        X = sup[self.feature_cols]
        y = sup["_y"]

        self._fit_model(X, y)
        return self

    def _fit_model(self, X: pd.DataFrame, y: pd.Series) -> None:
        raise NotImplementedError

    def _predict_one_step(self, feature_row: pd.DataFrame) -> float:
        pred = float(self.model.predict(feature_row)[0])
        return pred

    def predict(self, horizon_days: int) -> pd.DataFrame:
        if self._train_daily is None or self.model is None:
            raise RuntimeError("Modelo não treinado. Chame fit() antes.")
        horizon_days = int(horizon_days)
        last_date = pd.to_datetime(self._train_daily["date"]).max()
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=horizon_days, freq="D")

        # Work in transformed space to keep stability
        hist = self._train_daily.copy()
        hist = hist.sort_values("date").reset_index(drop=True)
        y = hist["value"].astype(float).to_numpy()
        y_t = np.log1p(np.clip(y, 0, None)) if self.use_log1p else y
        hist_t = list(y_t.tolist())

        # Get last known exo values for naive forecasting of features
        exo_cols = ["avg_freight_ratio", "avg_discount_ratio", "payment_pix_share"]
        last_exo = {}
        for col in exo_cols:
            if col in hist.columns:
                last_exo[col] = hist[col].iloc[-1]

        preds_t: List[float] = []
        for i, d in enumerate(future_dates):
            # Build a single-row feature set from history
            row = {"date": pd.to_datetime(d)}
            # Lags in transformed space
            for j in range(1, self.lag + 1):
                row[f"lag_{j}"] = hist_t[-j] if len(hist_t) >= j else hist_t[0]
            # Rolling
            for w in [7, 14, 30]:
                tail = hist_t[-w:] if len(hist_t) >= w else hist_t
                row[f"ma_{w}"] = float(np.mean(tail)) if len(tail) else 0.0
                row[f"std_{w}"] = float(np.std(tail)) if len(tail) else 0.0
            row["trend"] = float(len(hist_t) + i)

            # Exo Features (Naive extrapolation)
            for col in exo_cols:
                if col in last_exo:
                    row[f"{col}_lag1"] = last_exo[col]
                    row[f"{col}_mean7"] = last_exo[col] # Naive

            cal = _calendar_features(pd.DatetimeIndex([d])).iloc[0].to_dict()
            row.update({k: cal[k] for k in ["day_of_week", "day_of_month", "month", "quarter", "is_weekend", "is_double_date", "is_black_friday", "is_holiday"]})

            X_row = pd.DataFrame([row])[self.feature_cols]
            yhat_t = self._predict_one_step(X_row)
            # Safety clamp for transformed space
            if not np.isfinite(yhat_t):
                yhat_t = hist_t[-1] if len(hist_t) else 0.0
            preds_t.append(yhat_t)
            hist_t.append(yhat_t)

        # Inverse transform
        preds = np.expm1(np.array(preds_t)) if self.use_log1p else np.array(preds_t)
        preds = np.clip(preds, 0, None)

        # Confidence from in-sample residuals (simple, fast)
        try:
            sup = self._build_supervised(self._train_daily)
            X = sup[self.feature_cols]
            y_true_t = sup["_y"].to_numpy(dtype=float)
            y_pred_t = np.asarray(self.model.predict(X), dtype=float)
            resid_std = float(np.std(y_true_t - y_pred_t))
        except Exception:
            resid_std = 0.0

        # Convert residual std from transformed space to original approx using delta method (rough)
        # We'll approximate by scaling around forecast level.
        if self.use_log1p:
            # std in log1p space -> multiplicative band; approximate per-step
            lower = np.clip(np.expm1(np.array(preds_t) - 1.96 * resid_std), 0, None)
            upper = np.expm1(np.array(preds_t) + 1.96 * resid_std)
        else:
            lower = np.clip(preds - 1.96 * resid_std, 0, None)
            upper = preds + 1.96 * resid_std

        return pd.DataFrame({"date": future_dates, "forecast": preds, "lower_bound": lower, "upper_bound": upper})


class LightGBMRevenueForecast(_TreeGBMRevenueBase):
    key = "lightgbm"

    def __init__(self, lag: int = 14, use_log1p: bool = True, params: Optional[Dict[str, Any]] = None):
        super().__init__(lag=lag, use_log1p=use_log1p)
        self.params = params or {}

    def _fit_model(self, X: pd.DataFrame, y: pd.Series) -> None:
        from lightgbm import LGBMRegressor  # local import

        p = {
            "n_estimators": int(self.params.get("n_estimators", 400)),
            "learning_rate": float(self.params.get("learning_rate", 0.03)),
            "max_depth": int(self.params.get("max_depth", 5)),
            "num_leaves": int(self.params.get("num_leaves", 31)),
            "subsample": float(self.params.get("subsample", 0.9)),
            "colsample_bytree": float(self.params.get("colsample_bytree", 0.9)),
            "min_child_samples": int(self.params.get("min_child_samples", 20)),
            "random_state": 42,
            "verbosity": -1,
            "n_jobs": -1,
        }
        self.model = LGBMRegressor(**p)
        self.model.fit(X, y)


class XGBoostRevenueForecast(_TreeGBMRevenueBase):
    key = "xgboost"

    def __init__(self, lag: int = 14, use_log1p: bool = True, params: Optional[Dict[str, Any]] = None):
        super().__init__(lag=lag, use_log1p=use_log1p)
        self.params = params or {}

    def _fit_model(self, X: pd.DataFrame, y: pd.Series) -> None:
        from xgboost import XGBRegressor  # local import

        p = {
            "n_estimators": int(self.params.get("n_estimators", 500)),
            "learning_rate": float(self.params.get("learning_rate", 0.03)),
            "max_depth": int(self.params.get("max_depth", 5)),
            "subsample": float(self.params.get("subsample", 0.9)),
            "colsample_bytree": float(self.params.get("colsample_bytree", 0.9)),
            "reg_alpha": float(self.params.get("reg_alpha", 0.1)),
            "reg_lambda": float(self.params.get("reg_lambda", 1.0)),
            "random_state": 42,
            "n_jobs": -1,
            "verbosity": 0,
        }
        self.model = XGBRegressor(**p)
        self.model.fit(X, y)


def is_tensorflow_available() -> bool:
    try:
        import tensorflow  # noqa: F401
        return True
    except Exception:
        return False


class RevenueEnsemble:
    """Lean ensemble specialized for revenue (or other daily series): baseline + (lgb, xgb), optional lstm."""

    def __init__(self, include_lstm: bool = False, use_log1p: bool = True):
        self.include_lstm = bool(include_lstm and is_tensorflow_available())
        self.use_log1p = bool(use_log1p)
        self.models: Dict[str, RevenueForecastModel] = {
            "baseline": SeasonalNaiveBaseline(season_length=7),
            "lightgbm": LightGBMRevenueForecast(use_log1p=self.use_log1p),
            "xgboost": XGBoostRevenueForecast(use_log1p=self.use_log1p),
        }
        # LSTM is kept out of this initial implementation intentionally; we will decide after objective eval.
        self.weights: Dict[str, float] = {"lightgbm": 0.65, "xgboost": 0.35}
        self._daily: Optional[pd.DataFrame] = None

    def fit(self, daily: pd.DataFrame) -> "RevenueEnsemble":
        self._daily = daily.copy()
        # Fit ML models; baseline has no cost
        self.models["baseline"].fit(daily)
        self.models["lightgbm"].fit(daily)
        self.models["xgboost"].fit(daily)
        return self

    def tune_weights(self, daily: pd.DataFrame, horizon_days: int) -> Dict[str, Any]:
        """Compute simple weights inversely proportional to MAPE on last-horizon backtest.

        IMPORTANT: backtest() calls fit() internally. If we run it on the same model instances used
        for production prediction, it will *overwrite* their trained state with a truncated train split,
        causing forecasts to start earlier (e.g., 30 days before the real last date). To avoid this,
        we backtest on fresh model instances (clones) and keep the fitted production models intact.
        """
        # Clone models for backtest to avoid mutating self.models[*]
        lgb_ref: LightGBMRevenueForecast = self.models["lightgbm"]  # type: ignore[assignment]
        xgb_ref: XGBoostRevenueForecast = self.models["xgboost"]  # type: ignore[assignment]

        bt_lgb_model = LightGBMRevenueForecast(
            lag=getattr(lgb_ref, "lag", 14),
            use_log1p=getattr(lgb_ref, "use_log1p", True),
            params=getattr(lgb_ref, "params", None),
        )
        bt_xgb_model = XGBoostRevenueForecast(
            lag=getattr(xgb_ref, "lag", 14),
            use_log1p=getattr(xgb_ref, "use_log1p", True),
            params=getattr(xgb_ref, "params", None),
        )

        bt_lgb = bt_lgb_model.backtest(daily, horizon_days=horizon_days)
        bt_xgb = bt_xgb_model.backtest(daily, horizon_days=horizon_days)
        # If either NaN, keep defaults
        if not np.isfinite(bt_lgb.mape) or not np.isfinite(bt_xgb.mape):
            return {"weights": dict(self.weights), "backtest": {"lightgbm": bt_lgb, "xgboost": bt_xgb}}
        # Inverse mape weighting, with floor
        inv_lgb = 1.0 / max(bt_lgb.mape, 1e-6)
        inv_xgb = 1.0 / max(bt_xgb.mape, 1e-6)
        w_lgb = inv_lgb / (inv_lgb + inv_xgb)
        w_xgb = 1.0 - w_lgb
        # Clamp to avoid degeneracy
        w_lgb = float(np.clip(w_lgb, 0.2, 0.8))
        w_xgb = float(1.0 - w_lgb)
        self.weights = {"lightgbm": w_lgb, "xgboost": w_xgb}
        return {"weights": dict(self.weights), "backtest": {"lightgbm": bt_lgb, "xgboost": bt_xgb}}

    def predict(self, horizon_days: int, tune: bool = True) -> Dict[str, Any]:
        if self._daily is None:
            raise RuntimeError("Chame fit(daily) antes.")
        daily = self._daily
        horizon_days = int(horizon_days)

        meta = {}
        if tune:
            meta = self.tune_weights(daily, horizon_days=horizon_days)

        f_lgb = self.models["lightgbm"].predict(horizon_days)
        f_xgb = self.models["xgboost"].predict(horizon_days)
        w_lgb = float(self.weights.get("lightgbm", 0.65))
        w_xgb = float(self.weights.get("xgboost", 0.35))

        combined = f_lgb.copy()
        combined["forecast"] = w_lgb * f_lgb["forecast"].to_numpy() + w_xgb * f_xgb["forecast"].to_numpy()
        # Conservative CI: blend bounds
        combined["lower_bound"] = w_lgb * f_lgb["lower_bound"].to_numpy() + w_xgb * f_xgb["lower_bound"].to_numpy()
        combined["upper_bound"] = w_lgb * f_lgb["upper_bound"].to_numpy() + w_xgb * f_xgb["upper_bound"].to_numpy()
        combined["lower_bound"] = combined["lower_bound"].clip(lower=0)

        return {
            "forecast_df": combined,
            "model_used": "revenue_ensemble",
            "weights": dict(self.weights),
            "meta": meta,
        }


def forecast_cancel_rate(
    daily_cancel: pd.DataFrame,
    horizon_days: int,
    use_ensemble: bool = True,
    tune_weights: bool = False,
) -> pd.DataFrame:
    """
    Previsão da taxa de cancelamento diária (%) usando o mesmo pipeline ML do ensemble
    (LGBM + XGBoost) ou apenas LGBM, com use_log1p=False e previsões limitadas a [0, 100]%.
    daily_cancel: DataFrame com colunas 'date' e 'value' (taxa %), ex.: de prepare_daily_cancel_rate_series().
    Retorna DataFrame com date, forecast, lower_bound, upper_bound (todos em %).
    """
    if daily_cancel is None or daily_cancel.empty:
        return pd.DataFrame(columns=["date", "forecast", "lower_bound", "upper_bound"])
    daily_cancel = daily_cancel.copy()
    if "value" not in daily_cancel.columns or "date" not in daily_cancel.columns:
        return pd.DataFrame(columns=["date", "forecast", "lower_bound", "upper_bound"])
    daily_cancel["value"] = pd.to_numeric(daily_cancel["value"], errors="coerce").fillna(0.0).clip(0.0, 100.0)
    if len(daily_cancel) < 30:
        return pd.DataFrame(columns=["date", "forecast", "lower_bound", "upper_bound"])

    horizon_days = int(horizon_days)
    if use_ensemble:
        ensemble = RevenueEnsemble(use_log1p=False)
        ensemble.fit(daily_cancel)
        out = ensemble.predict(horizon_days, tune=tune_weights)
        pred = out["forecast_df"]
    else:
        model = LightGBMRevenueForecast(use_log1p=False)
        model.fit(daily_cancel)
        pred = model.predict(horizon_days)

    pred = pred.copy()
    pred["forecast"] = pred["forecast"].clip(0.0, 100.0)
    if "lower_bound" in pred.columns:
        pred["lower_bound"] = pred["lower_bound"].clip(0.0, 100.0)
    if "upper_bound" in pred.columns:
        pred["upper_bound"] = pred["upper_bound"].clip(0.0, 100.0)
    return pred

