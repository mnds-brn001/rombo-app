"""
Pedidos detalhados (uma linha por item): normaliza e grava order_items.parquet.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from dados_cliente.adaptador_cosmeticos import normalize_column_names

# Colunas canônicas de saída
_OUT_COLS = ["order_id", "product_id", "product_qty", "line_price", "order_purchase_timestamp"]


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def process_order_items_df(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df.copy())

    if "order_id" not in df.columns:
        raise ValueError("Falta identificador de pedido (order_id / pedido_id / similar).")

    pid_col = None
    for c in ("product_id", "sku", "codigo", "produto_id", "product_sku"):
        if c in df.columns:
            pid_col = c
            break
    if pid_col is None:
        raise ValueError("Falta identificador de produto/SKU (product_id / sku / codigo / similar).")

    qty_col = None
    for c in ("product_qty", "quantidade", "qtd", "qty", "product_quantity"):
        if c in df.columns:
            qty_col = c
            break
    qty = _to_num(df[qty_col]) if qty_col else pd.Series(1.0, index=df.index)

    price_col = None
    for c in ("price", "valor_item", "valor_unitario", "valorUnitario", "preco", "line_price", "valor"):
        if c in df.columns:
            price_col = c
            break
    line_price = _to_num(df[price_col]) if price_col else pd.Series(0.0, index=df.index)

    ts = None
    if "order_purchase_timestamp" in df.columns:
        ts = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    out = pd.DataFrame(
        {
            "order_id": df["order_id"].astype("string").str.strip(),
            "product_id": df[pid_col].astype("string").str.strip(),
            "product_qty": qty,
            "line_price": line_price,
            "order_purchase_timestamp": ts,
        }
    )
    return out


def process_order_items_csv(csv_path: Path, output_parquet: Path) -> tuple[bool, str]:
    try:
        df = pd.read_csv(csv_path, sep=None, engine="python", encoding="utf-8-sig", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, sep=None, engine="python", encoding="latin1", low_memory=False)

    out = process_order_items_df(df)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_parquet, index=False)
    return True, f"OK: {len(out):,} linhas de itens."
