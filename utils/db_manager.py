"""
DuckDB manager centralizado para consultas SQL rápidas no Streamlit.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import duckdb
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def _as_posix(path: Path) -> str:
    """Garante separador de barra para o DuckDB."""
    return path.resolve().as_posix()


class DuckDBManager:
    """Singleton simples para compartilhar conexão DuckDB no app."""

    _instance: "DuckDBManager | None" = None

    def __new__(cls) -> "DuckDBManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self) -> None:
        # Banco em memória é suficiente para dashboard; persista se precisar de cache em disco.
        self.conn = duckdb.connect(database=":memory:")
        self._register_default_views()

    # --- Registro de datasets -----------------------------------------------------------------
    def _register_default_views(self) -> None:
        """Registra tabelas mais usadas; ignora silenciosamente se não existir."""
        candidates: Dict[str, Iterable[Path]] = {
            "pedidos": [
                Path("data/processed/pedidos.parquet"),
                Path("dados_consolidados/cliente_merged.parquet"),
            ],
            "estoque": [
                Path("data/stock_snapshot/estoque_snapshot_latest.parquet"),
            ],
            "reviews": [
                Path("data/processed/product_reviews_with_categories.parquet"),
                Path("data/processed/product_reviews_individual.parquet"),
            ],
        }

        for view_name, paths in candidates.items():
            path = next((p for p in paths if p.exists()), None)
            if not path:
                logger.info("View %s não registrada (arquivo não encontrado)", view_name)
                continue
            try:
                self.register_parquet_view(view_name, path)
                logger.info("View %s registrada em %s", view_name, path)
            except Exception as exc:
                logger.error("Erro ao registrar view %s: %s", view_name, exc)

    def register_parquet_view(self, view_name: str, file_path: Path) -> None:
        """Cria/atualiza uma view que aponta para um arquivo parquet."""
        posix_path = _as_posix(file_path)
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{posix_path}')"
        )

    # --- Execução -----------------------------------------------------------------------------
    def query(self, sql: str, params: Optional[Iterable] = None) -> pd.DataFrame:
        """Executa SQL e retorna DataFrame pandas (evita cópias extras)."""
        try:
            cur = self.conn.execute(sql, params or [])
            return cur.df()
        except Exception as exc:
            logger.error("Erro ao executar query: %s", exc)
            # Conexão pode ter ficado em estado "closed pending query" (ex.: uso concorrente/rerun).
            # Tenta uma vez com nova instância.
            err_msg = str(exc).lower()
            if "closed" in err_msg or "pending" in err_msg or "unsuccessful" in err_msg:
                try:
                    DuckDBManager._instance = None
                    get_db.clear()
                    new_db = get_db()
                    cur = new_db.conn.execute(sql, params or [])
                    return cur.df()
                except Exception as retry_exc:
                    logger.error("Retry query falhou: %s", retry_exc)
            return pd.DataFrame()


@st.cache_resource
def get_db() -> DuckDBManager:
    """Resource cache do Streamlit para reusar a conexão entre interações."""
    return DuckDBManager()

