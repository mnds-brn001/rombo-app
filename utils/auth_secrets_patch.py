"""
Patch para evitar o bug do Streamlit: 400 'NoneType' object does not support item assignment
na rota /auth/login. Garante que get_secrets_auth_section() retorne um objeto cujo to_dict()
sempre devolve um dict mutável (nunca None), lendo .streamlit/secrets.toml diretamente.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

_PATCH_APPLIED = False


def _load_auth_dict_from_toml() -> dict[str, Any] | None:
    """Carrega a seção [auth] de .streamlit/secrets.toml como dict mutável."""
    try:
        import toml
    except ImportError:
        return None
    base = Path(__file__).resolve().parent.parent
    for name in ("secrets.toml", ".streamlit/secrets.toml"):
        path = base / name if "/" in name else base / ".streamlit" / name
        if path.exists():
            try:
                data = toml.load(path)
                auth = data.get("auth")
                if auth is None or not isinstance(auth, dict):
                    return None
                # Garantir dict mutável; nested também mutáveis
                result = {}
                for k, v in auth.items():
                    if isinstance(v, dict):
                        result[k] = dict(v)
                    else:
                        result[k] = v
                return result
            except Exception:
                return None
    return None


class _AuthSectionWrapper:
    """Wrapper que expõe .get() e .to_dict() retornando sempre dict mutável (nunca None)."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def to_dict(self) -> dict[str, Any]:
        # Retorna cópia mutável para a rota de auth poder fazer setdefault()
        out: dict[str, Any] = {}
        for k, v in self._data.items():
            if isinstance(v, dict):
                out[k] = dict(v)
            else:
                out[k] = v
        return out


def apply_auth_secrets_patch() -> None:
    """
    Aplica o patch em get_secrets_auth_section para evitar 400 NoneType na rota /auth/login.
    A rota de auth importa get_secrets_auth_section ao carregar, então precisamos patchar
    tanto auth_util quanto o módulo starlette_auth_routes que chama a função.
    """
    global _PATCH_APPLIED
    if _PATCH_APPLIED:
        return
    auth_dict = _load_auth_dict_from_toml()
    if auth_dict is None:
        return
    patched_fn = lambda: _AuthSectionWrapper(auth_dict)
    try:
        import streamlit.auth_util as auth_util
        auth_util.get_secrets_auth_section = patched_fn
        # A rota /auth/login usa a referência importada em starlette_auth_routes;
        # sem este patch a rota continua chamando a função original.
        try:
            import streamlit.web.server.starlette.starlette_auth_routes as auth_routes
            auth_routes.get_secrets_auth_section = patched_fn
        except Exception:
            pass
        _PATCH_APPLIED = True
    except Exception:
        pass
