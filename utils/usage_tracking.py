"""
Rastreamento de uso do app Streamlit (última vez online).

Registra cada acesso no Supabase (tabela app_usage_log) para você ver
quem acessou e quando, sem depender do analytics do Streamlit Cloud.

- Invisível: nenhum texto, cookie banner ou UI indica que há tracking.
- Anti-fake: throttle de 20 min — não fica atualizando "última vez online"
  com a aba só aberta (AFK); só grava quando há run (carregou ou interagiu).
- Se OIDC estiver configurado no Streamlit Cloud e o runtime expuser st.user: viewer_id = email.
- Se o app usar login Supabase (ENABLE_LOGIN_GATE=True): viewer_id = email após login.
- Caso contrário: viewer_id = session_id (único por sessão do navegador).
  Se o Analytics do Streamlit mostrar email mas o app só vir session_xxx, ative o portão
  de login (Supabase) no app para que o email seja gravado no app_usage_log.

Requer: SUPABASE_DB_URL em st.secrets ou variável de ambiente.
Tabela: public.app_usage_log (ver scripts/supabase_app_usage_schema.sql).
"""
from __future__ import annotations
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import streamlit as st
except ImportError:
    st = None

# Throttle 20 min: não conta "aba aberta sem interação" como online contínuo
_THROTTLE_MINUTES = 20
# Só gravar a partir do 2º run: health checks / keep-alive fazem 1 request = 1 run; usuário real gera rerun ao interagir
_MIN_RUNS_BEFORE_LOG = 2
_SESSION_KEY_LAST_LOGGED = "_usage_tracking_last_logged"
_SESSION_KEY_LAST_VIEWER_ID = "_usage_tracking_last_viewer_id"
_SESSION_KEY_VIEWER_ID = "_usage_tracking_viewer_id"
_SESSION_KEY_RUN_COUNT = "_usage_tracking_run_count"


def _get_secret_or_env(key: str, default: Optional[str] = None) -> Optional[str]:
    if st is not None:
        try:
            if key in st.secrets:
                v = st.secrets.get(key)
                return None if v is None else str(v)
        except Exception:
            pass
    return os.getenv(key, default) or default


def _ensure_sslmode(db_url: str) -> str:
    if "sslmode=" in db_url:
        return db_url
    joiner = "&" if "?" in db_url else "?"
    return f"{db_url}{joiner}sslmode=require"


def _email_from_oidc_user(user: Any) -> Optional[str]:
    """Extrai email do objeto usuário OIDC (st.user / st.experimental_user)."""
    if user is None:
        return None
    # Dict-like: .get("email") ou ["email"]
    for key in ("email", "mail", "preferred_username"):
        try:
            if callable(getattr(user, "get", None)):
                v = user.get(key)
            elif hasattr(user, "__getitem__"):
                v = user[key]
            else:
                v = getattr(user, key, None)
            if isinstance(v, str) and "@" in v:
                return v.strip().lower()
        except (KeyError, TypeError, AttributeError):
            continue
    # Atributo direto
    for attr in ("email", "mail"):
        v = getattr(user, attr, None)
        if isinstance(v, str) and "@" in v:
            return v.strip().lower()
    # to_dict() se existir (Streamlit expõe assim)
    try:
        to_dict = getattr(user, "to_dict", None)
        if callable(to_dict):
            d = to_dict()
            if isinstance(d, dict):
                for k in ("email", "mail", "preferred_username"):
                    v = d.get(k)
                    if isinstance(v, str) and "@" in v:
                        return v.strip().lower()
    except Exception:
        pass
    return None


def get_viewer_id() -> str:
    """
    Identificador do viewer: email (Supabase Auth ou OIDC) ou session_id.
    Prioridade: Supabase Auth > OIDC (st.user / st.experimental_user) > session_id em cache.
    """
    if st is None:
        return "no-streamlit"

    email: Optional[str] = None
    # 1) Login "na mão" com Supabase Auth (email em session_state)
    try:
        from utils.supabase_auth import get_logged_in_email
        email = get_logged_in_email()
    except Exception:
        pass
    # 2) OIDC (Streamlit Cloud): st.user (estável) e st.experimental_user
    if not email:
        for user_attr in ("user", "experimental_user"):
            u = getattr(st, user_attr, None) if st else None
            if u is not None:
                # Não exigir is_logged_in: em alguns ambientes o email vem mesmo sem o flag
                email = _email_from_oidc_user(u)
                if email:
                    break

    # Sempre preferir email quando logado (invalida cache de session_xxx após login)
    if email and isinstance(email, str) and "@" in email:
        viewer_id = email.strip().lower()
        st.session_state[_SESSION_KEY_VIEWER_ID] = viewer_id
        return viewer_id

    if _SESSION_KEY_VIEWER_ID in st.session_state:
        return st.session_state[_SESSION_KEY_VIEWER_ID]

    viewer_id = f"session_{uuid.uuid4().hex[:16]}"
    st.session_state[_SESSION_KEY_VIEWER_ID] = viewer_id
    return viewer_id


def record_usage(app_name: str) -> None:
    """
    Registra acesso ao app no Supabase (upsert por app_name + viewer_id).
    Só grava a partir do 2º run da sessão (evita health checks / keep-alive
    que fazem 1 request a cada ~5 min e criam sessão nova).
    Throttle 20 min: no máximo uma escrita a cada _THROTTLE_MINUTES por sessão.
    Invisível para o usuário.
    """
    if st is None:
        return

    db_url = _get_secret_or_env("SUPABASE_DB_URL")
    if not db_url:
        return

    # Contar runs desta sessão: 1º run = carga ou health check; 2º+ = interação ou refresh
    run_count = st.session_state.get(_SESSION_KEY_RUN_COUNT, 0) + 1
    st.session_state[_SESSION_KEY_RUN_COUNT] = run_count
    if run_count < _MIN_RUNS_BEFORE_LOG:
        return

    now = datetime.now(timezone.utc)
    viewer_id = get_viewer_id()

    if _SESSION_KEY_LAST_LOGGED in st.session_state:
        last = st.session_state[_SESSION_KEY_LAST_LOGGED]
        last_viewer = st.session_state.get(_SESSION_KEY_LAST_VIEWER_ID)
        # Se passou menos de 20 min E o viewer_id é o mesmo, retorna (throttle)
        # Se o viewer_id mudou (ex: fez login), grava imediatamente
        if (now - last).total_seconds() < _THROTTLE_MINUTES * 60 and last_viewer == viewer_id:
            return

    try:
        import psycopg2
        with psycopg2.connect(_ensure_sslmode(db_url)) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.app_usage_log (app_name, viewer_id, last_seen_at, first_seen_at, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (app_name, viewer_id)
                    DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
                    """,
                    (app_name, viewer_id, now, now, now),
                )
            conn.commit()
        st.session_state[_SESSION_KEY_LAST_LOGGED] = now
        st.session_state[_SESSION_KEY_LAST_VIEWER_ID] = viewer_id
    except Exception:
        pass
