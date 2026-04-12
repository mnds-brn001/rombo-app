"""
Login "na mão" com Supabase Auth (email + senha).

Não usa st.login() do Streamlit; você controla o fluxo e grava o email
em st.session_state para o tracking (app_usage_log) usar como viewer_id.

Requer em secrets ou .env: SUPABASE_URL e SUPABASE_ANON_KEY.
No Supabase Dashboard: Auth → Providers → Email habilitado; criar usuário de teste.
"""
from __future__ import annotations

import os
from typing import Optional

try:
    import streamlit as st
except ImportError:
    st = None

_SESSION_EMAIL = "_supabase_auth_email"
_SESSION_USER = "_supabase_auth_user"


def _get_config() -> tuple[Optional[str], Optional[str]]:
    url = None
    key = None
    if st is not None:
        try:
            url = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
            key = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")
        except Exception:
            pass
    if not url:
        url = os.getenv("SUPABASE_URL")
    if not key:
        key = os.getenv("SUPABASE_ANON_KEY")
    return (url, key)


def is_configured() -> bool:
    """True se SUPABASE_URL e SUPABASE_ANON_KEY estiverem definidos."""
    url, key = _get_config()
    return bool(url and key)


def get_logged_in_email() -> Optional[str]:
    """Email do usuário logado via Supabase Auth, ou None."""
    if st is None:
        return None
    return st.session_state.get(_SESSION_EMAIL)


def get_client():
    """Cliente Supabase (para login). Cria uma vez por sessão se quiser cachear."""
    from supabase import create_client
    url, key = _get_config()
    if not url or not key:
        raise ValueError("SUPABASE_URL e SUPABASE_ANON_KEY são obrigatórios para Supabase Auth.")
    return create_client(url, key)


def show_login_form() -> bool:
    """
    Mostra formulário de login (email + senha). Em caso de sucesso, grava
    o email em st.session_state e retorna True (chame st.rerun() em seguida).
    Retorna False se ainda não logou.
    """
    if st is None:
        return False
    st.markdown("### Acesse com sua conta")
    st.markdown("Use o email e senha cadastrados no Supabase Auth.")
    with st.form("supabase_login"):
        email = st.text_input("Email", type="default", placeholder="seu@email.com")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
    if not submitted or not email or not password:
        return False
    try:
        client = get_client()
        resp = client.auth.sign_in_with_password({"email": email.strip(), "password": password})
        if resp.user and getattr(resp.user, "email", None):
            st.session_state[_SESSION_EMAIL] = resp.user.email
            st.session_state[_SESSION_USER] = resp.user
            return True
    except Exception as e:
        st.error(f"Erro ao entrar: {e}")
    return False


def logout():
    """Limpa a sessão de login (Supabase Auth) e opcionalmente chama sign_out no Supabase."""
    if st is None:
        return
    if _SESSION_EMAIL in st.session_state:
        del st.session_state[_SESSION_EMAIL]
    if _SESSION_USER in st.session_state:
        del st.session_state[_SESSION_USER]
    try:
        client = get_client()
        client.auth.sign_out()
    except Exception:
        pass
