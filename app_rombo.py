"""
Insight Expert — Rombo financeiro (Streamlit).
Ponto de entrada: leitura rápida de cancelamentos e receita perdida.
"""
import os
import sys
import textwrap
from pathlib import Path
from urllib.parse import quote

import streamlit as st

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from components.glass_card import render_page_title
from components.rombo_theme import inject_rombo_demo_button_css, inject_rombo_landing_css
from paginas.diagnostico_n1 import show as show_rombo_relatorio
from utils.file_upload_manager import FileUploadManager
from utils.KPIs import load_data
from utils.theme_manager import get_theme_manager

st.set_page_config(
    page_title="Insight Expert — Rombo financeiro",
    page_icon="🩸",
    layout="wide",
    initial_sidebar_state="expanded",
)

get_theme_manager().apply_theme()

_ROMBO_DEFAULT_LINKEDIN = "https://www.linkedin.com/in/brunomendesinsightexpert/"
_ROMBO_DEFAULT_WA_PREFILL = (
    "Olá! Vi o Rombo financeiro no Insight Expert e quero conversar sobre cancelamentos e receita na minha operação."
)


def _secret_str(key: str) -> str:
    """Lê chave na raiz do secrets ou dentro de [auth] (TOML costuma aninhar chaves após [auth])."""
    try:
        sec = st.secrets
        if key in sec:
            return str(sec[key]).strip()
        auth = sec.get("auth") if hasattr(sec, "get") else None
        if auth is None and "auth" in sec:
            auth = sec["auth"]
        if isinstance(auth, dict) and key in auth:
            return str(auth[key]).strip()
    except Exception:
        pass
    return ""


def _resolve_contact_urls() -> tuple[str, str]:
    """LinkedIn (sempre) e WhatsApp (wa.me + texto pré-preenchido) se telefone estiver em env/secrets."""
    li = os.getenv("INSIGHTX_LINKEDIN_URL", _ROMBO_DEFAULT_LINKEDIN).strip()
    phone = os.getenv("INSIGHTX_WHATSAPP_PHONE", "").strip()
    wa_msg = os.getenv("INSIGHTX_WHATSAPP_PREFILL", _ROMBO_DEFAULT_WA_PREFILL).strip()
    li = _secret_str("INSIGHTX_LINKEDIN_URL") or li
    phone = _secret_str("INSIGHTX_WHATSAPP_PHONE") or phone
    wa_msg = _secret_str("INSIGHTX_WHATSAPP_PREFILL") or wa_msg
    phone = "".join(ch for ch in phone if ch.isdigit())
    # Brasil: DDD + celular (10 ou 11 dígitos) sem DDI → prefixa 55 para wa.me
    if phone and not phone.startswith("55") and len(phone) in (10, 11):
        phone = "55" + phone
    if len(phone) >= 12:
        base = f"https://wa.me/{phone}"
        if wa_msg:
            base += f"?text={quote(wa_msg, safe='')}"
        wa = base
    else:
        wa = ""
    return li, wa


def render_rombo_sidebar() -> None:
    """Logo Insight Expert + links de contato (sidebar fixa em todo o app Rombo)."""
    inject_rombo_landing_css()
    logo_path = Path("components/img/logo.png")
    with st.sidebar:
        if logo_path.exists():
            st.image(str(logo_path), use_container_width=False, width=140)
        st.caption("Contato")
        li, wa = _resolve_contact_urls()
        links = '<div class="rombo-sidebar-links">'
        links += (
            f'<a class="rombo-contact-pill rombo-contact-li" href="{li}" '
            'target="_blank" rel="noopener noreferrer">LinkedIn</a>'
        )
        if wa:
            links += (
                f'<a class="rombo-contact-pill rombo-contact-wa" href="{wa}" '
                'target="_blank" rel="noopener noreferrer">WhatsApp</a>'
            )
        links += "</div>"
        st.markdown(links, unsafe_allow_html=True)
        if not wa:
            st.caption("WhatsApp: defina `INSIGHTX_WHATSAPP_PHONE` nos secrets (DDI+DDD+número, só dígitos).")
        st.markdown("---")


def _resolve_demo_data_path() -> str:
    custom_env = os.getenv("INSIGHTX_DATA_PATH", "").strip()
    if custom_env:
        env_path = Path(custom_env)
        if env_path.is_dir():
            candidates = [
                env_path / "dados_processados.parquet",
                env_path / "cliente_merged.parquet",
            ]
            candidates.extend(sorted(env_path.glob("*.parquet")))
            for candidate in candidates:
                if candidate.exists():
                    return str(candidate)
        elif env_path.exists():
            return str(env_path)

    return "data/supabase_backup/2026-03-10_15-25/public_orders_enriched.parquet"


def _bucket_header_html(
    title: str,
    hint: str,
    tag: str,
    *,
    locked: bool = False,
    body: str = "",
) -> str:
    tag_cls = "rombo-bucket-tag rombo-tag-locked" if locked else "rombo-bucket-tag"
    return textwrap.dedent(
        f"""
        <div class="rombo-bucket-panel">
          <span class="{tag_cls}">{tag}</span>
          <h4>{title}</h4>
          <p class="rombo-bucket-hint">{hint}</p>
          {body}
        </div>
        """
    ).strip()


def render_upload_screen():
    inject_rombo_demo_button_css()
    _, center, _ = st.columns([1, 5, 1])
    with center:
        render_page_title(
            "Rombo financeiro",
            icon="💎",
            subtitle="Descubra, com os seus próprios pedidos, quanto dinheiro a operação deixa na mesa por causa "
            "de cancelamentos e atrito. Em minutos, você enxerga o impacto financeiro e os SKUs que mais pesam "
            "no resultado — e decide com clareza o próximo passo para recuperar receita.",
        )

        st.markdown(
            "<p class='rombo-funnel-note'>Abaixo, três blocos para você enxergar o caminho: hoje basta o arquivo de pedidos. Os outros dois mostram o que passamos a integrar quando você contrata o diagnóstico completo ou a assinatura do Insight Expert.</p>",
            unsafe_allow_html=True,
        )

        b1, b2, b3 = st.columns(3)

        with b1:
            st.markdown(
                _bucket_header_html(
                    "Seus pedidos (exportação ERP)",
                    "Envie o CSV <strong>Exportar Consulta de Pedidos</strong> (nome começa assim), "
                    "com campos separados por ponto e vírgula — é o formato que o seu ERP já exporta.",
                    "Passo 1 · envie agora",
                    locked=False,
                ),
                unsafe_allow_html=True,
            )
            uploaded_file = st.file_uploader(
                "Selecionar arquivo de pedidos (CSV)",
                type=["csv"],
                label_visibility="collapsed",
                key="rombo_pedidos_csv",
            )

            if uploaded_file is not None:
                with st.spinner("Lendo o seu arquivo…"):
                    manager = FileUploadManager()
                    success, msg, file_path = manager.save_uploaded_file(uploaded_file)

                    if success and file_path:
                        is_valid, val_msg, _df_preview = manager.validate_csv_structure(file_path)

                        if is_valid:
                            proc_success, proc_msg, processed_dir = manager.process_file_with_etl(file_path)

                            if proc_success and processed_dir:
                                st.success("Arquivo recebido. Abrindo a leitura do rombo na sua operação.")
                                st.session_state.rombo_data_path = str(processed_dir)
                                st.rerun()
                            else:
                                st.error(proc_msg)
                        else:
                            st.error(val_msg)
                    else:
                        st.error(msg)

        with b2:
            st.markdown(
                _bucket_header_html(
                    "Nível 1 — Diagnóstico Estratégico + Playbook",
                    "Projeto único (sem mensalidade), conforme porte. "
                    "Para quem precisa de <strong>clareza com dados</strong> — não opinião — sobre onde a operação sangra "
                    "antes de compromisso maior.",
                    "Passo 2 · Nível 1",
                    locked=True,
                    body=(
                        '<div class="rombo-bucket-locked-body">'
                        "<strong>O que você leva:</strong> conexão dos seus dados ao Insight Expert; diagnóstico com "
                        "matriz BCG híbrida e mapa de capital preso; lista priorizada de SKUs e categorias por impacto; "
                        "playbook com <strong>20 a 50 ações</strong> específicas; <strong>1 a 2 calls</strong> de leitura. "
                        "<strong>Resultado:</strong> saber onde agir primeiro e quanto isso representa em receita recuperável."
                        "</div>"
                    ),
                ),
                unsafe_allow_html=True,
            )

        with b3:
            st.markdown(
                _bucket_header_html(
                    "Nível 2 — Recorrência em escala (Enterprise)",
                    "Para operações <strong>complexas</strong> — multi-canal, integrações sob medida, múltiplos times. "
                    "<strong>Sob proposta.</strong> Fee variável pode compor o modelo (transparência em contrato).",
                    "Passo 3 · Nível 2",
                    locked=True,
                    body=(
                        '<div class="rombo-bucket-locked-body">'
                        "<strong>Em sua operação:</strong> setup enterprise, APIs, módulos e treinamento alinhados ao seu modelo — "
                        "com SLA definido. "
                        "<strong>Recorrência operacional do dia a dia</strong> (painel, alertas, calibração mensal) é o "
                        "<strong>Nível 2 — Assinatura</strong>. "
                        "</div>"
                    ),
                ),
                unsafe_allow_html=True,
            )

        st.markdown(
            "<br><hr style='border-color: rgba(255,255,255,0.08); margin: 28px 0 20px 0;'>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<p style='text-align:center;color:#94a3b8;margin-bottom:12px;'>"
            "Ainda sem arquivo? Veja um exemplo com dados anônimos de referência:</p>",
            unsafe_allow_html=True,
        )
        _, btn_col, _ = st.columns([1, 2, 1])
        with btn_col:
            if st.button("Ver exemplo com dados de demonstração", use_container_width=True, type="primary"):
                st.session_state.rombo_data_path = _resolve_demo_data_path()
                st.rerun()


def render_diagnostic_screen(data_path: str):
    try:
        df = load_data(custom_path=data_path)
    except Exception as e:
        st.error(f"Erro ao carregar dados processados: {e}")
        if st.button("Voltar"):
            st.session_state.rombo_data_path = None
            st.rerun()
        return

    if st.button("← Enviar outro arquivo"):
        st.session_state.rombo_data_path = None
        st.rerun()

    show_rombo_relatorio(df, data_path=data_path)


def main():
    render_rombo_sidebar()
    if "rombo_data_path" not in st.session_state:
        st.session_state.rombo_data_path = None
    # Sessões antigas do script app_n1.py
    if not st.session_state.rombo_data_path and st.session_state.get("n1_data_path"):
        st.session_state.rombo_data_path = st.session_state.pop("n1_data_path", None)
    if st.session_state.rombo_data_path is None:
        render_upload_screen()
    else:
        render_diagnostic_screen(st.session_state.rombo_data_path)


if __name__ == "__main__":
    main()
