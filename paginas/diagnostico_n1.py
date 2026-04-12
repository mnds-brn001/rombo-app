import html
import os
import textwrap

import streamlit as st
import pandas as pd

from components.glass_card import render_page_title
from components.rombo_theme import inject_rombo_bleed_css
from utils.KPIs import calculate_kpis, per_line_lost_revenue
from utils.svg_icons import get_svg_icon

_BENCHMARK_LOW = 5.0
_BENCHMARK_HIGH = 8.0
_ROMBO_DEFAULT_CALENDLY = "https://calendly.com/brunomendessj/15min"
_ROMBO_LINKEDIN = "https://www.linkedin.com/in/brunomendesinsightexpert/"


def _benchmark_extra_loss(lost_revenue: float, cancel_rate: float) -> float:
    """
    Estimativa grosseira: quanto da receita perdida com cancelamentos excede o que seria esperado
    se a taxa estivesse no teto de referência (8%), assumindo relação aproximadamente linear.
    """
    if cancel_rate <= 0.01 or lost_revenue <= 0:
        return 0.0
    lost_at_ref = lost_revenue * (_BENCHMARK_HIGH / cancel_rate)
    return max(0.0, lost_revenue - lost_at_ref)


def _render_blood_kpis(kpis_dict: dict[str, str]) -> None:
    parts = []
    for k, v in kpis_dict.items():
        parts.append(
            '<div class="rombo-blood-kpi">'
            f'<div class="rombo-kpi-label">{html.escape(k)}</div>'
            f'<div class="rombo-kpi-value">{html.escape(v)}</div>'
            "</div>"
        )
    st.markdown(f'<div class="rombo-blood-kpi-row">{"".join(parts)}</div>', unsafe_allow_html=True)


def _resolve_expert_cta_url() -> str:
    u = os.environ.get("INSIGHTX_EXPERT_CTA_URL", "").strip()
    if u:
        return u
    try:
        u = str(st.secrets["INSIGHTX_EXPERT_CTA_URL"]).strip()
    except Exception:
        u = ""
    return u or _ROMBO_DEFAULT_CALENDLY


def show(df: pd.DataFrame, *, data_path: str = "") -> None:
    """Relatório do rombo financeiro (cancelamentos). Lead já capturado na landing; benchmark + CTA aqui."""
    del data_path  # reservado para telemetria futura
    inject_rombo_bleed_css()

    render_page_title(
        "Rombo financeiro",
        icon="🩸",
        subtitle="Esta leitura mostra, em poucos minutos, o impacto em reais dos cancelamentos na sua base. "
        "O Diagnóstico Estratégico com playbook vai além: posiciona portfólio (BCG híbrido), capital preso, "
        "SKUs e categorias que mais pesam no resultado, e uma lista priorizada de ações para você executar.",
        wrapper_class="rombo-bleed-title",
    )

    kpis_eligible = calculate_kpis(df, eligible_only=True)
    kpis_full = calculate_kpis(df, eligible_only=False)

    if "pedido_cancelado" in df.columns:
        df_cancelled = df[df["pedido_cancelado"] == 1]
    else:
        df_cancelled = pd.DataFrame(columns=df.columns)

    revenue_lost = float(kpis_full.get("lost_revenue", 0.0))
    cancel_rate = float(kpis_full.get("cancellation_rate", 0.0))
    total_revenue = float(kpis_eligible.get("total_revenue", 0.0))
    total_orders = int(kpis_eligible.get("total_orders", 0))

    st.markdown(
        f'<p class="rombo-section-drip">{get_svg_icon("warning", size=26)} O rombo em números '
        "<span style='opacity:0.85'>— quanto a sua operação deixa de faturar com cancelamentos</span></p>",
        unsafe_allow_html=True,
    )

    kpis_dict = {
        "Dinheiro perdido com cancelamentos": f"R$ {revenue_lost:,.2f}",
        "Taxa de cancelamento": f"{cancel_rate:.1f}%",
        "Receita total na base": f"R$ {total_revenue:,.2f}",
        "Volume de pedidos": f"{total_orders:,}",
    }
    _render_blood_kpis(kpis_dict)

    extra_vs = _benchmark_extra_loss(revenue_lost, cancel_rate)
    extra_txt = html.escape(f"R$ {extra_vs:,.2f}")
    st.markdown(
        textwrap.dedent(
            f"""
            <div class="rombo-benchmark-strip">
              <strong>Sua taxa:</strong> {cancel_rate:.1f}% &nbsp;|&nbsp;
              <strong>Referência de mercado (varejo saudável):</strong> {_BENCHMARK_LOW:.0f}% a {_BENCHMARK_HIGH:.0f}%<br/>
              <strong>Impacto estimado:</strong> na faixa de referência, a perda por cancelamento costuma ser menor —
              aqui o <em>excesso</em> em relação a operar perto de {_BENCHMARK_HIGH:.0f}% de taxa soma cerca de <strong>{extra_txt}</strong>
              a mais em receita perdida (estimativa linear para leitura rápida, não é auditoria).
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(
        '<p class="rombo-section-drip">🔪 Onde mais dói na sua operação? '
        "<span style='opacity:0.85'>SKUs que mais concentram receita perdida por cancelamento</span></p>",
        unsafe_allow_html=True,
    )
    if not df_cancelled.empty and "product_id" in df_cancelled.columns:
        df_c = df_cancelled.copy()
        df_c["_lost_line"] = per_line_lost_revenue(df_c)
        top_sku_table = (
            df_c.groupby("product_id", as_index=False)
            .agg(
                cancelamentos=("order_id", "nunique"),
                receita_perdida=("_lost_line", "sum"),
            )
            .sort_values("receita_perdida", ascending=False)
            .head(10)
        )
        if not top_sku_table.empty:
            top_sku_table = top_sku_table.rename(
                columns={
                    "product_id": "SKU",
                    "cancelamentos": "Qtd. Cancelamentos",
                    "receita_perdida": "Receita Perdida",
                }
            )
            top_sku_table["Receita Perdida"] = top_sku_table["Receita Perdida"].round(2)
            st.dataframe(
                top_sku_table.style.format({"Receita Perdida": "R$ {:,.2f}"})
                .set_table_styles(
                    [
                        {
                            "selector": "thead th",
                            "props": [
                                ("background", "linear-gradient(180deg,#450a0a,#1c0a0c)"),
                                ("color", "#fecaca"),
                                ("font-weight", "700"),
                                ("border", "1px solid #7f1d1d"),
                            ],
                        }
                    ]
                )
                .set_properties(**{"background-color": "#1c1214", "color": "#fecdd3"}),
                use_container_width=True,
                height=420,
                hide_index=True,
                column_config={
                    "SKU": st.column_config.TextColumn("SKU", width="small"),
                    "Qtd. Cancelamentos": st.column_config.NumberColumn("Qtd. Cancelamentos", width="medium"),
                    "Receita Perdida": st.column_config.NumberColumn("Receita Perdida", width="medium"),
                },
            )
        else:
            st.info("Não há dados suficientes de cancelamento por SKU.")
    else:
        st.info("Não foram encontrados pedidos cancelados neste dataset.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        f'<p class="rombo-section-drip">{get_svg_icon("target", size=26, color="#fca5a5")} '
        "O que fazer a partir daqui</p>",
        unsafe_allow_html=True,
    )

    action_plan = textwrap.dedent(
        """
        <div class="rombo-action-blood">
            <p>Esta tela mostra <strong>onde está o vazamento em reais</strong>. Os próximos passos dependem do tamanho
            da dor que você viu acima — e de quanto você quer ir além do cancelamento isolado.</p>
            <ul>
                <li><strong>Agir já no que dói:</strong> revise campanhas, disponibilidade anunciada e estoque virtual dos SKUs
                que mais cancelam, até entender se o problema é ruptura, prazo de separação/postagem, expectativa do cliente ou fraude.</li>
                <li><strong>Cuidar do prazo de execução:</strong> muito cancelamento vem de atraso operacional — não só de
                “cliente difícil”. Vale medir o tempo real entre pagamento e postagem.</li>
                <li><strong>Quer decisões com playbook:</strong> no <span class="rombo-lp-brand">Diagnóstico Estratégico</span> a gente cruza
                essas perdas com giro, capital preso e mix de portfólio, e você recebe dezenas de ações priorizadas
                e calls para alinhar leitura e prioridade.</li>
                <li><strong>Quer o sistema rodando todo mês:</strong> na <span class="rombo-lp-brand">assinatura Insight Expert</span> você
                acompanha painéis, alertas e exportações acionáveis com a sua equipe — sem depender de alguém para “traduzir” gráfico.</li>
            </ul>
        </div>
        """
    ).strip()
    st.markdown(action_plan, unsafe_allow_html=True)

    cta_url = _resolve_expert_cta_url()
    li_esc = html.escape(_ROMBO_LINKEDIN)

    st.markdown('<div class="rombo-cta-block">', unsafe_allow_html=True)
    st.link_button("Quero reduzir esse rombo", cta_url, use_container_width=True)
    st.markdown(
        "<p class='rombo-cta-caption'>Dúvida se o número reflete a operação ou quer um plano para <strong>baixar essa taxa em cerca de 30 dias</strong>? "
        "Use o botão para <strong>agendar 15 minutos</strong> no Calendly com um especialista. "
        f"Se preferir só se apresentar antes, <a href=\"{li_esc}\" target=\"_blank\" rel=\"noopener\">fale no LinkedIn</a>.</p>",
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)
