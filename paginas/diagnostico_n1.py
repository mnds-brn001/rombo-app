import html
import os
import textwrap

import streamlit as st
import pandas as pd

from components.glass_card import render_page_title
from components.rombo_theme import inject_rombo_bleed_css
from utils.KPIs import calculate_kpis, per_line_lost_revenue
from utils.svg_icons import get_svg_icon

_ROMBO_DEFAULT_CALENDLY = "https://calendly.com/brunomendessj/15min"
_ROMBO_LINKEDIN = "https://www.linkedin.com/in/brunomendesinsightexpert/"


def _parse_benchmark_float(raw: str, default: float) -> float:
    if not raw or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip().replace(",", "."))
    except (TypeError, ValueError):
        return default


def _resolve_benchmark_config() -> tuple[float, float, str]:
    """
    Piso, teto e rótulo do benchmark (varejo por padrão).

    Configure no Streamlit Secrets ou no ambiente:
    - INSIGHTX_BENCHMARK_LOW (ex.: 5)
    - INSIGHTX_BENCHMARK_HIGH (ex.: 8)
    - INSIGHTX_BENCHMARK_SEGMENT (ex.: varejo saudável | marketplace de moda | B2B industrial)
    """
    low, high = 5.0, 8.0
    label = "varejo saudável"

    low = _parse_benchmark_float(os.getenv("INSIGHTX_BENCHMARK_LOW", ""), low)
    high = _parse_benchmark_float(os.getenv("INSIGHTX_BENCHMARK_HIGH", ""), high)
    seg = os.getenv("INSIGHTX_BENCHMARK_SEGMENT", "").strip()
    if seg:
        label = seg

    try:
        sec = st.secrets
        if "INSIGHTX_BENCHMARK_LOW" in sec:
            low = _parse_benchmark_float(str(sec["INSIGHTX_BENCHMARK_LOW"]), low)
        if "INSIGHTX_BENCHMARK_HIGH" in sec:
            high = _parse_benchmark_float(str(sec["INSIGHTX_BENCHMARK_HIGH"]), high)
        if "INSIGHTX_BENCHMARK_SEGMENT" in sec:
            s = str(sec["INSIGHTX_BENCHMARK_SEGMENT"]).strip()
            if s:
                label = s
    except Exception:
        pass

    if low > high:
        low, high = high, low
    if high <= 0.01:
        high = 8.0
    if low < 0:
        low = 0.0
    return low, high, label


def _benchmark_extra_loss(lost_revenue: float, cancel_rate: float, ref_ceiling_pct: float) -> float:
    """
    Estimativa grosseira: quanto da receita perdida com cancelamentos excede o que seria esperado
    se a taxa estivesse no teto de referência (ref_ceiling_pct), assumindo relação aproximadamente linear.
    """
    if cancel_rate <= 0.01 or lost_revenue <= 0 or ref_ceiling_pct <= 0:
        return 0.0
    lost_at_ref = lost_revenue * (ref_ceiling_pct / cancel_rate)
    return max(0.0, lost_revenue - lost_at_ref)


def _render_benchmark_strip(
    cancel_rate: float,
    revenue_lost: float,
    *,
    bench_low: float,
    bench_high: float,
    segment_label: str,
) -> None:
    label_esc = html.escape(segment_label)
    extra_vs = _benchmark_extra_loss(revenue_lost, cancel_rate, bench_high)
    extra_txt = html.escape(f"R$ {extra_vs:,.2f}")

    line1 = (
        f"<strong>Sua taxa:</strong> {cancel_rate:.1f}% &nbsp;|&nbsp;"
        f"<strong>Referência de mercado ({label_esc}):</strong> {bench_low:.0f}% a {bench_high:.0f}%<br/>"
    )

    if cancel_rate > bench_high:
        body = (
            f"<strong>Impacto estimado:</strong> acima do teto de referência ({bench_high:.0f}%), a perda por "
            "cancelamento tende a pesar mais — em leitura linear (não é auditoria), o <em>excesso</em> vs. operar "
            f"perto de {bench_high:.0f}% de taxa soma cerca de <strong>{extra_txt}</strong> a mais em receita perdida."
        )
    elif cancel_rate >= bench_low:
        body = (
            f"<strong>Leitura:</strong> sua taxa está <strong>dentro</strong> da faixa de referência "
            f"({bench_low:.0f}%–{bench_high:.0f}%). A receita perdida com cancelamentos continua material (card acima). "
            f"No modelo linear usado aqui, o indicador vs. teto de {bench_high:.0f}% fica em cerca de "
            f"<strong>{extra_txt}</strong> (leitura rápida, não é auditoria)."
        )
    else:
        body = (
            f"<strong>Leitura:</strong> sua taxa está <strong>abaixo</strong> do piso de referência ({bench_low:.0f}%). "
            f"Frente ao segmento <em>{label_esc}</em>, isso costuma ser positivo. O valor em reais perdido reflete a sua base; "
            f"o ajuste vs. teto ({bench_high:.0f}%) neste modelo linear aparece como cerca de <strong>{extra_txt}</strong>."
        )

    st.markdown(
        textwrap.dedent(
            f"""
            <div class="rombo-benchmark-strip">
              {line1}
              {body}
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


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
    # Para o relatório de rombo, exibir volume/base completos evita divergência de leitura
    # com os logs do ETL (que consideram o dataset inteiro, não só elegíveis).
    total_revenue = float(kpis_full.get("total_revenue", 0.0))
    total_orders = int(kpis_full.get("total_orders", 0))

    st.markdown(
        f'<p class="rombo-section-drip">{get_svg_icon("warning", size=26)} O rombo em números '
        "<span style='opacity:0.85'>— quanto a sua operação deixa de faturar com cancelamentos</span></p>",
        unsafe_allow_html=True,
    )

    kpis_dict = {
        "Dinheiro perdido com cancelamentos": f"R$ {revenue_lost:,.2f}",
        "Taxa de cancelamento": f"{cancel_rate:.1f}%",
        "Receita total na base (período)": f"R$ {total_revenue:,.2f}",
        "Volume de pedidos (período)": f"{total_orders:,}",
    }
    _render_blood_kpis(kpis_dict)

    bench_low, bench_high, bench_label = _resolve_benchmark_config()
    _render_benchmark_strip(
        cancel_rate,
        revenue_lost,
        bench_low=bench_low,
        bench_high=bench_high,
        segment_label=bench_label,
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
        if "product_id" not in df.columns:
            st.info(
                "Este export veio sem detalhamento de SKU no pedido. "
                "Conseguimos calcular rombo e taxa de cancelamento, mas o Top SKUs exige coluna de produto."
            )
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
