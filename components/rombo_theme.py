"""CSS helpers for the Rombo Financeiro funnel (metallic landing + bleed report)."""

import streamlit as st

ROMBO_LANDING_CSS = """
<style>
@keyframes romboBucketSheen {
  0% { transform: translateX(-60%); opacity: 0; }
  15% { opacity: 1; }
  100% { transform: translateX(60%); opacity: 0; }
}
.rombo-bucket-panel {
  position: relative;
  overflow: hidden;
  border-radius: 18px;
  padding: 14px 14px 10px 14px;
  margin-bottom: 8px;
  background: linear-gradient(145deg,
    rgba(30, 41, 59, 0.72) 0%,
    rgba(15, 23, 42, 0.88) 45%,
    rgba(30, 27, 46, 0.82) 100%);
  border: 1px solid rgba(226, 232, 240, 0.22);
  box-shadow:
    0 10px 40px rgba(0, 0, 0, 0.45),
    inset 0 1px 0 rgba(255, 255, 255, 0.12),
    inset 0 -1px 0 rgba(0, 0, 0, 0.35);
}
.rombo-bucket-panel::before {
  content: "";
  position: absolute;
  inset: 0;
  background: linear-gradient(
    120deg,
    transparent 0%,
    rgba(255, 255, 255, 0.06) 42%,
    rgba(255, 255, 255, 0.14) 50%,
    rgba(255, 255, 255, 0.05) 58%,
    transparent 100%
  );
  animation: romboBucketSheen 7s ease-in-out infinite;
  pointer-events: none;
}
.rombo-bucket-panel h4 {
  position: relative;
  z-index: 1;
  margin: 0 0 4px 0;
  font-size: 1rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  color: #e2e8f0;
}
.rombo-bucket-panel .rombo-bucket-hint {
  position: relative;
  z-index: 1;
  font-size: 0.78rem;
  line-height: 1.35;
  color: rgba(148, 163, 184, 0.95);
  margin: 0 0 10px 0;
}
.rombo-bucket-tag {
  position: relative;
  z-index: 1;
  display: inline-block;
  font-size: 0.68rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  padding: 3px 8px;
  border-radius: 999px;
  margin-bottom: 8px;
  border: 1px solid rgba(167, 139, 250, 0.35);
  color: #ddd6fe;
  background: rgba(88, 28, 135, 0.25);
}
.rombo-bucket-tag.rombo-tag-locked {
  border-color: rgba(148, 163, 184, 0.35);
  color: #cbd5e1;
  background: rgba(30, 41, 59, 0.5);
}
.rombo-bucket-locked-body {
  position: relative;
  z-index: 1;
  font-size: 0.82rem;
  line-height: 1.45;
  color: #94a3b8;
  padding: 10px 4px 6px 4px;
}
.rombo-funnel-note {
  text-align: center;
  color: #94a3b8;
  font-size: 0.9rem;
  line-height: 1.5;
  margin: 8px 0 20px 0;
  padding: 0 8px;
}
/* Sidebar Rombo: contato (pills, sem azul padrão) */
.rombo-sidebar-links {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 4px;
  padding-bottom: 4px;
}
.rombo-sidebar-links a.rombo-contact-pill {
  display: block;
  text-align: center;
  text-decoration: none !important;
  font-size: 0.82rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  padding: 10px 12px;
  border-radius: 10px;
  border: 1px solid rgba(148, 163, 184, 0.35);
  color: #e2e8f0 !important;
  background: linear-gradient(165deg,
    rgba(51, 65, 85, 0.55) 0%,
    rgba(30, 41, 59, 0.85) 100%);
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
  transition: border-color 0.15s ease, box-shadow 0.15s ease, color 0.15s ease;
}
.rombo-sidebar-links a.rombo-contact-li:hover {
  border-color: rgba(96, 165, 250, 0.55);
  color: #f8fafc !important;
  box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.2);
}
.rombo-sidebar-links a.rombo-contact-wa:hover {
  border-color: rgba(52, 211, 153, 0.5);
  color: #ecfdf5 !important;
  box-shadow: 0 0 0 1px rgba(52, 211, 153, 0.18);
}
</style>
"""

# Estilo do botão "dados de demonstração" na landing (único primary típico nesta tela)
ROMBO_DEMO_BUTTON_CSS = """
<style>
/* Streamlit: wrapper pode ser stButton ou stBaseButton */
section.main button[kind="primary"] {
  background: linear-gradient(165deg, #4338ca 0%, #6366f1 45%, #7c3aed 100%) !important;
  color: #f8fafc !important;
  border: 1px solid rgba(165, 180, 252, 0.45) !important;
  box-shadow:
    0 10px 28px rgba(67, 56, 202, 0.42),
    inset 0 1px 0 rgba(255, 255, 255, 0.12) !important;
  font-weight: 600 !important;
  letter-spacing: 0.02em !important;
  border-radius: 14px !important;
  min-height: 2.85rem !important;
  transition: filter 0.15s ease, box-shadow 0.15s ease !important;
}
section.main button[kind="primary"]:hover {
  filter: brightness(1.07);
  box-shadow:
    0 14px 36px rgba(99, 102, 241, 0.48),
    inset 0 1px 0 rgba(255, 255, 255, 0.14) !important;
}
section.main button[kind="primary"]:focus-visible {
  outline: 2px solid rgba(165, 180, 252, 0.65) !important;
  outline-offset: 2px !important;
}
</style>
"""

ROMBO_BLEED_CSS = """
<style>
@keyframes romboBloodPulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.25); }
  50% { box-shadow: 0 0 28px 2px rgba(220, 38, 38, 0.18); }
}
.insight-page-title-card.rombo-bleed-title {
  border-color: rgba(248, 113, 113, 0.35) !important;
  background: linear-gradient(135deg,
    rgba(50, 20, 28, 0.88) 0%,
    rgba(30, 20, 35, 0.92) 50%,
    rgba(40, 15, 22, 0.9) 100%) !important;
}
.insight-page-title-card.rombo-bleed-title h1 {
  background: linear-gradient(135deg,
    #fecaca 0%,
    #fca5a5 25%,
    #f8fafc 55%,
    #fca5a5 80%,
    #f87171 100%) !important;
  background-clip: text !important;
  -webkit-background-clip: text !important;
  -webkit-text-fill-color: transparent !important;
}
.insight-page-title-card.rombo-bleed-title .insight-page-title-subtitle {
  color: rgba(254, 202, 202, 0.92) !important;
}
.rombo-blood-kpi-row {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  justify-content: center;
  margin: 8px 0 24px 0;
}
.rombo-blood-kpi {
  flex: 1 1 200px;
  max-width: 260px;
  min-width: 160px;
  padding: 16px 18px;
  border-radius: 16px;
  border: 1px solid rgba(248, 113, 113, 0.28);
  background: linear-gradient(165deg, rgba(40, 15, 20, 0.92), rgba(15, 15, 24, 0.95));
  animation: romboBloodPulse 4.5s ease-in-out infinite;
}
.rombo-blood-kpi .rombo-kpi-label {
  font-size: 0.78rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #fca5a5;
  margin-bottom: 6px;
}
.rombo-blood-kpi .rombo-kpi-value {
  font-size: 1.35rem;
  font-weight: 800;
  color: #fef2f2;
  text-shadow: 0 0 18px rgba(220, 38, 38, 0.35);
}
.rombo-section-drip {
  font-size: 1.05rem;
  font-weight: 700;
  color: #fecaca;
  margin: 18px 0 10px 0;
  text-align: center;
  letter-spacing: 0.02em;
}
.rombo-table-wrap {
  border-radius: 14px;
  overflow: hidden;
  border: 1px solid rgba(185, 28, 28, 0.35);
  margin-top: 8px;
}
.rombo-action-blood {
  border-radius: 16px;
  border: 1px solid rgba(248, 113, 113, 0.22);
  background: linear-gradient(180deg, rgba(35, 12, 16, 0.9), rgba(15, 15, 22, 0.96));
  padding: 18px 22px;
  margin-top: 8px;
}
.rombo-action-blood ul { margin: 8px 0 0 0; padding-left: 1.2rem; }
.rombo-action-blood li { margin-bottom: 10px; color: #e2e8f0; line-height: 1.45; }
.rombo-action-blood p { color: #cbd5e1; margin: 0 0 8px 0; }
.rombo-action-blood strong { color: #fecaca; }
.rombo-lp-brand {
  background: linear-gradient(90deg, #2dd4bf, #06b6d4, #0d9488);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  font-weight: 800;
}
.rombo-benchmark-strip {
  max-width: 52rem;
  margin: 0 auto 22px auto;
  padding: 14px 18px;
  border-radius: 14px;
  border: 1px solid rgba(248, 113, 113, 0.35);
  background: linear-gradient(95deg, rgba(60, 15, 22, 0.85), rgba(25, 15, 28, 0.92));
  font-size: 0.95rem;
  line-height: 1.5;
  color: #fecdd3;
  text-align: center;
}
.rombo-benchmark-strip strong { color: #fff1f2; }
.rombo-lead-gate {
  max-width: 32rem;
  margin: 0 auto 28px auto;
  padding: 20px 22px;
  border-radius: 16px;
  border: 1px solid rgba(167, 139, 250, 0.35);
  background: linear-gradient(160deg, rgba(35, 30, 55, 0.9), rgba(20, 20, 32, 0.95));
}
.rombo-lead-gate h3 {
  margin: 0 0 8px 0;
  font-size: 1.1rem;
  color: #f1f5f9;
  text-align: center;
}
.rombo-lead-gate p {
  margin: 0 0 14px 0;
  font-size: 0.88rem;
  color: #94a3b8;
  text-align: center;
  line-height: 1.45;
}
.rombo-cta-block {
  max-width: 36rem;
  margin: 22px auto 8px auto;
  text-align: center;
}
.rombo-cta-caption {
  font-size: 0.88rem;
  color: #94a3b8;
  margin-top: 10px;
  line-height: 1.45;
}
</style>
"""


def inject_rombo_landing_css() -> None:
    st.markdown(ROMBO_LANDING_CSS, unsafe_allow_html=True)


def inject_rombo_demo_button_css() -> None:
    st.markdown(ROMBO_DEMO_BUTTON_CSS, unsafe_allow_html=True)


def inject_rombo_bleed_css() -> None:
    st.markdown(ROMBO_BLEED_CSS, unsafe_allow_html=True)
