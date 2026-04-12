from typing import Optional
import re
from pathlib import Path

SVG_ASSETS_DIR = Path(__file__).resolve().parent.parent / "components" / "img"


def _replace_svg_attribute(svg: str, attribute: str, value: str) -> str:
    pattern = rf'{attribute}="[^"]*"'
    if re.search(pattern, svg):
        return re.sub(pattern, f'{attribute}="{value}"', svg, count=1)
    return svg.replace("<svg", f'<svg {attribute}="{value}"', 1)


def _ensure_style_attribute(svg: str, style_value: str) -> str:
    combined_style = f'vertical-align:middle;{style_value}'
    if 'style="' in svg:
        return re.sub(
            r'style="([^"]*)"',
            lambda match: f'style="{combined_style}{match.group(1)}"',
            svg,
            count=1,
        )
    return svg.replace("<svg", f'<svg style="{combined_style}"', 1)


def _load_external_svg(filename: str, size: int, color_style: str) -> str:
    path = SVG_ASSETS_DIR / filename
    try:
        svg = path.read_text()
    except FileNotFoundError:
        return ""

    svg = svg.strip()
    svg = _replace_svg_attribute(svg, "width", str(size))
    svg = _replace_svg_attribute(svg, "height", str(size))
    svg = _ensure_style_attribute(svg, color_style)
    return svg
 
def get_svg_icon(name: str, size: int = 24, color: Optional[str] = None) -> str:
    """
    Retorna o SVG do ícone solicitado, ajustando tamanho e cor se necessário.
    Args:
        name (str): Nome do ícone
        size (int): Tamanho em pixels
        color (str): Cor CSS (opcional)
    Returns:
        str: SVG como string
    """
    svgs = {
        "chart": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M10.5 6a7.5 7.5 0 1 0 7.5 7.5h-7.5V6Z" />
  <path stroke-linecap="round" stroke-linejoin="round" d="M13.5 10.5H21A7.5 7.5 0 0 0 13.5 3v7.5Z" />
</svg>''',
        "money": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 18.75a60.07 60.07 0 0 1 15.797 2.101c.727.198 1.453-.342 1.453-1.096V18.75M3.75 4.5v.75A.75.75 0 0 1 3 6h-.75m0 0v-.375c0-.621.504-1.125 1.125-1.125H20.25M2.25 6v9m18-10.5v.75c0 .414.336.75.75.75h.75m-1.5-1.5h.375c.621 0 1.125.504 1.125 1.125v9.75c0 .621-.504 1.125-1.125 1.125h-.375m1.5-1.5H21a.75.75 0 0 0-.75.75v.75m0 0H3.75m0 0h-.375a1.125 1.125 0 0 1-1.125-1.125V15m1.5 1.5v-.75A.75.75 0 0 0 3 15h-.75M15 10.5a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm3 0h.008v.008H18V10.5Zm-12 0h.008v.008H6V10.5Z" />
</svg>''',
        "hourglass": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M200,75.64V40a16,16,0,0,0-16-16H72A16,16,0,0,0,56,40V76a16.07,16.07,0,0,0,6.4,12.8L114.67,128,62.4,167.2A16.07,16.07,0,0,0,56,180v36a16,16,0,0,0,16,16H184a16,16,0,0,0,16-16V180.36a16.09,16.09,0,0,0-6.35-12.77L141.27,128l52.38-39.6A16.05,16.05,0,0,0,200,75.64ZM72,40H184V75.64L178.23,80H77.33L72,76Zm56,78L98.67,96h58.4Zm56,98H72V180l48-36v24a8,8,0,0,0,16,0V144.08l48,36.28Z"></path></svg>''',
        "people": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M17.982 18.725A7.488 7.488 0 0 0 12 15.75a7.488 7.488 0 0 0-5.982 2.975m11.963 0a9 9 0 1 0-11.963 0m11.963 0A8.966 8.966 0 0 1 12 21a8.966 8.966 0 0 1-5.982-2.275M15 9.75a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
</svg>''',
        "smile": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M18 18.72a9.094 9.094 0 0 0 3.741-.479 3 3 0 0 0-4.682-2.72m.94 3.198.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0 1 12 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 0 1 6 18.719m12 0a5.971 5.971 0 0 0-.941-3.197m0 0A5.995 5.995 0 0 0 12 12.75a5.995 5.995 0 0 0-5.058 2.772m0 0a3 3 0 0 0-4.681 2.72 8.986 8.986 0 0 0 3.74.477m.94-3.197a5.971 5.971 0 0 0-.94 3.197M15 6.75a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm6 3a2.25 2.25 0 1 1-4.5 0 2.25 2.25 0 0 1 4.5 0Zm-13.5 0a2.25 2.25 0 1 1-4.5 0 2.25 2.25 0 0 1 4.5 0Z" />
</svg>''',
        "cancel": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="m9.75 9.75 4.5 4.5m0-4.5-4.5 4.5M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
</svg>''',
        "target": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M221.87,83.16A104.1,104.1,0,1,1,195.67,49l22.67-22.68a8,8,0,0,1,11.32,11.32l-96,96a8,8,0,0,1-11.32-11.32l27.72-27.72a40,40,0,1,0,17.87,31.09,8,8,0,1,1,16-.9,56,56,0,1,1-22.38-41.65L184.3,60.39a87.88,87.88,0,1,0,23.13,29.67,8,8,0,0,1,14.44-6.9Z"></path></svg>''',
        "cart_abandonment": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 24 24" style="vertical-align:middle;{color_style}">
  <g>
    <!-- Carrinho de compras -->
    <path d="M7 18c-1.1 0-1.99.9-1.99 2S5.9 22 7 22s2-.9 2-2-.9-2-2-2zM1 2v2h2l3.6 7.59-1.35 2.45c-.16.28-.25.61-.25.96 0 1.1.9 2 2 2h12v-2H7.42c-.14 0-.25-.11-.25-.25l.03-.12L8.1 13h7.45c.75 0 1.41-.41 1.75-1.03L21.7 4H5.21l-.94-2H1zm16 16c-1.1 0-1.99.9-1.99 2s.89 2 1.99 2 2-.9 2-2-.9-2-2-2z"/>
    <!-- X de abandono sobreposto -->
    <path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z" fill="rgba(220, 53, 69, 0.9)" stroke="rgba(220, 53, 69, 1)" stroke-width="0.5"/>
  </g>
</svg>''',
        "box": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M223.68,66.15,135.68,18a15.88,15.88,0,0,0-15.36,0l-88,48.17a16,16,0,0,0-8.32,14v95.64a16,16,0,0,0,8.32,14l88,48.17a15.88,15.88,0,0,0,15.36,0l88-48.17a16,16,0,0,0,8.32-14V80.18A16,16,0,0,0,223.68,66.15ZM128,32l80.34,44-29.77,16.3-80.35-44ZM128,120,47.66,76l33.9-18.56,80.34,44ZM40,90l80,43.78v85.79L40,175.82Zm176,85.78h0l-80,43.79V133.82l32-17.51V152a8,8,0,0,0,16,0V107.55L216,90v85.78Z"></path></svg>''',
        "money_loss": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M230.33,141.06a24.43,24.43,0,0,0-21.24-4.23l-41.84,9.62A28,28,0,0,0,140,112H89.94a31.82,31.82,0,0,0-22.63,9.37L44.69,144H16A16,16,0,0,0,0,160v40a16,16,0,0,0,16,16H120a7.93,7.93,0,0,0,1.94-.24l64-16a6.94,6.94,0,0,0,1.19-.4L226,182.82l.44-.2a24.6,24.6,0,0,0,3.93-41.56ZM16,160H40v40H16Zm203.43,8.21-38,16.18L119,200H56V155.31l22.63-22.62A15.86,15.86,0,0,1,89.94,128H140a12,12,0,0,1,0,24H112a8,8,0,0,0,0,16h32a8.32,8.32,0,0,0,1.79-.2l67-15.41.31-.08a8.6,8.6,0,0,1,6.3,15.9ZM164,96a36,36,0,0,0,5.9-.48,36,36,0,1,0,28.22-47A36,36,0,1,0,164,96Zm60-12a20,20,0,1,1-20-20A20,20,0,0,1,224,84ZM164,40a20,20,0,0,1,19.25,14.61,36,36,0,0,0-15,24.93A20.42,20.42,0,0,1,164,80a20,20,0,0,1,0-40Z"></path></svg>''',
        "refresh": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M224,48V96a8,8,0,0,1-8,8H168a8,8,0,0,1,0-16h28.69L182.06,73.37a79.56,79.56,0,0,0-56.13-23.43h-.45A79.52,79.52,0,0,0,69.59,72.71,8,8,0,0,1,58.41,61.27a96,96,0,0,1,135,.79L208,76.69V48a8,8,0,0,1,16,0Zm-37.59,135.29a80,80,0,0,1-112.47-.66L59.31,168H88a8,8,0,0,0,0-16H40a8,8,0,0,0-8,8v48a8,8,0,0,0,16,0V179.31l14.63,14.63A95.43,95.43,0,0,0,130,222.06h.53a95.36,95.36,0,0,0,67.07-27.33,8,8,0,0,0-11.18-11.44Z"></path></svg>''',
        "acquisition": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M18 18.72a9.094 9.094 0 0 0 3.741-.479 3 3 0 0 0-4.682-2.72m.94 3.198.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0 1 12 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 0 1 6 18.719m12 0a5.971 5.971 0 0 0-.941-3.197m0 0A5.995 5.995 0 0 0 12 12.75a5.995 5.995 0 0 0-5.058 2.772m0 0a3 3 0 0 0-4.681 2.72 8.986 8.986 0 0 0 3.74.477m.94-3.197a5.971 5.971 0 0 0-.94 3.197M15 6.75a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm6 3a2.25 2.25 0 1 1-4.5 0 2.25 2.25 0 0 1 4.5 0Zm-13.5 0a2.25 2.25 0 1 1-4.5 0 2.25 2.25 0 0 1 4.5 0Z" />
</svg>''',
        "retention": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M16.5 18.75h-9m9 0a3 3 0 0 1 3 3h-15a3 3 0 0 1 3-3m9 0v-3.375c0-.621-.503-1.125-1.125-1.125h-.871M7.5 18.75v-3.375c0-.621.504-1.125 1.125-1.125h.872m5.007 0H9.497m5.007 0a7.454 7.454 0 0 1-.982-3.172M9.497 14.25a7.454 7.454 0 0 0 .981-3.172M5.25 4.236c-.982.143-1.954.317-2.916.52A6.003 6.003 0 0 0 7.73 9.728M5.25 4.236V4.5c0 2.108.966 3.99 2.48 5.228M5.25 4.236V2.721C7.456 2.41 9.71 2.25 12 2.25c2.291 0 4.545.16 6.75.47v1.516M7.73 9.728a6.726 6.726 0 0 0 2.748 1.35m8.272-6.842V4.5c0 2.108-.966 3.99-2.48 5.228m2.48-5.492a46.32 46.32 0 0 1 2.916.52 6.003 6.003 0 0 1-5.395 4.972m0 0a6.726 6.726 0 0 1-2.749 1.35m0 0a6.772 6.772 0 0 1-3.044 0" />
</svg>''',
        "funnel": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M12 3c2.755 0 5.455.232 8.083.678.533.09.917.556.917 1.096v1.044a2.25 2.25 0 0 1-.659 1.591l-5.432 5.432a2.25 2.25 0 0 0-.659 1.591v2.927a2.25 2.25 0 0 1-1.244 2.013L9.75 21v-6.568a2.25 2.25 0 0 0-.659-1.591L3.659 7.409A2.25 2.25 0 0 1 3 5.818V4.774c0-.54.384-1.006.917-1.096A48.32 48.32 0 0 1 12 3Z" />
</svg>''',
        "conversion": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />
</svg>''',
        "guide": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M12 6.042A8.967 8.967 0 0 0 6 3.75c-1.052 0-2.062.18-3 .512v14.25A8.987 8.987 0 0 1 6 18c2.305 0 4.408.867 6 2.292m0-14.25a8.966 8.966 0 0 1 6-2.292c1.052 0 2.062.18 3 .512v14.25A8.987 8.987 0 0 0 18 18a8.967 8.967 0 0 0-6 2.292m0-14.25v14.25" />
</svg>''',
        "trend": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 18 9 11.25l4.306 4.306a11.95 11.95 0 0 1 5.814-5.518l2.74-1.22m0 0-5.94-2.281m5.94 2.28-2.28 5.941" />
</svg>''',
        "details": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M166,152a6,6,0,0,1-6,6H96a6,6,0,0,1,0-12h64A6,6,0,0,1,166,152Zm-6-38H96a6,6,0,0,0,0,12h64a6,6,0,0,0,0-12Zm54-66V216a14,14,0,0,1-14,14H56a14,14,0,0,1-14-14V48A14,14,0,0,1,56,34H93.17a45.91,45.91,0,0,1,69.66,0H200A14,14,0,0,1,214,48ZM94,64v2h68V64a34,34,0,0,0-68,0ZM202,48a2,2,0,0,0-2-2H170.33A45.77,45.77,0,0,1,174,64v8a6,6,0,0,1-6,6H88a6,6,0,0,1-6-6V64a45.77,45.77,0,0,1,3.67-18H56a2,2,0,0,0-2,2V216a2,2,0,0,0,2,2H200a2,2,0,0,0,2-2Z"></path></svg>''',
        "performance": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M115.76,155.76l96-96a6,6,0,0,1,8.48,8.48l-96,96a6,6,0,0,1-8.48-8.48ZM128,86a65.9,65.9,0,0,1,21.08,3.44,6,6,0,0,0,3.83-11.38,78,78,0,0,0-102.43,82.6,6,6,0,0,0,6,5.34,5.12,5.12,0,0,0,.67,0,6,6,0,0,0,5.3-6.62A69,69,0,0,1,62,152,66.08,66.08,0,0,1,128,86Zm98,15.9a6,6,0,1,0-10.68,5.48,98.35,98.35,0,0,1,5.16,77.25,2,2,0,0,1-1.91,1.37H37.46a2.07,2.07,0,0,1-1.91-1.41A98.23,98.23,0,0,1,128,54h.9a97,97,0,0,1,43.71,10.72A6,6,0,1,0,178.1,54,108.92,108.92,0,0,0,129,42h-1A110.06,110.06,0,0,0,24.23,188.58,14.08,14.08,0,0,0,37.46,198H218.53a14.06,14.06,0,0,0,13.22-9.37A110.34,110.34,0,0,0,226,101.9Z"></path></svg>''',
        "percentage": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="currentColor" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><path d="M204.24,60.23l-144,144a6,6,0,0,1-8.48-8.48l144-144a6,6,0,1,1,8.48,8.49ZM52,100A34,34,0,1,1,76,110,33.78,33.78,0,0,1,52,100Zm2-24a22,22,0,1,0,6.44-15.56A21.86,21.86,0,0,0,54,76ZM214,180A34,34,0,1,1,204,156,33.78,33.78,0,0,1,214,180Zm-12,0a21.87,21.87,0,0,0-6.44-15.56h0A22,22,0,1,0,202,180Z"></path></svg>''',
        "star": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M11.48 3.499a.562.562 0 0 1 1.04 0l2.125 5.111a.563.563 0 0 0 .475.345l5.518.442c.499.04.701.663.321.988l-4.204 3.602a.563.563 0 0 0-.182.557l1.285 5.385a.562.562 0 0 1-.84.61l-4.725-2.885a.562.562 0 0 0-.586 0L6.982 20.54a.562.562 0 0 1-.84-.61l1.285-5.386a.562.562 0 0 0-.182-.557l-4.204-3.602a.563.563 0 0 1 .321-.988l5.518-.442a.563.563 0 0 0 .475-.345L11.48 3.5Z" />
</svg>''',
        "document": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 0 0-3.375-3.375h-1.5A1.125 1.125 0 0 1 13.5 7.125v-1.5a3.375 3.375 0 0 0-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 0 0-9-9Z" />
</svg>''',
        # Novos ícones para substituir emojis da análise NLP
        "sun": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v2.25m6.364.386-1.591 1.591M21 12h-2.25m-.386 6.364-1.591-1.591M12 18.75V21m-4.773-4.227-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0Z" />
</svg>''',
        "rain": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v2.25m6.364.386-1.591 1.591M21 12h-2.25m-.386 6.364-1.591-1.591M12 18.75V21m-4.773-4.227-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0Z" />
  <path stroke-linecap="round" stroke-linejoin="round" d="M8.25 15.75l3-3m0 0l3 3m-3-3v6" />
</svg>''',
        "balance": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 3.75v4.5m0-4.5h4.5m-4.5 0l9 9m0 0v4.5m0-4.5h-4.5m4.5 0l-9-9" />
</svg>''',
        "warning": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
</svg>''',
        "minus": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 12h-15" />
</svg>''',
        "minus-circle": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M15 12H9m12 0a9 9 0 11-18 0 9 9 0 0118 0z" />
</svg>''',
        "search": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />
</svg>''',
        "clipboard": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M15.666 3.888A2.25 2.25 0 0 0 13.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 0 1-.75.75H9a.75.75 0 0 1-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 0 1-2.25 2.25H6.75A2.25 2.25 0 0 1 4.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 0 1 1.927-.184" />
</svg>''',
        "calendar": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" style="vertical-align:middle;{color_style}"><rect width="18" height="16" x="3" y="5" rx="2" stroke="currentColor" stroke-width="1.5"/><path stroke="currentColor" stroke-width="1.5" stroke-linecap="round" d="M16 3v4M8 3v4M3 9h18"/></svg>''',
        "cycle": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
</svg>''',
        "pin": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M18.375 12.739l-7.693 7.693a4.5 4.5 0 01-6.364-6.364l10.94-10.94A3 3 0 1119.5 7.372L8.552 18.32m.009-.01l-.01.01m5.699-9.941l-7.81 7.81a1.5 1.5 0 002.112 2.13" />
</svg>''',
        "bank": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="{size}" height="{size}" style="vertical-align:middle;{color_style}"><rect width="256" height="256" fill="none"/><polygon points="24 96 232 96 128 32 24 96" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="56" y1="96" x2="56" y2="176" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="104" y1="96" x2="104" y2="176" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="152" y1="96" x2="152" y2="176" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="200" y1="96" x2="200" y2="176" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="32" y1="176" x2="224" y2="176" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><line x1="16" y1="208" x2="240" y2="208" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/></svg>''',
        "revenue_forecast": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="{size}" height="{size}" style="vertical-align:middle;{color_style}"><rect width="256" height="256" fill="none"/><polyline points="224 208 32 208 32 48" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/><polyline points="224 96 160 152 96 104 32 160" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="12"/></svg>''',
        "insights": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.324.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 0 1 1.37.49l1.296 2.247a1.125 1.125 0 0 1-.26 1.431l-1.003.827c-.293.24-.438.613-.431.992a6.759 6.759 0 0 1 0 .255c-.007.378.138.75.43.99l1.005.828c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 0 1-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.57 6.57 0 0 1-.22.128c-.331.183-.581.495-.644.869l-.213 1.28c-.09.543-.56.941-1.11.941h-2.594c-.55 0-1.019-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 0 1-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 0 1-1.369-.49l-1.297-2.247a1.125 1.125 0 0 1 .26-1.431l1.004-.827c.292-.24.437-.613.43-.992a6.932 6.932 0 0 1 0-.255c.007-.378-.138-.75-.43-.99l-1.004-.828a1.125 1.125 0 0 1-.26-1.43l1.297-2.247a1.125 1.125 0 0 1 1.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.087.22-.128.332-.183.582-.495.644-.869l.214-1.281Z" />
  <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
</svg>''',
        "truck": '''<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" width="{size}" height="{size}" style="vertical-align:middle;{color_style}">
  <path stroke-linecap="round" stroke-linejoin="round" d="M8.25 18.75a1.5 1.5 0 0 1-3 0m3 0a1.5 1.5 0 0 0-3 0m3 0h6m-9 0H3.375a1.125 1.125 0 0 1-1.125-1.125V14.25m17.25 4.5a1.5 1.5 0 0 1-3 0m3 0a1.5 1.5 0 0 0-3 0m3 0h1.125c.621 0 1.129-.504 1.09-1.124a17.902 17.902 0 0 0-3.213-9.193 2.056 2.056 0 0 0-1.58-.86H14.25M16.5 18.75h-2.25m0-11.177v-.958c0-.568-.422-1.048-.987-1.106a48.554 48.554 0 0 0-10.026 0 1.106 1.106 0 0 0-.987 1.106v7.635m12-6.677v6.677m0 4.5v-4.5m0 0h-12" />
</svg>''',
        "estrela_digital": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><defs><filter id="glow-estrela" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/><feFlood flood-color="#e3bc44" flood-opacity="0.6" result="color"/><feComposite in="color" in2="blur" operator="in" result="glow"/><feMerge><feMergeNode in="glow"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><path fill="#e3bc44" filter="url(#glow-estrela)" d="M223.85,47.12a16,16,0,0,0-15-15c-12.58-.75-44.73.4-71.41,27.07L132.69,64H74.36A15.91,15.91,0,0,0,63,68.68L28.7,103a16,16,0,0,0,9.07,27.16l38.47,5.37,44.21,44.21,5.37,38.49a15.94,15.94,0,0,0,10.78,12.92,16.11,16.11,0,0,0,5.1.83A15.91,15.91,0,0,0,153,227.3L187.32,193A15.91,15.91,0,0,0,192,181.64V123.31l4.77-4.77C223.45,91.86,224.6,59.71,223.85,47.12ZM74.36,80h42.33L77.16,119.52,40,114.34Zm74.41-9.45a76.65,76.65,0,0,1,59.11-22.47,76.46,76.46,0,0,1-22.42,59.16L128,164.68,91.32,128ZM176,181.64,141.67,216l-5.19-37.17L176,139.31Zm-74.16,9.5C97.34,201,82.29,224,40,224a8,8,0,0,1-8-8c0-42.29,23-57.34,32.86-61.85a8,8,0,0,1,6.64,14.56c-6.43,2.93-20.62,12.36-23.12,38.91,26.55-2.5,36-16.69,38.91-23.12a8,8,0,1,1,14.56,6.64Z"/></svg>''',
        "vaca_leiteira": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><defs><filter id="glow-vaca" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/><feFlood flood-color="#7dc181" flood-opacity="0.6" result="color"/><feComposite in="color" in2="blur" operator="in" result="glow"/><feMerge><feMergeNode in="glow"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><path fill="#7dc181" opacity="0.2" d="M216,48H40a8,8,0,0,0-8,8V192a8,8,0,0,0,8,8H216a8,8,0,0,0,8-8V56A8,8,0,0,0,216,48ZM152,168a40,40,0,1,1,40-40A40,40,0,0,1,152,168Z"/><path fill="#7dc181" filter="url(#glow-vaca)" d="M216,40H40A16,16,0,0,0,24,56V192a16,16,0,0,0,16,16H56v16a8,8,0,0,0,16,0V208H184v16a8,8,0,0,0,16,0V208h16a16,16,0,0,0,16-16V56A16,16,0,0,0,216,40Zm0,152H40V56H216v64H199.32a48,48,0,1,0,0,16H216v56Zm-50.16-72a16,16,0,1,0,0,16H183a32,32,0,1,1,0-16Z"/></svg>''',
        "interrogacao": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><defs><filter id="glow-interrogacao" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/><feFlood flood-color="#5dade2" flood-opacity="0.6" result="color"/><feComposite in="color" in2="blur" operator="in" result="glow"/><feMerge><feMergeNode in="glow"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><path fill="#5dade2" opacity="0.2" d="M128,32a96,96,0,1,0,96,96A96,96,0,0,0,128,32Zm16,112L80,176l32-64,64-32Z"/><path fill="#5dade2" filter="url(#glow-interrogacao)" d="M128,24A104,104,0,1,0,232,128,104.11,104.11,0,0,0,128,24Zm0,192a88,88,0,1,1,88-88A88.1,88.1,0,0,1,128,216ZM172.42,72.84l-64,32a8.05,8.05,0,0,0-3.58,3.58l-32,64A8,8,0,0,0,80,184a8.1,8.1,0,0,0,3.58-.84l64-32a8.05,8.05,0,0,0,3.58-3.58l32-64a8,8,0,0,0-10.74-10.74ZM138,138,97.89,158.11,118,118l40.15-20.07Z"/></svg>''',
        "abacaxi": '''<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 256 256" style="vertical-align:middle;{color_style}"><defs><filter id="glow-abacaxi" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/><feFlood flood-color="#c34444" flood-opacity="0.6" result="color"/><feComposite in="color" in2="blur" operator="in" result="glow"/><feMerge><feMergeNode in="glow"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><path fill="#c34444" opacity="0.2" d="M216,208H40a16,16,0,0,1-13.84-24l88-152a16,16,0,0,1,27.7,0l88,152A16,16,0,0,1,216,208Z"/><path fill="#c34444" filter="url(#glow-abacaxi)" d="M96,208a8,8,0,0,1-8,8H40a24,24,0,0,1-20.77-36l34.29-59.25L39.47,124.5A8,8,0,1,1,35.33,109l32.77-8.77a8,8,0,0,1,9.8,5.66l8.79,32.77A8,8,0,0,1,81,148.5a8.37,8.37,0,0,1-2.08.27,8,8,0,0,1-7.72-5.93l-3.8-14.15L33.11,188A8,8,0,0,0,40,200H88A8,8,0,0,1,96,208Zm140.73-28-23.14-40a8,8,0,0,0-13.84,8l23.14,40A8,8,0,0,1,216,200H147.31l10.34-10.34a8,8,0,0,0-11.31-11.32l-24,24a8,8,0,0,0,0,11.32l24,24a8,8,0,0,0,11.31-11.32L147.31,216H216a24,24,0,0,0,20.77-36ZM128,32a7.85,7.85,0,0,1,6.92,4l34.29,59.25-14.08-3.78A8,8,0,0,0,151,106.92l32.78,8.79a8.23,8.23,0,0,0,2.07.27,8,8,0,0,0,7.72-5.93l8.79-32.79a8,8,0,1,0-15.45-4.14l-3.8,14.17L148.77,28a24,24,0,0,0-41.54,0L84.07,68a8,8,0,0,0,13.85,8l23.16-40A7.85,7.85,0,0,1,128,32Z"/></svg>''',
    }
    color_style = f'color: {color};' if color else ''
    # Ícones externos em arquivos SVG (pasta components/img)
    if name == "portfolio_map":
        return _load_external_svg("corr_svg.svg", size, color_style)
    if name == "microphone":
        return _load_external_svg("microphone_5853686.svg", size, color_style)
    if name == "alert_trumpet":
        return _load_external_svg("alert_trumpet.svg", size, color_style)
    if name == "nps_line":
        return _load_external_svg("nps_line.svg", size, color_style)
    if name == "check_list":
        return _load_external_svg("check-list312423.svg", size, color_style)
    if name == "customer_acqui":
        return _load_external_svg("customer-acqui.svg", size, color_style)
    if name == "caution":
        return _load_external_svg("caution.svg", size, color_style)
    if name == "portfolio":
        return _load_external_svg("portfolio.svg", size, color_style)
    if name == "orders_ts":
        return _load_external_svg("orders-ts.svg", size, color_style)
    if name == "price_tag":
        return _load_external_svg("price-tag.svg", size, color_style)
    if name == "delivery_man":
        return _load_external_svg("delivery-man.svg", size, color_style)
    if name == "executive_resume":
        return _load_external_svg("executive-resume.svg", size, color_style)
    if name == "line_analysis":
        return _load_external_svg("line-analysis.svg", size, color_style)
    if name == "customer_behaviour":
        return _load_external_svg("customer-behaviour.svg", size, color_style)
    if name == "pop_prem":
        return _load_external_svg("pop-prem.svg", size, color_style)
    if name == "forecast":
        return _load_external_svg("forecast.svg", size, color_style)
    if name == "calendar_icon":
        return _load_external_svg("calendar-icon.svg", size, color_style)
    if name == "rentabilidade_estrategica":
        return _load_external_svg("rentabilidade-estrategica.svg", size, color_style)
    if name == "insight_final":
        return _load_external_svg("insight-final.svg", size, color_style)
    if name == "machine_working":
        return _load_external_svg("machine-working.svg", size, color_style)

    svg = svgs.get(name, '').format(size=size, color_style=color_style)
    return svg 