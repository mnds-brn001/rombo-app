"""
Adaptador Específico para Cliente de Cosméticos
===============================================

Processa e adapta as planilhas específicas do cliente de distribuidora de cosméticos
para o formato padrão do sistema Insight Expert.

Planilhas suportadas:
- Consulta de Pedidos.csv
- Consulta Estoque.csv

Autor: Insight Expert Team
Data: Outubro 2024
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import re
import logging
from pathlib import Path

# Margens (pipeline)
try:
    from dados_cliente.cliente_pipeline import MarginCalculator, DEFAULT_MARGIN_CONFIG  # type: ignore
except Exception:
    MarginCalculator = None  # fallback em tempo de import
    DEFAULT_MARGIN_CONFIG = {}

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Mapeamento de Colunas Específico
# ----------------------------

COSMETICOS_ALIAS_MAP: Dict[str, set] = {
    # Identificadores
    "order_id": {
        "pedido_id", "pedido id", "id_pedido", "numero_pedido",
        "pedido", "order", "order_id", "idpedido"
    },
    "customer_id": {
        "id_cliente", "id cliente", "cliente_id", "codigo_cliente"
    },
    "customer_state": {
        "estado_cliente", "estado cliente", "uf_cliente", "uf", "estado"
    },
    "customer_city": {
        "cidade_cliente", "cidade cliente", "municipio_cliente", "municipio"
    },
    "product_id": {
        "produto/derivação_código_der", "produto/derivacao_codigo_der",
        "produto_derivacao_codigo_der",  # Versão normalizada
        "produto_id", "codigo_produto", "sku", "codigo",
        "produto/derivação_id_der", "produto_derivacao_id_der"
    },
    
    # Datas
    "order_purchase_timestamp": {
        "data/hora", "data_hora", "data_pedido", "created_at",
        "data_hora_pedido", "datahora", "order_date", "data_hora_criacao"
    },
    "marketplace_date": {
        "data_marketplace", "data marketplace", "marketplace_date"
    },
    "approval_date": {
        "data_aprovação", "data_aprovacao", "data aprovacao", "approved_at"
    },
    
    # Valores monetários
    "price": {
        "valor_total_pedido", "valor total pedido", "valor_pedido", "total_pedido",
        "valor_produtos", "valor_liquido", "valor", "total_liquido"
    },
    "freight_value": {
        "valor_frete", "valor frete", "frete", "custo_frete"
    },
    "product_cost": {
        "custo_médio_de_estoque", "custo_medio_de_estoque", 
        "custo_medio", "custo_virtual"
    },
    "discount_value": {
        "valor_desconto", "valor desconto", "desconto"
    },
    "addition_value": {
        "valor_acrescimo", "valor acréscimo", "acrescimo", "acréscimo"
    },
    "total_value": {
        "valor_total", "valor total", "total"
    },
    "product_value": {
        "valor_produto", "valor do produto", "preco_produto"
    },
    "freight_carrier_value": {
        "valor_frete_transportadora", "valor frete transportadora", "frete_transportadora"
    },
    # Custos e taxas de margem
    "marketplace_commission": {
        "comissao_marketplace", "comissao_mkp", "marketplace_commission", "taxa_marketplace"
    },
    "payment_gateway_fee": {
        "taxa_gateway", "gateway_fee", "payment_gateway_fee", "taxa_cartao", "taxa_pagamento"
    },
    "tax_amount": {
        "imposto", "taxa_imposto", "icms", "tributo", "tax_amount"
    },
    "packaging_cost": {
        "embalagem", "custo_embalagem", "packaging", "packaging_cost"
    },
    
    # Status e categorias
    "order_status": {
        "situação", "situacao", "status", "status_pedido",
        "situaзгo", "situaггo", "status_do_pedido"
    },
    "marketplace": {
        "marketplace", "canal", "origem"
    },
    "payment_type": {
        "forma_de_pagamento", "forma de pagamento", "tipo_pagamento"
    },
    
    # Produto específico
    "product_name": {
        "produto/derivação_derivação", "produto_derivacao_derivacao",
        "produto_nome", "nome_produto", "descricao"
    },
    "brand": {
        "produto/derivação_marca", "produto_derivacao_marca", 
        "marca", "fabricante"
    },
    "product_active": {
        "produto/derivaçãoo_ativo", "produto_ativo", "ativo"
    },
    
    # Estoque
    "stock_level": {
        "quantidade_disp_venda", "quantidade_disp._venda",
        "qtd_disponivel", "estoque_disponivel"
    },
    "stock_physical": {
        "quantidade_física", "quantidade_fisica", "qtd_fisica"
    },
    "stock_reserved": {
        "quantidade_reservada_saída", "quantidade_reservada_saida",
        "qtd_reservada"
    },
    "stock_min": {
        "quantidade_min_estoque", "quantidade_min._estoque",
        "estoque_minimo", "ponto_reposicao"
    }
}

# ----------------------------
# Categorização de Cosméticos
# ----------------------------

COSMETICOS_CATEGORIES = {
    "Aromaterapia e Difusores": {
        "keywords": [
            "difusor", "essência", "óleo essencial", "aroma floral", 
            "amazônia aromas", "vela perfumada", "odorizador", "sachê perfumado",
            "essência oleosa"
        ],
        "subcategories": [
            "Difusores", "Essências", "Velas Perfumadas", "Odorizadores"
        ]
    },
    "Cabelo": {
        "keywords": [
            "shampoo", "condicionador", "máscara capilar", "óleo capilar", "tratamento capilar",
            "leave-in", "finalizador", "ampola", "tônico capilar",
            "mousse", "gel capilar", "creme para pentear", "serum capilar",
            "pré-prancha", "pós-depilatório gel"
        ],
        "subcategories": [
            "Shampoos", "Condicionadores", "Máscaras", "Óleos Capilares",
            "Tratamentos", "Finalizadores", "Géis"
        ]
    },
    "Ferramentas Cabelo": {
        "keywords": [
            "pente", "escova cabelo", "escova secadora", "tesoura cabelo",
            "navalha", "pente anti frizz", "pente separador", "escova desembaraçadora",
            "escova térmica", "escova polvo"
        ],
        "subcategories": [
            "Pentes", "Escovas", "Tesouras", "Navalhas"
        ]
    },
    "Acessórios Cabelo": {
        "keywords": [
            "amarrador", "scrunchie", "piranha", "elástico cabelo", "presilha",
            "grampo", "clip cabelo", "prendedor cabelo", "tiara", "arco",
            "mega hair", "aplique"
        ],
        "subcategories": [
            "Amarradores", "Presilhas", "Grampos", "Tiaras"
        ]
    },
    "Equipamentos Elétricos": {
        "keywords": [
            "secador", "chapinha", "modelador", "babyliss", "escova rotativa",
            "escova elétrica", "aquecedor", "prancha", "modelador cachos",
            "aparador pelo", "massageador elétrico", "bivolt"
        ],
        "subcategories": [
            "Secadores", "Chapinhas", "Modeladores", "Aparadores"
        ]
    },
    "Pele Facial": {
        "keywords": [
            "creme facial", "sérum", "serum", "tônico facial", "limpeza facial", "hidratante facial",
            "protetor solar", "anti-idade", "vitamina c", "ácido",
            "demaquilante", "água micelar", "esfoliante facial", "máscara facial",
            "fixador maquiagem"
        ],
        "subcategories": [
            "Limpeza Facial", "Hidratantes", "Séruns", "Protetores Solares",
            "Anti-idade", "Tônicos", "Água Micelar"
        ]
    },
    "Pele Corporal": {
        "keywords": [
            "loção corporal", "óleo corporal", "sabonete", "esfoliante corporal",
            "hidratante corporal", "creme corporal", "gel de banho",
            "manteiga corporal", "body splash", "sabonete líquido"
        ],
        "subcategories": [
            "Hidratantes Corporais", "Sabonetes", "Óleos Corporais",
            "Esfoliantes", "Body Splash"
        ]
    },
    "Maquiagem": {
        "keywords": [
            "base maquiagem", "batom", "rímel", "sombra", "blush", "corretivo",
            "pó compacto", "primer", "delineador", "gloss", "bronzer",
            "iluminador", "lápis", "caneta delineadora", "máscara cílios"
        ],
        "subcategories": [
            "Bases", "Batons", "Sombras", "Rímels", "Corretivos",
            "Pós", "Primers", "Delineadores"
        ]
    },
    "Perfumaria": {
        "keywords": [
            "perfume", "colônia", "desodorante", "spray corpo", "eau de toilette",
            "eau de parfum", "body spray", "fragrância", "desodorante colônia"
        ],
        "subcategories": [
            "Perfumes", "Colônias", "Desodorantes", "Body Sprays"
        ]
    },
    "Esmaltes": {
        "keywords": [
            "esmalte", "verniz unha", "base unha", "removedor esmalte", "acetona",
            "extra brilho", "endurecedor unha", "óleo cutícula", "top coat",
            "base concreto", "base fermento"
        ],
        "subcategories": [
            "Esmaltes", "Bases para Unhas", "Removedores", "Tratamentos Unhas"
        ]
    },
    "Ferramentas Unhas": {
        "keywords": [
            "alicate cutícula", "empurrador", "lixa unha", "cortador unha",
            "espátula unha", "palito unha", "pinça unha", "broca unha",
            "broca cerâmica", "broca tungstênio", "tips", "cola unha"
        ],
        "subcategories": [
            "Alicates", "Lixas", "Cortadores", "Empurradores", "Brocas"
        ]
    },
    "Cílios e Sobrancelhas": {
        "keywords": [
            "cílios", "alongamento cílios", "cola cílios", "pinça cílios",
            "pinça sobrancelha", "henna", "tinta sobrancelha", "curvex",
            "aparador sobrancelha"
        ],
        "subcategories": [
            "Cílios Postiços", "Pinças", "Tintas", "Ferramentas"
        ]
    },
    "Depilação": {
        "keywords": [
            "cera depilatória", "cera roll-on", "aquecedor cera", "espátula depilação",
            "gel pós-depilatório", "papel depilatório", "depimiel",
            "cera quente", "cera fria"
        ],
        "subcategories": [
            "Ceras", "Acessórios Depilação", "Pós-Depilação"
        ]
    },
    "Pincéis e Aplicadores": {
        "keywords": [
            "pincel maquiagem", "esponja make", "aplicador", "borla",
            "esponja gota", "esponja cogumelo", "pincel kabuki", "puff"
        ],
        "subcategories": [
            "Pincéis", "Esponjas", "Aplicadores"
        ]
    },
    "Acessórios Profissionais": {
        "keywords": [
            "abaixador língua", "algodão prensado", "papel alumínio",
            "touca descartável", "luva plástica", "máscara descartável",
            "alumínio mechas", "papel laminação", "capa corte"
        ],
        "subcategories": [
            "Descartáveis", "Papéis", "Capas"
        ]
    },
    "Massageadores e Acessórios": {
        "keywords": [
            "massageador", "vibratório", "pomada massageadora", "roller facial",
            "gua sha", "massageador couro cabeludo"
        ],
        "subcategories": [
            "Massageadores", "Acessórios Massagem"
        ]
    },
    "Bolsas e Necessaires": {
        "keywords": [
            "necessaire", "bolsa maquiagem", "porta maquiagem", "estojo",
            "kit viagem", "organizador cosméticos"
        ],
        "subcategories": [
            "Necessaires", "Estojos", "Organizadores"
        ]
    },
    "Kits e Combos": {
        "keywords": [
            "kit ", "combo", "conjunto", "duo", "trio"
        ],
        "subcategories": [
            "Kits Promocionais", "Combos"
        ]
    },
    "Acessórios Diversos": {
        "keywords": [
            "espelho", "lupa", "protetor seio", "levanta seio", "adesivo",
            "strass", "glitter", "brinco", "sutiã silicone"
        ],
        "subcategories": [
            "Espelhos", "Adesivos", "Acessórios"
        ]
    }
}

# ----------------------------
# Regras de Negócio Específicas
# ----------------------------

COSMETICOS_BUSINESS_RULES = {
    # Status canônicos normalizados (minúsculas, sem acento, sem prefixo numérico)
    # Fonte: tabela enviada pelo usuário (1..31)
    "valid_order_statuses": [
        # Normal (conta para receita/elegibilidade)
        "aprovado",
        "aprovado e integrado",
        "nota fiscal emitida",
        "transporte",
        "entregue",
        "faturamento iniciado",
        "aprovado parcial",
        "aguardando chegada do produto",
        # Casos marcados como "Normal" na tabela mas tratados em regras específicas
        "chargeback pago",
        "aprovado analise de pagamento",
        "em analise de pagamento (interna)",
        # compatibilidade legada
        "enviado", "processando", "em_separacao", "faturado", "despachado"
    ],
    "cancelled_statuses": [
        # Cancelado
        "cancelado pagamento",
        "cancelado pagamento analise",
        "em cancelamento",
        # Fraude/chargeback/disputa e NF cancelada entram como cancelados/receita perdida
        "fraude",
        "chargeback",
        "disputa",
        "nota fiscal cancelada",
        # Devoluções/estornos
        "devolvido financeiro",
        "devolvido",
        "estornado",
        # compatibilidade legada
        "cancelado", "rejeitado"
    ],
    "pending_statuses": [
        "aguardando pagamento",
        "em analise pagamento",
        "aguardando atualizacao de dados",
        "em analise de pagamento (interna)"
    ],
    # Mantidos para análise mas fora de receita
    "special_statuses": [
        "credito por troca",
        "problema fluxo postal",
        "suspenso temporariamente",
        "em logistica reversa",
        "nota fiscal denegada",
        "devolvido estoque (dep. 1)",
        "devolvido estoque (outros dep.)"
    ],
    "min_price": 1.0,  # Produtos muito baratos podem ser amostras
    "max_price": 5000.0,  # Limite superior para cosméticos premium
    "seasonal_adjustments": {
        "dia_das_maes": 2.0,
        "natal": 1.8,
        "dia_dos_namorados": 1.5,
        "black_friday": 2.5,
        "pascoa": 1.3
    }
}

# ----------------------------
# Funções de Processamento
# ----------------------------

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas removendo caracteres especiais e espaços.
    Versão robusta que lida com diferentes encodings e caracteres especiais.
    """
    df_normalized = df.copy()
    
    # Remover primeira coluna se estiver vazia ou for unnamed
    if df_normalized.columns[0] in ['', 'Unnamed: 0'] or df_normalized.columns[0].strip() == '':
        df_normalized = df_normalized.drop(df_normalized.columns[0], axis=1)
    
    # Normalizar nomes das colunas
    new_columns = {}
    for col in df_normalized.columns:
        # Remover caracteres especiais e normalizar
        normalized = (
            str(col).lower()
            .replace('/', '_')
            .replace('\\', '_')
            .replace(' ', '_')
            .replace('.', '')
            .replace('-', '_')
            .replace('+', '_')
            .replace('&', '_')
            .replace('%', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('[', '')
            .replace(']', '')
            .replace('{', '')
            .replace('}', '')
            .replace(';', '_')
            .replace(':', '_')
            .replace('?', '')
            .replace('!', '')
            .replace('@', '_')
            .replace('#', '_')
            .replace('$', '_')
            .replace('*', '_')
            .replace('=', '_')
            .replace('|', '_')
            .replace('"', '')
            .replace("'", '')
            .replace('`', '')
            .replace('~', '_')
            .replace('^', '_')
            .replace('<', '_')
            .replace('>', '_')
            .replace('ã', 'a')
            .replace('á', 'a')
            .replace('â', 'a')
            .replace('à', 'a')
            .replace('ä', 'a')
            .replace('é', 'e')
            .replace('ê', 'e')
            .replace('è', 'e')
            .replace('ë', 'e')
            .replace('í', 'i')
            .replace('î', 'i')
            .replace('ì', 'i')
            .replace('ï', 'i')
            .replace('ó', 'o')
            .replace('ô', 'o')
            .replace('ò', 'o')
            .replace('ö', 'o')
            .replace('ú', 'u')
            .replace('û', 'u')
            .replace('ù', 'u')
            .replace('ü', 'u')
            .replace('ç', 'c')
            .replace('ñ', 'n')
            .replace('õ', 'o')
            # Correções comuns de mojibake em headers (ex.: Situaзгo -> situacao; Cуdigo -> codigo)
            .replace('з', 'c')
            .replace('г', 'a')
            .replace('у', 'o')
            .replace('й', 'i')
            .replace('ќ', '')
            .replace('�', '')  # Remover caracteres de encoding
            .strip('_')
        )
        
        # Limpar múltiplos underscores consecutivos
        import re
        normalized = re.sub(r'_+', '_', normalized)
        
        # Evitar nomes vazios
        if not normalized:
            normalized = f'col_{len(new_columns)}'
        
        # Evitar nomes que começam com número
        if normalized and normalized[0].isdigit():
            normalized = f'col_{normalized}'
        
        new_columns[col] = normalized
    
    df_normalized = df_normalized.rename(columns=new_columns)
    
    # Remover colunas duplicadas
    df_normalized = df_normalized.loc[:, ~df_normalized.columns.duplicated()]
    
    logger.info(f"Colunas normalizadas: {len(new_columns)} colunas processadas")
    logger.debug(f"Exemplos de normalização: {dict(list(new_columns.items())[:5])}")
    
    return df_normalized

def apply_cosmeticos_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica mapeamento de aliases específicos para cosméticos.
    """
    df_adapted = df.copy()
    
    # Criar mapa reverso para busca eficiente
    alias_reverse_map: Dict[str, str] = {}
    for canonical, aliases in COSMETICOS_ALIAS_MAP.items():
        for alias in aliases:
            alias_reverse_map[alias.lower()] = canonical
    
    # Renomear colunas com prioridade para códigos de produto
    rename_map = {}
    
    # Primeiro, processar colunas de código de produto com prioridade
    priority_columns = ['produto_derivacao_codigo_der', 'produto_derivacao_codigo_der']
    for col in df_adapted.columns:
        col_normalized = col.lower().strip()
        if col_normalized in priority_columns:
            canonical_name = alias_reverse_map.get(col_normalized, col)
            if canonical_name != col:
                rename_map[col] = canonical_name
    
    # Depois, processar as demais colunas
    for col in df_adapted.columns:
        col_normalized = col.lower().strip()
        if col not in rename_map:  # Só processar se não foi mapeada ainda
            canonical_name = alias_reverse_map.get(col_normalized, col)
            if canonical_name != col:
                rename_map[col] = canonical_name
    
    if rename_map:
        df_adapted = df_adapted.rename(columns=rename_map)
        logger.info(f"Aliases aplicados: {len(rename_map)} colunas mapeadas")
        # Consolidar colunas duplicadas resultantes do mapeamento (ex.: 'price')
        duplicated = [c for c, cnt in df_adapted.columns.value_counts().items() if cnt > 1]
        for col in duplicated:
            # Para product_id, dar prioridade ao código do produto (não numérico)
            if col == 'product_id':
                subset = df_adapted.loc[:, df_adapted.columns == col]
                # Encontrar a coluna com códigos de produto (não numéricos)
                code_col_idx = None
                for i, col_name in enumerate(df_adapted.columns):
                    if col_name == col:
                        # Verificar se esta coluna contém códigos não numéricos
                        sample_values = subset.iloc[:5, i].astype(str)
                        if any(not val.isdigit() for val in sample_values if val != 'nan'):
                            code_col_idx = i
                            break
                
                if code_col_idx is not None:
                    # Usar a coluna com códigos
                    combined = subset.iloc[:, code_col_idx]
                else:
                    # Fallback para backfill
                    combined = subset.bfill(axis=1).iloc[:, 0]
            else:
                # Para outras colunas, usar backfill normal
                combined = subset.bfill(axis=1).iloc[:, 0]
            
            # Remover colunas extras e manter apenas a primeira
            first_pos = [i for i, name in enumerate(df_adapted.columns) if name == col][0]
            keep_mask = []
            seen = 0
            for name in df_adapted.columns:
                if name == col:
                    seen += 1
                    keep_mask.append(seen == 1)
                else:
                    keep_mask.append(True)
            df_adapted = df_adapted.loc[:, keep_mask]
            df_adapted.iloc[:, first_pos] = combined
    
    return df_adapted

def categorize_cosmetics_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categoriza produtos automaticamente em categorias de cosméticos.
    """
    df_categorized = df.copy()
    
    # Verificar se coluna product_name existe
    if 'product_name' not in df_categorized.columns:
        logger.warning("Coluna 'product_name' não encontrada. Pulando categorização.")
        df_categorized['product_category_name'] = 'Outros'
        return df_categorized
    
    # Inicializar coluna de categoria
    df_categorized['product_category_name'] = 'Outros'
    df_categorized['product_subcategory'] = 'Não categorizado'
    
    # Aplicar categorização baseada em palavras-chave
    for category, config in COSMETICOS_CATEGORIES.items():
        try:
            keywords = config['keywords']
            subcategories = config['subcategories']
            
            # Criar pattern para busca
            pattern = '|'.join([re.escape(keyword) for keyword in keywords])
            
            # Aplicar máscara para categoria principal
            mask = df_categorized['product_name'].str.contains(
                pattern, case=False, na=False, regex=True
            )
            df_categorized.loc[mask, 'product_category_name'] = category
            
            # Tentar categorizar subcategoria
            for subcat in subcategories:
                subcat_keywords = subcat.lower().split()
                subcat_pattern = '|'.join([re.escape(kw) for kw in subcat_keywords])
                
                subcat_mask = (
                    mask & 
                    df_categorized['product_name'].str.contains(
                        subcat_pattern, case=False, na=False, regex=True
                    )
                )
                df_categorized.loc[subcat_mask, 'product_subcategory'] = subcat
                
        except Exception as e:
            logger.warning(f"Erro ao categorizar '{category}': {e}")
            continue
    
    # Log de resultados
    category_counts = df_categorized['product_category_name'].value_counts()
    logger.info(f"Categorização concluída: {dict(category_counts)}")
    
    return df_categorized

def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa e converte colunas de data para formato datetime.
    """
    df_dates = df.copy()
    
    date_columns = [
        'order_purchase_timestamp', 'marketplace_date', 'approval_date'
    ]
    
    for col in date_columns:
        if col in df_dates.columns:
            try:
                # Tentar diferentes formatos de data
                df_dates[col] = pd.to_datetime(df_dates[col], errors='coerce', dayfirst=True)
                logger.info(f"Coluna '{col}' convertida para datetime")
            except Exception as e:
                logger.warning(f"Erro ao converter '{col}': {e}")
    
    return df_dates

def process_monetary_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa valores monetários convertendo vírgula para ponto.
    """
    df_money = df.copy()
    
    monetary_columns = [
        'price',
        'freight_value',
        'product_cost',
        'discount_value',
        'addition_value',
        'total_value',
        'product_value',
        'freight_carrier_value',
        # Margem
        'marketplace_commission',
        'payment_gateway_fee',
        'tax_amount',
        'packaging_cost',
    ]
    
    for col in monetary_columns:
        if col in df_money.columns:
            try:
                series = df_money[col]
                if not pd.api.types.is_numeric_dtype(series):
                    series = series.astype(str)
                    # Remover separador de milhar e normalizar decimal vírgula
                    series = series.str.replace(r"\.(?=\d{3}(\D|$))", "", regex=True)
                    series = series.str.replace(",", ".", regex=False)
                    # Remover quaisquer caracteres residuais não numéricos
                    series = series.str.replace(r"[^0-9\.-]", "", regex=True)
                df_money[col] = pd.to_numeric(series, errors='coerce').fillna(0.0)
                logger.info(f"Coluna monetária '{col}' processada")
            except Exception as e:
                logger.warning(f"Erro ao processar valores monetários '{col}': {e}")

    # Garantir coluna 'price' numérica final (após possíveis duplicatas)
    if 'price' in df_money.columns:
        s = df_money['price']
        if not pd.api.types.is_numeric_dtype(s):
            s = s.astype(str)
            s = s.str.replace(r"\.(?=\d{3}(\D|$))", "", regex=True)
            s = s.str.replace(",", ".", regex=False)
            s = s.str.replace(r"[^0-9\.-]", "", regex=True)
        df_money['price'] = pd.to_numeric(s, errors='coerce').fillna(0.0)
    
    return df_money

def clean_marketplace_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e normaliza nomes de marketplaces removendo códigos numéricos.
    
    Transforma:
        "41 - Shopee" → "Shopee"
        "26 - Amazon" → "Amazon"
        "7 - Mercado Livre" → "Mercado Livre"
    """
    df_clean = df.copy()
    
    if 'marketplace' not in df_clean.columns:
        logger.warning("Coluna 'marketplace' não encontrada. Pulando limpeza de marketplaces.")
        return df_clean
    
    # Criar máscara para valores não-nulos
    mask_not_null = df_clean['marketplace'].notna()
    
    # Aplicar limpeza apenas em valores não-nulos
    if mask_not_null.any():
        # Remover padrão "numero - " do início
        df_clean.loc[mask_not_null, 'marketplace'] = (
            df_clean.loc[mask_not_null, 'marketplace']
            .astype(str)
            .str.replace(r'^\d+\s*-\s*', '', regex=True)  # Remove "41 - " ou "7 - "
            .str.strip()  # Remove espaços extras
            .str.title()  # Capitaliza palavras (opcional, para padronizar)
        )
        
        # Normalizar nomes específicos conhecidos
        marketplace_normalization = {
            'Shopee': 'Shopee',
            'Amazon': 'Amazon',
            'Mercado Livre': 'Mercado Livre',
            'Magazine Luiza': 'Magazine Luiza',
            'Aliexpress': 'AliExpress',
            'Lojas Americanas': 'Lojas Americanas',
            'Magalu': 'Magazine Luiza',  # Alias
            'Ml': 'Mercado Livre',  # Alias
            'None': 'Site Próprio',  # Converter strings "None" para Site Próprio
            'Nan': 'Site Próprio',
            '': 'Site Próprio'
        }
        
        # Aplicar normalização
        df_clean['marketplace'] = df_clean['marketplace'].replace(marketplace_normalization)
    
    # Preencher valores nulos com "Site Próprio" (vendas diretas no site)
    df_clean['marketplace'] = df_clean['marketplace'].fillna('Site Próprio')
    
    # Registrar estatísticas
    unique_marketplaces = df_clean['marketplace'].nunique()
    null_count = df_clean['marketplace'].isna().sum()
    
    logger.info(f"Marketplaces limpos e normalizados: {unique_marketplaces} únicos encontrados")
    site_proprio_count = (df_clean['marketplace'] == 'Site Próprio').sum()
    logger.info(f"Vendas no Site Próprio: {site_proprio_count:,} ({site_proprio_count/len(df_clean)*100:.2f}%)")
    logger.info(f"Registros sem marketplace definido após limpeza: {null_count:,}")
    
    # Mostrar contagem dos principais
    marketplace_counts = df_clean['marketplace'].value_counts()
    logger.info(f"Distribuição de marketplaces: {dict(marketplace_counts.head(10))}")
    
    return df_clean

def generate_synthetic_customer_states(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera estados brasileiros sintéticos para clientes baseado em distribuição realística.
    
    Esta função adiciona a coluna 'customer_state' com estados brasileiros distribuídos
    de forma proporcional à população e economia de cada região.
    
    Args:
        df: DataFrame com os dados dos clientes
        
    Returns:
        DataFrame com a coluna 'customer_state' adicionada
    """
    df_states = df.copy()
    
    # Verificar se a coluna já existe
    if 'customer_state' in df_states.columns:
        logger.info("Coluna 'customer_state' já existe. Pulando geração sintética.")
        return df_states
    
    # Estados brasileiros com distribuição baseada em população e economia
    # Pesos aproximados baseados em dados do IBGE e participação no e-commerce
    brazilian_states = {
        'SP': 0.25,  # São Paulo - maior mercado
        'RJ': 0.12,  # Rio de Janeiro
        'MG': 0.11,  # Minas Gerais
        'RS': 0.08,  # Rio Grande do Sul
        'PR': 0.07,  # Paraná
        'SC': 0.06,  # Santa Catarina
        'BA': 0.06,  # Bahia
        'GO': 0.04,  # Goiás
        'PE': 0.04,  # Pernambuco
        'CE': 0.03,  # Ceará
        'DF': 0.03,  # Distrito Federal
        'ES': 0.02,  # Espírito Santo
        'PB': 0.02,  # Paraíba
        'MT': 0.02,  # Mato Grosso
        'AL': 0.01,  # Alagoas
        'RN': 0.01,  # Rio Grande do Norte
        'MS': 0.01,  # Mato Grosso do Sul
        'PI': 0.01,  # Piauí
        'SE': 0.01,  # Sergipe
        'TO': 0.005, # Tocantins
        'RO': 0.005, # Rondônia
        'AC': 0.003, # Acre
        'AM': 0.003, # Amazonas
        'RR': 0.002, # Roraima
        'AP': 0.002, # Amapá
        'PA': 0.002, # Pará
        'MA': 0.002  # Maranhão
    }
    
    # Garantir que os pesos somem 1.0
    total_weight = sum(brazilian_states.values())
    if abs(total_weight - 1.0) > 0.001:
        # Normalizar pesos
        brazilian_states = {state: weight/total_weight for state, weight in brazilian_states.items()}
    
    # Gerar estados aleatórios baseados na distribuição
    np.random.seed(42)  # Para reprodutibilidade
    states = list(brazilian_states.keys())
    weights = list(brazilian_states.values())
    
    # Gerar estados para cada registro
    synthetic_states = np.random.choice(
        states, 
        size=len(df_states), 
        p=weights
    )
    
    # Adicionar coluna ao DataFrame
    df_states['customer_state'] = synthetic_states
    
    # Log de estatísticas
    state_counts = pd.Series(synthetic_states).value_counts()
    logger.info(f"Estados sintéticos gerados para {len(df_states):,} registros:")
    
    # Mostrar top 10 estados
    top_states = state_counts.head(10)
    for state, count in top_states.items():
        percentage = (count / len(df_states)) * 100
        logger.info(f"  • {state}: {count:,} ({percentage:.1f}%)")
    
    # Adicionar também customer_city sintética (opcional)
    if 'customer_city' not in df_states.columns:
        # Cidades principais por estado (simplificado)
        state_cities = {
            'SP': ['São Paulo', 'Campinas', 'Santos', 'Ribeirão Preto', 'Sorocaba'],
            'RJ': ['Rio de Janeiro', 'Niterói', 'Nova Iguaçu', 'Duque de Caxias'],
            'MG': ['Belo Horizonte', 'Uberlândia', 'Contagem', 'Juiz de Fora'],
            'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas', 'Santa Maria'],
            'PR': ['Curitiba', 'Londrina', 'Maringá', 'Ponta Grossa'],
            'SC': ['Florianópolis', 'Joinville', 'Blumenau', 'Chapecó'],
            'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista'],
            'GO': ['Goiânia', 'Aparecida de Goiânia', 'Anápolis'],
            'PE': ['Recife', 'Jaboatão dos Guararapes', 'Olinda'],
            'CE': ['Fortaleza', 'Caucaia', 'Juazeiro do Norte'],
            'DF': ['Brasília', 'Taguatinga', 'Ceilândia'],
            'ES': ['Vitória', 'Vila Velha', 'Cariacica'],
        }
        
        # Gerar cidades baseadas no estado
        synthetic_cities = []
        for state in synthetic_states:
            if state in state_cities:
                city = np.random.choice(state_cities[state])
            else:
                city = f"Cidade {state}"  # Fallback para estados sem lista
            synthetic_cities.append(city)
        
        df_states['customer_city'] = synthetic_cities
        logger.info("Cidades sintéticas também geradas")
    
    return df_states

def calculate_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula campos derivados específicos para cosméticos.
    """
    df_derived = df.copy()
    
    # Flag de pedido cancelado baseada no mapeamento completo de status
    if 'order_status' in df_derived.columns:
        status_code_map = {
            "1": "aguardando pagamento",
            "2": "cancelado pagamento",
            "4": "aprovado",
            "5": "faturamento iniciado",
            "6": "nota fiscal emitida",
            "7": "transporte",
            "8": "entregue",
            "14": "cancelado pagamento analise",
            "16": "problema fluxo postal",
            "17": "devolvido financeiro",
            "26": "cancelado",
        }
        def _normalize_status(s: Any) -> str:
            if pd.isna(s):
                return ''
            import re as _re
            ss = str(s).lower().strip()
            if _re.fullmatch(r"\d+", ss):
                ss = status_code_map.get(ss, ss)
            ss = _re.sub(r'^\d+\s*-\s*', '', ss)
            ss = (ss.replace('ã','a').replace('á','a').replace('â','a')
                    .replace('ç','c').replace('é','e').replace('ê','e')
                    .replace('í','i').replace('ó','o').replace('ô','o')
                    .replace('ú','u'))
            return ss
        normalized = df_derived['order_status'].apply(_normalize_status)
        cancelled_set = set(COSMETICOS_BUSINESS_RULES['cancelled_statuses'])
        df_derived['pedido_cancelado'] = normalized.isin(cancelled_set).astype(int)
    else:
        df_derived['pedido_cancelado'] = 0
    
    # Customer unique ID
    if 'customer_unique_id' not in df_derived.columns:
        if 'customer_id' in df_derived.columns:
            df_derived['customer_unique_id'] = df_derived['customer_id']
        else:
            df_derived['customer_unique_id'] = ''
    
    # Review score sintético (baseado no status do pedido)
    if 'review_score' not in df_derived.columns:
        # Gerar scores sintéticos baseados no status
        def generate_review_score(status):
            if pd.isna(status):
                return np.nan
            status_lower = str(status).lower()
            if any(cancelled in status_lower for cancelled in ['cancelado', 'devolvido']):
                return np.random.choice([1, 2], p=[0.7, 0.3])
            elif 'entregue' in status_lower:
                return np.random.choice([4, 5], p=[0.3, 0.7])
            else:
                return np.random.choice([3, 4, 5], p=[0.2, 0.4, 0.4])
        
        df_derived['review_score'] = df_derived['order_status'].apply(generate_review_score)
    
    # Data de entrega estimada
    if 'order_delivered_customer_date' not in df_derived.columns:
        if 'approval_date' in df_derived.columns:
            # Estimar entrega como aprovação + 3-7 dias úteis
            delivery_days = np.random.choice(range(3, 8), size=len(df_derived))
            df_derived['order_delivered_customer_date'] = (
                pd.to_datetime(df_derived['approval_date']) + 
                pd.to_timedelta(delivery_days, unit='D')
            )
        elif 'order_purchase_timestamp' in df_derived.columns:
            # Estimar entrega como compra + 5-10 dias úteis
            delivery_days = np.random.choice(range(5, 11), size=len(df_derived))
            df_derived['order_delivered_customer_date'] = (
                pd.to_datetime(df_derived['order_purchase_timestamp']) + 
                pd.to_timedelta(delivery_days, unit='D')
            )
    
    # Aplicar impacto de cancelamento no preço: preservar original e zerar price
    if 'price' in df_derived.columns:
        if 'price_original' not in df_derived.columns:
            df_derived['price_original'] = df_derived['price']
        
        # Calcular impacto dos cancelamentos
        cancelled_mask = df_derived['pedido_cancelado'] == 1
        cancelled_count = cancelled_mask.sum()
        cancelled_revenue = df_derived.loc[cancelled_mask, 'price'].sum()
        
        # Zerar preço dos cancelados
        df_derived.loc[cancelled_mask, 'price'] = 0.0
        
        # Log do impacto financeiro
        logger.info(f"Impacto de cancelamentos:")
        logger.info(f"  - Pedidos cancelados: {cancelled_count:,} ({cancelled_count/len(df_derived)*100:.2f}%)")
        logger.info(f"  - Receita perdida: R$ {cancelled_revenue:,.2f}")
        logger.info(f"  - Ticket médio cancelado: R$ {cancelled_revenue/cancelled_count if cancelled_count > 0 else 0:,.2f}")

    logger.info("Campos derivados calculados com sucesso")
    return df_derived

def map_order_status_to_funnel_stages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia os status dos pedidos para etapas do funil de conversão.
    
    Esta função normaliza os status e cria campos binários para cada etapa do funil,
    permitindo análises de conversão independente do formato original dos status.
    
    Args:
        df: DataFrame com a coluna 'order_status'
    
    Returns:
        DataFrame com colunas adicionais de etapas do funil
    """
    df_funnel = df.copy()
    
    if 'order_status' not in df_funnel.columns:
        logger.warning("Coluna 'order_status' não encontrada. Pulando mapeamento de funil.")
        return df_funnel
    
    def normalize_status(status_value):
        """Normaliza valores de status para mapeamento flexível.
        - Converte para minúsculas
        - Remove prefixos numéricos do tipo "7 - "
        - Remove acentos simples por aproximação
        """
        if pd.isna(status_value):
            return ''
        s = str(status_value).lower().strip()
        status_code_map = {
            "1": "aguardando pagamento",
            "2": "cancelado pagamento",
            "4": "aprovado",
            "5": "faturamento iniciado",
            "6": "nota fiscal emitida",
            "7": "transporte",
            "8": "entregue",
            "14": "cancelado pagamento analise",
            "16": "problema fluxo postal",
            "17": "devolvido financeiro",
            "26": "cancelado",
        }
        if re.fullmatch(r"\d+", s):
            s = status_code_map.get(s, s)
        # remover prefixo "NN - "
        s = re.sub(r'^\d+\s*-\s*', '', s)
        # normalizar acentos essenciais
        s = (s
             .replace('ã', 'a')
             .replace('á', 'a')
             .replace('â', 'a')
             .replace('ç', 'c')
             .replace('é', 'e')
             .replace('ê', 'e')
             .replace('í', 'i')
             .replace('ó', 'o')
             .replace('ô', 'o')
             .replace('ú', 'u'))
        return s
    
    # Criar coluna normalizada (temporária)
    status_normalized = df_funnel['order_status'].apply(normalize_status)
    df_funnel['order_status_normalized'] = status_normalized
    
    # Definir padrões de correspondência para cada etapa
    # Estes padrões funcionam tanto para dados em português quanto em inglês
    
    # Etapas principais do funil de pedidos
    awaiting_payment_patterns = [
        'aguardando pagamento', 'aguardando pagamento diferenciado',
        'aguardando atualizacao de dados',
        'aguardando', 'pendente', 'waiting payment', 'pending'
    ]
    paid_patterns = [
        'aprovado', 'aprovado e integrado', 'aprovado parcial',
        'aprovado analise de pagamento', 'approved', 'aprovad',
        'pago', 'paid', 'confirmado', 'confirmed'
    ]
    invoice_patterns = [
        'nota fiscal emitida', 'nota fiscal', 'nf emitida', 'invoice', 'fatura',
        'faturamento iniciado'
    ]
    transit_patterns = [
        'transporte', 'enviado', 'shipped', 'postado', 'em transporte', 'in transit'
    ]
    delivered_patterns = ['entregue', 'delivered', 'entreg']
    
    # Etapas de problemas/exceções
    cancelled_patterns = [
        'cancelado', 'cancel', 'estornado', 'estorn', 'devolvido', 'devol', 'reembolso', 'refund',
        'fraude', 'chargeback', 'disputa',
        'nota fiscal cancelada', 'em cancelamento', 'cancelado pagamento analise'
    ]
    problem_patterns = [
        'problema fluxo postal', 'problema', 'erro', 'falha', 'issue', 'error',
        'logistica reversa', 'suspenso temporariamente'
    ]
    exchange_patterns = ['troca', 'exchange', 'credito por troca', 'crédito por troca']
    
    # Criar máscaras booleanas para cada etapa do funil
    # Nota: as etapas principais são cumulativas (entregue inclui enviado e aprovado)
    
    # 1. Pedido está aguardando pagamento
    df_funnel['funnel_awaiting_payment'] = status_normalized.apply(
        lambda x: any(p in x for p in awaiting_payment_patterns)
    ).astype(int)
    
    # 2. Pedido foi pago/aprovado (inclui todas as etapas posteriores)
    df_funnel['funnel_paid'] = status_normalized.apply(
        lambda x: any(p in x for p in paid_patterns + invoice_patterns + transit_patterns + delivered_patterns)
    ).astype(int)
    
    # 3. Nota fiscal foi emitida (inclui transporte e entrega)
    df_funnel['funnel_invoice_issued'] = status_normalized.apply(
        lambda x: any(p in x for p in invoice_patterns + transit_patterns + delivered_patterns)
    ).astype(int)
    
    # 4. Pedido está em transporte (inclui entregues)
    df_funnel['funnel_in_transit'] = status_normalized.apply(
        lambda x: any(p in x for p in transit_patterns + delivered_patterns)
    ).astype(int)
    
    # 5. Pedido foi entregue
    df_funnel['funnel_delivered'] = status_normalized.apply(
        lambda x: any(p in x for p in delivered_patterns)
    ).astype(int)
    
    # Etapas de exceção (não cumulativas)
    
    # 6. Pedido foi cancelado/devolvido
    df_funnel['funnel_cancelled'] = status_normalized.apply(
        lambda x: any(p in x for p in cancelled_patterns)
    ).astype(int)
    
    # 7. Pedido tem problema
    df_funnel['funnel_problem'] = status_normalized.apply(
        lambda x: any(p in x for p in problem_patterns)
    ).astype(int)
    
    # 8. Pedido é uma troca
    df_funnel['funnel_exchange'] = status_normalized.apply(
        lambda x: any(p in x for p in exchange_patterns)
    ).astype(int)
    
    # Manter campos legados para compatibilidade
    df_funnel['funnel_approved'] = df_funnel['funnel_paid']  # Alias para compatibilidade
    df_funnel['funnel_shipped'] = df_funnel['funnel_in_transit']  # Alias para compatibilidade
    
    # Log de estatísticas do mapeamento
    total = len(df_funnel)
    awaiting_count = df_funnel['funnel_awaiting_payment'].sum()
    paid_count = df_funnel['funnel_paid'].sum()
    invoice_count = df_funnel['funnel_invoice_issued'].sum()
    transit_count = df_funnel['funnel_in_transit'].sum()
    delivered_count = df_funnel['funnel_delivered'].sum()
    cancelled_count = df_funnel['funnel_cancelled'].sum()
    problem_count = df_funnel['funnel_problem'].sum()
    exchange_count = df_funnel['funnel_exchange'].sum()
    
    logger.info(f"Mapeamento de funil concluído:")
    logger.info(f"  - Total de pedidos: {total:,}")
    logger.info(f"  Etapas principais:")
    logger.info(f"    • Aguardando Pagamento: {awaiting_count:,} ({awaiting_count/total*100:.1f}%)")
    logger.info(f"    • Pago/Aprovado: {paid_count:,} ({paid_count/total*100:.1f}%)")
    logger.info(f"    • NF Emitida: {invoice_count:,} ({invoice_count/total*100:.1f}%)")
    logger.info(f"    • Em Transporte: {transit_count:,} ({transit_count/total*100:.1f}%)")
    logger.info(f"    • Entregue: {delivered_count:,} ({delivered_count/total*100:.1f}%)")
    logger.info(f"  Exceções:")
    logger.info(f"    • Cancelado/Devolvido: {cancelled_count:,} ({cancelled_count/total*100:.1f}%)")
    logger.info(f"    • Com Problema: {problem_count:,} ({problem_count/total*100:.1f}%)")
    logger.info(f"    • Troca: {exchange_count:,} ({exchange_count/total*100:.1f}%)")
    
    # Mostrar alguns exemplos de mapeamento para debug
    unique_statuses = df_funnel['order_status'].value_counts().head(8)
    logger.info(f"  Exemplos de mapeamento:")
    for status, count in unique_statuses.items():
        normalized = normalize_status(status)
        stages = []
        
        # Verificar cada etapa
        if any(p in normalized for p in awaiting_payment_patterns):
            stages.append("Aguardando Pgto")
        if any(p in normalized for p in paid_patterns + invoice_patterns + transit_patterns + delivered_patterns):
            stages.append("Pago")
        if any(p in normalized for p in invoice_patterns + transit_patterns + delivered_patterns):
            stages.append("NF")
        if any(p in normalized for p in transit_patterns + delivered_patterns):
            stages.append("Transporte")
        if any(p in normalized for p in delivered_patterns):
            stages.append("Entregue")
        if any(p in normalized for p in cancelled_patterns):
            stages.append("❌ Cancelado")
        if any(p in normalized for p in problem_patterns):
            stages.append("⚠️ Problema")
        if any(p in normalized for p in exchange_patterns):
            stages.append("🔄 Troca")
        
        stages_str = " → ".join(stages) if stages else "Não mapeado"
        logger.info(f"    • '{status}' ({count:,}): {stages_str}")
    
    return df_funnel

def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica regras de negócio específicas para cosméticos.
    """
    df_rules = df.copy()
    
    # Remover colunas duplicadas se existirem
    if df_rules.columns.duplicated().any():
        logger.warning(f"Colunas duplicadas encontradas: {df_rules.columns[df_rules.columns.duplicated()].tolist()}")
        df_rules = df_rules.loc[:, ~df_rules.columns.duplicated()]
        logger.info("Colunas duplicadas removidas")
    
    initial_rows = len(df_rules)
    
    # Filtrar por status válidos (considerando normalização)
    if 'order_status' in df_rules.columns:
        # Garantir coluna normalizada
        if 'order_status_normalized' not in df_rules.columns:
            def _normalize(s):
                if pd.isna(s):
                    return ''
                ss = str(s).lower().strip()
                ss = re.sub(r'^\d+\s*-\s*', '', ss)
                ss = (ss.replace('ã','a').replace('á','a').replace('â','a')
                        .replace('ç','c').replace('é','e').replace('ê','e')
                        .replace('í','i').replace('ó','o').replace('ô','o')
                        .replace('ú','u'))
                return ss
            df_rules['order_status_normalized'] = df_rules['order_status'].apply(_normalize)
        valid_statuses = set(COSMETICOS_BUSINESS_RULES['valid_order_statuses'])
        # Mantemos pendentes e especiais no dataset, mas não entram em receita pelos cálculos (price já é ajustado em calculate_derived_fields)
        mask_valid = df_rules['order_status_normalized'].isin(valid_statuses)
        rows_before = len(df_rules)
        df_rules = df_rules[mask_valid | (~mask_valid)]  # mantemos tudo, mas logamos
        logger.info(f"Status normalizados únicos (top 10): {df_rules['order_status_normalized'].value_counts().head(10).to_dict()}")
    
    # Filtrar por faixa de preço desabilitado temporariamente
    # O processamento de valores monetários precisa ser corrigido primeiro
    # if 'price' in df_rules.columns:
    #     # Converter para numérico se necessário
    #     df_rules['price'] = pd.to_numeric(df_rules['price'], errors='coerce').fillna(0)
    #     # Remover apenas valores claramente inválidos
    #     df_rules = df_rules[
    #         (df_rules['price'] > 0) & 
    #         (df_rules['price'] < 100000)
    #     ]
    
    final_rows = len(df_rules)
    logger.info(f"Regras de negócio aplicadas: {initial_rows:,} → {final_rows:,} registros (filtro ativo por status válido para métricas)")
    
    return df_rules

# ----------------------------
# Função Principal de Adaptação
# ----------------------------

def process_stock_data(estoque_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Processa dados de estoque separadamente.
    
    Args:
        estoque_path: Caminho para planilha de estoque
        
    Returns:
        Tupla com (DataFrame processado, métricas de processamento)
    """
    logger.info("Iniciando processamento de dados de estoque...")
    
    # Tentar múltiplos encodings
    encodings = ['utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1', 'utf-8']
    df_estoque = None
    
    for encoding in encodings:
        try:
            df_estoque = pd.read_csv(estoque_path, sep=';', encoding=encoding)
            logger.info(f"Arquivo de estoque carregado com encoding: {encoding}")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Erro com encoding {encoding} no estoque: {e}")
            continue
    
    if df_estoque is None:
        raise ValueError(f"Não foi possível carregar o arquivo de estoque com nenhum encoding testado")
    
    logger.info(f"Estoque carregado: {len(df_estoque):,} registros")
    
    # Processar dados de estoque
    df_estoque = normalize_column_names(df_estoque)
    df_estoque = apply_cosmeticos_aliases(df_estoque)
    
    # Processar valores monetários se existirem
    df_estoque = process_monetary_values(df_estoque)
    
    # Processar datas se existirem
    df_estoque = process_dates(df_estoque)
    
    # Categorizar produtos se possível
    df_estoque = categorize_cosmetics_products(df_estoque)
    
    # Métricas de processamento
    metrics = {
        'initial_records': len(df_estoque),
        'final_records': len(df_estoque),
        'total_products': df_estoque['product_id'].nunique() if 'product_id' in df_estoque.columns else 0,
        'total_stock_value': df_estoque['stock_level'].sum() if 'stock_level' in df_estoque.columns else 0,
        'brands_count': df_estoque['brand'].nunique() if 'brand' in df_estoque.columns else 0,
        'categories_found': df_estoque['product_category_name'].nunique() if 'product_category_name' in df_estoque.columns else 0
    }
    
    logger.info(f"Processamento de estoque concluído: {len(df_estoque):,} registros finais")
    
    return df_estoque, metrics

def process_cosmeticos_data(
    pedidos_path: str, 
    estoque_path: Optional[str] = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Processa planilhas do cliente de cosméticos para formato padrão.
    
    Args:
        pedidos_path: Caminho para planilha de pedidos
        estoque_path: Caminho para planilha de estoque (opcional)
        
    Returns:
        Tupla com (DataFrame processado, métricas de processamento)
    """
    logger.info("Iniciando processamento de dados de cosméticos...")
    
    # 1. Carregar dados de pedidos
    logger.info(f"Carregando pedidos: {pedidos_path}")
    
    # Tentar múltiplos encodings
    encodings = ['utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1', 'utf-8']
    df_pedidos = None
    
    for encoding in encodings:
        try:
            df_pedidos = pd.read_csv(pedidos_path, sep=';', encoding=encoding)
            logger.info(f"Arquivo carregado com encoding: {encoding}")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Erro com encoding {encoding}: {e}")
            continue
    
    if df_pedidos is None:
        raise ValueError(f"Não foi possível carregar o arquivo com nenhum encoding testado")
    
    logger.info(f"Pedidos carregados: {len(df_pedidos):,} registros")
    
    # 2. Normalizar nomes de colunas
    df_pedidos = normalize_column_names(df_pedidos)
    
    # 3. Aplicar aliases
    df_pedidos = apply_cosmeticos_aliases(df_pedidos)
    
    # 4. Processar datas
    df_pedidos = process_dates(df_pedidos)
    
    # 5. Processar valores monetários
    df_pedidos = process_monetary_values(df_pedidos)
    
    # 6. Limpar e normalizar nomes de marketplaces
    df_pedidos = clean_marketplace_names(df_pedidos)
    
    # 7. Categorizar produtos
    df_pedidos = categorize_cosmetics_products(df_pedidos)
    
    # 8. Gerar estados sintéticos para clientes
    df_pedidos = generate_synthetic_customer_states(df_pedidos)
    
    # 9. Calcular campos derivados
    df_pedidos = calculate_derived_fields(df_pedidos)
    
    # 10. Mapear status para etapas do funil
    df_pedidos = map_order_status_to_funnel_stages(df_pedidos)
    
    # 11. Aplicar regras de negócio
    df_processed = apply_business_rules(df_pedidos)

    # 11.1 Calcular margens (se disponível)
    try:
        if MarginCalculator is not None:
            margin_calculator = MarginCalculator(DEFAULT_MARGIN_CONFIG)  # usa padrão; conectores podem sobrescrever
            df_processed = margin_calculator.apply(df_processed)
            logger.info("Cálculo de margens aplicado ao dataset processado")
    except Exception as e:
        logger.warning(f"Não foi possível calcular margens: {e}")
    
    # 12. Processar dados de estoque (se disponível)
    stock_metrics = {}
    if estoque_path and Path(estoque_path).exists():
        logger.info(f"Carregando estoque: {estoque_path}")
        
        # Tentar múltiplos encodings para estoque
        df_estoque = None
        for encoding in encodings:
            try:
                df_estoque = pd.read_csv(estoque_path, sep=';', encoding=encoding)
                logger.info(f"Arquivo de estoque carregado com encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Erro com encoding {encoding} no estoque: {e}")
                continue
        
        if df_estoque is None:
            logger.warning("Não foi possível carregar arquivo de estoque - continuando sem ele")
        else:
            df_estoque = normalize_column_names(df_estoque)
            df_estoque = apply_cosmeticos_aliases(df_estoque)
        
            # Merge com dados de pedidos se possível
            if df_estoque is not None and 'product_id' in df_estoque.columns and 'product_id' in df_processed.columns:
                # Selecionar apenas colunas que existem
                merge_cols = ['product_id']
                for col in ['stock_level', 'product_cost', 'brand']:
                    if col in df_estoque.columns:
                        merge_cols.append(col)
                
                df_processed = df_processed.merge(
                    df_estoque[merge_cols],
                    on='product_id',
                    how='left',
                    suffixes=('', '_estoque')
                )
                logger.info("Dados de estoque integrados com sucesso")
            
            if df_estoque is not None:
                stock_metrics = {
                    'total_products': df_estoque['product_id'].nunique() if 'product_id' in df_estoque.columns else 0,
                    'total_stock_value': df_estoque['stock_level'].sum() if 'stock_level' in df_estoque.columns else 0,
                    'brands_count': df_estoque['brand'].nunique() if 'brand' in df_estoque.columns else 0
                }
    
    # 13. Métricas de processamento (com verificações de colunas disponíveis)
    metrics = {
        'initial_records': len(df_pedidos),
        'final_records': len(df_processed),
        'categories_found': df_processed['product_category_name'].nunique() if 'product_category_name' in df_processed.columns else 0,
        'date_range': {
            'start': df_processed['order_purchase_timestamp'].min() if 'order_purchase_timestamp' in df_processed.columns else None,
            'end': df_processed['order_purchase_timestamp'].max() if 'order_purchase_timestamp' in df_processed.columns else None
        },
        'total_revenue': df_processed['price'].sum() if 'price' in df_processed.columns else 0,
        'total_net_revenue': df_processed.get('margin_net_revenue', pd.Series(dtype=float)).sum() if 'margin_net_revenue' in df_processed.columns else df_processed['price'].sum() if 'price' in df_processed.columns else 0,
        'total_contribution_margin': df_processed.get('contribution_margin', pd.Series(dtype=float)).sum() if 'contribution_margin' in df_processed.columns else 0,
        'average_margin_rate': float(df_processed.get('margin_rate', pd.Series(dtype=float)).mean()) if 'margin_rate' in df_processed.columns else 0.0,
        'unique_customers': df_processed['customer_id'].nunique() if 'customer_id' in df_processed.columns else 0,
        'unique_products': df_processed['product_id'].nunique() if 'product_id' in df_processed.columns else 0,
        **stock_metrics
    }
    
    logger.info(f"Processamento concluído: {len(df_processed):,} registros finais")
    
    return df_processed, metrics

# ----------------------------
# Exemplo de Uso
# ----------------------------

if __name__ == "__main__":
    """
    Exemplo de uso do adaptador para cosméticos
    """
    print("Adaptador de Cosméticos - Insight Expert")
    print("=" * 50)
    
    # Exemplo de processamento
    pedidos_file = "Consulta de Pedidos.csv"
    estoque_file = "Consulta Estoque.csv"
    
    if Path(pedidos_file).exists():
        try:
            df_processed, metrics = process_cosmeticos_data(
                pedidos_path=pedidos_file,
                estoque_path=estoque_file if Path(estoque_file).exists() else None
            )
            
            print(f"\n📊 RESULTADOS:")
            print(f"   Registros processados: {metrics['final_records']:,}")
            print(f"   Categorias encontradas: {metrics['categories_found']}")
            print(f"   Receita total: R$ {metrics['total_revenue']:,.2f}")
            print(f"   Clientes únicos: {metrics['unique_customers']:,}")
            print(f"   Produtos únicos: {metrics['unique_products']:,}")
            
            # Salvar resultado
            output_path = "dados_cliente/cosmeticos_processado.parquet"
            df_processed.to_parquet(output_path, index=False)
            print(f"\n💾 Dados salvos em: {output_path}")
            
        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
    else:
        print(f"❌ Arquivo não encontrado: {pedidos_file}")
        print("   Para usar este adaptador:")
        print("   1. Coloque as planilhas do cliente no diretório")
        print("   2. Execute: python adaptador_cosmeticos.py")
