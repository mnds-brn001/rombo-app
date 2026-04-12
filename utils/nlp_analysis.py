from __future__ import annotations
import re
import os
import streamlit as st
import pandas as pd
import numpy as np
import types
import hashlib
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path

# --- nltk -------------------------------------------------------------------
try:
    import nltk  # type: ignore
    from nltk.corpus import stopwords  # type: ignore
    from nltk.tokenize import word_tokenize  # type: ignore
    from nltk.stem import WordNetLemmatizer  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – fallback stub
    nltk = types.ModuleType("nltk")

    def _dummy(*args, **kwargs):  # noqa: D401
        return []

    nltk.corpus = types.ModuleType("nltk.corpus")
    nltk.corpus.stopwords = _dummy  # type: ignore
    nltk.tokenize = types.ModuleType("nltk.tokenize")  # type: ignore
    nltk.tokenize.word_tokenize = _dummy  # type: ignore
    nltk.stem = types.ModuleType("nltk.stem")  # type: ignore

    class _Lemma:  # noqa: D401
        def lemmatize(self, *args, **kwargs):
            return args[0] if args else ""

    nltk.stem.WordNetLemmatizer = _Lemma  # type: ignore

    # Atributos adicionais usados em download_nltk_data()
    nltk.data = types.SimpleNamespace(find=lambda *args, **kwargs: None)  # type: ignore
    nltk.download = lambda *args, **kwargs: None  # type: ignore

    import sys

    sys.modules["nltk"] = nltk
    stopwords = types.SimpleNamespace(words=lambda lang: set())  # type: ignore
    word_tokenize = lambda text: text.split()  # type: ignore
    WordNetLemmatizer = _Lemma  # type: ignore

# --- matplotlib -------------------------------------------------------------
try:
    import matplotlib.pyplot as plt  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – fallback stub
    plt = types.ModuleType("matplotlib.pyplot")

    def _dummy(*args, **kwargs):  # noqa: D401
        return None

    plt.figure = _dummy  # type: ignore
    plt.imshow = _dummy  # type: ignore
    plt.axis = _dummy  # type: ignore

    import sys

    matplotlib_stub = types.ModuleType("matplotlib")
    matplotlib_stub.pyplot = plt  # type: ignore
    sys.modules["matplotlib"] = matplotlib_stub
    sys.modules["matplotlib.pyplot"] = plt

# --- wordcloud --------------------------------------------------------------
try:
    from wordcloud import WordCloud  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    WordCloud = None  # type: ignore
# --- scikit-learn -----------------------------------------------------------
try:
    from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer  # type: ignore
    from sklearn.decomposition import LatentDirichletAllocation, NMF  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    class _SklearnDummy:  # noqa: D401
        def __init__(self, *args, **kwargs):
            pass

        def fit_transform(self, *args, **kwargs):  # noqa: D401
            import numpy as _np
            return _np.ones((1, 1))

        def fit(self, *args, **kwargs):  # noqa: D401
            import numpy as _np
            self.components_ = _np.ones((1, 1))
            return self

        transform = fit_transform  # alias

    CountVectorizer = _SklearnDummy  # type: ignore
    TfidfVectorizer = _SklearnDummy  # type: ignore
    LatentDirichletAllocation = _SklearnDummy  # type: ignore
    NMF = _SklearnDummy  # type: ignore

def _hash_path(path: Path) -> str:
    """Função customizada para hashear objetos Path."""
    return str(path.absolute())

def _hash_dataframe(df: pd.DataFrame) -> str:
    """Função customizada para hashear DataFrames baseada no conteúdo."""
    # Hash do DataFrame completo para cache mais preciso
    # MD5 usado apenas para cache de hash, não para segurança
    return hashlib.md5(pd.util.hash_pandas_object(df).values.tobytes()).hexdigest()  # nosec B324

# =============================================================================
# CUSTOM STOP WORDS - E-COMMERCE PORTUGUÊS (EXPANDIDO)
# =============================================================================

CUSTOM_STOP_WORDS = {
    # Palavras temporais genéricas
    'dia', 'dias', 'semana', 'semanas', 'mês', 'meses', 'ano', 'anos',
    'hoje', 'ontem', 'amanhã', 'agora', 'ainda', 'já', 'sempre', 'nunca',
    'depois', 'antes', 'durante', 'enquanto', 'logo', 'tarde', 'cedo',
    
    # Palavras transacionais genéricas
    'produto', 'produtos', 'item', 'itens', 'loja', 'site', 'compra', 'compras',
    'comprei', 'comprado', 'comprar', 'recebi', 'recebido', 'receber', 'chegou',
    'chegado', 'chegar', 'pedido', 'pedidos', 'encomenda', 'veio', 'vir',
    
    # Verbos auxiliares e de ligação
    'foi', 'ser', 'sido', 'sendo', 'estar', 'estava', 'estou', 'está', 'estão',
    'ter', 'tinha', 'tenho', 'tem', 'tive', 'haver', 'houve', 'há',
    
    # Conectores e articuladores genéricos
    'pois', 'então', 'porém', 'contudo', 'todavia', 'entretanto', 'portanto',
    'assim', 'dessa', 'desse', 'deste', 'desta', 'nisso', 'nisto',
    
    # Intensificadores genéricos (sem valor semântico)
    'muito', 'muita', 'pouco', 'pouca', 'bastante', 'demais', 'bem', 'mal',
    'mais', 'menos', 'tão', 'tanto', 'quão', 'quanto',
    
    # Pronomes e artigos
    'meu', 'minha', 'meus', 'minhas', 'seu', 'sua', 'seus', 'suas',
    'nosso', 'nossa', 'nossos', 'nossas', 'dele', 'dela', 'deles', 'delas',
    
    # Palavras de contato sem valor semântico
    'contato', 'contatei', 'falar', 'falei', 'disse', 'dizer',
    
    # Advérbios genéricos
    'apenas', 'somente', 'só', 'realmente', 'certamente', 'talvez', 'quase',
}

# =============================================================================
# TÓPICOS SEMÂNTICOS EXPANDIDOS
# =============================================================================

# TÓPICO 1: QUALIDADE DO PRODUTO
TOPIC_QUALIDADE_PRODUTO = {
    # ========== AVALIAÇÕES GERAIS ==========
    'qualidade', 'ótimo', 'ótima', 'excelente', 'excepcional', 'impecável',
    'perfeito', 'perfeita', 'maravilhoso', 'maravilhosa', 'incrível', 
    'sensacional', 'espetacular', 'fantástico', 'fantástica', 'show',
    'top', 'premium', 'luxo', 'luxuoso', 'luxuosa', 'sofisticado', 'sofisticada',
    'amei', 'adorei', 'aprovei', 'recomendo', 'super', 'mega',
    'bom', 'boa', 'razoável', 'aceitável', 'ok', 'normal', 'comum',
    'regular', 'mediano', 'mediana', 'esperado', 'esperada',
    'ruim', 'péssimo', 'péssima', 'horrível', 'terrível', 'péssima',
    'lixo', 'porcaria', 'decepção', 'decepcionante', 'frustrante',
    'insatisfeito', 'insatisfeita', 'arrependido', 'arrependida',
    'chinês', 'falsificado', 'falso', 'falsa', 'imitação', 'pirata',
    
    # ========== CARACTERÍSTICAS FÍSICAS ==========
    'tamanho', 'grande', 'pequeno', 'pequena', 'médio', 'média',
    'gigante', 'enorme', 'imenso', 'minúsculo', 'minúscula', 'tiny',
    'largo', 'larga', 'estreito', 'estreita', 'fino', 'fina',
    'grosso', 'grossa', 'espesso', 'espessa', 'comprido', 'comprida',
    'curto', 'curta', 'alto', 'alta', 'baixo', 'baixa',
    'leve', 'pesado', 'pesada', 'volumoso', 'volumosa', 'compacto', 'compacta',
    'denso', 'densa', 'oco', 'oca', 'maciço', 'maciça',
    'textura', 'macio', 'macia', 'suave', 'aveludado', 'aveludada',
    'áspero', 'áspera', 'rugoso', 'rugosa', 'liso', 'lisa',
    'sedoso', 'sedosa', 'cremoso', 'cremosa', 'líquido', 'líquida',
    'pastoso', 'pastosa', 'sólido', 'sólida', 'gelatinoso', 'gelatinosa',
    'cor', 'colorido', 'colorida', 'vibrante', 'vivo', 'viva',
    'opaco', 'opaca', 'brilhante', 'fosco', 'fosca', 'translúcido',
    'transparente', 'escuro', 'escura', 'claro', 'clara', 'desbotado',
    'desbotada', 'manchado', 'manchada', 'uniforme', 'desigual',
    'durável', 'resistente', 'forte', 'robusto', 'robusta', 'sólido', 'sólida',
    'frágil', 'delicado', 'delicada', 'quebradiço', 'quebradiça',
    'queima', 'derrete', 'descasca', 'desbota', 'mancha', 'suja',
    'durabilidade', 'resistência', 'fragilidade',
    'acabamento', 'detalhe', 'detalhes', 'costura', 'costurado', 'costurada',
    'emenda', 'junção', 'encaixe', 'alinhado', 'alinhada', 'torto', 'torta',
    'desalinhado', 'desalinhada', 'mal feito', 'mal feita', 'bem feito', 'bem feita',
    'caprichado', 'caprichada', 'desleixado', 'desleixada', 'refinado', 'refinada',
    'original', 'autêntico', 'autêntica', 'legítimo', 'legítima', 'genuíno', 'genuína',
    'falsificado', 'falsificada', 'pirata', 'réplica', 'cópia', 'imitação',
    'importado', 'importada', 'nacional', 'brasileiro', 'brasileira',
    'embalagem', 'embalado', 'embalada', 'caixa', 'pacote', 'envelope',
    'lacrado', 'lacrada', 'selado', 'selada', 'violado', 'violada',
    'proteção', 'protegido', 'protegida', 'amassado', 'amassada',
    'danificado', 'danificada', 'intacto', 'intacta', 'perfeito', 'perfeita',
    'plástico bolha', 'papel kraft', 'isopor', 'papelão', 'caixinha',
    'saquinho', 'saco', 'envelope bolha', 'bem embalado', 'mal embalado',
    'funciona', 'funcionando', 'funcionou', 'função', 'defeito', 'defeituoso',
    'defeituosa', 'quebrado', 'quebrada', 'estragado', 'estragada', 'avariado',
    'avariada', 'completo', 'completa', 'incompleto', 'incompleta',
    'faltando', 'falta', 'manual', 'instruções', 'acessório', 'acessórios',
    'componente', 'componentes', 'parte', 'partes', 'peça', 'peças',
}

# TÓPICO 2: CARACTERÍSTICAS ESPECÍFICAS - COSMÉTICOS E BELEZA
TOPIC_COSMETICOS_BELEZA = {
    'cheiro', 'cheiroso', 'cheirosa', 'aroma', 'aromático', 'aromática',
    'fragrância', 'perfume', 'perfumado', 'perfumada', 'essência',
    'fedido', 'fedida', 'fedor', 'fétido', 'fétida', 'enjoativo', 'enjoativa',
    'suave', 'forte', 'intenso', 'intensa', 'marcante', 'discreto', 'discreta',
    'natural', 'artificial', 'químico', 'química', 'sintético', 'sintética',
    'textura', 'consistência', 'cremoso', 'cremosa', 'líquido', 'líquida',
    'gel', 'mousse', 'espuma', 'oleoso', 'oleosa', 'gorduroso', 'gordurosa',
    'seco', 'seca', 'ressecado', 'ressecada', 'pegajoso', 'pegajosa',
    'grudento', 'grudenta', 'leve', 'pesado', 'pesada', 'denso', 'densa',
    'espalhável', 'absorve', 'absorveu', 'absorção', 'penetra', 'penetração',
    'espalha', 'espalhou', 'aplicação', 'aplica', 'aplicou', 'rende',
    'rendimento', 'efeito', 'resultado', 'desempenho',
    'hidratante', 'hidrata', 'hidratou', 'hidratação', 'nutrição', 'nutre',
    'nutritivo', 'nutritiva', 'umectante', 'emoliente', 'ressecou',
    'resseca', 'seca', 'úmido', 'úmida', 'molhado', 'molhada',
    'pele', 'cutâneo', 'cutânea', 'derme', 'epiderme',
    'macia', 'sedosa', 'aveludada', 'lisa', 'áspera', 'rugosa',
    'oleosa', 'seca', 'mista', 'normal', 'sensível', 'acneica',
    'irritada', 'irritação', 'vermelhidão', 'coceira', 'ardor', 'queimação',
    'alergia', 'alérgica', 'reação', 'manchou', 'mancha', 'clareou', 'clareia',
    'uniformiza', 'uniformizou', 'ilumina', 'iluminou', 'luminosidade',
    'cabelo', 'capilar', 'fio', 'fios', 'raiz', 'pontas', 'couro cabeludo',
    'liso', 'cacheado', 'crespo', 'ondulado', 'encaracolado',
    'macio', 'sedoso', 'brilhoso', 'brilho', 'opaco', 'fosco',
    'hidratado', 'ressecado', 'danificado', 'quebradiço', 'elástico',
    'volume', 'volumoso', 'murcho', 'liso', 'alisa', 'alisou',
    'define', 'definição', 'modela', 'modelou', 'controla', 'controlou',
    'frizz', 'arrepiado', 'embaraçado', 'emaranhado', 'desembaraça',
    'desembaraçou', 'penteia', 'penteou', 'escova', 'escovou',
    'unha', 'unhas', 'esmalte', 'base', 'top coat', 'extra brilho',
    'secante', 'removedor', 'acetona', 'fortalecedor', 'endurecedor',
    'cobertura', 'pigmentação', 'opaco', 'transparente', 'translúcido',
    'brilhante', 'metálico', 'perolado', 'glitter', 'cintilante',
    'cremoso', 'chip', 'lasca', 'lascou', 'descasca', 'descascou',
    'duração', 'dura', 'durou', 'secou', 'seca', 'demora', 'demorou',
    'vegano', 'vegana', 'cruelty free', 'testado', 'testada', 'dermatológico',
    'dermatológica', 'hipoalergênico', 'hipoalergênica', 'sensível',
    'natural', 'orgânico', 'orgânica', 'químico', 'química', 'parabenos',
    'sulfato', 'silicone', 'álcool', 'fragrância', 'corante', 'conservante',
    'ingrediente', 'ingredientes', 'componente', 'fórmula', 'composição',
    'ativo', 'ativa', 'princípio ativo', 'extrato', 'óleo', 'essência',
    'anvisa', 'registro', 'lote', 'validade', 'vencido', 'vencida',
    'aprovado', 'aprovada', 'certificado', 'certificada', 'selo',
    'regulamentado', 'regulamentada', 'seguro', 'segura',
}

# TÓPICO 3: LOGÍSTICA E ENTREGA
TOPIC_LOGISTICA_ENTREGA = {
    'entrega', 'entregar', 'entregue', 'rápido', 'rápida', 'rapidez',
    'veloz', 'ágil', 'agilidade', 'expresso', 'expressa', 'urgente',
    'instantâneo', 'instantânea', 'imediato', 'imediata', 'super rápido',
    'super rápida', 'rapidíssimo', 'rapidíssima', 'flash',
    'prazo', 'pontual', 'pontualidade', 'dentro', 'antecipado', 'antecipada',
    'antes', 'adiantado', 'adiantada', 'previsto', 'prevista', 'esperado',
    'esperada', 'estimado', 'estimada', 'prometido', 'prometida',
    'demorou', 'demora', 'demorado', 'demorada', 'lento', 'lenta', 'lentidão',
    'atrasado', 'atrasada', 'atraso', 'fora', 'extrapolou', 'excedeu',
    'depois', 'tardou', 'tardio', 'tardia', 'aguardando', 'espera', 'esperando',
    'rastreio', 'rastreamento', 'rastrear', 'código', 'tracking', 'track',
    'atualizado', 'atualizada', 'atualização', 'sem informação', 'sem atualização',
    'parado', 'parada', 'estacionado', 'estacionada', 'status', 'situação',
    'movimentação', 'movimentado', 'movimentada', 'objeto', 'encomenda',
    'correios', 'sedex', 'pac', 'carta', 'encomenda normal', 'mini envios',
    'agência', 'unidade', 'carteiro',
    'transportadora', 'jadlog', 'loggi', 'azul cargo', 'total express',
    'braspress', 'tnt', 'fedex', 'dhl', 'ups', 'sequoia', 'mandae',
    'kangu', 'melhor envio', 'intelipost', 'flash', 'rappi', 'lalamove',
    'motoboy', 'entregador', 'entregadora', 'motorista', 'despachante',
    'frete', 'envio', 'postagem', 'despacho', 'postado', 'postada',
    'despachado', 'despachada', 'expedido', 'expedida',
    'grátis', 'gratuito', 'gratuita', 'free', 'sem custo', 'cortesia',
    'caro', 'cara', 'barato', 'barata', 'custo', 'valor', 'taxa',
    'cobrado', 'cobrada', 'pago', 'paga',
    'endereço', 'cep', 'casa', 'apartamento', 'apto', 'trabalho',
    'escritório', 'prédio', 'condomínio', 'portaria', 'porteiro',
    'recepção', 'vizinho', 'vizinha', 'familiar', 'responsável',
    'errado', 'errada', 'incorreto', 'incorreta', 'incompleto', 'incompleta',
    'faltando', 'falta', 'complemento', 'número', 'rua', 'avenida',
    'bairro', 'cidade', 'estado', 'referência',
    'tentativa', 'tentou', 'ausente', 'não estava', 'ninguém', 'retirar',
    'retirada', 'aviso', 'agendamento', 'reagendar', 'remarcar',
    'amassado', 'amassada', 'danificado', 'danificada', 'violado', 'violada',
    'aberto', 'aberta', 'rasgado', 'rasgada', 'molhado', 'molhada',
    'quebrado', 'quebrada', 'intacto', 'intacta', 'perfeito', 'perfeita',
    'caixa', 'papelão', 'envelope', 'saco', 'plástico bolha', 'proteção',
    'bem embalado', 'mal embalado', 'sem proteção', 'frágil',
    'extraviado', 'extraviada', 'perdido', 'perdida', 'sumiu', 'desapareceu',
    'roubado', 'roubada', 'furtado', 'furtada', 'não chegou', 'não recebi',
    'devolver', 'devolução', 'reenvio', 'reenviar',
}

# TÓPICO 4: ATENDIMENTO AO CLIENTE
TOPIC_ATENDIMENTO_CLIENTE = {
    'atendimento', 'atendente', 'atendeu', 'suporte', 'sac', 'educado', 'educada',
    'gentil', 'atencioso', 'atenciosa', 'prestativo', 'prestativa', 'solicito',
    'solícita', 'cordial', 'simpático', 'simpática', 'amável', 'paciente',
    'profissional', 'competente', 'eficiente', 'excelente', 'ótimo', 'ótima',
    'maravilhoso', 'maravilhosa', 'perfeito', 'perfeita',
    'grosseiro', 'grosseira', 'mal educado', 'mal educada', 'rude', 'ríspido',
    'ríspida', 'ignorou', 'despreparado', 'despreparada', 'incompetente',
    'amador', 'péssimo', 'péssima', 'horrível', 'terrível', 'desatencioso',
    'desatenciosa', 'impaciente', 'arrogante', 'debochado', 'debochada',
    'whatsapp', 'zap', 'wpp', 'telegram', 'mensagem', 'msg', 'direct',
    'dm', 'inbox', 'chat', 'chatbot', 'bot', 'automático', 'automática',
    'telefone', 'ligação', 'ligou', 'ligar', 'telefonema', 'atender',
    'atendeu', 'chamada', 'linha', 'ocupado', 'ocupada', 'mudo', 'muda',
    'caixa postal', 'gravação', 'ura', 'ramal',
    'email', 'e-mail', 'mensagem', 'site', 'plataforma', 'sistema',
    'formulário', 'ticket', 'protocolo', 'chamado', 'abertura',
    'instagram', 'insta', 'facebook', 'face', 'twitter', 'tiktok',
    'comentário', 'post', 'story', 'stories', 'publicação',
    'rápido', 'rápida', 'rapidez', 'instantâneo', 'instantânea', 'imediato',
    'imediata', 'ágil', 'agilidade', 'pronto', 'prontidão', 'veloz',
    'respondeu', 'resposta', 'retorno', 'retornou',
    'demorou', 'demora', 'demorado', 'demorada', 'lento', 'lenta',
    'atrasado', 'atrasada', 'atraso', 'esperando', 'aguardando',
    'sem resposta', 'não respondeu', 'ignorou', 'deixou', 'largou',
    'minutos', 'minuto', 'horas', 'hora', 'dias', 'dia', 'úteis',
    'segundos', 'segundo', 'imediato', 'imediata',
    'troca', 'trocar', 'trocou', 'devolução', 'devolver', 'devolveu',
    'reenvio', 'reenviar', 'reenviou', 'substituição', 'substituir',
    'substituiu', 'arrependimento', 'desistência', 'desistir', 'desisti',
    'reembolso', 'reembolsar', 'reembolsou', 'estorno', 'estornar', 'estornou',
    'ressarcimento', 'ressarcir', 'ressarciu', 'crédito', 'devolver dinheiro',
    'dinheiro de volta', 'cancelamento', 'cancelar', 'cancelou',
    'nota fiscal', 'nf', 'danfe', 'xml', 'cupom', 'voucher', 'comprovante',
    'recibo', 'garantia', 'certificado', 'termo', 'política',
    'resolveu', 'resolvido', 'resolvida', 'solucionou', 'solucionado',
    'solucionada', 'solução', 'resolver', 'solucionar', 'consertou',
    'conserto', 'arrumou', 'arrumado', 'arrumada', 'correção', 'corrigiu',
    'corrigido', 'corrigida', 'ajustou', 'ajuste', 'fix', 'fixou',
    'problema', 'defeito', 'erro', 'falha', 'bug', 'transtorno',
    'inconveniente', 'dificuldade', 'complicação', 'empecilho',
    'obstáculo', 'impedimento',
    'reclamação', 'reclamar', 'reclamei', 'queixa', 'queixar', 'queixei',
    'insatisfação', 'insatisfeito', 'insatisfeita', 'descontente',
    'descontentamento', 'frustrado', 'frustrada', 'frustração',
    'gerente', 'supervisor', 'supervisora', 'coordenador', 'coordenadora',
    'responsável', 'ouvidoria', 'ombudsman', 'procon', 'reclame aqui',
    'consumidor.gov', 'juizado', 'processo', 'ação', 'justiça',
    'pendência', 'pendente', 'aguardando', 'esperando', 'follow up',
    'acompanhamento', 'acompanhar', 'acompanhei', 'andamento',
    'status', 'situação', 'posição', 'prazo', 'deadline',
    'previsão', 'estimativa',
}

# TÓPICO 5: PREÇO E CUSTO-BENEFÍCIO
TOPIC_PRECO_VALOR = {
    'preço', 'valor', 'custo', 'quantia', 'montante', 'total',
    'reais', 'reais', 'centavos', 'dinheiro', 'grana', 'gasto',
    'caro', 'cara', 'carinho', 'carinha', 'caríssimo', 'caríssima',
    'barato', 'barata', 'baratinho', 'baratinha', 'baratíssimo', 'baratíssima',
    'acessível', 'inacessível', 'salgado', 'salgada', 'pesado', 'pesada',
    'justo', 'justa', 'injusto', 'injusta', 'razoável', 'irrazoável',
    'abusivo', 'abusiva', 'exorbitante', 'exagerado', 'exagerada',
    'benefício', 'custo-benefício', 'relação', 'vale', 'compensa', 'compensou',
    'investimento', 'vale a pena', 'não vale', 'valeu', 'não valeu',
    'vantagem', 'vantajoso', 'vantajosa', 'desvantagem', 'desvantajoso',
    'economia', 'economizar', 'economizou', 'econômico', 'econômica',
    'desperdício', 'perder dinheiro', 'jogar fora', 'prejuízo',
    'desconto', 'descontão', 'descontaço', 'promoção', 'promocional',
    'oferta', 'ofertão', 'super oferta', 'queima', 'liquidação',
    'black friday', 'cyber monday', 'outlet', 'saldão', 'sale',
    'cupom', 'voucher', 'código', 'cashback', 'cash back',
    'porcentagem', 'por cento', '%', 'desconto de', 'off',
    '10%', '20%', '30%', '40%', '50%', '60%', '70%', '80%', '90%',
    'pagamento', 'pagar', 'paguei', 'pago',
    'parcela', 'parcelas', 'parcelado', 'parcelada', 'parcelar',
    'prestação', 'prestações', 'entrada', 'sinal',
    'sem juros', 'com juros', 'juros', 'juro', 'taxa',
    '1x', '2x', '3x', '4x', '5x', '6x', '10x', '12x', '18x', '24x',
    'à vista', 'vista', 'boleto', 'pix', 'transferência', 'débito',
    'crédito', 'cartão', 'carteira digital', 'paypal', 'mercado pago',
    'picpay', 'ame', 'nubank', 'inter', 'c6', 'neon',
    'concorrente', 'concorrência', 'mercado', 'similar', 'parecido', 'parecida',
    'mesmo', 'mesma', 'igual', 'diferença', 'diferente', 'alternativa',
    'outra loja', 'outro site', 'shopee', 'mercado livre', 'amazon',
    'magalu', 'americanas', 'submarino', 'aliexpress', 'shein',
    'melhor', 'pior', 'mais caro', 'mais barato', 'mais em conta',
    'vantagem', 'desvantagem', 'compensa', 'não compensa',
    'vale cada centavo', 'vale o preço', 'justifica', 'compensa', 'investimento',
    'premium', 'luxo', 'qualidade', 'diferenciado', 'diferenciada',
    'não vale', 'enganação', 'decepção', 'esperava mais', 'mais do mesmo',
    'chinês', 'falsificado', 'ordinário', 'ordinária', 'vagabundo', 'vagabunda',
    'esperava', 'esperado', 'esperada', 'expectativa', 'achei que',
    'pensei que', 'imaginei', 'prometeu', 'prometido', 'prometida',
    'propaganda', 'marketing', 'anúncio', 'anunciado', 'anunciada',
    'fotos', 'imagem', 'descrição', 'conforme', 'diferente',
}

# TÓPICO 6: EXPERIÊNCIA DE COMPRA
TOPIC_EXPERIENCIA_COMPRA = {
    'site', 'website', 'plataforma', 'sistema', 'página', 'aplicativo',
    'app', 'mobile', 'celular', 'computador', 'desktop',
    'fácil', 'facilidade', 'simples', 'intuitivo', 'intuitiva',
    'prático', 'prática', 'funcional', 'rápido', 'rápida',
    'difícil', 'dificuldade', 'complicado', 'complicada', 'confuso', 'confusa',
    'travou', 'trava', 'lento', 'lenta', 'bug', 'erro', 'problema',
    'busca', 'buscar', 'procurar', 'encontrar', 'encontrei', 'achei',
    'categoria', 'filtro', 'filtrar', 'ordenar', 'classificar',
    'menu', 'página', 'seção', 'aba',
    'descrição', 'informação', 'informações', 'detalhes', 'especificações',
    'ficha técnica', 'características', 'dados', 'manual',
    'completo', 'completa', 'incompleto', 'incompleta', 'claro', 'clara',
    'confuso', 'confusa', 'faltando', 'falta', 'omitiu', 'omissão',
    'foto', 'fotos', 'imagem', 'imagens', 'figura', 'ilustração',
    'vídeo', 'vídeos', 'animação', 'gif', 'zoom',
    'nítido', 'nítida', 'borrado', 'borrada', 'desfocado', 'desfocada',
    'escuro', 'escura', 'claro', 'clara', 'ângulo', 'detalhe',
    'avaliação', 'avaliações', 'review', 'reviews', 'comentário', 'comentários',
    'nota', 'estrela', 'estrelas', 'feedback', 'opinião', 'opiniões',
    'carrinho', 'sacola', 'cesta', 'adicionar', 'adicionou', 'remover',
    'removeu', 'selecionar', 'selecionou', 'escolher', 'escolheu',
    'checkout', 'finalizar', 'finalizou', 'concluir', 'concluiu',
    'pagar', 'pagamento', 'confirmação', 'confirmar', 'confirmou',
    'travou', 'trava', 'erro', 'bug', 'não funcionou', 'não foi',
    'não consegui', 'impediu', 'bloqueou', 'falhou', 'falha',
    'primeira vez', 'primeiro pedido', 'primeira compra', 'conheci',
    'descobri', 'experimentei', 'teste', 'testei',
    'compro sempre', 'sempre compro', 'já comprei', 'voltei', 'volto',
    'cliente fiel', 'cliente antigo', 'habitual', 'recorrente',
    'recompra', 'repetir', 'repito', 'segunda vez', 'terceira vez',
    'recomendo', 'indico', 'indicação', 'recomendação', 'compartilhei',
    'compartilhar', 'contei', 'falei bem', 'elogio', 'elogiei',
    'não recomendo', 'não indico', 'não comprem', 'fujam', 'evitem',
    'amei', 'adorei', 'apaixonei', 'feliz', 'felicidade', 'alegre',
    'alegria', 'satisfeito', 'satisfeita', 'satisfação', 'contente',
    'encantado', 'encantada', 'surpreso', 'surpresa', 'surpreendeu',
    'superou', 'excedeu', 'melhor que esperava',
    'decepção', 'decepcionado', 'decepcionada', 'frustrado', 'frustrada',
    'frustração', 'insatisfeito', 'insatisfeita', 'insatisfação',
    'arrependido', 'arrependida', 'arrependimento', 'chateado', 'chateada',
    'nervoso', 'nervosa', 'irritado', 'irritada', 'raiva', 'ódio',
}

# TÓPICO 7: PROBLEMAS E DEFEITOS ESPECÍFICOS
TOPIC_PROBLEMAS_DEFEITOS = {
    'quebrado', 'quebrada', 'quebrou', 'quebrar', 'rachado', 'rachada',
    'rachou', 'rachar', 'trincado', 'trincada', 'trincou', 'trincar',
    'amassado', 'amassada', 'amassou', 'amassar', 'danificado', 'danificada',
    'danificou', 'danificar', 'avariado', 'avariada', 'avariou', 'avariar',
    'vazou', 'vazar', 'vazamento', 'derramou', 'derramar', 'derramado',
    'derramada', 'escorreu', 'escorrer', 'molhou', 'molhado', 'molhada',
    'torto', 'torta', 'entortou', 'entortar', 'deformado', 'deformada',
    'deformou', 'deformar', 'amoleceu', 'amolecer', 'derreteu', 'derreter',
    'descascou', 'descascar', 'descascado', 'descascada', 'desbotou',
    'desbotar', 'desbotado', 'desbotada', 'manchou', 'manchar',
    'manchado', 'manchada', 'enferrujou', 'enferrujar', 'ferrugem',
    'oxidou', 'oxidar', 'oxidado', 'oxidada', 'mofo', 'mofado', 'mofada',
    'não funciona', 'não funcionou', 'defeito', 'defeituoso', 'defeituosa',
    'estragado', 'estragada', 'estragou', 'estragar', 'pifou', 'pifar',
    'morreu', 'morrer', 'parou', 'parar',
    'às vezes funciona', 'funciona mal', 'falha', 'falhando', 'instável',
    'trava', 'travou', 'congelou', 'congelar', 'reinicia', 'reiniciou',
    'incompatível', 'não encaixa', 'não serve', 'errado', 'errada',
    'tamanho errado', 'modelo errado', 'não é o certo',
    'incompleto', 'incompleta', 'faltando', 'falta', 'faltou',
    'sem', 'sem o', 'sem a', 'cadê', 'onde está', 'sumiu',
    'acessório', 'peça', 'parte', 'componente', 'manual', 'instruções',
    'errado', 'errada', 'trocado', 'trocada', 'diferente', 'outro',
    'outra', 'não é o que pedi', 'não é o que comprei', 'confundiram',
    'alergia', 'alérgica', 'alérgico', 'reação', 'reagiu', 'irritação',
    'irritou', 'ardeu', 'ardor', 'queimou', 'queimação', 'coceira',
    'coçou', 'vermelhidão', 'vermelho', 'vermelha', 'mancha', 'manchou',
    'inchado', 'inchada', 'inchaço', 'inchou', 'bolha', 'bolhas',
    'perigoso', 'perigosa', 'perigo', 'risco', 'inseguro', 'insegura',
    'tóxico', 'tóxica', 'nocivo', 'nociva', 'prejudicial', 'veneno',
    'falsificado', 'falsificada', 'falso', 'falsa', 'pirata', 'réplica',
    'cópia', 'imitação', 'fake', 'não é original', 'não é legítimo',
    'chinês', 'importado', 'paralelo',
}

# TÓPICO 8: ELOGIOS E PONTOS POSITIVOS
TOPIC_ELOGIOS_POSITIVOS = {
    'amei', 'adorei', 'apaixonei', 'gostei', 'curti', 'aprovei',
    'recomendo', 'indico', 'super recomendo', 'super indico',
    'excelente', 'ótimo', 'ótima', 'maravilhoso', 'maravilhosa',
    'perfeito', 'perfeita', 'impecável', 'sensacional', 'incrível',
    'fantástico', 'fantástica', 'espetacular', 'show', 'top',
    'demais', 'nota 10', 'nota mil', '10/10', '1000/10',
    'superou', 'superou expectativas', 'melhor que esperava', 'surpreendeu',
    'surpreendente', 'inesperado', 'inesperada', 'além do esperado',
    'excedeu', 'muito mais', 'muito melhor', 'acima da média',
    'satisfeito', 'satisfeita', 'satisfação', 'feliz', 'felicidade',
    'contente', 'alegre', 'radiante', 'realizado', 'realizada',
    'qualidade', 'boa qualidade', 'alta qualidade', 'qualidade superior',
    'premium', 'luxo', 'sofisticado', 'sofisticada', 'refinado', 'refinada',
    'voltarei', 'compro sempre', 'sempre compro', 'cliente fiel',
    'favorita', 'favorito', 'preferida', 'preferido', 'minha loja',
    'obrigado', 'obrigada', 'agradeço', 'gratidão', 'grata', 'grato',
    'parabéns', 'meus parabéns', 'muito obrigado', 'muito obrigada',
}

# Comprehensive Portuguese stopwords for fallback (when NLTK fails)
PORTUGUESE_STOPWORDS_FALLBACK = {
    # Artigos
    'o', 'a', 'os', 'as', 'um', 'uma', 'uns', 'umas',
    # Pronomes
    'eu', 'tu', 'você', 'ele', 'ela', 'nós', 'vós', 'vocês', 'eles', 'elas',
    'me', 'mim', 'comigo', 'te', 'ti', 'contigo', 'lhe', 'lhes',
    'o', 'a', 'os', 'as', 'no', 'na', 'nos', 'nas', 'ao', 'aos', 'à', 'às',
    'meu', 'minha', 'meus', 'minhas', 'teu', 'tua', 'teus', 'tuas',
    'seu', 'sua', 'seus', 'suas', 'nosso', 'nossa', 'nossos', 'nossas',
    'dele', 'dela', 'deles', 'delas',
    
    # Preposições
    'a', 'ante', 'após', 'até', 'com', 'contra', 'de', 'desde', 'em',
    'entre', 'para', 'per', 'perante', 'por', 'sem', 'sob', 'sobre', 'trás',
    
    # Conjunções
    'e', 'mas', 'porém', 'contudo', 'todavia', 'entretanto', 'portanto',
    'pois', 'porque', 'que', 'se', 'como', 'quando', 'onde', 'caso',
    'embora', 'conquanto', 'ainda que', 'mesmo que', 'ou', 'ora', 'quer',
    
    # Advérbios genéricos
    'não', 'sim', 'também', 'tampouco', 'só', 'somente', 'apenas',
    'talvez', 'quiçá', 'acaso', 'porventura', 'certamente', 'decerto',
    'realmente', 'deveras', 'efetivamente',
    
    # Verbos auxiliares
    'ser', 'estar', 'ter', 'haver', 'fazer', 'ir', 'vir', 'poder', 'dever',
    'é', 'são', 'era', 'foram', 'foi', 'sendo', 'sido',
    'está', 'estão', 'estava', 'estavam', 'esteve', 'estiveram', 'estando', 'estado',
    'tem', 'têm', 'tinha', 'tinham', 'teve', 'tiveram', 'tendo', 'tido',
    'há', 'havia', 'houve', 'houveram', 'havendo', 'havido',
    
    # Pronomes demonstrativos
    'este', 'esta', 'estes', 'estas', 'esse', 'essa', 'esses', 'essas',
    'aquele', 'aquela', 'aqueles', 'aquelas', 'isto', 'isso', 'aquilo',
    
    # Pronomes indefinidos
    'algum', 'alguma', 'alguns', 'algumas', 'nenhum', 'nenhuma', 'nenhuns', 'nenhumas',
    'todo', 'toda', 'todos', 'todas', 'outro', 'outra', 'outros', 'outras',
    'muito', 'muita', 'muitos', 'muitas', 'pouco', 'pouca', 'poucos', 'poucas',
    'certo', 'certa', 'certos', 'certas', 'vário', 'vária', 'vários', 'várias',
    'tanto', 'tanta', 'tantos', 'tantas', 'quanto', 'quanta', 'quantos', 'quantas',
    'qualquer', 'quaisquer', 'cada', 'qual', 'quais',
    
    # Contrações comuns
    'do', 'da', 'dos', 'das', 'no', 'na', 'nos', 'nas', 'ao', 'aos', 'à', 'às',
    'pelo', 'pela', 'pelos', 'pelas', 'num', 'numa', 'nuns', 'numas',
    'dum', 'duma', 'duns', 'dumas', 'neste', 'nesta', 'nestes', 'nestas',
    'nesse', 'nessa', 'nesses', 'nessas', 'naquele', 'naquela', 'naqueles', 'naquelas',
    'deste', 'desta', 'destes', 'destas', 'desse', 'dessa', 'desses', 'dessas',
    'daquele', 'daquela', 'daqueles', 'daquelas',
}

# MAPA CONSOLIDADO DE TÓPICOS
TOPIC_CLASSIFICATION_MAP = {
    'Qualidade do Produto': TOPIC_QUALIDADE_PRODUTO,
    'Cosméticos e Beleza': TOPIC_COSMETICOS_BELEZA,
    'Logística e Entrega': TOPIC_LOGISTICA_ENTREGA,
    'Atendimento ao Cliente': TOPIC_ATENDIMENTO_CLIENTE,
    'Preço e Valor': TOPIC_PRECO_VALOR,
    'Experiência de Compra': TOPIC_EXPERIENCIA_COMPRA,
    'Problemas e Defeitos': TOPIC_PROBLEMAS_DEFEITOS,
    'Elogios e Positivos': TOPIC_ELOGIOS_POSITIVOS,
}

def download_nltk_data() -> bool:
    """
    Download required NLTK data if not already downloaded.
    Implementa múltiplas estratégias para garantir sucesso em produção.
    
    Returns:
        bool: True if successful, False otherwise
    """
    import os
    
    # Estratégia 1: Verificar se os dados já estão disponíveis
    try:
        nltk.data.find('tokenizers/punkt')
        nltk.data.find('corpora/stopwords')
        nltk.data.find('corpora/wordnet')
        nltk.data.find('corpora/omw-1.4')  # Verificar omw-1.4 também
        print("✅ Dados NLTK já disponíveis")
        return True
    except LookupError:
        print("📥 Dados NLTK não encontrados, iniciando download...")
    
    # Estratégia 2: Configurar múltiplos diretórios de dados NLTK
    try:
        # Definir múltiplos diretórios possíveis para máxima compatibilidade
        import tempfile
        temp_base = tempfile.gettempdir()  # Seguro: usa tempfile ao invés de hardcoded
        possible_dirs = [
            os.path.expanduser('~/nltk_data'),
            os.path.join(temp_base, 'nltk_data'),  # Seguro: tempfile.gettempdir()
            './nltk_data',
            os.path.join(os.getcwd(), 'nltk_data'),
        ]
        
        nltk_data_dir = None
        for test_dir in possible_dirs:
            try:
                if not os.path.exists(test_dir):
                    os.makedirs(test_dir, exist_ok=True)
                
                # Testar se consegue escrever no diretório
                test_file = os.path.join(test_dir, '.test_write')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                
                # Se chegou até aqui, o diretório é válido
                nltk_data_dir = test_dir
                break
            except Exception:
                continue
        
        if nltk_data_dir:
            # Configurar NLTK para usar este diretório
            if nltk_data_dir not in nltk.data.path:
                nltk.data.path.insert(0, nltk_data_dir)  # Inserir no início para prioridade
            
            # Configurar variável de ambiente
            os.environ['NLTK_DATA'] = nltk_data_dir
            print(f"📁 Diretório NLTK configurado: {nltk_data_dir}")
        else:
            print("⚠️ Nenhum diretório NLTK válido encontrado, usando padrão")
        
    except Exception as e:
        print(f"⚠️ Erro ao configurar diretórios NLTK: {e}")
    
    # Estratégia 3: Download agressivo com múltiplas tentativas
    datasets = ['punkt', 'stopwords', 'wordnet', 'omw-1.4']  # Adicionado omw-1.4 para lematização
    success_count = 0
    
    for dataset in datasets:
        max_retries = 5  # Aumentado de 3 para 5
        for attempt in range(max_retries):
            try:
                print(f"📥 Baixando {dataset} (tentativa {attempt + 1}/{max_retries})")
                
                # Configurar SSL e timeout mais agressivamente
                import urllib.request
                import ssl
                
                # Criar contexto SSL permissivo
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                # Configurar timeout maior
                urllib.request.socket.setdefaulttimeout(120)  # 2 minutos
                
                # Tentar diferentes estratégias de download
                download_strategies = [
                    {'quiet': True, 'force': False, 'halt_on_error': False},
                    {'quiet': False, 'force': True, 'halt_on_error': False},  # Forçar se necessário
                    {'quiet': True, 'force': True, 'halt_on_error': True}   # Última tentativa
                ]
                
                dataset_downloaded = False
                for strategy in download_strategies:
                    try:
                        result = nltk.download(dataset, **strategy)
                        if result:
                            print(f"✅ {dataset} baixado com sucesso (estratégia {strategy})")
                            
                            # Verificar se precisa descompactar (wordnet e omw-1.4 às vezes ficam como ZIP)
                            if dataset in ['wordnet', 'omw-1.4'] and nltk_data_dir:
                                try:
                                    import zipfile
                                    corpora_dir = os.path.join(nltk_data_dir, 'corpora')
                                    zip_file = os.path.join(corpora_dir, f'{dataset}.zip')
                                    
                                    if os.path.exists(zip_file) and not os.path.exists(os.path.join(corpora_dir, dataset)):
                                        print(f"🔧 Descompactando {dataset}...")
                                        zipfile.ZipFile(zip_file).extractall(corpora_dir)
                                        print(f"✅ {dataset} descompactado com sucesso")
                                except Exception as extract_error:
                                    print(f"⚠️ Erro ao descompactar {dataset}: {extract_error}")
                            
                            if not dataset_downloaded:  # Só contar uma vez por dataset
                                success_count += 1
                                dataset_downloaded = True
                            break
                    except Exception as strategy_error:
                        print(f"❌ Estratégia {strategy} falhou: {strategy_error}")
                        continue
                else:
                    # Se todas as estratégias falharam, tentar download direto
                    print(f"🔄 Tentando download direto de {dataset}...")
                    try:
                        # Download manual via URL (última tentativa)
                        import urllib.request
                        base_url = "https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages"
                        
                        if dataset == 'punkt':
                            url = f"{base_url}/tokenizers/punkt.zip"
                        elif dataset == 'stopwords':
                            url = f"{base_url}/corpora/stopwords.zip"
                        elif dataset == 'wordnet':
                            url = f"{base_url}/corpora/wordnet.zip"
                        elif dataset == 'omw-1.4':
                            url = f"{base_url}/corpora/omw-1.4.zip"
                        
                        print(f"📥 Tentando download direto de {url}")
                        # Apenas tentar, não implementar download completo aqui
                        # (seria muito complexo)
                        
                    except Exception as direct_error:
                        print(f"❌ Download direto falhou: {direct_error}")
                
                if dataset_downloaded:
                    break  # Sucesso, sair do loop de tentativas
                    
            except Exception as e:
                print(f"❌ Erro geral no download de {dataset}: {e}")
                if attempt == max_retries - 1:
                    print(f"🚨 Falha definitiva no download de {dataset}")
    
    # Estratégia 4: Verificação final e relatório
    try:
        nltk.data.find('tokenizers/punkt')
        nltk.data.find('corpora/stopwords')
        nltk.data.find('corpora/wordnet')
        nltk.data.find('corpora/omw-1.4')  # Verificar omw-1.4 também
        print(f"✅ NLTK configurado com sucesso! ({success_count}/4 datasets)")
        return True
    except LookupError:
        print(f"❌ NLTK não configurado corretamente ({success_count}/4 datasets)")
        print("🔄 Continuando com fallback robusto...")
        return False

def preprocess_text(text: str) -> str:
    """Preprocess text for NLP analysis."""
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove special characters and numbers, but keep letters with accents
    text = re.sub(r'[^a-záéíóúâêîôûãõçà\s]', '', text)
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Remove stopwords (Portuguese + custom)
    try:
        stop_words = set(stopwords.words('portuguese')).union(CUSTOM_STOP_WORDS)
    except Exception:
        # Fallback para stopwords se NLTK falhar
        stop_words = PORTUGUESE_STOPWORDS_FALLBACK.union(CUSTOM_STOP_WORDS)
    
    tokens = [token for token in tokens if token not in stop_words and len(token) > 2]
    
    # Lemmatize
    try:
        lemmatizer = WordNetLemmatizer()
        tokens = [lemmatizer.lemmatize(token) for token in tokens]
    except Exception:
        # Se lemmatização falhar, continuar sem ela
        pass
    
    return ' '.join(tokens)

def preprocess_text_fallback(text: str) -> str:
    """Fallback preprocessing when NLTK completely fails."""
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove special characters and numbers, but keep letters with accents
    text = re.sub(r'[^a-záéíóúâêîôûãõçà\s]', '', text)
    
    # Simple tokenization (split by whitespace)
    tokens = text.split()
    
    # Remove stopwords using fallback list
    all_stopwords = PORTUGUESE_STOPWORDS_FALLBACK.union(CUSTOM_STOP_WORDS)
    tokens = [token for token in tokens if token not in all_stopwords and len(token) > 2]
    
    return ' '.join(tokens)

def generate_wordcloud(text: str, title: str, background_color: str = 'white', colormap: str = 'viridis') -> Optional[Any]:
    """
    Generate and return a wordcloud figure with dark background and semantic colors.
    
    Returns:
        matplotlib figure or None if WordCloud is not available or text is empty
    """
    if WordCloud is None or not text or not text.strip():
        return None

    # Usar fundo escuro grafite
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color='#1e293b',  # Fundo grafite escuro
        max_words=100,
        colormap=colormap,  # Colormap semântico
        contour_width=0,
        relative_scaling=0.5,
        min_font_size=10
    ).generate(text)
    
    # Criar figura com fundo transparente
    fig, ax = plt.subplots(figsize=(10, 5), facecolor='#1e293b')
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    ax.set_facecolor('#1e293b')
    fig.patch.set_facecolor('#1e293b')
    plt.tight_layout(pad=0)
    return fig

def extract_topics(text: str, n_topics: int = 3, n_words: int = 10, method: str = 'lda') -> List[str]:
    """Extract topics using either LDA or NMF."""
    # Check if text is empty or too short
    if not text or len(text.strip()) < 10:
        return [f"Não há texto suficiente para análise de tópicos"]
    
    # Create document-term matrix using CountVectorizer instead of TfidfVectorizer
    # This is more suitable for our single-document case
    vectorizer = CountVectorizer(
        max_features=1000,
        stop_words=list(CUSTOM_STOP_WORDS)
    )
    
    try:
        dtm = vectorizer.fit_transform([text])
        
        # Check if we have enough features
        if dtm.shape[1] < n_topics:
            return [f"Não há palavras suficientes para {n_topics} tópicos"]
        
        # Choose topic modeling method
        if method == 'lda':
            model = LatentDirichletAllocation(
                n_components=min(n_topics, dtm.shape[1]),
                random_state=42,
                max_iter=10
            )
        else:  # NMF
            model = NMF(
                n_components=min(n_topics, dtm.shape[1]),
                random_state=42,
                max_iter=200
            )
        
        # Fit model and extract topics
        model.fit(dtm)
        feature_names = vectorizer.get_feature_names_out()
        topics = []

        for topic_idx, topic in enumerate(model.components_):
            top_words_idx = topic.argsort()[:-n_words-1:-1]
            top_words = [feature_names[i] for i in top_words_idx]
            topics.append(f"Tópico {topic_idx + 1}: {', '.join(top_words)}")
        
        return topics
        
    except Exception as e:
        return [f"Erro na análise de tópicos: {str(e)}"]

def classify_words_by_ecommerce_topic(words: List[str]) -> Dict[str, List[str]]:
    """
    Classifica uma lista de palavras nos tópicos semânticos de e-commerce.
    
    Args:
        words: Lista de palavras para classificar
        
    Returns:
        Dicionário com as palavras agrupadas por tópico
    """
    # Mapeamento interno simplificado para as chaves de resultado
    topic_keys = {
        'Qualidade do Produto': 'produto',
        'Cosméticos e Beleza': 'cosmeticos',
        'Logística e Entrega': 'entrega',
        'Atendimento ao Cliente': 'atendimento',
        'Preço e Valor': 'preco',
        'Experiência de Compra': 'experiencia',
        'Problemas e Defeitos': 'problemas',
        'Elogios e Positivos': 'elogios',
    }
    
    classified = {key: [] for key in topic_keys.values()}
    classified['outros'] = []
    
    for word in words:
        word_lower = word.lower().strip()
        found = False
        
        # Verificar em cada tópico
        for topic_name, keywords in TOPIC_CLASSIFICATION_MAP.items():
            if word_lower in keywords:
                internal_key = topic_keys.get(topic_name)
                if internal_key:
                    classified[internal_key].append(word)
                    found = True
                    break # Assume que a palavra pertence ao primeiro tópico encontrado (prioridade)
        
        if not found:
            classified['outros'].append(word)
    
    return classified

def extract_ecommerce_topics(text: str, n_words: int = 15) -> Dict[str, Any]:
    """
    Extrai e classifica palavras-chave em tópicos semânticos de e-commerce.
    
    Usa dicionários expandidos com 8 categorias.
    
    Args:
        text: Texto pré-processado para análise
        n_words: Número máximo de palavras a extrair por tópico
        
    Returns:
        Dicionário com tópicos classificados e suas palavras
    """
    if not text or len(text.strip()) < 10:
        return {'summary': []}
    
    # Tokenizar e contar frequências
    words = text.split()
    word_freq = pd.Series(words).value_counts()
    
    # Pegar as palavras mais frequentes (aumentado para capturar mais diversidade)
    top_words = word_freq.head(200).index.tolist()
    
    # Classificar palavras
    classified = classify_words_by_ecommerce_topic(top_words)
    
    # Definição de metadados para cada tópico (ícone, cor, label)
    topic_metadata = {
        'produto': {'icon': '📦', 'color': '#3b82f6', 'label': 'Qualidade do Produto'},
        'cosmeticos': {'icon': '💄', 'color': '#ec4899', 'label': 'Cosméticos e Beleza'},
        'entrega': {'icon': '🚚', 'color': '#f59e0b', 'label': 'Logística e Entrega'},
        'atendimento': {'icon': '💬', 'color': '#8b5cf6', 'label': 'Atendimento'},
        'preco': {'icon': '💰', 'color': '#10b981', 'label': 'Preço e Valor'},
        'experiencia': {'icon': '🛍️', 'color': '#a855f7', 'label': 'Experiência de Compra'},
        'problemas': {'icon': '⚠️', 'color': '#ef4444', 'label': 'Problemas e Defeitos'},
        'elogios': {'icon': '🌟', 'color': '#eab308', 'label': 'Elogios e Positivos'},
        'outros': {'icon': '📝', 'color': '#64748b', 'label': 'Outros'},
    }
    
    result = {}
    summary = []
    
    # Construir o resultado
    for key, data in classified.items():
        meta = topic_metadata.get(key, topic_metadata['outros'])
        
        topic_info = {
            'words': data[:n_words],
            'count': len(data),
            'icon': meta['icon'],
            'color': meta['color'],
            'label': meta['label']
        }
        
        result[key] = topic_info
        
        # Adicionar ao resumo se tiver palavras
        if data and key != 'outros':
            summary.append({
                'topic': key,
                'label': meta['label'],
                'icon': meta['icon'],
                'color': meta['color'],
                'words': data[:5],  # Top 5 para o resumo
                'count': len(data)
            })
            
    # Ordenar resumo por relevância (contagem de palavras encontradas)
    summary.sort(key=lambda x: x['count'], reverse=True)
    result['summary'] = summary
    
    return result

def format_ecommerce_topics_for_display(topics_result: Dict[str, Any], max_topics: int = 4) -> List[str]:
    """
    Formata os tópicos de e-commerce para exibição na interface.
    
    Args:
        topics_result: Resultado de extract_ecommerce_topics
        max_topics: Número máximo de tópicos a exibir
        
    Returns:
        Lista de strings formatadas para exibição
    """
    if not topics_result.get('summary'):
        return ["Não há dados suficientes para identificar tópicos"]
    
    formatted = []
    for topic in topics_result['summary'][:max_topics]:
        if topic['words']:
            words_str = ', '.join(topic['words'])
            formatted.append(f"{topic['icon']} {topic['label']}: {words_str}")
    
    if not formatted:
        return ["Não há dados suficientes para identificar tópicos"]
    
    return formatted

def analyze_sentiment_patterns(reviews: List[str]) -> Dict[str, Dict[str, int]]:
    """Analyze patterns in reviews to identify common sentiments."""
    # Define sentiment patterns
    positive_patterns = {
        'qualidade': r'(boa|ótima|excelente)\s+qualidade',
        'entrega': r'(entrega\s+rápida|chegou\s+antes)',
        'recomendação': r'(recomendo|voltarei\s+a\s+comprar)',
        'satisfação': r'(muito\s+satisfeito|adorei|gostei\s+muito)',
        'preço': r'(bom\s+preço|preço\s+justo|custo\s+benefício)'
    }
    
    negative_patterns = {
        'atraso': r'(atrasado|não\s+chegou|demora)',
        'qualidade': r'(má\s+qualidade|péssimo|ruim)',
        'problema': r'(defeito|problema|quebrado)',
        'atendimento': r'(péssimo\s+atendimento|sem\s+resposta)',
        'preço': r'(caro|não\s+vale|preço\s+alto)'
    }
    
    patterns_found = {
        'positive': {k: 0 for k in positive_patterns},
        'negative': {k: 0 for k in negative_patterns}
    }
    
    # Count pattern occurrences
    for review in reviews:
        if isinstance(review, str):
            review = review.lower()
            for category, pattern in positive_patterns.items():
                if re.search(pattern, review):
                    patterns_found['positive'][category] += 1
            
            for category, pattern in negative_patterns.items():
                if re.search(pattern, review):
                    patterns_found['negative'][category] += 1
    
    return patterns_found

def analyze_reviews(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze customer reviews and return insights."""
    # Check if required columns exist
    if 'review_score' not in df.columns or 'review_comment_message' not in df.columns:
        return {
            'positive_wordcloud': None,
            'neutral_wordcloud': None,
            'negative_wordcloud': None,
            'positive_freq': pd.Series(),
            'neutral_freq': pd.Series(),
            'negative_freq': pd.Series(),
            'positive_topics_lda': ["Dados insuficientes"],
            'neutral_topics_lda': ["Dados insuficientes"],
            'negative_topics_lda': ["Dados insuficientes"],
            'positive_topics_nmf': ["Dados insuficientes"],
            'neutral_topics_nmf': ["Dados insuficientes"],
            'negative_topics_nmf': ["Dados insuficientes"],
            'sentiment_patterns': {
                'positive': {'qualidade': 0, 'entrega': 0, 'recomendação': 0, 'satisfação': 0, 'preço': 0},
                'neutral': {'qualidade': 0, 'entrega': 0, 'recomendação': 0, 'satisfação': 0, 'preço': 0},
                'negative': {'atraso': 0, 'qualidade': 0, 'problema': 0, 'atendimento': 0, 'preço': 0}
            },
            'metrics': {
                'avg_positive_length': 0,
                'avg_neutral_length': 0,
                'avg_negative_length': 0,
                'positive_count': 0,
                'neutral_count': 0,
                'negative_count': 0,
            }
        }
    
    # Garantir que pacotes NLTK necessários existam (funciona também em ambiente sem cache)
    try:
        print("🔄 Inicializando dados NLTK...")
        success = download_nltk_data()
        if success:
            print("✅ NLTK inicializado com sucesso!")
        else:
            print("⚠️ NLTK não pôde ser inicializado completamente, usando fallback robusto")
    except Exception as e:
        print(f"❌ Erro na inicialização NLTK: {e}")
        print("🔄 Continuando com fallback robusto...")
        # Se falhar, ainda podemos continuar – apenas pulará etapas que exigem NLP pesada
        pass

    # ==============================================================
    # Sentiment classification (TEXT-BASED)
    # ==============================================================
    # Objetivo: separar Positiva/Neutra/Negativa pelo SENTIMENTO do texto,
    # e não pela nota. Mantém fallback por nota quando não houver texto.

    # --------------------------------------------------------------
    # Sentiment engines (opcional):
    # - LeIA (PT-BR): pip install leia-br  -> from leia import SentimentIntensityAnalyzer
    # - VADER (NLTK): mais adequado para EN; não recomendado como default em PT-BR
    # --------------------------------------------------------------
    _leia_analyzer = None
    try:
        from leia import SentimentIntensityAnalyzer as _LeIASIA  # type: ignore
        _leia_analyzer = _LeIASIA()
    except Exception:
        _leia_analyzer = None

    POSITIVE_CUES = {
        "amei", "adorei", "recomendo", "excelente", "ótimo", "otimo", "perfeito",
        "maravilhoso", "incrível", "sensacional", "top", "bom", "boa", "gostei",
        "chegou rápido", "chegou rapido", "entrega rápida", "entrega rapida",
        "super recomendo", "vale a pena", "muito bom", "muito boa",
    }
    NEGATIVE_CUES = {
        "ruim", "péssimo", "pessimo", "horrível", "horrivel", "terrível", "terrivel",
        "não gostei", "nao gostei", "não recomendo", "nao recomendo", "decepção", "decepcao",
        "demorou", "atraso", "atrasado", "não chegou", "nao chegou", "defeito", "quebrado",
        "não funciona", "nao funciona", "cancelar", "reembolso", "devolução", "devolucao",
    }

    def _classify_sentiment_text(text: Any) -> str:
        if not isinstance(text, str):
            return "unknown"
        t = text.strip().lower()
        if not t:
            return "unknown"

        # 1) LeIA (preferencial em PT-BR) se disponível
        if _leia_analyzer is not None:
            try:
                scores = _leia_analyzer.polarity_scores(text)
                compound = float(scores.get("compound", 0.0) or 0.0)
                # Thresholds compatíveis com VADER/LeIA: neutro ~ [-0.05, 0.05]
                if compound >= 0.05:
                    return "positive"
                if compound <= -0.05:
                    return "negative"
                return "neutral"
            except Exception:
                # Se LeIA falhar em algum texto, cair para regras
                pass

        # Negation overrides (quick wins)
        if "não gostei" in t or "nao gostei" in t or "não recomendo" in t or "nao recomendo" in t:
            return "negative"

        pos = 0
        neg = 0

        # Phrase cues
        for cue in POSITIVE_CUES:
            if cue in t:
                pos += 1
        for cue in NEGATIVE_CUES:
            if cue in t:
                neg += 1

        # Token-level cues (fallback)
        try:
            tokens = re.findall(r"[a-záéíóúâêîôûãõçà]+", t)
            for tok in tokens:
                if tok in POSITIVE_CUES:
                    pos += 1
                if tok in NEGATIVE_CUES:
                    neg += 1
        except Exception:
            pass

        score = pos - neg
        if score >= 1:
            return "positive"
        if score <= -1:
            return "negative"
        return "neutral"

    dfx = df.copy()
    # text for sentiment
    dfx["__text"] = dfx.get("review_comment_message", pd.Series(dtype=str)).fillna("").astype(str)
    dfx["__sentiment"] = dfx["__text"].apply(_classify_sentiment_text)

    # Fallback: if text is missing/unknown, classify by rating (keeps coverage)
    if "review_score" in dfx.columns:
        rs = pd.to_numeric(dfx["review_score"], errors="coerce")
        fb = pd.Series(index=dfx.index, dtype=object)
        fb[rs >= 4] = "positive"
        fb[rs == 3] = "neutral"
        fb[rs <= 2] = "negative"
        unknown_mask = dfx["__sentiment"].isin(["unknown"]) | (dfx["__text"].str.strip() == "")
        dfx.loc[unknown_mask, "__sentiment"] = fb.loc[unknown_mask].fillna("neutral")

    # Only text entries for NLP content (wordcloud/topics)
    positive_reviews = dfx.loc[dfx["__sentiment"] == "positive", "__text"]
    neutral_reviews = dfx.loc[dfx["__sentiment"] == "neutral", "__text"]
    negative_reviews = dfx.loc[dfx["__sentiment"] == "negative", "__text"]

    positive_reviews = positive_reviews[positive_reviews.str.strip() != ""]
    neutral_reviews = neutral_reviews[neutral_reviews.str.strip() != ""]
    negative_reviews = negative_reviews[negative_reviews.str.strip() != ""]

    # If no text available at all, return early with empty metrics
    if (
        positive_reviews.empty
        and neutral_reviews.empty
        and negative_reviews.empty
    ):
        return {
            'positive_wordcloud': None,
            'neutral_wordcloud': None,
            'negative_wordcloud': None,
            'positive_freq': pd.Series(dtype=int),
            'neutral_freq': pd.Series(dtype=int),
            'negative_freq': pd.Series(dtype=int),
            'positive_topics_lda': ["Dados insuficientes"],
            'neutral_topics_lda': ["Dados insuficientes"],
            'negative_topics_lda': ["Dados insuficientes"],
            'positive_topics_nmf': ["Dados insuficientes"],
            'neutral_topics_nmf': ["Dados insuficientes"],
            'negative_topics_nmf': ["Dados insuficientes"],
            'sentiment_patterns': {
                'positive': {'qualidade': 0, 'entrega': 0, 'recomendação': 0, 'satisfação': 0, 'preço': 0},
                'neutral': {'qualidade': 0, 'entrega': 0, 'recomendação': 0, 'satisfação': 0, 'preço': 0},
                'negative': {'atraso': 0, 'qualidade': 0, 'problema': 0, 'atendimento': 0, 'preço': 0}
            },
            'metrics': {
                'avg_positive_length': 0,
                'avg_neutral_length': 0,
                'avg_negative_length': 0,
                'positive_count': 0,
                'neutral_count': 0,
                'negative_count': 0,
            }
        }
    
    # Preprocess reviews with error handling
    try:
        positive_text = ' '.join(positive_reviews.apply(preprocess_text))
        neutral_text = ' '.join(neutral_reviews.apply(preprocess_text))
        negative_text = ' '.join(negative_reviews.apply(preprocess_text))
    except Exception as e:
        # Fallback robusto com filtragem de stopwords
        print(f"Aviso: Falha no processamento NLP completo, usando fallback com filtragem: {e}")
        try:
            positive_text = ' '.join(positive_reviews.apply(preprocess_text_fallback))
            neutral_text = ' '.join(neutral_reviews.apply(preprocess_text_fallback))
            negative_text = ' '.join(negative_reviews.apply(preprocess_text_fallback))
        except Exception as e2:
            # Último fallback: processamento mínimo mas com filtragem básica
            print(f"Aviso: Usando fallback mínimo: {e2}")
            all_stopwords = PORTUGUESE_STOPWORDS_FALLBACK.union(CUSTOM_STOP_WORDS)
            
            def basic_clean(text):
                if not isinstance(text, str):
                    return ""
                text = text.lower()
                text = re.sub(r'[^a-záéíóúâêîôûãõçà\s]', '', text)
                words = [w for w in text.split() if w not in all_stopwords and len(w) > 2]
                return ' '.join(words)
            
            positive_text = ' '.join(positive_reviews.apply(basic_clean))
            neutral_text = ' '.join(neutral_reviews.apply(basic_clean))
            negative_text = ' '.join(negative_reviews.apply(basic_clean))
    
    # Generate wordclouds with semantic colormaps
    try:
        positive_wordcloud = generate_wordcloud(positive_text, "Avaliações Positivas", '#1e293b', 'Greens')
        neutral_wordcloud = generate_wordcloud(neutral_text, "Avaliações Neutras", '#1e293b', 'Purples')
        negative_wordcloud = generate_wordcloud(negative_text, "Avaliações Negativas", '#1e293b', 'Reds')
    except Exception as e:
        print(f"Aviso: Falha ao gerar wordclouds: {e}")
        positive_wordcloud = None
        neutral_wordcloud = None
        negative_wordcloud = None
    
    # Calculate word frequencies
    def get_word_frequencies(text: str) -> pd.Series:
        words = text.split()
        freq = pd.Series(words).value_counts().head(20)
        return freq
    
    positive_freq = get_word_frequencies(positive_text)
    neutral_freq = get_word_frequencies(neutral_text)
    negative_freq = get_word_frequencies(negative_text)
    
    # Extract topics using both LDA and NMF
    positive_topics_lda = extract_topics(positive_text, method='lda')
    neutral_topics_lda = extract_topics(neutral_text, method='lda')
    negative_topics_lda = extract_topics(negative_text, method='lda')
    
    positive_topics_nmf = extract_topics(positive_text, method='nmf')
    neutral_topics_nmf = extract_topics(neutral_text, method='nmf')
    negative_topics_nmf = extract_topics(negative_text, method='nmf')
    
    # Extract e-commerce semantic topics (nova funcionalidade)
    positive_ecommerce_topics = extract_ecommerce_topics(positive_text)
    neutral_ecommerce_topics = extract_ecommerce_topics(neutral_text)
    negative_ecommerce_topics = extract_ecommerce_topics(negative_text)
    
    # Format for display
    positive_topics_semantic = format_ecommerce_topics_for_display(positive_ecommerce_topics)
    neutral_topics_semantic = format_ecommerce_topics_for_display(neutral_ecommerce_topics)
    negative_topics_semantic = format_ecommerce_topics_for_display(negative_ecommerce_topics)
    
    # Analyze sentiment patterns
    positive_patterns = analyze_sentiment_patterns(positive_reviews)
    neutral_patterns = analyze_sentiment_patterns(neutral_reviews)
    negative_patterns = analyze_sentiment_patterns(negative_reviews)
    
    # Calculate additional metrics
    sentiment_metrics = {
        'avg_positive_length': positive_reviews.str.len().mean() if len(positive_reviews) > 0 else 0,
        'avg_neutral_length': neutral_reviews.str.len().mean() if len(neutral_reviews) > 0 else 0,
        'avg_negative_length': negative_reviews.str.len().mean() if len(negative_reviews) > 0 else 0,
        'positive_count': len(positive_reviews),
        'neutral_count': len(neutral_reviews),
        'negative_count': len(negative_reviews),
    }
    
    return {
        'positive_wordcloud': positive_wordcloud,
        'neutral_wordcloud': neutral_wordcloud,
        'negative_wordcloud': negative_wordcloud,
        'positive_freq': positive_freq,
        'neutral_freq': neutral_freq,
        'negative_freq': negative_freq,
        'positive_topics_lda': positive_topics_lda,
        'neutral_topics_lda': neutral_topics_lda,
        'negative_topics_lda': negative_topics_lda,
        'positive_topics_nmf': positive_topics_nmf,
        'neutral_topics_nmf': neutral_topics_nmf,
        'negative_topics_nmf': negative_topics_nmf,
        # Novos tópicos semânticos de e-commerce
        'positive_topics_semantic': positive_topics_semantic,
        'neutral_topics_semantic': neutral_topics_semantic,
        'negative_topics_semantic': negative_topics_semantic,
        'positive_ecommerce_topics': positive_ecommerce_topics,
        'neutral_ecommerce_topics': neutral_ecommerce_topics,
        'negative_ecommerce_topics': negative_ecommerce_topics,
        'sentiment_patterns': {
            'positive': positive_patterns,
            'neutral': neutral_patterns,
            'negative': negative_patterns
        },
        'metrics': sentiment_metrics
    } 

# Streamlit-specific wrapper functions
def download_nltk_data_cached() -> None:
    """Cached version of download_nltk_data for Streamlit."""
    @st.cache_data(
        hash_funcs={
            Path: _hash_path,
            pd.DataFrame: _hash_dataframe
        }
    )
    def _cached_download():
        return download_nltk_data()
    
    success = _cached_download()
    if not success:
        st.warning("Não foi possível baixar os dados do NLTK. Algumas funcionalidades podem não funcionar corretamente.")

def generate_wordcloud_with_warning(text: str, title: str, background_color: str = 'white', colormap: str = 'viridis') -> Optional[Any]:
    """Generate wordcloud with Streamlit warning if WordCloud is not available."""
    result = generate_wordcloud(text, title, background_color, colormap)
    if result is None:
        st.warning("A biblioteca 'wordcloud' não está instalada. Não é possível gerar a nuvem de palavras.")
    return result 