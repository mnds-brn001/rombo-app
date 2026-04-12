Sinto que os tópicos acabaram ficando muito restritivos.

Aqui, bolei umas mais elaboradas, gostaria que implementasse pra mim, e se você tiver sugestões que somem a usabilidade, estou aberto a isso.
---------
🎯 DICIONÁRIOS SEMÂNTICOS EXPANDIDOS - ANÁLISE DE VOZ DO CLIENTE E-COMMERCE

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
# TÓPICO 1: QUALIDADE DO PRODUTO
# =============================================================================

TOPIC_QUALIDADE_PRODUTO = {
    # ========== AVALIAÇÕES GERAIS ==========
    # Positivas
    'qualidade', 'ótimo', 'ótima', 'excelente', 'excepcional', 'impecável',
    'perfeito', 'perfeita', 'maravilhoso', 'maravilhosa', 'incrível', 
    'sensacional', 'espetacular', 'fantástico', 'fantástica', 'show',
    'top', 'premium', 'luxo', 'luxuoso', 'luxuosa', 'sofisticado', 'sofisticada',
    'amei', 'adorei', 'aprovei', 'recomendo', 'super', 'mega',
    
    # Intermediárias
    'bom', 'boa', 'razoável', 'aceitável', 'ok', 'normal', 'comum',
    'regular', 'mediano', 'mediana', 'esperado', 'esperada',
    
    # Negativas
    'ruim', 'péssimo', 'péssima', 'horrível', 'terrível', 'péssima',
    'lixo', 'porcaria', 'decepção', 'decepcionante', 'frustrante',
    'insatisfeito', 'insatisfeita', 'arrependido', 'arrependida',
    'chinês', 'falsificado', 'falso', 'falsa', 'imitação', 'pirata',
    
    # ========== CARACTERÍSTICAS FÍSICAS ==========
    # Dimensões
    'tamanho', 'grande', 'pequeno', 'pequena', 'médio', 'média',
    'gigante', 'enorme', 'imenso', 'minúsculo', 'minúscula', 'tiny',
    'largo', 'larga', 'estreito', 'estreita', 'fino', 'fina',
    'grosso', 'grossa', 'espesso', 'espessa', 'comprido', 'comprida',
    'curto', 'curta', 'alto', 'alta', 'baixo', 'baixa',
    
    # Peso e densidade
    'leve', 'pesado', 'pesada', 'volumoso', 'volumosa', 'compacto', 'compacta',
    'denso', 'densa', 'oco', 'oca', 'maciço', 'maciça',
    
    # Texturas e superfícies
    'textura', 'macio', 'macia', 'suave', 'aveludado', 'aveludada',
    'áspero', 'áspera', 'rugoso', 'rugosa', 'liso', 'lisa',
    'sedoso', 'sedosa', 'cremoso', 'cremosa', 'líquido', 'líquida',
    'pastoso', 'pastosa', 'sólido', 'sólida', 'gelatinoso', 'gelatinosa',
    
    # Cores e aparência
    'cor', 'colorido', 'colorida', 'vibrante', 'vivo', 'viva',
    'opaco', 'opaca', 'brilhante', 'fosco', 'fosca', 'translúcido',
    'transparente', 'escuro', 'escura', 'claro', 'clara', 'desbotado',
    'desbotada', 'manchado', 'manchada', 'uniforme', 'desigual',
    
    # ========== DURABILIDADE E RESISTÊNCIA ==========
    'durável', 'resistente', 'forte', 'robusto', 'robusta', 'sólido', 'sólida',
    'frágil', 'delicado', 'delicada', 'quebradiço', 'quebradiça',
    'queima', 'derrete', 'descasca', 'desbota', 'mancha', 'suja',
    'durabilidade', 'resistência', 'fragilidade',
    
    # ========== ACABAMENTO E DETALHES ==========
    'acabamento', 'detalhe', 'detalhes', 'costura', 'costurado', 'costurada',
    'emenda', 'junção', 'encaixe', 'alinhado', 'alinhada', 'torto', 'torta',
    'desalinhado', 'desalinhada', 'mal feito', 'mal feita', 'bem feito', 'bem feita',
    'caprichado', 'caprichada', 'desleixado', 'desleixada', 'refinado', 'refinada',
    
    # ========== ORIGINALIDADE E AUTENTICIDADE ==========
    'original', 'autêntico', 'autêntica', 'legítimo', 'legítima', 'genuíno', 'genuína',
    'falsificado', 'falsificada', 'pirata', 'réplica', 'cópia', 'imitação',
    'importado', 'importada', 'nacional', 'brasileiro', 'brasileira',
    
    # ========== EMBALAGEM ==========
    'embalagem', 'embalado', 'embalada', 'caixa', 'pacote', 'envelope',
    'lacrado', 'lacrada', 'selado', 'selada', 'violado', 'violada',
    'proteção', 'protegido', 'protegida', 'amassado', 'amassada',
    'danificado', 'danificada', 'intacto', 'intacta', 'perfeito', 'perfeita',
    'plástico bolha', 'papel kraft', 'isopor', 'papelão', 'caixinha',
    'saquinho', 'saco', 'envelope bolha', 'bem embalado', 'mal embalado',
    
    # ========== FUNCIONALIDADE ==========
    'funciona', 'funcionando', 'funcionou', 'função', 'defeito', 'defeituoso',
    'defeituosa', 'quebrado', 'quebrada', 'estragado', 'estragada', 'avariado',
    'avariada', 'completo', 'completa', 'incompleto', 'incompleta',
    'faltando', 'falta', 'manual', 'instruções', 'acessório', 'acessórios',
    'componente', 'componentes', 'parte', 'partes', 'peça', 'peças',
}

# =============================================================================
# TÓPICO 2: CARACTERÍSTICAS ESPECÍFICAS - COSMÉTICOS E BELEZA
# =============================================================================

TOPIC_COSMETICOS_BELEZA = {
    # ========== OLFATO ==========
    'cheiro', 'cheiroso', 'cheirosa', 'aroma', 'aromático', 'aromática',
    'fragrância', 'perfume', 'perfumado', 'perfumada', 'essência',
    'fedido', 'fedida', 'fedor', 'fétido', 'fétida', 'enjoativo', 'enjoativa',
    'suave', 'forte', 'intenso', 'intensa', 'marcante', 'discreto', 'discreta',
    'natural', 'artificial', 'químico', 'química', 'sintético', 'sintética',
    
    # ========== TEXTURA E APLICAÇÃO ==========
    'textura', 'consistência', 'cremoso', 'cremosa', 'líquido', 'líquida',
    'gel', 'mousse', 'espuma', 'oleoso', 'oleosa', 'gorduroso', 'gordurosa',
    'seco', 'seca', 'ressecado', 'ressecada', 'pegajoso', 'pegajosa',
    'grudento', 'grudenta', 'leve', 'pesado', 'pesada', 'denso', 'densa',
    'espalhável', 'absorve', 'absorveu', 'absorção', 'penetra', 'penetração',
    'espalha', 'espalhou', 'aplicação', 'aplica', 'aplicou', 'rende',
    'rendimento', 'efeito', 'resultado', 'desempenho',
    
    # ========== HIDRATAÇÃO E NUTRIÇÃO ==========
    'hidratante', 'hidrata', 'hidratou', 'hidratação', 'nutrição', 'nutre',
    'nutritivo', 'nutritiva', 'umectante', 'emoliente', 'ressecou',
    'resseca', 'seca', 'úmido', 'úmida', 'molhado', 'molhada',
    
    # ========== EFEITOS NA PELE ==========
    'pele', 'cutâneo', 'cutânea', 'derme', 'epiderme',
    'macia', 'sedosa', 'aveludada', 'lisa', 'áspera', 'rugosa',
    'oleosa', 'seca', 'mista', 'normal', 'sensível', 'acneica',
    'irritada', 'irritação', 'vermelhidão', 'coceira', 'ardor', 'queimação',
    'alergia', 'alérgica', 'reação', 'manchou', 'mancha', 'clareou', 'clareia',
    'uniformiza', 'uniformizou', 'ilumina', 'iluminou', 'luminosidade',
    
    # ========== EFEITOS NO CABELO ==========
    'cabelo', 'capilar', 'fio', 'fios', 'raiz', 'pontas', 'couro cabeludo',
    'liso', 'cacheado', 'crespo', 'ondulado', 'encaracolado',
    'macio', 'sedoso', 'brilhoso', 'brilho', 'opaco', 'fosco',
    'hidratado', 'ressecado', 'danificado', 'quebradiço', 'elástico',
    'volume', 'volumoso', 'murcho', 'liso', 'alisa', 'alisou',
    'define', 'definição', 'modela', 'modelou', 'controla', 'controlou',
    'frizz', 'arrepiado', 'embaraçado', 'emaranhado', 'desembaraça',
    'desembaraçou', 'penteia', 'penteou', 'escova', 'escovou',
    
    # ========== EFEITOS NAS UNHAS ==========
    'unha', 'unhas', 'esmalte', 'base', 'top coat', 'extra brilho',
    'secante', 'removedor', 'acetona', 'fortalecedor', 'endurecedor',
    'cobertura', 'pigmentação', 'opaco', 'transparente', 'translúcido',
    'brilhante', 'metálico', 'perolado', 'glitter', 'cintilante',
    'cremoso', 'chip', 'lasca', 'lascou', 'descasca', 'descascou',
    'duração', 'dura', 'durou', 'secou', 'seca', 'demora', 'demorou',
    
    # ========== COMPOSIÇÃO E INGREDIENTES ==========
    'vegano', 'vegana', 'cruelty free', 'testado', 'testada', 'dermatológico',
    'dermatológica', 'hipoalergênico', 'hipoalergênica', 'sensível',
    'natural', 'orgânico', 'orgânica', 'químico', 'química', 'parabenos',
    'sulfato', 'silicone', 'álcool', 'fragrância', 'corante', 'conservante',
    'ingrediente', 'ingredientes', 'componente', 'fórmula', 'composição',
    'ativo', 'ativa', 'princípio ativo', 'extrato', 'óleo', 'essência',
    
    # ========== SEGURANÇA E REGULAÇÃO ==========
    'anvisa', 'registro', 'lote', 'validade', 'vencido', 'vencida',
    'aprovado', 'aprovada', 'certificado', 'certificada', 'selo',
    'regulamentado', 'regulamentada', 'seguro', 'segura',
}

# =============================================================================
# TÓPICO 3: LOGÍSTICA E ENTREGA
# =============================================================================

TOPIC_LOGISTICA_ENTREGA = {
    # ========== PRAZO ==========
    # Velocidade positiva
    'entrega', 'entregar', 'entregue', 'rápido', 'rápida', 'rapidez',
    'veloz', 'ágil', 'agilidade', 'expresso', 'expressa', 'urgente',
    'instantâneo', 'instantânea', 'imediato', 'imediata', 'super rápido',
    'super rápida', 'rapidíssimo', 'rapidíssima', 'flash',
    
    # Pontualidade
    'prazo', 'pontual', 'pontualidade', 'dentro', 'antecipado', 'antecipada',
    'antes', 'adiantado', 'adiantada', 'previsto', 'prevista', 'esperado',
    'esperada', 'estimado', 'estimada', 'prometido', 'prometida',
    
    # Lentidão e atraso
    'demorou', 'demora', 'demorado', 'demorada', 'lento', 'lenta', 'lentidão',
    'atrasado', 'atrasada', 'atraso', 'fora', 'extrapolou', 'excedeu',
    'depois', 'tardou', 'tardio', 'tardia', 'aguardando', 'espera', 'esperando',
    
    # ========== RASTREAMENTO ==========
    'rastreio', 'rastreamento', 'rastrear', 'código', 'tracking', 'track',
    'atualizado', 'atualizada', 'atualização', 'sem informação', 'sem atualização',
    'parado', 'parada', 'estacionado', 'estacionada', 'status', 'situação',
    'movimentação', 'movimentado', 'movimentada', 'objeto', 'encomenda',
    
    # ========== TRANSPORTADORA ==========
    # Correios
    'correios', 'sedex', 'pac', 'carta', 'encomenda normal', 'mini envios',
    'agência', 'unidade', 'carteiro',
    
    # Transportadoras privadas
    'transportadora', 'jadlog', 'loggi', 'azul cargo', 'total express',
    'braspress', 'tnt', 'fedex', 'dhl', 'ups', 'sequoia', 'mandae',
    'kangu', 'melhor envio', 'intelipost', 'flash', 'rappi', 'lalamove',
    
    # Entregadores
    'motoboy', 'entregador', 'entregadora', 'motorista', 'despachante',
    
    # ========== FRETE ==========
    'frete', 'envio', 'postagem', 'despacho', 'postado', 'postada',
    'despachado', 'despachada', 'expedido', 'expedida',
    'grátis', 'gratuito', 'gratuita', 'free', 'sem custo', 'cortesia',
    'caro', 'cara', 'barato', 'barata', 'custo', 'valor', 'taxa',
    'cobrado', 'cobrada', 'pago', 'paga',
    
    # ========== ENDEREÇO E ENTREGA ==========
    # Local de entrega
    'endereço', 'cep', 'casa', 'apartamento', 'apto', 'trabalho',
    'escritório', 'prédio', 'condomínio', 'portaria', 'porteiro',
    'recepção', 'vizinho', 'vizinha', 'familiar', 'responsável',
    
    # Problemas de endereço
    'errado', 'errada', 'incorreto', 'incorreta', 'incompleto', 'incompleta',
    'faltando', 'falta', 'complemento', 'número', 'rua', 'avenida',
    'bairro', 'cidade', 'estado', 'referência',
    
    # Tentativas de entrega
    'tentativa', 'tentou', 'ausente', 'não estava', 'ninguém', 'retirar',
    'retirada', 'aviso', 'agendamento', 'reagendar', 'remarcar',
    
    # ========== ESTADO DA ENCOMENDA ==========
    # Condição física
    'amassado', 'amassada', 'danificado', 'danificada', 'violado', 'violada',
    'aberto', 'aberta', 'rasgado', 'rasgada', 'molhado', 'molhada',
    'quebrado', 'quebrada', 'intacto', 'intacta', 'perfeito', 'perfeita',
    
    # Embalagem de transporte
    'caixa', 'papelão', 'envelope', 'saco', 'plástico bolha', 'proteção',
    'bem embalado', 'mal embalado', 'sem proteção', 'frágil',
    
    # ========== PROBLEMAS GRAVES ==========
    'extraviado', 'extraviada', 'perdido', 'perdida', 'sumiu', 'desapareceu',
    'roubado', 'roubada', 'furtado', 'furtada', 'não chegou', 'não recebi',
    'devolver', 'devolução', 'reenvio', 'reenviar',
}

# =============================================================================
# TÓPICO 4: ATENDIMENTO AO CLIENTE
# =============================================================================

TOPIC_ATENDIMENTO_CLIENTE = {
    # ========== QUALIDADE DO ATENDIMENTO ==========
    # Positivo
    'atendimento', 'atendente', 'atendeu', 'suporte', 'sac', 'educado', 'educada',
    'gentil', 'atencioso', 'atenciosa', 'prestativo', 'prestativa', 'solicito',
    'solícita', 'cordial', 'simpático', 'simpática', 'amável', 'paciente',
    'profissional', 'competente', 'eficiente', 'excelente', 'ótimo', 'ótima',
    'maravilhoso', 'maravilhosa', 'perfeito', 'perfeita',
    
    # Negativo
    'grosseiro', 'grosseira', 'mal educado', 'mal educada', 'rude', 'ríspido',
    'ríspida', 'ignorou', 'despreparado', 'despreparada', 'incompetente',
    'amador', 'péssimo', 'péssima', 'horrível', 'terrível', 'desatencioso',
    'desatenciosa', 'impaciente', 'arrogante', 'debochado', 'debochada',
    
    # ========== CANAIS DE COMUNICAÇÃO ==========
    # Mensageria
    'whatsapp', 'zap', 'wpp', 'telegram', 'mensagem', 'msg', 'direct',
    'dm', 'inbox', 'chat', 'chatbot', 'bot', 'automático', 'automática',
    
    # Telefone
    'telefone', 'ligação', 'ligou', 'ligar', 'telefonema', 'atender',
    'atendeu', 'chamada', 'linha', 'ocupado', 'ocupada', 'mudo', 'muda',
    'caixa postal', 'gravação', 'ura', 'ramal',
    
    # Digital
    'email', 'e-mail', 'mensagem', 'site', 'plataforma', 'sistema',
    'formulário', 'ticket', 'protocolo', 'chamado', 'abertura',
    
    # Redes sociais
    'instagram', 'insta', 'facebook', 'face', 'twitter', 'tiktok',
    'comentário', 'post', 'story', 'stories', 'publicação',
    
    # ========== TEMPO DE RESPOSTA ==========
    # Rápido
    'rápido', 'rápida', 'rapidez', 'instantâneo', 'instantânea', 'imediato',
    'imediata', 'ágil', 'agilidade', 'pronto', 'prontidão', 'veloz',
    'respondeu', 'resposta', 'retorno', 'retornou',
    
    # Lento
    'demorou', 'demora', 'demorado', 'demorada', 'lento', 'lenta',
    'atrasado', 'atrasada', 'atraso', 'esperando', 'aguardando',
    'sem resposta', 'não respondeu', 'ignorou', 'deixou', 'largou',
    
    # Unidades de tempo
    'minutos', 'minuto', 'horas', 'hora', 'dias', 'dia', 'úteis',
    'segundos', 'segundo', 'imediato', 'imediata',
    
    # ========== PÓS-VENDA ==========
    # Devolução e troca
    'troca', 'trocar', 'trocou', 'devolução', 'devolver', 'devolveu',
    'reenvio', 'reenviar', 'reenviou', 'substituição', 'substituir',
    'substituiu', 'arrependimento', 'desistência', 'desistir', 'desisti',
    
    # Financeiro
    'reembolso', 'reembolsar', 'reembolsou', 'estorno', 'estornar', 'estornou',
    'ressarcimento', 'ressarcir', 'ressarciu', 'crédito', 'devolver dinheiro',
    'dinheiro de volta', 'cancelamento', 'cancelar', 'cancelou',
    
    # Documentação
    'nota fiscal', 'nf', 'danfe', 'xml', 'cupom', 'voucher', 'comprovante',
    'recibo', 'garantia', 'certificado', 'termo', 'política',
    
    # ========== RESOLUÇÃO DE PROBLEMAS ==========
    # Solução
    'resolveu', 'resolvido', 'resolvida', 'solucionou', 'solucionado',
    'solucionada', 'solução', 'resolver', 'solucionar', 'consertou',
    'conserto', 'arrumou', 'arrumado', 'arrumada', 'correção', 'corrigiu',
    'corrigido', 'corrigida', 'ajustou', 'ajuste', 'fix', 'fixou',
    
    # Problema
    'problema', 'defeito', 'erro', 'falha', 'bug', 'transtorno',
    'inconveniente', 'dificuldade', 'complicação', 'empecilho',
    'obstáculo', 'impedimento',
    
    # Reclamação
    'reclamação', 'reclamar', 'reclamei', 'queixa', 'queixar', 'queixei',
    'insatisfação', 'insatisfeito', 'insatisfeita', 'descontente',
    'descontentamento', 'frustrado', 'frustrada', 'frustração',
    
    # Escalação
    'gerente', 'supervisor', 'supervisora', 'coordenador', 'coordenadora',
    'responsável', 'ouvidoria', 'ombudsman', 'procon', 'reclame aqui',
    'consumidor.gov', 'juizado', 'processo', 'ação', 'justiça',
    
    # ========== PENDÊNCIAS E FOLLOW-UP ==========
    'pendência', 'pendente', 'aguardando', 'esperando', 'follow up',
    'acompanhamento', 'acompanhar', 'acompanhei', 'andamento',
    'status', 'situação', 'posição', 'prazo', 'deadline',
    'previsão', 'estimativa',
}

# =============================================================================
# TÓPICO 5: PREÇO E CUSTO-BENEFÍCIO
# =============================================================================

TOPIC_PRECO_VALOR = {
    # ========== VALOR NOMINAL ==========
    # Termos gerais
    'preço', 'valor', 'custo', 'quantia', 'montante', 'total',
    'reais', 'reais', 'centavos', 'dinheiro', 'grana', 'gasto',
    
    # Avaliação subjetiva
    'caro', 'cara', 'carinho', 'carinha', 'caríssimo', 'caríssima',
    'barato', 'barata', 'baratinho', 'baratinha', 'baratíssimo', 'baratíssima',
    'acessível', 'inacessível', 'salgado', 'salgada', 'pesado', 'pesada',
    'justo', 'justa', 'injusto', 'injusta', 'razoável', 'irrazoável',
    'abusivo', 'abusiva', 'exorbitante', 'exagerado', 'exagerada',
    
    # ========== CUSTO-BENEFÍCIO ==========
    'benefício', 'custo-benefício', 'relação', 'vale', 'compensa', 'compensou',
    'investimento', 'vale a pena', 'não vale', 'valeu', 'não valeu',
    'vantagem', 'vantajoso', 'vantajosa', 'desvantagem', 'desvantajoso',
    'economia', 'economizar', 'economizou', 'econômico', 'econômica',
    'desperdício', 'perder dinheiro', 'jogar fora', 'prejuízo',
    
    # ========== PROMOÇÕES E DESCONTOS ==========
    # Promoções
    'desconto', 'descontão', 'descontaço', 'promoção', 'promocional',
    'oferta', 'ofertão', 'super oferta', 'queima', 'liquidação',
    'black friday', 'cyber monday', 'outlet', 'saldão', 'sale',
    'cupom', 'voucher', 'código', 'cashback', 'cash back',
    
    # Percentuais
    'porcentagem', 'por cento', '%', 'desconto de', 'off',
    '10%', '20%', '30%', '40%', '50%', '60%', '70%', '80%', '90%',
    
    # ========== FORMAS DE PAGAMENTO ==========
    # Métodos
    'pagamento', 'pagar', 'paguei', 'pago',

# Parcelamento
'parcela', 'parcelas', 'parcelado', 'parcelada', 'parcelar',
'prestação', 'prestações', 'entrada', 'sinal',
'sem juros', 'com juros', 'juros', 'juro', 'taxa',
'1x', '2x', '3x', '4x', '5x', '6x', '10x', '12x', '18x', '24x',

# Formas
'à vista', 'vista', 'boleto', 'pix', 'transferência', 'débito',
'crédito', 'cartão', 'carteira digital', 'paypal', 'mercado pago',
'picpay', 'ame', 'nubank', 'inter', 'c6', 'neon',

# ========== COMPARAÇÃO DE MERCADO ==========
# Concorrência
'concorrente', 'concorrência', 'mercado', 'similar', 'parecido', 'parecida',
'mesmo', 'mesma', 'igual', 'diferença', 'diferente', 'alternativa',
'outra loja', 'outro site', 'shopee', 'mercado livre', 'amazon',
'magalu', 'americanas', 'submarino', 'aliexpress', 'shein',

# Comparativos
'melhor', 'pior', 'mais caro', 'mais barato', 'mais em conta',
'vantagem', 'desvantagem', 'compensa', 'não compensa',

# ========== PERCEPÇÃO DE QUALIDADE × PREÇO ==========
# Positivo (vale o preço)
'vale cada centavo', 'vale o preço', 'justifica', 'compensa', 'investimento',
'premium', 'luxo', 'qualidade', 'diferenciado', 'diferenciada',

# Negativo (não vale)
'não vale', 'enganação', 'decepção', 'esperava mais', 'mais do mesmo',
'chinês', 'falsificado', 'ordinário', 'ordinária', 'vagabundo', 'vagabunda',

# ========== EXPECTATIVA × REALIDADE ==========
'esperava', 'esperado', 'esperada', 'expectativa', 'achei que',
'pensei que', 'imaginei', 'prometeu', 'prometido', 'prometida',
'propaganda', 'marketing', 'anúncio', 'anunciado', 'anunciada',
'fotos', 'imagem', 'descrição', 'conforme', 'diferente',

=============================================================================
TÓPICO 6: EXPERIÊNCIA DE COMPRA
=============================================================================
TOPIC_EXPERIENCIA_COMPRA = {
# ========== NAVEGAÇÃO E USABILIDADE ==========
# Site/App
'site', 'website', 'plataforma', 'sistema', 'página', 'aplicativo',
'app', 'mobile', 'celular', 'computador', 'desktop',

# Usabilidade
'fácil', 'facilidade', 'simples', 'intuitivo', 'intuitiva',
'prático', 'prática', 'funcional', 'rápido', 'rápida',
'difícil', 'dificuldade', 'complicado', 'complicada', 'confuso', 'confusa',
'travou', 'trava', 'lento', 'lenta', 'bug', 'erro', 'problema',

# Busca e navegação
'busca', 'buscar', 'procurar', 'encontrar', 'encontrei', 'achei',
'categoria', 'filtro', 'filtrar', 'ordenar', 'classificar',
'menu', 'página', 'seção', 'aba',

# ========== INFORMAÇÕES DO PRODUTO ==========
# Descrição
'descrição', 'informação', 'informações', 'detalhes', 'especificações',
'ficha técnica', 'características', 'dados', 'manual',
'completo', 'completa', 'incompleto', 'incompleta', 'claro', 'clara',
'confuso', 'confusa', 'faltando', 'falta', 'omitiu', 'omissão',

# Imagens e vídeos
'foto', 'fotos', 'imagem', 'imagens', 'figura', 'ilustração',
'vídeo', 'vídeos', 'animação', 'gif', 'zoom',
'nítido', 'nítida', 'borrado', 'borrada', 'desfocado', 'desfocada',
'escuro', 'escura', 'claro', 'clara', 'ângulo', 'detalhe',

# Avaliações de outros clientes
'avaliação', 'avaliações', 'review', 'reviews', 'comentário', 'comentários',
'nota', 'estrela', 'estrelas', 'feedback', 'opinião', 'opiniões',

# ========== PROCESSO DE COMPRA ==========
# Carrinho
'carrinho', 'sacola', 'cesta', 'adicionar', 'adicionou', 'remover',
'removeu', 'selecionar', 'selecionou', 'escolher', 'escolheu',

# Checkout
'checkout', 'finalizar', 'finalizou', 'concluir', 'concluiu',
'pagar', 'pagamento', 'confirmação', 'confirmar', 'confirmou',

# Problemas no processo
'travou', 'trava', 'erro', 'bug', 'não funcionou', 'não foi',
'não consegui', 'impediu', 'bloqueou', 'falhou', 'falha',

# ========== FIDELIDADE E RECOMPRA ==========
# Primeira impressão
'primeira vez', 'primeiro pedido', 'primeira compra', 'conheci',
'descobri', 'experimentei', 'teste', 'testei',

# Recompra
'compro sempre', 'sempre compro', 'já comprei', 'voltei', 'volto',
'cliente fiel', 'cliente antigo', 'habitual', 'recorrente',
'recompra', 'repetir', 'repito', 'segunda vez', 'terceira vez',

# Indicação
'recomendo', 'indico', 'indicação', 'recomendação', 'compartilhei',
'compartilhar', 'contei', 'falei bem', 'elogio', 'elogiei',
'não recomendo', 'não indico', 'não comprem', 'fujam', 'evitem',

# ========== EMOCIONAL ==========
# Positivo
'amei', 'adorei', 'apaixonei', 'feliz', 'felicidade', 'alegre',
'alegria', 'satisfeito', 'satisfeita', 'satisfação', 'contente',
'encantado', 'encantada', 'surpreso', 'surpresa', 'surpreendeu',
'superou', 'excedeu', 'melhor que esperava',

# Negativo
'decepção', 'decepcionado', 'decepcionada', 'frustrado', 'frustrada',
'frustração', 'insatisfeito', 'insatisfeita', 'insatisfação',
'arrependido', 'arrependida', 'arrependimento', 'chateado', 'chateada',
'nervoso', 'nervosa', 'irritado', 'irritada', 'raiva', 'ódio',

=============================================================================
TÓPICO 7: PROBLEMAS E DEFEITOS ESPECÍFICOS
=============================================================================
TOPIC_PROBLEMAS_DEFEITOS = {
# ========== PROBLEMAS FÍSICOS ==========
# Quebra e dano
'quebrado', 'quebrada', 'quebrou', 'quebrar', 'rachado', 'rachada',
'rachou', 'rachar', 'trincado', 'trincada', 'trincou', 'trincar',
'amassado', 'amassada', 'amassou', 'amassar', 'danificado', 'danificada',
'danificou', 'danificar', 'avariado', 'avariada', 'avariou', 'avariar',

# Vazamento e derramamento
'vazou', 'vazar', 'vazamento', 'derramou', 'derramar', 'derramado',
'derramada', 'escorreu', 'escorrer', 'molhou', 'molhado', 'molhada',

# Deformação
'torto', 'torta', 'entortou', 'entortar', 'deformado', 'deformada',
'deformou', 'deformar', 'amoleceu', 'amolecer', 'derreteu', 'derreter',

# Degradação
'descascou', 'descascar', 'descascado', 'descascada', 'desbotou',
'desbotar', 'desbotado', 'desbotada', 'manchou', 'manchar',
'manchado', 'manchada', 'enferrujou', 'enferrujar', 'ferrugem',
'oxidou', 'oxidar', 'oxidado', 'oxidada', 'mofo', 'mofado', 'mofada',

# ========== PROBLEMAS FUNCIONAIS ==========
# Não funciona
'não funciona', 'não funcionou', 'defeito', 'defeituoso', 'defeituosa',
'estragado', 'estragada', 'estragou', 'estragar', 'pifou', 'pifar',
'morreu', 'morrer', 'parou', 'parar',

# Falhas intermitentes
'às vezes funciona', 'funciona mal', 'falha', 'falhando', 'instável',
'trava', 'travou', 'congelou', 'congelar', 'reinicia', 'reiniciou',

# Incompatibilidade
'incompatível', 'não encaixa', 'não serve', 'errado', 'errada',
'tamanho errado', 'modelo errado', 'não é o certo',

# ========== PROBLEMAS COM CONTEÚDO ==========
# Faltando partes
'incompleto', 'incompleta', 'faltando', 'falta', 'faltou',
'sem', 'sem o', 'sem a', 'cadê', 'onde está', 'sumiu',
'acessório', 'peça', 'parte', 'componente', 'manual', 'instruções',

# Item errado
'errado', 'errada', 'trocado', 'trocada', 'diferente', 'outro',
'outra', 'não é o que pedi', 'não é o que comprei', 'confundiram',

# ========== PROBLEMAS DE SEGURANÇA ==========
# Alergias e reações
'alergia', 'alérgica', 'alérgico', 'reação', 'reagiu', 'irritação',
'irritou', 'ardeu', 'ardor', 'queimou', 'queimação', 'coceira',
'coçou', 'vermelhidão', 'vermelho', 'vermelha', 'mancha', 'manchou',
'inchado', 'inchada', 'inchaço', 'inchou', 'bolha', 'bolhas',

# Perigos
'perigoso', 'perigosa', 'perigo', 'risco', 'inseguro', 'insegura',
'tóxico', 'tóxica', 'nocivo', 'nociva', 'prejudicial', 'veneno',

# ========== PROBLEMAS DE AUTENTICIDADE ==========
'falsificado', 'falsificada', 'falso', 'falsa', 'pirata', 'réplica',
'cópia', 'imitação', 'fake', 'não é original', 'não é legítimo',
'chinês', 'importado', 'paralelo',}

=============================================================================
TÓPICO 8: ELOGIOS E PONTOS POSITIVOS
=============================================================================
TOPIC_ELOGIOS_POSITIVOS = {
# ========== ELOGIOS GERAIS ==========
'amei', 'adorei', 'apaixonei', 'gostei', 'curti', 'aprovei',
'recomendo', 'indico', 'super recomendo', 'super indico',
'excelente', 'ótimo', 'ótima', 'maravilhoso', 'maravilhosa',
'perfeito', 'perfeita', 'impecável', 'sensacional', 'incrível',
'fantástico', 'fantástica', 'espetacular', 'show', 'top',
'demais', 'nota 10', 'nota mil', '10/10', '1000/10',
# ========== SUPERAÇÃO DE EXPECTATIVAS ==========
'superou', 'superou expectativas', 'melhor que esperava', 'surpreendeu',
'surpreendente', 'inesperado', 'inesperada', 'além do esperado',
'excedeu', 'muito mais', 'muito melhor', 'acima da média',

# ========== SATISFAÇÃO ==========
'satisfeito', 'satisfeita', 'satisfação', 'feliz', 'felicidade',
'contente', 'alegre', 'radiante', 'realizado', 'realizada',

# ========== QUALIDADE ==========
'qualidade', 'boa qualidade', 'alta qualidade', 'qualidade superior',
'premium', 'luxo', 'sofisticado', 'sofisticada', 'refinado', 'refinada',

# ========== FIDELIZAÇÃO ==========
'voltarei', 'compro sempre', 'sempre compro', 'cliente fiel',
'favorita', 'favorito', 'preferida', 'preferido', 'minha loja',

# ========== GRATIDÃO ==========
'obrigado', 'obrigada', 'agradeço', 'gratidão', 'grata', 'grato',
'parabéns', 'meus parabéns', 'muito obrigado', 'muito obrigada',

=============================================================================
STOPWORDS COMPLETAS - PORTUGUÊS (FALLBACK EXPANDIDO)
=============================================================================
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
=============================================================================
DICIONÁRIO CONSOLIDADO PARA CLASSIFICAÇÃO AUTOMÁTICA
=============================================================================
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
=============================================================================
FUNÇÃO AUXILIAR: CLASSIFICAR REVIEW EM MÚLTIPLOS TÓPICOS
=============================================================================
def classify_review_topics(review_text, min_score=0.1):
"""
Classifica um review em múltiplos tópicos com scores ponderados.
Args:
    review_text (str): Texto do review
    min_score (float): Score mínimo para considerar o tópico (0-1)

Returns:
    dict: {nome_topico: score_normalizado}
"""
from collections import Counter
import re

# Tokeniza e limpa o texto
words = re.findall(r'\b\w+\b', review_text.lower())
words = [w for w in words if w not in CUSTOM_STOP_WORDS | PORTUGUESE_STOPWORDS_FALLBACK]

# Conta palavras por tópico
topic_scores = {}
for topic_name, keywords in TOPIC_CLASSIFICATION_MAP.items():
    matches = sum(1 for word in words if word in keywords)
    topic_scores[topic_name] = matches

# Normaliza scores (0-1)
total_matches = sum(topic_scores.values())
if total_matches > 0:
    topic_scores = {k: v/total_matches for k, v in topic_scores.items()}

# Filtra por score mínimo
topic_scores = {k: v for k, v in topic_scores.items() if v >= min_score}

# Ordena por relevância
topic_scores = dict(sorted(topic_scores.items(), key=lambda x: x[1], reverse=True))

return topic_scores
=============================================================================
EXEMPLO DE USO
=============================================================================
if name == "main":
# Exemplo de review
sample_review = """
Adorei o produto! A qualidade é excelente, muito macio e cheiroso.
A entrega foi super rápida, chegou antes do prazo. O atendimento
pelo WhatsApp foi ótimo, muito atenciosos. O preço está justo,
vale cada centavo. Recomendo!
"""
topics = classify_review_topics(sample_review)

print("Tópicos identificados:")
for topic, score in topics.items():
    print(f"  {topic}: {score:.2%}")

---

## 🎯 **MELHORIAS IMPLEMENTADAS:**

### **1. Expansão Massiva de Vocabulário:**
- **8 tópicos** completos (vs 5 originais)
- **~2.000 keywords** no total (vs ~200 originais)
- Cobertura de **variações morfológicas** (masculino/feminino, singular/plural)

### **2. Novos Tópicos Adicionados:**
- **Cosméticos e Beleza** (específico para seu nicho)
- **Experiência de Compra** (UX do site/app)
- **Problemas e Defeitos** (troubleshooting)
- **Elogios e Positivos** (sentimento positivo)

### **3. Stopwords Expandidas:**
- **300+ stopwords** portuguesas
- Incluindo contrações, pronomes, verbos auxiliares

### **4. Função de Classificação Automática:**
- Classifica review em **múltiplos tópicos** simultaneamente
- Score normalizado (0-1) por tópico
- Filtragem por threshold configurável

---

## 🚀 **PRÓXIMOS PASSOS:**

1. **Integrar ao pipeline LDA**
2. **Treinar modelo com novos tópicos**
3. **Validar com reviews reais**
4. **Ajustar pesos e thresholds**