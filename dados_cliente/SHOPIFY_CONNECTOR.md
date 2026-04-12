# Shopify Connector (Admin API) — Orders + Inventory + Reviews

Este documento descreve como implementar um conector para Shopify que gere um `DataFrame` compatível com o pipeline do Insight Expert (especialmente `public.orders_enriched` com **1 linha por item/variant**), além de preparar a coleta de **estoque** e **reviews**.

Ele espelha o padrão do `VTEX_CONNECTOR.md`: foco em (1) endpoints, (2) mapeamento campo-a-campo, (3) decisões quando dados não existem ou vêm incompletos.

> Observacao de escopo: Shopify nao tem um “endpoint REST padrao de reviews de produto” para toda loja. Em geral, reviews de produto sao disponibilizadas via apps ou via metaobjects padrao (quando habilitados/permitidos).

---

## 1) Configuracao (Auth + variaveis)

### 1.1 Auth (Admin API)
Para o conector, use um dos fluxos abaixo:

1. **Private app / Access token** (comum em integrações):
   - Header REST e GraphQL: `X-Shopify-Access-Token: <access_token>`
2. **OAuth (App instalado na store)**:
   - mesmo header acima, usando o token de acesso emitido pelo OAuth.

### 1.2 Variaveis recomendadas
- `SHOPIFY_STORE_DOMAIN` (ex.: `minha-loja.myshopify.com`)
- `SHOPIFY_ADMIN_API_ACCESS_TOKEN`
- `SHOPIFY_API_VERSION` (ex.: `2025-01` ou `2024-10`)

### 1.3 Scopes (ajuste no seu app)
Vocês devem conferir os scopes exigidos para:
- Ler pedidos: `orders` / `read_orders`
- Ler inventario: `read_inventory`
- Ler produtos/variants (para mapear): `read_products`
- Ler metaobjects (reviews padrao via metaobjects): `read_metaobjects` (quando aplicavel)

> Para publicar em publico global com seguranca: depois de definir a primeira store “teste”, registre os erros 403/401 e valide quais scopes estao realmente necessarios.

---

## 2) Endpoints usados

### 2.1 Pedidos (Orders)

#### REST Admin API (Orders)
Base:
`https://{shop}/admin/api/{version}/`

Lista de pedidos (paginação + filtro por data):
- `GET /orders.json?status=any&created_at_min=YYYY-MM-DDT00:00:00Z&created_at_max=YYYY-MM-DDT23:59:59Z&limit=250`

Detalhe do pedido:
- `GET /orders/{order_id}.json`

> Observacao: dependendo do endpoint e versão, os objetos podem vir com `line_items` inline ou exigir campos adicionais. O conector deve assumir “best-effort” e normalizar sempre 1 linha por item.

#### GraphQL Admin API (Orders)
HTTP endpoint (unico):
- `POST /admin/api/{version}/graphql.json`

Operacoes principais:
- `orders(...)` (lista paginada)
- `order(id: ...)` (detalhe)

Na pratica, o GraphQL costuma ser melhor para buscar `lineItems` e totals com menor numero de chamadas, mas o REST e suficiente como MVP.

---

### 2.2 Estoque (Inventory)

No Shopify, o “estoque” em geral esta dividido entre:
- **InventoryItem** (o item/variant que tem estoque)
- **InventoryLevel** (quantidade por `location`)

#### REST Admin API (InventoryItem / InventoryLevel)
Base:
`https://{shop}/admin/api/{version}/`

InventoryItem:
- `GET /inventory_items.json?ids=...`
- `GET /inventory_items.json?limit=250&page=...` (se nao tiver ids)

Exemplo (REST): `inventory_items` por `ids`

```http
GET /admin/api/{version}/inventory_items.json?ids=808950810,39072856,457924702
```

Query parameters:
- `ids` (string, **≤ 100 ids**): lista separada por vírgula de IDs de `InventoryItem`
- `limit/page`: apenas quando nao usar `ids`

Payload (exemplo):
```json
{
  "inventory_items": [
    {
      "cost": "25.00",
      "country_code_of_origin": "FR",
      "created_at": "2012-08-24T14:01:47-04:00",
      "id": 450789469,
      "province_code_of_origin": "QC",
      "sku": "IPOD2008PINK",
      "tracked": true,
      "updated_at": "2012-08-24T14:01:47-04:00",
      "requires_shipping": true,
      "admin_graphql_api_id": "gid://shopify/InventoryItem/39072856"
    }
  ]
}
```

Como usar no seu conector:
- `InventoryItem.id` (REST id) → chave interna do item de inventário (útil para buscar `InventoryLevel`)
- `sku` → fallback para `product_sku` (quando você nao tiver `variant.sku` no contexto)
- `cost` → pode virar `product_cost`/`capital_imobilizado` em snapshots de estoque (best-effort)

InventoryLevel:
- `GET /inventory_levels.json?inventory_item_ids=...`
- `GET /inventory_levels.json?location_ids=...`

### 2.2.1 InventoryLevel (relacionamento e coleta)
Relacionamentos (modelo do Shopify):
- **Product Variant** -> 1:1 com **InventoryItem**
- **InventoryItem** -> 1:N com **InventoryLevel** (uma por **Location** onde o item está estocado)
- **InventoryLevel** -> 1:1 com (InventoryItem, Location) e representa a **quantidade available** para aquela localizacao.

Endpoint (REST) — lista de `inventory_levels`:
```http
GET /admin/api/{version}/inventory_levels.json?inventory_item_ids=...&location_ids=...
```

Parâmetros (conforme doc):
- `inventory_item_ids` (≤ 50): lista separada por vírgula de IDs de `InventoryItem`
- `location_ids` (≤ 50): lista separada por vírgula de IDs de `Location`
- `limit` (≤ 250, default 50)
- `updated_at_min` (opcional): filtra por `updated_at` (formato ISO-8601)

Exemplo de payload:
```json
{
  "inventory_levels": [
    {
      "inventory_item_id": 49148385,
      "location_id": 655441491,
      "available": 2,
      "updated_at": "2026-01-09T17:04:11-05:00",
      "admin_graphql_api_id": "gid://shopify/InventoryLevel/655441491?inventory_item_id=49148385"
    }
  ]
}
```

Como mapear para o seu snapshot:
- `available` -> `stock_level` (por location) no interim
- no “snapshot enriched” do seu sistema, o padrão costuma ser **agregar**:
  - `stock_level = SUM(available) por inventory_item_id`
  - depois mapear `inventory_item_id` -> `variant_id` (que no seu pipeline sera o `product_id`) e então agregar para `product_id` se necessário

#### GraphQL Admin API (InventoryItem / InventoryLevel)
Mesmo endpoint HTTP `/graphql.json`.

Operacoes relevantes:
- `inventoryItem(id: ID!)`
- `inventoryItems(first: Int, query: String, ...)`
- `inventoryLevels(...)` (frequentemente acessivel via `inventoryItem`/`inventoryLevels`)

> Para o pipeline: como vamos usar `variant_id` como `product_id`, o conector pode:
> 1) recuperar `InventoryLevel` por `inventory_item_id` e somar `available`
> 2) mapear `inventory_item_id` -> `variant_id` (product_id no pipeline) na etapa de snapshot

---

### 2.3 Reviews (Avaliacao de produtos)

## 2.3.1 Reviews via metaobjects padrao (GraphQL)
O caminho “mais universal” no Shopify atual e usar metaobjects do tipo **`product_review`** (quando disponivel na store).

Operacoes GraphQL:
- `standardMetaobjectDefinitionEnable(type: "product_review")` (quando necessário)
- `metaobjects(type: "product_review", first: N, query: "...")`
- `metaobject(id: ...)`
- `metaobjectByHandle(handle: ...)`

Ponto de atencao:
- os **fields** (chaves) dentro do metaobject dependem da standard definition e, em alguns casos, do que esta configurado/enviado pela integracao/app.
- para ficar “seguro” antes de publicar: voce deve confirmar os nomes de campos pelo GraphQL (ex.: `rating`, `title`, `body`, `product`, etc.) na store teste.

### 2.3.1.1 Captura dos dados de review (o que vem em `fields`)
No metaobject padrão do tipo `product_review`, os campos relevantes para o seu conector aparecem dentro de `fields` (cada campo tem `key` e `value`).

Os campos mais importantes (do guia do Shopify) incluem:
- `rating`: **objeto JSON** no formato `{"scale_min":"1.0","scale_max":"5.0","value":"5.0"}` (valor numérico vem em `rating.value`)
- `submitted_at`: data/hora em que a review foi enviada
- `source`: origem (ex.: `email`)
- `body`: texto da review
- `title`: título (opcional)
- referências: `product`, `product_variant`, `order` e `author` (quando disponíveis)
- `app_verification_status`: tipicamente `verified_buyer`, `verified_reviewer` ou `unverified` (requerido por implementação no programa padrão)
- status de publicação: `capabilities.publishable.status` (`ACTIVE` vs `DRAFT`) e `published_at`

Fonte: [Standard product review metaobject definition](https://shopify.dev/docs/apps/build/metaobjects/standard-review-metaobject)

### 2.3.1.2 Como mapear para o seu schema canônico de reviews
Seu `reviews_loader.py` unifica reviews para as colunas:
- `review_id`
- `review_date`
- `review_score`
- `review_comment_message`
- `product_id` (chave única do pipeline = variant_id)
- `product_name`
- `product_category_name`
- `review_source`

Mapeamento sugerido (best-effort):
- `review_id` → `metaobject.id` (ou `handle`)
- `review_date` → `fields["submitted_at"]` (ou `published_at` quando preferir “data pública”)
- `review_score` → parse do JSON em `fields["rating"]` e leitura de `rating.value`
- `review_comment_message` → `fields["body"]`
- `review_source` → `fields["source"]`
- `product_id` → resolver `fields["product_variant"]` para `variant_id`
- `product_sku` (opcional) → a `variant.sku` do `ProductVariant`
- `product_name` → por referência `fields["product"]` (normalmente precisa de query adicional em `Product(id)` pra obter title)
- `product_category_name` → derivar de `Product.productType`/collections (requer join/caching por produto)

Filtro recomendado:
- só incluir reviews com `capabilities.publishable.status = "ACTIVE"` pra evitar rascunhos

Exemplo (GraphQL) de leitura de metaobjects para `product_review`:
```graphql
query {
  metaobjects(
    type: "product_review"
    first: 50
  ) {
    edges {
      node {
        id
        handle
        capabilities {
          publishable { status }
        }
        updatedAt
        fields {
          key
          value
        }
      }
    }
  }
}
```
> Em produção você vai paginar (via cursor/`after`) e pode adicionar filtro por data/marketplace conforme sua estratégia.

## 2.3.2 Reviews via app (quando metaobjects padrao nao existe)
Se sua store usa um app de reviews, a fonte pode ser:
- banco do app (via API propria)
- export via webhook
- ou metaobjects criados pelo app (dependendo de como o app integra)

Nesse caso, o conector precisa “trocar a fonte” em vez de tentar um endpoint universal.

### 2.3.2.1 Variante: Judge.me (exemplo)
Quando a loja usa `judge.me`, as reviews podem vir dos endpoints do app, por exemplo:
- `GET /api/v1/reviews`
- `GET /api/v1/reviews/{id}`

Mapeamento (baseado no payload que você colou):
- `review_id` → `review.id`
- `review_date` → `review.created_at` (ou `updated_at` se você preferir “última alteração”)
- `review_score` → `review.rating`
- `review_comment_message` → `review.body`
- `review_source` → `review.source`
- `product_name` → `review.product_title` (ou fallback: `review.product_handle`)
- `product_id` → resolver no Shopify o `variant_id` (idealmente via SKU/variant quando o app expõe; se o app só traz produto, o join por SKU pode ficar incompleto no Nível 1)
- `product_sku` (opcional) → quando disponível no payload/app ou quando for possível inferir a partir da `variant.sku` no Shopify
- `product_category_name` → geralmente exige buscar/derivar via catálogo do Shopify (ex.: ProductType/collections)

---

## 3) Saida esperada: `orders_enriched` (1 linha por item/variant)

O objetivo e que `ShopifyConnector.get_orders(start_date, end_date)` retorne um `DataFrame` com colunas compatíveis com o subset usado em:
- `utils/KPIs.py` (`REQUIRED_COLUMNS`)

### 3.1 Colunas canônicas (minimas) que o pipeline espera
Para orders, o minimo e:
- `order_id`
- `customer_id`
- `customer_unique_id`
- `product_id`
- `order_purchase_timestamp`
- `order_delivered_customer_date` (best-effort)
- `marketplace_date`
- `product_category_name`
- `category_name` (alias comum; pode ser igual a `product_category_name`)
- `customer_state`
- `transportadoraNome`
- `marketplace`
- `price`
- `freight_value`
- `valorTotal`
- `order_status`
- `pedido_cancelado` (o loader cria via funil, mas e bom garantir consistente)

---

## 4) Mapeamento campo-a-campo (Shopify -> canônico)

> Nota: Shopify usa ids/handles em formato `gid://shopify/...` (GraphQL) e ids numericos em REST. Normalizamos para string no conector.

### 4.1 IDs
- `order_id` ← `Order.id` (REST) ou `Order.id` (GraphQL)
- `product_id` ← **`variant_id`** (linha) (isso alinha com inventario por variant)
- `product_sku` ← `variant.sku`
- `customer_id` ← `customer.id` quando existir (senão, pode ser email/phone como fallback)
- `customer_unique_id` ← `customer.id` (preferencia) ou `email` (fallback) ou `phone`/`guest_<hash>`

### 4.2 Datas
- `order_purchase_timestamp` ← `created_at` / `createdAt`
- `marketplace_date` ← mesma origem (geralmente `created_at`)
- `order_delivered_customer_date` ← best-effort:
  - se houver informacao de delivered no objeto de fulfillment (ex.: deliveredAt), use isso;
  - caso contrario, use `fulfilled_at` / data do fulfillment (ou deixe `NULL` e aceite que delivery-delay vira desconhecido)

### 4.3 Categorias
- `product_category_name` / `category_name`:
  - melhor: buscar categoria via `Product` / `ProductType` / collections (se voce definir a regra)
  - fallback MVP: deixar vazio e preencher depois (o motor ainda consegue classificar por performance e cancelamento, mas a visibilidade de categoria fica pior)

### 4.4 Geografia e transportadora
- `customer_state` ← `shipping_address.province_code` ou `shipping_address.state`
- `transportadoraNome` ← melhor estimativa:
  - `shipping_lines[].carrier_identifier` ou `shipping_lines[].title`
  - fallback: `null` se nao houver shipping_lines

### 4.5 Monetario (price, freight, valorTotal)
Para cada linha (line item / variant):
- `price` ← `line_item.price` (unitario)
- `product_qty` ← `line_item.quantity` (unidades)
- `freight_value`:
  - recomendacao: pegar frete total do pedido e alocar proporcionalmente ao valor dos itens (por linha) quando voce precisar de consistencia por SKU;
  - MVP: repetir `shipping_total` em todas as linhas (similar ao que alguns templates fazem) ou setar 0 se o seu motor ainda nao usa freight em detalhes.
- `valorTotal`:
  - recomendacao: usar total do pedido (ou recalcular: `subtotal + shipping - discounts`)
  - repetir por linha para manter compatibilidade com o pipeline item-level

### 4.6 Status / cancelamento
O seu funil (`_map_order_status_to_funnel`) procura tokens como:
- pending/aguardando
- paid/confirmado/aprovado
- shipped/transit
- delivered/entregue
- cancelado/cancel/refund

Entao defina `order_status` no conector como:
- se `cancelled_at` existir (ou `financial_status` = voided/refunded) → incluir token `cancelado`
- se `fulfillment_status` = fulfilled → incluir token `entregue` (ou `delivered`)
- se `financial_status` indica pago e nao entregue → incluir token `pago`
- caso contrario → incluir token `pendente`

Assim o pipeline consegue construir `funnel_*` e `pedido_cancelado`.

---

## 5) Logica recomendada para implementar o conector

### 5.1 Metodo `connect()`
- criar `requests.Session()`
- setar header `X-Shopify-Access-Token`
- opcional: testar um endpoint leve (ex.: buscar um pedido recente) para validar credenciais

### 5.2 Metodo `get_orders(start_date, end_date)`
1) Buscar lista de pedidos no intervalo:
   - REST: use `created_at_min/max` e paginação por `page` ou Link headers
   - GraphQL: use `orders(first:N, query:"created_at:>=... created_at:<...")`
2) Para cada pedido:
   - carregar detalhe (se a lista nao trouxer line items suficientes)
   - gerar 1 linha por `line_items[]`
3) Calcular:
   - `valorTotal` (por pedido)
   - repetir `valorTotal` por linha
   - `price` (por linha)
   - `freight_value` (alocado ou repetido)
   - datas e status normalizados
4) Retornar `DataFrame` com colunas canônicas

### 5.3 Metodo `get_stock()`
Para integrar com o motor de estoque, crie um snapshot especifico:
1) Determinar lista de variants (product_id) que representam o portifolio;
2) Buscar:
   - `inventory_items` e `inventory_levels` (por location)
3) Agregar no snapshot:
   - `stock_level` = soma de available quantities por location
   - (opcional) manter `warehouse_id/warehouse_name` por linha

> Observacao: o “snapshot enriched” atual do projeto e gerado por um pipeline Magazord especifico. Para Shopify, voce cria um coletor equivalente que gere o CSV/Parquet base no formato que o enrichment espera.

### 5.4 Metodo `get_reviews()` (opcional)
- Regra global do pipeline (evita o join silencioso quebrar):
  - `product_id` = `variant_id` (chave única do sistema)
  - `product_sku` = atributo auxiliar (texto “SKU”), nao a chave

Para o “motor” do Nível 1 funcionar, `get_reviews()` deve retornar (no mínimo):
  - `review_id`
  - `review_date` (ou campo equivalente usado como data)
  - `review_score` (1..5)
  - `review_comment_message`
  - `product_id` (variant_id)  <-- chave única para join com orders/estoque
  - `product_category_name` (categoria por produto)
  - `product_sku` (opcional, so para enriquecimento/diagnostico)
- Se usar metaobjects padrao (`product_review`):
  - consultar metaobjects do tipo `product_review`
  - filtrar por data (preferir `capabilities.publishable.status = "ACTIVE"`)
  - mapear campos:
    - `rating` → `review_score` (ler `rating.value`)
    - `body` → `review_comment_message`
    - `submitted_at` → `review_date`
    - `product_variant` → resolver `product_id` (variant_id) e preencher `product_sku` (sku) como auxiliar
    - `product` → resolver `product_category_name` (ex.: `productType` ou coleção definida na sua regra)
- Importante:
  - se sua ingest/tabelas “unificadas” usam `product_sku` como chave de join por legado, entao nesse caso faça `product_sku = variant_id` no momento de montar o dataset unificado; mas mantenha `product_sku` (SKU texto) como atributo separado (se a sua estrutura permitir).
- Se a store não expõe metaobjects padrão:
  - “trocar a fonte” para a API/webhook do app de reviews (ex.: `judge.me`)
  - fazer o enriquecimento: mapear `product_handle`/ids do app → `variant.sku` e categoria no Shopify
- Se usar app (ex.: Judge.me):
  - o payload precisa permitir resolver `product_id` (variant_id) no Shopify
  - se o app so traz `product_handle` (produto) e nao traz qual variant foi avaliada, voce vai ter que resolver por `SKU/variant` (quando houver no payload) ou aceitar que as reviews vao ficar “category-level” e o join por SKU pode ficar incompleto no Nível 1.
- Se nao houver reviews:
  - retorne DataFrame vazio; o score deve cair para o peso de cancellation e performance (o pipeline já trata defaults/0 para `avg_rating`).

---

## 6) Checklist para validacao antes de publicar (global)

1) **Pedidos**
   - 100 pedidos de teste
   - conferir que:
     - total do pedido bate com `valorTotal`
     - `order_id`, `product_id`, `price`, `product_qty` existem em todas as linhas
2) **Status -> Funil**
   - validar que pedidos cancelados produzem `pedido_cancelado = 1` no dataset final
3) **Inventario**
   - validar que a soma do available em locations bate com o estoque visto no Admin (best-effort)
4) **Reviews**
   - confirmar se `metaobjects(type:"product_review")` retorna dados na store teste
   - confirmar quais `fields.key/value` carregam rating e comentario

---

## 7) Lacunas que ainda precisam de confirmacao (por store)
- `order_delivered_customer_date`: qual campo chega com data de entrega (fulfillment vs tracking)
- `freight_value` e alocacao por item: se a sua loja tem shipping_lines consistente em todos pedidos
- reviews:
  - metaobject padrao `product_review` esta habilitado?
  - quais keys exatas existem dentro de `fields`?

Quando voce confirmar essas lacunas em 1 store real, voce consegue “fechar” o documento no nivel de seguranca para publicacao.

