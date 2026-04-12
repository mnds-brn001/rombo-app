# VTEX Connector (OMS + Logistics + Reviews) — Compatibilidade e Mapeamento

Este documento descreve como o `VTEXConnector` (implementado em `dados_cliente/sistema_conectores.py`) se encaixa no pipeline atual do Insight Expert e como os payloads da VTEX são mapeados para o formato “canônico” que o dashboard usa (especialmente `orders_enriched` / item-level).

## 1) Configuração (via `.env` ou `erp_config`)

O conector lê primeiro de `erp_config` e depois de variáveis de ambiente.

### Variáveis obrigatórias

- **`VTEX_ACCOUNT_NAME`**: nome da conta VTEX (ex.: `luxstore`)
- **`VTEX_APP_KEY`**: AppKey
- **`VTEX_APP_TOKEN`**: AppToken

### Variáveis recomendadas (com defaults)

- **`VTEX_ENVIRONMENT`**: ambiente/host prefix (default: `vtexcommercestable`)
- **`VTEX_DOMAIN`**: domínio (default: `com.br`)
- **`VTEX_BASE_URL`** (opcional): sobrescreve o host completo do OMS/Logistics (ex.: `https://{account}.{environment}.com.br`)
- **`VTEX_REVIEWS_BASE_URL`** (opcional): sobrescreve base do Reviews (default: `https://{account}.myvtex.com/reviews-and-ratings/api`)
- **`VTEX_ORDERS_PER_PAGE`** (default: `50`)
- **`VTEX_FALLBACK_MAX_PAGES`** (default: `10`) — proteção caso o endpoint não retorne `paging.pages`
- **`VTEX_THREADS`** (default: `6`) — apenas quando `parallel=True` em `get_orders`

### Para estoque (Logistics)

- **`VTEX_SKU_IDS`**: lista separada por vírgula com `skuId`s (ex.: `18,19,20`)
  - O `VTEXConnector.get_stock()` usa estes ids para chamar Logistics e gerar snapshot.

## 2) Quais endpoints o conector usa

### OMS (Orders)

- **List Orders**: `GET /api/oms/pvt/orders`
  - Tentativa de filtro: `f_creationDate=creationDate:[<start> TO <end>]`
  - Paginação: `page`, `per_page`
  - Se o filtro não for aceito (HTTP 400), o conector cai no fallback sem filtro e filtra por `creationDate/authorizedDate/lastChange` no cliente (com limite de páginas).

- **Order Detail**: `GET /api/oms/pvt/orders/{orderId}`
  - Expande o pedido em itens (`items[]`) e gera **1 linha por item**.

### Logistics (Inventory)

- **Inventory by SKU**: `GET /api/logistics/pvt/inventory/skus/{skuId}`
  - Retorna `balance[]` com quantidades por warehouse.

### Reviews & Ratings

- **Rating**: `GET /reviews-and-ratings/api/rating/{productId}`
- **List Reviews**: `GET /reviews-and-ratings/api/reviews?page=<n>&pageSize=<n>`

## 3) Saída esperada: item-level (compatível com `orders_enriched`)

O objetivo é que `VTEXConnector.get_orders(start_date, end_date)` devolva um DataFrame que se comporta como o do `MagazordConnector.get_orders(...)` para alimentar:

- ingestão de `orders_enriched` (1 linha por SKU/linha de pedido)
- agregações de volume por pedido (`sku_count`, `units_quantity`)
- funil e KPIs do painel

### Observação importante sobre valores

Na VTEX, valores geralmente vêm em **centavos**. O conector converte para BRL:

\[
\text{BRL} = \frac{\text{cents}}{100}
\]

## 4) Mapeamento campo-a-campo (VTEX → canônico)

### 4.1 List Orders (`/api/oms/pvt/orders`) → base do pedido

- **`order_id`** ← `list[].orderId`
- **`order_purchase_timestamp`** ← `list[].creationDate`
- **`marketplace_date`** ← `list[].creationDate`
- **`marketplace`** ← `list[].origin` (e `affiliateId` quando existir: `origin:affiliateId`)
- **`order_status`** ← `list[].statusDescription` (fallback: `list[].status`)
- **`order_status_code`** ← `list[].status`
- **`payment_type`** ← `list[].paymentNames`

### 4.2 Order Detail (`/api/oms/pvt/orders/{orderId}`) → enriquecimento e itens

#### Totais (repetidos por item para compatibilidade)

- **`valorProduto`** ← `totals[id="Items"].value / 100`
- **`valorFrete`** ← `totals[id="Shipping"].value / 100`
- **`valorDesconto`** ← `abs(totals[id="Discounts"].value) / 100`
- **`valorTotal`** ← `detail.value / 100` (fallback: `Items + Shipping + Tax - Discounts`)
- **`valorTotalFinal`** ← `valorTotal`

> Por design, esses totais vêm **iguais em todas as linhas (itens)** do mesmo pedido. Isso casa com o padrão do pipeline atual (que trabalha item-level, mas precisa de totals).

#### Cliente / endereço (best-effort)

- **`customer_unique_id`** ← `clientProfileData.email` (fallback: `clientProfileData.document`)
- **`customer_state`** ← `shippingData.address.state`
- **`customer_city`** ← `shippingData.address.city`

#### Entrega / transportadora (best-effort)

- **`carrier_name`** ← `shippingData.logisticsInfo[0].deliveryCompany`
- **`order_delivered_customer_date`** ← `packageAttachment.packages[0].courierStatus.deliveredDate` (quando existir)

#### Itens (`detail.items[]`)

Para cada item:

- **`product_qty`** ← `items[].quantity`
- **`product_id`** ← `items[].sellerSku` (fallback: `items[].id`)
- **`product_sku`** ← `items[].sellerSku` (fallback: `items[].id`)
- **`product_name`** ← `items[].name`
- **`price`** (unit líquido) ← `items[].sellingPrice / 100`
- **`price_gross`** (unit lista) ← `items[].listPrice / 100`
- **`total_item_value`** ← `price * product_qty`
- **`ean`** ← `items[].ean` (se vier preenchido)
- **`brand`** ← `items[].additionalInfo.brandName` (quando existir)
- **`category_name`** ← `None` (OMS não garante categoria; vem via Catalog API se/quando integrarmos)

Campos de frete/desconto por item:

- **`freight_value`** ← `0.0` (não rateamos frete por item neste template)
- **`discount_value`** ← `0.0` (não rateamos desconto por item neste template)

## 5) Estoque (Logistics) — pontos de atenção e decisão de modelagem

### Multi-warehouse

`/inventory/skus/{skuId}` pode retornar múltiplos `balance[]` (um por warehouse). O template retorna **uma linha por warehouse** com:

- `stock_level` = `(totalQuantity - reservedQuantity)` quando **não** for unlimited
- `reserved_quantity` = `reservedQuantity`
- `warehouseId`, `warehouseName`

### Unlimited quantity

Se `hasUnlimitedQuantity=true`, o template seta:

- `stock_level = None`
- `has_unlimited_quantity = true`

**Decisão recomendada para o dashboard** (alinhamento): tratar `unlimited` como “sem restrição” e evitar que valores fictícios inflacionem score de recomendação (ex.: usar `NULL`/flag e regras específicas).

## 6) Compatibilidade com o pipeline atual (Magazord → VTEX)

### O que é “plugável” agora

- **Orders item-level**: já sai no formato que o pipeline entende (campos principais e `product_qty`)
- **Valores**: `valorTotal`/`valorProduto`/`valorFrete`/`valorDesconto` já vêm convertidos para BRL
- **Status**: mantemos `status` e `statusDescription` para o funil inferir etapas

### O que tende a precisar de refinamento (normal e esperado)

- **Categoria / árvore de produto**: OMS não garante categoria; ideal integrar Catalog API para popular `category_name`, `product_category_name`
- **Rateio de frete/desconto por item**: opcional (se precisar margens por SKU)
- **Estoque full snapshot**: para catálogo inteiro, precisamos buscar SKU IDs via Catalog API (ou armazenar uma lista mestre local). O template começa com `VTEX_SKU_IDS`.

## 7) Exemplo rápido de uso (no seu código)

No `erp_config`:

```python
erp_type = "vtex"
erp_config = {
  "account_name": "luxstore",
  "environment": "vtexcommercestable",
  "domain": "com.br",
  "app_key": os.getenv("VTEX_APP_KEY"),
  "app_token": os.getenv("VTEX_APP_TOKEN"),
}
```

Depois:

```python
connector = setup_cliente_connector(erp_type="vtex", erp_config=erp_config)
df = connector.erp_connector.get_orders(start_date, end_date, parallel=True)
```

## 8) Checklist técnico (para “segurança” na entrega)

- Validar que `valorTotal` bate com relatórios financeiros da VTEX (centavos → BRL).
- Validar `product_qty` com casos de bundle/kit (quando `bundleItems[]` existir).
- Definir regra final para `unlimited stock` (não “explodir” recomendação).
- Se necessário, integrar Catalog API para categoria/marca/EAN mais completos.

