## Черновики поставок: что есть и как дергать с фронта

### Эндпоинты и тела запросов/ответов
- `POST /api/ozon/drafts/create/`
  - Body: `{ "store_id": <int>, "supplyType": "CREATE_TYPE_CROSSDOCK", "destinationWarehouse": { "warehouse_id": <int>, "name": "..." }, "shipments": [ { "warehouse": "<cluster name>", "items": [ { "sku": <int>, "quantity": <int> }, ... ] }, ... ] }`
  - Resp 202: `{ "batch_id": "uuid", "batch_seq": <int>, "store_id": <int>, "drop_off_point_warehouse_id": <int>, "drafts": [ { "draft_id": <int>, "warehouse": "...", "cluster_id": <int>, "status": "queued" }, ... ] }`

- `GET /api/ozon/drafts/batch/<batch_id>/`
  - Resp 200: `{ "batch_id": "uuid", "batch_seq": <int>, "store": <int>, "status": "...", "drop_off_point_warehouse_id": <int>, "drafts": [ { "id": <int>, "logistic_cluster_id": <int>, "logistic_cluster_name": "...", "operation_id": "...", "draft_id": <int|null>, "supply_warehouse": [...], "selected_supply_warehouse": {...}, "selected_timeslot": {...}, "status": "...", "attempts": <int>, "next_attempt_at": "...", "error_message": "..." }, ... ] }`
  - В драфтах также есть `operation_id_supply` (после финального создания заявки) и `status: "created"` если заявка создана.

- `GET /api/ozon/drafts/batches/?store_id=<id>` (опц. store_id)
  - Resp 200: массив батчей в том же формате, каждый с `drafts`.
  - В этот список **не входят** батчи, у которых все черновики уже перешли в статус `created` (подтвержденные поставки).

- `GET /api/ozon/drafts/batches/confirmed/?store_id=<id>` (опц. store_id)
  - Батчи, где все черновики в статусе `created` (подтвержденные поставки).
  - Формат как у списка батчей, но в `drafts` будут только `created` черновики с полями `operation_id_supply`, `selected_timeslot`, `supply_order_ids`, `supply_bundle_items` и т.д.
  - Структура для фронта (ключевые поля):
    ```json
    [
      {
        "batch_id": "uuid",
        "batch_seq": 12,
        "store": 1,
        "status": "completed",
        "drop_off_point_warehouse_id": 21896333622000,
        "drop_off_point_name": "САНКТ-ПЕТЕРБУРГ_РФЦ_Кроссдокинг",
        "created_at": "ISO",
        "drafts": [
          {
            "id": 101,
            "logistic_cluster_name": "Краснодар",
            "operation_id_supply": "019b....",
            "draft_id": 78321044,
            "selected_supply_warehouse": {
              "warehouse_id": 1020001007805000,
              "name": "ВОРОНЕЖ_2_РФЦ",
              "address": "..."
            },
            "selected_timeslot": {
              "from_in_timezone": "2026-01-15T12:00:00Z",
              "to_in_timezone": "2026-01-15T13:00:00Z"
            },
            "supply_order_ids": [80537687],
            "supply_order_states": ["DATA_FILLING"],
            "supply_status_updated_at": "ISO",
            "supply_bundle_items": [
              {
                "sku": 1010410937,
                "quantity": 23,
                "offer_id": "DREAM WF4",
                "icon_path": "https://...",
                "name": "Прикуриватель...",
                "barcode": "2000000016948",
                "product_id": 534151104
              }
            ]
          }
        ]
      }
    ]
    ```
  - Если нужно подтянуть свежие данные по заказам/товарам, вызовите `GET /api/ozon/drafts/batch/<batch_id>/supply-info/?refresh=1`, после этого данные в `supply_*` обновятся.

- `POST /api/ozon/drafts/<draft_id>/select-warehouse/`
  - Body: `{ "warehouse_id": "<id из supply_warehouse>" }`
  - Resp 200: `{ "draft_id": <int>, "selected_supply_warehouse": {...} }`

- `DELETE /api/ozon/drafts/<draft_id>/`
  - Resp 200: `{ "deleted": true, "batch_deleted": <bool> }` (удаляет черновик; если батч пустой — удаляет и его).

- `POST /api/ozon/drafts/timeslots/fetch/`
  - Body: `{ "batch_id": "uuid", "date_from": "2025-12-19T00:00:00Z", "days": <int> }`
  - Resp: 200/207/400 `{ "results": [ { "draft_id": <int>, "timeslot_response": {...} }, ... ], "errors": [ { "draft_id": <int>, "error": "...", "status_code": <int?> }, ... ] }`
  - Внутри запроса есть небольшая задержка между вызовами к OZON и повтор при 429 (минимум 1-2 попытки).

- `GET /api/ozon/drafts/batch/<batch_id>/timeslots/`
  - Resp 200:
    ```json
    {
      "batch_id": "uuid",
      "drafts": [
        {
          "draft_id": 123,
          "timeslot_response": { /* raw OZON */ },
          "selected_supply_warehouse": { /* выбранный склад */ },
          "selected_timeslot": { /* если будет выбор */ },
          "timeslot_updated_at": "ISO",
          "timeslots_by_warehouse": [
            {
              "warehouse_id": 21896333622000,
              "warehouse_name": "САНКТ-ПЕТЕРБУРГ_РФЦ_Кроссдокинг",
              "warehouse_address": "...",
              "warehouse_timezone": "Europe/Moscow",
              "dates": [
                {
                  "date": "2025-12-16",
                  "timeslots": [
                    { "from": "2025-12-16T00:00:00Z", "to": "2025-12-16T01:00:00Z" }
                  ]
                }
              ]
            }
          ]
        }
      ],
      "common_dates": ["YYYY-MM-DD"],
      "common_timeslots": [
        {
          "date": "YYYY-MM-DD",
          "timeslots": [
            { "from": "2025-12-16T00:00:00Z", "to": "2025-12-16T01:00:00Z" }
          ]
        }
      ]
    }
    ```

- `POST /api/ozon/drafts/batch/<batch_id>/move-draft/`
  - Body: `{ "draft_id": <int> }`
  - Действие: создаёт новый батч и переносит туда выбранный черновик.
  - Resp 200: `{ "draft_id": <int>, "old_batch_id": "uuid", "new_batch_id": "uuid", "new_batch_seq": <int> }`

- `POST /api/ozon/drafts/batch/<batch_id>/confirm-supply/`
  - Body: `{ "timeslot": { "from_in_timezone": "2025-12-19T09:00:00Z", "to_in_timezone": "2025-12-19T10:00:00Z" } }`
  - Действие: **ставит черновики в очередь** на создание поставок. Фактические запросы к OZON выполняет планировщик в фоне с задержкой и повторами при 429.
  - Resp 200/207/400: `{ "results": [ { "draft_id": <int>, "status": "supply_queued" } ], "errors": [ { "draft_id": <int>, "error": "..." } ] }`
  - В черновик сохраняются: `selected_timeslot`, `status: "supply_queued"`. Дальше статус меняется фоном на `supply_in_progress`, `created` или `supply_failed`.

- `GET /api/ozon/drafts/batch/<batch_id>/supply-info/`
  - Query: `?refresh=1` (форсирует повторные запросы к OZON и обновление данных).
  - Берёт все черновики батча со статусом `created`, для каждого вызывает:
    1) `/v1/draft/supply/create/status` по `operation_id_supply` → `order_ids`
    2) `/v3/supply-order/get` по `order_ids`
    3) `/v1/supply-order/bundle` по `bundle_ids` из заказа, чтобы получить товары.
  - Resp 200/207/400:
    ```json
    {
      "results": [
        {
          "draft_id": 101,
          "order_ids": [80537687],
          "orders": [ /* raw ответ v3/supply-order/get */ ],
          "order_states": ["DATA_FILLING", "READY_TO_SUPPLY"],
          "bundle_items": [
            { "sku": 1010410937, "quantity": 23, "offer_id": "...", "icon_path": "...", "name": "...", "barcode": "...", "product_id": 534151104 }
          ],
          "cached": false,
          "supply_status_updated_at": "ISO"
        }
      ],
      "errors": [ { "draft_id": 101, "error": "...", "status_code": 400 } ]
    }
    ```
  - В черновике сохраняются `supply_order_ids`, `supply_order_response`, `supply_bundle_items` (их же можно брать из обычного списка батчей).
  - Если данные уже сохранены и `refresh` не указан, вернётся кэш (поле `cached: true`) без запросов к OZON.

### Что фронт получает для поставок
- В черновике (и в списках батчей/подтвержденных батчей):
  - `supply_order_ids` — идентификаторы заявок;
  - `supply_order_states` — статусы заявок (`DATA_FILLING`, `READY_TO_SUPPLY`, `ACCEPTED_AT_SUPPLY_WAREHOUSE`, `IN_TRANSIT`, `ACCEPTANCE_AT_STORAGE_WAREHOUSE`, `REPORTS_CONFIRMATION_AWAITING`, `REPORT_REJECTED`, `COMPLETED`, `REJECTED_AT_SUPPLY_WAREHOUSE`, `CANCELLED`, `OVERDUE`, `UNSPECIFIED`);
  - `supply_bundle_items` — товары в поставке: `sku`, `quantity`, `offer_id`, `icon_path`, `name`, `barcode`, `product_id`.
  - `supply_status_updated_at` — когда последний раз обновляли данные поставки.
- Через `GET /api/ozon/drafts/batch/<batch_id>/supply-info/` сразу приходят `order_states` и `bundle_items` (их же сохраняем в черновик).

### Формат запроса на создание (аналог `war_data.json`)
```http
POST /api/ozon/drafts/create/
Authorization: Bearer <JWT>
Content-Type: application/json

{
  "store_id": <ID магазина>,
  "supplyType": "CREATE_TYPE_CROSSDOCK",
  "destinationWarehouse": {
    "warehouse_id": 21896333622000,
    "name": "САНКТ-ПЕТЕРБУРГ_РФЦ_Кроссдокинг"
  },
  "shipments": [
    {
      "warehouse": "Краснодар",                // logistic_cluster_name из OzonWarehouseDirectory
      "items": [
        { "sku": 849086014, "quantity": 24 },
        { "sku": 849086015, "quantity": 0 }    // quantity <= 0 игнорируется
      ]
    },
    {
      "warehouse": "Москва, МО и Дальние регионы",
      "items": [
        { "sku": 123, "quantity": 10 }
      ]
    }
  ]
}
```
- Для каждого `shipment.warehouse` создается отдельный черновик (cluster_ids берутся из справочника по имени кластера, drop_off_point_warehouse_id — из `destinationWarehouse.warehouse_id`).
- Если кластер не найден или все количества ≤ 0 — черновик для такого `shipment` не создается.

### Ответ на создание (возвращается сразу)
```json
{
  "batch_id": "uuid",
  "batch_seq": 12,
  "store_id": 5,
  "drop_off_point_warehouse_id": 21896333622000,
  "drafts": [
    { "draft_id": 101, "warehouse": "Краснодар", "cluster_id": 200, "status": "queued" },
    { "draft_id": 102, "warehouse": "Москва, МО и Дальние регионы", "cluster_id": 154, "status": "queued" }
  ]
}
```
- `status` черновиков стартует как `queued`. Дальше сервер фоном дергает OZON и обновляет статус/данные.

### Статусы черновиков
- `queued` — ждет окна по лимитам или повтора после 429.
- `in_progress` — отправляем запрос в OZON.
- `draft_created` — получили `operation_id` от `/v1/draft/create`; info-запрос повторяется каждые ~10 секунд, пока статус OZON не станет `CALCULATION_STATUS_SUCCESS`.
- `info_loaded` — получили `draft_id` и `supply_warehouse` от `/v1/draft/create/info`; первый склад автоматически сохраняется в `selected_supply_warehouse`.
- `supply_queued` — поставка поставлена в очередь, ожидает фонового создания.
- `supply_in_progress` — создаём поставку в OZON.
- `supply_failed` — ошибка создания поставки.
- `created` — создана финальная заявка на поставку через `/v1/draft/supply/create` (хранится `operation_id_supply`, выбранный таймслот).
- `failed` — ошибка, смотреть `error_message`.

Статусы батча: `processing`, `completed`, `partial` (если есть ошибки).

### Получение статуса батча
```http
GET /api/ozon/drafts/batch/<batch_id>/
Authorization: Bearer <JWT>
```
Ответ (ключевые поля) — или возьми общий список `GET /api/ozon/drafts/batches/` (опц. `?store_id=<id>`), там батчи уже с черновиками.
```json
{
  "batch_id": "uuid",
  "batch_seq": 12,
  "store": 5,
  "status": "processing",
  "drop_off_point_warehouse_id": 21896333622000,
  "drafts": [
    {
      "id": 101,
      "logistic_cluster_id": 200,
      "logistic_cluster_name": "Краснодар",
      "operation_id": "019b0...",
      "draft_id": 78321044,                 // после info-запроса
      "supply_warehouse": [...],            // склады доставки из info
      "status": "info_loaded",
      "attempts": 1,
      "next_attempt_at": null,
      "error_message": ""
    }
  ]
}
```

### Поведение с лимитами OZON
- Между запросами: минимум 30 секунд (2/мин).
- 50/час: при превышении — откладываем магазин на +1 час.
- 429: откладываем конкретный черновик на +60 сек и повторяем.
- До 3 попыток на черновик; ошибки пишутся в `error_message`.

### Флоу на фронте (подробно)
1. Создать батч: `POST /api/ozon/drafts/create/` → сохранить `batch_id`, `drafts[*].draft_id` (локальный ID). На сервере сразу создаются черновики со статусом `queued`.
2. Пуллинг статуса: `GET /api/ozon/drafts/batch/<batch_id>/` (или список `/api/ozon/drafts/batches/`). Отображать по черновику:
   - `status` (`queued/in_progress/draft_created/info_loaded/failed`);
   - `operation_id`/`draft_id` (OZON);
   - `supply_warehouse` (список предложенных складов), `selected_supply_warehouse` (авто первый).
   - `error_message`, `next_attempt_at` — показать паузу/ошибку, если есть.
3. Выбор склада (если нужно другой): `POST /api/ozon/drafts/<draft_id>/select-warehouse/` с `{ "warehouse_id": <id из supply_warehouse> }`. После — повторно читать статус (поле `selected_supply_warehouse`).
4. Таймслоты:
   - Убедиться, что у черновиков есть `draft_id` (статус `info_loaded`) и выбран склад.
   - Вызвать `POST /api/ozon/drafts/timeslots/fetch/` с `{"batch_id":"...","date_from":"<ISO UTC>","days":<int>}`. Сервер пройдётся по всем черновикам батча, запросит `/v1/draft/timeslot/info` и сохранит ответы в `timeslot_response`.
   - Читать `GET /api/ozon/drafts/batch/<batch_id>/timeslots/`: по каждому черновику → `timeslot_response`, `selected_supply_warehouse`, `selected_timeslot` (если будет выбор), `timeslot_updated_at`; поле `common_dates` — пересечение дат доступных слотов по всем черновикам.
5. Подтверждение поставок: `POST /api/ozon/drafts/batch/<batch_id>/confirm-supply/` с выбранным тайм-слотом → черновики переходят в `supply_queued`, дальше фоновый процесс создаёт поставки.
6. UI: подсвечивать ошибки/отложенные повторы (если `error_message` или `next_attempt_at`), показывать прогресс до `created`, давать выбор склада и слотов на основе полученных данных.

### Планировщик на бэке
- Запускается `python backend/run_scheduler.py` (в docker-сервисе `celery_scheduler`), каждые ~5 секунд проверяет батчи `queued/processing`, обрабатывает черновики с троттлингом, не допускает одновременной обработки одного магазина в нескольких потоках. Также в фоне создаёт поставки для черновиков со статусом `supply_queued`.

## Доступ к магазинам (шаринг)
- `GET /auth/stores/` теперь возвращает магазины владельца **и** магазины, куда пользователя пригласили с принятым доступом. В ответе есть `is_owner` и `owner_username`. Если `is_owner=false`, чувствительные ключи (`api_key`, `performance_client_secret`) обнуляются, `client_id` отдается в маске (`abc***xyz`).
- Создать приглашение (только владелец): `POST /auth/stores/<store_id>/invite/` с `{ "username": "telegram_nick" }` → вернет `status: "pending"`.
- Принять/отклонить приглашение (адресат приглашения): `POST /auth/stores/<store_id>/invite/respond/` с `{ "decision": "accept" }` или `{ "decision": "reject" }`.
- Пользователь с принятым доступом может работать со всеми эндпоинтами черновиков/таймслотов/планнера для этого магазина, но не может менять/удалять сам магазин и не видит ключи.
- Список приглашений для текущего пользователя: `GET /auth/stores/invites/` → массив `{ store_id, store_name, status, invited_by, created_at }`. Возвращаются только `pending`/`rejected` (accepted не отдаются). По нему фронт понимает, что пришло новое приглашение и показывает кнопку принять/отклонить.
- Список всех пользователей с доступом к магазину (только владелец): `GET /auth/stores/<store_id>/accesses/` → `[ { user_id, username, telegram_id, status, invited_by, is_owner } ]`.
- Отозвать доступ конкретного пользователя (только владелец): `DELETE /auth/stores/<store_id>/accesses/<user_id>/`.

---

## FBS заказы (бот + фронт)

### Статусы OZON
- `awaiting_packaging` — ожидает сборки
- `awaiting_deliver` — ожидает отгрузки
- `acceptance_in_progress` — идет приемка
- `delivering` — доставляется
- `delivered` — доставлен
- `cancelled` — отменен

### Синк заказов из OZON
`POST /api/ozon/postings/sync/`

**body**
```json
{
  "store_id": 1,
  "status": "awaiting_packaging",
  "since": "2025-12-22T00:00:00Z",
  "to": "2025-12-23T23:59:59Z",
  "limit": 1000,
  "return_data": true
}
```

Если `since/to` не переданы, берём **последние 3 месяца**.  
Если передан только `since` — `to` = сейчас.  
Если передан только `to` — `since` = `to - 3 месяца`.

**ответ**
```json
{
  "synced": 120,
  "created": 15,
  "updated": 105,
  "postings": [ ... ]
}
```

Если ключ устарел → **401**:
```json
{
  "error": "Необходимо заменить API ключ",
  "detail": "..."
}
```

### Быстрый актуальный ответ для главной вкладки (awaiting_packaging)
`POST /api/ozon/postings/refresh/`

**Что делает**
- Синхронизирует OZON по статусам `awaiting_packaging` и `awaiting_deliver`.
- Возвращает список по выбранному статусу (по умолчанию `awaiting_packaging`) и сразу counts.

**body**
```json
{
  "store_id": 1,
  "status": "awaiting_packaging",
  "since": "2025-12-01T00:00:00Z",
  "to": "2025-12-31T23:59:59Z",
  "limit": 1000
}
```

**ответ**
```json
{
  "store_id": 1,
  "status": "awaiting_packaging",
  "count": 15,
  "counts": {
    "awaiting_packaging": 15,
    "awaiting_deliver": 87,
    "acceptance_in_progress": 3,
    "delivering": 721,
    "delivered": 0,
    "cancelled": 0,
    "unknown": 0
  },
  "total": 826,
  "sync": {
    "awaiting_packaging": { "synced": 120, "created": 15, "updated": 105 },
    "awaiting_deliver": { "synced": 240, "created": 10, "updated": 230 }
  },
  "postings": [ ... ]
}
```

### Получить список заказов из БД
`GET /api/ozon/postings/?store_id=1&status=awaiting_packaging&needs_label=1&include_archived=1`

**params**
- `status` — можно несколько через запятую: `awaiting_packaging,awaiting_deliver`
- `needs_label=1` — только те, что требуют печати
- `include_archived=1` — включить архив (по умолчанию скрыт)
- `force_refresh=1` — принудительный синк OZON перед ответом (для `awaiting_packaging/awaiting_deliver`)
- `since`, `to` — интервал синка (если не переданы, используется 3 месяца)

**ответ**
```json
{
  "store_id": 1,
  "status": "awaiting_packaging",
  "count": 15,
  "counts": { "awaiting_packaging": 15, "awaiting_deliver": 87, "delivering": 721, "delivered": 0, "cancelled": 0, "unknown": 0 },
  "total": 826,
  "postings": [ ... ]
}
```

`counts` всегда возвращается с учетом архива (delivered/cancelled), чтобы табы были полными.

Для `awaiting_packaging` и `awaiting_deliver` эндпоинт **автоматически** делает синк с OZON (с троттлингом).  
Чтобы получить максимально актуально — используйте `force_refresh=1`.

### Структура заказа (OzonFbsPosting)
```json
{
  "id": 123,
  "posting_number": "0120864608-0117-1",
  "order_id": 32988888032,
  "order_number": "0120864608-0117",
  "status": "awaiting_deliver",
  "substatus": "posting_in_carriage",

  "delivery_method_id": 21800343131000,
  "delivery_method_name": "Доставка Ozon самостоятельно, Санкт-Петербург",
  "delivery_method_warehouse_id": 21800343131000,
  "delivery_method_warehouse": "ABSwh",
  "tpl_provider_id": 24,
  "tpl_provider": "Доставка Ozon",
  "tpl_integration_type": "ozon",

  "tracking_number": "",
  "in_process_at": "2025-12-24T00:26:48Z",
  "shipment_date": "2025-12-24T18:30:00Z",
  "delivering_date": null,

  "status_changed_at": "2025-12-24T00:26:48Z",
  "awaiting_packaging_at": "2025-12-24T00:00:00Z",
  "awaiting_deliver_at": "2025-12-24T00:26:48Z",
  "acceptance_in_progress_at": null,
  "delivering_at": null,
  "delivered_at": null,
  "cancelled_at": null,
  "archived_at": null,

  "needs_label": true,
  "labels_printed_at": null,
  "print_count": 0,
  "label_ready": true,
  "label_status": "completed",
  "label_file_url": "https://cdn1.ozone.ru/....pdf",
  "label_file_path": "/workspace/backend/media/ozon/labels/1/0120864608-0117-1_5819_big_label.pdf",

  "products": [ ... ],
  "available_actions": [ ... ],
  "cancellation": { ... },

  "last_seen_at": "2025-12-24T00:26:48Z",
  "last_synced_at": "2025-12-24T00:26:48Z",
  "created_at": "2025-12-24T00:26:48Z",
  "updated_at": "2025-12-24T00:26:48Z"
}
```

### Количества по статусам (для табов)
`GET /api/ozon/postings/counts/?store_id=1`

**ответ**
```json
{
  "store_id": 1,
  "counts": {
    "awaiting_packaging": 15,
    "awaiting_deliver": 87,
    "acceptance_in_progress": 3,
    "delivering": 721,
    "delivered": 0,
    "cancelled": 0,
    "unknown": 0
  },
  "total": 826
}
```

### Печать (защита от дублей)
`POST /api/ozon/postings/print/`

**body**
```json
{
  "store_id": 1,
  "posting_numbers": ["0120864608-0117-1"],
  "force": false
}
```

Если уже печатали → **409**:
```json
{
  "error": "already_printed",
  "message": "Этот заказ уже был распечатан, вы уверены?",
  "posting_numbers": ["0120864608-0117-1"]
}
```

Повторная печать:
```json
{
  "store_id": 1,
  "posting_numbers": ["0120864608-0117-1"],
  "force": true
}
```

### Этикетки (awaiting_deliver)
`POST /api/ozon/postings/labels/`

**Как работает**
- Для каждого `posting_number` создаётся отдельная задача в OZON.
- Если этикетка уже есть в БД, OZON не дергается.
- Возвращается **один PDF**, где 1 этикетка = 1 страница.
- В PDF добавляется подпись `Кол-во товара: N шт.`.

**body**
```json
{
  "store_id": 1,
  "posting_numbers": ["0120864608-0117-1", "0120864608-0118-1"],
  "label_type": "big_label",
  "wait_seconds": 2
}
```

**ответы**
- Если все этикетки готовы → возвращается PDF.
- Если часть ещё в обработке → **202**:
```json
{
  "status": "pending",
  "ready": ["0120864608-0117-1"],
  "pending": ["0120864608-0118-1"],
  "errors": []
}
```

**важно**
- Этикетки формируются только для `awaiting_deliver`.
- `label_type` по умолчанию `big_label` (доступно `small_label`).
- Перед формированием этикеток бэк **сам** обновляет статус `awaiting_deliver` из OZON.

### Экспорт CSV
`GET /api/ozon/postings/export/?store_id=1`

Колонки:
- `posting_number`
- `awaiting_packaging_at`
- `awaiting_deliver_at`
- `acceptance_in_progress_at`
- `delivering_at`
- `delivered_at`
- `cancelled_at`

### Сводка для бота/дашборда
`GET /api/ozon/postings/summary/?store_id=1&period_days=1&avg_days=14&risk_days=2`

**ответ**
```json
{
  "delivering_count": 12,
  "total_active_count": 84,
  "not_delivered_count": 9,
  "not_delivered_postings": ["..."],
  "avg_deliver_hours": 36.5,
  "risk_postings": ["..."],
  "risk_days": 2
}
```

### Настройки бота (сортировка PDF)
`GET /api/ozon/bot/settings/?store_id=1`

`POST /api/ozon/bot/settings/?store_id=1`
```json
{
  "pdf_sort_mode": "offer_id",
  "pdf_sort_ascending": true
}
```

### Рекомендованный флоу на фронте
1. Открыли вкладку “Ожидают сборки” → `POST /api/ozon/postings/sync/` со статусом `awaiting_packaging`.
2. Читаем список через `GET /api/ozon/postings/?store_id=...&status=awaiting_packaging`.
3. Аналогично для остальных вкладок: `awaiting_deliver`, `acceptance_in_progress`, `delivering`.
4. Подсветка новых к печати — `needs_label=true` (ставится при переходе в `awaiting_deliver`).
5. Печать → `POST /api/ozon/postings/print/`. При дубле фронт подтверждает повтор (force=true).
6. Архивные (delivering/delivered/cancelled) скрыты по умолчанию, показываются по `include_archived=1`.
По умолчанию **включает архивные** (delivered/cancelled). Если нужны только активные — добавьте `include_archived=0`.
