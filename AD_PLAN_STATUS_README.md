# Статусы рекламных планов

## Описание

Добавлена система статусов для рекламных планов (`AdPlanRequest`), которая позволяет различать предпросмотр и активированные планы.

## Статусы

### 1. **PREVIEW** (Предпросмотр)
- План создан для тестирования настроек
- Пользователь подбирает правильные параметры
- Может быть перезаписан при следующем запуске `update_abc_sheet`

### 2. **ACTIVATED** (Активирован)
- План утвержден и используется для создания рекламных кампаний
- Защищен от автоматического удаления
- Требует ручной активации через `activate_ad_plan()`

## Логика работы

### В функции `update_abc_sheet()`:

```python
# Проверяем статусы существующих запросов
existing_activated_requests = AdPlanRequest.objects.filter(
    store=store, 
    status=AdPlanRequest.ACTIVATED
).exists()

if existing_activated_requests:
    # Если есть активированные запросы, создаем только предпросмотр
    request_status = AdPlanRequest.PREVIEW
else:
    # Если нет активированных запросов, удаляем все старые записи
    AdPlanRequest.objects.filter(store=store).delete()
    request_status = AdPlanRequest.PREVIEW
```

### Правила:

1. **Если есть активированные планы** → создается только предпросмотр
2. **Если нет активированных планов** → удаляются все старые записи и создается предпросмотр
3. **Активированные планы защищены** от автоматического удаления

## Функции

### `activate_ad_plan(ad_plan_request_id)`
Активирует рекламный план, изменяя статус с `PREVIEW` на `ACTIVATED`.

```python
# Пример использования
activate_ad_plan(123)  # Активирует план с ID 123
```

### `create_campaigns_from_preview(ad_plan_request_id)`
Создает рекламные кампании на основе предпросмотра плана и автоматически активирует план.

```python
# Пример использования
create_campaigns_from_preview(123)  # Создает кампании для плана с ID 123
create_campaigns_from_preview()     # Создает кампании для последнего предпросмотра
```

## Админка

В Django Admin добавлено:
- Поле `status` в список отображаемых полей
- Фильтр по статусу
- Возможность изменения статуса вручную

## API Эндпоинты

### `POST /ozon/analytics/abc/update`
Обновляет лист ABC в Google Sheets.

**Параметры:**
- `spreadsheet_url` (опционально): URL Google таблицы
- `sa_json_path` (опционально): Путь к JSON файлу сервисного аккаунта
- `sync` (опционально): Синхронный режим (по умолчанию: true)

**Примеры запросов:**

**Синхронный режим (по умолчанию):**
```json
{
    "spreadsheet_url": "https://docs.google.com/spreadsheets/d/...",
    "sync": true
}
```

**Асинхронный режим:**
```json
{
    "spreadsheet_url": "https://docs.google.com/spreadsheets/d/...",
    "sync": false
}
```

**Ответы:**

**Асинхронный:**
```json
{
    "status": "accepted",
    "task_id": "task-uuid",
    "message": "Задача обновления ABC листа запущена"
}
```

**Синхронный:**
```json
{
    "status": "completed",
    "message": "Обновление ABC листа завершено успешно"
}
```

### `POST /ozon/campaigns/create-from-preview/`
Создает рекламные кампании из предпросмотра плана.

**Параметры:**
- `ad_plan_request_id` (опционально): ID запроса рекламного плана

**Пример запроса:**
```json
{
    "ad_plan_request_id": 123
}
```

**Ответ:**
```json
{
    "status": "accepted",
    "task_id": "task-uuid",
    "message": "Задача создания рекламных кампаний запущена"
}
```

### `POST /ozon/createorupdateads/`
Обновляет статус рекламного плана.

**Параметры:**
- `ad_plan_request_id`: ID запроса рекламного плана
- `action`: Действие (по умолчанию: "activate")

**Пример запроса:**
```json
{
    "ad_plan_request_id": 123,
    "action": "activate"
}
```

**Ответ:**
```json
{
    "status": "accepted",
    "task_id": "task-uuid",
    "message": "Задача активации рекламного плана запущена"
}
```

## Миграция

Для применения изменений необходимо создать и применить миграцию:

```bash
python manage.py makemigrations ozon
python manage.py migrate
```

## Интеграция с Google Sheets

### Поле `google_sheet_row`

В модель `AdPlanItem` добавлено поле `google_sheet_row`, которое сохраняет номер строки в Google таблице, куда была записана информация о кампании.

**Логика работы:**
- При создании рекламного плана в `update_abc_sheet()` каждый элемент плана получает номер строки
- Нумерация начинается с 13-й строки (как в Google таблице)
- Номер строки сохраняется в базе данных для последующего использования

**Пример:**
```python
# Элемент плана с номером строки 15 в Google таблице
ad_plan_item = AdPlanItem.objects.get(id=1)
print(f"Строка в Google таблице: {ad_plan_item.google_sheet_row}")  # 15
```

## Примеры использования

### 1. Создание предпросмотра
```python
# Автоматически при запуске update_abc_sheet()
# Создается план со статусом PREVIEW
```

### 2. Активация плана
```python
from ozon.tasks import activate_ad_plan

# Активируем план после проверки настроек
activate_ad_plan(plan_id)
```

### 3. Проверка статуса
```python
from ozon.models import AdPlanRequest

# Проверяем есть ли активированные планы
has_activated = AdPlanRequest.objects.filter(
    store=store, 
    status=AdPlanRequest.ACTIVATED
).exists()
```

### 4. Обновление ABC листа через API
```bash
# Синхронное обновление (по умолчанию - дождаться завершения)
curl -X POST http://localhost:8000/ozon/analytics/abc/update \
  -H "Content-Type: application/json" \
  -d '{}'

# Асинхронное обновление (запустить в фоне)
curl -X POST http://localhost:8000/ozon/analytics/abc/update \
  -H "Content-Type: application/json" \
  -d '{"sync": false}'
```

### 5. Создание кампаний через API
```bash
# Создать кампании для конкретного плана
curl -X POST http://localhost:8000/ozon/campaigns/create-from-preview/ \
  -H "Content-Type: application/json" \
  -d '{"ad_plan_request_id": 123}'

# Создать кампании для последнего предпросмотра
curl -X POST http://localhost:8000/ozon/campaigns/create-from-preview/ \
  -H "Content-Type: application/json" \
  -d '{}'
```

### 6. Обновление плана через API
```bash
# Активировать план
curl -X POST http://localhost:8000/ozon/createorupdateads/ \
  -H "Content-Type: application/json" \
  -d '{"ad_plan_request_id": 123, "action": "activate"}'
```
