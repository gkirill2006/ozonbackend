Ozon Ads Backend — Обзор и руководство
=====================================

В этом документе описаны основные задачи, эндпоинты, модели и интеграция с Google-таблицей, используемые для управления рекламой Ozon.

Содержание
----------
1) Обзор архитектуры
2) Ключевые задачи (backend/ozon/tasks.py)
3) Утилиты (backend/ozon/utils.py)
4) API-эндпоинты (backend/ozon/views.py → backend/ozon/urls.py)
5) Модели данных (backend/ozon/models.py)
6) Колонки Google-таблицы (Main_ADV)
7) Типовые сценарии
8) Заметки по настройке

1) Обзор архитектуры
--------------------
- Google-таблица Main_ADV является источником конфигурации и действий.
- Автоматические кампании (AdPlanItem) управляются через Ozon Performance API.
- Отчеты по эффективности (CampaignPerformanceReport + Entry) сохраняются и используются для KPI и перерасчета бюджетов.

2) Ключевые задачи (backend/ozon/tasks.py)
------------------------------------------
- Каталог / Товары / Склад / Продажи:
  - sync_all_ozon_categories(), sync_all_products(), sync_all_warehouse_stocks(), sync_all_fbs_stocks(), sync_product_daily_analytics()
- ABC и Бюджет:
  - update_abc_sheet(spreadsheet_url=None, sa_json_path=None, consider_spent=0)
    - Если consider_spent=1: вычесть moneySpent (с 1-го дня текущего месяца) и распределить остаток на оставшиеся дни месяца.

**Подробно: update_abc_sheet**
- **Назначение:** обновляет Google‑таблицу с ABC‑анализом и бюджетами. Считает общий рекламный бюджет как долю от выручки, при необходимости вычитает уже потраченное, распределяет недельный/дневной бюджет по товарам, формирует список TOP‑N и заполняет два листа: `ABC` и `Main_ADV`.
- **Вход:**
  - `spreadsheet_url` и `sa_json_path` или переменные окружения `ABC_SPREADSHEET_URL`, `GOOGLE_SA_JSON_PATH`.
  - `consider_spent` (0/1) — учитывать ли уже потраченный с начала месяца бюджет по Performance‑отчетам.
- **Источник данных:**
  - Выручка и количество из `ProductDailyAnalytics` за окно `V13` (дней, без сегодняшнего).
  - Остатки: `FbsStock` и `WarehouseStock`.
  - Кампании: `ManualCampaign` и `AdPlanItem` для статусов и исключения/включения в TOP‑N.
  - Потрачено (если `consider_spent=1`): `moneySpent` из `CampaignPerformanceReportEntry` с 1‑го числа текущего месяца.
- **Настройки `Main_ADV`:**
  - `V13` — окно дней ABC; `V14` — доля выручки на рекламу; `V15` — максимум товаров в TOP‑N.
  - `V16`/`W16` — мин/макс цена; `V17` — train_days (инфо);
  - `V18`/`V19`/`V20` — доли A/B/C; `V21` — режим распределения: 0 — равномерно, 1 — по выручке;
  - `V22` — минимальный недельный бюджет на товар;
  - `V24` — добавлять товары с уже существующими РК (0 — нет, 1 — да);
  - `V25` — учитывать суммарный недельный бюджет ручных РК (вычитается; сумма пишется в `C8`);
  - `V26`/`V27` — минимальные остатки FBS/FBO; `Y` (с `Y13`) — исключаемые `offer_id`.
- **Ход работы:**
  - Определяет магазин по `V23` (имя или `client_id`). При отсутствии — завершает работу.
  - Агрегирует продажи по SKU за `V13` дней, сортирует по выручке, присваивает метки A/B/C по кумулятивной выручке и записывает лист `ABC` (A:J) с раскраской колонки `F`.
  - Считает рекламный бюджет: `total_revenue * V14`. При `consider_spent=1` вычитает `moneySpent` с начала месяца и распределяет остаток на оставшиеся дни месяца, затем переводит в неделю/день.
  - Если `V25=1`, уменьшает недельный бюджет на сумму недельных бюджетов ручных РК (`RUNNING/STOPPED`) и пишет эту сумму в `C8`.
  - Определяет `n_max`: `floor(weekly_budget / V22)` либо использует `V15` (если задан), применяет фильтры по `Y`, цене (`V16/W16`) и остаткам (`V26/V27`). Поведение по `V24` управляет включением товаров с уже существующими РК.
  - Распределяет недельный бюджет по выбранным SKU: равномерно (`V21=0`) или пропорционально выручке (`V21=1`), соблюдая минимум `V22` и не превышая общий недельный бюджет.
  - Обновляет сводку: `B4` — выручка; `B5/B6` — месячный бюджет; `C6` — недельный (до учета ручных); `D6` — дневной; `E4/E5/E6` — метки времени; `C8` — неделя ручных РК (при `V25=1`).
  - Заполняет `Main_ADV` с 13‑й строки: `A` — ID кампании, `B` — вкл/выкл (1/0, пусто для ручных), `C` — статус, `D` — название РК, `E` — тип (Ручная/Авто), `F` — товар, `G` — SKU, `H` — FBS остаток, `I` — FBO остаток, `J` — недельный бюджет, `K` — недельный бюджет ручной РК, `L` — дневной бюджет.
- **Результат:** лист `ABC` отражает структуру ассортимента по ABC, `Main_ADV` — актуальные бюджеты, статусы кампаний и TOP‑N.
- **Когда запускать:** ежемесячно (`consider_spent=0`) и по кнопке «Обновить РК» в течение месяца (`consider_spent=1`).
- **Заметки для разработчика:** денежные расчеты на `Decimal` (округление до 0.01), недельный минимум — `V22`; `moneySpent` парсится из JSON `totals` в `CampaignPerformanceReportEntry`; запись в Google Sheets батч‑диапазонами.
- Ручные кампании:
  - sync_manual_campaigns(store_id=None)
- Создание/обновление реклам из таблицы:
  - create_or_update_AD(...): читает A:L, создает/обновляет автоматические кампании, останавливает отсутствующие.
- Синхронизация активности из таблицы:
  - sync_campaign_activity_with_sheets(..., override_training=0): синхронизирует B (вкл/выкл) с Ozon и обновляет колонку C.
- Мониторинг:
  - monitor_auto_campaigns_weekly(), reactivate_campaign_later()
- Отчеты по эффективности:
  - submit_performance_report_requests(), fetch_performance_reports(), submit_daily_reports_for_campaign(), submit_auto_reports_for_day(), submit_reports_for_campaigns(), submit_auto_reports_for_yesterday()/today()
- KPI в таблицу:
  - update_auto_campaign_kpis_in_sheets(): заполняет M..S
- Кнопка перерасчета (Обновить РК):
  - reforecast_ad_budgets_for_period(): update_abc_sheet(consider_spent=1) → create_or_update_AD()
- Система Старт/Стоп (S3):
  - toggle_store_ads_status(store_id,...): переключает StoreAdControl и пишет S3

3) Утилиты (backend/ozon/utils.py)
----------------------------------
- Токены: request_performance_token(), get_store_performance_token(store)
- Операции с кампаниями: create_cpc_product_campaign(), update_campaign_budget(), activate/deactivate (+ обертки *_for_store)
- Хелперы Seller API для товаров, складских остатков и продаж

4) API-эндпоинты
-----------------
- POST /ozon/products/ — синхронизировать товары
- POST /ozon/categories/sync/ — синхронизировать дерево категорий
- POST /ozon/warehouse/sync/ — синхронизировать остатки (FBO)
- POST /ozon/sales/sync/ — синхронизировать продажи (FBO/FBS)
- POST /ozon/analytics/ — ProductAnalytics_V2_View
- POST /ozon/analytics/products/by-item/ — ProductAnalyticsByItemView
- POST /ozon/analytics/abc/update — запустить update_abc_sheet (sync/async)
- GET  /ozon/createorupdateads/ — прочитать таблицу и создать/обновить кампании (sync)
- POST /ozon/campaigns/sync-override/ — синхронизация активности с override_training=1
- POST /ozon/ads/toggle/ — переключить StoreAdControl и записать S3

5) Модели данных (backend/ozon/models.py)
-----------------------------------------
- Product, Category, ProductType, WarehouseStock, Sale, FbsStock
- DeliveryCluster, DeliveryClusterItemAnalytics, DeliveryAnalyticsSummary
- ProductDailyAnalytics
- AdPlanItem (автоматические), ManualCampaign (ручные)
- CampaignPerformanceReport, CampaignPerformanceReportEntry
- StoreAdControl (is_system_enabled) — мастер-флаг запуска/остановки для магазина

6) Колонки Google-таблицы (Main_ADV)
------------------------------------
- V13 — окно в днях для ABC-анализа
- V14 — процент/доля выручки на рекламу
- V17 — train_days
- V23 — магазин (name или client_id)
- Строки ABC: A=campaign_id, B=active, C=status, D=название кампании, E=тип, F=offer_id, G=SKU, J=недельный бюджет, K=недельный ручной бюджет, L=дневной бюджет
- Строки KPI (через update_auto_campaign_kpis_in_sheets): M..S
- S3 — глобальный статус системы (Включен/Выключен)

7) Типовые сценарии
--------------------
- Ежемесячный запуск: update_abc_sheet(consider_spent=0) → create_or_update_AD()
- Кнопка «Обновить РК»: reforecast_ad_budgets_for_period() → update_abc_sheet(consider_spent=1) → create_or_update_AD()
- Старт/Стоп: POST /ozon/ads/toggle/ → переключает StoreAdControl и S3
- Почасовая синхронизация активности: sync_campaign_activity_with_sheets() (записывает статусы в колонку C)
- KPI в таблицу: update_auto_campaign_kpis_in_sheets()

8) Заметки по настройке
------------------------
- Требуются миграции для: StoreAdControl, CampaignPerformanceReport, CampaignPerformanceReportEntry
  - Команда: `python manage.py makemigrations ozon && python manage.py migrate`
- Должны быть заданы учетные данные Ozon Performance API в OzonStore (performance_client_id/secret)
- Для корректного перерасчета бюджетов убедитесь, что отчеты загружены (submit_* + fetch_performance_reports)
