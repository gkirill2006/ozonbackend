# ЕЖЕДНЕВНАЯ АНАЛИТИКА ПО ТОВАРУ
class ProductDailyAnalytics(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='daily_analytics')    
    sku = models.BigIntegerField()
    offer_id = models.CharField(max_length=255, blank=True)  # Артикул товара
    name = models.CharField(max_length=500, blank=True)  # Название товара    
    # Дата аналитики
    date = models.DateField()
    # Метрики
    revenue = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    ordered_units = models.PositiveIntegerField(default=0)
    # Служебные поля
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    class Meta:
        unique_together = ("store", "date", "sku")
        verbose_name = "Ежедневная аналитика товара"
        verbose_name_plural = "Ежедневная аналитика товаров"

    def __str__(self):
        return f"{self.date} | SKU {self.sku} | {self.revenue} ₽"
    
    
ANALYTICS_DATA_URL = "https://api-seller.ozon.ru/v1/analytics/data"

def _ozon_headers(store: OzonStore) -> dict:
    return {
        "Client-Id": store.client_id,
        "Api-Key": store.api_key,
        "Content-Type": "application/json",
    }

def _post_with_rate_limit(url: str, headers: dict, payload: dict, max_retries: int = 6):
    """
    Ожидать 5 секунд и повторить попытку, если code==8 и сообщение указывает на ограничение частоты.  
    max_retries ограничивает общее время ожидания примерно одной минутой.  
    """
    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            body = resp.json()
            # Ozon sometimes returns 200 with error payloads for rate limit
            if isinstance(body, dict) and body.get("code") == 8:
                logger.info("[⏳] Rate limit hit (code 8). Sleeping 10s before retry...")
                time.sleep(5)
                continue
            return resp
        # Non-200: check rate limit payload
        try:
            body = resp.json()
        except Exception:
            body = None
        if isinstance(body, dict) and body.get("code") == 8:
            logger.info("[⏳] Rate limit hit (non-200). Sleeping 10s before retry...")
            time.sleep(10)
            continue
        # Other errors
        resp.raise_for_status()
    raise Exception("Exceeded max retries due to rate limiting on Ozon analytics/data")


def _iter_analytics_pages(store: OzonStore, date_from: str, date_to: str):
    headers = _ozon_headers(store)
    limit = 1000
    # По примеру из запроса пользователя offset=1
    offset = 1
    while True:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["revenue", "ordered_units"],
            "dimension": ["sku", "day"],
            "filters": [],
            "sort": [{"key": "hits_view_search", "order": "DESC"}],
            "limit": limit,
            "offset": offset,
        }
        resp = _post_with_rate_limit(ANALYTICS_DATA_URL, headers, payload)
        data = resp.json().get("result", {}).get("data", [])
        if not data:
            break
        yield data
        if len(data) < limit:
            break
        # Смещаемся на следующий блок
        offset += limit


def _save_analytics_batch(store: OzonStore, rows: list):

    # Map of sku -> (offer_id, name)
    skus = []
    for row in rows:
        dims = row.get("dimensions", [])
        if len(dims) >= 1:
            sku_str = dims[0].get("id")
            try:
                skus.append(int(sku_str))
            except Exception:
                continue

    product_map = {
        p.sku: (p.offer_id, p.name)
        for p in Product.objects.filter(store=store, sku__in=skus)
    }

    objects_to_upsert = []
    for row in rows:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [])
        if len(dims) < 2 or len(metrics) < 2:
            continue
        sku_str = dims[0].get("id")
        date_id_str = dims[1].get("id")  # например: "2025-08-01"
        name_value = dims[0].get("name", "")
        try:
            sku_val = int(sku_str)
        except Exception:
            continue
        offer_id_val, product_name_val = product_map.get(sku_val, ("", name_value))

        # Преобразуем типы корректно
        revenue_val = Decimal(str(metrics[0] or 0))
        ordered_units_val = int(metrics[1] or 0)

        # Дата как date-объект
        try:
            date_val = dt_date.fromisoformat(date_id_str)
        except Exception:
            # Пропустим строку, если дата некорректна
            continue

        objects_to_upsert.append(
            ProductDailyAnalytics(
                store=store,
                sku=sku_val,
                offer_id=offer_id_val,
                name=product_name_val,
                date=date_val,
                revenue=revenue_val,
                ordered_units=ordered_units_val,
            )
        )

    # Upsert by unique (store, date, sku)
    for obj in objects_to_upsert:
        ProductDailyAnalytics.objects.update_or_create(
            store=obj.store, date=obj.date, sku=obj.sku,
            defaults={
                "offer_id": obj.offer_id,
                "name": obj.name,
                "revenue": obj.revenue,
                "ordered_units": obj.ordered_units,
            }
        )


@shared_task(name="Синхронизация ежедневной аналитики по товарам")
def sync_product_daily_analytics():
    """
    Ежедневно:
    - если записей нет, грузим за последние 30 дней;
    - иначе обновляем данные за прошедшие 10 дней (данные Озона могут меняться).
    
    Важно: Озон может обновлять данные аналитики в течение 10 дней после даты,
    поэтому мы обновляем данные за этот период каждый день для получения
    наиболее актуальной информации.
    """
    for store in OzonStore.objects.all():
        try:
            if not ProductDailyAnalytics.objects.filter(store=store).exists():
                # Первичная загрузка: последние 30 дней
                date_to = dt_date.today() - timedelta(days=1)
                date_from = date_to - timedelta(days=29)
                logger.info(f"[📊] {store}: первичная загрузка аналитики {date_from}..{date_to}")
            else:
                # Ежедневное обновление: прошедшие 10 дней для актуализации данных
                date_to = dt_date.today() - timedelta(days=1)
                date_from = date_to - timedelta(days=9)  # 10 дней включая вчерашний
                logger.info(f"[📊] {store}: обновление аналитики за прошедшие 10 дней ({date_from}..{date_to})")

            df_str = date_from.strftime("%Y-%m-%d")
            dt_str = date_to.strftime("%Y-%m-%d")

            for page in _iter_analytics_pages(store, df_str, dt_str):
                _save_analytics_batch(store, page)

            logger.info(f"[✅] {store}: аналитика обновлена за период {df_str}..{dt_str}")
        except Exception as e:
            logger.error(f"[❌] Ошибка загрузки аналитики для {store}: {e}")
            

    
@shared_task(name="Обновление листа ABC1 из ProductDailyAnalytics")
def update_abc_sheet(spreadsheet_url: str = None, sa_json_path: str = None, consider_spent: int = 0):
    """
    Обновляет лист ABC из ProductDailyAnalytics.
    """
    
    spreadsheet_url = spreadsheet_url or os.getenv(
        "ABC_SPREADSHEET_URL",
        "https://docs.google.com/spreadsheets/d/1-_XS6aRZbpeEPFDyxH3OV0IMbl_GUUEysl6ZJXoLmQQ",
    )
    sa_json_path = sa_json_path or os.getenv(
        "GOOGLE_SA_JSON_PATH",
        "/workspace/ozon-469708-c5f1eca77c02.json",
    )

    # Авторизация в Google Sheets
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
    gc = gspread.authorize(creds)
    t0 = time.perf_counter()
    sh = gc.open_by_url(spreadsheet_url)
    t_open = time.perf_counter(); logger.info(f"[⏱] Открытие таблицы: {t_open - t0:.3f}s")

    # Читаем параметры из Main_ADV одним батч-запросом
    ws_main = sh.worksheet('Main_ADV')
    # Настройки сдвинулись на один столбец вправо: теперь колонка T (и U для max цены)
    param_cells = ['V13','V14','V15','V16','W16','V17','V21','V18','V19','V20', 'V22', 'V23','V24','V25','V26', 'V27']
    param_vals = ws_main.batch_get([f'{c}:{c}' for c in param_cells])
    cell_value = {}
    
    def _get(cell_ref: str) -> str:
        return (cell_value.get(cell_ref) or '')
    
    for i, c in enumerate(param_cells):
        try:
            cell_value[c] = (param_vals[i][0][0] if param_vals[i] and param_vals[i][0] else '')
        except Exception:
            cell_value[c] = ''
            

    # T13 — строка вида "28 дней"/"3 дня"
    t13_value = _get('V13')
    digits = ''.join(ch for ch in (t13_value or '') if ch.isdigit())
    days = int(digits) if digits else 3
    # Вспомогательные парсеры чисел/процентов
    def _parse_decimal(cell_value: str, default: str = '0') -> Decimal:
        s = (cell_value or '').strip().replace(' ', '')
        cleaned = ''.join(ch for ch in s.replace(',', '.') if ch.isdigit() or ch == '.')
        if cleaned == '':
            cleaned = default
        try:
            return Decimal(cleaned)
        except Exception:
            return Decimal(default)

    def _parse_int(cell_value: str, default: int = 0) -> int:
        s = (cell_value or '').strip()
        digits_local = ''.join(ch for ch in s if ch.isdigit())
        return int(digits_local) if digits_local else default

    def _parse_percent(cell_value: str, default: Decimal = Decimal('0')) -> Decimal:
        val = _parse_decimal(cell_value, '0')
        # Трактуем целые значения как проценты: 1 -> 1% -> 0.01; 10 -> 10% -> 0.10
        # Значения уже в долях (<1) оставляем как есть (например 0.1)
        if val >= 1:
            return (val / Decimal('100'))
        return val

    # T23 — название магазина (регистр не учитываем)
    store_name_value = (_get('V23') or '').strip()
    store = None
    if store_name_value:
        store = (
            OzonStore.objects.filter(name__iexact=store_name_value).first()
            or OzonStore.objects.filter(client_id__iexact=store_name_value).first()
            or OzonStore.objects.filter(name__icontains=store_name_value).first()
            or OzonStore.objects.filter(client_id__icontains=store_name_value).first()
        )
    # Исправлено описание координат: используем реальные V13..V27
    t_params = time.perf_counter(); logger.info(f"[⏱] Чтение параметров (V13..V27): {t_params - t_open:.3f}s")
    if not store:
        logger.warning(f"[⚠️] Магазин из Main_ADV!S23 не найден: '{store_name_value}'. Пропускаем обновление ABC1.")
        return
    logger.info(f"[📄] ABC по магазину: {store}")

    # Считываем  настройки
    promo_budget_pct = _parse_percent(_get('V14'), Decimal('0'))
    max_items = _parse_int(_get('V15'), 0)
    price_min = _parse_decimal(_get('V16'), '0')
    price_max = _parse_decimal(_get('W16'), '0')
    train_days = _parse_int(_get('V17'), 0)
    a_share = _parse_percent(_get('V18'))
    b_share = _parse_percent(_get('V19'))
    c_share = _parse_percent(_get('V20'))
    budget_mode = _parse_int(_get('V21'), 0)
    min_budget =  _parse_int(_get('V22'), 0)
    
    # Новые параметры
    add_existing_campaigns = _parse_int(_get('V24'), 0)  # Добавлять товар, если уже есть РК
    consider_manual_budget = _parse_int(_get('V25'), 0)  # Учитывать бюджет ручных РК при создании
    # recalc_budget_changes = _parse_int(_get('V26'), 0)   # Пересчитывать бюджет с учетом изменений
    min_fbs_stock = _parse_int(_get('V26'), 0)           # Остаток FBS min, шт
    min_fbo_stock = _parse_int(_get('V27'), 0)           # Остаток FBO min, шт


    # total_share = a_share + b_share + c_share
    # if total_share == 0:
    #     a_share, b_share, c_share = Decimal('0.80'), Decimal('0.15'), Decimal('0.05')
    logger.info(f"Настройки: promo_budget={promo_budget_pct}, max_items={max_items}, price_min={price_min}, price_max={price_max}, train_days={train_days}, budget_mode={budget_mode}")
    logger.info(f"min_fbs_stock = {min_fbs_stock} min_fbo_stock = {min_fbo_stock}")
    logger.info(f"ABC проценты: A={a_share*100}%, B={b_share*100}%, C={c_share*100}%")

    # Готовим словари остатков по SKU для фильтрации
    fbs_by_sku = {
        row['sku']: row['total'] or 0
        for row in FbsStock.objects.filter(store=store)
            .values('sku')
            .annotate(total=Sum('present'))
    }
    fbo_by_sku = {
        row['sku']: row['total'] or 0
        for row in WarehouseStock.objects.filter(store=store)
            .values('sku')
            .annotate(total=Sum('available_stock_count'))
    }
    logger.info(f"[ℹ️] Загружены остатки: FBS для {len(fbs_by_sku)} SKU, FBO для {len(fbo_by_sku)} SKU")

    # Больше не используем AdPlanRequest - работаем только с AdPlanItem
    logger.info(f"[ℹ️] Работаем напрямую с AdPlanItem для магазина {store}")

    # Диапазон дат: последние N дней без сегодняшнего
    today = dt_date.today()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=days - 1)
    logger.info(f"date_from = {date_from} date_to = {date_to}")
    # Агрегация и сортировка на стороне БД
    base_qs = ProductDailyAnalytics.objects.filter(store=store, date__gte=date_from, date__lte=date_to)
    total_revenue_val = base_qs.aggregate(t=Sum('revenue'))['t'] or 0
    #Сумарная выручка
    total_revenue = Decimal(str(total_revenue_val))

    # Агрегаты + вычисление кумулятивной суммы по выручке в БД
    agg_qs = (
        base_qs.values('offer_id', 'name', 'sku')
        .annotate(revenue_sum=Sum('revenue'), units_sum=Sum('ordered_units'))
        .order_by('-revenue_sum')
    )
    t_qs = time.perf_counter(); logger.info(f"[⏱] ORM агрегация+сортировка: {t_qs - t_params:.3f}s (rows={agg_qs.count()})")
    
    # Получаем данные о рекламных кампаниях для добавления в ABC
    from .models import ManualCampaign
    
    # Функция для перевода статусов на русский язык
    def _translate_campaign_status(status, is_manual=True):
        """Переводит статус кампании на русский язык"""
        if is_manual:
            # Статусы ручных кампаний
            status_translations = {
                ManualCampaign.CAMPAIGN_STATE_RUNNING: 'Запущена',
                ManualCampaign.CAMPAIGN_STATE_ACTIVE: 'Активна',
                ManualCampaign.CAMPAIGN_STATE_INACTIVE: 'Неактивна',
                ManualCampaign.CAMPAIGN_STATE_PLANNED: 'Запланирована',
                ManualCampaign.CAMPAIGN_STATE_STOPPED: 'Остановлена',
                ManualCampaign.CAMPAIGN_STATE_ARCHIVED: 'Архивная',
                ManualCampaign.CAMPAIGN_STATE_FINISHED: 'Завершена',
                ManualCampaign.CAMPAIGN_STATE_PAUSED: 'Приостановлена',
                ManualCampaign.CAMPAIGN_STATE_ENDED: 'Завершена',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_DRAFT: 'Черновик модерации',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_IN_PROGRESS: 'На модерации',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_FAILED: 'Не прошла модерацию',
                ManualCampaign.CAMPAIGN_STATE_UNKNOWN: 'Неизвестно',
            }
        else:
            # Статусы автоматических кампаний
            status_translations = {
                'PREVIEW': 'Предпросмотр',
                'ACTIVATED': 'Активирована',
                'UNKNOWN': 'Неизвестно',
                'CAMPAIGN_STATE_RUNNING': 'Запущена',
                'CAMPAIGN_STATE_ACTIVE': 'Активна',
                'CAMPAIGN_STATE_INACTIVE': 'Неактивна',
                'CAMPAIGN_STATE_PLANNED': 'Запланирована',
                'CAMPAIGN_STATE_STOPPED': 'Остановлена',
                'CAMPAIGN_STATE_ARCHIVED': 'Архивная',
                'CAMPAIGN_STATE_FINISHED': 'Завершена',
                'CAMPAIGN_STATE_PAUSED': 'Приостановлена',
                'CAMPAIGN_STATE_ENDED': 'Завершена',
                'CAMPAIGN_STATE_MODERATION_DRAFT': 'Черновик модерации',
                'CAMPAIGN_STATE_MODERATION_IN_PROGRESS': 'На модерации',
                'CAMPAIGN_STATE_MODERATION_FAILED': 'Не прошла модерацию',
                'CAMPAIGN_STATE_UNKNOWN': 'Неизвестно',
            }
        
        return status_translations.get(status, status)
    
    # Получаем все SKU из результатов для поиска кампаний
    all_skus = [v['sku'] for v in agg_qs if v['sku']]
    logger.info(f"[ℹ️] Обрабатываем {len(all_skus)} уникальных SKU для поиска кампаний")
    
    # Получаем ручные кампании по SKU
    manual_campaigns_dict = {}
    if all_skus:
        # Учитываем только запущенные и остановленные кампании
        active_states = [
            'CAMPAIGN_STATE_RUNNING',
            'CAMPAIGN_STATE_STOPPED'
        ]
        logger.info(f"[ℹ️] Ищем кампании только со статусами: {active_states}")
        
        # Получаем кампании по основному SKU
        manual_campaigns_by_sku = ManualCampaign.objects.filter(
            store=store, 
            sku__in=all_skus,
            state__in=active_states
        ).select_related('store')
        
        # Получаем кампании, где SKU есть в sku_list
        manual_campaigns_by_sku_list = ManualCampaign.objects.filter(
            store=store,
            sku_list__overlap=all_skus,
            state__in=active_states
        ).select_related('store')
        
        # Объединяем результаты
        manual_campaigns = list(manual_campaigns_by_sku) + list(manual_campaigns_by_sku_list)
        # Убираем дубликаты по ID кампании
        seen_ids = set()
        unique_campaigns = []
        for campaign in manual_campaigns:
            if campaign.id not in seen_ids:
                seen_ids.add(campaign.id)
                unique_campaigns.append(campaign)
        manual_campaigns = unique_campaigns
        
        sku_added_count = 0
        for campaign in manual_campaigns:
            campaign_sku_count = 0
            # Добавляем основной SKU
            if campaign.sku:
                manual_campaigns_dict[campaign.sku] = {
                    'name': campaign.name,
                    'type': 'Ручное',  # Ручная
                    'ozon_updated_at': campaign.ozon_updated_at,
                    'status': _translate_campaign_status(campaign.state, is_manual=True)
                }
                sku_added_count += 1
            
            # Добавляем все SKU из sku_list
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                for sku_item in campaign.sku_list:
                    if sku_item and sku_item not in manual_campaigns_dict:
                        manual_campaigns_dict[sku_item] = {
                            'name': campaign.name,
                            'type': 'Ручное',  # Ручная
                            'ozon_updated_at': campaign.ozon_updated_at,
                            'status': _translate_campaign_status(campaign.state, is_manual=True)
                        }
                        sku_added_count += 1
            
            # Логируем информацию о кампании
            campaign_sku_count = 1 if campaign.sku else 0
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                campaign_sku_count += len([sku for sku in campaign.sku_list if sku])
            logger.info(f"[ℹ️] Кампания {campaign.name} (ID: {campaign.ozon_campaign_id}) содержит {campaign_sku_count} SKU")
    
    # Логируем количество найденных ручных кампаний
    logger.info(f"[ℹ️] Найдено {len(manual_campaigns_dict)} SKU с ручными кампаниями для магазина {store} (добавлено {sku_added_count} SKU)")
    if manual_campaigns_dict:
        logger.info(f"[ℹ️] SKU с ручными кампаниями: {list(manual_campaigns_dict.keys())[:10]}{'...' if len(manual_campaigns_dict) > 10 else ''}")
    
    # Получаем автоматические кампании по SKU (если ручных нет)
    auto_campaigns_dict = {}
    if all_skus:
        auto_campaigns = AdPlanItem.objects.filter(
            store=store,
            sku__in=all_skus
        ).exclude(
            sku__in=manual_campaigns_dict.keys()  # Исключаем SKU, для которых уже есть ручные кампании
        ).select_related('store')
        
        for campaign in auto_campaigns:
            if campaign.sku and campaign.sku not in auto_campaigns_dict:
                # Получаем реальный статус автоматической кампании из базы данных
                auto_status = campaign.state if campaign.state else 'CAMPAIGN_STATE_UNKNOWN'
                
                auto_campaigns_dict[campaign.sku] = {
                    # Предпочитаем явное название кампании из модели
                    'name': (campaign.campaign_name or campaign.name or campaign.offer_id),
                    'type': 'Авто',  # Автоматическая
                    'ozon_updated_at': None,  # У автоматических кампаний нет ozon_updated_at
                    'status': _translate_campaign_status(auto_status, is_manual=False),
                    'ozon_campaign_id': campaign.ozon_campaign_id or ''
                }
    
    # Логируем количество найденных автоматических кампаний
    logger.info(f"[ℹ️] Найдено {len(auto_campaigns_dict)} SKU с автоматическими кампаниями для магазина {store}")

    rows = []
    # Создаем словарь для быстрого поиска названий товаров по SKU
    sku_to_name_dict = {}
    for v in agg_qs:
        revenue = Decimal(str(v['revenue_sum'] or 0))
        units = int(v['units_sum'] or 0)
        avg_price = (revenue / units) if units else Decimal('0')
        avg_price = avg_price.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        
        # Сохраняем соответствие SKU -> название товара
        sku_to_name_dict[v['sku']] = v['offer_id'] or v['name']
        
        # Получаем информацию о кампании для текущего SKU
        sku = v['sku']
        campaign_name = ''
        management_type = ''
        last_update_date = ''
        campaign_status = ''
        
        if sku:
            # Проверяем сначала ручные кампании (приоритет)
            if sku in manual_campaigns_dict:
                campaign_info = manual_campaigns_dict[sku]
                campaign_name = campaign_info['name']
                management_type = campaign_info['type']
                campaign_status = campaign_info['status']
                if campaign_info['ozon_updated_at']:
                    last_update_date = campaign_info['ozon_updated_at'].strftime('%d-%m-%Y')
            # Если ручных нет, проверяем автоматические
            elif sku in auto_campaigns_dict:
                campaign_info = auto_campaigns_dict[sku]
                campaign_name = campaign_info['name']
                management_type = campaign_info['type']
                campaign_status = campaign_info['status']
                # У автоматических кампаний нет даты обновления из Ozon
        
        # Формируем строку: [Артикул, SKU, Продажи руб., Продажи шт., Цена товара, ABC, Название РК, Тип управления, Дата обновления, Статус]
        rows.append([
            v['offer_id'] or v['name'],  # A: Артикул
            v['sku'],                    # B: SKU
            float(revenue),              # C: Продажи, руб.
            units,                       # D: Продажи, шт.
            float(avg_price),            # E: Цена товара, руб.
            '',                          # F: ABC (будет заполнено позже)
            campaign_name,               # G: Название рекламной кампании
            management_type,             # H: Тип управления (Р/А)
            last_update_date,            # I: Дата последнего обновления в Ozon
            campaign_status              # J: Статус кампании
        ])
    t_agg = time.perf_counter(); logger.info(f"[⏱] Подготовка строк из результатов БД: {t_agg - t_qs:.3f}s (rows={len(rows)})")

    # Больше не создаем AdPlanRequest - работаем только с AdPlanItem
    logger.info(f"[ℹ️] Подготовка завершена, переходим к обработке данных")

    # Заполняем сводные значения в Main_ADV (B4, B6..D8, E8)
    try:
        from datetime import datetime as _dt
        def _to_int(val: Decimal) -> int:
            return int(val.to_integral_value(rounding=ROUND_HALF_UP))

        # Основные суммы по категориям
        a_total = total_revenue * a_share
        b_total = total_revenue * b_share
        c_total = total_revenue * c_share

        # Месяц/неделя/день
        a_week = a_total / Decimal('4')
        a_day = a_week / Decimal('7')
        b_week = b_total / Decimal('4')
        b_day = b_week / Decimal('7')
        c_week = c_total / Decimal('4')
        c_day = c_week / Decimal('7')

        ws_main.update('B4', [[float(total_revenue)]])
        # Рекламный бюджет за +1 период: total_revenue * promo_budget_pct (уже доля 0..1)
        budget_total = total_revenue * promo_budget_pct

        # Если нужно, учитываем уже потраченный бюджет с начала текущего месяца
        if int(consider_spent or 0) == 1:
            try:
                from .models import CampaignPerformanceReportEntry
                # Начало текущего месяца (1-е число)
                since_date_consider = timezone.localdate().replace(day=1)
                spent_sum = Decimal('0')
                for _e in CampaignPerformanceReportEntry.objects.filter(
                    store=store,
                    report_date__gte=since_date_consider,
                    report_date__lte=timezone.localdate(),
                ).iterator():
                    _tot = _e.totals or {}
                    s = str(_tot.get('moneySpent') or '').replace('\u00A0','').replace('\u202F','').replace(' ','').replace(',', '.')
                    try:
                        spent_sum += Decimal(s)
                    except Exception:
                        continue
                logger.info(f"[♻️] Учитываем уже потраченное с {since_date_consider}: {spent_sum}")
                budget_total = max(Decimal('0'), budget_total - spent_sum)
            except Exception as _e:
                logger.warning(f"[⚠️] Не удалось учесть потраченный бюджет: {_e}")
        # Расчёт недельного/дневного бюджета
        # Если consider_spent == 1, то учитываем уже потраченное, распределяем остаток на оставшиеся дни месяца,
        # иначе используем месячную схему (делим на 4 недели)
        if int(consider_spent or 0) == 1:
            today = timezone.localdate()
            #Получаем полную дату: первое число следующего месяца
            next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
            # получаем последний день текущего месяца
            end_of_month = next_month - timedelta(days=1)
            # days_left, сколько дней осталось до конца месяца, включая текущий день
            # Например: сегодня 20 сентября, конец месяца 30 сентября days_left = 10 + 1 = 11
            days_left = (end_of_month - today).days + 1
            if days_left <= 0:
                days_left = 1
            budget_total_ONE_DAY = (budget_total / Decimal(str(days_left))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            budget_total_ONE_WEEK = (budget_total_ONE_DAY * Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            logger.info(f"[📆] consider_spent=1: days_left={days_left}; week={budget_total_ONE_WEEK}, day={budget_total_ONE_DAY}")
        else:
            budget_total_ONE_WEEK = budget_total / Decimal('4')
            budget_total_ONE_DAY = budget_total_ONE_WEEK / Decimal('7')
        
        # Сохраняем недельный бюджет ДО корректировки для записи в C6
        budget_total_ONE_WEEK_original = budget_total_ONE_WEEK
        
        # T25: Учитывать бюджет ручных РК при создании (0 - не учитывать, 1 - учитывать)
        manual_budget_sum = Decimal('0')
        if consider_manual_budget == 1:
            # Получаем суммарный недельный бюджет ручных кампаний со статусами RUNNING и STOPPED
            manual_budget_sum = ManualCampaign.objects.filter(
                store=store,
                state__in=[
                    ManualCampaign.CAMPAIGN_STATE_RUNNING,
                    ManualCampaign.CAMPAIGN_STATE_STOPPED
                ]
            ).aggregate(total_budget=Sum('week_budget'))['total_budget'] or Decimal('0')
            
            logger.info(f"[💰] Суммарный недельный бюджет ручных кампаний (RUNNING/STOPPED): {manual_budget_sum}")
            logger.info(f"[💰] Недельный бюджет до корректировки: {budget_total_ONE_WEEK}")
            
            # Записываем сумму ручных кампаний в ячейку C8
            ws_main.update('C8', [[float(manual_budget_sum)]])
            
            # Уменьшаем доступный бюджет на сумму ручных кампаний
            budget_total_ONE_WEEK = max(Decimal('0'), budget_total_ONE_WEEK - manual_budget_sum)
            budget_total_ONE_DAY = budget_total_ONE_WEEK / Decimal('7')
            
            logger.info(f"[💰] Недельный бюджет после корректировки: {budget_total_ONE_WEEK}")
            
            if budget_total_ONE_WEEK <= 0:
                logger.warning(f"[⚠️] После учета ручных кампаний недельный бюджет стал <= 0. Создание автоматических кампаний невозможно.")
                return
        else:
            # Если T25=0, все равно записываем 0 в ячейку C8 для очистки
            ws_main.update('C8', [[0]])
        # plan_request больше нет - просто логируем бюджет
        logger.info(f"[💰] Общий бюджет: {budget_total}")
        ws_main.update('B5', [[_to_int(budget_total)]])
        ws_main.update('B6', [[_to_int(budget_total)]])
        ws_main.update('C6', [[_to_int(budget_total_ONE_WEEK_original)]])  # Недельный бюджет ДО корректировки
        ws_main.update('D6', [[_to_int(budget_total_ONE_DAY)]])

        ws_main.update('E4', [[_dt.now().strftime('%d/%m/%y')]])
        ws_main.update('E5', [[_dt.now().strftime('%d/%m/%y')]])        
        ws_main.update('E6', [[_dt.now().strftime('%d/%m/%y')]])

    except Exception as e:
        logger.error(f"[❌] Ошибка при обновлении Main_ADV сводных полей: {e}")

    # Фиксируем время на сводные обновления и задаём опорную точку перед ABC
    t_after_main = time.perf_counter(); logger.info(f"[⏱] Обновление сводных (Main_ADV): {t_after_main - t_agg:.3f}s")
    # Сортировка уже выполнена в БД
    t_sort = t_after_main; logger.info(f"[⏱] Сортировка: {t_sort - t_after_main:.3f}s")

    # ABC по выручке: кумулятив по float с тонким логированием суб-этапов
    _abc_t0 = time.perf_counter()
    total_revenue_float = float(total_revenue)
    a_cap = total_revenue_float * float(a_share)
    ab_cap = a_cap + total_revenue_float * float(b_share)
    logger.info(f"Итого выручка: {total_revenue_float}")
    logger.info(f"Целевая сумма A: {a_cap}")
    logger.info(f"Целевая сумма B: {ab_cap - a_cap}")
    logger.info(f"Целевая сумма C: {total_revenue_float - ab_cap}")
    _abc_t1 = time.perf_counter()

    # Быстрый префикс-суммы
    revs = [float(r[2]) for r in rows]
    _abc_t2 = time.perf_counter()
    cum = 0.0
    cum_sums = [0.0] * len(revs)
    for i in range(len(revs)):
        cum += revs[i]
        cum_sums[i] = cum
    _abc_t3 = time.perf_counter()
    labels = ['C'] * len(rows)
    for i, cs in enumerate(cum_sums):
        if cs <= a_cap:
            labels[i] = 'A'
        elif cs <= ab_cap:
            labels[i] = 'B'
    for i in range(len(rows)):
        rows[i][5] = labels[i]  # Устанавливаем ABC метку в позицию F (индекс 5)
    _abc_t4 = time.perf_counter()
    logger.info(f"[⏱] ABC substeps: caps={_abc_t1-_abc_t0:.3f}s, revs={_abc_t2-_abc_t1:.3f}s, cum={_abc_t3-_abc_t2:.3f}s, label={_abc_t4-_abc_t3:.3f}s")

    t_abc = time.perf_counter(); logger.info(f"[⏱] Расчёт ABC и присвоение категорий: {t_abc - t_sort:.3f}s")


    # Пишем на лист ABC: сначала шапка, затем блок данных явно в диапазон A2:J...
    ws_abc = sh.worksheet('ABC')
    header = ['Артикул', 'SKU', 'Продажи, руб.', 'Продажи, шт.', 'Цена товара, руб.', 'ABC', 'Название рекламной кампании', 'Тип управления', 'Дата последнего обновления в Ozon', 'Статус']
    # Перезапишем шапку на всякий случай
    ws_abc.update('A1:J1', [header], value_input_option='USER_ENTERED')
    # Очистим только тело
    ws_abc.batch_clear(['A2:J10000'])
    if rows:
        end_row = 1 + len(rows)  # начиная со 2-й строки
        ws_abc.update(f'A2:J{end_row}', rows, value_input_option='USER_ENTERED')
    t_write_abc = time.perf_counter(); logger.info(f"[⏱] Запись на лист ABC: {t_write_abc - t_abc:.3f}s")
    # Раскраска по ABC: группируем смежные диапазоны и применяем за минимальное число операций
    a_fmt = CellFormat(backgroundColor=Color(0.0118, 1.0, 0.0))
    b_fmt = CellFormat(backgroundColor=Color(1.0, 1.0, 0.0))
    c_fmt = CellFormat(backgroundColor=Color(1.0, 0.0, 0.0))
    values = ws_abc.col_values(6)[1:]  # без заголовка (колонка F - ABC)
    formats = []
    def add_run(start_idx, end_idx, fmt):
        if start_idx is None:
            return
        formats.append((f'F{start_idx}:F{end_idx}', fmt))
    # собираем последовательности для каждой метки
    current_label = 'None'
    run_start = None
    for i, val in enumerate(values, start=2):
        if val != current_label:
            # завершить предыдущую
            if current_label == 'A':
                add_run(run_start, i-1, a_fmt)
            elif current_label == 'B':
                add_run(run_start, i-1, b_fmt)
            elif current_label == 'C':
                add_run(run_start, i-1, c_fmt)
            # начать новую, если валидная
            current_label = val if val in ('A','B','C') else None
            run_start = i if current_label else None
    # fin
    if current_label == 'A':
        add_run(run_start, len(values)+1, a_fmt)
    elif current_label == 'B':
        add_run(run_start, len(values)+1, b_fmt)
    elif current_label == 'C':
        add_run(run_start, len(values)+1, c_fmt)
    if formats:
        format_cell_ranges(ws_abc, formats)
    t_format = time.perf_counter(); logger.info(f"[⏱] Форматирование листа ABC: {t_format - t_write_abc:.3f}s")



    # ------------------------------
    # TOP-N: фильтр по цене по avg_price и пропорциональное распределение
    # ------------------------------
    
    # Инициализируем список SKU с существующими ручными кампаниями
    existing_campaigns_rows = []
    
    try:
        
        logger.info(f" Минимальный бюджет (T22): {min_budget}")
        if min_budget <= 0:
            raise ValueError('Минимальный бюджет (T22) должен быть > 0')
        
        # Читаем список исключений из столбца Y
        exclusion_offer_ids = set()
        try:
            # Читаем все значения из столбца Y, начиная с Y13
            # col_values возвращает список строк для всего столбца
            # Столбец Y имеет индекс 25. Нам нужны строки с 13-й, что соответствует индексу 12 в 0-индексированном списке
            raw_exclusions = ws_main.col_values(25)[12:]  # Y13 и далее
            for item in raw_exclusions:
                item = item.strip()
                if item:  # Обрабатываем только непустые элементы
                    # Исключения - это offer_id (артикулы товаров)
                    exclusion_offer_ids.add(item)
        except Exception as e:
            logger.error(f"[❌] Ошибка при чтении списка исключений из Main_ADV!Y: {e}")
        
        logger.info(f"[ℹ️] Список исключений (offer_id) из Main_ADV!Y: {exclusion_offer_ids}")
        
        n_max = int((budget_total_ONE_WEEK // min_budget)) if min_budget > 0 else 0
        if 'max_items' in locals() and max_items and max_items > 0:
            n_max = int(max_items)+1
            logger.info(f"[ℹ️] max_items задан: используем n_max={n_max}")
        else:
            logger.info(f"[✅] Сколько товаров можно прокормить: {n_max}")
        t_topn_start = time.perf_counter()
        logger.info(f"[⏱] Подготовка к TOP-N (параметры): {t_topn_start - t_format:.3f}s")

        # Отбор TOP-N: по выручке, используем avg_price из rows[4]
        selected = []
        # r[0] — offer_id или name
        # r[1] — sku (int)
        # r[2] — revenue, суммарная выручка (float)
        # r[3] — units, суммарное кол-во (int)
        # r[4] — avg_price, средняя цена (float)
        for r in rows:
            if len(selected) >= n_max:
                break
            offer_id = r[0]  # offer_id находится по индексу 0            
            sku = r[1]  # SKU товара            
            # Проверяем, не находится ли товар в списке исключений
            if offer_id in exclusion_offer_ids:
                logger.info(f"[🚫] offer_id '{offer_id}' находится в списке исключений, пропускаем.")
                continue
            
            # T24: Добавлять товар, если уже есть РК (0 - не добавлять, 1 - добавлять)
            if add_existing_campaigns == 0:
                # Проверяем, есть ли уже кампания для этого SKU
                has_manual_campaign = sku in manual_campaigns_dict if sku else False
                has_auto_campaign = sku in auto_campaigns_dict if sku else False
                
                if has_auto_campaign:
                    logger.info(f"[ℹ️] SKU {sku} уже имеет автоматическую кампанию, добавляем в selected с доп. полями")
                    # Получаем информацию об автоматической кампании
                    campaign_info = auto_campaigns_dict.get(sku, {})
                    campaign_name = campaign_info.get('name', 'Неизвестная кампания')
                    campaign_status = campaign_info.get('status', 'Неизвестный статус')
                    ozon_campaign_id = campaign_info.get('ozon_campaign_id', '')
                    
                    # Определяем, включена ли кампания (1) или выключена (0)
                    # Кампания считается включенной, если статус 'Активна' или 'Запущена'
                    is_enabled = 1 if campaign_status in ['Активна', 'Запущена'] else 0
                    
                    # Добавляем в список существующих кампаний для отображения
                    existing_campaigns_rows.append([
                        ozon_campaign_id, # A: ID кампании (ozon_campaign_id)
                        is_enabled,       # B: Включена (1) / выключена (0)
                        campaign_status,  # C: Статус в Ozon
                        offer_id,         # D: Артикул
                        sku,              # E: SKU
                        float(r[2]),      # F: Продажи, руб.
                        r[3],             # G: Продажи, шт.
                        float(r[4]),      # H: Цена товара, руб.
                        r[5] if len(r) > 5 else '',  # I: ABC
                        'Авто',           # J: Тип управления
                        '',               # K: Дата обновления (будет заполнена позже)
                        campaign_status   # L: Статус кампании
                    ])
                    
                    # Добавляем товар в selected с дополнительными полями от автоматической кампании
                    # Расширяем строку r дополнительными полями
                    extended_r = r + [
                        campaign_name,    # G: Название рекламной кампании
                        'Авто',           # H: Тип управления
                        '',               # I: Дата последнего обновления в Ozon
                        campaign_status   # J: Статус кампании
                    ]
                    selected.append(extended_r)
                    logger.info(f"[📋] Добавлен SKU {sku} с автоматической кампанией '{campaign_name}' в selected (статус: {campaign_status}, включена: {is_enabled})")
                    continue
                if has_manual_campaign:
                    logger.info(f"[ℹ️] SKU {sku} уже имеет рекламную кампанию, добавляем в список существующих")
                    # Получаем информацию о кампании
                    campaign_info = manual_campaigns_dict.get(sku, {})
                    campaign_name = campaign_info.get('name', 'Неизвестная кампания')
                    campaign_status = campaign_info.get('status', 'Неизвестный статус')
                    
                    # Добавляем в список существующих кампаний
                    existing_campaigns_rows.append([
                        campaign_name,    # A: ID кампании (название)
                        '',               # B: Пустота для ручных кампаний
                        campaign_status,  # C: Статус в Ozon
                        offer_id,         # D: Артикул
                        sku,              # E: SKU
                        float(r[2]),      # F: Продажи, руб.
                        r[3],             # G: Продажи, шт.
                        float(r[4]),      # H: Цена товара, руб.
                        r[5] if len(r) > 5 else '',  # I: ABC
                        'Ручное',         # J: Тип управления
                        '',               # K: Дата обновления (будет заполнена позже)
                        campaign_status   # L: Статус кампании
                    ])
                    logger.info(f"[📋] Добавлен SKU {sku} с существующей кампанией '{campaign_name}' (статус: {campaign_status})")
                    continue
                
            avg_price_val = Decimal(str(r[4])) if len(r) > 4 and r[4] is not None else Decimal('0')
            if price_min and price_min > 0 and avg_price_val < price_min:
                continue
            if price_max and price_max > 0 and avg_price_val > price_max:
                continue
            
            # Проверяем остатки FBS и FBO, если настройки заданы
            if min_fbs_stock > 0 or min_fbo_stock > 0:
                fbs_stock = fbs_by_sku.get(sku, 0)
                fbo_stock = fbo_by_sku.get(sku, 0)
                logger.info(f"[ℹ️] SKU {sku} проверяем остатки FBS = {fbs_stock} FBO = {fbo_stock}")
                # Если остатки отсутствуют, пропускаем товар
                
                if min_fbs_stock > 0 and fbs_stock < min_fbs_stock:
                    logger.info(f"[🚫] SKU {sku} исключен: остаток FBS {fbs_stock} < минимального {min_fbs_stock}")
                    continue
                    
                if min_fbo_stock > 0 and fbo_stock < min_fbo_stock:
                    logger.info(f"[🚫] SKU {sku} исключен: остаток FBO {fbo_stock} < минимального {min_fbo_stock}")
                    continue
            
            selected.append(r)
        t_select = time.perf_counter(); logger.info(f"[⏱] Отбор TOP-N: {t_select - t_topn_start:.3f}s (selected={len(selected)})")

        # Если max_items > 0, перерасчёт: используем первые max_items (n_max уже равен max_items)
        if selected and max_items and max_items > 0:
            selected = selected[:int(max_items)]
        # for t_data in selected:
        #     logger.info(t_data)
        # Пропорциональное распределение по выбранным товарам:
        # Считаем общую выручку всех отобранных товаров (для пропорционального распределения)
        selected_total_revenue = sum(Decimal(str(r[2])) for r in selected) if selected else Decimal('0')
        out_rows = []  # Список строк для записи в таблицу
        items_to_save = []  # Список товаров для сохранения в базу данных
        campaign_names = []  # Столбец C: «Артикул товара + дата создания»
        sum_week = Decimal('0')  # Сумма всех недельных бюджетов
        logger.info(f"selected_total_revenue = {selected_total_revenue}")
        for r in selected:
            offer_or_name = r[0]  # Артикул или название товара
            sku = r[1]  # SKU товара
            revenue_val = r[2]  # Выручка товара
            # units = r[3]  # не используется здесь
            revenue_dec = Decimal(str(revenue_val))  # Конвертируем выручку в Decimal для точных вычислений
            
            # Режим распределения: 0 — равномерно, 1 — по весу (выручке)
            share = Decimal('0')  # для безопасного логирования, если распределение равномерное
            if 'budget_mode' in locals() and budget_mode == 0 and selected:
                amount = (budget_total_ONE_WEEK / Decimal(len(selected)))  # недельный бюджет на товар (равномерно)
            elif selected_total_revenue > 0:
                share = (revenue_dec / selected_total_revenue)  # Доля выручки товара от общей выручки
                amount = budget_total_ONE_WEEK * share  # недельный бюджет на товар (пропорционально выручке)
            else:
                amount = (budget_total_ONE_WEEK / Decimal(len(selected))) if selected else Decimal('0')  # Fallback на равномерное
                
            # Считаем недельный бюджет напрямую из amount и округляем до 2 знаков после запятой
            week_amt = amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            # Минималка по неделе: если бюджет меньше минимального, устанавливаем минимальный
            if week_amt < min_budget:
                week_amt = Decimal(str(min_budget)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
            # Контроль суммы: если превышаем недельный бюджет — прекращаем и не добавляем текущий товар
            if (sum_week + week_amt) > budget_total_ONE_WEEK+1:
                logger.info("[⛔] Сумма недельных бюджетов превысила недельный бюджет, остановка подбора TOP-N")
                break
            logger.info(f"sum_week = {sum_week} | week_amt = {week_amt} | share = {round(share*100,3)} | r = {r}")

            sum_week += week_amt  # Добавляем к общей сумме недельных бюджетов
            day_amt = (week_amt / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # Считаем дневной бюджет: недельный делим на 7 дней и округляем до 2 знаков
            
            # Формируем название для колонки D:
            # Приоритет: ручная кампания -> автоматическая -> артикул + дата
            if sku in manual_campaigns_dict and manual_campaigns_dict[sku].get('name'):
                campaign_name_with_status = manual_campaigns_dict[sku]['name']
            elif sku in auto_campaigns_dict and auto_campaigns_dict[sku].get('name'):
                campaign_name_with_status = auto_campaigns_dict[sku]['name']
            else:
                campaign_name_with_status = f"{offer_or_name} {_dt.now().strftime('%d/%m/%y')}"
            
            # Если есть ручная кампания, добавляем статус
            if sku in manual_campaigns_dict:
                manual_campaign = ManualCampaign.objects.filter(store=store, sku=sku).first()
                if manual_campaign:
                    status_russian = _translate_campaign_status(manual_campaign.state)
            
            campaign_names.append([campaign_name_with_status])  # D: формируем название кампании со статусом
            # Получаем недельный бюджет ручной кампании, если есть
            manual_week_budget = ''
            if sku in manual_campaigns_dict:
                manual_campaign = ManualCampaign.objects.filter(store=store, sku=sku).first()
                if manual_campaign:
                    manual_week_budget = float(manual_campaign.week_budget)
            
            # Получаем название товара по SKU из словаря
            product_name = sku_to_name_dict.get(sku, offer_or_name)
            
            out_rows.append([
                product_name,  # F: Название товара (артикул)
                int(sku),  # G: SKU товара
                float(week_amt),  # H: Недельный бюджет (с 2 знаками после запятой)
                manual_week_budget,  # I: Недельный бюджет ручной кампании
                float(day_amt),  # J: Дневной бюджет (с 2 знаками после запятой)
            ])
            items_to_save.append((int(sku), str(product_name), week_amt, day_amt))  # Сохраняем для записи в базу
        
        # Проверяем разницу между суммой бюджетов и целевым бюджетом
        budget_diff = abs(sum_week - budget_total_ONE_WEEK)
        logger.info(f"[ℹ️] Сумма бюджетов: {sum_week}, Целевой бюджет: {budget_total_ONE_WEEK}, Разница: {budget_diff}")
        
        # T26: Пересчитывать бюджет с учетом изменений (0 - не пересчитывать, 1 - пересчитывать)
        # Если разница больше 5 рублей и включен пересчет, пересчитываем бюджеты пропорционально
        if  budget_diff > Decimal('5') and sum_week > 0:
            logger.info(f"[🔄] Разница больше 5 рублей, пересчитываем бюджеты")
            # Вычисляем коэффициент корректировки
            correction_factor = budget_total_ONE_WEEK / sum_week
            logger.info(f"[ℹ️] Коэффициент корректировки: {correction_factor}")
            
            # Пересчитываем бюджеты и обновляем списки
            new_out_rows = []
            new_items_to_save = []
            new_sum_week = Decimal('0')
            
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i) in enumerate(items_to_save):
                # Пересчитываем недельный бюджет
                new_week_amt = (week_amt_i * correction_factor).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                # Пересчитываем дневной бюджет
                new_day_amt = (new_week_amt / Decimal('7')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                
                new_sum_week += new_week_amt
                
                # Обновляем строку для таблицы
                new_out_rows.append([
                    out_rows[i][0],  # F: Артикул товара
                    out_rows[i][1],  # G: SKU товара
                    float(new_week_amt),  # H: Новый недельный бюджет
                    out_rows[i][3] if len(out_rows[i]) > 3 else 0.0,  # I: Недельный бюджет ручной кампании (не изменяется)
                    float(new_day_amt),  # J: Новый дневной бюджет
                ])
                
                # Обновляем данные для сохранения в БД
                new_items_to_save.append((sku_i, offer_id_i, new_week_amt, new_day_amt))
            
            # Заменяем старые данные новыми
            out_rows = new_out_rows
            items_to_save = new_items_to_save
            sum_week = new_sum_week
            
            logger.info(f"[✅] Бюджеты пересчитаны. Новая сумма: {sum_week}")
        # elif recalc_budget_changes == 0 and budget_diff > Decimal('5'):
        #     logger.info(f"[ℹ️] Пересчет бюджетов отключен (T26=0), оставляем как есть")
        
        t_alloc = time.perf_counter(); logger.info(f"[⏱] Распределение бюджета TOP-N: {t_alloc - t_select:.3f}s (итого_неделя={sum_week})")
        logger.info(f"[📋] Собрано SKU с существующими кампаниями: {len(existing_campaigns_rows)}")

        # Добавляем ВСЕ товары из ручных кампаний в конец списка для отображения
        existing_campaigns_added = 0
        logger.info(f"[📋] Добавляем ВСЕ товары из ручных кампаний в конец списка")
        
        # Получаем все ручные кампании с нужными статусами
        manual_campaigns = ManualCampaign.objects.filter(
            store=store,
            state__in=[
                'CAMPAIGN_STATE_RUNNING',
                'CAMPAIGN_STATE_STOPPED'
            ]
        ).select_related('store')
        
        # Проходим по каждой кампании и выводим ВСЕ товары в ней
        for campaign in manual_campaigns:
            campaign_name = campaign.name
            campaign_status = _translate_campaign_status(campaign.state)
            
            # Собираем все SKU из кампании (основной + из списка)
            all_skus_in_campaign = []
            
            # Добавляем основной SKU
            if campaign.sku:
                all_skus_in_campaign.append(campaign.sku)
            
            # Добавляем все SKU из sku_list
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                for sku_item in campaign.sku_list:
                    if sku_item and sku_item not in all_skus_in_campaign:
                        all_skus_in_campaign.append(sku_item)
            
            # Выводим каждый SKU из кампании
            for sku in all_skus_in_campaign:
                # Получаем название товара по SKU из словаря
                product_name = sku_to_name_dict.get(sku, f"SKU_{sku}")
                
                # Формируем название кампании без даты для колонки D (ручные — без даты)
                campaign_name_no_date = campaign_name
                
                # Бюджеты кампании: неделя/день (если не заданы — 0)
                manual_week_budget_val = float(campaign.week_budget or 0)
                manual_day_budget_val = float(campaign.daily_budget or 0)

                # Добавляем в out_rows с бюджетом кампании в колонке J (дневной)
                out_rows.append([
                    product_name,  # F: Название товара (артикул)
                    int(sku),     # G: SKU товара
                    0.0,                      # H: Недельный бюджет (не распределяем для существующих кампаний)
                    manual_week_budget_val,   # I: Недельный бюджет ручной кампании
                    manual_day_budget_val,    # J: Дневной бюджет кампании (требование)
                ])
                
                # Добавляем в campaign_names без даты
                campaign_names.append([campaign_name_no_date])
                
                # Добавляем в items_to_save с нулевыми бюджетами
                items_to_save.append((int(sku), product_name, Decimal('0'), Decimal('0')))
                
                existing_campaigns_added += 1
                logger.info(f"[📋] Добавлен SKU {sku} из кампании '{campaign_name}' (название: {product_name}, статус: {campaign_status})")
        
        logger.info(f"[📋] Добавлено товаров из ручных кампаний в конец списка: {existing_campaigns_added}")

        start_row = 13
        ws_main.batch_clear([f'A{start_row}:L1000'])  # Очищаем включая столбец L

        # Словари остатков уже созданы выше
        if out_rows:
            # Заполняем столбец A (ID кампании), C (статус), E (тип управления) для существующих кампаний
            campaign_ids_col_a = []
            campaign_statuses_col_c = []
            campaign_types_col_e = []
            
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i) in enumerate(items_to_save):
                # Ищем кампанию, в которой находится этот SKU (включая sku_list)
                manual_campaign = ManualCampaign.objects.filter(
                    store=store,
                    state__in=[
                        'CAMPAIGN_STATE_RUNNING',
                        'CAMPAIGN_STATE_STOPPED'
                    ]
                ).filter(
                    models.Q(sku=sku_i) |  # Основной SKU
                    models.Q(sku_list__contains=[sku_i])  # SKU в списке
                ).first()
                
                if manual_campaign:
                    # SKU унаследовал campaign_id от кампании
                    campaign_id = manual_campaign.ozon_campaign_id
                    campaign_status = _translate_campaign_status(manual_campaign.state)
                    campaign_type = 'Ручная'
                elif sku_i in auto_campaigns_dict:
                    # Для автоматических кампаний
                    campaign_info = auto_campaigns_dict.get(sku_i, {})
                    campaign_id = campaign_info.get('ozon_campaign_id', '')
                    campaign_status = campaign_info.get('status', '')
                    campaign_type = 'Авто'
                else:
                    # Для товаров без кампаний - пустые значения
                    campaign_id = ''
                    campaign_status = ''
                    campaign_type = ''
                
                campaign_ids_col_a.append([campaign_id])
                campaign_statuses_col_c.append([campaign_status])
                campaign_types_col_e.append([campaign_type])
            
            # Записываем данные в Google Sheets
            if campaign_ids_col_a:
                ws_main.update(f'A{start_row}:A{start_row + len(campaign_ids_col_a) - 1}', campaign_ids_col_a)
            
            # Заполняем столбец B (активация): по умолчанию 1, для ручных - пустота, для автоматических - из модели
            activation_values = []
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i) in enumerate(items_to_save):
                # Проверяем, есть ли ручная кампания для этого SKU
                if sku_i in manual_campaigns_dict:
                    # Для ручных кампаний - пустота
                    activation_values.append([''])
                elif sku_i in auto_campaigns_dict:
                    # Для автоматических кампаний - берем состояние из модели
                    campaign_info = auto_campaigns_dict.get(sku_i, {})
                    campaign_status = campaign_info.get('status', 'Неизвестный статус')
                    # Кампания считается включенной, если статус 'Активна' или 'Запущена'
                    is_enabled = 1 if campaign_status in ['Активна', 'Запущена'] else 0
                    activation_values.append([is_enabled])
                else:
                    # По умолчанию - 1
                    activation_values.append([1])
            
            if activation_values:
                ws_main.update(f'B{start_row}:B{start_row + len(activation_values) - 1}', activation_values)
            
            # Заполняем столбец C (статус кампании)
            if campaign_statuses_col_c:
                ws_main.update(f'C{start_row}:C{start_row + len(campaign_statuses_col_c) - 1}', campaign_statuses_col_c)
            
            # Заполняем столбец D (Название кампании): «Артикул + дата»
            ws_main.update(f'D{start_row}:D{start_row + len(campaign_names) - 1}', campaign_names)
            
            # Заполняем столбец E (тип управления)
            if campaign_types_col_e:
                ws_main.update(f'E{start_row}:E{start_row + len(campaign_types_col_e) - 1}', campaign_types_col_e)
            
            # Заполняем столбцы F-G (артикул и SKU), H (остаток FBS), I (остаток FBO) и J-L (бюджеты)
            cols_FG = [[row[0], row[1]] for row in out_rows]
            # Остатки по порядку items_to_save
            fbs_col_H = [[int(fbs_by_sku.get(int(sku_i), 0))] for (sku_i, _offer, _w, _d) in items_to_save]
            fbo_col_I = [[int(fbo_by_sku.get(int(sku_i), 0))] for (sku_i, _offer, _w, _d) in items_to_save]
            cols_JKL = [[row[2], row[3], row[4]] for row in out_rows]
            ws_main.update(f'F{start_row}:G{start_row + len(out_rows) - 1}', cols_FG)
            if fbs_col_H:
                ws_main.update(f'H{start_row}:H{start_row + len(fbs_col_H) - 1}', fbs_col_H)
            if fbo_col_I:
                ws_main.update(f'I{start_row}:I{start_row + len(fbo_col_I) - 1}', fbo_col_I)
            ws_main.update(f'J{start_row}:L{start_row + len(out_rows) - 1}', cols_JKL)
        t_write_topn = time.perf_counter(); logger.info(f"[⏱] Запись блока TOP-N: {t_write_topn - t_alloc:.3f}s (строк={len(out_rows)})")

        # Сопоставим ABC-метку по SKU из rows
        abc_by_sku = {}
        for rr in rows:
            if len(rr) > 5:
                try:
                    abc_by_sku[int(rr[1])] = rr[5]
                except Exception:
                    continue


        
    except Exception as e:
        logger.error(f"[❌] Ошибка при формировании TOP-N: {e}")

    logger.info(f"[✅] ABC обновлён за {date_from}..{date_to}. Строк: {len(rows)}")
    
@shared_task(name="Чтение данных из Google Sheets и создание рекламной компании")
def create_or_update_AD(spreadsheet_url: str = None, sa_json_path: str = None, worksheet_name: str = "Main_ADV", start_row: int = 13, block_size: int = 100):
    """
    Читает данные из Google Sheets до тех пор, пока не встретит 5 пустых строк подряд.
    
    Args:
        spreadsheet_url: URL Google таблицы
        sa_json_path: Путь к файлу сервисного аккаунта
        worksheet_name: Название листа (по умолчанию "Main_ADV")
        start_row: Номер строки, с которой начинать чтение (по умолчанию 13)
        block_size: Размер блока для чтения (по умолчанию 100 строк)
    
    Returns:
        list: Массив строк с данными из таблицы
    """
    
    spreadsheet_url = spreadsheet_url or "https://docs.google.com/spreadsheets/d/1-_XS6aRZbpeEPFDyxH3OV0IMbl_GUUEysl6ZJXoLmQQ"
    sa_json_path = sa_json_path or "/workspace/ozon-469708-c5f1eca77c02.json"
    
    logger.info(f"[📖] Начинаем чтение данных из Google Sheets: {worksheet_name}, строка {start_row}")
    
    try:
        # Авторизация в Google Sheets
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        gc = gspread.authorize(creds)
        t0 = time.perf_counter()
        
        # Открываем таблицу и лист
        sh = gc.open_by_url(spreadsheet_url)
        ws = sh.worksheet(worksheet_name)

        t_open = time.perf_counter()
        logger.info(f"[⏱] Открытие таблицы: {t_open - t0:.3f}s")
        
        # Читаем данные блоками для оптимизации
        data_rows = []
        empty_row_count = 0
        current_row = start_row
        max_empty_rows = 5  # Остановиться после 5 пустых строк подряд
        # block_size передается как параметр
        
        logger.info(f"[📊] Начинаем чтение блоками по {block_size} строк с строки {start_row}")
        
        while empty_row_count < max_empty_rows:
            try:
                # Читаем блок строк A:L (с учётом новых колонок бюджетов J-L)
                end_row = current_row + block_size - 1
                block_range = f'A{current_row}:L{end_row}'
                
                t_block_start = time.perf_counter()
                block_data = ws.get(block_range)
                t_block_read = time.perf_counter()
                
                logger.debug(f"[📦] Блок {current_row}-{end_row}: чтение за {t_block_read - t_block_start:.3f}s")
                
                if not block_data:
                    # Весь блок пустой
                    empty_row_count += block_size
                    current_row += block_size
                    logger.debug(f"[⭕] Блок {current_row}-{end_row}: полностью пустой")
                    continue             
                
                # Обрабатываем каждую строку в блоке
                rows_with_data_in_block = 0
                consecutive_empty_in_block = 0
                
                for i, row_data in enumerate(block_data):
                    row_number = current_row + i
                    
                    # Дополняем до 12 столбцов если нужно
                    row_values = row_data[:]
                    while len(row_values) < 12:
                        row_values.append('')
                    
                    # Проверяем, есть ли хотя бы одно непустое значение
                    has_data = any(str(cell).strip() for cell in row_values)
                    
                    if has_data:
                        # Строка содержит данные
                        data_rows.append({
                            'row_number': row_number,
                            'campaign_id': row_values[0],           # A: ID Кампании
                            'active': row_values[1],               # B: ВКЛ.
                            'status': row_values[2],               # C: Статус
                            'campaign_name': row_values[3],        # D: Название кампании
                            'campaign_type': row_values[4],        # E: Тип кампании
                            'article': row_values[5],              # F: Артикул
                            'sku': row_values[6],                  # G: SKU
                            'week_budget': row_values[9],          # J: Бюджет на нед.
                            'manual_week_budget': row_values[10],  # K: Бюджет на нед. РУЧНОЙ
                            'day_budget': row_values[11],          # L: Бюджет на день, руб.
                        })
                        rows_with_data_in_block += 1
                        consecutive_empty_in_block = 0  # Сбрасываем счетчик пустых строк в блоке
                        empty_row_count = 0  # Сбрасываем общий счетчик пустых строк
                else:
                        # Строка пустая
                        consecutive_empty_in_block += 1
                
                # Обновляем счетчик пустых строк
                if rows_with_data_in_block == 0:
                    # Весь блок пустой
                    empty_row_count += block_size
                else:
                    # В блоке есть данные, но может быть пустые строки в конце
                    # Проверяем последние строки блока
                    empty_at_end = 0
                    for i in range(len(block_data) - 1, -1, -1):
                        row_values = block_data[i][:]
                        while len(row_values) < 10:
                            row_values.append('')
                        if not any(str(cell).strip() for cell in row_values):
                            empty_at_end += 1
                        else:
                            break
                    empty_row_count = empty_at_end
                
                current_row += len(block_data)
                logger.debug(f"[📦] Блок обработан: {rows_with_data_in_block} строк с данными, {consecutive_empty_in_block} пустых")
                
                # Если прочитали меньше строк чем ожидали, значит достигли конца листа
                if len(block_data) < block_size:
                    logger.info(f"[📄] Достигнут конец листа на строке {current_row}")
                    break
                
                # Защита от бесконечного цикла
                if current_row > start_row + 10000:
                    logger.warning(f"[⚠️] Достигнут лимит строк (10000), останавливаем чтение")
                    break
                    
            except Exception as e:
                logger.error(f"[❌] Ошибка при чтении блока начиная со строки {current_row}: {e}")
                # В случае ошибки переходим к следующему блоку
                current_row += block_size
                empty_row_count += block_size
        
        t_read = time.perf_counter()
        logger.info(f"[⏱] Чтение данных завершено: {t_read - t_open:.3f}s")
        logger.info(f"[📊] Прочитано строк с данными: {len(data_rows)}")
        logger.info(f"[📊] Последняя обработанная строка: {current_row - 1}")
        logger.info(f"[📊] Остановка: {empty_row_count} пустых строк подряд")
        
        # Получаем настройки из Google Sheets
        try:
            # Получаем название магазина из ячейки T23
            store_name_cell = ws.get('V23')[0][0] if ws.get('V23') and ws.get('V23')[0] else ''
            logger.info(f"[🏪] Название магазина из T23: '{store_name_cell}'")
            
            # Получаем время обучения из ячейки T17 (в днях)
            train_days_cell = ws.get('V17')[0][0] if ws.get('V17') and ws.get('V17')[0] else '0'
            try:
                train_days = int(train_days_cell) if train_days_cell else 0
            except (ValueError, TypeError):
                train_days = 0
            logger.info(f"[📅] Время обучения из V17: {train_days} дней")
            
            # Находим магазин в базе данных
            store = None
            if store_name_cell:
                try:
                    store = OzonStore.objects.get(name=store_name_cell)
                    logger.info(f"[✅] Магазин найден: {store}")
                except OzonStore.DoesNotExist:
                    logger.error(f"[❌] Магазин '{store_name_cell}' не найден в базе данных")
                    return data_rows
            else:
                logger.error(f"[❌] Ячейка T23 пустая - не указан магазин")
                return data_rows
        except Exception as e:
            logger.error(f"[❌] Ошибка при получении настроек из Google Sheets: {e}")
            return data_rows
        
        # Получаем токен один раз для всех операций
        from .utils import get_store_performance_token
        try:
            token_info = get_store_performance_token(store)
            access_token = token_info.get("access_token")
            if not access_token:
                logger.error(f"[❌] Не удалось получить access_token для магазина {store}")
                return data_rows
            logger.info(f"[🔑] Токен Performance API получен успешно для магазина {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка получения токена Performance API: {e}")
            return data_rows
        
        # Утилита: нормализация флага активности из ячейки B
        def _is_sheet_active(val: str):
            s = str(val or '').strip().lower()
            if s in ('1', 'true', 'да', 'вкл', 'on', 'включена'):
                return True
            if s in ('0', 'false', 'нет', 'выкл', 'off', 'выключена'):
                return False
            return None

        # Обрабатываем данные рекламных кампаний
        campaigns_created = 0
        campaigns_updated = 0
        campaigns_skipped = 0
        
        for ad_data in data_rows:
            # print(ad_data)
            # continue
            campaign_id = str(ad_data['campaign_id']).strip()
            
            if not campaign_id:
                # campaign_id пустое - создаем рекламу в Ozon
                try:
                    sku = str(ad_data['sku']).strip()
                    campaign_name = str(ad_data['campaign_name']).strip()
                    week_budget = ad_data['week_budget']
                    manual_week_budget = ad_data['manual_week_budget']
                    active = str(ad_data['active']).strip()  # Параметр из ячейки B
                    
                    # Проверяем, что все необходимые данные есть
                    if not sku or not campaign_name or not week_budget:
                        logger.warning(f"[⚠️] Строка {ad_data['row_number']}: пропущена из-за пустых данных (SKU: '{sku}', название: '{campaign_name}', бюджет: '{week_budget}')")
                        campaigns_skipped += 1
                        continue
                    
                    try:
                        # Нормализуем бюджет: убираем пробелы (вкл. неразрывные) и меняем запятую на точку
                        week_budget_str = str(week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.')
                        week_budget_float = float(week_budget_str)
                        if week_budget_float <= 0:
                            logger.warning(f"[⚠️] Строка {ad_data['row_number']}: пропущена из-за нулевого бюджета ({week_budget_float})")
                            campaigns_skipped += 1
                            continue
                    except (ValueError, TypeError):
                        logger.warning(f"[⚠️] Строка {ad_data['row_number']}: некорректный бюджет '{week_budget}'")
                        campaigns_skipped += 1
                        continue
                    
                    # Обрабатываем ручной бюджет
                    try:
                        manual_budget_str = str(manual_week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.') if manual_week_budget else '0'
                        manual_budget_float = float(manual_budget_str) if manual_budget_str else 0.0
                    except (ValueError, TypeError):
                        manual_budget_float = 0.0
                        logger.debug(f"[ℹ️] Строка {ad_data['row_number']}: некорректный ручной бюджет '{manual_week_budget}', устанавливаем 0")
                    
                    # Выбираем бюджет к созданию: если указан ручной бюджет > 0, используем его, иначе расчётный
                    used_week_budget = manual_budget_float if manual_budget_float and manual_budget_float > 0 else week_budget_float
                    logger.info(f"[🚀] Создаем кампанию для SKU {sku}: '{campaign_name}', бюджет: {used_week_budget} (источник: {'ручной' if (manual_budget_float and manual_budget_float>0) else 'расчетный'})")

                    resp = create_cpc_product_campaign(
                        access_token=access_token,
                        sku=int(sku),
                        campaign_name=campaign_name,
                        weekly_budget_rub=used_week_budget,
                        placement = "PLACEMENT_TOP_PROMOTION",
                        product_autopilot_strategy = "TOP_MAX_CLICKS",
                        auto_increase_percent = 0
                    )
                    
                    if resp and isinstance(resp, dict) and resp.get('campaign_id'):
                        # Кампания создана успешно, записываем ID в таблицу
                        try:
                            campaign_id = str(resp['campaign_id'])
                            row_number = ad_data['row_number']
                            cell_a = f'A{row_number}'
                            ws.update(cell_a, [[campaign_id]])
                            logger.info(f"[✅] Кампания создана для SKU {sku}: ID {campaign_id}, записано в ячейку {cell_a}")
                            # Проставляем тип кампании в колонке E — 'Авто'
                            try:
                                ws.update(f'E{row_number}', [["Авто"]])
                                logger.debug(f"[📝] Проставлен тип кампании 'Авто' в E{row_number}")
                            except Exception as e_type:
                                logger.warning(f"[⚠️] Не удалось проставить тип 'Авто' в E{row_number}: {e_type}")
                            
                            # Создаем запись в AdPlanItem для отслеживания этой кампании
                            try:
                                # Создаем AdPlanItem напрямую
                                ad_plan_item = AdPlanItem.objects.create(
                                    store=store,
                                    sku=int(sku),
                                    offer_id='',  # Пока не знаем offer_id
                                    name=campaign_name,
                                    week_budget=used_week_budget,
                                    day_budget=used_week_budget / 7,
                                    manual_budget=manual_budget_float,  # Ручной бюджет из столбца I
                                    train_days=train_days,
                                    abc_label='',
                                    has_existing_campaign=False,  # Это новая кампания
                                    ozon_campaign_id=campaign_id,
                                    campaign_name=campaign_name,
                                    campaign_type='CPC_PRODUCT',
                                    state=AdPlanItem.CAMPAIGN_STATE_PLANNED,  # Изначально запланирована
                                    google_sheet_row=row_number,
                                    is_active_in_sheets=(active == '1')  # Сохраняем статус активности из Google Sheets
                                )
                                
                                logger.info(f"[📝] Создана запись AdPlanItem для кампании {campaign_id} (SKU: {sku})")
                                
                            except Exception as db_error:
                                logger.error(f"[❌] Ошибка создания записи AdPlanItem для кампании {campaign_id}: {db_error}")
                            
                            # Проверяем параметр активации из ячейки B
                            if active == '1':
                                try:
                                    logger.info(f"[🔛] Активируем кампанию {campaign_id} (параметр B=1)")
                                    activate_response = activate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    logger.info(f"[✅] Кампания {campaign_id} активирована успешно")
                                    
                                    # Обновляем данные кампании из ответа API
                                    _update_campaign_from_ozon_response(ad_plan_item, activate_response)
                                    logger.info(f"[📝] Данные кампании {campaign_id} обновлены из ответа Ozon API")
                                    
                                except Exception as activate_error:
                                    logger.error(f"[❌] Ошибка активации кампании {campaign_id}: {activate_error}")
                            else:
                                logger.debug(f"[ℹ️] Кампания {campaign_id} не активируется (параметр B='{active}')")
                            
                            campaigns_created += 1
                        except Exception as update_error:
                            logger.error(f"[❌] Кампания создана (ID: {resp.get('campaign_id')}), но не удалось обновить ячейку A{ad_data['row_number']}: {update_error}")
                            campaigns_created += 1  # Кампания все равно создана
                    elif resp:
                        # Ответ есть, но нет campaign_id
                        logger.warning(f"[⚠️] Получен ответ для SKU {sku}, но нет campaign_id: {resp}")
                        campaigns_skipped += 1
                    else:
                        logger.error(f"[❌] Не удалось создать кампанию для SKU {sku}")
                        campaigns_skipped += 1
                        
                except Exception as e:
                    logger.error(f"[❌] Ошибка при создании кампании для строки {ad_data['row_number']}: {e}")
                    campaigns_skipped += 1
            else:
                # campaign_id не пустое - проверяем существующие кампании
                try:
                    # Проверяем сначала автоматические кампании (это целевая область функции)
                    auto_campaign = AdPlanItem.objects.filter(
                        store=store,
                        ozon_campaign_id=campaign_id
                    ).first()
                    
                    if auto_campaign:
                        # Проверяем дату создания и время обучения
                        from django.utils import timezone
                        from datetime import timedelta
                        
                        campaign_age_days = (timezone.now() - auto_campaign.created_at).days
                        logger.debug(f"[📅] Кампания {campaign_id} создана {campaign_age_days} дней назад, время обучения: {train_days} дней")
                        

                        try:
                            week_budget = ad_data['week_budget']
                            week_budget_str = str(week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.') if week_budget else '0'
                            week_budget_float = float(week_budget_str) if week_budget_str else 0.0
                            
                            if week_budget_float > 0:
                                logger.info(f"[🔄] Обновляем бюджет кампании {campaign_id}: {auto_campaign.week_budget} -> {week_budget_float}")
                                
                                # Обновляем бюджет через API Ozon
                                try:
                                    api_response = update_campaign_budget(
                                        access_token=access_token,
                                        campaign_id=campaign_id,
                                        weekly_budget_rub=week_budget_float
                                    )
                                    logger.info(f"[🌐] API Ozon: бюджет кампании {campaign_id} обновлен успешно")
                                    
                                    # Обновляем в базе данных только после успешного API вызова
                                    auto_campaign.week_budget = week_budget_float
                                    auto_campaign.day_budget = week_budget_float / 7
                                    auto_campaign.save(update_fields=['week_budget', 'day_budget'])
                                    
                                    logger.info(f"[✅] Бюджет кампании {campaign_id} обновлен в базе данных")
                                    campaigns_updated += 1  # Считаем как обновленную кампанию
                                    
                                except Exception as api_error:
                                    logger.error(f"[❌] Ошибка API при обновлении бюджета кампании {campaign_id}: {api_error}")
                                    # Не обновляем базу данных при ошибке API
                                    campaigns_skipped += 1
                            else:
                                logger.warning(f"[⚠️] Строка {ad_data['row_number']}: некорректный бюджет для обновления: {week_budget}")
                                campaigns_skipped += 1
                                
                        except Exception as update_error:
                            logger.error(f"[❌] Ошибка при обновлении бюджета кампании {campaign_id}: {update_error}")
                            campaigns_skipped += 1

                        # Гарантируем, что в колонке E указан тип 'Авто' для существующей авто-кампании
                        try:
                            ws.update(f"E{ad_data['row_number']}", [["Авто"]])
                            logger.debug(f"[📝] Обновлён тип кампании 'Авто' в E{ad_data['row_number']}")
                        except Exception as e_type2:
                            logger.warning(f"[⚠️] Не удалось обновить тип 'Авто' в E{ad_data['row_number']}: {e_type2}")

                        # 3. Активность по ячейке B: 0 — деактивировать, 1 — активировать
                        try:
                            desired = _is_sheet_active(ad_data.get('active'))
                            if desired is not None:
                                if desired:
                                    logger.info(f"[🔛] Активируем существующую кампанию {campaign_id} (B=1)")
                                    api_resp = activate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    # Обновляем модель по ответу; если state не пришёл — проставим ACTIVE
                                    if isinstance(api_resp, dict) and api_resp:
                                        _update_campaign_from_ozon_response(auto_campaign, api_resp)
                                    if not (isinstance(api_resp, dict) and api_resp.get('state')):
                                        auto_campaign.state = AdPlanItem.CAMPAIGN_STATE_ACTIVE
                                        auto_campaign.save(update_fields=['state'])
                                    campaigns_updated += 1
                                else:
                                    logger.info(f"[🔴] Деактивируем существующую кампанию {campaign_id} (B=0)")
                                    api_resp = deactivate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    if isinstance(api_resp, dict) and api_resp:
                                        _update_campaign_from_ozon_response(auto_campaign, api_resp)
                                    if not (isinstance(api_resp, dict) and api_resp.get('state')):
                                        auto_campaign.state = AdPlanItem.CAMPAIGN_STATE_INACTIVE
                                        auto_campaign.save(update_fields=['state'])
                                    campaigns_updated += 1
                        except Exception as act_err:
                            logger.error(f"[❌] Ошибка смены активности кампании {campaign_id} по B: {act_err}")

                    else:
                        # Если авто не нашли — проверяем, не ручная ли это кампания
                        manual_campaign = ManualCampaign.objects.filter(
                            store=store,
                            ozon_campaign_id=campaign_id
                        ).first()
                        if manual_campaign:
                            logger.debug(f"[⏭️] Строка {ad_data['row_number']}: найдено соответствие ManualCampaign (ID: {campaign_id}), пропускаем управление по B")
                            campaigns_skipped += 1
                        else:
                            # Кампания не найдена ни в ручных, ни в автоматических
                            logger.warning(f"[⚠️] Кампания {campaign_id} не найдена в базе данных (строка {ad_data['row_number']})")
                            campaigns_skipped += 1
                        
                except Exception as e:
                    logger.error(f"[❌] Ошибка при обработке существующей кампании {campaign_id} (строка {ad_data['row_number']}): {e}")
                    campaigns_skipped += 1
        
        # Останавливаем в Ozon те авто-кампании из модели, которых нет в новом списке таблицы
        try:
            # Раньше тут был фильтр по campaign_type == 'Авто', из-за пустых значений
            # в колонке E живые кампании ошибочно считались «отсутствующими в листе» и выключались.
            # Теперь берём любой непустой campaign_id из листа, а отбор «только авто» обеспечивается тем,
            # что ниже мы итерируемся только по AdPlanItem (авто) в базе.
            present_auto_ids = {
                str(row.get('campaign_id')).strip()
                for row in data_rows
                if str(row.get('campaign_id')).strip()
            }
            stopped_count = 0
            active_states = [
                AdPlanItem.CAMPAIGN_STATE_RUNNING,
                AdPlanItem.CAMPAIGN_STATE_ACTIVE,
                AdPlanItem.CAMPAIGN_STATE_PLANNED,
            ]
            # Берём все авто-кампании магазина с ID
            stale_ads = AdPlanItem.objects.filter(store=store).exclude(ozon_campaign_id='')
            for ad in stale_ads:
                cid = str(ad.ozon_campaign_id)
                if cid not in present_auto_ids:
                    try:
                        # Деактивируем через Performance API
                        deact_resp = deactivate_campaign(access_token=access_token, campaign_id=cid)
                        _update_campaign_from_ozon_response(ad, deact_resp)
                        ad.save(update_fields=['state', 'payment_type', 'total_budget', 'week_budget', 'day_budget', 'from_date', 'to_date', 'placement', 'product_autopilot_strategy', 'ozon_created_at', 'ozon_updated_at'])
                        stopped_count += 1
                        logger.info(f"[🛑] Отключили кампанию {cid}, отсутствует в листе")
                    except Exception as e:
                        logger.error(f"[❌] Ошибка деактивации кампании {cid}: {e}")
            if stopped_count:
                logger.info(f"[📉] Остановлено кампаний, отсутствующих в листе: {stopped_count}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при остановке кампаний, отсутствующих в листе: {e}")

        logger.info(f"[📊] Обработка завершена: создано {campaigns_created} кампаний, обновлено {campaigns_updated} кампаний, пропущено {campaigns_skipped}")
        return data_rows
        
    except Exception as e:
        logger.error(f"[❌] Ошибка при чтении данных из Google Sheets: {e}")
        return []

