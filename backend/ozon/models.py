from tabnanny import verbose
from django.db import models
from users.models import User, OzonStore

class Product(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='products')

    product_id = models.BigIntegerField(unique=True)
    sku = models.BigIntegerField(unique=True)
    offer_id = models.CharField(max_length=255)
    name = models.CharField(max_length=500, blank=True)  # Название товара
    barcodes = models.JSONField(default=list, blank=True)
    category = models.CharField(max_length=255, blank=True)  # Категория
    type_name = models.CharField(max_length=255, blank=True)  # Тип товара

    type_id = models.BigIntegerField(null=True, blank=True)  # Тип товара (для запроса типа)
    description_category_id = models.BigIntegerField(null=True, blank=True)


     # Цены
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
  

 # Статусы
    is_archived = models.BooleanField(default=False)
    is_autoarchived = models.BooleanField(default=False)
    is_discounted = models.BooleanField(default=False)
    is_kgt = models.BooleanField(default=False)
    is_super = models.BooleanField(default=False)
    is_seasonal = models.BooleanField(default=False)
    is_prepayment_allowed = models.BooleanField(default=False)
    
    # Фото
    primary_image = models.URLField(blank=True, null=True)
    def __str__(self):
        return f"{self.offer_id} ({self.name})"

# Модель для хранения категорий товаров    
class Category(models.Model):
    category_id = models.BigIntegerField(unique=True)  # description_category_id
    name = models.CharField(max_length=255)
    disabled = models.BooleanField(default=False)

    def __str__(self):
        return self.name

# Модель для хранения типов товаров
class ProductType(models.Model):
    type_id = models.BigIntegerField(unique=True)
    name = models.CharField(max_length=255)
    disabled = models.BooleanField(default=False)
    category = models.ForeignKey(Category, on_delete=models.CASCADE, related_name="types")

    def __str__(self):
        return self.name

# Модель для хранения остатков на складах
class WarehouseStock(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='wharehouse_stocks')
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="stocks", null=True, blank=True)

    sku = models.BigIntegerField()
    warehouse_id = models.BigIntegerField()
    warehouse_name = models.CharField(max_length=255)

    available_stock_count = models.IntegerField(default=0)
    valid_stock_count = models.IntegerField(default=0)
    waiting_docs_stock_count = models.IntegerField(default=0)
    expiring_stock_count = models.IntegerField(default=0)
    transit_defect_stock_count = models.IntegerField(default=0)
    stock_defect_stock_count = models.IntegerField(default=0)
    excess_stock_count = models.IntegerField(default=0)
    other_stock_count = models.IntegerField(default=0)
    requested_stock_count = models.IntegerField(default=0)
    transit_stock_count = models.IntegerField(default=0)
    return_from_customer_stock_count = models.IntegerField(default=0)

    cluster_id = models.BigIntegerField(null=True, blank=True)
    cluster_name = models.CharField(max_length=255, blank=True)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('store', 'sku', 'cluster_id', 'warehouse_id')
        verbose_name_plural = 'Отатки на складах'
        verbose_name = 'Отатки на складах'

    def __str__(self):
        return f"{self.warehouse_name} / SKU {self.sku} / {self.available_stock_count} шт."

# Модель для хранения продаж FBS+FBO
class Sale(models.Model):
    FBO = 'FBO'
    FBS = 'FBS'
    SALE_TYPE_CHOICES = [
        (FBO, 'FBO'),
        (FBS, 'FBS'),
    ]

    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='sales')
    sale_type = models.CharField(max_length=3, choices=SALE_TYPE_CHOICES)
    sku = models.BigIntegerField()
    date = models.DateTimeField()  # created_at / shipment_date
    quantity = models.IntegerField(default=0)

    price = models.DecimalField(max_digits=10, decimal_places=2)
    payout = models.DecimalField(max_digits=10, decimal_places=2)
    commission_amount = models.DecimalField(max_digits=10, decimal_places=2)

    warehouse_id = models.BigIntegerField(null=True, blank=True)
    cluster_from = models.CharField(max_length=255, blank=True)
    cluster_to = models.CharField(max_length=255, blank=True)

    status = models.CharField(max_length=100)

    customer_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)  # FBS only
    tpl_provider = models.CharField(max_length=255, null=True, blank=True)  # FBS only

    posting_number = models.CharField(max_length=255, null=True, blank=True)  # Номер отправки
    # Дополнительные поля

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.sale_type} / SKU {self.sku} / {self.quantity} шт. / {self.date.date()}"


class FbsStock(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='fbs_stocks')
    product_id = models.BigIntegerField()
    sku = models.BigIntegerField()
    fbs_sku = models.BigIntegerField()
    
    present = models.PositiveIntegerField(default=0)
    reserved = models.PositiveIntegerField(default=0)

    warehouse_id = models.BigIntegerField()
    warehouse_name = models.CharField(max_length=255)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "FBS остаток"
        verbose_name_plural = "FBS остатки"
        unique_together = ('store', 'sku', 'warehouse_id')

    def __str__(self):
        return f"{self.warehouse_name} / SKU {self.sku} / {self.present} шт."

# ОБЩАЯ АНАЛИТИКА ПО КЛАСТЕРУ
class DeliveryCluster(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name="delivery_clusters")
    delivery_cluster_id = models.PositiveIntegerField()
    name = models.CharField(max_length=100)
    type = models.CharField(max_length=50)

    average_delivery_time = models.FloatField()
    impact_share = models.FloatField()
    lost_profit = models.DecimalField(max_digits=12, decimal_places=2)
    recommended_supply = models.PositiveIntegerField()

    class Meta:
        unique_together = ("store", "delivery_cluster_id")  # ✅ уникальность только в рамках магазина
        verbose_name = "Кластер доставки"
        verbose_name_plural = "Кластеры доставки"

    def __str__(self):
        return f"{self.name} ({self.delivery_cluster_id})"

# ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ
class DeliveryClusterItemAnalytics(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name="cluster_item_analytics")
    cluster_id = models.PositiveIntegerField()
    cluster_name = models.CharField(max_length=100)
    sku = models.BigIntegerField()
    offer_id = models.CharField(max_length=100)
    delivery_schema = models.CharField(max_length=10)

    average_delivery_time = models.FloatField()
    average_delivery_time_status = models.CharField(max_length=20)
    impact_share = models.FloatField()
    attention_level = models.CharField(max_length=20)
    recommended_supply = models.PositiveIntegerField()
    recommended_supply_FBO = models.PositiveIntegerField(null=True, blank=True)
    recommended_supply_FBS = models.PositiveIntegerField(null=True, blank=True)


    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("store", "cluster_id", "sku")
        verbose_name = "Аналитика по кластеру"
        verbose_name_plural = "Аналитика по кластерам"

    def __str__(self):
        return f"{self.store} | {self.sku} | {self.cluster_name}"
    
    
 # Модель для хранения сводной аналитики по доставке   
class DeliveryAnalyticsSummary(models.Model):
    store = models.OneToOneField(OzonStore, on_delete=models.CASCADE, related_name='delivery_summary')

    average_delivery_time = models.FloatField()
    average_delivery_time_status = models.CharField(max_length=50)
    total_orders = models.IntegerField()

    lost_profit = models.DecimalField(max_digits=15, decimal_places=2)
    impact_share = models.FloatField()
    attention_level = models.CharField(max_length=50)
    recommended_supply = models.IntegerField()

    updated_at = models.DateTimeField(auto_now=True)
    class Meta:
        verbose_name = "Сводную аналитику доставки"
        verbose_name_plural = "Сводная аналитика доставки"

    def __str__(self):
        return f"Delivery Summary for {self.store.name or self.store.client_id}"


# ЕЖЕДНЕВНАЯ АНАЛИТИКА ПО ТОВАРУ
class ProductDailyAnalytics(models.Model):
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='daily_analytics')

    # Идентификаторы товара
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


#  АВТО РЕКЛАМНАЯ КАМПАНИЯ (одна строка кампании для SKU)
class AdPlanItem(models.Model):
    # Статусы кампании (аналогично ManualCampaign)
    CAMPAIGN_STATE_UNKNOWN = 'CAMPAIGN_STATE_UNKNOWN'
    CAMPAIGN_STATE_ACTIVE = 'CAMPAIGN_STATE_ACTIVE'
    CAMPAIGN_STATE_INACTIVE = 'CAMPAIGN_STATE_INACTIVE'
    CAMPAIGN_STATE_PAUSED = 'CAMPAIGN_STATE_PAUSED'
    CAMPAIGN_STATE_ENDED = 'CAMPAIGN_STATE_ENDED'
    CAMPAIGN_STATE_RUNNING = 'CAMPAIGN_STATE_RUNNING'
    CAMPAIGN_STATE_PLANNED = 'CAMPAIGN_STATE_PLANNED'
    CAMPAIGN_STATE_STOPPED = 'CAMPAIGN_STATE_STOPPED'
    CAMPAIGN_STATE_ARCHIVED = 'CAMPAIGN_STATE_ARCHIVED'
    CAMPAIGN_STATE_MODERATION_DRAFT = 'CAMPAIGN_STATE_MODERATION_DRAFT'
    CAMPAIGN_STATE_MODERATION_IN_PROGRESS = 'CAMPAIGN_STATE_MODERATION_IN_PROGRESS'
    CAMPAIGN_STATE_MODERATION_FAILED = 'CAMPAIGN_STATE_MODERATION_FAILED'
    CAMPAIGN_STATE_FINISHED = 'CAMPAIGN_STATE_FINISHED'
    
    STATE_CHOICES = [
        (CAMPAIGN_STATE_UNKNOWN, 'Неизвестно'),
        (CAMPAIGN_STATE_ACTIVE, 'Активна'),
        (CAMPAIGN_STATE_INACTIVE, 'Неактивна'),
        (CAMPAIGN_STATE_PAUSED, 'Приостановлена'),
        (CAMPAIGN_STATE_ENDED, 'Завершена'),
        (CAMPAIGN_STATE_RUNNING, 'Запущена'),
        (CAMPAIGN_STATE_PLANNED, 'Запланирована'),
        (CAMPAIGN_STATE_STOPPED, 'Остановлена (нехватка бюджета)'),
        (CAMPAIGN_STATE_ARCHIVED, 'Архивная'),
        (CAMPAIGN_STATE_MODERATION_DRAFT, 'Черновик модерации'),
        (CAMPAIGN_STATE_MODERATION_IN_PROGRESS, 'На модерации'),
        (CAMPAIGN_STATE_MODERATION_FAILED, 'Не прошла модерацию'),
        (CAMPAIGN_STATE_FINISHED, 'Завершена (дата в прошлом)'),
    ]
    
    # Магазин для которого создана кампания
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='ad_plan_items')

    # Идентификаторы товара
    sku = models.BigIntegerField()
    offer_id = models.CharField(max_length=255, blank=True)
    name = models.CharField(max_length=500, blank=True)

    # Бюджеты
    week_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    day_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    manual_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Ручной бюджет")
    # Сколько дней отдаём кампании на обучение (из Google Sheets V17)
    train_days = models.PositiveIntegerField(default=5, verbose_name="Дней на обучение")

    # ABC
    abc_label = models.CharField(max_length=1, blank=True)

    # Есть ли уже реклама для данного SKU
    has_existing_campaign = models.BooleanField(default=False, verbose_name="Есть реклама для SKU")

    # Для интеграции с Оzon Performance API
    ozon_campaign_id = models.CharField(max_length=100, blank=True)
    campaign_name = models.CharField(max_length=255, blank=True)
    campaign_type = models.CharField(max_length=50, blank=True)
    state = models.CharField(max_length=50, choices=STATE_CHOICES, default=CAMPAIGN_STATE_UNKNOWN, verbose_name="Статус кампании")
    
    # Дополнительные поля из Ozon API
    payment_type = models.CharField(max_length=20, blank=True, verbose_name="Тип оплаты")  # CPO/CPC/CPM
    total_budget = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True, verbose_name="Общий бюджет")
    from_date = models.DateField(null=True, blank=True, verbose_name="Дата начала")
    to_date = models.DateField(null=True, blank=True, verbose_name="Дата окончания")
    placement = models.CharField(max_length=50, blank=True, verbose_name="Размещение")
    product_autopilot_strategy = models.CharField(max_length=50, blank=True, verbose_name="Стратегия автопилота")
    
    # Временные метки из Ozon API
    ozon_created_at = models.DateTimeField(null=True, blank=True, verbose_name="Дата создания в Ozon")
    ozon_updated_at = models.DateTimeField(null=True, blank=True, verbose_name="Дата обновления в Ozon")
    
    # Интеграция с Google Sheets
    google_sheet_row = models.PositiveIntegerField(null=True, blank=True, verbose_name="Номер строки в Google таблице")
    is_active_in_sheets = models.BooleanField(default=False, verbose_name="Активна в Google Sheets (колонка B)")

    # Места под KPI кампании (по желанию заполним отдельным таском)
    adv_sales_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    adv_sales_units = models.IntegerField(null=True, blank=True)
    adv_spend = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    adv_drr_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    total_sales_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    total_sales_units = models.IntegerField(null=True, blank=True)
    tacos_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    
    # Свойства для работы со статусами кампании
    @property
    def is_active(self):
        """Возвращает True, если кампания активна"""
        return self.state in [
            self.CAMPAIGN_STATE_RUNNING,
            self.CAMPAIGN_STATE_PLANNED,
            self.CAMPAIGN_STATE_ACTIVE
        ]
    
    @property
    def is_moderation_in_progress(self):
        """Возвращает True, если кампания на модерации"""
        return self.state in [
            self.CAMPAIGN_STATE_MODERATION_IN_PROGRESS,
            self.CAMPAIGN_STATE_MODERATION_DRAFT
        ]
    
    @property
    def is_finished(self):
        """Возвращает True, если кампания завершена"""
        return self.state in [
            self.CAMPAIGN_STATE_FINISHED,
            self.CAMPAIGN_STATE_ENDED,
            self.CAMPAIGN_STATE_ARCHIVED
        ]
    
    @property
    def is_stopped(self):
        """Возвращает True, если кампания остановлена"""
        return self.state == self.CAMPAIGN_STATE_STOPPED
    
    @property
    def can_be_automated(self):
        """Возвращает True, если кампанию можно автоматизировать"""
        return self.state in [
            self.CAMPAIGN_STATE_INACTIVE,
            self.CAMPAIGN_STATE_PAUSED,
            self.CAMPAIGN_STATE_ENDED,
            self.CAMPAIGN_STATE_ARCHIVED,
            self.CAMPAIGN_STATE_MODERATION_FAILED,
            self.CAMPAIGN_STATE_FINISHED
        ]

    class Meta:
        indexes = [
            models.Index(fields=['store', 'sku']),
            models.Index(fields=['ozon_campaign_id']),
        ]
        verbose_name = 'Рекламная кампания'
        verbose_name_plural = 'Рекламные кампании'

    def __str__(self):
        return f"{self.store} | SKU {self.sku} | {self.campaign_name or 'Без названия'}"


# МОДЕЛЬ ДЛЯ ХРАНЕНИЯ РЕКЛАМНЫХ КАМПАНИЙ, СОЗДАННЫХ ВРУЧНУЮ
class ManualCampaign(models.Model):
    # Статусы кампании
    CAMPAIGN_STATE_UNKNOWN = 'CAMPAIGN_STATE_UNKNOWN'
    CAMPAIGN_STATE_ACTIVE = 'CAMPAIGN_STATE_ACTIVE'
    CAMPAIGN_STATE_INACTIVE = 'CAMPAIGN_STATE_INACTIVE'
    CAMPAIGN_STATE_PAUSED = 'CAMPAIGN_STATE_PAUSED'
    CAMPAIGN_STATE_ENDED = 'CAMPAIGN_STATE_ENDED'
    CAMPAIGN_STATE_RUNNING = 'CAMPAIGN_STATE_RUNNING'
    CAMPAIGN_STATE_PLANNED = 'CAMPAIGN_STATE_PLANNED'
    CAMPAIGN_STATE_STOPPED = 'CAMPAIGN_STATE_STOPPED'
    CAMPAIGN_STATE_ARCHIVED = 'CAMPAIGN_STATE_ARCHIVED'
    CAMPAIGN_STATE_MODERATION_DRAFT = 'CAMPAIGN_STATE_MODERATION_DRAFT'
    CAMPAIGN_STATE_MODERATION_IN_PROGRESS = 'CAMPAIGN_STATE_MODERATION_IN_PROGRESS'
    CAMPAIGN_STATE_MODERATION_FAILED = 'CAMPAIGN_STATE_MODERATION_FAILED'
    CAMPAIGN_STATE_FINISHED = 'CAMPAIGN_STATE_FINISHED'
    
    STATE_CHOICES = [
        (CAMPAIGN_STATE_UNKNOWN, 'Неизвестно'),
        (CAMPAIGN_STATE_ACTIVE, 'Активна'),
        (CAMPAIGN_STATE_INACTIVE, 'Неактивна'),
        (CAMPAIGN_STATE_PAUSED, 'Приостановлена'),
        (CAMPAIGN_STATE_ENDED, 'Завершена'),
        (CAMPAIGN_STATE_RUNNING, 'Запущена'),
        (CAMPAIGN_STATE_PLANNED, 'Запланирована'),
        (CAMPAIGN_STATE_STOPPED, 'Остановлена (нехватка бюджета)'),
        (CAMPAIGN_STATE_ARCHIVED, 'Архивная'),
        (CAMPAIGN_STATE_MODERATION_DRAFT, 'Черновик модерации'),
        (CAMPAIGN_STATE_MODERATION_IN_PROGRESS, 'На модерации'),
        (CAMPAIGN_STATE_MODERATION_FAILED, 'Не прошла модерацию'),
        (CAMPAIGN_STATE_FINISHED, 'Завершена (дата в прошлом)'),
    ]
    
    # Типы оплаты
    PAYMENT_TYPE_CPO = 'CPO'
    PAYMENT_TYPE_CPC = 'CPC'
    PAYMENT_TYPE_CPM = 'CPM'
    
    PAYMENT_TYPE_CHOICES = [
        (PAYMENT_TYPE_CPO, 'CPO'),
        (PAYMENT_TYPE_CPC, 'CPC'),
        (PAYMENT_TYPE_CPM, 'CPM'),
    ]
    
    # Типы объектов рекламы
    ADV_OBJECT_TYPE_SKU = 'SKU'
    ADV_OBJECT_TYPE_CATEGORY = 'CATEGORY'
    
    ADV_OBJECT_TYPE_CHOICES = [
        (ADV_OBJECT_TYPE_SKU, 'SKU'),
        (ADV_OBJECT_TYPE_CATEGORY, 'Категория'),
    ]
    
    # Основные поля
    name = models.CharField(max_length=500, verbose_name="Название кампании")
    offer_id = models.CharField(max_length=255, blank=True, verbose_name="Offer ID")
    sku = models.BigIntegerField(null=True, blank=True, verbose_name="SKU")
    ozon_campaign_id = models.CharField(max_length=100, unique=True, verbose_name="ID кампании в Ozon")
    
    # Для кампаний с множественными SKU
    sku_list = models.JSONField(default=list, blank=True, verbose_name="Список всех SKU в кампании")
    offer_id_list = models.JSONField(default=list, blank=True, verbose_name="Список всех Offer ID в кампании")
    
    # Бюджеты
    week_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Недельный бюджет")
    daily_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Дневной бюджет")
    total_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Общий бюджет")
    
    # Статус и состояние
    state = models.CharField(
        max_length=50, 
        choices=STATE_CHOICES, 
        default=CAMPAIGN_STATE_UNKNOWN,
        verbose_name="Статус кампании"
    )
    
    # Типы и настройки
    payment_type = models.CharField(
        max_length=10, 
        choices=PAYMENT_TYPE_CHOICES, 
        default=PAYMENT_TYPE_CPO,
        verbose_name="Тип оплаты"
    )
    
    adv_object_type = models.CharField(
        max_length=20, 
        choices=ADV_OBJECT_TYPE_CHOICES, 
        default=ADV_OBJECT_TYPE_SKU,
        verbose_name="Тип рекламного объекта"
    )
    
    # Даты
    from_date = models.DateField(null=True, blank=True, verbose_name="Дата начала")
    to_date = models.DateField(null=True, blank=True, verbose_name="Дата окончания")
    
    # Размещение
    placement = models.JSONField(default=list, blank=True, verbose_name="Места размещения")
    
    # Автопилот
    product_autopilot_strategy = models.CharField(max_length=100, blank=True, verbose_name="Стратегия автопилота")
    product_campaign_mode = models.CharField(max_length=100, blank=True, verbose_name="Режим кампании")
    
    # Автоувеличение бюджета
    auto_increase_percent = models.PositiveIntegerField(default=0, verbose_name="Процент автоувеличения")
    auto_increased_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Автоувеличенный бюджет")
    is_auto_increased = models.BooleanField(default=False, verbose_name="Автоувеличение включено")
    recommended_auto_increase_percent = models.PositiveIntegerField(default=0, verbose_name="Рекомендуемый процент автоувеличения")
    
    # Временные метки
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Дата создания")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Дата обновления")
    
    # Временные метки из Ozon API
    ozon_created_at = models.DateTimeField(null=True, blank=True, verbose_name="Дата создания в Ozon")
    ozon_updated_at = models.DateTimeField(null=True, blank=True, verbose_name="Дата обновления в Ozon")
    
    # Связь с магазином (если нужно)
    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='manual_campaigns', null=True, blank=True)
    
    class Meta:
        verbose_name = "Ручная рекламная кампания"
        verbose_name_plural = "Ручные рекламные кампании"
        indexes = [
            models.Index(fields=['ozon_campaign_id']),
            models.Index(fields=['sku']),
            models.Index(fields=['state']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"{self.name} (ID: {self.ozon_campaign_id})"


# Флаг управления рекламной системой по магазину (старт/стоп)
class StoreAdControl(models.Model):
    store = models.OneToOneField(OzonStore, on_delete=models.CASCADE, related_name='ad_control')
    is_system_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Вкл/Выкл"
        verbose_name_plural = "Вкл/Выкл"

    def __str__(self):
        return f"{self.store} | {'Включен' if self.is_system_enabled else 'Выключен'}"


# ХРАНЕНИЕ ОТЧЁТОВ PERFORMANCE API (статистика рекламных кампаний)
class CampaignPerformanceReport(models.Model):
    """
    Сохраняет запрос/ответ отчёта Performance API по кампании за период.
    Жизненный цикл:
    - создаём запись со статусом PENDING после POST /statistics/json (сохраняем UUID, период, запрос)
    - по готовности отчёта (GET /statistics/report?UUID=...) записываем ответ, строки, тоталы и статус READY
    """

    STATUS_PENDING = 'PENDING'
    STATUS_READY = 'READY'
    STATUS_ERROR = 'ERROR'
    STATUS_CHOICES = [
        (STATUS_PENDING, 'Ожидается'),
        (STATUS_READY, 'Готов'),
        (STATUS_ERROR, 'Ошибка'),
    ]

    store = models.ForeignKey(OzonStore, on_delete=models.CASCADE, related_name='performance_reports')
    ozon_campaign_id = models.CharField(max_length=100, help_text='ID кампании в Ozon (campaignId)')

    # UUID отчёта, возвращаемый после POST statistics/json
    report_uuid = models.CharField(max_length=64, unique=True)

    # Период отчёта (RFC 3339 в API; здесь сохраняем как DateTime)
    date_from = models.DateTimeField()
    date_to = models.DateTimeField()

    # Служебные поля состояния
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    error_message = models.TextField(blank=True)
    requested_at = models.DateTimeField(auto_now_add=True)
    ready_at = models.DateTimeField(null=True, blank=True)
    last_checked_at = models.DateTimeField(null=True, blank=True)

    # Полезные данные
    request_payload = models.JSONField(null=True, blank=True, help_text='Тело запроса на построение отчёта')
    raw_response = models.JSONField(null=True, blank=True, help_text='Полный ответ отчёта как есть')
    totals = models.JSONField(null=True, blank=True, help_text='Сводные метрики отчёта (report.totals)')
    rows = models.JSONField(null=True, blank=True, help_text='Строки отчёта (report.rows)')

    class Meta:
        indexes = [
            models.Index(fields=['store', 'ozon_campaign_id']),
            models.Index(fields=['report_uuid']),
            models.Index(fields=['date_from', 'date_to']),
        ]
        unique_together = (
            ('store', 'ozon_campaign_id', 'date_from', 'date_to'),
        )
        verbose_name = 'Отчёт Performance API'
        verbose_name_plural = 'Отчёты Performance API'

    def __str__(self):
        return f"Report {self.ozon_campaign_id} [{self.date_from:%Y-%m-%d}..{self.date_to:%Y-%m-%d}] ({self.status})"


class CampaignPerformanceReportEntry(models.Model):
    """
    Детализация отчёта по конкретной кампании внутри CampaignPerformanceReport.
    Поддерживает случаи, когда один UUID содержит несколько кампаний.
    """

    report = models.ForeignKey(CampaignPerformanceReport, on_delete=models.CASCADE, related_name='entries')
    ozon_campaign_id = models.CharField(max_length=100)

    totals = models.JSONField(null=True, blank=True)
    rows = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['ozon_campaign_id']),
        ]
        unique_together = (
            ('report', 'ozon_campaign_id'),
        )
        verbose_name = 'Кампания в отчёте Performance'
        verbose_name_plural = 'Кампании в отчёте Performance'

    def __str__(self):
        return f"Entry {self.ozon_campaign_id} of {self.report_id}"
    @property
    def is_active(self):
        """Проверяет, активна ли кампания"""
        return self.state in [
            self.CAMPAIGN_STATE_ACTIVE, 
            self.CAMPAIGN_STATE_RUNNING,
            self.CAMPAIGN_STATE_PLANNED
        ]
    
    @property
    def can_be_automated(self):
        """Проверяет, можно ли автоматизировать эту кампанию"""
        # Нельзя автоматизировать активные кампании и кампании в процессе модерации
        non_automated_states = [
            self.CAMPAIGN_STATE_ACTIVE,
            self.CAMPAIGN_STATE_RUNNING,
            self.CAMPAIGN_STATE_PLANNED,
            self.CAMPAIGN_STATE_MODERATION_DRAFT,
            self.CAMPAIGN_STATE_MODERATION_IN_PROGRESS,
            self.CAMPAIGN_STATE_UNKNOWN
        ]
        return self.state not in non_automated_states
    
    @property
    def is_moderation_in_progress(self):
        """Проверяет, находится ли кампания на модерации"""
        return self.state in [
            self.CAMPAIGN_STATE_MODERATION_DRAFT,
            self.CAMPAIGN_STATE_MODERATION_IN_PROGRESS
        ]
    
    @property
    def is_finished(self):
        """Проверяет, завершена ли кампания"""
        return self.state in [
            self.CAMPAIGN_STATE_FINISHED,
            self.CAMPAIGN_STATE_ENDED,
            self.CAMPAIGN_STATE_ARCHIVED
        ]
    
    @property
    def is_stopped(self):
        """Проверяет, остановлена ли кампания"""
        return self.state in [
            self.CAMPAIGN_STATE_STOPPED,
            self.CAMPAIGN_STATE_INACTIVE
        ]
