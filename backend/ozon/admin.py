from django.contrib import admin
from django.contrib.admin.filters import DateFieldListFilter
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from datetime import timedelta, date as dt_date
from .models import (Product, WarehouseStock, Sale, FbsStock, Category, DeliveryCluster, DeliveryClusterItemAnalytics,
                     DeliveryAnalyticsSummary, ProductDailyAnalytics, AdPlanItem, ManualCampaign, CampaignPerformanceReport, CampaignPerformanceReportEntry)

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'store',
        'product_id',
        'sku',
        'offer_id',
        'name',
        'category',
        'type_name',
        'price',
    )
    search_fields = ('offer_id', 'name', 'sku', 'product_id')
    list_filter = ('category', 'store')
    readonly_fields = ('product_id', 'sku')  # если ты не хочешь, чтобы их правили вручную

@admin.register(WarehouseStock)
class WarehouseStockAdmin(admin.ModelAdmin):
    list_display = (
        'warehouse_id',
        'product',
        'sku',
        'warehouse_name',
        'available_stock_count',
        'valid_stock_count',
        'waiting_docs_stock_count',
        'expiring_stock_count',
        'transit_defect_stock_count',
        'stock_defect_stock_count',
        'excess_stock_count',
        'other_stock_count',
        'requested_stock_count',
        'transit_stock_count',
        'return_from_customer_stock_count',
        'cluster_name',
        'updated_at',
    )
    search_fields = ('product__name', 'sku', 'warehouse_id', "cluster_name")
    list_filter = ('warehouse_name', 'cluster_name', 'store', 'warehouse_id')
    readonly_fields = ('updated_at',)
    
@admin.register(Sale)
class SaleAdmin(admin.ModelAdmin):
    list_display = (
        'date',
        'sale_type',
        'store',
        'sku',
        'quantity',
        'price',
        'payout',
        # 'commission_amount',
        # 'customer_price',
        # 'tpl_provider',
        'status',
        # 'cluster_from',
        'cluster_to',
        'warehouse_id',
        'posting_number',
        'created_at'
    )
    list_filter = ('sale_type', 'status', 'store', 'cluster_from', 'cluster_to')
    search_fields = ('sku', 'posting_number', 'cluster_from', 'posting_number')
    ordering = ('-date',)
    readonly_fields = ('created_at',)
    
@admin.register(FbsStock)
class FbsStockAdmin(admin.ModelAdmin):
    list_display = (
        'store',
        'sku',
        'fbs_sku',
        'product_id',
        'present',
        'reserved',
        'warehouse_id',
        'warehouse_name',
        'updated_at',
    )
    search_fields = ('sku', 'fbs_sku', 'warehouse_name',)
    list_filter = ('warehouse_name', 'store',)
    ordering = ('-updated_at',)
    readonly_fields = ('updated_at',)
    
@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = (

        'category_id',
        'name',
    )



@admin.register(DeliveryCluster)
class DeliveryClusterAdmin(admin.ModelAdmin):
    list_display = (
        "store", 
        "delivery_cluster_id", 
        "name", 
        "type", 
        "average_delivery_time", 
        "impact_share", 
        "lost_profit", 
        "recommended_supply"
    )
    list_filter = ("store", "type")
    search_fields = ("name", "delivery_cluster_id")
    ordering = ("store", "delivery_cluster_id")

@admin.register(DeliveryClusterItemAnalytics)
class DeliveryClusterItemAnalyticsAdmin(admin.ModelAdmin):
    list_display = (
        "store", 
        "cluster_id", 
        "cluster_name", 
        "sku", 
        "offer_id", 
        "average_delivery_time", 
        "average_delivery_time_status", 
        "impact_share", 
        "attention_level", 
        "recommended_supply", 
        "recommended_supply_FBO",
        "recommended_supply_FBS",
        "updated_at",
    )
    list_filter = ("store", "cluster_id", "average_delivery_time_status", "attention_level")
    search_fields = ("sku", "offer_id", "cluster_name")
    ordering = ("-updated_at",)
    
    
@admin.register(DeliveryAnalyticsSummary)
class DeliveryAnalyticsSummaryAdmin(admin.ModelAdmin):
    list_display = (
        'store',
        'average_delivery_time',
        'average_delivery_time_status',
        'total_orders',
        'lost_profit',
        'impact_share',
        'attention_level',
        'recommended_supply',
        'updated_at',
    )
    list_filter = ('average_delivery_time_status', 'attention_level')
    search_fields = ('store__name', 'store__client_id')


class YesterdayDateFilter(DateFieldListFilter):
    """Кастомный фильтр даты: вместо 'Сегодня' показываем 'Вчера'."""
    def __init__(self, field, request, params, model, model_admin, field_path):
        super().__init__(field, request, params, model, model_admin, field_path)
        now = timezone.localtime(timezone.now())
        today = now.date()
        yesterday = today - timedelta(days=1)

        last_7_since = today - timedelta(days=7)
        last_7_until = today + timedelta(days=1)

        month_start = today.replace(day=1)
        next_month_start = (month_start + timedelta(days=32)).replace(day=1)

        year_start = today.replace(month=1, day=1)
        next_year_start = dt_date(today.year + 1, 1, 1)

        self.links = (
            (_('Любая дата'), {}),
            (_('Вчера'), {
                self.lookup_kwarg_since: str(yesterday),
                self.lookup_kwarg_until: str(today),
            }),
            (_('Последние 7 дней'), {
                self.lookup_kwarg_since: str(last_7_since),
                self.lookup_kwarg_until: str(last_7_until),
            }),
            (_('Этот месяц'), {
                self.lookup_kwarg_since: str(month_start),
                self.lookup_kwarg_until: str(next_month_start),
            }),
            (_('Этот год'), {
                self.lookup_kwarg_since: str(year_start),
                self.lookup_kwarg_until: str(next_year_start),
            }),
        )


@admin.register(ProductDailyAnalytics)
class ProductDailyAnalyticsAdmin(admin.ModelAdmin):
    # Отображаем ключевые поля аналитики
    list_display = (
        'date',
        'store',
        'sku',
        'offer_id',
        'name',
        'revenue',
        'ordered_units',
        'created_at',
        'updated_at'
    )
    # Фильтрация и удобная навигация по дате
    list_filter = ('store', ('date', YesterdayDateFilter), 'offer_id', 'sku', 'created_at')
    date_hierarchy = 'date'
    # Поиск по SKU, offer_id и названию
    search_fields = ('sku', 'offer_id', 'name')
    # Сортировка по дате (сначала новые)
    ordering = ('-date', '-created_at')
    readonly_fields = ('created_at',)


@admin.register(AdPlanItem)
class AdPlanItemAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'store', 'sku', 'offer_id', 'week_budget', 'manual_budget', 'state', 'payment_type', 'has_existing_campaign', 'is_active_in_sheets', 'abc_label',
        'ozon_campaign_id', 'campaign_name', 'campaign_type', 'ozon_created_at', 'ozon_updated_at', 'created_at'
    )
    list_filter = ('abc_label', 'store', 'campaign_type', 'state', 'payment_type', 'has_existing_campaign', 'is_active_in_sheets')
    search_fields = ('sku', 'offer_id', 'ozon_campaign_id', 'campaign_name', 'store__name')
    ordering = ('-created_at',)
    readonly_fields = ('created_at',)


@admin.register(ManualCampaign)
class ManualCampaignAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'ozon_campaign_id', 'sku', 'sku_list', 'offer_id', 'state', 'payment_type',
        'week_budget', 'daily_budget', 'total_budget', 'store', 'ozon_created_at', 'ozon_updated_at', 'created_at', 'updated_at'
    )
    list_filter = (
        'state', 'payment_type', 'adv_object_type', 'store',
        ('from_date', DateFieldListFilter),
        ('to_date', DateFieldListFilter),
        ('ozon_created_at', DateFieldListFilter),
        ('ozon_updated_at', DateFieldListFilter),
        ('created_at', DateFieldListFilter),
        ('updated_at', DateFieldListFilter),
    )
    search_fields = ('name', 'ozon_campaign_id', 'sku', 'offer_id', 'store__name', 'sku_list')
    ordering = ('-created_at',)
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'ozon_campaign_id', 'store')
        }),
        ('Товар', {
            'fields': ('sku', 'offer_id', 'sku_list', 'offer_id_list', 'adv_object_type')
        }),
        ('Бюджеты', {
            'fields': ('week_budget', 'daily_budget', 'total_budget')
        }),
        ('Статус и настройки', {
            'fields': ('state', 'payment_type', 'from_date', 'to_date', 'placement')
        }),
        ('Автопилот', {
            'fields': ('product_autopilot_strategy', 'product_campaign_mode')
        }),
        ('Автоувеличение бюджета', {
            'fields': ('auto_increase_percent', 'auto_increased_budget', 'is_auto_increased', 'recommended_auto_increase_percent')
        }),
        ('Временные метки', {
            'fields': ('ozon_created_at', 'ozon_updated_at', 'created_at', 'updated_at')
        }),
    )
    
class CampaignPerformanceReportEntryInline(admin.TabularInline):
    model = CampaignPerformanceReportEntry
    extra = 0
    can_delete = False
    fields = (
        'ozon_campaign_id', 'row_count', 'views', 'clicks', 'money_spent', 'orders', 'orders_money', 'ctr', 'drr',
    )
    readonly_fields = fields

    def _get_num(self, obj, key):
        v = (obj.totals or {}).get(key)
        if v is None:
            return ''
        s = str(v).replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')
        try:
            return float(s)
        except Exception:
            return s

    def row_count(self, obj):
        rows = obj.rows or []
        return len(rows) if isinstance(rows, list) else 0

    def views(self, obj):
        return self._get_num(obj, 'views')

    def clicks(self, obj):
        return self._get_num(obj, 'clicks')

    def money_spent(self, obj):
        return self._get_num(obj, 'moneySpent')

    def orders(self, obj):
        return self._get_num(obj, 'orders')

    def orders_money(self, obj):
        return self._get_num(obj, 'ordersMoney')

    def ctr(self, obj):
        return self._get_num(obj, 'ctr')

    def drr(self, obj):
        return self._get_num(obj, 'drr')


@admin.register(CampaignPerformanceReport)
class CampaignPerformanceReportAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'store', 'ozon_campaign_id', 'report_uuid', 'status',
        'date_from', 'date_to', 'requested_at', 'ready_at', 'entries_count',
        'totals_views', 'totals_clicks', 'totals_money_spent', 'totals_orders', 'totals_orders_money'
    )
    list_filter = (
        'status', 'store',
        ('date_from', DateFieldListFilter),
        ('date_to', DateFieldListFilter),
        ('requested_at', DateFieldListFilter),
        ('ready_at', DateFieldListFilter),
    )
    search_fields = ('report_uuid', 'ozon_campaign_id', 'store__name', 'store__client_id')
    ordering = ('-requested_at',)
    readonly_fields = ('requested_at', 'ready_at', 'last_checked_at')
    date_hierarchy = 'date_from'
    inlines = [CampaignPerformanceReportEntryInline]

    def entries_count(self, obj):
        return obj.entries.count()

    def _get_num(self, obj, key):
        v = (obj.totals or {}).get(key)
        if v is None:
            return ''
        s = str(v).replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')
        try:
            return float(s)
        except Exception:
            return s

    def totals_views(self, obj):
        return self._get_num(obj, 'views')

    def totals_clicks(self, obj):
        return self._get_num(obj, 'clicks')

    def totals_money_spent(self, obj):
        return self._get_num(obj, 'moneySpent')

    def totals_orders(self, obj):
        return self._get_num(obj, 'orders')

    def totals_orders_money(self, obj):
        return self._get_num(obj, 'ordersMoney')

@admin.register(CampaignPerformanceReportEntry)
class CampaignPerformanceReportEntryAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'report_date_from', 'report_date_to', 'ozon_campaign_id', 'row_count',
        'views', 'clicks', 'money_spent', 'orders', 'orders_money', 'ctr', 'drr',
        'created_at', 'updated_at'
    )
    list_filter = (
        ('report__date_from', DateFieldListFilter),
        ('report__date_to', DateFieldListFilter),
        ('created_at', DateFieldListFilter),
        ('updated_at', DateFieldListFilter),
    )
    search_fields = ('ozon_campaign_id', 'report__report_uuid')
    ordering = ('report__date_from', 'ozon_campaign_id', 'id')

    def _get_num(self, obj, key):
        v = (obj.totals or {}).get(key)
        if v is None:
            return ''
        s = str(v).replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')
        try:
            return float(s)
        except Exception:
            return s

    def row_count(self, obj):
        rows = obj.rows or []
        return len(rows) if isinstance(rows, list) else 0

    def views(self, obj):
        return self._get_num(obj, 'views')

    def clicks(self, obj):
        return self._get_num(obj, 'clicks')

    def money_spent(self, obj):
        return self._get_num(obj, 'moneySpent')

    def orders(self, obj):
        return self._get_num(obj, 'orders')

    def orders_money(self, obj):
        return self._get_num(obj, 'ordersMoney')

    def ctr(self, obj):
        return self._get_num(obj, 'ctr')

    def drr(self, obj):
        return self._get_num(obj, 'drr')

    # Период отчёта из связанного Report
    def report_date_from(self, obj):
        return obj.report.date_from.date() if obj.report and obj.report.date_from else ''
    report_date_from.short_description = 'Date From'
    report_date_from.admin_order_field = 'report__date_from'
    report_date_from.admin_order_field = 'report__date_from'

    def report_date_to(self, obj):
        return obj.report.date_to.date() if obj.report and obj.report.date_to else ''
    report_date_to.short_description = 'Date To'
    report_date_to.admin_order_field = 'report__date_to'
    report_date_to.admin_order_field = 'report__date_to'
