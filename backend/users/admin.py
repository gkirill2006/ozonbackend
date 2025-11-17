from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import (
    User,
    OzonStore,
    StoreFilterSettings,
    StoreRequiredProduct,
    StoreExcludedProduct,
)
from ozon.tasks import sync_full_store_data
from django.contrib import messages

@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('username',)

    # При создании нового пользователя
    # add_fieldsets = (
    #     (None, {
    #         'classes': ('wide',),
    #         'fields': ('username', 'password1', 'password2', ),
    #     }),
    # )

    # При редактировании пользователя


@admin.register(OzonStore)
class OzonStoreAdmin(admin.ModelAdmin):
    list_display = ('name', 'client_id', 'api_key',)
    actions = ['sync_selected_stores']

    @admin.action(description="🔁 Синхронизировать выбранные магазины")
    def sync_selected_stores(self, request, queryset):
        count = 0
        for store in queryset:
            sync_full_store_data.delay(store.id)
            count += 1
        self.message_user(request, f"Задачи синхронизации запущены для {count} магазинов.", messages.INFO)


class RequiredProductInline(admin.TabularInline):
    model = StoreRequiredProduct
    extra = 1


class ExcludedProductInline(admin.TabularInline):
    model = StoreExcludedProduct
    extra = 1


@admin.register(StoreFilterSettings)
class StoreFilterSettingsAdmin(admin.ModelAdmin):
    list_display = ('store', 'planning_days', 'analysis_period', 'sort_by', 'updated_at')
    search_fields = ('store__name', 'store__client_id')
    inlines = [RequiredProductInline, ExcludedProductInline]
