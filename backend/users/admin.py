from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import User, OzonStore
from ozon.tasks import sync_full_store_data
from django.contrib import messages

@admin.register(User)
class UserAdmin(BaseUserAdmin):
    list_display = ('username', 'is_staff')

    # При создании нового пользователя
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'password1', 'password2', ),
        }),
    )

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
