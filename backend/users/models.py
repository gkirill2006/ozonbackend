import uuid
from django.contrib.auth.models import AbstractUser
from django.db import models

class User(AbstractUser):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    def __str__(self):
        return self.username

class OzonStore(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='ozon_stores')
    name = models.CharField(max_length=255, blank=True)  # название магазина (по желанию)
    client_id = models.CharField(max_length=100)
    api_key = models.CharField(max_length=255)
    google_sheet_url = models.URLField(blank=True, null=True)  # ссылка на Google-таблицу магазина
    
    # Performance API
    performance_service_account_number = models.CharField(max_length=50, blank=True, null=True)
    performance_client_id = models.CharField(max_length=255, blank=True, null=True)
    performance_client_secret = models.CharField(max_length=500, blank=True, null=True)
    
    def __str__(self):
        return f"{self.name or self.client_id} ({self.user.username})"
