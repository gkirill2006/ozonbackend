import uuid
from decimal import Decimal
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, BaseUserManager
import random



# Create your models here.
class UserManager(BaseUserManager):
    def create_user(self, telegram_id, password=None, **extra_fields):
        if not telegram_id:
            raise ValueError('The Telegram ID must be set')
        user = self.model(telegram_id=telegram_id, **extra_fields)
        user.set_password(password)
        user.referral_code = self.generate_referral_code()
        user.save(using=self._db)
        return user

    def generate_referral_code(self):
        while True:
            code = str(random.randint(10000000, 99999999))
            if not User.objects.filter(referral_code=code).exists():
                return code

    def create_superuser(self, telegram_id, password=None, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True.')
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')

        return self.create_user(telegram_id, password, **extra_fields)
class User(AbstractBaseUser, PermissionsMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    telegram_id = models.BigIntegerField(unique=True, null=True, blank=True)
    language_code = models.CharField(max_length=10, blank=True, null=True)
    is_bot = models.BooleanField(default=False)  # type: ignore
    username = models.CharField(max_length=150, blank=True, null=True)
    referral_code = models.CharField(max_length=64, unique=False)
    referred_by = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='referrals')
    referral_percent = models.DecimalField(max_digits=5, decimal_places=2, default=5.00)
    # registration_date = models.DateTimeField(auto_now_add=True)
    
    photo_url = models.URLField(blank=True, null=True)
    first_name = models.CharField(max_length=255, blank=True, null=True)
    last_name = models.CharField(max_length=255, blank=True, null=True)
    
    is_active = models.BooleanField(default=True)  # type: ignore
    is_staff = models.BooleanField(default=False)  # type: ignore
    is_superuser = models.BooleanField(default=False)  # type: ignore

    objects = UserManager()

    USERNAME_FIELD = 'telegram_id'
    REQUIRED_FIELDS = []

    def __str__(self):
        return str(self.telegram_id)

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


class StoreFilterSettings(models.Model):
    SORT_ORDERS = [
        ('orders', 'Orders'),
        ('revenue', 'Revenue'),
        ('ozon-rec', 'Ozon recommendations'),
    ]

    store = models.OneToOneField(OzonStore, on_delete=models.CASCADE, related_name='filter_settings')
    planning_days = models.PositiveIntegerField(default=28)
    analysis_period = models.PositiveIntegerField(default=28)
    warehouse_weight = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal('1.00'))
    price_min = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('1000.00'))
    price_max = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('5000.00'))
    turnover_min = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal('10.00'))
    turnover_max = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal('90.00'))
    show_no_need = models.BooleanField(default=False)
    sort_by = models.CharField(max_length=32, choices=SORT_ORDERS, default='orders')
    specific_weight_threshold = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal('0.0100'))
    turnover_from_stock = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal('5.00'))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Store filter setting"
        verbose_name_plural = "Store filter settings"

    def __str__(self):
        return f"Filters for {self.store}"


class StoreRequiredProduct(models.Model):
    filter_settings = models.ForeignKey(
        StoreFilterSettings,
        on_delete=models.CASCADE,
        related_name='required_products',
    )
    article = models.CharField(max_length=255)
    quantity = models.PositiveIntegerField(default=1)

    class Meta:
        unique_together = ('filter_settings', 'article')
        verbose_name = "Store required product"
        verbose_name_plural = "Store required products"

    def __str__(self):
        return f"{self.article} x{self.quantity}"


class StoreExcludedProduct(models.Model):
    filter_settings = models.ForeignKey(
        StoreFilterSettings,
        on_delete=models.CASCADE,
        related_name='excluded_products',
    )
    article = models.CharField(max_length=255)

    class Meta:
        unique_together = ('filter_settings', 'article')
        verbose_name = "Store excluded product"
        verbose_name_plural = "Store excluded products"

    def __str__(self):
        return self.article
