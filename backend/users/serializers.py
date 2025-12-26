#dev
import logging
logger = logging.getLogger(__name__)

#-----------------------------------
import json

from rest_framework import serializers
from .models import (
    User,
    OzonStore,
    StoreFilterSettings,
    StoreRequiredProduct,
    StoreExcludedProduct,
)
from django.utils import timezone
import hashlib
import hmac
from rest_framework_simplejwt.tokens import RefreshToken
from urllib.parse import parse_qsl, unquote

print("serializers.py imported", flush=True)
class SessionLoginSerializer(serializers.Serializer):
    """
    Сериализатор для логина сессии через Telegram бота.
    """
    telegram_id = serializers.IntegerField()
    session_id = serializers.CharField()
    username = serializers.CharField(required=False, allow_blank=True)
    language_code = serializers.CharField(required=False, allow_blank=True)
    is_bot = serializers.BooleanField(default=False)
    referred_by = serializers.CharField(required=False, allow_blank=True)

    def create(self, validated_data):
        telegram_id = validated_data['telegram_id']
        session_id = validated_data['session_id']
        
        # Создаем или обновляем пользователя
        user, created = User.objects.get_or_create(telegram_id=telegram_id)
        
        # Обновляем данные пользователя
        user.username = validated_data.get('username', user.username)
        user.language_code = validated_data.get('language_code', user.language_code)
        user.is_bot = validated_data.get('is_bot', user.is_bot)
        user.referred_by = validated_data.get('referred_by', user.referred_by)
        user.save()
        
        # Сохраняем session_id в кэше с привязкой к пользователю
        from django.core.cache import cache
        cache.set(session_id, user.id, timeout=600)  # 10 минут
        
        return user


class ConfirmLoginSerializer(serializers.Serializer):
    session_id = serializers.CharField(max_length=64)
    telegram_id = serializers.IntegerField()
    language_code = serializers.CharField(max_length=10, required=False, allow_null=True)
    is_bot = serializers.BooleanField(required=False, allow_null=True)
    username = serializers.CharField(max_length=150, required=False, allow_null=True)
    referred_by = serializers.CharField(max_length=64, required=False, allow_null=True, allow_blank=True)

    def validate_session_id(self, value):
        if not value:
            raise serializers.ValidationError("Session ID is required.")
        return value

    def validate_telegram_id(self, value):
        if not value:
            raise serializers.ValidationError("Telegram ID is required.")
        return value

    def save(self):
        from django.core.cache import cache
        
        validated_data = self.validated_data
        session_id = validated_data['session_id']
        telegram_id = validated_data['telegram_id']
        language_code = validated_data.get('language_code')
        is_bot = validated_data.get('is_bot')
        username = validated_data.get('username')
        referred_by = validated_data.get('referred_by')
        
        # Попробуем найти пользователя
        user = User.objects.filter(telegram_id=telegram_id).first()
        
        if referred_by:
            user_referred_id = User.objects.filter(referral_code=referred_by).first()
        else:
            user_referred_id = None
            
        if not user:
            # Если пользователь не найден, создаём его через UserManager
            user = User.objects.create_user(
                telegram_id=telegram_id,
                language_code=language_code,
                is_bot=is_bot,
                username=username,
                referred_by=user_referred_id
            )

        # Обновляем дополнительные поля, если они переданы
        if language_code:
            user.language_code = language_code
        if is_bot is not None:
            user.is_bot = is_bot
        if username:
            user.username = username

        user.save()

        # Связываем session_id с идентификатором пользователя через кеш
        cache.set(session_id, user.id, timeout=300)
        print(f"referred_by 2 = {referred_by}")
        return user

class BotInitSerializer(serializers.Serializer):
    """
    Сериализатор для инициализации бота.
    """
    telegram_id = serializers.IntegerField()
    username = serializers.CharField(required=False, allow_blank=True)
    language_code = serializers.CharField(required=False, allow_blank=True)
    is_bot = serializers.BooleanField(default=False)

    def create(self, validated_data):
        user, created = User.objects.get_or_create(telegram_id=validated_data['telegram_id'])
        user.username = validated_data.get('username', user.username)
        user.language_code = validated_data.get('language_code', user.language_code)
        user.is_bot = validated_data.get('is_bot', user.is_bot)
        user.save()
        return user
    

     
def check_validate_init_data(hash_str, init_data, token, c_str="WebAppData"):
    """
    Validates the data received from the Telegram web app using HMAC
    Based on: https://core.telegram.org/bots/webapps#validating-data-received-via-the-web-app
    """
    init_data = sorted([
        chunk.split("=")
        for chunk in unquote(init_data).split("&")
        if not chunk.startswith("hash=")
    ], key=lambda x: x[0])

    data_check_string = "\n".join([f"{k}={v}" for k, v in init_data])
    # logger.debug(f"📋 Constructed data_check_string:\n{data_check_string}")

    secret_key = hmac.new(c_str.encode(), token.encode(), hashlib.sha256).digest()
    hmac_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    logger.debug(f"🔐 Computed HMAC hash: {hmac_hash}")
    logger.debug(f"✅ Expected hash: {hash_str}")

    return hmac_hash == hash_str  

class TelegramWebAppLoginSerializer(serializers.Serializer):
    initData = serializers.CharField()

    def validate(self, data):
        token = self.context.get('telegram_bot_token')
        if not token:
            logger.error("Missing Telegram Bot Token in context")
            raise serializers.ValidationError("Missing Telegram Bot Token in context")

        init_data_str = data['initData']
        # logger.debug(f"\U0001F4E9 Raw initData string from request: {init_data_str}")

        parsed = dict(parse_qsl(unquote(init_data_str)))
        check_hash = parsed.get('hash')
        if not check_hash:
            raise serializers.ValidationError("Missing hash parameter in initData")

        if not check_validate_init_data(check_hash, init_data_str, token):
            logger.error("❌ Invalid Telegram login signature: HMAC mismatch")
            raise serializers.ValidationError("Invalid Telegram login signature")

        parsed.pop('hash', None)
        parsed.pop('signature', None)
        self.validated_init_data = parsed
        logger.debug(f"✅ Validated and parsed init data: {self.validated_init_data}")
        return data

    def save(self):
        init_data = self.validated_init_data
        user_json = init_data.get('user')

        try:
            user_data = json.loads(user_json)
            logger.debug(f"👤 Parsed user data: {user_data}")
        except json.JSONDecodeError:
            logger.error("Invalid JSON in 'user' field")
            raise serializers.ValidationError("Invalid JSON in 'user' field")

        telegram_id = int(user_data['id'])
        username = user_data.get('username')
        first_name = user_data.get('first_name')
        last_name = user_data.get('last_name')
        photo_url = user_data.get('photo_url')

        user = User.objects.filter(telegram_id=telegram_id).first()
        if not user:
            user = User.objects.create_user(
                telegram_id=telegram_id,
                username=username or '',
                first_name=first_name or '',
                last_name=last_name or '',
                photo_url=photo_url or ''
            )
        else:
            user.username = username or user.username
            user.first_name = first_name or user.first_name
            user.last_name = last_name or user.last_name
            user.photo_url = photo_url or user.photo_url

        user.save()

        logger.debug(f"✅ User {'created' if not user else 'updated'}: {user}")

        return user
    
    def generate_tokens(self, user):
        refresh = RefreshToken.for_user(user)
        return {
            'access': str(refresh.access_token),
            'refresh': str(refresh)
        }


class OzonStoreSerializer(serializers.ModelSerializer):
    is_owner = serializers.SerializerMethodField()
    owner_username = serializers.SerializerMethodField()

    class Meta:
        model = OzonStore
        fields = [
            'id',
            'name',
            'client_id',
            'api_key',
            'api_key_invalid_at',
            'google_sheet_url',
            'performance_service_account_number',
            'performance_client_id',
            'performance_client_secret',
            'is_owner',
            'owner_username',
        ]
        read_only_fields = ['id', 'is_owner', 'owner_username', 'api_key_invalid_at']

    def get_is_owner(self, obj):
        request = self.context.get('request')
        return bool(request and getattr(request, 'user', None) == obj.user)

    def get_owner_username(self, obj):
        return obj.user.username if obj.user else None

    def to_representation(self, instance):
        data = super().to_representation(instance)
        is_owner = self.get_is_owner(instance)
        # Скрываем чувствительные ключи для пользователей, которые не владелец магазина
        if not is_owner:
            client_id = instance.client_id or ""
            if client_id:
                data['client_id'] = f"{client_id[:3]}***{client_id[-3:]}"
            else:
                data['client_id'] = None
            data['api_key'] = None
            data['performance_client_secret'] = None
        return data


class StoreRequiredProductSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)

    class Meta:
        model = StoreRequiredProduct
        fields = ['id', 'article', 'quantity']
        extra_kwargs = {
            'article': {'allow_blank': False},
            'quantity': {'min_value': 1},
        }


class StoreExcludedProductSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)

    class Meta:
        model = StoreExcludedProduct
        fields = ['id', 'article']
        extra_kwargs = {
            'article': {'allow_blank': False},
        }


class StoreFilterSettingsSerializer(serializers.ModelSerializer):
    price_min = serializers.DecimalField(max_digits=12, decimal_places=2, coerce_to_string=False)
    price_max = serializers.DecimalField(max_digits=12, decimal_places=2, coerce_to_string=False)
    warehouse_weight = serializers.DecimalField(max_digits=6, decimal_places=2, coerce_to_string=False)
    turnover_min = serializers.DecimalField(max_digits=6, decimal_places=2, coerce_to_string=False)
    turnover_max = serializers.DecimalField(max_digits=6, decimal_places=2, coerce_to_string=False)
    specific_weight_threshold = serializers.DecimalField(max_digits=6, decimal_places=4, coerce_to_string=False)
    turnover_from_stock = serializers.DecimalField(max_digits=6, decimal_places=2, coerce_to_string=False)
    store_id = serializers.IntegerField(source='store.id', read_only=True)
    required_products = StoreRequiredProductSerializer(many=True, required=False)
    excluded_products = StoreExcludedProductSerializer(many=True, required=False)

    class Meta:
        model = StoreFilterSettings
        fields = [
            'id',
            'store_id',
            'planning_days',
            'analysis_period',
            'warehouse_weight',
            'price_min',
            'price_max',
            'turnover_min',
            'turnover_max',
            'show_no_need',
            'sort_by',
            'specific_weight_threshold',
            'turnover_from_stock',
            'required_products',
            'excluded_products',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'store_id', 'created_at', 'updated_at']

    def _replace_related(self, instance, related_name, data_list, model, defaults):
        manager = getattr(instance, related_name)
        manager.all().delete()
        bulk = []
        for item in data_list:
            attrs = defaults(item)
            attrs['filter_settings'] = instance
            bulk.append(model(**attrs))
        model.objects.bulk_create(bulk)

    def update(self, instance, validated_data):
        required_data = validated_data.pop('required_products', None)
        excluded_data = validated_data.pop('excluded_products', None)

        instance = super().update(instance, validated_data)

        if required_data is not None:
            self._replace_related(
                instance,
                'required_products',
                required_data,
                StoreRequiredProduct,
                lambda item: {
                    'article': item['article'].strip(),
                    'quantity': item.get('quantity') or 1,
                },
            )

        if excluded_data is not None:
            self._replace_related(
                instance,
                'excluded_products',
                excluded_data,
                StoreExcludedProduct,
                lambda item: {'article': item['article'].strip()},
            )

        return instance
