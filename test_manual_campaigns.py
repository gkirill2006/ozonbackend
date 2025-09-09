#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функциональности синхронизации ручных рекламных кампаний
"""

import os
import sys
import django
from decimal import Decimal

# Добавляем путь к Django проекту
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from ozon.models import ManualCampaign, OzonStore
from ozon.tasks import sync_manual_campaigns


def test_manual_campaign_model():
    """Тестирует создание и работу модели ManualCampaign"""
    print("🧪 Тестирование модели ManualCampaign...")
    
    try:
        # Получаем первый магазин для тестирования
        store = OzonStore.objects.first()
        if not store:
            print("❌ Магазины не найдены в базе данных")
            return False
            
        print(f"✅ Используем магазин: {store}")
        
        # Создаем тестовую кампанию
        campaign = ManualCampaign.objects.create(
            name="Тестовая кампания",
            ozon_campaign_id="TEST_12345",
            sku=12345,
            offer_id="TEST_OFFER_001",
            week_budget=Decimal("1000.00"),
            daily_budget=Decimal("150.00"),
            total_budget=Decimal("5000.00"),
            state=ManualCampaign.CAMPAIGN_STATE_INACTIVE,
            payment_type=ManualCampaign.PAYMENT_TYPE_CPO,
            adv_object_type=ManualCampaign.ADV_OBJECT_TYPE_SKU,
            store=store
        )
        
        print(f"✅ Создана тестовая кампания: {campaign}")
        print(f"   - ID: {campaign.id}")
        print(f"   - Название: {campaign.name}")
        print(f"   - SKU: {campaign.sku}")
        print(f"   - Статус: {campaign.state}")
        print(f"   - Активна: {campaign.is_active}")
        print(f"   - Можно автоматизировать: {campaign.can_be_automated}")
        
        # Проверяем свойства
        assert campaign.is_active == False, "Неактивная кампания должна возвращать False для is_active"
        assert campaign.can_be_automated == True, "Неактивная кампания должна быть доступна для автоматизации"
        
        # Очищаем тестовые данные
        campaign.delete()
        print("✅ Тестовая кампания удалена")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования модели: {e}")
        return False


def test_campaign_validation():
    """Тестирует валидацию данных кампании"""
    print("\n🧪 Тестирование валидации данных...")
    
    try:
        # Проверяем, что нельзя создать кампанию с дублирующим ozon_campaign_id
        store = OzonStore.objects.first()
        if not store:
            print("❌ Магазины не найдены в базе данных")
            return False
            
        # Создаем первую кампанию
        campaign1 = ManualCampaign.objects.create(
            name="Первая кампания",
            ozon_campaign_id="DUPLICATE_001",
            sku=11111,
            store=store
        )
        
        # Пытаемся создать вторую с тем же ozon_campaign_id
        try:
            campaign2 = ManualCampaign.objects.create(
                name="Вторая кампания",
                ozon_campaign_id="DUPLICATE_001",  # Дублирующий ID
                sku=22222,
                store=store
            )
            print("❌ Ошибка: удалось создать кампанию с дублирующим ID")
            return False
        except Exception as e:
            print(f"✅ Правильно: не удалось создать кампанию с дублирующим ID - {e}")
        
        # Очищаем тестовые данные
        campaign1.delete()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования валидации: {e}")
        return False


def test_campaign_queries():
    """Тестирует запросы к модели кампаний"""
    print("\n🧪 Тестирование запросов к модели...")
    
    try:
        store = OzonStore.objects.first()
        if not store:
            print("❌ Магазины не найдены в базе данных")
            return False
            
        # Создаем несколько тестовых кампаний
        campaigns_data = [
            {
                'name': 'Активная кампания 1',
                'ozon_campaign_id': 'ACTIVE_001',
                'sku': 10001,
                'state': ManualCampaign.CAMPAIGN_STATE_ACTIVE,
                'store': store
            },
            {
                'name': 'Неактивная кампания 1',
                'ozon_campaign_id': 'INACTIVE_001',
                'sku': 10002,
                'state': ManualCampaign.CAMPAIGN_STATE_INACTIVE,
                'store': store
            },
            {
                'name': 'Приостановленная кампания 1',
                'ozon_campaign_id': 'PAUSED_001',
                'sku': 10003,
                'state': ManualCampaign.CAMPAIGN_STATE_PAUSED,
                'store': store
            }
        ]
        
        created_campaigns = []
        for data in campaigns_data:
            campaign = ManualCampaign.objects.create(**data)
            created_campaigns.append(campaign)
            print(f"✅ Создана кампания: {campaign.name} (статус: {campaign.state})")
        
        # Тестируем фильтры
        active_campaigns = ManualCampaign.objects.filter(state=ManualCampaign.CAMPAIGN_STATE_ACTIVE)
        print(f"✅ Активных кампаний: {active_campaigns.count()}")
        
        inactive_campaigns = ManualCampaign.objects.filter(state=ManualCampaign.CAMPAIGN_STATE_INACTIVE)
        print(f"✅ Неактивных кампаний: {inactive_campaigns.count()}")
        
        # Тестируем поиск по SKU
        campaign_by_sku = ManualCampaign.objects.filter(sku=10001).first()
        if campaign_by_sku:
            print(f"✅ Найдена кампания по SKU 10001: {campaign_by_sku.name}")
        
        # Очищаем тестовые данные
        for campaign in created_campaigns:
            campaign.delete()
        print("✅ Тестовые кампании удалены")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования запросов: {e}")
        return False


def test_sync_task_import():
    """Тестирует импорт таска синхронизации"""
    print("\n🧪 Тестирование импорта таска синхронизации...")
    
    try:
        # Проверяем, что таск можно импортировать
        from ozon.tasks import sync_manual_campaigns, fetch_campaigns_from_ozon, fetch_campaign_objects_from_ozon
        
        print("✅ Таск sync_manual_campaigns успешно импортирован")
        print("✅ Функция fetch_campaigns_from_ozon успешно импортирована")
        print("✅ Функция fetch_campaign_objects_from_ozon успешно импортирована")
        
        # Проверяем, что это Celery таск
        if hasattr(sync_manual_campaigns, 'delay'):
            print("✅ Таск является Celery таском (есть метод delay)")
        else:
            print("⚠️ Таск не является Celery таском")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка импорта таска: {e}")
        return False


def test_token_function():
    """Тестирует функцию получения токена"""
    print("\n🧪 Тестирование функции получения токена...")
    
    try:
        store = OzonStore.objects.first()
        if not store:
            print("❌ Магазины не найдены в базе данных")
            return False
            
        # Проверяем, что у магазина есть необходимые поля для Performance API
        if not hasattr(store, 'performance_client_id') or not store.performance_client_id:
            print("⚠️ У магазина отсутствует performance_client_id")
            return False
            
        if not hasattr(store, 'performance_client_secret') or not store.performance_client_secret:
            print("⚠️ У магазина отсутствует performance_client_secret")
            return False
            
        print(f"✅ Магазин имеет необходимые поля для Performance API")
        print(f"   - performance_client_id: {store.performance_client_id[:10]}...")
        print(f"   - performance_client_secret: {'*' * len(store.performance_client_secret)}")
        
        # Проверяем, что можно импортировать функцию получения токена
        try:
            from ozon.utils import get_store_performance_token
            print("✅ Функция get_store_performance_token успешно импортирована")
        except ImportError as e:
            print(f"❌ Не удалось импортировать get_store_performance_token: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования функции токена: {e}")
        return False


def main():
    """Основная функция тестирования"""
    print("🚀 Запуск тестирования функциональности ручных рекламных кампаний\n")
    
    tests = [
        test_manual_campaign_model,
        test_campaign_validation,
        test_campaign_queries,
        test_sync_task_import,
        test_token_function
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
            print()
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте {test.__name__}: {e}\n")
    
    print(f"📊 Результаты тестирования: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("🎉 Все тесты пройдены успешно!")
        return True
    else:
        print("⚠️ Некоторые тесты не пройдены")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
