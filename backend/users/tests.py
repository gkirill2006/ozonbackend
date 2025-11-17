from decimal import Decimal
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from .models import User, OzonStore, StoreFilterSettings


class UserStoreAPITests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(telegram_id=111111, password='testpass')
        self.other_user = User.objects.create_user(telegram_id=222222, password='testpass')

    def authenticate(self):
        self.client.force_authenticate(user=self.user)

    def _create_store(self, **kwargs):
        defaults = {
            'user': self.user,
            'name': 'Primary Store',
            'client_id': 'client-123',
            'api_key': 'api-key-123',
            'google_sheet_url': 'https://example.com/sheet',
            'performance_service_account_number': '1234567890',
            'performance_client_id': 'perf-client',
            'performance_client_secret': 'perf-secret',
        }
        defaults.update(kwargs)
        return OzonStore.objects.create(**defaults)

    def test_user_can_create_store(self):
        self.authenticate()
        payload = {
            'name': 'My Store',
            'client_id': 'client-abc',
            'api_key': 'api-key-abc',
            'google_sheet_url': 'https://example.com/sheet1',
            'performance_service_account_number': '9876543210',
            'performance_client_id': 'perf-client-1',
            'performance_client_secret': 'perf-secret-1',
        }
        url = reverse('user-store-list')
        response = self.client.post(url, payload, format='json')

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(OzonStore.objects.filter(id=response.data['id'], user=self.user).exists())

    def test_user_can_update_own_store(self):
        store = self._create_store(name='Old Name')
        self.authenticate()
        url = reverse('user-store-detail', args=[store.pk])
        response = self.client.patch(url, {'name': 'New Name'}, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        store.refresh_from_db()
        self.assertEqual(store.name, 'New Name')

    def test_user_can_delete_own_store(self):
        store = self._create_store()
        self.authenticate()
        url = reverse('user-store-detail', args=[store.pk])
        response = self.client.delete(url)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(OzonStore.objects.filter(pk=store.pk).exists())

    def test_user_cannot_modify_foreign_store(self):
        foreign_store = self._create_store(user=self.other_user, client_id='foreign-client', api_key='foreign-key')
        self.authenticate()
        url = reverse('user-store-detail', args=[foreign_store.pk])
        response = self.client.patch(url, {'name': 'Hacked'}, format='json')

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_get_filter_settings_creates_defaults(self):
        store = self._create_store()
        self.authenticate()
        url = reverse('store-filter-settings', args=[store.pk])

        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['store_id'], store.pk)
        self.assertEqual(response.data['planning_days'], 28)
        self.assertEqual(response.data['required_products'], [])
        self.assertEqual(response.data['excluded_products'], [])
        self.assertTrue(StoreFilterSettings.objects.filter(store=store).exists())

    def test_update_filter_settings(self):
        store = self._create_store()
        self.authenticate()
        url = reverse('store-filter-settings', args=[store.pk])

        payload = {
            'planning_days': 14,
            'price_min': 1500,
            'price_max': 2500,
            'show_no_need': True,
            'sort_by': 'revenue',
            'required_products': [
                {'article': 'DT830', 'quantity': 50},
                {'article': 'IEC320', 'quantity': 30},
            ],
            'excluded_products': [
                {'article': 'CronsteinForConditioner'},
                {'article': 'Dream WF-0963'},
            ],
        }

        response = self.client.patch(url, payload, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        settings = StoreFilterSettings.objects.get(store=store)
        self.assertEqual(len(response.data['required_products']), 2)
        self.assertEqual(len(response.data['excluded_products']), 2)
        self.assertEqual(settings.planning_days, 14)
        self.assertEqual(settings.sort_by, 'revenue')
        self.assertEqual(settings.show_no_need, True)
        self.assertEqual(settings.price_min, Decimal('1500'))
        self.assertEqual(settings.price_max, Decimal('2500'))
        self.assertEqual(settings.required_products.count(), 2)
        self.assertEqual(settings.excluded_products.count(), 2)
        self.assertTrue(settings.required_products.filter(article='DT830', quantity=50).exists())
        self.assertTrue(settings.excluded_products.filter(article='Dream WF-0963').exists())

    def test_user_cannot_access_foreign_store_filters(self):
        store = self._create_store(user=self.other_user, client_id='foreign', api_key='foreign-key')
        self.authenticate()
        url = reverse('store-filter-settings', args=[store.pk])

        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
