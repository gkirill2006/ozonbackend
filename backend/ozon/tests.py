from decimal import Decimal
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from users.models import (
    User,
    OzonStore,
    StoreFilterSettings,
    StoreRequiredProduct,
    StoreExcludedProduct,
)


class PlannerViewTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(telegram_id=1001, password="pass")
        self.other_user = User.objects.create_user(telegram_id=2002, password="pass")
        self.store = OzonStore.objects.create(
            user=self.user,
            name="Primary",
            client_id="client-1",
            api_key="api-1",
        )
        self.url = reverse("ozon-planner")

    def _create_filter_settings(self):
        settings = StoreFilterSettings.objects.create(
            store=self.store,
            planning_days=14,
            analysis_period=10,
            warehouse_weight=Decimal("1.00"),
            price_min=Decimal("100.00"),
            price_max=Decimal("500.00"),
            turnover_min=Decimal("5.00"),
            turnover_max=Decimal("25.00"),
            specific_weight_threshold=Decimal("0.0100"),
            turnover_from_stock=Decimal("3.00"),
            show_no_need=True,
            sort_by="revenue",
        )
        StoreRequiredProduct.objects.create(
            filter_settings=settings,
            article="DT830",
            quantity=50,
        )
        StoreExcludedProduct.objects.create(
            filter_settings=settings,
            article="CronsteinForConditioner",
        )
        return settings

    def test_requires_authentication(self):
        response = self.client.post(self.url, {"store_id": self.store.id}, format="json")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_requires_store_id(self):
        self.client.force_authenticate(self.user)
        response = self.client.post(self.url, {}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_store_must_belong_to_user(self):
        foreign_store = OzonStore.objects.create(
            user=self.other_user,
            name="Foreign",
            client_id="client-foreign",
            api_key="api-foreign",
        )
        self.client.force_authenticate(self.user)
        response = self.client.post(self.url, {"store_id": foreign_store.id}, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_returns_data_using_filter_settings(self):
        self._create_filter_settings()
        self.client.force_authenticate(self.user)
        response = self.client.post(self.url, {"store_id": self.store.id}, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("clusters", response.data)
        self.assertIn("summary", response.data)
        self.assertIsInstance(response.data["clusters"], list)
        self.assertIsInstance(response.data["summary"], list)
