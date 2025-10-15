import requests
from celery import shared_task
from django.utils import timezone
from users.models import OzonStore
from .models import (DeliveryCluster, DeliveryClusterItemAnalytics, DeliveryAnalyticsSummary, Category, ProductType,
                     Product, WarehouseStock, Sale, FbsStock, ProductDailyAnalytics, AdPlanItem,
                     OzonStore, ManualCampaign, CampaignPerformanceReportEntry)

from .utils import create_cpc_product_campaign, update_campaign_budget, activate_campaign, deactivate_campaign

import json
from collections import defaultdict
import time
from decimal import Decimal, ROUND_HALF_UP
from django.db.models import Sum, F, ExpressionWrapper, DecimalField
from django.utils import timezone
from datetime import date as dt_date, timedelta
from math import ceil
from functools import reduce
from operator import or_
from django.db import models, transaction
from datetime import datetime, date, timedelta
import calendar
import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import CellFormat, Color, format_cell_ranges
from gspread.utils import rowcol_to_a1

import time

import logging
import os
logger = logging.getLogger(__name__)





# Обновляем каталоги для всех магазинов
@shared_task(name="Обновление каталогов для всех магазинов")
def sync_all_ozon_categories():
    stores = OzonStore.objects.all()
    for store in stores:
        try:
            logger.info(f"[▶️] Начинаем синхронизацию категорий для магазина: {store}")
            fetch_and_save_category_tree(store.client_id, store.api_key)
            logger.info(f"[✅] Завершено для магазина: {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка для магазина {store}: {e}")
def fetch_and_save_category_tree(client_id, api_key):
    url = "https://api-seller.ozon.ru/v1/description-category/tree"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json={})
    if response.status_code != 200:
        raise Exception(f"Ozon API error: {response.status_code} {response.text}")

    tree = response.json().get("result", [])

    def process_node(node, parent_category=None):
        if "description_category_id" in node:
            category, _ = Category.objects.update_or_create(
                category_id=node["description_category_id"],
                defaults={
                    "name": node.get("category_name", ""),
                    "disabled": node.get("disabled", False)
                }
            )
            for child in node.get("children", []):
                process_node(child, parent_category=category)

        elif "type_id" in node:
            ProductType.objects.update_or_create(
                type_id=node["type_id"],
                defaults={
                    "name": node.get("type_name", ""),
                    "disabled": node.get("disabled", False),
                    "category": parent_category
                }
            )

    for node in tree:
        process_node(node)
        
#Обновление и добавление товаров
@shared_task(name="Обновление и добавление товаров")
def sync_all_products():
    stores = OzonStore.objects.all()
    for store in stores:
        try:
            logger.info(f"[▶️] Начинаем синхронизацию товаров для магазина: {store}")
            _sync_products_for_store(store)
            logger.info(f"[✅] Синхронизация завершена: {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при синхронизации товаров {store}: {e}")
def _sync_products_for_store(store):
    basic_items = fetch_all_products_from_ozon(store.client_id, store.api_key)
    product_ids = [item["product_id"] for item in basic_items]
    detailed_items = fetch_detailed_products_from_ozon(store.client_id, store.api_key, product_ids)

    total_saved = 0
    for item in detailed_items:
        type_id = item.get("type_id")
        category_id = item.get("description_category_id")

        type_name = ""
        category_name = ""

        if type_id:
            type_obj = ProductType.objects.filter(type_id=type_id).first()
            if type_obj:
                type_name = type_obj.name

        if category_id:
            category_obj = Category.objects.filter(category_id=category_id).first()
            if category_obj:
                category_name = category_obj.name

        Product.objects.update_or_create(
            store=store,
            product_id=item["id"],
            defaults={
                "sku": item["sources"][0]["sku"] if item.get("sources") else None,
                "offer_id": item.get("offer_id", ""),
                "name": item.get("name", ""),
                "barcodes": item.get("barcodes", []),
                "category": category_name,
                "type_name": type_name,
                "type_id": type_id,
                "description_category_id": category_id,
                "price": float(item["price"]) if item.get("price") else None,
                "is_archived": item.get("is_archived", False),
                "is_autoarchived": item.get("is_autoarchived", False),
                "is_discounted": item.get("is_discounted", False),
                "is_kgt": item.get("is_kgt", False),
                "is_super": item.get("is_super", False),
                "is_seasonal": item.get("is_seasonal", False),
                "is_prepayment_allowed": item.get("is_prepayment_allowed", False),
                "primary_image": (item.get("primary_image") or [None])[0],
            }
        )
        total_saved += 1

    logger.info(f"[📦] Сохранено {total_saved} товаров для {store}")
def fetch_all_products_from_ozon(client_id, api_key):
    """
    Возвращает все товары с Ozon API.
    """
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    url = "https://api-seller.ozon.ru/v3/product/list"
    last_id = ""
    all_items = []

    while True:
        payload = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": 1000
        }

        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            raise Exception(f"Ozon API error: {resp.status_code} {resp.text}")

        data = resp.json().get("result", {})
        items = data.get("items", [])
        all_items.extend(items)

        last_id = data.get("last_id")
        if not last_id:
            break

    return all_items

# Синхронизация остатков на складах    
@shared_task(name="Синхронизация остатков на складах")
def sync_all_warehouse_stocks():
    stores = OzonStore.objects.all()
    for store in stores:
        try:
            logger.info(f"[🏬] Синхронизация остатков для {store}")
            sync_warehouse_stock_for_store(store)
            logger.info(f"[✅] Остатки обновлены для {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при синхронизации остатков {store}: {e}")
def sync_warehouse_stock_for_store(store):
    # Собираем все SKU
    skus = list(
        Product.objects.filter(store=store)
        .exclude(sku__isnull=True)
        .values_list("sku", flat=True)
    )

    if not skus:
        logger.info(f"[ℹ️] Нет SKU для магазина {store}, пропускаем")
        return

    # Получаем остатки по API
    stock_items = fetch_warehouse_stock(store.client_id, store.api_key, skus)

    # Удаляем старые остатки
    WarehouseStock.objects.filter(store=store).delete()

    updated_count = 0
    for item in stock_items:
        sku = item["sku"]
        product = Product.objects.filter(store=store, sku=sku).first()

        WarehouseStock.objects.update_or_create(
            store=store,
            sku=sku,
            cluster_id=item.get("cluster_id"),
            warehouse_id=item.get("warehouse_id"),
            defaults={
                "product": product,
                "warehouse_name": item.get("warehouse_name", ""),
                "available_stock_count": item.get("available_stock_count", 0),
                "valid_stock_count": item.get("valid_stock_count", 0),
                "waiting_docs_stock_count": item.get("waiting_docs_stock_count", 0),
                "expiring_stock_count": item.get("expiring_stock_count", 0),
                "transit_defect_stock_count": item.get("transit_defect_stock_count", 0),
                "stock_defect_stock_count": item.get("stock_defect_stock_count", 0),
                "excess_stock_count": item.get("excess_stock_count", 0),
                "other_stock_count": item.get("other_stock_count", 0),
                "requested_stock_count": item.get("requested_stock_count", 0),
                "transit_stock_count": item.get("transit_stock_count", 0),
                "return_from_customer_stock_count": item.get("return_from_customer_stock_count", 0),
                "cluster_name": item.get("cluster_name", ""),
            }
        )
        updated_count += 1

    logger.info(f"[📦] Обновлено {updated_count} остатков для магазина {store}")
def fetch_warehouse_stock(client_id, api_key, skus: list):
    url = "https://api-seller.ozon.ru/v1/analytics/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    all_results = []

    for i in range(0, len(skus), 100):
        batch = skus[i:i + 100]
        payload = {"skus": batch}

        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            raise Exception(f"Ozon API error: {resp.status_code} {resp.text}")

        data = resp.json().get("items", [])
        all_results.extend(data)

    return all_results

# Синхронизация продаж    
@shared_task(name="Синхронизация продаж")
def sync_all_sales(days=1):
    stores = OzonStore.objects.all()
    for store in stores:
        try:
            logger.info(f"[💰] Синхронизация продаж за {days} дней для магазина {store}")
            sync_sales_for_store(store, days)
            logger.info(f"[✅] Продажи обновлены для магазина {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при синхронизации продаж для {store}: {e}")
def sync_sales_for_store(store, days):
    from .utils import fetch_fbo_sales, fetch_fbs_sales 
    from django.utils import timezone
    from datetime import timedelta
    
    # Определяем период для загрузки данных
    if not Sale.objects.filter(store=store).exists():
        logger.info(f"[🆕] Новый магазин {store}, загружаем данные за 60 дней")
        days = 60
    else:
        # Находим последнюю запись по created_at
        if days==1:
            last_sale = Sale.objects.filter(store=store).order_by('-created_at').first()
            if last_sale:
                # Вычисляем количество дней с последней записи + 1 день для перестраховки
                days_since_last = (timezone.now() - last_sale.created_at).days
                days = max(days_since_last + 1, 1)  # Минимум 1 день
                logger.info(f"[📅] Последняя запись {store}: {last_sale.created_at}, загружаем данные за {days} дней")
            else:
                logger.info(f"[⚠️] Не удалось найти последнюю запись для {store}, используем {days} дней")
    
    fbo_sales = fetch_fbo_sales(store.client_id, store.api_key, days)
    fbs_sales = fetch_fbs_sales(store.client_id, store.api_key, days)

    total_created = 0
    total_updated = 0

    for sale_data in fbo_sales + fbs_sales:
        obj, created = Sale.objects.update_or_create(
            posting_number=sale_data["posting_number"],
            sku=sale_data["sku"],
            sale_type=sale_data["sale_type"],
            defaults={
                "store": store,
                "date": sale_data["date"],
                "price": sale_data["price"],
                "quantity": sale_data["quantity"],
                "payout": sale_data["payout"],
                "commission_amount": sale_data["commission_amount"],
                "warehouse_id": sale_data["warehouse_id"],
                "cluster_from": sale_data["cluster_from"],
                "cluster_to": sale_data["cluster_to"],
                "status": sale_data["status"],
                "customer_price": sale_data.get("customer_price"),
                "tpl_provider": sale_data.get("tpl_provider"),
            }
        )
        if created:
            total_created += 1
        else:
            total_updated += 1

    logger.info(f"[📈] Продаж создано: {total_created}, обновлено: {total_updated} для {store}")

# Синхронизация остатков FBS        
@shared_task(name="Синхронизация остатков FBS")
def sync_all_fbs_stocks():
    stores = OzonStore.objects.all()
    for store in stores:
        try:
            logger.info(f"[📦] Синхронизация FBS остатков для {store}")
            _sync_fbs_stock_for_store(store)
            logger.info(f"[✅] FBS остатки обновлены для {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при FBS-синхронизации для {store}: {e}")
def _sync_fbs_stock_for_store(store):
    from .utils import fetch_fbs_stocks  # или без импорта, если функция рядом

    skus = list(
        Product.objects.filter(store=store)
        .exclude(sku__isnull=True)
        .values_list("sku", flat=True)
    )

    if not skus:
        logger.info(f"[ℹ️] Нет SKU для магазина {store}, пропускаем")
        return

    stock_items = fetch_fbs_stocks(store.client_id, store.api_key, skus)

    FbsStock.objects.filter(store=store).delete()

    stock_objects = [
        FbsStock(
            store=store,
            product_id=item.get("product_id"),
            sku=item.get("sku"),
            fbs_sku=item.get("sku"),
            present=item.get("present", 0),
            reserved=item.get("reserved", 0),
            warehouse_id=item.get("warehouse_id"),
            warehouse_name=item.get("warehouse_name", "")
        )
        for item in stock_items
    ]

    FbsStock.objects.bulk_create(stock_objects)
    logger.info(f"[📦] Сохранено {len(stock_objects)} FBS-остатков для {store}")



def fetch_detailed_products_from_ozon(client_id, api_key, product_ids):
    """
    Делает батч-запросы по 1000 product_id к Ozon /v3/product/info/list
    """
    url = "https://api-seller.ozon.ru/v3/product/info/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    all_results = []

    for i in range(0, len(product_ids), 1000):
        batch = product_ids[i:i + 1000]
        payload = {"product_id": batch}

        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            raise Exception(f"Ozon API error: {resp.status_code} {resp.text}")

        data = resp.json().get("items", [])
        all_results.extend(data)

    return all_results




# ОБЩАЯ АНАЛИТИКА ПО КЛАСТЕРУ выгрузка в БД
OZON_ANALYTICS_URL = "https://api-seller.ozon.ru/v1/analytics/average-delivery-time"
OZON_CLUSTER_URL = "https://api-seller.ozon.ru/v1/cluster/list"

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
        
@shared_task
def update_delivery_clusters():
    for store in OzonStore.objects.all():
        headers = {
            "Client-Id": store.client_id,
            "Api-Key": store.api_key,
            "Content-Type": "application/json"
        }

        try:
            # 1. Получаем метрики доставки
            response = requests.post(OZON_ANALYTICS_URL, json={"delivery_schema": "ALL"}, headers=headers)
            response.raise_for_status()
            
            
            # Сохраняем общий total-блок
            total = response.json().get("total", {})

            if total:
                orders_count = total.get("orders_count", {})
                total_orders = orders_count.get("total", 0)

                DeliveryAnalyticsSummary.objects.update_or_create(
                    store=store,
                    defaults={
                        "average_delivery_time": total.get("average_delivery_time", 0),
                        "average_delivery_time_status": total.get("average_delivery_time_status", ""),
                        "total_orders": total_orders,
                        "lost_profit": total.get("lost_profit", 0),
                        "impact_share": total.get("exact_impact_share", 0),
                        "attention_level": total.get("attention_level", ""),
                        "recommended_supply": total.get("recommended_supply", 0),
                    }
    )
            
            
            data = response.json().get("data", [])
            cluster_ids = []
            metrics_map = {}

            for item in data:
                cluster_id = item["delivery_cluster_id"]
                metrics = item["metrics"]
                cluster_ids.append(cluster_id)
                metrics_map[cluster_id] = metrics

            if not cluster_ids:
                continue

            # 2. Получаем названия кластеров
            valid_cluster_ids = [cid for cid in cluster_ids if cid and int(cid) > 0]

            for cluster_ids_chunk in chunked(valid_cluster_ids, 10):
                print(json.dumps([str(cid) for cid in cluster_ids_chunk]))
                cluster_resp = requests.post(OZON_CLUSTER_URL, json={
                    "cluster_ids": [str(cid) for cid in cluster_ids_chunk],
                    "cluster_type": "CLUSTER_TYPE_OZON"
                }, headers=headers)
                
                cluster_resp.raise_for_status()
                cluster_info = cluster_resp.json().get("clusters", [])

                for cluster in cluster_info:
                    cid = cluster["id"]
                    name = cluster["name"]
                    ctype = cluster["type"]
                    metrics = metrics_map.get(cid)
                    if not metrics:
                        continue

                    DeliveryCluster.objects.update_or_create(
                        store=store,
                        delivery_cluster_id=cid,
                        defaults={
                            "name": name,
                            "type": ctype,
                            "average_delivery_time": metrics["average_delivery_time"],
                            "impact_share": metrics["exact_impact_share"],
                            "lost_profit": metrics["lost_profit"],
                            "recommended_supply": metrics["recommended_supply"]
                        }
                    )
        except Exception as e:
            print(f"[{store}] Ошибка при обновлении кластеров: {e}")



# ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ
@shared_task(name="ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ")
def update_cluster_item_analytics():
    for store in OzonStore.objects.all():
        headers = {
            "Client-Id": store.client_id,
            "Api-Key": store.api_key,
            "Content-Type": "application/json"
        }
        cluster_names = {
            c.delivery_cluster_id: c.name
            for c in DeliveryCluster.objects.filter(store=store)
        }

        all_skus_seen = set()
        print(cluster_names)
        for cluster_id in cluster_names:
            print(f"Обновление аналитики для кластера {cluster_id} ({cluster_names[cluster_id]}) в магазине {store}")
            
            # Цикл для FBS
            print(f"Получение данных FBS для кластера {cluster_id}")
            offset = 0
            limit = 1000
            while True:
                payload = {
                    "cluster_id": cluster_id,
                    "limit": limit,
                    "offset": offset,
                    "filters": {
                        "delivery_schema": "FBS",
                        "supply_period": "FOUR_WEEKS"
                    }
                }

                try:
                    resp = requests.post(
                        "https://api-seller.ozon.ru/v1/analytics/average-delivery-time/details",
                        json=payload,
                        headers=headers
                    )
                    resp.raise_for_status()
                    data = resp.json().get("data", [])

                    if not data:
                        break

                    for entry in data:
                        item = entry["item"]
                        metrics = entry["metrics"]
                        if cluster_id == 154 and item["sku"] == 787678187:
                            logging.info(f"Получено FBS для {item['sku']}: {metrics['recommended_supply']}")
                        # Получаем существующую запись или создаем новую
                        analytics_obj, created = DeliveryClusterItemAnalytics.objects.update_or_create(
                            store=store,
                            cluster_id=cluster_id,
                            sku=item["sku"],
                            defaults={
                                "offer_id": item["offer_id"],
                                "cluster_name": cluster_names.get(cluster_id, ""),
                                "delivery_schema": item["delivery_schema"],
                                "average_delivery_time": metrics["average_delivery_time"],
                                "average_delivery_time_status": metrics["average_delivery_time_status"],
                                "impact_share": metrics["exact_impact_share"],
                                "attention_level": metrics["attention_level"],
                                "recommended_supply": metrics["recommended_supply"],
                                "recommended_supply_FBS": metrics["recommended_supply"]
                            }
                        )
                        
                        # Если запись уже существовала, обновляем только FBS поле
                        if not created:
                            analytics_obj.recommended_supply_FBS = metrics["recommended_supply"]
                            analytics_obj.save(update_fields=['recommended_supply_FBS'])

                        all_skus_seen.add((store.id, cluster_id, item["sku"]))

                    if len(data) < limit:
                        break
                    offset += limit
                except Exception as e:
                    print(f"[{store}] Ошибка при получении деталей FBS по кластеру {cluster_id}: {e}")
                    break

            # Цикл для FBO
            print(f"Получение данных FBO для кластера {cluster_id}")
            offset = 0
            while True:
                payload = {
                    "cluster_id": cluster_id,
                    "limit": limit,
                    "offset": offset,
                    "filters": {
                        "delivery_schema": "FBO",
                        "supply_period": "FOUR_WEEKS"
                    }
                }

                try:
                    resp = requests.post(
                        "https://api-seller.ozon.ru/v1/analytics/average-delivery-time/details",
                        json=payload,
                        headers=headers
                    )
                    resp.raise_for_status()
                    data = resp.json().get("data", [])

                    if not data:
                        break

                    for entry in data:
                        item = entry["item"]
                        metrics = entry["metrics"]
                        
                        # Получаем существующую запись или создаем новую
                        analytics_obj, created = DeliveryClusterItemAnalytics.objects.update_or_create(
                            store=store,
                            cluster_id=cluster_id,
                            sku=item["sku"],
                            defaults={
                                "offer_id": item["offer_id"],
                                "cluster_name": cluster_names.get(cluster_id, ""),
                                "delivery_schema": item["delivery_schema"],
                                "average_delivery_time": metrics["average_delivery_time"],
                                "average_delivery_time_status": metrics["average_delivery_time_status"],
                                "impact_share": metrics["exact_impact_share"],
                                "attention_level": metrics["attention_level"],
                                "recommended_supply": metrics["recommended_supply"],
                                "recommended_supply_FBO": metrics["recommended_supply"]
                            }
                        )
                        
                        # Если запись уже существовала, обновляем только FBO поле
                        if not created:
                            analytics_obj.recommended_supply_FBO = metrics["recommended_supply"]
                            analytics_obj.save(update_fields=['recommended_supply_FBO'])

                        all_skus_seen.add((store.id, cluster_id, item["sku"]))

                    if len(data) < limit:
                        break
                    offset += limit
                except Exception as e:
                    print(f"[{store}] Ошибка при получении деталей FBO по кластеру {cluster_id}: {e}")
                    break

        # Обновляем recommended_supply максимальным значением между FBO и FBS
        print(f"Обновление recommended_supply для магазина {store}")
        analytics_records = DeliveryClusterItemAnalytics.objects.filter(store=store)
        
        for record in analytics_records:
            fbo_supply = record.recommended_supply_FBO if record.recommended_supply_FBO is not None else 0
            fbs_supply = record.recommended_supply_FBS if record.recommended_supply_FBS is not None else 0
            max_supply = max(fbo_supply, fbs_supply)
            
            if record.recommended_supply != max_supply:
                record.recommended_supply = max_supply
                record.save(update_fields=['recommended_supply'])

        # Удаляем неактуальные записи
        to_keep = reduce(or_, [models.Q(cluster_id=cid, sku=sku) for (_, cid, sku) in all_skus_seen], models.Q(pk=None))

        DeliveryClusterItemAnalytics.objects.filter(store=store).exclude(to_keep).delete()
        
        
@shared_task(name="При создании нового магазина запускается этот таск")
def sync_full_store_data(store_id):
    try:
        store = OzonStore.objects.get(id=store_id)
        logger.info(f"[🔄] Полная инициализация магазина {store}")

        # Товары
        _sync_products_for_store(store)

        # Остатки FBO
        sync_warehouse_stock_for_store(store)

        # Остатки FBS
        _sync_fbs_stock_for_store(store)

        # Продажи (глубина по умолчанию определится сама)
        sync_sales_for_store(store, days=60)

        logger.info(f"[✅] Полная инициализация завершена для магазина {store}")
    except Exception as e:
        logger.error(f"[❌] Ошибка при полной инициализации магазина: {e}")
        raise e


# =========================
# АНАЛИТИКА: /v1/analytics/data
# =========================

ANALYTICS_DATA_URL = "https://api-seller.ozon.ru/v1/analytics/data"

def _ozon_headers(store: OzonStore) -> dict:
    return {
        "Client-Id": store.client_id,
        "Api-Key": store.api_key,
        "Content-Type": "application/json",
    }

def _post_with_rate_limit(url: str, headers: dict, payload: dict, max_retries: int = 6):
    """
    POST с обработкой ответа об ограничении частоты запросов Ozon.  
    Ожидать 5 секунд и повторить попытку, если code==8 и сообщение указывает на ограничение частоты.  
    max_retries ограничивает общее время ожидания примерно одной минутой.  

    """
    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            body = resp.json()
            # Ozon sometimes returns 200 with error payloads for rate limit
            if isinstance(body, dict) and body.get("code") == 8:
                logger.info("[⏳] Rate limit hit (code 8). Sleeping 10s before retry...")
                time.sleep(5)
                continue
            return resp
        # Non-200: check rate limit payload
        try:
            body = resp.json()
        except Exception:
            body = None
        if isinstance(body, dict) and body.get("code") == 8:
            logger.info("[⏳] Rate limit hit (non-200). Sleeping 10s before retry...")
            time.sleep(10)
            continue
        # Other errors
        resp.raise_for_status()
    raise Exception("Exceeded max retries due to rate limiting on Ozon analytics/data")


def _iter_analytics_pages(store: OzonStore, date_from: str, date_to: str):
    headers = _ozon_headers(store)
    limit = 1000
    # По примеру из запроса пользователя offset=1
    offset = 1
    while True:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["revenue", "ordered_units"],
            "dimension": ["sku", "day"],
            "filters": [],
            "sort": [{"key": "hits_view_search", "order": "DESC"}],
            "limit": limit,
            "offset": offset,
        }
        resp = _post_with_rate_limit(ANALYTICS_DATA_URL, headers, payload)
        data = resp.json().get("result", {}).get("data", [])
        if not data:
            break
        yield data
        if len(data) < limit:
            break
        # Смещаемся на следующий блок
        offset += limit


def _save_analytics_batch(store: OzonStore, rows: list):

    # Map of sku -> (offer_id, name)
    skus = []
    for row in rows:
        dims = row.get("dimensions", [])
        if len(dims) >= 1:
            sku_str = dims[0].get("id")
            try:
                skus.append(int(sku_str))
            except Exception:
                continue

    product_map = {
        p.sku: (p.offer_id, p.name)
        for p in Product.objects.filter(store=store, sku__in=skus)
    }

    objects_to_upsert = []
    for row in rows:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [])
        if len(dims) < 2 or len(metrics) < 2:
            continue
        sku_str = dims[0].get("id")
        date_id_str = dims[1].get("id")  # например: "2025-08-01"
        name_value = dims[0].get("name", "")
        try:
            sku_val = int(sku_str)
        except Exception:
            continue
        offer_id_val, product_name_val = product_map.get(sku_val, ("", name_value))

        # Преобразуем типы корректно
        revenue_val = Decimal(str(metrics[0] or 0))
        ordered_units_val = int(metrics[1] or 0)

        # Дата как date-объект
        try:
            date_val = dt_date.fromisoformat(date_id_str)
        except Exception:
            # Пропустим строку, если дата некорректна
            continue

        objects_to_upsert.append(
            ProductDailyAnalytics(
                store=store,
                sku=sku_val,
                offer_id=offer_id_val,
                name=product_name_val,
                date=date_val,
                revenue=revenue_val,
                ordered_units=ordered_units_val,
            )
        )

    # Upsert by unique (store, date, sku)
    for obj in objects_to_upsert:
        ProductDailyAnalytics.objects.update_or_create(
            store=obj.store, date=obj.date, sku=obj.sku,
            defaults={
                "offer_id": obj.offer_id,
                "name": obj.name,
                "revenue": obj.revenue,
                "ordered_units": obj.ordered_units,
            }
        )


@shared_task(name="Синхронизация ежедневной аналитики по товарам")
def sync_product_daily_analytics():
    """
    Ежедневно:
    - если записей нет, грузим за последние 30 дней;
    - иначе обновляем данные за прошедшие 10 дней (данные Озона могут меняться).
    
    Важно: Озон может обновлять данные аналитики в течение 10 дней после даты,
    поэтому мы обновляем данные за этот период каждый день для получения
    наиболее актуальной информации.
    """
    for store in OzonStore.objects.all():
        try:
            if not ProductDailyAnalytics.objects.filter(store=store).exists():
                # Первичная загрузка: последние 30 дней
                date_to = dt_date.today() - timedelta(days=1)
                date_from = date_to - timedelta(days=29)
                logger.info(f"[📊] {store}: первичная загрузка аналитики {date_from}..{date_to}")
            else:
                # Ежедневное обновление: прошедшие 10 дней для актуализации данных
                date_to = dt_date.today() - timedelta(days=1)
                date_from = date_to - timedelta(days=9)  # 10 дней включая вчерашний
                logger.info(f"[📊] {store}: обновление аналитики за прошедшие 10 дней ({date_from}..{date_to})")

            df_str = date_from.strftime("%Y-%m-%d")
            dt_str = date_to.strftime("%Y-%m-%d")

            for page in _iter_analytics_pages(store, df_str, dt_str):
                _save_analytics_batch(store, page)

            logger.info(f"[✅] {store}: аналитика обновлена за период {df_str}..{dt_str}")
        except Exception as e:
            logger.error(f"[❌] Ошибка загрузки аналитики для {store}: {e}")





# =========================
# GOOGLE SHEETS: ABC отчёт
# =========================


def _update_campaign_from_ozon_response(ad_plan_item: AdPlanItem, api_response: dict):
    """
    Обновляет данные кампании AdPlanItem из ответа Ozon Performance API.
    
    Args:
        ad_plan_item: Экземпляр AdPlanItem для обновления
        api_response: Ответ от API активации кампании
    """
    if not api_response or not isinstance(api_response, dict):
        logger.warning(f"[⚠️] Пустой или некорректный ответ API для кампании {ad_plan_item.ozon_campaign_id}")
        return
    
    update_fields = []
    
    # Обновляем статус кампании
    if 'state' in api_response and api_response['state']:
        ad_plan_item.state = api_response['state']
        update_fields.append('state')
        logger.debug(f"[📝] Статус кампании: {api_response['state']}")
    
    # Обновляем тип оплаты
    if 'paymentType' in api_response and api_response['paymentType']:
        ad_plan_item.payment_type = api_response['paymentType']
        update_fields.append('payment_type')
    
    # Обновляем бюджеты (конвертируем из микрорублей)
    if 'budget' in api_response and api_response['budget']:
        try:
            total_budget_micros = int(api_response['budget'])
            ad_plan_item.total_budget = Decimal(total_budget_micros) / Decimal('1000000')
            update_fields.append('total_budget')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректный общий бюджет: {api_response['budget']}")
    
    if 'weeklyBudget' in api_response and api_response['weeklyBudget']:
        try:
            weekly_budget_micros = int(api_response['weeklyBudget'])
            ad_plan_item.week_budget = Decimal(weekly_budget_micros) / Decimal('1000000')
            update_fields.append('week_budget')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректный недельный бюджет: {api_response['weeklyBudget']}")
    
    if 'dailyBudget' in api_response and api_response['dailyBudget']:
        try:
            daily_budget_micros = int(api_response['dailyBudget'])
            ad_plan_item.day_budget = Decimal(daily_budget_micros) / Decimal('1000000')
            update_fields.append('day_budget')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректный дневной бюджет: {api_response['dailyBudget']}")
    
    # Обновляем даты
    if 'fromDate' in api_response and api_response['fromDate']:
        try:
            ad_plan_item.from_date = datetime.strptime(api_response['fromDate'], '%Y-%m-%d').date()
            update_fields.append('from_date')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректная дата начала: {api_response['fromDate']}")
    
    if 'toDate' in api_response and api_response['toDate']:
        try:
            ad_plan_item.to_date = datetime.strptime(api_response['toDate'], '%Y-%m-%d').date()
            update_fields.append('to_date')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректная дата окончания: {api_response['toDate']}")
    
    # Обновляем размещение и стратегию
    if 'placement' in api_response and api_response['placement']:
        if isinstance(api_response['placement'], list):
            ad_plan_item.placement = ', '.join(api_response['placement'])
        else:
            ad_plan_item.placement = str(api_response['placement'])
        update_fields.append('placement')
    
    if 'productAutopilotStrategy' in api_response and api_response['productAutopilotStrategy']:
        ad_plan_item.product_autopilot_strategy = api_response['productAutopilotStrategy']
        update_fields.append('product_autopilot_strategy')
    
    # Обновляем временные метки из Ozon
    if 'createdAt' in api_response and api_response['createdAt']:
        try:
            ad_plan_item.ozon_created_at = datetime.fromisoformat(api_response['createdAt'].replace('Z', '+00:00'))
            update_fields.append('ozon_created_at')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректная дата создания: {api_response['createdAt']}")
    
    if 'updatedAt' in api_response and api_response['updatedAt']:
        try:
            ad_plan_item.ozon_updated_at = datetime.fromisoformat(api_response['updatedAt'].replace('Z', '+00:00'))
            update_fields.append('ozon_updated_at')
        except (ValueError, TypeError):
            logger.warning(f"[⚠️] Некорректная дата обновления: {api_response['updatedAt']}")
    
    # Сохраняем изменения
    if update_fields:
        ad_plan_item.save(update_fields=update_fields)
        logger.info(f"[💾] Обновлены поля кампании {ad_plan_item.ozon_campaign_id}: {', '.join(update_fields)}")
    else:
        logger.debug(f"[ℹ️] Нет данных для обновления кампании {ad_plan_item.ozon_campaign_id}")

def fetch_campaigns_from_ozon(store: OzonStore) -> list:
    """
    Получает список рекламных кампаний из Ozon Performance API.   
    """
    try:
        # Получаем токен для магазина
        from .utils import get_store_performance_token
        token_info = get_store_performance_token(store)
        access_token = token_info.get("access_token")
        
        if not access_token:
            logger.error(f"[❌] Не удалось получить access_token для магазина {store}")
            return []
        
        url = "https://api-performance.ozon.ru:443/api/client/campaign"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Получаем все кампании (без фильтров)
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            campaigns = data.get('list', [])
            
            # Если ответ содержит одну кампанию, оборачиваем в список
            if isinstance(campaigns, dict):
                campaigns = [campaigns]
                
            logger.info(f"[ℹ️] Получено {len(campaigns)} кампаний из Ozon Performance API для магазина {store}")
            return campaigns
        else:
            logger.error(f"[❌] Ошибка API Ozon Performance для магазина {store}: {response.status_code} {response.text}")
            return []
            
    except Exception as e:
        logger.error(f"[❌] Ошибка при получении кампаний для магазина {store}: {e}")
        return []

def fetch_campaign_objects_from_ozon(store: OzonStore, campaign_id: str) -> list:
    """
    Получает объекты (товары/SKU) рекламной кампании из Ozon Performance API.    
    Args:
        store: Объект магазина OzonStore
        campaign_id: ID кампании
        
    Returns:
        Список объектов кампании или пустой список при ошибке
    """
    try:
        # Получаем токен для магазина
        from .utils import get_store_performance_token
        token_info = get_store_performance_token(store)
        access_token = token_info.get("access_token")
        
        if not access_token:
            logger.error(f"[❌] Не удалось получить access_token для магазина {store}")
            return []
        
        url = f"https://api-performance.ozon.ru:443/api/client/campaign/{campaign_id}/objects"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            objects = data.get('list', [])
            logger.info(f"[ℹ️] Получено {len(objects)} объектов для кампании {campaign_id} магазина {store}")
            return objects
        else:
            logger.error(f"[❌] Ошибка API Ozon Performance для объектов кампании {campaign_id} магазина {store}: "
                        f"{response.status_code} {response.text}")
            return []
            
    except Exception as e:
        logger.error(f"[❌] Ошибка при получении объектов кампании {campaign_id} для магазина {store}: {e}")
        return []



# =========================
# update_abc_sheet
# =========================
# Общие вспомогательные функции для работы с настройками листа

def _read_mandatory_offer_ids(ws, column_index: int = 29, start_row: int = 13) -> tuple[list[str], set[str]]:
    """
    Возвращает список и множество обязательных offer_id, прочитанных из указанного столбца.
    Чтение прекращается при первом пустом значении после start_row.
    """
    mandatory_list: list[str] = []
    mandatory_set: set[str] = set()
    try:
        raw_values = ws.col_values(column_index)
    except Exception as err:
        logger.error(f"[❌] Ошибка при чтении списка обязательных товаров из колонки {column_index}: {err}")
        return mandatory_list, mandatory_set

    for raw in raw_values[start_row - 1:]:
        value = (raw or '').strip()
        if not value:
            break
        if value not in mandatory_set:
            mandatory_set.add(value)
            mandatory_list.append(value)
    return mandatory_list, mandatory_set

# Основная функция которая  создает ABC отчет. 
# обновляет Google‑таблицу с ABC‑анализом и бюджетами. Считает общий рекламный бюджет 
# как долю от выручки, при необходимости вычитает уже потраченное, 
# распределяет недельный/дневной бюджет по товарам, 
# формирует список TOP‑N и заполняет два листа: `ABC` и `Main_ADV`.   
# параметр consider_spent (0/1) — учитывать ли уже потраченный с начала месяца бюджет по Performance‑отчетам. 
# Если стоит 1, то бюджет будет персчитан с учетом уже потраченых средств в этом месяце, 
# остаток расчитываается именно до конца месяца


@shared_task(name="Обновление листа ABC из ProductDailyAnalytics")
def update_abc_sheet(spreadsheet_url: str = None, sa_json_path: str = None, consider_spent: int = 0):
    """
    Обновляет лист ABC из ProductDailyAnalytics.
    """
    
    spreadsheet_url = spreadsheet_url or None
    if not spreadsheet_url:
        return False
    
    # spreadsheet_url = "https://docs.google.com/spreadsheets/d/1eJO9j-Uod-3RI6RGrKuhyC2Ptv_oaa5RIggDXDXecmQ"
    sa_json_path = sa_json_path or os.getenv(
        "GOOGLE_SA_JSON_PATH",
        "/workspace/ozon-469708-c5f1eca77c02.json",
    )

    # Авторизация в Google Sheets
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
    gc = gspread.authorize(creds)
    t0 = time.perf_counter()
    sh = gc.open_by_url(spreadsheet_url)
    t_open = time.perf_counter(); logger.info(f"[⏱] Открытие таблицы: {t_open - t0:.3f}s")

    # Читаем параметры из Main_ADV одним батч-запросом
    ws_main = sh.worksheet('Main_ADV')
    start_row = 13  # начало блока кампаний на листе
    param_cells = ['V13','V14','V15','V16','W16','V17','V21','V18','V19','V20', 'V22', 'V23','V24','V25','V26', 'V27']
    param_vals = ws_main.batch_get([f'{c}:{c}' for c in param_cells])
    cell_value = {}
    
    def _get(cell_ref: str) -> str:
        return (cell_value.get(cell_ref) or '')
    
    for i, c in enumerate(param_cells):
        try:
            cell_value[c] = (param_vals[i][0][0] if param_vals[i] and param_vals[i][0] else '')
        except Exception:
            cell_value[c] = ''
            

    # T13 — строка вида "28 дней"/"3 дня"
    t13_value = _get('V13')
    digits = ''.join(ch for ch in (t13_value or '') if ch.isdigit())
    days = int(digits) if digits else 3
    # Вспомогательные парсеры чисел/процентов
    def _parse_decimal(cell_value: str, default: str = '0') -> Decimal:
        s = (cell_value or '').strip().replace(' ', '')
        cleaned = ''.join(ch for ch in s.replace(',', '.') if ch.isdigit() or ch == '.')
        if cleaned == '':
            cleaned = default
        try:
            return Decimal(cleaned)
        except Exception:
            return Decimal(default)

    def _parse_int(cell_value: str, default: int = 0) -> int:
        s = (cell_value or '').strip()
        digits_local = ''.join(ch for ch in s if ch.isdigit())
        return int(digits_local) if digits_local else default

    def _parse_percent(cell_value: str, default: Decimal = Decimal('0')) -> Decimal:
        val = _parse_decimal(cell_value, '0')
        # Трактуем целые значения как проценты: 1 -> 1% -> 0.01; 10 -> 10% -> 0.10
        # Значения уже в долях (<1) оставляем как есть (например 0.1)
        if val >= 1:
            return (val / Decimal('100'))
        return val

    # T23 — название магазина (регистр не учитываем)
    store_name_value = (_get('V23') or '').strip()
    store = None
    if store_name_value:
        store = (
            OzonStore.objects.filter(name__iexact=store_name_value).first()
            or OzonStore.objects.filter(client_id__iexact=store_name_value).first()
            or OzonStore.objects.filter(name__icontains=store_name_value).first()
            or OzonStore.objects.filter(client_id__icontains=store_name_value).first()
        )
    # Сохраняем текущие значения ручных бюджетов (колонка K) по SKU
    def _extract_sku(value) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        try:
            s = str(value).strip()
        except Exception:
            return None
        digits = ''.join(ch for ch in s if ch.isdigit())
        if not digits:
            return None
        try:
            return int(digits)
        except ValueError:
            return None

    existing_manual_budget_by_sku: dict[int, Decimal] = {}
    try:
        col_g_values = ws_main.col_values(7)  # колонки G: SKU
        total_rows = len(col_g_values)
        end_row = total_rows if total_rows >= start_row else start_row - 1
        if end_row >= start_row:
            existing_rows = ws_main.get(f'G{start_row}:K{end_row}')
            for row in existing_rows:
                if not row or len(row) < 5:
                    continue
                sku_raw = row[0]
                manual_budget_raw = row[4] if len(row) > 4 else ''
                manual_budget_raw = '' if manual_budget_raw is None else str(manual_budget_raw).strip()
                sku_int = _extract_sku(sku_raw)
                if sku_int is None or manual_budget_raw == '':
                    continue
                manual_budget_dec = _parse_decimal(manual_budget_raw, manual_budget_raw or '0')
                existing_manual_budget_by_sku[sku_int] = manual_budget_dec
    except Exception as e:
        logger.warning(f"[⚠️] Не удалось прочитать существующие ручные бюджеты из листа: {e}")

    # Исправлено описание координат: используем реальные V13..V27
    t_params = time.perf_counter(); logger.info(f"[⏱] Чтение параметров (V13..V27): {t_params - t_open:.3f}s")
    if not store:
        logger.warning(f"[⚠️] Магазин из Main_ADV!S23 не найден: '{store_name_value}'. Пропускаем обновление ABC1.")
        return
    logger.info(f"[📄] ABC по магазину: {store}")

    # Считываем  настройки
    promo_budget_pct = _parse_percent(_get('V14'), Decimal('0'))
    max_items = _parse_int(_get('V15'), 0)
    price_min = _parse_decimal(_get('V16'), '0')
    price_max = _parse_decimal(_get('W16'), '0')
    train_days = _parse_int(_get('V17'), 0)
    a_share = _parse_percent(_get('V18'))
    b_share = _parse_percent(_get('V19'))
    c_share = _parse_percent(_get('V20'))
    budget_mode = _parse_int(_get('V21'), 0)
    min_budget =  _parse_int(_get('V22'), 0)
    
    # Новые параметры
    add_existing_campaigns = _parse_int(_get('V24'), 0)  # Добавлять товар, если уже есть РК
    consider_manual_budget = _parse_int(_get('V25'), 0)  # Учитывать бюджет ручных РК при создании
    # recalc_budget_changes = _parse_int(_get('V26'), 0)   # Пересчитывать бюджет с учетом изменений
    min_fbs_stock = _parse_int(_get('V26'), 0)           # Остаток FBS min, шт
    min_fbo_stock = _parse_int(_get('V27'), 0)           # Остаток FBO min, шт


    # total_share = a_share + b_share + c_share
    # if total_share == 0:
    #     a_share, b_share, c_share = Decimal('0.80'), Decimal('0.15'), Decimal('0.05')
    logger.info(f"Настройки: promo_budget={promo_budget_pct}, max_items={max_items}, price_min={price_min}, price_max={price_max}, train_days={train_days}, budget_mode={budget_mode}")
    logger.info(f"min_fbs_stock = {min_fbs_stock} min_fbo_stock = {min_fbo_stock}")
    logger.info(f"ABC проценты: A={a_share*100}%, B={b_share*100}%, C={c_share*100}%")

    # Готовим словари остатков по SKU для фильтрации
    fbs_by_sku = {
        row['sku']: row['total'] or 0
        for row in FbsStock.objects.filter(store=store)
            .values('sku')
            .annotate(total=Sum('present'))
    }
    fbo_by_sku = {
        row['sku']: row['total'] or 0
        for row in WarehouseStock.objects.filter(store=store)
            .values('sku')
            .annotate(total=Sum('available_stock_count'))
    }
    logger.info(f"[ℹ️] Загружены остатки: FBS для {len(fbs_by_sku)} SKU, FBO для {len(fbo_by_sku)} SKU")

    # Больше не используем AdPlanRequest - работаем только с AdPlanItem
    logger.info(f"[ℹ️] Работаем напрямую с AdPlanItem для магазина {store}")

    # Диапазон дат: последние N дней без сегодняшнего
    today = dt_date.today()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=days - 1)
    logger.info(f"date_from = {date_from} date_to = {date_to}")
    # Агрегация и сортировка на стороне БД
    base_qs = ProductDailyAnalytics.objects.filter(store=store, date__gte=date_from, date__lte=date_to)
    total_revenue_val = base_qs.aggregate(t=Sum('revenue'))['t'] or 0
    #Сумарная выручка
    total_revenue = Decimal(str(total_revenue_val))

    # Агрегаты + вычисление кумулятивной суммы по выручке в БД
    agg_qs = (
        base_qs.values('offer_id', 'name', 'sku')
        .annotate(revenue_sum=Sum('revenue'), units_sum=Sum('ordered_units'))
        .filter(revenue_sum__gt=0)
        .order_by('-revenue_sum')
    )
    t_qs = time.perf_counter(); logger.info(f"[⏱] ORM агрегация+сортировка: {t_qs - t_params:.3f}s (rows={agg_qs.count()})")
    
    # Получаем данные о рекламных кампаниях для добавления в ABC
    from .models import ManualCampaign
    
    # Функция для перевода статусов на русский язык
    def _translate_campaign_status(status, is_manual=True):
        """Переводит статус кампании на русский язык"""
        if is_manual:
            # Статусы ручных кампаний
            status_translations = {
                ManualCampaign.CAMPAIGN_STATE_RUNNING: 'Запущена',
                ManualCampaign.CAMPAIGN_STATE_ACTIVE: 'Активна',
                ManualCampaign.CAMPAIGN_STATE_INACTIVE: 'Неактивна',
                ManualCampaign.CAMPAIGN_STATE_PLANNED: 'Запланирована',
                ManualCampaign.CAMPAIGN_STATE_STOPPED: 'Остановлена',
                ManualCampaign.CAMPAIGN_STATE_ARCHIVED: 'Архивная',
                ManualCampaign.CAMPAIGN_STATE_FINISHED: 'Завершена',
                ManualCampaign.CAMPAIGN_STATE_PAUSED: 'Приостановлена',
                ManualCampaign.CAMPAIGN_STATE_ENDED: 'Завершена',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_DRAFT: 'Черновик модерации',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_IN_PROGRESS: 'На модерации',
                ManualCampaign.CAMPAIGN_STATE_MODERATION_FAILED: 'Не прошла модерацию',
                ManualCampaign.CAMPAIGN_STATE_UNKNOWN: 'Неизвестно',
            }
        else:
            # Статусы автоматических кампаний
            status_translations = {
                'PREVIEW': 'Предпросмотр',
                'ACTIVATED': 'Активирована',
                'UNKNOWN': 'Неизвестно',
                'CAMPAIGN_STATE_RUNNING': 'Запущена',
                'CAMPAIGN_STATE_ACTIVE': 'Активна',
                'CAMPAIGN_STATE_INACTIVE': 'Неактивна',
                'CAMPAIGN_STATE_PLANNED': 'Запланирована',
                'CAMPAIGN_STATE_STOPPED': 'Остановлена (превышен бюджет)',
                'CAMPAIGN_STATE_ARCHIVED': 'Архивная',
                'CAMPAIGN_STATE_FINISHED': 'Завершена',
                'CAMPAIGN_STATE_PAUSED': 'Приостановлена',
                'CAMPAIGN_STATE_ENDED': 'Завершена',
                'CAMPAIGN_STATE_MODERATION_DRAFT': 'Черновик модерации',
                'CAMPAIGN_STATE_MODERATION_IN_PROGRESS': 'На модерации',
                'CAMPAIGN_STATE_MODERATION_FAILED': 'Не прошла модерацию',
                'CAMPAIGN_STATE_UNKNOWN': 'Неизвестно',
            }
        
        return status_translations.get(status, status)
    
    # Получаем все SKU из результатов для поиска кампаний
    all_skus = [v['sku'] for v in agg_qs if v['sku']]
    logger.info(f"[ℹ️] Обрабатываем {len(all_skus)} уникальных SKU для поиска кампаний")
    
    # Получаем ручные кампании по SKU
    manual_campaigns_dict: dict[int, list[dict]] = defaultdict(list)
    manual_campaign_entries_total = 0
    if all_skus:
        # Учитываем только запущенные и остановленные кампании
        active_states = [
            'CAMPAIGN_STATE_RUNNING',
            'CAMPAIGN_STATE_STOPPED'
        ]
        logger.info(f"[ℹ️] Ищем кампании только со статусами: {active_states}")
        
        # Получаем кампании по основному SKU
        manual_campaigns_by_sku = ManualCampaign.objects.filter(
            store=store, 
            sku__in=all_skus,
            state__in=active_states
        ).select_related('store')
        
        # Получаем кампании, где SKU есть в sku_list
        manual_campaigns_by_sku_list = ManualCampaign.objects.filter(
            store=store,
            sku_list__overlap=all_skus,
            state__in=active_states
        ).select_related('store')
        
        # Объединяем результаты
        manual_campaigns = list(manual_campaigns_by_sku) + list(manual_campaigns_by_sku_list)
        # Убираем дубликаты по ID кампании
        seen_ids = set()
        unique_campaigns = []
        for campaign in manual_campaigns:
            if campaign.id not in seen_ids:
                seen_ids.add(campaign.id)
                unique_campaigns.append(campaign)
        manual_campaigns = unique_campaigns
        
        for campaign in manual_campaigns:
            campaign_sku_count = 0
            # Добавляем основной SKU
            if campaign.sku:
                manual_campaigns_dict[campaign.sku].append({
                    'name': campaign.name,
                    'type': 'Ручное',  # Ручная
                    'ozon_updated_at': campaign.ozon_updated_at,
                    'status': _translate_campaign_status(campaign.state, is_manual=True),
                    'ozon_campaign_id': campaign.ozon_campaign_id or '',
                    'store_id': campaign.store_id,
                })
                manual_campaign_entries_total += 1
                campaign_sku_count += 1
            
            # Добавляем все SKU из sku_list
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                for sku_item in campaign.sku_list:
                    if sku_item:
                        manual_campaigns_dict[sku_item].append({
                            'name': campaign.name,
                            'type': 'Ручное',  # Ручная
                            'ozon_updated_at': campaign.ozon_updated_at,
                            'status': _translate_campaign_status(campaign.state, is_manual=True),
                            'ozon_campaign_id': campaign.ozon_campaign_id or '',
                            'store_id': campaign.store_id,
                        })
                        manual_campaign_entries_total += 1
                        campaign_sku_count += 1
            
            # Логируем информацию о кампании
            campaign_sku_count = 1 if campaign.sku else 0
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                campaign_sku_count += len([sku for sku in campaign.sku_list if sku])
            logger.info(f"[ℹ️] Кампания {campaign.name} (ID: {campaign.ozon_campaign_id}) содержит {campaign_sku_count} SKU")
    
    # Логируем количество найденных ручных кампаний
    logger.info(f"[ℹ️] Найдено {manual_campaign_entries_total} ручных кампаний по SKU для магазина {store} (уникальных SKU: {len(manual_campaigns_dict)})")
    if manual_campaigns_dict:
        logger.info(f"[ℹ️] SKU с ручными кампаниями: {list(manual_campaigns_dict.keys())[:10]}{'...' if len(manual_campaigns_dict) > 10 else ''}")

    def _get_first_manual_campaign(sku):
        entries = manual_campaigns_dict.get(sku)
        return entries[0] if entries else {}
    
    # Получаем автоматические кампании по SKU (если ручных нет)
    auto_campaigns_dict = {}
    if all_skus:
        auto_campaigns = AdPlanItem.objects.filter(
            store=store,
            sku__in=all_skus
        ).exclude(
            sku__in=manual_campaigns_dict.keys()  # Исключаем SKU, для которых уже есть ручные кампании
        ).select_related('store')
        
        for campaign in auto_campaigns:
            if campaign.sku and campaign.sku not in auto_campaigns_dict:
                # Получаем реальный статус автоматической кампании из базы данных
                auto_status = campaign.state if campaign.state else 'CAMPAIGN_STATE_UNKNOWN'
                
                auto_campaigns_dict[campaign.sku] = {
                    # Предпочитаем явное название кампании из модели
                    'name': (campaign.campaign_name or campaign.name or campaign.offer_id),
                    'type': 'Авто',  # Автоматическая
                    'ozon_updated_at': None,  # У автоматических кампаний нет ozon_updated_at
                    'status': _translate_campaign_status(auto_status, is_manual=False),
                    'ozon_campaign_id': campaign.ozon_campaign_id or ''
                }
    
    # Логируем количество найденных автоматических кампаний
    logger.info(f"[ℹ️] Найдено {len(auto_campaigns_dict)} SKU с автоматическими кампаниями для магазина {store}")

    rows = []
    # Создаем словарь для быстрого поиска названий товаров по SKU
    sku_to_name_dict = {}
    for v in agg_qs:
        revenue = Decimal(str(v['revenue_sum'] or 0))
        units = int(v['units_sum'] or 0)
        avg_price = (revenue / units) if units else Decimal('0')
        avg_price = avg_price.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        
        # Сохраняем соответствие SKU -> название товара
        sku_to_name_dict[v['sku']] = v['offer_id'] or v['name']
        
        # Получаем информацию о кампании для текущего SKU
        sku = v['sku']
        campaign_name = ''
        management_type = ''
        last_update_date = ''
        campaign_status = ''
        
        if sku:
            # Проверяем сначала ручные кампании (приоритет)
            if sku in manual_campaigns_dict:
                campaign_info = _get_first_manual_campaign(sku)
                campaign_name = campaign_info['name']
                management_type = campaign_info['type']
                campaign_status = campaign_info['status']
                if campaign_info['ozon_updated_at']:
                    last_update_date = campaign_info['ozon_updated_at'].strftime('%d-%m-%Y')
            # Если ручных нет, проверяем автоматические
            elif sku in auto_campaigns_dict:
                campaign_info = auto_campaigns_dict[sku]
                campaign_name = campaign_info['name']
                management_type = campaign_info['type']
                campaign_status = campaign_info['status']
                # У автоматических кампаний нет даты обновления из Ozon
        
        # Формируем строку: [Артикул, SKU, Продажи руб., Продажи шт., Цена товара, ABC, Название РК, Тип управления, Дата обновления, Статус]
        rows.append([
            v['offer_id'] or v['name'],  # A: Артикул
            v['sku'],                    # B: SKU
            float(revenue),              # C: Продажи, руб.
            units,                       # D: Продажи, шт.
            float(avg_price),            # E: Цена товара, руб.
            '',                          # F: ABC (будет заполнено позже)
            campaign_name,               # G: Название рекламной кампании
            management_type,             # H: Тип управления (Р/А)
            last_update_date,            # I: Дата последнего обновления в Ozon
            campaign_status              # J: Статус кампании
        ])
    t_agg = time.perf_counter(); logger.info(f"[⏱] Подготовка строк из результатов БД: {t_agg - t_qs:.3f}s (rows={len(rows)})")

    # Больше не создаем AdPlanRequest - работаем только с AdPlanItem
    logger.info(f"[ℹ️] Подготовка завершена, переходим к обработке данных")

    # Заполняем сводные значения в Main_ADV (B4, B6..D8, E8)
    try:
        # from datetime import datetime as _dt
        def _to_int(val: Decimal) -> int:
            return int(val.to_integral_value(rounding=ROUND_HALF_UP))

        # Основные суммы по категориям
        a_total = total_revenue * a_share
        b_total = total_revenue * b_share
        c_total = total_revenue * c_share

        # Месяц/неделя/день
        a_week = a_total / Decimal('4')
        a_day = a_week / Decimal('7')
        b_week = b_total / Decimal('4')
        b_day = b_week / Decimal('7')
        c_week = c_total / Decimal('4')
        c_day = c_week / Decimal('7')

        ws_main.update('B4', [[float(total_revenue)]])
        # Рекламный бюджет за +1 период: total_revenue * promo_budget_pct (уже доля 0..1)
        budget_total = total_revenue * promo_budget_pct

        # Если нужно, учитываем уже потраченный бюджет с начала текущего месяца
        if int(consider_spent or 0) == 1:
            try:
                # Начало текущего месяца (1-е число)
                since_date_consider = timezone.localdate().replace(day=1)
                spent_sum = Decimal('0')
                for _e in CampaignPerformanceReportEntry.objects.filter(
                    store=store,
                    report_date__gte=since_date_consider,
                    report_date__lte=timezone.localdate(),
                ).iterator():
                    _tot = _e.totals or {}
                    s = str(_tot.get('moneySpent') or '').replace('\u00A0','').replace('\u202F','').replace(' ','').replace(',', '.')
                    try:
                        spent_sum += Decimal(s)
                    except Exception:
                        continue
                logger.info(f"[♻️] Учитываем уже потраченное с {since_date_consider}: {spent_sum}")
                budget_total = max(Decimal('0'), budget_total - spent_sum)
            except Exception as _e:
                logger.warning(f"[⚠️] Не удалось учесть потраченный бюджет: {_e}")
        # Расчёт недельного/дневного бюджета
        # Если consider_spent == 1, то учитываем уже потраченное, распределяем остаток на оставшиеся дни месяца,
        # иначе используем месячную схему (делим на 4 недели)
        if int(consider_spent or 0) == 1:
            today = timezone.localdate()
            #Получаем полную дату: первое число следующего месяца
            next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
            # получаем последний день текущего месяца
            end_of_month = next_month - timedelta(days=1)
            # days_left, сколько дней осталось до конца месяца, включая текущий день
            # Например: сегодня 20 сентября, конец месяца 30 сентября days_left = 10 + 1 = 11
            days_left = (end_of_month - today).days + 1
            if days_left <= 0:
                days_left = 1
            budget_total_ONE_DAY = (budget_total / Decimal(str(days_left))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            budget_total_ONE_WEEK = (budget_total_ONE_DAY * Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            logger.info(f"[📆] consider_spent=1: days_left={days_left}; week={budget_total_ONE_WEEK}, day={budget_total_ONE_DAY}")
        else:
            today = date.today()

            # 1. Находим первое число следующего месяца
            if today.month == 12:
                next_month = today.replace(year=today.year + 1, month=1, day=1)
            else:
                next_month = today.replace(month=today.month + 1, day=1)

            # 2. Последний день текущего месяца = (первое число следующего месяца - 1 день)
            end_of_month = next_month - timedelta(days=1)

            # 3. Количество дней в текущем месяце
            days_in_month = end_of_month.day

            budget_total_ONE_WEEK = (budget_total / Decimal(str(days_in_month)) * Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            budget_total_ONE_DAY = (budget_total / Decimal(str(days_in_month))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # budget_total_ONE_WEEK = budget_total / Decimal('4')
            # budget_total_ONE_DAY = budget_total_ONE_WEEK / Decimal('7')
        
        # Сохраняем недельный бюджет ДО корректировки для записи в C6
        budget_total_ONE_WEEK_original = budget_total_ONE_WEEK
        
        # T25: Учитывать бюджет ручных РК при создании (0 - не учитывать, 1 - учитывать)
        manual_budget_sum = Decimal('0')
        if consider_manual_budget == 1:
            # Получаем суммарный недельный бюджет ручных кампаний со статусами RUNNING
            manual_budget_sum = ManualCampaign.objects.filter(
                store=store,
                state__in=[
                    ManualCampaign.CAMPAIGN_STATE_RUNNING
                    # ManualCampaign.CAMPAIGN_STATE_STOPPED
                ]
            ).aggregate(total_budget=Sum('week_budget'))['total_budget'] or Decimal('0')
            
            logger.info(f"[💰] Суммарный недельный бюджет ручных кампаний (RUNNING/STOPPED): {manual_budget_sum}")
            logger.info(f"[💰] Недельный бюджет до корректировки: {budget_total_ONE_WEEK}")
            
            # Записываем сумму ручных кампаний в ячейку C8
            ws_main.update('C8', [[float(manual_budget_sum)]])
            
            # Уменьшаем доступный бюджет на сумму ручных кампаний
            budget_total_ONE_WEEK = max(Decimal('0'), budget_total_ONE_WEEK - manual_budget_sum)
            budget_total_ONE_DAY = budget_total_ONE_WEEK / Decimal('7')
            
            logger.info(f"[💰] Недельный бюджет после корректировки: {budget_total_ONE_WEEK}")
            
            
        else:
            # Если T25=0, все равно записываем 0 в ячейку C8 для очистки
            ws_main.update('C8', [[0]])
        # plan_request больше нет - просто логируем бюджет
        logger.info(f"[💰] Общий бюджет: {budget_total}")
        ws_main.update('B5', [[_to_int(budget_total)]])
        ws_main.update('B6', [[_to_int(budget_total)]])
        ws_main.update('C6', [[_to_int(budget_total_ONE_WEEK_original)]])  # Недельный бюджет ДО корректировки
        # ws_main.update('D6', [[_to_int(budget_total_ONE_DAY)]])
        ws_main.update('D6', [[_to_int(budget_total_ONE_WEEK_original / Decimal('7'))]])
        

        ws_main.update('E4', [[datetime.now().strftime('%d/%m/%y')]])
        ws_main.update('E5', [[datetime.now().strftime('%d/%m/%y')]])        
        ws_main.update('E6', [[datetime.now().strftime('%d/%m/%y')]])

    except Exception as e:
        logger.error(f"[❌] Ошибка при обновлении Main_ADV сводных полей: {e}")

    # Фиксируем время на сводные обновления и задаём опорную точку перед ABC
    t_after_main = time.perf_counter(); logger.info(f"[⏱] Обновление сводных (Main_ADV): {t_after_main - t_agg:.3f}s")
    # Сортировка уже выполнена в БД
    t_sort = t_after_main; logger.info(f"[⏱] Сортировка: {t_sort - t_after_main:.3f}s")

    # ABC по выручке: кумулятив по float с тонким логированием суб-этапов
    _abc_t0 = time.perf_counter()
    total_revenue_float = float(total_revenue)
    a_cap = total_revenue_float * float(a_share)
    ab_cap = a_cap + total_revenue_float * float(b_share)
    logger.info(f"Итого выручка: {total_revenue_float}")
    logger.info(f"Целевая сумма A: {a_cap}")
    logger.info(f"Целевая сумма B: {ab_cap - a_cap}")
    logger.info(f"Целевая сумма C: {total_revenue_float - ab_cap}")
    _abc_t1 = time.perf_counter()

    # Быстрый префикс-суммы
    revs = [float(r[2]) for r in rows]
    _abc_t2 = time.perf_counter()
    cum = 0.0
    cum_sums = [0.0] * len(revs)
    for i in range(len(revs)):
        cum += revs[i]
        cum_sums[i] = cum
    _abc_t3 = time.perf_counter()
    labels = ['C'] * len(rows)
    for i, cs in enumerate(cum_sums):
        if cs <= a_cap:
            labels[i] = 'A'
        elif cs <= ab_cap:
            labels[i] = 'B'
    for i in range(len(rows)):
        rows[i][5] = labels[i]  # Устанавливаем ABC метку в позицию F (индекс 5)
    _abc_t4 = time.perf_counter()
    logger.info(f"[⏱] ABC substeps: caps={_abc_t1-_abc_t0:.3f}s, revs={_abc_t2-_abc_t1:.3f}s, cum={_abc_t3-_abc_t2:.3f}s, label={_abc_t4-_abc_t3:.3f}s")

    t_abc = time.perf_counter(); logger.info(f"[⏱] Расчёт ABC и присвоение категорий: {t_abc - t_sort:.3f}s")


    # Пишем на лист ABC: сначала шапка, затем блок данных явно в диапазон A2:J...
    ws_abc = sh.worksheet('ABC')
    header = ['Артикул', 'SKU', 'Продажи, руб.', 'Продажи, шт.', 'Цена товара, руб.', 'ABC', 'Название рекламной кампании', 'Тип управления', 'Дата последнего обновления в Ozon', 'Статус']
    # Перезапишем шапку на всякий случай
    ws_abc.update('A1:J1', [header], value_input_option='USER_ENTERED')
    # Очистим только тело
    ws_abc.batch_clear(['A2:J10000'])
    if rows:
        end_row = 1 + len(rows)  # начиная со 2-й строки
        ws_abc.update(f'A2:J{end_row}', rows, value_input_option='USER_ENTERED')
    t_write_abc = time.perf_counter(); logger.info(f"[⏱] Запись на лист ABC: {t_write_abc - t_abc:.3f}s")
    # Раскраска по ABC: группируем смежные диапазоны и применяем за минимальное число операций
    a_fmt = CellFormat(backgroundColor=Color(0.0118, 1.0, 0.0))
    b_fmt = CellFormat(backgroundColor=Color(1.0, 1.0, 0.0))
    c_fmt = CellFormat(backgroundColor=Color(1.0, 0.0, 0.0))
    values = ws_abc.col_values(6)[1:]  # без заголовка (колонка F - ABC)
    formats = []
    def add_run(start_idx, end_idx, fmt):
        if start_idx is None:
            return
        formats.append((f'F{start_idx}:F{end_idx}', fmt))
    # собираем последовательности для каждой метки
    current_label = 'None'
    run_start = None
    for i, val in enumerate(values, start=2):
        if val != current_label:
            # завершить предыдущую
            if current_label == 'A':
                add_run(run_start, i-1, a_fmt)
            elif current_label == 'B':
                add_run(run_start, i-1, b_fmt)
            elif current_label == 'C':
                add_run(run_start, i-1, c_fmt)
            # начать новую, если валидная
            current_label = val if val in ('A','B','C') else None
            run_start = i if current_label else None
    # fin
    if current_label == 'A':
        add_run(run_start, len(values)+1, a_fmt)
    elif current_label == 'B':
        add_run(run_start, len(values)+1, b_fmt)
    elif current_label == 'C':
        add_run(run_start, len(values)+1, c_fmt)
    if formats:
        format_cell_ranges(ws_abc, formats)
    t_format = time.perf_counter(); logger.info(f"[⏱] Форматирование листа ABC: {t_format - t_write_abc:.3f}s")

    if budget_total_ONE_WEEK <= 0:
        logger.warning(f"[⚠️] После учета ручных кампаний недельный бюджет стал <= 0. Создание автоматических кампаний невозможно.")
        return

    # ------------------------------
    # TOP-N: фильтр по цене по avg_price и пропорциональное распределение
    # ------------------------------
    
    # Инициализируем список SKU с существующими ручными кампаниями
    existing_campaigns_rows = []
    manual_rows_added: set[tuple[str, int]] = set()
    
    try:
        
        logger.info(f" Минимальный бюджет (T22): {min_budget}")
        if min_budget <= 0:
            raise ValueError('Минимальный бюджет (T22) должен быть > 0')
        
        # Читаем список исключений из столбца AB
        exclusion_offer_ids = set()
        try:
            # Читаем все значения из столбца AB, начиная с AB3
            # col_values возвращает список строк для всего столбца
            # Столбец AB имеет индекс 28. Нам нужны строки с 13-й, что соответствует индексу 12 в 0-индексированном списке
            raw_exclusions = ws_main.col_values(28)[12:]  # AB13 и далее
            for item in raw_exclusions:
                item = item.strip()
                if item:  # Обрабатываем только непустые элементы
                    # Исключения - это offer_id (артикулы товаров)
                    exclusion_offer_ids.add(item)
        except Exception as e:
            logger.error(f"[❌] Ошибка при чтении списка исключений из Main_ADV!Y: {e}")
        
        logger.info(f"[ℹ️] Список исключений (offer_id) из Main_ADV!Y: {exclusion_offer_ids}")

        # Читаем список обязательных товаров из столбца AC (обязательные offer_id)
        mandatory_offer_ids_list, mandatory_offer_ids_set = _read_mandatory_offer_ids(ws_main, column_index=29, start_row=13)
        logger.info(f"[ℹ️] Список обязательных offer_id из Main_ADV!AC: {mandatory_offer_ids_list}")
        
        n_max = int((budget_total_ONE_WEEK // min_budget)) if min_budget > 0 else 0
        if 'max_items' in locals() and max_items and max_items > 0:
            n_max = int(max_items)+1
            logger.info(f"[ℹ️] max_items задан: используем n_max={n_max}")
        else:
            logger.info(f"[✅] Сколько товаров можно прокормить: {n_max}")
        t_topn_start = time.perf_counter()
        logger.info(f"[⏱] Подготовка к TOP-N (параметры): {t_topn_start - t_format:.3f}s")

        # Отбор TOP-N: по выручке, используем avg_price из rows[4]
        selected = []
        # r[0] — offer_id или name
        # r[1] — sku (int)
        # r[2] — revenue, суммарная выручка (float)
        # r[3] — units, суммарное кол-во (int)
        # r[4] — avg_price, средняя цена (float)
        for r in rows:
            if len(selected) >= n_max:
                break
            offer_id = r[0]  # offer_id находится по индексу 0            
            sku = r[1]  # SKU товара            
            # Проверяем, не находится ли товар в списке исключений
            if offer_id in exclusion_offer_ids:
                logger.info(f"[🚫] offer_id '{offer_id}' находится в списке исключений, пропускаем.")
                continue

            avg_price_val = Decimal(str(r[4])) if len(r) > 4 and r[4] is not None else Decimal('0')
            if price_min and price_min > 0 and avg_price_val < price_min:
                continue
            if price_max and price_max > 0 and avg_price_val > price_max:
                continue
            
            # Проверяем остатки FBS и FBO, если настройки заданы
            if min_fbs_stock > 0 or min_fbo_stock > 0:
                fbs_stock = fbs_by_sku.get(sku, 0)
                fbo_stock = fbo_by_sku.get(sku, 0)
                logger.info(f"[ℹ️] SKU {sku} проверяем остатки FBS = {fbs_stock} FBO = {fbo_stock}")
                # Если остатки отсутствуют, пропускаем товар
                
                if min_fbs_stock > 0 and fbs_stock < min_fbs_stock:
                    logger.info(f"[🚫] SKU {sku} исключен: остаток FBS {fbs_stock} < минимального {min_fbs_stock}")
                    continue
                    
                if min_fbo_stock > 0 and fbo_stock < min_fbo_stock:
                    logger.info(f"[🚫] SKU {sku} исключен: остаток FBO {fbo_stock} < минимального {min_fbo_stock}")
                    continue            
            # T24: Добавлять товар, если уже есть РК (0 - не добавлять, 1 - добавлять)
            if add_existing_campaigns == 0:
                # Проверяем, есть ли уже кампания для этого SKU
                has_manual_campaign = sku in manual_campaigns_dict if sku else False
                has_auto_campaign = sku in auto_campaigns_dict if sku else False
                
                if has_auto_campaign:
                    logger.info(f"[ℹ️] SKU {sku} уже имеет автоматическую кампанию, добавляем в selected с доп. полями")
                    # Получаем информацию об автоматической кампании
                    campaign_info = auto_campaigns_dict.get(sku, {})
                    campaign_name = campaign_info.get('name', 'Неизвестная кампания')
                    campaign_status = campaign_info.get('status', 'Неизвестный статус')
                    ozon_campaign_id = campaign_info.get('ozon_campaign_id', '')
                    
                    # Определяем, включена ли кампания (1) или выключена (0)
                    # Кампания считается включенной, если статус 'Активна' или 'Запущена'
                    is_enabled = 1 if campaign_status in ['Активна', 'Запущена'] else 0
                    
                    # Добавляем в список существующих кампаний для отображения
                    existing_campaigns_rows.append([
                        ozon_campaign_id, # A: ID кампании (ozon_campaign_id)
                        is_enabled,       # B: Включена (1) / выключена (0)
                        campaign_status,  # C: Статус в Ozon
                        offer_id,         # D: Артикул
                        sku,              # E: SKU
                        float(r[2]),      # F: Продажи, руб.
                        r[3],             # G: Продажи, шт.
                        float(r[4]),      # H: Цена товара, руб.
                        r[5] if len(r) > 5 else '',  # I: ABC
                        'Авто',           # J: Тип управления
                        '',               # K: Дата обновления (будет заполнена позже)
                        campaign_status   # L: Статус кампании
                    ])
                    
                    # Добавляем товар в selected с дополнительными полями от автоматической кампании
                    # Расширяем строку r дополнительными полями
                    extended_r = r + [
                        campaign_name,    # G: Название рекламной кампании
                        'Авто',           # H: Тип управления
                        '',               # I: Дата последнего обновления в Ozon
                        campaign_status   # J: Статус кампании
                    ]
                    selected.append(extended_r)
                    logger.info(f"[📋] Добавлен SKU {sku} с автоматической кампанией '{campaign_name}' в selected (статус: {campaign_status}, включена: {is_enabled})")
                    continue
                if has_manual_campaign:
                    logger.info(f"[ℹ️] SKU {sku} уже имеет рекламную кампанию, добавляем в список существующих")
                    manual_entries = manual_campaigns_dict.get(sku, [])
                    for manual_entry in manual_entries:
                        campaign_name = manual_entry.get('name', 'Неизвестная кампания')
                        campaign_status = manual_entry.get('status', 'Неизвестный статус')
                        ozon_campaign_id = manual_entry.get('ozon_campaign_id') or campaign_name
                        unique_key = (ozon_campaign_id, sku)
                        if unique_key in manual_rows_added:
                            continue
                        manual_rows_added.add(unique_key)
                        existing_campaigns_rows.append([
                            ozon_campaign_id,  # A: ID кампании
                            '',                # B: Пустота для ручных кампаний
                            campaign_status,   # C: Статус в Ozon
                            offer_id,          # D: Артикул
                            sku,               # E: SKU
                            float(r[2]),       # F: Продажи, руб.
                            r[3],              # G: Продажи, шт.
                            float(r[4]),       # H: Цена товара, руб.
                            r[5] if len(r) > 5 else '',  # I: ABC
                            'Ручное',          # J: Тип управления
                            '',                # K: Дата обновления (будет заполнена позже)
                            campaign_status    # L: Статус кампании
                        ])
                        logger.info(f"[📋] Добавлен SKU {sku} с существующей кампанией '{campaign_name}' (статус: {campaign_status})")
                    continue
                

            
            selected.append(r)
        t_select = time.perf_counter(); logger.info(f"[⏱] Отбор TOP-N: {t_select - t_topn_start:.3f}s (selected={len(selected)})")

        # Гарантируем, что обязательные товары попали в выборку
        selected_offer_ids = {str(row[0]).strip() for row in selected if row and len(row) > 0}
        mandatory_added = 0
        for offer_id in mandatory_offer_ids_list:
            if offer_id in exclusion_offer_ids:
                logger.info(f"[⚠️] Обязательный offer_id '{offer_id}' присутствует в списке исключений — пропускаем")
                continue
            if offer_id in selected_offer_ids:
                continue
            candidate = next((row for row in rows if str(row[0]).strip() == offer_id), None)
            if candidate:
                # проверяем фильтр по остаткам, чтобы не нарушать логику FBS/FBO
                sku_candidate = candidate[1]
                if min_fbs_stock > 0 or min_fbo_stock > 0:
                    fbs_candidate = fbs_by_sku.get(sku_candidate, 0)
                    fbo_candidate = fbo_by_sku.get(sku_candidate, 0)
                    if min_fbs_stock > 0 and fbs_candidate < min_fbs_stock:
                        logger.info(
                            f"[⚠️] Обязательный offer_id '{offer_id}' пропущен: FBS остаток {fbs_candidate} < {min_fbs_stock}"
                        )
                        continue
                    if min_fbo_stock > 0 and fbo_candidate < min_fbo_stock:
                        logger.info(
                            f"[⚠️] Обязательный offer_id '{offer_id}' пропущен: FBO остаток {fbo_candidate} < {min_fbo_stock}"
                        )
                        continue
                selected.append(candidate)
                selected_offer_ids.add(offer_id)
                mandatory_added += 1
                logger.info(f"[⭐] Добавлен обязательный offer_id '{offer_id}' в выборку")
            else:
                logger.warning(f"[⚠️] Обязательный offer_id '{offer_id}' не найден среди данных — пропущен")

        if mandatory_added:
            logger.info(f"[ℹ️] Добавлено обязательных товаров: {mandatory_added}")

        # Если max_items > 0, перерасчёт: используем максимум, но обязательные товары не удаляем
        if selected and max_items and max_items > 0 and len(selected) > int(max_items):
            max_items_int = int(max_items)
            mandatory_selected = [row for row in selected if str(row[0]).strip() in mandatory_offer_ids_set]
            optional_selected = [row for row in selected if str(row[0]).strip() not in mandatory_offer_ids_set]

            if len(mandatory_selected) >= max_items_int:
                selected = mandatory_selected[:max_items_int]
            else:
                selected = mandatory_selected + optional_selected[:max_items_int - len(mandatory_selected)]
        # for t_data in selected:
        #     logger.info(t_data)
        # Пропорциональное распределение по выбранным товарам:
        # Считаем общую выручку всех отобранных товаров (для пропорционального распределения)
        if selected and mandatory_offer_ids_set:
            selected.sort(key=lambda row: 0 if str(row[0]).strip() in mandatory_offer_ids_set else 1)

        selected_total_revenue = sum(Decimal(str(r[2])) for r in selected) if selected else Decimal('0')
        share_values = []  # Значения доли выручки для колонки AX
        out_rows = []  # Список строк для записи в таблицу
        items_to_save = []  # Список товаров для сохранения в базу данных
        campaign_names = []  # Столбец C: «Артикул товара + дата создания»
        sum_week = Decimal('0')  # Сумма всех недельных бюджетов
        logger.info(f"selected_total_revenue = {selected_total_revenue}")
        
        for r in selected:
            offer_or_name = r[0]  # Артикул или название товара
            sku = r[1]  # SKU товара
            revenue_val = r[2]  # Выручка товара
            # units = r[3]  # не используется здесь
            revenue_dec = Decimal(str(revenue_val))  # Конвертируем выручку в Decimal для точных вычислений
            
            # Режим распределения: 0 — равномерно, 1 — по весу (выручке)
            share = Decimal('0')
            share_raw = Decimal('0')
            if selected_total_revenue > 0:
                share_raw = (revenue_dec / selected_total_revenue)

            if 'budget_mode' in locals() and budget_mode == 0 and selected:
                amount = (budget_total_ONE_WEEK / Decimal(len(selected)))  # недельный бюджет на товар (равномерно)
                share = share_raw
            elif selected_total_revenue > 0:
                share = share_raw  # Доля выручки товара от общей выручки
                amount = budget_total_ONE_WEEK * share  # недельный бюджет на товар (пропорционально выручке)
            else:
                share = share_raw
                amount = (budget_total_ONE_WEEK / Decimal(len(selected))) if selected else Decimal('0')  # Fallback на равномерное
                
            # Считаем недельный бюджет напрямую из amount и округляем до 2 знаков после запятой
            week_amt = amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            # Минималка по неделе: если бюджет меньше минимального, устанавливаем минимальный
            if week_amt < min_budget:
                week_amt = Decimal(str(min_budget)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
            # Контроль суммы: если превышаем недельный бюджет — прекращаем и не добавляем текущий товар
            if (sum_week + week_amt) > budget_total_ONE_WEEK + Decimal('1'):
                if str(offer_or_name).strip() in mandatory_offer_ids_set:
                    logger.info(
                        f"[⚠️] Обязательный товар {offer_or_name} превышает недельный бюджет, включаем его сверх лимита"
                    )
                else:
                    logger.info("[⛔] Сумма недельных бюджетов превысила недельный бюджет, остановка подбора TOP-N")
                    break
            logger.info(f"sum_week = {sum_week} | week_amt = {week_amt} | share = {round(share*100,3)} | r = {r}")

            sum_week += week_amt  # Добавляем к общей сумме недельных бюджетов
            day_amt = (week_amt / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # Считаем дневной бюджет: недельный делим на 7 дней и округляем до 2 знаков
            
            # Формируем название для колонки D:
            # Приоритет: ручная кампания -> автоматическая -> артикул + дата
            manual_campaign_entry = _get_first_manual_campaign(sku) if sku in manual_campaigns_dict else {}
            if manual_campaign_entry.get('name'):
                campaign_name_with_status = manual_campaign_entry['name']
            elif sku in auto_campaigns_dict and auto_campaigns_dict[sku].get('name'):
                campaign_name_with_status = auto_campaigns_dict[sku]['name']
            else:
                campaign_name_with_status = f"{offer_or_name} {timezone.localtime().strftime('%d/%m/%y')}"
            
            # Если есть ручная кампания, добавляем статус
            if sku in manual_campaigns_dict:
                manual_campaign = ManualCampaign.objects.filter(store=store, sku=sku).first()
                if manual_campaign:
                    status_russian = _translate_campaign_status(manual_campaign.state)
            
            campaign_names.append([campaign_name_with_status])  # D: формируем название кампании со статусом
            # Получаем недельный бюджет ручной кампании, если есть
            manual_week_budget = ''
            if sku in manual_campaigns_dict:
                manual_campaign = ManualCampaign.objects.filter(store=store, sku=sku).first()
                if manual_campaign:
                    manual_week_budget = float(manual_campaign.week_budget)
            sheet_manual_budget = existing_manual_budget_by_sku.get(_extract_sku(sku))
            if sheet_manual_budget is not None:
                if manual_week_budget in ('', None):
                    manual_week_budget = float(sheet_manual_budget)
                else:
                    try:
                        if Decimal(str(manual_week_budget)) == Decimal('0') and sheet_manual_budget != Decimal('0'):
                            manual_week_budget = float(sheet_manual_budget)
                    except Exception:
                        manual_week_budget = float(sheet_manual_budget)

            # Получаем название товара по SKU из словаря
            product_name = sku_to_name_dict.get(sku, offer_or_name)
            
            if isinstance(manual_week_budget, Decimal):
                manual_week_budget = float(manual_week_budget)

            meta_info = {}
            if sku in manual_campaigns_dict:
                meta_info = {
                    'type': 'manual',
                }
            else:
                auto_info = auto_campaigns_dict.get(sku, {})
                status_val = auto_info.get('status', '')
                is_enabled = 1 if status_val in ['Активна', 'Запущена'] else 0
                meta_info = {
                    'type': 'auto' if auto_info else 'auto_new',
                    'campaign_id': auto_info.get('ozon_campaign_id', ''),
                    'status': status_val,
                    'name': auto_info.get('name', campaign_name_with_status),
                    'is_enabled': is_enabled,
                }

            out_rows.append([
                product_name,  # F: Название товара (артикул)
                int(sku),  # G: SKU товара
                float(week_amt),  # H: Недельный бюджет (с 2 знаками после запятой)
                manual_week_budget,  # I: Недельный бюджет ручной кампании
                float(day_amt),  # J: Дневной бюджет (с 2 знаками после запятой)
            ])
            items_to_save.append((int(sku), str(product_name), week_amt, day_amt, meta_info))  # Сохраняем для записи в базу
            share_output = share.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP) if selected_total_revenue > 0 else Decimal('0')
            share_values.append(float(share_output))
        
        # Проверяем разницу между суммой бюджетов и целевым бюджетом
        budget_diff = abs(sum_week - budget_total_ONE_WEEK)
        logger.info(f"[ℹ️] Сумма бюджетов: {sum_week}, Целевой бюджет: {budget_total_ONE_WEEK}, Разница: {budget_diff}")
        
        # T26: Пересчитывать бюджет с учетом изменений (0 - не пересчитывать, 1 - пересчитывать)
        # Если разница больше 5 рублей и включен пересчет, пересчитываем бюджеты пропорционально
        if  budget_diff > Decimal('5') and sum_week > 0:
            logger.info(f"[🔄] Разница больше 5 рублей, пересчитываем бюджеты")
            # Вычисляем коэффициент корректировки
            correction_factor = budget_total_ONE_WEEK / sum_week
            logger.info(f"[ℹ️] Коэффициент корректировки: {correction_factor}")
            
            # Пересчитываем бюджеты и обновляем списки
            new_out_rows = []
            new_items_to_save = []
            new_sum_week = Decimal('0')
            
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i, meta_info) in enumerate(items_to_save):
                # Пересчитываем недельный бюджет
                new_week_amt = (week_amt_i * correction_factor).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                # Пересчитываем дневной бюджет
                new_day_amt = (new_week_amt / Decimal('7')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                
                new_sum_week += new_week_amt
                
                # Обновляем строку для таблицы
                new_out_rows.append([
                    out_rows[i][0],  # F: Артикул товара
                    out_rows[i][1],  # G: SKU товара
                    float(new_week_amt),  # H: Новый недельный бюджет
                    out_rows[i][3] if len(out_rows[i]) > 3 else 0.0,  # I: Недельный бюджет ручной кампании (не изменяется)
                    float(new_day_amt),  # J: Новый дневной бюджет
                ])
                
                # Обновляем данные для сохранения в БД
                new_items_to_save.append((sku_i, offer_id_i, new_week_amt, new_day_amt, meta_info))
            
            # Заменяем старые данные новыми
            out_rows = new_out_rows
            items_to_save = new_items_to_save
            sum_week = new_sum_week
            
            logger.info(f"[✅] Бюджеты пересчитаны. Новая сумма: {sum_week}")
        # elif recalc_budget_changes == 0 and budget_diff > Decimal('5'):
        #     logger.info(f"[ℹ️] Пересчет бюджетов отключен (T26=0), оставляем как есть")
        
        t_alloc = time.perf_counter(); logger.info(f"[⏱] Распределение бюджета TOP-N: {t_alloc - t_select:.3f}s (итого_неделя={sum_week})")
        logger.info(f"[📋] Собрано SKU с существующими кампаниями: {len(existing_campaigns_rows)}")

        # Добавляем ВСЕ товары из ручных кампаний в конец списка для отображения
        existing_campaigns_added = 0
        logger.info(f"[📋] Добавляем ВСЕ товары из ручных кампаний в конец списка")
        
        # Получаем все ручные кампании с нужными статусами
        manual_campaigns = ManualCampaign.objects.filter(
            store=store,
            state__in=[
                'CAMPAIGN_STATE_RUNNING',
                'CAMPAIGN_STATE_STOPPED'
            ]
        ).select_related('store')
        
        # Проходим по каждой кампании и выводим ВСЕ товары в ней
        for campaign in manual_campaigns:
            campaign_name = campaign.name
            campaign_status = _translate_campaign_status(campaign.state)
            
            # Собираем все SKU из кампании (основной + из списка)
            all_skus_in_campaign = []
            
            # Добавляем основной SKU
            if campaign.sku:
                all_skus_in_campaign.append(campaign.sku)
            
            # Добавляем все SKU из sku_list
            if campaign.sku_list and isinstance(campaign.sku_list, list):
                for sku_item in campaign.sku_list:
                    if sku_item and sku_item not in all_skus_in_campaign:
                        all_skus_in_campaign.append(sku_item)
            
            # Выводим каждый SKU из кампании
            for sku in all_skus_in_campaign:
                # Получаем название товара по SKU из словаря
                product_name = sku_to_name_dict.get(sku, f"SKU_{sku}")
                
                # Формируем название кампании без даты для колонки D (ручные — без даты)
                campaign_name_no_date = campaign_name
                
                # Бюджеты кампании: неделя/день (если не заданы — 0)
                manual_week_budget_val = float(campaign.week_budget or 0)
                manual_day_budget_val = float(campaign.daily_budget or 0)
                sheet_manual_budget = existing_manual_budget_by_sku.get(_extract_sku(sku))
                if sheet_manual_budget is not None:
                    try:
                        if manual_week_budget_val == 0 and sheet_manual_budget != Decimal('0'):
                            manual_week_budget_val = float(sheet_manual_budget)
                            manual_day_budget_val = float((sheet_manual_budget / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                    except Exception:
                        manual_week_budget_val = float(sheet_manual_budget)
                        manual_day_budget_val = float((sheet_manual_budget / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

                if isinstance(manual_week_budget_val, Decimal):
                    manual_week_budget_val = float(manual_week_budget_val)

                # Добавляем в out_rows с бюджетом кампании в колонке J (дневной)
                out_rows.append([
                    product_name,  # F: Название товара (артикул)
                    int(sku),     # G: SKU товара
                    0.0,                      # H: Недельный бюджет (не распределяем для существующих кампаний)
                    manual_week_budget_val,   # I: Недельный бюджет ручной кампании
                    manual_day_budget_val,    # J: Дневной бюджет кампании (требование)
                ])
                
                # Добавляем в campaign_names без даты
                campaign_names.append([campaign_name_no_date])
                
                # Добавляем в items_to_save с нулевыми бюджетами
                meta_info = {
                    'type': 'manual',
                    'campaign_id': campaign.ozon_campaign_id or '',
                    'status': campaign_status,
                    'name': campaign_name_no_date,
                }
                items_to_save.append((int(sku), product_name, Decimal('0'), Decimal('0'), meta_info))
                share_values.append('')

                existing_campaigns_added += 1
                logger.info(f"[📋] Добавлен SKU {sku} из кампании '{campaign_name}' (название: {product_name}, статус: {campaign_status})")
        
        logger.info(f"[📋] Добавлено товаров из ручных кампаний в конец списка: {existing_campaigns_added}")

        ws_main.batch_clear([f'A{start_row}:S1000', f'AX{start_row}:AX1000'])  # Очищаем рабочий блок и колонку долей

        # Сбрасываем заливку в колонках H-I перед записью остатков (убираем серый цвет)
        try:
            default_fill = CellFormat(backgroundColor=Color(1, 1, 1))
            format_cell_ranges(ws_main, [(f'H{start_row}:I1000', default_fill)])
        except Exception as fmt_err:
            logger.warning(f"[⚠️] Не удалось сбросить формат ячеек H{start_row}:I1000: {fmt_err}")

        # Словари остатков уже созданы выше
        if out_rows:
            # Заполняем столбец A (ID кампании), C (статус), E (тип управления) для существующих кампаний
            campaign_ids_col_a = []
            campaign_statuses_col_c = []
            campaign_types_col_e = []
            
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i, meta_info) in enumerate(items_to_save):
                entry_type = meta_info.get('type')
                if entry_type == 'manual':
                    campaign_id = meta_info.get('campaign_id', '')
                    campaign_status = meta_info.get('status', '')
                    campaign_type = 'Ручная'
                elif entry_type == 'auto':
                    campaign_id = meta_info.get('campaign_id', '')
                    campaign_status = meta_info.get('status', '')
                    campaign_type = 'Авто'
                elif entry_type == 'auto_new':
                    campaign_id = meta_info.get('campaign_id', '')
                    campaign_status = meta_info.get('status', '')
                    campaign_type = 'Авто'
                else:
                    campaign_id = ''
                    campaign_status = ''
                    campaign_type = ''
                
                campaign_ids_col_a.append([campaign_id])
                campaign_statuses_col_c.append([campaign_status])
                campaign_types_col_e.append([campaign_type])
            
            # Записываем данные в Google Sheets
            if campaign_ids_col_a:
                ws_main.update(f'A{start_row}:A{start_row + len(campaign_ids_col_a) - 1}', campaign_ids_col_a)
            
            # Заполняем столбец B (активация): по умолчанию 1, для ручных - пустота, для автоматических - из модели
            activation_values = []
            for i, (sku_i, offer_id_i, week_amt_i, day_amt_i, meta_info) in enumerate(items_to_save):
                entry_type = meta_info.get('type')
                if entry_type == 'manual':
                    activation_values.append([''])
                elif entry_type in ('auto', 'auto_new'):
                    activation_values.append([meta_info.get('is_enabled', 1)])
                else:
                    activation_values.append([1])
            
            if activation_values:
                ws_main.update(f'B{start_row}:B{start_row + len(activation_values) - 1}', activation_values)
            
            # Заполняем столбец C (статус кампании)
            if campaign_statuses_col_c:
                ws_main.update(f'C{start_row}:C{start_row + len(campaign_statuses_col_c) - 1}', campaign_statuses_col_c)
            
            # Заполняем столбец D (Название кампании): «Артикул + дата»
            ws_main.update(f'D{start_row}:D{start_row + len(campaign_names) - 1}', campaign_names)
            
            # Заполняем столбец E (тип управления)
            if campaign_types_col_e:
                ws_main.update(f'E{start_row}:E{start_row + len(campaign_types_col_e) - 1}', campaign_types_col_e)
            
            # Заполняем столбцы F-G (артикул и SKU), H (остаток FBS), I (остаток FBO) и J-L (бюджеты)
            cols_FG = [[row[0], row[1]] for row in out_rows]
            # Остатки по порядку items_to_save
            fbs_col_H = [[int(fbs_by_sku.get(int(sku_i), 0))] for (sku_i, _offer, _w, _d, _meta) in items_to_save]
            fbo_col_I = [[int(fbo_by_sku.get(int(sku_i), 0))] for (sku_i, _offer, _w, _d, _meta) in items_to_save]
            cols_JKL = [[row[2], row[3], row[4]] for row in out_rows]
            ws_main.update(f'F{start_row}:G{start_row + len(out_rows) - 1}', cols_FG)
            if fbs_col_H:
                ws_main.update(f'H{start_row}:H{start_row + len(fbs_col_H) - 1}', fbs_col_H)
            if fbo_col_I:
                ws_main.update(f'I{start_row}:I{start_row + len(fbo_col_I) - 1}', fbo_col_I)
            ws_main.update(f'J{start_row}:L{start_row + len(out_rows) - 1}', cols_JKL)
            if share_values:
                if len(share_values) != len(out_rows):
                    logger.warning(f"[⚠️] Количество значений долей ({len(share_values)}) не совпадает с числом строк ({len(out_rows)}). Усечение до минимума.")
                limit = min(len(share_values), len(out_rows))
                if limit > 0:
                    shares_col_ax = [[share_values[i]] for i in range(limit)]
                    ws_main.update(f'AX{start_row}:AX{start_row + limit - 1}', shares_col_ax)
        t_write_topn = time.perf_counter(); logger.info(f"[⏱] Запись блока TOP-N: {t_write_topn - t_alloc:.3f}s (строк={len(out_rows)})")

        # Сопоставим ABC-метку по SKU из rows
        abc_by_sku = {}
        for rr in rows:
            if len(rr) > 5:
                try:
                    abc_by_sku[int(rr[1])] = rr[5]
                except Exception:
                    continue


        
    except Exception as e:
        logger.error(f"[❌] Ошибка при формировании TOP-N: {e}")

    logger.info(f"[✅] ABC обновлён за {date_from}..{date_to}. Строк: {len(rows)}")
    

# =========================
# ABC: ежемесячная обёртка
# =========================
@shared_task(name="Обновление ABC в первый день месяца")
def update_abc_sheet_if_first_day(spreadsheet_url: str = None):
    """Вызывает update_abc_sheet только если сегодня первое число месяца."""
    today = timezone.localdate()
    if today.day != 1:
        logger.info(f"[ℹ️] Сегодня {today:%d.%m.%Y}, не первый день месяца — ABC не обновляем")
        return {"skipped": True, "reason": "not first day"}
        
    if not spreadsheet_url:
        stores = OzonStore.objects.all()
        for store in stores:
            spreadsheet_url = store.google_sheet_url
            if not spreadsheet_url:
                continue

            logger.info(f"[🗓️] Первый день месяца — запускаем обновление ABC для магазина {store.name}")            
            update_abc_sheet(spreadsheet_url= spreadsheet_url)
            create_or_update_AD(spreadsheet_url = spreadsheet_url)
        return True
            
    else:
        logger.info("[🗓️] Первый день месяца — запускаем обновление ABC")
        # return 0
        update_abc_sheet(spreadsheet_url= spreadsheet_url)
        create_or_update_AD(spreadsheet_url = spreadsheet_url)
        return True

#---------------- Перерасчёт недельных бюджетов авто-кампаний ------------------------

@shared_task(name="Перерасчёт недельных бюджетов авто-кампаний по понедельникам")
def scheduled_rebalance_auto_weekly_budgets_monday(
    sa_json_path: str = None,
    worksheet_name: str = "Main_ADV",
    start_row: int = 13,
):
    """
    Плановый запуск перерасчёта недельных бюджетов только по понедельникам.
    Проходит по всем магазинам с настроенной таблицей и вызывает rebalance_auto_weekly_budgets.
    """
    today = timezone.localdate()
    if today.weekday() != 0:  # 0 = понедельник
        logger.info(f"[ℹ️] Сегодня {today:%d.%m.%Y} (weekday={today.weekday()}), не понедельник — перерасчёт пропущен")
        return {"skipped": True, "reason": "not monday"}

    stores_qs = (
        OzonStore.objects.exclude(google_sheet_url__isnull=True)
        .exclude(google_sheet_url='')
    )
    stores = list(stores_qs)
    if not stores:
        logger.info("[ℹ️] Нет магазинов с заполненным google_sheet_url для планового перерасчёта бюджетов")
        return {"processed": 0, "results": [], "errors": []}

    processed = 0
    results: list[dict] = []
    errors: list[dict] = []

    for store in stores:
        sheet_url = (store.google_sheet_url or '').strip()
        if not sheet_url:
            continue
        processed += 1
        try:
            result = rebalance_auto_weekly_budgets(
                spreadsheet_url=sheet_url,
                sa_json_path=sa_json_path,
                worksheet_name=worksheet_name,
                start_row=start_row,
            )
        except Exception as err:
            logger.error(f"[❌] Ошибка перерасчёта недельных бюджетов для магазина {store.id}: {err}")
            errors.append({"store_id": store.id, "error": str(err)})
            continue

        if isinstance(result, dict) and result.get('error'):
            errors.append({"store_id": store.id, "error": result['error']})
            continue

        success = result if isinstance(result, dict) else {"result": result}
        success["store_id"] = store.id
        results.append(success)

    summary = {"processed": processed, "results": results}
    if errors:
        summary["errors"] = errors
    return summary

@shared_task(name="Перерасчёт недельных бюджетов авто-кампаний")
def rebalance_auto_weekly_budgets(
    spreadsheet_url: str = None,
    sa_json_path: str = None,
    worksheet_name: str = "Main_ADV",
    start_row: int = 13,
):
    """
    Перераспределяет недельные бюджеты автоматических кампаний с учётом фактических расходов
    и активных ручных кампаний. Обновляет колонку J (недельный бюджет) и week/day budgets в AdPlanItem.
    """
    try:
        today = timezone.localdate()
        if not spreadsheet_url:
            logger.error("[❌] Не указан spreadsheet_url для перерасчёта недельных бюджетов")
            return {"error": "spreadsheet_url not provided"}

        
        spreadsheet_url = spreadsheet_url or ""
        sa_json_path = sa_json_path or os.getenv(
            "GOOGLE_SA_JSON_PATH",
            "/workspace/ozon-469708-c5f1eca77c02.json",
        )

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        gc = gspread.authorize(creds)

        sh = gc.open_by_url(spreadsheet_url)
        ws = sh.worksheet(worksheet_name)

        def _sanitize(val: str | None) -> str:
            if not val:
                return ''
            return str(val).strip().replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')

        def _parse_decimal(val: str | None, default: Decimal = Decimal('0')) -> Decimal:
            s = _sanitize(val)
            if not s:
                return default
            try:
                return Decimal(s)
            except Exception:
                try:
                    return Decimal(str(float(s)))
                except Exception:
                    return default


        month_start = today.replace(day=1)
        last_day = calendar.monthrange(today.year, today.month)[1]
        month_end = date(today.year, today.month, last_day)
        days_left = (month_end - today).days + 1
        if days_left <= 0:
                logger.info(f"[ℹ️] Конец месяца уже наступил ({today}), перерасчёт не требуется")
                return {"skipped": True, "reason": "no days left"}

        store_name = (ws.acell('V23').value or '').strip()
        if not store_name:
            logger.error("[❌] Ячейка V23 пустая — магазин не указан")
            return {"error": "store not set"}

        store = (
            OzonStore.objects.filter(name__iexact=store_name).first()
            or OzonStore.objects.filter(client_id__iexact=store_name).first()
            or OzonStore.objects.filter(name__icontains=store_name).first()
            or OzonStore.objects.filter(client_id__icontains=store_name).first()
        )
        if not store:
            logger.error(f"[❌] Магазин '{store_name}' не найден")
            return {"error": f"store '{store_name}' not found"}

        month_budget = _parse_decimal(ws.acell('B5').value)
        min_budget = _parse_decimal(ws.acell('V22').value)
        min_budget = max(min_budget, Decimal('0'))

        mandatory_offer_ids_list, mandatory_offer_ids_set = _read_mandatory_offer_ids(ws, column_index=29, start_row=start_row)
        if mandatory_offer_ids_list:
            logger.info(f"[ℹ️] Обязательные offer_id для перерасчёта: {mandatory_offer_ids_list}")
        else:
            logger.info("[ℹ️] Обязательные offer_id для перерасчёта не заданы")

        manual_campaigns = list(
            ManualCampaign.objects.filter(store=store).exclude(ozon_campaign_id__isnull=True).exclude(ozon_campaign_id='')
        )
        manual_active_states = {
            ManualCampaign.CAMPAIGN_STATE_RUNNING,
            ManualCampaign.CAMPAIGN_STATE_ACTIVE,
            ManualCampaign.CAMPAIGN_STATE_PLANNED,
            ManualCampaign.CAMPAIGN_STATE_STOPPED
        }
        manual_week_reserved = sum(
            Decimal(str(c.week_budget or 0))
            for c in manual_campaigns
            if c.state in manual_active_states
        )
        manual_campaign_ids = [str(c.ozon_campaign_id) for c in manual_campaigns if c.ozon_campaign_id]

        auto_items = list(
            AdPlanItem.objects.filter(store=store).exclude(ozon_campaign_id__isnull=True).exclude(ozon_campaign_id='')
        )
        auto_campaign_ids = [str(item.ozon_campaign_id) for item in auto_items]
        ad_items_by_campaign = {
            str(item.ozon_campaign_id): item for item in auto_items if item.ozon_campaign_id
        }
        ad_items_by_offer = {
            (item.offer_id or '').strip(): item for item in auto_items if (item.offer_id or '').strip()
        }

        def _sum_spend(campaign_ids: list[str]) -> Decimal:
            if not campaign_ids:
                return Decimal('0')
            total = Decimal('0')
            entries = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                ozon_campaign_id__in=campaign_ids,
                report_date__gte=month_start,
                report_date__lte=today,
            )
            for entry in entries.iterator():
                totals = entry.totals or {}
                total += _parse_decimal(totals.get('moneySpent'))
            return total

        total_spent_manual = _sum_spend(manual_campaign_ids)
        total_spent_auto = _sum_spend(auto_campaign_ids)

        # available_month_budget = month_budget - total_spent_manual - total_spent_auto - manual_week_reserved
        available_month_budget = month_budget - total_spent_manual - total_spent_auto

        if available_month_budget < 0:
            available_month_budget = Decimal('0')

        period_days = min(7, days_left)
        if period_days <= 0:
            logger.info(f"[ℹ️] Нет дней для перерасчёта (period_days={period_days})")
            return {"skipped": True, "reason": "period days zero"}

        weekly_pool = (available_month_budget / Decimal(days_left)) * Decimal(period_days) - manual_week_reserved if days_left else Decimal('0')
        weekly_pool = max(weekly_pool, Decimal('0'))

        campaign_type_index = 4  # колонка E
        offer_id_index = 5       # колонка F
        sku_index = 6            # колонка G
        share_index = 49         # колонка AX

        auto_rows: list[dict] = []
        ozon_access_token: str | None = None

        while True:
            col_e = ws.col_values(5)
            col_a = ws.col_values(1)
            total_rows = len(col_a)
            manual_start = None
            for idx in range(start_row, len(col_e) + 1):
                if str(col_e[idx - 1]).strip().lower().startswith('руч'):
                    manual_start = idx
                    break
            auto_end = manual_start - 1 if manual_start else total_rows
            if auto_end < start_row:
                logger.info("[ℹ️] На листе нет автоматических кампаний для перерасчёта")
                return {"skipped": True, "reason": "no auto rows"}

            auto_rows_values = ws.get(f'A{start_row}:AX{auto_end}')

            auto_rows = []
            auto_offer_ids_in_sheet: set[str] = set()
            empty_auto_rows: list[int] = []
            for i, row in enumerate(auto_rows_values):
                row_number = start_row + i
                row_values = row[:] if row else []
                if len(row_values) <= share_index:
                    row_values += [''] * (share_index + 1 - len(row_values))

                has_data = any(str(cell).strip() for cell in row_values)
                if not has_data:
                    empty_auto_rows.append(row_number)
                    continue

                campaign_id_raw = str(row_values[0]).strip() if len(row_values) > 0 else ''
                campaign_type = str(row_values[campaign_type_index]).strip().lower() if len(row_values) > campaign_type_index else ''
                if campaign_type.startswith('руч'):
                    empty_auto_rows.append(row_number)
                    continue

                offer_id_cell = str(row_values[offer_id_index]).strip() if len(row_values) > offer_id_index else ''
                fallback_offer_id = ''
                if campaign_id_raw and campaign_id_raw in ad_items_by_campaign and not offer_id_cell:
                    fallback_offer_id = (ad_items_by_campaign[campaign_id_raw].offer_id or '').strip()

                offer_id_effective = offer_id_cell or fallback_offer_id
                if offer_id_cell:
                    auto_offer_ids_in_sheet.add(offer_id_cell)
                if fallback_offer_id:
                    auto_offer_ids_in_sheet.add(fallback_offer_id)

                share_raw = row_values[share_index] if len(row_values) > share_index else ''
                share_value = _parse_decimal(share_raw)
                if share_value < 0:
                    share_value = Decimal('0')

                if campaign_id_raw:
                    key = campaign_id_raw
                elif offer_id_effective:
                    key = f"MANDATORY::{offer_id_effective}"
                else:
                    key = f"MANDATORY::ROW{row_number}"

                auto_rows.append({
                    'row_number': row_number,
                    'campaign_id': campaign_id_raw,
                    'key': key,
                    'share': share_value,
                    'offer_id': offer_id_effective,
                    'sku': str(row_values[sku_index]).strip() if len(row_values) > sku_index else '',
                    'row_values': row_values,
                })

            if not auto_rows:
                logger.info("[ℹ️] Автоматических кампаний для перерасчёта не найдено")
                return {"skipped": True, "reason": "no auto campaigns"}

            # 1) Удаляем кампании, которые больше не являются обязательными
            rows_to_delete: list[int] = []
            removal_entries: list[tuple[dict, AdPlanItem]] = []
            for row in auto_rows:
                campaign_id = row['campaign_id']
                if not campaign_id:
                    continue
                ad_item = ad_items_by_campaign.get(campaign_id)
                if not ad_item or not ad_item.is_mandatory:
                    continue
                offer_id_cell = (row.get('offer_id') or '').strip()
                if not offer_id_cell:
                    offer_id_cell = (ad_item.offer_id or '').strip()
                if offer_id_cell in mandatory_offer_ids_set:
                    continue
                removal_entries.append((row, ad_item))
                rows_to_delete.append(row['row_number'])

            if removal_entries:
                if ozon_access_token is None:
                    try:
                        from .utils import get_store_performance_token  # локальный импорт, чтобы не тянуть токен без надобности
                        token_info = get_store_performance_token(store)
                        ozon_access_token = token_info.get("access_token")
                        if not ozon_access_token:
                            logger.error(f"[❌] Не удалось получить access_token для деактивации кампаний магазина {store}")
                    except Exception as token_err:
                        logger.error(f"[❌] Ошибка получения access_token для деактивации кампаний магазина {store}: {token_err}")
                        ozon_access_token = None

                for row, ad_item in removal_entries:
                    campaign_id = ad_item.ozon_campaign_id
                    if ozon_access_token:
                        try:
                            deactivate_campaign(access_token=ozon_access_token, campaign_id=campaign_id)
                            logger.info(f"[🛑] Кампания {campaign_id} деактивирована в Ozon (убрана из обязательных)")
                        except Exception as deactivate_err:
                            logger.error(f"[❌] Не удалось деактивировать кампанию {campaign_id} в Ozon: {deactivate_err}")
                    ad_item.is_mandatory = False
                    ad_item.is_active_in_sheets = False
                    ad_item.google_sheet_row = None
                    try:
                        ad_item.save(update_fields=['is_mandatory', 'is_active_in_sheets', 'google_sheet_row'])
                    except Exception as save_err:
                        logger.warning(f"[⚠️] Не удалось обновить флаги кампании {campaign_id} после удаления: {save_err}")

                for row_num in sorted(rows_to_delete, reverse=True):
                    try:
                        ws.delete_rows(row_num)
                        logger.info(f"[🗑️] Строка {row_num} удалена с листа Main_ADV (кампания больше не обязательна)")
                    except Exception as delete_err:
                        logger.error(f"[❌] Не удалось удалить строку {row_num} из Main_ADV: {delete_err}")

                # После удаления строк перечитываем лист
                continue

            # 2) Добавляем обязательные кампании, отсутствующие в списке
            rows_to_append: list[list] = []
            appended_offers: list[str] = []
            for offer_id in mandatory_offer_ids_list:
                if not offer_id or offer_id in auto_offer_ids_in_sheet:
                    continue
                item = ad_items_by_offer.get(offer_id)
                sku_value = item.sku if item and item.sku else None
                campaign_name_value = (item.campaign_name or item.name) if item else None
                if not sku_value:
                    product = Product.objects.filter(store=store, offer_id__iexact=offer_id).first()
                    if product:
                        sku_value = product.sku
                        if not campaign_name_value:
                            campaign_name_value = product.name or f"Auto {offer_id}"
                if not sku_value:
                    logger.warning(f"[⚠️] Не удалось определить SKU для обязательного offer_id '{offer_id}', строка не добавлена")
                    continue
                if item and not item.is_mandatory:
                    item.is_mandatory = True
                    try:
                        item.save(update_fields=['is_mandatory'])
                    except Exception as save_err:
                        logger.warning(f"[⚠️] Не удалось установить флаг is_mandatory для кампании {item.ozon_campaign_id}: {save_err}")
                sku_str = str(int(sku_value)) if str(sku_value).isdigit() else str(sku_value)
                week_budget_str = str(float(min_budget)) if min_budget and min_budget > 0 else '0'
                row_values = [
                    '',
                    '1',
                    '',
                    campaign_name_value or f"Auto {offer_id}",
                    'Авто',
                    offer_id,
                    sku_str,
                    '',
                    '',
                    week_budget_str,
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                ]
                rows_to_append.append(row_values)
                appended_offers.append(offer_id)

            if rows_to_append:
                for offer_id, row_values in zip(appended_offers, rows_to_append):
                    auto_offer_ids_in_sheet.add(offer_id)
                    new_dict = {
                        'row_number': auto_end + len(auto_rows) + 1,
                        'campaign_id': '',
                        'key': f"MANDATORY::{offer_id}",
                        'share': Decimal('0'),
                        'offer_id': offer_id,
                        'sku': row_values[6],
                        'row_values': row_values,
                    }
                    auto_rows.append(new_dict)

            break

        share_sum = sum((row['share'] for row in auto_rows if row['share'] > 0), Decimal('0'))
        share_sum = share_sum if share_sum > 0 else Decimal('0')

        if weekly_pool <= 0:
            logger.info("[ℹ️] Свободный бюджет равен нулю — усекаем недельные бюджеты до нуля")

        per_campaign_equal = (weekly_pool / Decimal(len(auto_rows))) if weekly_pool > 0 else Decimal('0')

        # === Шаг 1. Первичный расчёт ===
        new_budgets: dict[str, Decimal] = {}
        for row in auto_rows:
            row_key = row['key']
            if weekly_pool <= 0:
                week_amount = Decimal('0')
            elif share_sum > 0:
                if row['share'] > 0:
                    week_amount = weekly_pool * (row['share'] / share_sum)
                else:
                    week_amount = Decimal('0')
            else:
                week_amount = per_campaign_equal

            # Применяем минимальный бюджет
            if min_budget > 0:
                week_amount = max(week_amount, min_budget)

            week_amount = week_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            new_budgets[row_key] = week_amount

        # === Шаг 2. Проверяем общий лимит ===
        total_assigned = sum(new_budgets.values())

        if weekly_pool > 0 and total_assigned > weekly_pool:
            # Разделяем кампании: с минимальным бюджетом и выше
            min_locked = {cid: b for cid, b in new_budgets.items() if b <= min_budget}
            adjustable = {cid: b for cid, b in new_budgets.items() if b > min_budget}

            # Сколько остаётся после вычета минималок
            remaining_pool = weekly_pool - sum(min_locked.values())

            if remaining_pool < 0:
                # Если даже минимальные бюджеты превышают пул — уменьшаем пропорционально
                scale = weekly_pool / total_assigned
                for cid in new_budgets:
                    new_budgets[cid] = (new_budgets[cid] * scale).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            else:
                # Масштабируем только те, что выше минимума
                total_adjustable = sum(adjustable.values())
                if total_adjustable > 0:
                    scale = remaining_pool / total_adjustable
                    for cid in adjustable:
                        new_budgets[cid] = (new_budgets[cid] * scale).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # === Итоговое выравнивание (минимальная погрешность) ===
        def _recalc_total() -> Decimal:
            return sum(new_budgets.values(), Decimal('0'))

        total_assigned = _recalc_total()
        diff = weekly_pool - total_assigned
        if abs(diff) >= Decimal('0.01'):
            if diff < 0:
                # Нужно уменьшить сумму: прежде всего пробуем скорректировать значения выше минимума
                adjustable_keys = [key for key, val in new_budgets.items() if val > min_budget]
                reducible = sum((new_budgets[key] - min_budget) for key in adjustable_keys)
                need = abs(diff)
                if reducible > Decimal('0') and need <= reducible + Decimal('0.005'):
                    if reducible > Decimal('0'):
                        scale = (reducible - need) / reducible if reducible else Decimal('0')
                        for key in adjustable_keys:
                            excess = new_budgets[key] - min_budget
                            new_value = min_budget + (excess * scale)
                            new_budgets[key] = new_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    scale = (weekly_pool / total_assigned) if total_assigned > 0 else Decimal('0')
                    for key in new_budgets:
                        new_budgets[key] = (new_budgets[key] * scale).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            else:
                # Нужно добавить бюджет — просто докидываем к первой кампании
                first_key = next(iter(new_budgets))
                new_budgets[first_key] = (new_budgets[first_key] + diff).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            total_assigned = _recalc_total()
            tail_diff = weekly_pool - total_assigned
            if abs(tail_diff) >= Decimal('0.01'):
                first_key = next(iter(new_budgets))
                new_value = new_budgets[first_key] + tail_diff
                if new_value < min_budget:
                    new_value = min_budget
                new_budgets[first_key] = new_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        auto_map = {str(item.ozon_campaign_id): item for item in auto_items}
        updated_count = 0
        new_rows_as: list[list] = []
        debug_budget_rows: list[tuple[str, float]] = []

        for idx, row in enumerate(auto_rows):
            key = row['key']
            cid = row['campaign_id']
            week_amount = new_budgets.get(key, Decimal('0'))
            day_amount = (week_amount / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            row_values = row['row_values']
            if len(row_values) <= share_index:
                row_values += [''] * (share_index + 1 - len(row_values))
            row_values[9] = float(week_amount)
            row_values[11] = float(day_amount)

            new_rows_as.append(row_values[:19])
            debug_budget_rows.append((cid or row['offer_id'], float(week_amount)))

            new_row_number = start_row + idx

            if cid:
                ad_item = auto_map.get(cid)
                if ad_item:
                    ad_item.week_budget = float(week_amount)
                    ad_item.day_budget = float(day_amount)
                    save_fields = ['week_budget', 'day_budget']
                    if ad_item.google_sheet_row != new_row_number:
                        ad_item.google_sheet_row = new_row_number
                        save_fields.append('google_sheet_row')
                    try:
                        ad_item.save(update_fields=save_fields)
                    except Exception as save_err:
                        logger.warning(f"[⚠️] Не удалось сохранить бюджеты для кампании {cid}: {save_err}")
            updated_count += 1

        new_end_row = start_row + len(new_rows_as) - 1
        try:
            ws.batch_clear([f'A{start_row}:S{auto_end}'])
        except Exception as clear_err:
            logger.warning(f"[⚠️] Не удалось очистить диапазон авто-кампаний: {clear_err}")

        if new_rows_as:
            try:
                ws.update(f'A{start_row}:S{new_end_row}', new_rows_as, value_input_option='USER_ENTERED')
            except Exception as write_err:
                logger.error(f"[❌] Ошибка при записи обновлённого блока авто-кампаний: {write_err}")
        if debug_budget_rows:
            sum_budget = sum(budget for _, budget in debug_budget_rows)
            logger.info("[🧮] Новые недельные бюджеты авто-кампаний:")
            for cid, budget in debug_budget_rows:
                logger.info(f"    - {cid}: {budget}")
            logger.info(f"[🧾] Сумма новых недельных бюджетов: {sum_budget}")

        logger.debug(f"[📊] Итоговая сумма недельных бюджетов: {float(total_assigned)} (пул: {float(weekly_pool)})")

        logger.info(
            f"[✅] Перерасчёт бюджетов завершён. Авто кампаний: {len(auto_rows)}."
            f" Monthly budget={month_budget}, spent_auto={total_spent_auto}, spent_manual={total_spent_manual},"
            f" Всего израсходованно={total_spent_auto + total_spent_manual},"
            f" Недельный ручной бюджет={manual_week_reserved}, Итого осталось бюджета для авто компаний на неделю={weekly_pool}"
        )

        try:
            ws.update('C8', [[float(manual_week_reserved)]], value_input_option='USER_ENTERED')
            ws.update('C7', [[float((available_month_budget / Decimal(days_left)) * Decimal(period_days))]], value_input_option='USER_ENTERED')
            ws.update('D7', [[float((available_month_budget / Decimal(days_left)) * Decimal(1))]], value_input_option='USER_ENTERED')

            ws.update('E7', [[datetime.now().strftime('%d/%m/%y')]])

        except Exception as c8_err:
            logger.warning(f"[⚠️] Не удалось обновить C8 недельным бюджетом ручных кампаний: {c8_err}")

        # remaining_month_budget = month_budget - total_spent_manual - total_spent_auto - manual_week_reserved
        remaining_month_budget = month_budget - total_spent_manual - total_spent_auto

        if remaining_month_budget < 0:
            remaining_month_budget = Decimal('0')
        try:
            ws.update('B7', [[float(remaining_month_budget)]], value_input_option='USER_ENTERED')
            ws.update('B9', [[float(remaining_month_budget)]], value_input_option='USER_ENTERED')

        except Exception as b9_err:
            logger.warning(f"[⚠️] Не удалось обновить B9 остатком бюджета: {b9_err}")

        result_payload = {
            "updated": updated_count,
            "weekly_pool": float(weekly_pool),
            "manual_reserved": float(manual_week_reserved),
            "spent_auto": float(total_spent_auto),
            "spent_manual": float(total_spent_manual),
        }
        try:
            logger.info("[🔁] Запускаем create_or_update_AD после перерасчёта бюджетов")
            create_or_update_AD(
                spreadsheet_url=spreadsheet_url,
                sa_json_path=sa_json_path,
                worksheet_name=worksheet_name,
                start_row=start_row,
            )
        except Exception as update_err:
            logger.error(f"[❌] Ошибка при запуске create_or_update_AD после перерасчёта: {update_err}")

        return result_payload

    except Exception as e:
        logger.error(f"[❌] Ошибка перерасчёта недельных бюджетов авто-кампаний: {e}")
        return {"error": str(e)}



# =========================
# sync_manual_campaigns
# =========================    
# Раз в час обновляем все ручные компании
# Не зависит от флага включена или выключена компнаия
@shared_task(name="Синхронизация ручных рекламных кампаний")
def sync_manual_campaigns(store_id: int = None):
    try:
        from .models import ManualCampaign, Product
        
        # Определяем магазины для обработки
        if store_id:
            stores = OzonStore.objects.filter(id=store_id)
        else:
            stores = OzonStore.objects.all()
            
        if not stores.exists():
            logger.warning(f"[⚠️] Магазины для синхронизации не найдены")
            return
            
        total_synced = 0
        total_created = 0
        total_updated = 0
        total_errors = 0
        total_skipped = 0
        
        for store in stores:
            try:
                logger.info(f"[▶️] Синхронизация ручных кампаний для магазина: {store}")
                
                # Получаем список кампаний
                campaigns = fetch_campaigns_from_ozon(store)
                
                if not campaigns:
                    logger.info(f"[ℹ️] Для магазина {store} не найдено активных кампаний")
                    continue

                # Список автоматических кампаний (чтобы не дублировать)
                auto_campaign_ids = set(
                    AdPlanItem.objects.filter(store=store)
                    .exclude(ozon_campaign_id__isnull=True)
                    .exclude(ozon_campaign_id='')
                    .values_list('ozon_campaign_id', flat=True)
                )
                logger.info(f"[ℹ️] Найдено {len(auto_campaign_ids)} авто-кампаний в AdPlanItem для магазина {store}")

                # Удаляем из ManualCampaign кампании, которые присутствуют среди автоматических
                if auto_campaign_ids:
                    deleted_count, _ = ManualCampaign.objects.filter(
                        store=store,
                        ozon_campaign_id__in=auto_campaign_ids
                    ).delete()
                    if deleted_count:
                        logger.info(f"[🧹] Удалено {deleted_count} кампаний из ManualCampaign, так как они есть среди автоматических")
                
                # Получаем список ID кампаний, которые уже есть в ManualCampaign
                existing_campaign_ids = set(
                    ManualCampaign.objects.filter(
                        store=store,
                        ozon_campaign_id__isnull=False
                    ).exclude(
                        ozon_campaign_id=''
                    ).values_list('ozon_campaign_id', flat=True)
                )
                
                # if existing_campaign_ids:
                #     logger.info(f"[🔍] Найдено {len(existing_campaign_ids)} кампаний в ManualCampaign для магазина {store}")
                # else:
                #     logger.info(f"[ℹ️] В ManualCampaign нет кампаний для магазина {store}")
                
                # Обрабатываем каждую кампанию
                for campaign_data in campaigns:
                    try:
                        campaign_id = campaign_data.get('id')
                        if not campaign_id:
                            logger.warning(f"[⚠️] Пропускаем кампанию без ID: {campaign_data}")
                            continue
                        
                        # Проверяем, есть ли эта кампания уже в ManualCampaign
                        # campaign_exists = str(campaign_id) in existing_campaign_ids
                        # if campaign_exists:
                        #     logger.info(f"[ℹ️] Кампания {campaign_id} уже существует в ManualCampaign, будет обновлена")

                        # Пропускаем кампанию, если она есть среди автоматических
                        if str(campaign_id) in auto_campaign_ids:
                            total_skipped += 1
                            # logger.info(f"[⏭️] Кампания {campaign_id} относится к авто-кампаниям. Пропускаем из синхронизации ручных.")
                            continue
                        
                        # Проверяем обязательные поля
                        if not isinstance(campaign_data, dict):
                            # logger.warning(f"[⚠️] Пропускаем кампанию с некорректными данными: {campaign_data}")
                            continue
                        
                        # Проверяем наличие обязательных полей
                        if 'title' not in campaign_data:
                            logger.warning(f"[⚠️] Пропускаем кампанию {campaign_id} без названия")
                            continue
                            
                        # Получаем объекты кампании (SKU/товары)
                        campaign_objects = fetch_campaign_objects_from_ozon(
                            store, campaign_id
                        )
                        
                        # Определяем SKU и offer_id из объектов кампании
                        sku = None
                        offer_id = None
                        
                        # Логируем информацию о кампании для отладки
                        # logger.info(f"[🔍] Обработка кампании {campaign_id}: {campaign_data.get('title', 'Без названия')}")
                        
                        # Обрабатываем все объекты кампании (SKU/товары)
                        # В ручных кампаниях может быть несколько SKU, поэтому обрабатываем все
                        # Все SKU сохраняются в одном месте - в ManualCampaign
                        # Это упрощает управление и не требует создания множественных записей
                        sku_list = []
                        offer_id_list = []
                        
                        if campaign_objects and len(campaign_objects) > 0:
                            # logger.info(f"[🔍] Найдено {len(campaign_objects)} объектов в кампании {campaign_id}")
                            
                            for obj in campaign_objects:
                                sku_raw = obj.get('id')
                                
                                # Безопасная конвертация SKU в int
                                try:
                                    sku_item = int(sku_raw) if sku_raw is not None else None
                                    if sku_item:
                                        sku_list.append(sku_item)
                                        
                                        # Пытаемся найти offer_id по SKU в базе товаров
                                        product = Product.objects.filter(sku=sku_item).first()
                                        if product:
                                            offer_id_list.append(product.offer_id)
                                        else:
                                            offer_id_list.append(None)
                                            
                                except (ValueError, TypeError):
                                    logger.warning(f"[⚠️] Некорректный SKU для кампании {campaign_id}: {sku_raw}")
                            
                            # Для обратной совместимости оставляем первый SKU как основной
                            # Все остальные SKU сохраняются в sku_list и offer_id_list
                            sku = sku_list[0] if sku_list else None
                            offer_id = offer_id_list[0] if offer_id_list else None
                            
                            # logger.info(f"[ℹ️] Обработано SKU: {sku_list}, offer_id: {offer_id_list}")
                        else:
                            logger.warning(f"[⚠️] В кампании {campaign_id} не найдено объектов (SKU)")
                        
                        # Подготавливаем данные для сохранения
                        # Безопасная конвертация бюджетов с проверкой на None
                        daily_budget_raw = campaign_data.get('dailyBudget')
                        total_budget_raw = campaign_data.get('budget') 
                        weekly_budget_raw = campaign_data.get('weeklyBudget')
                        
                        daily_budget = Decimal(daily_budget_raw) / 100 if daily_budget_raw is not None else Decimal('0')
                        total_budget = Decimal(total_budget_raw) / 100 if total_budget_raw is not None else Decimal('0')
                        
                        # Обрабатываем недельный бюджет (он может быть в микрорублях, поэтому делим на 1000000)
                        if weekly_budget_raw is not None and str(weekly_budget_raw) != '0':
                            weekly_budget = Decimal(weekly_budget_raw) / 1000000
                        else:
                            weekly_budget = daily_budget * 7  # Приблизительно из дневного
                        
                        # Безопасная обработка дат
                        from_date = campaign_data.get('fromDate')
                        to_date = campaign_data.get('toDate')
                        
                        # Конвертируем строки дат в объекты date, если они есть и не пустые
                        try:
                            if from_date and isinstance(from_date, str) and from_date.strip():
                                from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
                            elif not from_date or (isinstance(from_date, str) and not from_date.strip()):
                                from_date = None
                        except (ValueError, TypeError):
                            from_date = None
                            
                        try:
                            if to_date and isinstance(to_date, str) and to_date.strip():
                                to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
                            elif not to_date or (isinstance(to_date, str) and not to_date.strip()):
                                to_date = None
                        except (ValueError, TypeError):
                            to_date = None
                        
                        # Обрабатываем временные метки из Ozon API
                        ozon_created_at = None
                        ozon_updated_at = None
                        
                        try:
                            created_at_str = campaign_data.get('createdAt')
                            if created_at_str:
                                from django.utils import timezone
                                # Парсим ISO формат с timezone: "2019-10-07T06:28:44.055042Z"
                                ozon_created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                                # Конвертируем в timezone-aware datetime
                                if timezone.is_naive(ozon_created_at):
                                    ozon_created_at = timezone.make_aware(ozon_created_at)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"[⚠️] Не удалось распарсить createdAt для кампании {campaign_id}: {e}")
                            ozon_created_at = None
                        
                        try:
                            updated_at_str = campaign_data.get('updatedAt')
                            if updated_at_str:
                                from django.utils import timezone
                                # Парсим ISO формат с timezone: "2020-10-01T06:28:44.055042Z"
                                ozon_updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
                                # Конвертируем в timezone-aware datetime
                                if timezone.is_naive(ozon_updated_at):
                                    ozon_updated_at = timezone.make_aware(ozon_updated_at)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"[⚠️] Не удалось распарсить updatedAt для кампании {campaign_id}: {e}")
                            ozon_updated_at = None
                        
                        # Проверяем, что store не None
                        if not store:
                            logger.error(f"[❌] Store не может быть None для кампании {campaign_id}")
                            continue
                        
                        # Проверяем, что store имеет необходимые поля для Performance API
                        if not hasattr(store, 'performance_client_id') or not store.performance_client_id:
                            logger.error(f"[❌] У магазина {store} отсутствует performance_client_id")
                            continue
                        
                        if not hasattr(store, 'performance_client_secret') or not store.performance_client_secret:
                            logger.error(f"[❌] У магазина {store} отсутствует performance_client_secret")
                            continue
                        
                        # Проверяем, что все обязательные поля заполнены
                        if not campaign_data.get('title'):
                            logger.warning(f"[⚠️] Пропускаем кампанию {campaign_id} без названия")
                            continue
                        
                        campaign_defaults = {
                            'name': str(campaign_data.get('title', '')) if campaign_data.get('title') else '',
                            'offer_id': str(offer_id) if offer_id else '',  # Основной Offer ID для обратной совместимости
                            'sku': sku,  # Основной SKU для обратной совместимости (первый из списка)
                            'sku_list': sku_list,  # Список всех SKU в кампании (для множественных SKU)
                            'offer_id_list': offer_id_list,  # Список всех Offer ID в кампании (для множественных SKU)
                            'week_budget': weekly_budget,  # Используем недельный бюджет
                            'daily_budget': daily_budget,
                            'total_budget': total_budget,
                            'state': str(campaign_data.get('state', ManualCampaign.CAMPAIGN_STATE_UNKNOWN)),
                            'payment_type': str(campaign_data.get('PaymentType', campaign_data.get('paymentType', ManualCampaign.PAYMENT_TYPE_CPO))),
                            'adv_object_type': str(campaign_data.get('advObjectType', ManualCampaign.ADV_OBJECT_TYPE_SKU)),
                            'from_date': from_date,
                            'to_date': to_date,
                            'placement': campaign_data.get('placement') if campaign_data.get('placement') and isinstance(campaign_data.get('placement'), list) else [],
                            'product_autopilot_strategy': str(campaign_data.get('productAutopilotStrategy', '')) if campaign_data.get('productAutopilotStrategy') else '',
                            'product_campaign_mode': str(campaign_data.get('productCampaignMode', '')) if campaign_data.get('productCampaignMode') else '',
                            'ozon_created_at': ozon_created_at,
                            'ozon_updated_at': ozon_updated_at,
                            'store': store,
                        }
                        
                        # Обрабатываем автоувеличение бюджета
                        auto_increase = campaign_data.get('autoIncrease', {})
                        if auto_increase:
                            auto_increased_budget_raw = auto_increase.get('autoIncreasedBudget')
                            auto_increased_budget = Decimal(auto_increased_budget_raw) / 100 if auto_increased_budget_raw is not None else Decimal('0')
                            
                            campaign_defaults.update({
                                'auto_increase_percent': int(auto_increase.get('autoIncreasePercent', 0)) if auto_increase.get('autoIncreasePercent') is not None else 0,
                                'auto_increased_budget': auto_increased_budget,
                                'is_auto_increased': bool(auto_increase.get('isAutoIncreased', False)),
                                'recommended_auto_increase_percent': int(auto_increase.get('recommendedAutoIncreasePercent', 0)) if auto_increase.get('recommendedAutoIncreasePercent') is not None else 0,
                            })
                        
                        # Создаем или обновляем кампанию
                        # logger.info(f"[🔍] Создание/обновление кампании {campaign_id} с данными: {campaign_defaults}")
                        
                        campaign, created = ManualCampaign.objects.update_or_create(
                            ozon_campaign_id=str(campaign_id),
                            defaults=campaign_defaults
                        )
                        
                        if created:
                            total_created += 1
                            # logger.info(f"[✅] Создана новая ручная кампания: {campaign.name} (ID: {campaign_id})")
                        else:
                            total_updated += 1
                            # logger.info(f"[🔄] Обновлена ручная кампания: {campaign.name} (ID: {campaign_id})")
                        

                        total_synced += 1
                        
                    except Exception as e:
                        total_errors += 1
                        logger.error(f"[❌] Ошибка обработки кампании {campaign_id}: {e}")
                        # logger.error(f"[🔍] Данные кампании: {campaign_data}")
                        continue
                        
                # logger.info(f"[✅] Синхронизация завершена для магазина: {store}")
                
            except Exception as e:
                total_errors += 1
                logger.error(f"[❌] Ошибка синхронизации для магазина {store}: {e}")
                continue
        
        logger.info(f"[📊] Итоги синхронизации ручных кампаний: "
                   f"синхронизировано {total_synced}, создано {total_created}, "
                   f"обновлено {total_updated}, пропущено {total_skipped}, ошибок {total_errors}")
        
    except Exception as e:
        logger.error(f"[❌] Критическая ошибка синхронизации ручных кампаний: {e}")


#---------------------------------------------------------------

# =========================
# create_or_update_AD
# =========================    
# Данная функция считывает данные с гугл таблицы и на основе этих данных создает и обновялет 
# автоматические рекламные компании
# Не зависит от флага включена или выключена компнаия
# @shared_task(name="Чтение данных из Google Sheets и создание рекламной компании")
def create_or_update_AD(spreadsheet_url: str = None, sa_json_path: str = None, worksheet_name: str = "Main_ADV", start_row: int = 13, block_size: int = 100):
    """
    Читает данные из Google Sheets до тех пор, пока не встретит 5 пустых строк подряд.
    
    Args:
        spreadsheet_url: URL Google таблицы
        sa_json_path: Путь к файлу сервисного аккаунта
        worksheet_name: Название листа (по умолчанию "Main_ADV")
        start_row: Номер строки, с которой начинать чтение (по умолчанию 13)
        block_size: Размер блока для чтения (по умолчанию 100 строк)
    
    Returns:
        list: Массив строк с данными из таблицы
    """
    
    spreadsheet_url = spreadsheet_url or ""
    sa_json_path = sa_json_path or "/workspace/ozon-469708-c5f1eca77c02.json"
    
    logger.info(f"[📖] Начинаем чтение данных из Google Sheets: {worksheet_name}, строка {start_row}")
    
    try:
        # Авторизация в Google Sheets
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        gc = gspread.authorize(creds)
        t0 = time.perf_counter()
        
        # Открываем таблицу и лист
        sh = gc.open_by_url(spreadsheet_url)
        ws = sh.worksheet(worksheet_name)

        t_open = time.perf_counter()
        logger.info(f"[⏱] Открытие таблицы: {t_open - t0:.3f}s")
        
        # Читаем данные блоками для оптимизации
        data_rows = []
        empty_row_count = 0
        current_row = start_row
        max_empty_rows = 5  # Остановиться после 5 пустых строк подряд
        # block_size передается как параметр
        
        logger.info(f"[📊] Начинаем чтение блоками по {block_size} строк с строки {start_row}")
        
        while empty_row_count < max_empty_rows:
            try:
                # Читаем блок строк A:L (с учётом новых колонок бюджетов J-L)
                end_row = current_row + block_size - 1
                block_range = f'A{current_row}:L{end_row}'
                
                t_block_start = time.perf_counter()
                block_data = ws.get(block_range)
                t_block_read = time.perf_counter()
                
                logger.debug(f"[📦] Блок {current_row}-{end_row}: чтение за {t_block_read - t_block_start:.3f}s")
                
                if not block_data:
                    # Весь блок пустой
                    empty_row_count += block_size
                    current_row += block_size
                    logger.debug(f"[⭕] Блок {current_row}-{end_row}: полностью пустой")
                    continue             
                
                # Обрабатываем каждую строку в блоке
                rows_with_data_in_block = 0
                consecutive_empty_in_block = 0
                
                for i, row_data in enumerate(block_data):
                    row_number = current_row + i
                    
                    # Дополняем до 12 столбцов если нужно
                    row_values = row_data[:]
                    while len(row_values) < 12:
                        row_values.append('')
                    
                    # Проверяем, есть ли хотя бы одно непустое значение
                    has_data = any(str(cell).strip() for cell in row_values)
                    
                    if has_data:
                        # Строка содержит данные
                        data_rows.append({
                            'row_number': row_number,
                            'campaign_id': row_values[0],           # A: ID Кампании
                            'active': row_values[1],               # B: ВКЛ.
                            'status': row_values[2],               # C: Статус
                            'campaign_name': row_values[3],        # D: Название кампании
                            'campaign_type': row_values[4],        # E: Тип кампании
                            'article': row_values[5],              # F: Артикул
                            'sku': row_values[6],                  # G: SKU
                            'week_budget': row_values[9],          # J: Бюджет на нед.
                            'manual_week_budget': row_values[10],  # K: Бюджет на нед. РУЧНОЙ
                            'day_budget': row_values[11],          # L: Бюджет на день, руб.
                        })
                        rows_with_data_in_block += 1
                        consecutive_empty_in_block = 0  # Сбрасываем счетчик пустых строк в блоке
                        empty_row_count = 0  # Сбрасываем общий счетчик пустых строк
                else:
                        # Строка пустая
                        consecutive_empty_in_block += 1
                
                # Обновляем счетчик пустых строк
                if rows_with_data_in_block == 0:
                    # Весь блок пустой
                    empty_row_count += block_size
                else:
                    # В блоке есть данные, но может быть пустые строки в конце
                    # Проверяем последние строки блока
                    empty_at_end = 0
                    for i in range(len(block_data) - 1, -1, -1):
                        row_values = block_data[i][:]
                        while len(row_values) < 10:
                            row_values.append('')
                        if not any(str(cell).strip() for cell in row_values):
                            empty_at_end += 1
                        else:
                            break
                    empty_row_count = empty_at_end
                
                current_row += len(block_data)
                logger.debug(f"[📦] Блок обработан: {rows_with_data_in_block} строк с данными, {consecutive_empty_in_block} пустых")
                
                # Если прочитали меньше строк чем ожидали, значит достигли конца листа
                if len(block_data) < block_size:
                    logger.info(f"[📄] Достигнут конец листа на строке {current_row}")
                    break
                
                # Защита от бесконечного цикла
                if current_row > start_row + 10000:
                    logger.warning(f"[⚠️] Достигнут лимит строк (10000), останавливаем чтение")
                    break
                    
            except Exception as e:
                logger.error(f"[❌] Ошибка при чтении блока начиная со строки {current_row}: {e}")
                # В случае ошибки переходим к следующему блоку
                current_row += block_size
                empty_row_count += block_size
        
        t_read = time.perf_counter()
        logger.info(f"[⏱] Чтение данных завершено: {t_read - t_open:.3f}s")
        logger.info(f"[📊] Прочитано строк с данными: {len(data_rows)}")
        logger.info(f"[📊] Последняя обработанная строка: {current_row - 1}")
        logger.info(f"[📊] Остановка: {empty_row_count} пустых строк подряд")
        
        # Получаем настройки из Google Sheets
        try:
            # Получаем название магазина из ячейки T23
            store_name_cell = ws.get('V23')[0][0] if ws.get('V23') and ws.get('V23')[0] else ''
            logger.info(f"[🏪] Название магазина из T23: '{store_name_cell}'")
            
            # Получаем время обучения из ячейки T17 (в днях)
            train_days_cell = ws.get('V17')[0][0] if ws.get('V17') and ws.get('V17')[0] else '0'
            try:
                train_days = int(train_days_cell) if train_days_cell else 0
            except (ValueError, TypeError):
                train_days = 0
            logger.info(f"[📅] Время обучения из V17: {train_days} дней")
            
            # Находим магазин в базе данных
            store = None
            if store_name_cell:
                try:
                    store = OzonStore.objects.get(name=store_name_cell)
                    logger.info(f"[✅] Магазин найден: {store}")
                except OzonStore.DoesNotExist:
                    logger.error(f"[❌] Магазин '{store_name_cell}' не найден в базе данных")
                    return data_rows
            else:
                logger.error(f"[❌] Ячейка T23 пустая - не указан магазин")
                return data_rows
        except Exception as e:
            logger.error(f"[❌] Ошибка при получении настроек из Google Sheets: {e}")
            return data_rows
        
        # Список обязательных offer_id из листа Main_ADV
        mandatory_offer_ids_list, mandatory_offer_ids_set = _read_mandatory_offer_ids(ws, column_index=29, start_row=13)
        if mandatory_offer_ids_list:
            logger.info(f"[ℹ️] Обязательные offer_id на листе: {mandatory_offer_ids_list}")
        else:
            logger.info("[ℹ️] На листе нет обязательных offer_id")

        # Получаем токен один раз для всех операций
        from .utils import get_store_performance_token
        try:
            token_info = get_store_performance_token(store)
            access_token = token_info.get("access_token")
            if not access_token:
                logger.error(f"[❌] Не удалось получить access_token для магазина {store}")
                return data_rows
            logger.info(f"[🔑] Токен Performance API получен успешно для магазина {store}")
        except Exception as e:
            logger.error(f"[❌] Ошибка получения токена Performance API: {e}")
            return data_rows
        
        # Утилита: нормализация флага активности из ячейки B
        def _is_sheet_active(val: str):
            s = str(val or '').strip().lower()
            if s in ('1', 'true', 'да', 'вкл', 'on', 'включена'):
                return True
            if s in ('0', 'false', 'нет', 'выкл', 'off', 'выключена'):
                return False
            return None

        # Обрабатываем данные рекламных кампаний
        campaigns_created = 0
        campaigns_updated = 0
        campaigns_skipped = 0
        
        for ad_data in data_rows:
            # print(ad_data)
            # continue
            campaign_id = str(ad_data['campaign_id']).strip()
            
            if not campaign_id:
                # campaign_id пустое - создаем рекламу в Ozon
                try:
                    sku = str(ad_data['sku']).strip()
                    offer_id = str(ad_data['article']).strip()
                    campaign_name = str(ad_data['campaign_name']).strip()
                    week_budget = ad_data['week_budget']
                    manual_week_budget = ad_data['manual_week_budget']
                    active = str(ad_data['active']).strip()  # Параметр из ячейки B
                    
                    # Проверяем, что все необходимые данные есть
                    if not sku or not campaign_name or not week_budget:
                        logger.warning(f"[⚠️] Строка {ad_data['row_number']}: пропущена из-за пустых данных (SKU: '{sku}', название: '{campaign_name}', бюджет: '{week_budget}')")
                        campaigns_skipped += 1
                        continue
                    
                    try:
                        # Нормализуем бюджет: убираем пробелы (вкл. неразрывные) и меняем запятую на точку
                        week_budget_str = str(week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.')
                        week_budget_float = float(week_budget_str)
                        if week_budget_float <= 0:
                            logger.warning(f"[⚠️] Строка {ad_data['row_number']}: пропущена из-за нулевого бюджета ({week_budget_float})")
                            campaigns_skipped += 1
                            continue
                    except (ValueError, TypeError):
                        logger.warning(f"[⚠️] Строка {ad_data['row_number']}: некорректный бюджет '{week_budget}'")
                        campaigns_skipped += 1
                        continue
                    
                    # Обрабатываем ручной бюджет
                    try:
                        manual_budget_str = str(manual_week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.') if manual_week_budget else '0'
                        manual_budget_float = float(manual_budget_str) if manual_budget_str else 0.0
                    except (ValueError, TypeError):
                        manual_budget_float = 0.0
                        logger.debug(f"[ℹ️] Строка {ad_data['row_number']}: некорректный ручной бюджет '{manual_week_budget}', устанавливаем 0")
                    
                    # Выбираем бюджет к созданию: если указан ручной бюджет > 0, используем его, иначе расчётный
                    used_week_budget = manual_budget_float if manual_budget_float and manual_budget_float > 0 else week_budget_float
                    logger.info(f"[🚀] Создаем кампанию для SKU {sku}: '{campaign_name}', бюджет: {used_week_budget} (источник: {'ручной' if (manual_budget_float and manual_budget_float>0) else 'расчетный'})")

                    resp = create_cpc_product_campaign(
                        access_token=access_token,
                        sku=int(sku),
                        campaign_name=campaign_name,
                        weekly_budget_rub=used_week_budget,
                        placement = "PLACEMENT_TOP_PROMOTION",
                        product_autopilot_strategy = "TOP_MAX_CLICKS",
                        auto_increase_percent = 0
                    )
                    
                    if resp and isinstance(resp, dict) and resp.get('campaign_id'):
                        # Кампания создана успешно, записываем ID в таблицу
                        try:
                            campaign_id = str(resp['campaign_id'])
                            row_number = ad_data['row_number']
                            cell_a = f'A{row_number}'
                            ws.update(cell_a, [[campaign_id]])
                            try:
                                format_cell_ranges(ws, [(cell_a, CellFormat(horizontalAlignment='RIGHT'))])
                            except Exception as fmt_err:
                                logger.warning(f"[⚠️] Не удалось выровнять ячейку {cell_a} по правому краю: {fmt_err}")
                            logger.info(f"[✅] Кампания создана для SKU {sku}: ID {campaign_id}, записано в ячейку {cell_a}")
                            status_text = "Активна" if active == '1' else "Неактивна"
                            try:
                                ws.update(f'C{row_number}', [[status_text]])
                            except Exception as status_err:
                                logger.warning(f"[⚠️] Не удалось записать статус в C{row_number}: {status_err}")
                            # Проставляем тип кампании в колонке E — 'Авто'
                            try:
                                ws.update(f'E{row_number}', [["Авто"]])
                                logger.debug(f"[📝] Проставлен тип кампании 'Авто' в E{row_number}")
                            except Exception as e_type:
                                logger.warning(f"[⚠️] Не удалось проставить тип 'Авто' в E{row_number}: {e_type}")
                            
                            # Создаем запись в AdPlanItem для отслеживания этой кампании
                            try:
                                # Создаем AdPlanItem напрямую
                                ad_plan_item = AdPlanItem.objects.create(
                                    store=store,
                                    sku=int(sku),
                                    offer_id=offer_id,
                                    name=campaign_name,
                                    week_budget=used_week_budget,
                                    day_budget=used_week_budget / 7,
                                    manual_budget=manual_budget_float,  # Ручной бюджет из столбца I
                                    train_days=train_days,
                                    abc_label='',
                                    has_existing_campaign=False,  # Это новая кампания
                                    ozon_campaign_id=campaign_id,
                                    campaign_name=campaign_name,
                                    campaign_type='CPC_PRODUCT',
                                    state=AdPlanItem.CAMPAIGN_STATE_PLANNED,  # Изначально запланирована
                                    google_sheet_row=row_number,
                                    is_active_in_sheets=(active == '1'),  # Сохраняем статус активности из Google Sheets
                                    is_mandatory=bool(offer_id and offer_id in mandatory_offer_ids_set),
                                )
                                
                                logger.info(f"[📝] Создана запись AdPlanItem для кампании {campaign_id} (SKU: {sku})")
                                
                            except Exception as db_error:
                                logger.error(f"[❌] Ошибка создания записи AdPlanItem для кампании {campaign_id}: {db_error}")
                            
                            # Проверяем параметр активации из ячейки B
                            if active == '1':
                                try:
                                    logger.info(f"[🔛] Активируем кампанию {campaign_id} (параметр B=1)")
                                    activate_response = activate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    logger.info(f"[✅] Кампания {campaign_id} активирована успешно")
                                    
                                    # Обновляем данные кампании из ответа API
                                    _update_campaign_from_ozon_response(ad_plan_item, activate_response)
                                    logger.info(f"[📝] Данные кампании {campaign_id} обновлены из ответа Ozon API")
                                    
                                except Exception as activate_error:
                                    logger.error(f"[❌] Ошибка активации кампании {campaign_id}: {activate_error}")
                            else:
                                logger.debug(f"[ℹ️] Кампания {campaign_id} не активируется (параметр B='{active}')")
                            
                            campaigns_created += 1
                        except Exception as update_error:
                            logger.error(f"[❌] Кампания создана (ID: {resp.get('campaign_id')}), но не удалось обновить ячейку A{ad_data['row_number']}: {update_error}")
                            campaigns_created += 1  # Кампания все равно создана
                    elif resp:
                        # Ответ есть, но нет campaign_id
                        logger.warning(f"[⚠️] Получен ответ для SKU {sku}, но нет campaign_id: {resp}")
                        campaigns_skipped += 1
                    else:
                        logger.error(f"[❌] Не удалось создать кампанию для SKU {sku}")
                        campaigns_skipped += 1
                        
                except Exception as e:
                    logger.error(f"[❌] Ошибка при создании кампании для строки {ad_data['row_number']}: {e}")
                    campaigns_skipped += 1
            else:
                # campaign_id не пустое - проверяем существующие кампании
                try:
                    # Проверяем сначала автоматические кампании (это целевая область функции)
                    auto_campaign = AdPlanItem.objects.filter(
                        store=store,
                        ozon_campaign_id=campaign_id
                    ).first()
                    offer_id = str(ad_data.get('article') or '').strip()
                    
                    if auto_campaign:
                        # Проверяем дату создания и время обучения
                        from django.utils import timezone
                        manual_week_budget = ad_data['manual_week_budget']

                        
                        campaign_age_days = (timezone.now() - auto_campaign.created_at).days
                        logger.debug(f"[📅] Кампания {campaign_id} создана {campaign_age_days} дней назад, время обучения: {train_days} дней")
                        

                        pending_updates: list[str] = []
                        if offer_id and auto_campaign.offer_id != offer_id:
                            auto_campaign.offer_id = offer_id
                            pending_updates.append('offer_id')
                        mandatory_flag = bool(offer_id and offer_id in mandatory_offer_ids_set)
                        if mandatory_flag and not auto_campaign.is_mandatory:
                            auto_campaign.is_mandatory = True
                            pending_updates.append('is_mandatory')
                        if auto_campaign.google_sheet_row != ad_data['row_number']:
                            auto_campaign.google_sheet_row = ad_data['row_number']
                            pending_updates.append('google_sheet_row')

                        saved_after_budget = False

                        try:
                            # Обрабатываем ручной бюджет
                            try:
                                manual_budget_str = str(manual_week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.') if manual_week_budget else '0'
                                manual_budget_float = float(manual_budget_str) if manual_budget_str else 0.0
                            except (ValueError, TypeError):
                                manual_budget_float = 0.0
                                logger.debug(f"[ℹ️] Строка {ad_data['row_number']}: некорректный ручной бюджет '{manual_week_budget}', устанавливаем 0")
                            
                            # Выбираем бюджет к созданию: если указан ручной бюджет > 0, используем его, иначе расчётный
                            week_budget = ad_data['week_budget']
                            week_budget_str = str(week_budget).strip().replace(' ', '').replace('\xa0', '').replace('\u00A0', '').replace('\u202f', '').replace('\u202F', '').replace(',', '.') if week_budget else '0'
                            week_budget_float = float(week_budget_str) if week_budget_str else 0.0
                            used_week_budget = manual_budget_float if manual_budget_float and manual_budget_float > 0 else week_budget_float
                            
                            if used_week_budget > 0:
                                logger.info(f"[🔄] Обновляем бюджет кампании {campaign_id}: {auto_campaign.week_budget} -> {used_week_budget}")
                                
                                # Обновляем бюджет через API Ozon
                                try:
                                    api_response = update_campaign_budget(
                                        access_token=access_token,
                                        campaign_id=campaign_id,
                                        weekly_budget_rub=used_week_budget
                                    )
                                    logger.info(f"[🌐] API Ozon: бюджет кампании {campaign_id} обновлен успешно")
                                    
                                    # Обновляем в базе данных только после успешного API вызова
                                    update_fields: list[str] = list(pending_updates)
                                    if auto_campaign.week_budget != week_budget_float:
                                        auto_campaign.week_budget = week_budget_float
                                        update_fields.append('week_budget')
                                    day_budget_value = round(used_week_budget / 7)
                                    if auto_campaign.day_budget != day_budget_value:
                                        auto_campaign.day_budget = day_budget_value
                                        update_fields.append('day_budget')
                                    if auto_campaign.manual_budget != manual_budget_float:
                                        auto_campaign.manual_budget = manual_budget_float
                                        update_fields.append('manual_budget')

                                    if update_fields:
                                        unique_updates = list(dict.fromkeys(update_fields))
                                        auto_campaign.save(update_fields=unique_updates)
                                        saved_after_budget = True
                                    
                                    logger.info(f"[✅] Бюджет кампании {campaign_id} обновлен в базе данных")
                                    campaigns_updated += 1  # Считаем как обновленную кампанию
                                    
                                except Exception as api_error:
                                    logger.error(f"[❌] Ошибка API при обновлении бюджета кампании {campaign_id}: {api_error}")
                                    # Не обновляем базу данных при ошибке API
                                    campaigns_skipped += 1
                            else:
                                logger.warning(f"[⚠️] Строка {ad_data['row_number']}: некорректный бюджет для обновления: {week_budget}")
                                campaigns_skipped += 1
                        except Exception as update_error:
                            logger.error(f"[❌] Ошибка при обновлении бюджета кампании {campaign_id}: {update_error}")
                            campaigns_skipped += 1

                        if pending_updates and not saved_after_budget:
                            unique_updates = list(dict.fromkeys(pending_updates))
                            auto_campaign.save(update_fields=unique_updates)

                        # Гарантируем, что в колонке E указан тип 'Авто' для существующей авто-кампании
                        try:
                            ws.update(f"E{ad_data['row_number']}", [["Авто"]])
                            logger.debug(f"[📝] Обновлён тип кампании 'Авто' в E{ad_data['row_number']}")
                        except Exception as e_type2:
                            logger.warning(f"[⚠️] Не удалось обновить тип 'Авто' в E{ad_data['row_number']}: {e_type2}")

                        # 3. Активность по ячейке B: 0 — деактивировать, 1 — активировать
                        try:
                            desired = _is_sheet_active(ad_data.get('active'))
                            if desired is not None:
                                if desired:
                                    logger.info(f"[🔛] Активируем существующую кампанию {campaign_id} (B=1)")
                                    api_resp = activate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    # Обновляем модель по ответу; если state не пришёл — проставим ACTIVE
                                    if isinstance(api_resp, dict) and api_resp:
                                        _update_campaign_from_ozon_response(auto_campaign, api_resp)
                                    if not (isinstance(api_resp, dict) and api_resp.get('state')):
                                        auto_campaign.state = AdPlanItem.CAMPAIGN_STATE_ACTIVE
                                        auto_campaign.save(update_fields=['state'])
                                    campaigns_updated += 1
                                else:
                                    logger.info(f"[🔴] Деактивируем существующую кампанию {campaign_id} (B=0)")
                                    api_resp = deactivate_campaign(access_token=access_token, campaign_id=campaign_id)
                                    if isinstance(api_resp, dict) and api_resp:
                                        _update_campaign_from_ozon_response(auto_campaign, api_resp)
                                    if not (isinstance(api_resp, dict) and api_resp.get('state')):
                                        auto_campaign.state = AdPlanItem.CAMPAIGN_STATE_INACTIVE
                                        auto_campaign.save(update_fields=['state'])
                                    campaigns_updated += 1
                        except Exception as act_err:
                            logger.error(f"[❌] Ошибка смены активности кампании {campaign_id} по B: {act_err}")

                    else:
                        # Если авто не нашли — проверяем, не ручная ли это кампания
                        manual_campaign = ManualCampaign.objects.filter(
                            store=store,
                            ozon_campaign_id=campaign_id
                        ).first()
                        if manual_campaign:
                            logger.debug(f"[⏭️] Строка {ad_data['row_number']}: найдено соответствие ManualCampaign (ID: {campaign_id}), пропускаем управление по B")
                            campaigns_skipped += 1
                        else:
                            # Кампания не найдена ни в ручных, ни в автоматических
                            logger.warning(f"[⚠️] Кампания {campaign_id} не найдена в базе данных (строка {ad_data['row_number']})")
                            campaigns_skipped += 1
                        
                except Exception as e:
                    logger.error(f"[❌] Ошибка при обработке существующей кампании {campaign_id} (строка {ad_data['row_number']}): {e}")
                    campaigns_skipped += 1
        
        # Останавливаем в Ozon те авто-кампании из модели, которых нет в новом списке таблицы
        try:
            # Раньше тут был фильтр по campaign_type == 'Авто', из-за пустых значений
            # в колонке E живые кампании ошибочно считались «отсутствующими в листе» и выключались.
            # Теперь берём любой непустой campaign_id из листа, а отбор «только авто» обеспечивается тем,
            # что ниже мы итерируемся только по AdPlanItem (авто) в базе.
            present_auto_ids = {
                str(row.get('campaign_id')).strip()
                for row in data_rows
                if str(row.get('campaign_id')).strip()
            }
            stopped_count = 0
            active_states = [
                AdPlanItem.CAMPAIGN_STATE_RUNNING,
                AdPlanItem.CAMPAIGN_STATE_ACTIVE,
                AdPlanItem.CAMPAIGN_STATE_PLANNED,
            ]
            # Берём все авто-кампании магазина с ID
            stale_ads = AdPlanItem.objects.filter(store=store).exclude(ozon_campaign_id='')
            for ad in stale_ads:
                cid = str(ad.ozon_campaign_id)
                if cid not in present_auto_ids:
                    try:
                        # Деактивируем через Performance API
                        deact_resp = deactivate_campaign(access_token=access_token, campaign_id=cid)
                        _update_campaign_from_ozon_response(ad, deact_resp)
                        ad.save(update_fields=['state', 'payment_type', 'total_budget', 'week_budget', 'day_budget', 'from_date', 'to_date', 'placement', 'product_autopilot_strategy', 'ozon_created_at', 'ozon_updated_at'])
                        stopped_count += 1
                        logger.info(f"[🛑] Отключили кампанию {cid}, отсутствует в листе")
                    except Exception as e:
                        logger.error(f"[❌] Ошибка деактивации кампании {cid}: {e}")
            if stopped_count:
                logger.info(f"[📉] Остановлено кампаний, отсутствующих в листе: {stopped_count}")
        except Exception as e:
            logger.error(f"[❌] Ошибка при остановке кампаний, отсутствующих в листе: {e}")

        logger.info(f"[📊] Обработка завершена: создано {campaigns_created} кампаний, обновлено {campaigns_updated} кампаний, пропущено {campaigns_skipped}")
        return data_rows
        
    except Exception as e:
        logger.error(f"[❌] Ошибка при чтении данных из Google Sheets: {e}")
        return []

# =============================
# sync_campaign_activity_with_sheets
# =============================
# Запускается в периодик таске раз в час 
# 1. Проверяет ячейку B -включена компнаия или нет
# проверяет в БД не изменилось ли значение, и в лучае изменения останавливает или запускает компанию в Озоне

# 2. Проверяет ячейку K -Бюджет на нед, РУЧНОЙ
# Если задан ручной бюджет, то проверяет в моделе был ли задан ручной бюджет ранее и если есть изменения, 
# то обновляет неделеный бюджет у РК в озоне
@shared_task(name="Синхронизация активности кампаний с Google Sheets")
def sync_campaign_activity_with_sheets(
        spreadsheet_url: str = None,
        sa_json_path: str = None,
        worksheet_name: str = "Main_ADV",
        start_row: int = 13,
        block_size: int = 100,
    ):
    """
    Сканирует Google Sheets для проверки активности кампаний (колонка B).
    Сверяет с базой данных и синхронизирует состояние кампаний в Ozon.
    

    Args:
        spreadsheet_url: URL Google таблицы
        sa_json_path: Путь к JSON файлу сервисного аккаунта
        worksheet_name: Имя листа в таблице
        start_row: Начальная строка для чтения данных
        block_size: Размер блока для чтения (оптимизация)
    
    Returns:
        dict: Статистика синхронизации
    """
    logger.info(f"[🔄] Начало синхронизации активности кампаний с Google Sheets")

    sa_json_path = sa_json_path or os.getenv(
        "GOOGLE_SA_JSON_PATH",
        "/workspace/ozon-469708-c5f1eca77c02.json",
    )

    store_hint = None
    if not spreadsheet_url:
        stores = list(
            OzonStore.objects.exclude(google_sheet_url__isnull=True).exclude(google_sheet_url='')
        )
        if not stores:
            logger.info("[ℹ️] Нет магазинов с заполненным google_sheet_url — синхронизация пропущена")
            return {"processed": 0, "results": [], "skipped": [{"reason": "no stores"}]}

        if len(stores) > 1:
            results: list[dict] = []
            errors: list[dict] = []
            skipped: list[dict] = []
            for store in stores:
                sheet_url = (store.google_sheet_url or '').strip()
                if not sheet_url:
                    skipped.append({"store_id": store.id, "reason": "no google_sheet_url"})
                    continue
                res = sync_campaign_activity_with_sheets(
                    spreadsheet_url=sheet_url,
                    sa_json_path=sa_json_path,
                    worksheet_name=worksheet_name,
                    start_row=start_row,
                    block_size=block_size,
                )
                entry = {"store_id": store.id}
                if isinstance(res, dict):
                    entry.update(res)
                if entry.get("error"):
                    errors.append(entry)
                elif entry.get("skipped"):
                    skipped.append(entry)
                else:
                    results.append(entry)

            summary = {"processed": len(results), "results": results}
            if skipped:
                summary["skipped"] = skipped
            if errors:
                summary["errors"] = errors
            return summary

        store_hint = stores[0]
        spreadsheet_url = (store_hint.google_sheet_url or '').strip()
        if not spreadsheet_url:
            logger.warning(f"[⚠️] У магазина {store_hint} отсутствует google_sheet_url")
            return {"skipped": True, "reason": "no google_sheet_url", "store_id": store_hint.id}
    else:
        store_hint = OzonStore.objects.filter(google_sheet_url=spreadsheet_url).first()

    if not spreadsheet_url:
        logger.error("[❌] Не указан spreadsheet_url для синхронизации")
        return {"error": "spreadsheet_url not provided"}

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scope)
        client = gspread.authorize(creds)
        
        # Открываем таблицу и лист
        spreadsheet = client.open_by_url(spreadsheet_url)
        ws = spreadsheet.worksheet(worksheet_name)
        
        # Получаем название магазина из ячейки T23
        try:
            store_name = ws.acell('V23').value
            if not store_name or store_name.strip() == "":
                logger.error(f"[❌] Ячейка V23 пустая - не указан магазин")
                return {"error": "Не указан магазин в V23"}
            
            store = store_hint or OzonStore.objects.filter(google_sheet_url=spreadsheet_url).first()
            if not store:
                store = (
                    OzonStore.objects.filter(name__iexact=store_name.strip()).first()
                    or OzonStore.objects.filter(client_id__iexact=store_name.strip()).first()
                )
            if not store:
                logger.error(f"[❌] Магазин '{store_name}' не найден в базе данных")
                return {"error": f"Магазин '{store_name}' не найден"}
                
        except Exception as e:
            logger.error(f"[❌] Ошибка при получении настроек из Google Sheets: {e}")
            return {"error": f"Ошибка получения настроек: {e}"}

        def _sanitize_number_string(raw_value: str | None) -> str:
            if raw_value is None:
                return ''
            s = str(raw_value).strip()
            if not s:
                return ''
            return s.replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')

        def _parse_int_cell(raw_value: str | None) -> int | None:
            s = _sanitize_number_string(raw_value)
            if not s:
                return None
            try:
                return int(Decimal(s))
            except Exception:
                try:
                    return int(float(s))
                except Exception:
                    return None

        def _parse_decimal_cell(raw_value: str | None) -> Decimal | None:
            s = _sanitize_number_string(raw_value)
            if not s:
                return None
            try:
                return Decimal(s)
            except Exception:
                try:
                    return Decimal(str(float(s)))
                except Exception:
                    return None

        def _normalize_campaign_id(raw_value: str | None) -> str:
            """
            Приводит значение из таблицы к строке campaign_id без апострофов и служебных символов.
            """
            if raw_value is None:
                return ''
            s = str(raw_value).strip()
            if not s:
                return ''
            # Удаляем служебный префикс, которым Google Sheets помечает текстовые числа
            s = s.lstrip("'").lstrip("’").lstrip("`")
            # Убираем пробелы и неразрывные пробелы внутри
            s = s.replace('\u00A0', '').replace('\u202F', '').replace(' ', '')
            # Если значение пришло как 17456100.0 — убираем дробную часть с нулями
            if s.endswith('.0'):
                s = s[:-2]
            return s

        # Минимальные остатки из настроек
        min_fbs_stock = _parse_int_cell(ws.acell('V26').value)
        min_fbo_stock = _parse_int_cell(ws.acell('V27').value)

        # Кэш остаточных данных по SKU
        fbs_stock_by_sku = {
            row['sku']: int(row['total'] or 0)
            for row in FbsStock.objects.filter(store=store)
                .values('sku')
                .annotate(total=Sum('present'))
        }
        fbo_stock_by_sku = {
            row['sku']: int(row['total'] or 0)
            for row in WarehouseStock.objects.filter(store=store)
                .values('sku')
                .annotate(total=Sum('available_stock_count'))
        }
        logger.info(f"[ℹ️] Кэш остатков: FBS={len(fbs_stock_by_sku)}, FBO={len(fbo_stock_by_sku)}")

        grey_fill = CellFormat(backgroundColor=Color(0.85, 0.85, 0.85))
        white_fill = CellFormat(backgroundColor=Color(1, 1, 1))

        def _parse_sku(value) -> int | None:
            if value is None:
                return None
            s = str(value).strip()
            if not s:
                return None
            s = s.replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')
            try:
                return int(Decimal(s))
            except Exception:
                try:
                    return int(float(s))
                except Exception:
                    return None

#----------- Проверяем глобальный флаг запуска/остановки рекламы для магазина
        try:
            from .models import StoreAdControl
            control = StoreAdControl.objects.filter(store=store).first()
            if control and not control.is_system_enabled:
                logger.info(f"[⛔] Рекламная система для магазина {store} выключена. Выходим из sync_campaign_activity_with_sheets.")
                return {"skipped": True, "reason": "store_ads_disabled"}
        except Exception as ctrl_err:
            logger.warning(f"[⚠️] Не удалось проверить статус StoreAdControl для {store}: {ctrl_err}")

        # Получаем токен один раз для всех операций
        try:
            from .utils import get_store_performance_token
            token_info = get_store_performance_token(store)
            access_token = token_info.get("access_token")
            if not access_token:
                logger.error(f"[❌] Не удалось получить токен для магазина {store.name}")
                return {"error": f"Ошибка получения токена для {store.name}"}
        except Exception as e:
            logger.error(f"[❌] Ошибка при получении токена: {e}")
            return {"error": f"Ошибка токена: {e}"}
        
        # Читаем данные из Google Sheets по блокам
        current_row = start_row
        empty_rows_count = 0
        max_empty_rows = 5
        
        campaigns_activated = 0
        campaigns_deactivated = 0
        campaigns_synced = 0
        campaigns_skipped = 0
        budgets_updated = 0
        
        logger.info(f"[📖] Начинаем чтение данных с строки {start_row} блоками по {block_size}")
        
        while empty_rows_count < max_empty_rows:
            end_row = current_row + block_size - 1
            
            # Читаем блок данных: A (campaign_id), B (active), C (sku), K (manual weekly budget)
            try:
                range_name = f'A{current_row}:L{end_row}'
                values = ws.get(range_name)
                format_requests = []
                value_requests = []

                if not values:
                    empty_rows_count += block_size
                    current_row += block_size
                    continue
                
                # Проверяем, есть ли данные в блоке
                has_data = False
                for row in values:
                    if len(row) > 0 and any(str(cell).strip() for cell in row):
                        has_data = True
                        break
                
                if not has_data:
                    empty_rows_count += block_size
                    current_row += block_size
                    logger.debug(f"[⭕] Блок {current_row}-{end_row}: полностью пустой")
                    continue
                
                # Обрабатываем каждую строку в блоке
                for i, row in enumerate(values):
                    row_number = current_row + i

                    # Проверяем, есть ли данные в строке
                    if len(row) == 0 or not any(str(cell).strip() for cell in row):
                        empty_rows_count += 1
                        if empty_rows_count >= max_empty_rows:
                            break
                        continue
                    else:
                        empty_rows_count = 0  # Сбрасываем счетчик пустых строк

                    # Извлекаем данные из строки
                    campaign_id = str(row[0]).strip() if len(row) > 0 else ""
                    active_value = str(row[1]).strip() if len(row) > 1 else ""
                    week_budget_value = str(row[9]).strip() if len(row) > 9 else ""      # Колонка J (индекс 9)
                    manual_budget_value = str(row[10]).strip() if len(row) > 10 else ""  # Колонка K (индекс 10)
                    sheet_day_budget_value = str(row[11]).strip() if len(row) > 11 else ""
                    
                    # Пропускаем строки без campaign_id
                    if not campaign_id:
                        continue
                    
                    # Определяем желаемое состояние активности
                    should_be_active = active_value == "1"
                    
                    logger.debug(f"[🔍] Строка {row_number}: campaign_id={campaign_id}, active={active_value},  manual_budget={manual_budget_value}")
                    
                    # Ищем кампанию в автоматических кампаниях
                    ad_plan_item = AdPlanItem.objects.filter(
                        store=store,
                        ozon_campaign_id=campaign_id
                    ).first()
                    
                    if not ad_plan_item:
                        logger.debug(f"[⏭️] Кампания {campaign_id} не найдена в автоматических кампаниях (строка {row_number})")
                        campaigns_skipped += 1
                        continue

                    if ad_plan_item.is_active_in_sheets != should_be_active:
                        ad_plan_item.is_active_in_sheets = should_be_active
                        try:
                            ad_plan_item.save(update_fields=['is_active_in_sheets'])
                        except Exception as save_err:
                            logger.warning(f"[⚠️] Не удалось обновить is_active_in_sheets для кампании {campaign_id}: {save_err}")

                    sku_cell = row[6] if len(row) > 6 else None
                    sku_int = _parse_sku(sku_cell)
                    fbs_value = fbs_stock_by_sku.get(sku_int, 0) if sku_int is not None else ''
                    fbo_value = fbo_stock_by_sku.get(sku_int, 0) if sku_int is not None else ''

                    value_requests.append({
                        'range': f'H{row_number}:I{row_number}',
                        'values': [[fbs_value, fbo_value]]
                    })

                    h_cell = f'H{row_number}'
                    i_cell = f'I{row_number}'

                    fbs_below_min = isinstance(fbs_value, int) and min_fbs_stock is not None and fbs_value < min_fbs_stock
                    fbo_below_min = isinstance(fbo_value, int) and min_fbo_stock is not None and fbo_value < min_fbo_stock

                    # Подсветка ячеек остатков: серый цвет, если значение ниже нормы, иначе белый
                    if isinstance(fbs_value, int):
                        format_requests.append((h_cell, grey_fill if fbs_below_min else white_fill))
                    else:
                        format_requests.append((h_cell, white_fill))

                    if isinstance(fbo_value, int):
                        format_requests.append((i_cell, grey_fill if fbo_below_min else white_fill))
                    else:
                        format_requests.append((i_cell, white_fill))

                    is_low_stock = fbs_below_min or fbo_below_min
                    if is_low_stock:
                        #  Остатки ниже нормы → ставим флаг и при необходимости отключаем кампанию в Ozon, оставляя настройки в таблице
                        if not ad_plan_item.paused_due_to_low_stock:
                            ad_plan_item.paused_due_to_low_stock = True
                            try:
                                ad_plan_item.save(update_fields=['paused_due_to_low_stock'])
                            except Exception as save_err:
                                logger.warning(f"[⚠️] Не удалось сохранить флаг низкого остатка для кампании {campaign_id}: {save_err}")
                        if ad_plan_item.is_active:
                            try:
                                logger.info(f"[🛑] Деактивация кампании {campaign_id} из-за низких остатков (FBS={fbs_value}, FBO={fbo_value})")
                                deactivate_response = deactivate_campaign(access_token=access_token, campaign_id=campaign_id)
                                _update_campaign_from_ozon_response(ad_plan_item, deactivate_response)
                                try:
                                    ws.update(f'C{row_number}', [["Неактивна"]])
                                except Exception as ws_err:
                                    logger.warning(f"[⚠️] Не удалось обновить статус в C{row_number} после деактивации по остаткам: {ws_err}")
                                campaigns_deactivated += 1
                                campaigns_synced += 1
                            except Exception as stock_deactivate_err:
                                logger.error(f"[❌] Ошибка деактивации кампании {campaign_id} по остаткам: {stock_deactivate_err}")
                        # Не активируем кампанию дальше в этом цикле
                    else:
                        # Остатки восстановились → снимаем флаг, далее обычная логика активирует кампанию, если в Sheets стоит "1"
                        if ad_plan_item.paused_due_to_low_stock:
                            ad_plan_item.paused_due_to_low_stock = False
                            try:
                                ad_plan_item.save(update_fields=['paused_due_to_low_stock'])
                            except Exception as save_err:
                                logger.warning(f"[⚠️] Не удалось сбросить флаг низкого остатка для кампании {campaign_id}: {save_err}")

                    # Значение недельного бюджета из колонки J (если заполнено)
                    week_budget_decimal = _parse_decimal_cell(week_budget_value)
                    week_budget_float = float(week_budget_decimal) if week_budget_decimal is not None else None
                    week_budget_synced = False

                    # Проверяем изменения в ручном бюджете (колонка K)
                    if manual_budget_value:
                        try:
                            mb_str = _sanitize_number_string(manual_budget_value)
                            manual_budget_float = float(mb_str) if mb_str else 0.0
                            manual_budget_decimal = Decimal(str(manual_budget_float))
                            manual_day_budget = (manual_budget_decimal / Decimal('7')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                            manual_day_int = int(manual_day_budget)

                            sheet_day_decimal = _parse_decimal_cell(sheet_day_budget_value)
                            if sheet_day_decimal is None or sheet_day_decimal.quantize(Decimal('1'), rounding=ROUND_HALF_UP) != manual_day_budget:
                                value_requests.append({
                                    'range': f'L{row_number}:L{row_number}',
                                    'values': [[manual_day_int]]
                                })

                            current_manual_budget = float(ad_plan_item.manual_budget or 0)
                            start_dt = ad_plan_item.ozon_created_at or ad_plan_item.created_at
                            age_days = (timezone.now().date() - start_dt.date()).days if start_dt else 0
                            train_days = int(ad_plan_item.train_days or 0)

                            if abs(manual_budget_float - current_manual_budget) > 0.01:  # Учитываем погрешность float
                                logger.info(f"[💰] Кампания {campaign_id}: обновление ручного недельного бюджета {current_manual_budget} -> {manual_budget_float}")

                                try:
                                    budget_response = update_campaign_budget(
                                        access_token=access_token,
                                        campaign_id=campaign_id,
                                        weekly_budget_rub=manual_budget_float
                                    )
                                    logger.info(f"[🌐] API Ozon: бюджет кампании {campaign_id} обновлен успешно")

                                    _update_campaign_from_ozon_response(ad_plan_item, budget_response)

                                    ad_plan_item.manual_budget = manual_budget_decimal
                                    ad_plan_item.week_budget = manual_budget_decimal
                                    ad_plan_item.day_budget = manual_day_int
                                    ad_plan_item.save(update_fields=['manual_budget', 'week_budget', 'day_budget'])

                                    budget_updated = True
                                    budgets_updated += 1
                                    logger.info(f"[✅] Бюджет кампании {campaign_id} обновлен в базе данных")

                                except Exception as budget_error:
                                    logger.error(f"[❌] Ошибка обновления бюджета кампании {campaign_id}: {budget_error}")

                        except (ValueError, TypeError) as parse_error:
                            logger.warning(f"[⚠️] Некорректное значение ручного бюджета '{manual_budget_value}' для кампании {campaign_id}: {parse_error}")
                    else:
                        # Поле ручного бюджета очищено в таблице → возвращаемся на автоматический бюджет из модели
                        prev_manual = float(ad_plan_item.manual_budget or 0)
                        if prev_manual > 0:
                            try:
                                target_week_budget = week_budget_float if week_budget_float is not None else float(ad_plan_item.week_budget or 0)
                                budget_source = "колонка J" if week_budget_float is not None else "модель"
                                logger.info(f"[↩️] Кампания {campaign_id}: ручной бюджет очищен в Sheets; устанавливаем недельный бюджет = {target_week_budget} (источник: {budget_source})")
                                if target_week_budget > 0:
                                    response = update_campaign_budget(
                                        access_token=access_token,
                                        campaign_id=campaign_id,
                                        weekly_budget_rub=target_week_budget
                                    )
                                    logger.info(f"[🌐] API Ozon: недельный бюджет кампании {campaign_id} возвращен успешно")
                                    _update_campaign_from_ozon_response(ad_plan_item, response)
                                    week_budget_synced = week_budget_float is not None and abs(target_week_budget - week_budget_float) < 0.01
                                # Сбрасываем ручной бюджет в БД и пересчитываем дневной по текущему недельному
                                ad_plan_item.manual_budget = 0
                                try:
                                    # Если week_budget хранится Decimal — безопасно делим
                                    ad_plan_item.day_budget = (ad_plan_item.week_budget or 0) / 7
                                except Exception:
                                    ad_plan_item.day_budget = target_week_budget / 7 if target_week_budget else 0
                                ad_plan_item.save(update_fields=['manual_budget', 'day_budget'])
                                budgets_updated += 1
                            except Exception as e:
                                logger.error(f"[❌] Ошибка возврата на автоматический бюджет для кампании {campaign_id}: {e}")

                    # Если ручной бюджет не используется и значение в колонке J изменилось — обновляем недельный бюджет
                    if not manual_budget_value and week_budget_decimal is not None and week_budget_float is not None and not week_budget_synced:
                        try:
                            current_week_budget = float(ad_plan_item.week_budget or 0)
                            if abs(week_budget_float - current_week_budget) > 0.01:
                                if week_budget_float <= 0:
                                    logger.warning(f"[⚠️] Кампания {campaign_id}: новое значение недельного бюджета <= 0 ({week_budget_float}) — обновление пропущено")
                                else:
                                    logger.info(f"[💵] Кампания {campaign_id}: обновление недельного бюджета {current_week_budget} -> {week_budget_float} по колонке J")
                                    budget_response = update_campaign_budget(
                                        access_token=access_token,
                                        campaign_id=campaign_id,
                                        weekly_budget_rub=week_budget_float
                                    )
                                    logger.info(f"[🌐] API Ozon: недельный бюджет кампании {campaign_id} обновлен успешно по колонке J")
                                    _update_campaign_from_ozon_response(ad_plan_item, budget_response)
                                    budgets_updated += 1
                                    week_budget_synced = True
                        except Exception as budget_error:
                            logger.error(f"[❌] Ошибка обновления недельного бюджета кампании {campaign_id} по колонке J: {budget_error}")
                    
#-------------------Прверяем, нужно ли отключить или заново вклчить компанию в ОЗОНЕ    
                
                    # При низких остатках пропускаем попытку активации — вернёмся сюда, когда значения вновь превысят пороги
                    if fbs_below_min or fbo_below_min:
                        logger.debug(f"[⏸️] Кампания {campaign_id} пропущена для активации: низкие остатки")
                        continue

                    # Проверяем текущее состояние активности в базе
                    current_is_active = ad_plan_item.is_active
                    current_sheets_active = ad_plan_item.is_active_in_sheets
                    
                    # Проверяем, нужна ли синхронизация
                    needs_sync = False
                    action = None
                    
                    # Если статус в Google Sheets изменился
                    if current_sheets_active != should_be_active:
                        needs_sync = True
                        action = "activate" if should_be_active else "deactivate"
                        logger.info(f"[🔄] Кампания {campaign_id}: изменение активности в Sheets {current_sheets_active} -> {should_be_active}")
                    
                    # Если статус в Ozon не соответствует желаемому
                    elif current_is_active != should_be_active:
                        needs_sync = True
                        action = "activate" if should_be_active else "deactivate"
                        logger.info(f"[🔄] Кампания {campaign_id}: несоответствие статуса Ozon {current_is_active} vs Sheets {should_be_active}")
                    
                    if needs_sync:
                        try:
                            if action == "activate":
                                # Проверяем, не была ли кампания остановлена по бюджету
                                if ad_plan_item.state == AdPlanItem.CAMPAIGN_STATE_STOPPED:
                                    logger.info(f"[💰] Кампания {campaign_id}: остановлена по бюджету (CAMPAIGN_STATE_STOPPED). Пропускаем активацию.")
                                    campaigns_skipped += 1
                                    continue
                                
                                logger.info(f"[🔛] Активация кампании {campaign_id}")
                                activate_response = activate_campaign(access_token=access_token, campaign_id=campaign_id)
                                
                                # Обновляем данные кампании из ответа API
                                _update_campaign_from_ozon_response(ad_plan_item, activate_response)
                                
                                # Обновляем статус в колонке C для этой строки
                                try:
                                    ws.update(f'C{row_number}', [["Активна"]])
                                except Exception as ws_err:
                                    logger.warning(f"[⚠️] Не удалось обновить статус в C{row_number}: {ws_err}")
                                
                                campaigns_activated += 1
                                logger.info(f"[✅] Кампания {campaign_id} активирована успешно")
                                
                            elif action == "deactivate":
                                logger.info(f"[🔴] Деактивация кампании {campaign_id}")
                                deactivate_response = deactivate_campaign(access_token=access_token, campaign_id=campaign_id)
                                
                                # Обновляем данные кампании из ответа API
                                _update_campaign_from_ozon_response(ad_plan_item, deactivate_response)
                                
                                # Обновляем статус в колонке C для этой строки
                                try:
                                    ws.update(f'C{row_number}', [["Неактивна"]])
                                except Exception as ws_err:
                                    logger.warning(f"[⚠️] Не удалось обновить статус в C{row_number}: {ws_err}")
                                
                                campaigns_deactivated += 1
                                logger.info(f"[✅] Кампания {campaign_id} деактивирована успешно")
                            
                            campaigns_synced += 1
                            
                        except Exception as sync_error:
                            logger.error(f"[❌] Ошибка синхронизации кампании {campaign_id}: {sync_error}")
                            campaigns_skipped += 1
                    else:
                        logger.debug(f"[✅] Кампания {campaign_id}: синхронизация не требуется")

                if value_requests:
                    try:
                        ws.batch_update(value_requests)
                        if format_requests:
                            format_cell_ranges(ws, format_requests)
                    except Exception as stock_err:
                        logger.warning(f"[⚠️] Не удалось обновить остатки/формат для блока {current_row}-{end_row}: {stock_err}")

                current_row += block_size
                
            except Exception as block_error:
                logger.error(f"[❌] Ошибка при обработке блока {current_row}-{end_row}: {block_error}")
                current_row += block_size
                continue
        

        
        logger.info(f"[📊] Синхронизация завершена: активировано {campaigns_activated}, деактивировано {campaigns_deactivated}, синхронизировано {campaigns_synced}, бюджетов обновлено {budgets_updated}, пропущено {campaigns_skipped}")
        
        # Обновляем статусы в колонке C с учетом флага paused_due_to_low_stock
        try:
            logger.info(f"[📝] Обновляем статусы в колонке C с учетом флага низких остатков")
            
            # Локальная функция для перевода статусов автоматических кампаний
            def _translate_auto_campaign_status(status):
                """Переводит статус автоматической кампании на русский язык"""
                status_translations = {
                    'PREVIEW': 'Предпросмотр',
                    'ACTIVATED': 'Активирована',
                    'UNKNOWN': 'Неизвестно',
                    'CAMPAIGN_STATE_RUNNING': 'Запущена',
                    'CAMPAIGN_STATE_ACTIVE': 'Активна',
                    'CAMPAIGN_STATE_INACTIVE': 'Неактивна',
                    'CAMPAIGN_STATE_PLANNED': 'Запланирована',
                    'CAMPAIGN_STATE_STOPPED': 'Остановлена (превышен бюджет)',
                    'CAMPAIGN_STATE_ARCHIVED': 'Архивная',
                    'CAMPAIGN_STATE_FINISHED': 'Завершена',
                    'CAMPAIGN_STATE_PAUSED': 'Приостановлена',
                    'CAMPAIGN_STATE_ENDED': 'Завершена',
                    'CAMPAIGN_STATE_MODERATION_DRAFT': 'Черновик модерации',
                    'CAMPAIGN_STATE_MODERATION_IN_PROGRESS': 'На модерации',
                    'CAMPAIGN_STATE_MODERATION_FAILED': 'Не прошла модерацию',
                    'CAMPAIGN_STATE_UNKNOWN': 'Неизвестно',
                }
                return status_translations.get(status, 'Неизвестно')
            
            status_updates = []
            
            # Читаем колонку A один раз и строим карту campaign_id -> row_index
            try:
                col_a_values = ws.col_values(1)
            except Exception as col_err:
                logger.warning(f"[⚠️] Не удалось прочитать колонку A для обновления статусов: {col_err}")
                col_a_values = []

            campaign_row_map = {
                norm_id: idx
                for idx, value in enumerate(col_a_values, start=1)
                if (norm_id := _normalize_campaign_id(value))
            }

            # Читаем все кампании из базы данных для этого магазина
            ad_plan_items = AdPlanItem.objects.filter(store=store).exclude(ozon_campaign_id__isnull=True).exclude(ozon_campaign_id='')

            for ad_plan_item in ad_plan_items:
                if not ad_plan_item.ozon_campaign_id:
                    continue
                    
                # Определяем актуальный статус с учетом флага низких остатков
                if ad_plan_item.paused_due_to_low_stock:
                    actual_status = "Неактивна (низкие остатки)"
                else:
                    # Используем функцию перевода статуса
                    actual_status = _translate_auto_campaign_status(ad_plan_item.state)
                
                # Находим строку с этим campaign_id в таблице
                row_idx = campaign_row_map.get(_normalize_campaign_id(ad_plan_item.ozon_campaign_id))
                if row_idx:
                    status_updates.append({
                        'range': f'C{row_idx}',
                        'values': [[actual_status]]
                    })
                    logger.debug(f"[📝] Обновляем статус для кампании {ad_plan_item.ozon_campaign_id} в строке {row_idx}: {actual_status}")
                else:
                    logger.warning(f"[⚠️] Не удалось найти строку для кампании {ad_plan_item.ozon_campaign_id}: campaign_id отсутствует в колонке A")
            
            # Применяем все обновления статусов одним запросом
            if status_updates:
                ws.batch_update(status_updates)
                logger.info(f"[✅] Обновлено {len(status_updates)} статусов в колонке C")
            else:
                logger.info(f"[ℹ️] Нет статусов для обновления в колонке C")
                
        except Exception as status_update_err:
            logger.warning(f"[⚠️] Ошибка при обновлении статусов в колонке C: {status_update_err}")
        
        # Записываем дату и время выполнения в ячейку K4
        try:
            now = datetime.now()
            formatted_datetime = now.strftime("%d-%m-%Y %H:%M")
            ws.update('K4', [[formatted_datetime]])
            logger.info(f"[📅] Дата выполнения записана в K4: {formatted_datetime}")
        except Exception as date_error:
            logger.warning(f"[⚠️] Не удалось записать дату выполнения в K4: {date_error}")
        

        
    except Exception as e:
        logger.error(f"[❌] Ошибка при синхронизации активности кампаний: {e}")
        
        # Пытаемся записать дату ошибки в K4, если есть доступ к Google Sheets
        try:
            if 'ws' in locals():
                now = datetime.now()
                formatted_datetime = now.strftime("%d-%m-%Y %H:%M")
                ws.update('K4', [[f"ОШИБКА {formatted_datetime}"]])
                logger.info(f"[📅] Дата ошибки записана в K4: ОШИБКА {formatted_datetime}")
        except Exception as date_error:
            logger.warning(f"[⚠️] Не удалось записать дату ошибки в K4: {date_error}")
        
        return {"error": str(e)}
    
    


# =============================
# Performance API: отчёты статистики
# =============================
def _rfc3339(dt: datetime) -> str:
    # Всегда в UTC с суффиксом Z
    if dt.tzinfo is None:
        dt = timezone.make_aware(dt, timezone=timezone.utc) if hasattr(timezone, 'make_aware') else dt
    dt_utc = dt.astimezone(timezone.utc) if hasattr(timezone, 'utc') else dt
    return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')



#--------Перерасчёт: Срабатывает при нажатии кнопки Обновить РК-------------
@shared_task(name="Перерасчёт бюджета РК за период (с учётом потраченного)")
def reforecast_ad_budgets_for_period(spreadsheet_url: str = None, sa_json_path: str = None, worksheet_name: str = "Main_ADV"):
    try:

        update_abc_sheet(spreadsheet_url=spreadsheet_url, sa_json_path=sa_json_path, consider_spent=1)
        create_or_update_AD(spreadsheet_url=spreadsheet_url,sa_json_path=sa_json_path,worksheet_name=worksheet_name,start_row=13,block_size=100)

    except Exception as e:
        logger.error(f"[❌] reforecast_ad_budgets_for_period: {e}")
        return {"error": str(e)}
# -------------------------------------


#--------Кнопка Старт/Стоп ---------------
@shared_task(name="Кнопка Старт/Стоп")
def toggle_store_ads_status(
    store_id: int,
    spreadsheet_url: str = None,
    sa_json_path: str = None,
    worksheet_name: str = "Main_ADV",
    mode: str = "toggle",  # 'toggle' | 'on' | 'off'
):
    """
    Меняет флаг в модели StoreAdControl для указанного магазина на противоположный
    и обновляет ячейку S3 в Google Sheets текущим значением ("Включен"/"Выключен").
    Args:
        store_id: ID магазина (OzonStore.id)
        spreadsheet_url, sa_json_path, worksheet_name: параметры таблицы (опционально)

    Returns:
        dict: {"status": "on"|"off"}
    """
    try:
        # Получаем магазин
        store = OzonStore.objects.filter(id=store_id).first()
        if not store:
            return {"error": f"store id={store_id} not found"}

        # Текущее состояние и желаемое действие
        from .models import StoreAdControl
        ctrl, _ = StoreAdControl.objects.get_or_create(store=store)
        previous = bool(ctrl.is_system_enabled)
        if mode == "on":
            desired = True
        elif mode == "off":
            desired = False
        else:  # toggle
            desired = not previous

        # Если состояние не меняется — просто отразим его в ответе и таблице
        ctrl.is_system_enabled = desired
        ctrl.save(update_fields=["is_system_enabled", "updated_at"])
        logger.info(f"[🔀] StoreAdControl для {store}: previous={previous} -> desired={desired} (mode={mode})")

        # Обновляем S3 (только статус системы; остальные данные листа не трогаем)
        spreadsheet_url = spreadsheet_url or ""
        sa_json_path = sa_json_path or os.getenv(
            "GOOGLE_SA_JSON_PATH",
            "/workspace/ozon-469708-c5f1eca77c02.json",
        )
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(spreadsheet_url)
        ws = sh.worksheet(worksheet_name)

        try:
            ws.update('S3', [["Включен" if desired else "Выключен"]])
        except Exception as ws_err:
            logger.warning(f"[⚠️] Не удалось записать статус в S3: {ws_err}")

        # Логика: если выключили систему — деактивируем ВСЕ АВТОкампании в Ozon и фиксируем состояние в БД
        if not desired and previous != desired:
            try:
                from .utils import get_store_performance_token
                token_info = get_store_performance_token(store)
                access_token = token_info.get("access_token")
                if not access_token:
                    raise Exception("Не удалось получить access_token для магазина")

                # Собираем только автоматические кампании (AdPlanItem) по магазину
                from .models import AdPlanItem
                campaign_ids = set(
                    AdPlanItem.objects.filter(store=store)
                    .exclude(ozon_campaign_id__isnull=True)
                    .exclude(ozon_campaign_id='')
                    .values_list('ozon_campaign_id', flat=True)
                )

                # Деактивируем авто-кампании через Performance API (с ретраями)
                deactivated = 0
                failed_ids = []
                for cid in campaign_ids:
                    ok = False
                    for attempt in range(3):
                        try:
                            deactivate_campaign(access_token=access_token, campaign_id=str(cid))
                            # Обновляем модель: помечаем как остановлена и выключена в Sheets
                            from .models import AdPlanItem as _Ad
                            _Ad.objects.filter(store=store, ozon_campaign_id=str(cid)).update(
                                state=_Ad.CAMPAIGN_STATE_INACTIVE,
                            )
                            deactivated += 1
                            ok = True
                            break
                        except Exception as api_err:
                            logger.warning(f"[⚠️] Деактивация {cid} (попытка {attempt+1}/3) не удалась: {api_err}")
                            time.sleep(2)
                    if not ok:
                        failed_ids.append(str(cid))

                if failed_ids:
                    logger.error(f"[🔴] Выключение {store}: деактивировано={deactivated}, ошибок={len(failed_ids)}: {failed_ids}")
                else:
                    logger.info(f"[🔴] Система выключена для {store}. Деактивировано авто-кампаний: {deactivated}.")
            except Exception as off_err:
                logger.error(f"[❌] Ошибка при массовой деактивации кампаний для {store}: {off_err}")
        elif desired and previous != desired:
            # Если включили систему — полная актуализация из листа одним запуском
            try:
                logger.info(f"[▶️] Система включена для {store}. Запускаем create_or_update_AD для актуализации кампаний")
                create_or_update_AD(
                    spreadsheet_url=spreadsheet_url,
                    sa_json_path=sa_json_path,
                    worksheet_name=worksheet_name,
                    start_row=13,
                    block_size=100,
                )
            except Exception as on_err:
                logger.error(f"[❌] Ошибка запуска create_or_update_AD для {store}: {on_err}")

        # После переключения состояния — проставляем актуальные статусы кампаний в колонку C одним запросом
        try:
            from .models import AdPlanItem as _Ad, ManualCampaign as _MC

            def _translate_state(status: str) -> str:
                m = {
                    'PREVIEW': 'Предпросмотр',
                    'ACTIVATED': 'Активирована',
                    'CAMPAIGN_STATE_RUNNING': 'Запущена',
                    'CAMPAIGN_STATE_ACTIVE': 'Активна',
                    'CAMPAIGN_STATE_INACTIVE': 'Неактивна',
                    'CAMPAIGN_STATE_PLANNED': 'Запланирована',
                    'CAMPAIGN_STATE_STOPPED': 'Остановлена (превышен бюджет)',
                    'CAMPAIGN_STATE_ARCHIVED': 'Архивная',
                    'CAMPAIGN_STATE_FINISHED': 'Завершена',
                    'CAMPAIGN_STATE_PAUSED': 'Приостановлена',
                    'CAMPAIGN_STATE_ENDED': 'Завершена',
                    'CAMPAIGN_STATE_MODERATION_DRAFT': 'Черновик модерации',
                    'CAMPAIGN_STATE_MODERATION_IN_PROGRESS': 'На модерации',
                    'CAMPAIGN_STATE_MODERATION_FAILED': 'Не прошла модерацию',
                }
                return m.get((status or '').strip(), 'Неизвестно')

            start_row_c = 13
            logger.info(f"[📝] Обновляем статусы в колонке C одним запросом, начиная с {start_row_c}")
            # Читаем колонку A (campaign_id) и колонку C (текущие статусы)
            a_vals = ws.col_values(1)  # вся колонка A
            c_vals = ws.col_values(3)  # вся колонка C
            a_slice = a_vals[start_row_c - 1:]
            c_slice = c_vals[start_row_c - 1:] if len(c_vals) >= start_row_c - 1 else []
            n = max(len(a_slice), len(c_slice))
            # Собираем список уникальных ID для батч‑поиска в БД
            ids = set()
            for i in range(n):
                if i < len(a_slice):
                    cid = (a_slice[i] or '').strip()
                    if cid:
                        ids.add(cid)
            # Загружаем одним запросом состояния авто и ручных
            ads = {str(x.ozon_campaign_id): x.state for x in _Ad.objects.filter(store=store, ozon_campaign_id__in=list(ids))}
            mans = {str(x.ozon_campaign_id): x.state for x in _MC.objects.filter(store=store, ozon_campaign_id__in=list(ids))}
            # Формируем финальные значения C
            out_c = []
            changes = 0
            for i in range(n):
                cid = (a_slice[i] or '').strip() if i < len(a_slice) else ''
                current = (c_slice[i] or '').strip() if i < len(c_slice) else ''
                if cid and cid in ads:
                    target = _translate_state(ads[cid])
                elif cid and cid in mans:
                    target = _translate_state(mans[cid])
                else:
                    target = current
                if target != current:
                    changes += 1
                out_c.append([target])
            # Если нечего менять — выходим
            if n == 0:
                logger.info("[ℹ️] Нет строк для обновления статусов в колонке C")
            else:
                rng = f"C{start_row_c}:C{start_row_c + n - 1}"
                try:
                    ws.update(rng, out_c)
                    logger.info(f"[✅] Обновили статусы в {rng}. Изменено строк: {changes} из {n}")
                except Exception as write_err:
                    # Пробуем один бэкофф при 429
                    msg = str(write_err)
                    if '429' in msg or 'Quota exceeded' in msg:
                        backoff = 45
                        logger.warning(f"[⏳] 429 при обновлении {rng}. Ждём {backoff}s и повторяем…")
                        time.sleep(backoff)
                        ws.update(rng, out_c)
                        logger.info(f"[✅] Повторное обновление {rng} успешно после бэкоффа")
                    else:
                        raise
        except Exception as upd_err:
            logger.warning(f"[⚠️] Не удалось обновить статусы кампаний (колонка C) одним запросом: {upd_err}")

        return {
            "previous": "on" if previous else "off",
            "current": "on" if desired else "off",
            "mode": mode,
        }
    except Exception as e:
        logger.error(f"[❌] toggle_store_ads_status: {e}")
        return {"error": str(e)}
#-------------------------------------



#--------Performance: эксперимент — 10 дневных отчётов по одной кампании---------------

def _make_aware(dt: datetime) -> datetime:
    try:
        from django.utils import timezone as dj_tz
        if dt.tzinfo is None:
            return dj_tz.make_aware(dt, dj_tz.get_default_timezone())
        return dt
    except Exception:
        return dt


def _resolve_store_for_campaign(ozon_campaign_id: str, store_id: int | None = None):
    if store_id:
        return OzonStore.objects.filter(id=store_id).first()
    # Пытаемся найти по ManualCampaign затем по AdPlanItem
    mc = None
    try:
        mc = ManualCampaign.objects.filter(ozon_campaign_id=str(ozon_campaign_id)).select_related('store').first()
    except Exception:
        mc = None
    if mc and mc.store:
        return mc.store
    ap = AdPlanItem.objects.filter(ozon_campaign_id=str(ozon_campaign_id)).select_related('store').first()
    return ap.store if ap else None


#--------Performance: получить готовые отчёты — по UUID вытягивает результаты и помечает READY/ERROR---------------
#-------Запускаем раз в час желательно страт 00:30
@shared_task(name="Performance: получить готовые отчёты")
def fetch_performance_reports(max_reports: int = 50):
    """
    Идёт по CampaignPerformanceReport со статусом PENDING, забирает готовые отчёты
    по UUID и сохраняет totals/rows/raw_response, проставляет READY/ERROR.
    """
    from .models import CampaignPerformanceReport
    from .utils import get_store_performance_token

    pending_qs = CampaignPerformanceReport.objects.filter(status=CampaignPerformanceReport.STATUS_PENDING).order_by('requested_at')
    processed = 0
    ready = 0
    failed = 0

    for obj in pending_qs[:max_reports]:
        processed += 1
        obj.last_checked_at = timezone.now()
        try:
            store = obj.store
            token_info = get_store_performance_token(store)
            access_token = token_info.get('access_token')
            if not access_token:
                raise Exception("Нет access_token")
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            }
            url = f"https://api-performance.ozon.ru:443/api/client/statistics/report?UUID={obj.report_uuid}"
            max_attempts = 30
            retry_delay_sec = 10
            report_ready = False
            for attempt in range(1, max_attempts + 1):
                resp = requests.get(url, headers=headers, timeout=30)

                if resp.status_code in (401, 403):
                    # Обновим токен и повторим один раз в рамках попытки
                    try:
                        token_info = get_store_performance_token(store)
                        access_token = token_info.get('access_token')
                        headers["Authorization"] = f"Bearer {access_token}"
                        time.sleep(1)
                        resp = requests.get(url, headers=headers, timeout=30)
                    except Exception as t_err:
                        logger.error(f"[🔐] Не удалось обновить токен для отчёта {obj.report_uuid}: {t_err}")

                if resp.status_code == 202:
                    obj.save(update_fields=['last_checked_at'])
                    if attempt < max_attempts:
                        logger.info(
                            f"[⏳] Отчёт {obj.report_uuid} ещё готовится (попытка {attempt}/{max_attempts}). Повтор через {retry_delay_sec}s"
                        )
                        time.sleep(retry_delay_sec)
                        continue
                    logger.info(
                        f"[⏳] Отчёт {obj.report_uuid} не готов после {max_attempts} попыток. Оставляем в ожидании"
                    )
                    break

                if resp.status_code == 404:
                    obj.save(update_fields=['last_checked_at'])
                    if attempt < max_attempts:
                        logger.info(
                            f"[⏳] Отчёт {obj.report_uuid} пока недоступен (404 report not found, попытка {attempt}/{max_attempts}). Повтор через {retry_delay_sec}s"
                        )
                        time.sleep(retry_delay_sec)
                        continue
                    logger.info(
                        f"[⏳] Отчёт {obj.report_uuid} не найден после {max_attempts} попыток. Оставляем в ожидании"
                    )
                    break

                if resp.status_code != 200:
                    obj.status = CampaignPerformanceReport.STATUS_ERROR
                    obj.error_message = f"{resp.status_code} {resp.text}"
                    obj.save(update_fields=['status', 'error_message', 'last_checked_at'])
                    failed += 1
                    break

                report_ready = True
                break

            if not report_ready:
                if resp.status_code in (202, 404):
                    continue
                if resp.status_code != 200:
                    continue
                # Если сюда попали — считаем, что отчёт не готов и нет смысла разбирать данные
                continue

            data = resp.json() if resp.text else {}
            obj.raw_response = data

            # Поддерживаем 2 формата: одиночный и множественный по кампаниям
            top_level_report = data.get('report')
            report_date = timezone.localtime(obj.date_from).date() if obj.date_from else timezone.localdate()

            if top_level_report:
                # Считаем, что это одиночная кампания (или неизвестная) — используем parent.ozon_campaign_id
                obj.rows = top_level_report.get('rows') if isinstance(top_level_report.get('rows'), list) else None
                obj.totals = top_level_report.get('totals') if isinstance(top_level_report.get('totals'), dict) else None
                # Создаём/обновляем entry для связанной кампании, если известно
                camp_id = obj.ozon_campaign_id or ''
                if camp_id:
                    CampaignPerformanceReportEntry.objects.update_or_create(
                        store=obj.store,
                        ozon_campaign_id=str(camp_id),
                        report_date=report_date,
                        defaults={
                            'report': obj,
                            'rows': obj.rows,
                            'totals': obj.totals,
                        }
                    )
            else:
                # Ожидаем словарь { "<campaignId>": { title, report: { rows, totals } }, ... }
                obj.rows = None
                obj.totals = None
                for cid, payload in data.items():
                    if not isinstance(payload, dict):
                        continue
                    rep = payload.get('report') or {}
                    rows = rep.get('rows') if isinstance(rep.get('rows'), list) else None
                    totals = rep.get('totals') if isinstance(rep.get('totals'), dict) else None
                    if rows is None and totals is None:
                        continue
                    CampaignPerformanceReportEntry.objects.update_or_create(
                        store=obj.store,
                        ozon_campaign_id=str(cid),
                        report_date=report_date,
                        defaults={
                            'report': obj,
                            'rows': rows,
                            'totals': totals,
                        }
                    )

            obj.status = CampaignPerformanceReport.STATUS_READY
            obj.ready_at = timezone.now()
            obj.save(update_fields=['raw_response', 'rows', 'totals', 'status', 'ready_at', 'last_checked_at'])
            ready += 1
            logger.info(f"[📥] Получен отчёт UUID={obj.report_uuid} для {store}")
        except Exception as e:
            obj.status = CampaignPerformanceReport.STATUS_ERROR
            obj.error_message = str(e)
            obj.save(update_fields=['status', 'error_message', 'last_checked_at'])
            failed += 1

    return {"processed": processed, "ready": ready, "failed": failed}
#-------------------------------------


#Performance: прод — мгновенный дневной отчёт по всем кампаниям
def fetch_daily_campaign_statistics(date_str: str, store_id: int | None = None):
    """
    Запрашивает дневной отчёт по всем кампаниям за указанную дату без предварительного формирования
    и обновляет CampaignPerformanceReport/CampaignPerformanceReportEntry.
    """
    from .models import (
        CampaignPerformanceReport,
        CampaignPerformanceReportEntry,
        AdPlanItem,
        ManualCampaign,
    )
    from .utils import get_store_performance_token

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception as e:
        logger.error(f"[❌] Некорректная дата '{date_str}': {e}")
        return {"processed": 0, "updated": 0, "errors": 1, "reason": "invalid date"}

    stores_qs = OzonStore.objects.exclude(performance_client_id__isnull=True).exclude(performance_client_id='')
    if store_id:
        stores_qs = stores_qs.filter(id=store_id)

    stores = list(stores_qs)
    if not stores:
        logger.info("[ℹ️] Нет магазинов с настройками Performance API для мгновенного отчёта")
        return {"processed": 0, "updated": 0, "errors": 0, "reason": "no stores"}

    url_base = "https://api-performance.ozon.ru:443/api/client/statistics/daily/json"
    query = f"?dateFrom={target_date:%Y-%m-%d}&dateTo={target_date:%Y-%m-%d}"

    processed = 0
    updated_entries = 0
    errors = 0
    results: list[dict] = []

    for store in stores:
        processed += 1
        manual_ids = set(
            ManualCampaign.objects.filter(store=store)
            .exclude(ozon_campaign_id__isnull=True)
            .exclude(ozon_campaign_id='')
            .values_list('ozon_campaign_id', flat=True)
        )
        auto_ids = set(
            AdPlanItem.objects.filter(store=store)
            .exclude(ozon_campaign_id__isnull=True)
            .exclude(ozon_campaign_id='')
            .values_list('ozon_campaign_id', flat=True)
        )

        try:
            token_info = get_store_performance_token(store)
            access_token = token_info.get('access_token')
            if not access_token:
                raise Exception('нет access_token')

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            }
            resp = requests.get(f"{url_base}{query}", headers=headers, timeout=30)

            if resp.status_code in (401, 403):
                token_info = get_store_performance_token(store)
                access_token = token_info.get('access_token')
                if not access_token:
                    raise Exception('нет access_token после обновления')
                headers["Authorization"] = f"Bearer {access_token}"
                resp = requests.get(f"{url_base}{query}", headers=headers, timeout=30)

            if resp.status_code != 200:
                logger.error(
                    f"[❌] daily/json {store}: {resp.status_code} {resp.text}"
                )
                errors += 1
                results.append({"store_id": store.id, "status": resp.status_code})
                continue

            payload = resp.json() if resp.text else {}
            rows = payload.get('rows') if isinstance(payload.get('rows'), list) else []
            if not rows:
                logger.info(
                    f"[ℹ️] Магазин {store}: в daily/json за {target_date:%Y-%m-%d} нет данных"
                )
                results.append({"store_id": store.id, "status": "empty"})
                continue

            day_start = _make_aware(datetime.combine(target_date, datetime.min.time()))
            day_end = _make_aware(datetime.combine(target_date, datetime.max.time()))

            store_updates = 0
            manual_found = 0
            auto_found = 0
            unknown_found = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                campaign_id = str(row.get('id') or '').strip()
                if not campaign_id:
                    continue

                totals_payload = dict(row)
                rows_payload = [dict(row)]

                report_uuid = f"daily-{store.id}-{campaign_id}-{target_date:%Y%m%d}"
                report_defaults = {
                    'status': CampaignPerformanceReport.STATUS_READY,
                    'ready_at': timezone.now(),
                    'last_checked_at': timezone.now(),
                    'error_message': '',
                    'raw_response': {
                        'source': 'daily_json',
                        'rows': rows_payload,
                    },
                    'totals': totals_payload,
                    'rows': rows_payload,
                    'request_payload': {
                        'dateFrom': f"{target_date:%Y-%m-%d}",
                        'dateTo': f"{target_date:%Y-%m-%d}",
                        'source': 'daily_json',
                    },
                }

                report_obj, created_report = CampaignPerformanceReport.objects.get_or_create(
                    store=store,
                    ozon_campaign_id=campaign_id,
                    date_from=day_start,
                    date_to=day_end,
                    defaults={
                        'report_uuid': report_uuid,
                        **report_defaults,
                    }
                )
                if not created_report:
                    update_fields = []
                    for field, value in report_defaults.items():
                        current = getattr(report_obj, field, None)
                        if current != value:
                            setattr(report_obj, field, value)
                            update_fields.append(field)
                    if not report_obj.report_uuid:
                        report_obj.report_uuid = report_uuid
                        update_fields.append('report_uuid')
                    if update_fields:
                        report_obj.save(update_fields=update_fields)

                entry_defaults = {
                    'report': report_obj,
                    'rows': rows_payload,
                    'totals': totals_payload,
                }
                entry_obj, _ = CampaignPerformanceReportEntry.objects.update_or_create(
                    store=store,
                    ozon_campaign_id=campaign_id,
                    report_date=target_date,
                    defaults=entry_defaults,
                )

                store_updates += 1
                updated_entries += 1

                if campaign_id in manual_ids:
                    manual_found += 1
                elif campaign_id in auto_ids:
                    auto_found += 1
                else:
                    unknown_found += 1

            results.append(
                {
                    "store_id": store.id,
                    "updated": store_updates,
                    "manual": manual_found,
                    "auto": auto_found,
                    "unknown": unknown_found,
                }
            )
        except Exception as e:
            logger.error(f"[❌] Ошибка обработки daily/json для {store}: {e}")
            errors += 1
            results.append({"store_id": store.id, "error": str(e)})

    return {
        "processed": processed,
        "updated": updated_entries,
        "errors": errors,
        "results": results,
    }
#-------------------------------------
#--------Performance: прод — запросить отчёт по всем кампаниям на указанную дату---------------
def submit_campaign_reports_for_day(
    date_str: str,
    store_id: int | None = None,
    batch_size: int = 10,
    retry_interval_sec: int = 10,
    campaign_kind: str = "auto",
):
    """
    Формирует отчёт за один день по кампаниям Performance в зависимости от типа (авто/ручные).

    campaign_kind:
        - "auto": используется AdPlanItem (по умолчанию)
        - "manual": используется ManualCampaign
    Только кампании в состояниях RUNNING, ACTIVE или INACTIVE попадают в выборку.
    """
    from .utils import get_store_performance_token
    from .models import CampaignPerformanceReport

    try:
        base = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception as e:
        logger.error(f"[❌] Некорректная дата date_str='{date_str}': {e}")
        return {"created": 0, "errors": 1}
    day_start = _make_aware(base.replace(hour=0, minute=0, second=0, microsecond=0))
    day_end = _make_aware(base.replace(hour=23, minute=59, second=59, microsecond=999999))

    stores_qs = OzonStore.objects.all()
    if store_id:
        stores_qs = stores_qs.filter(id=store_id)

    created = 0
    errors = 0
    uuids = []

    campaign_kind = (campaign_kind or "auto").lower()
    campaign_config = {
        "auto": {
            "model": AdPlanItem,
            "states": {
                AdPlanItem.CAMPAIGN_STATE_RUNNING,
                AdPlanItem.CAMPAIGN_STATE_ACTIVE,
                AdPlanItem.CAMPAIGN_STATE_INACTIVE,
            },
        },
        "manual": {
            "model": ManualCampaign,
            "states": {
                ManualCampaign.CAMPAIGN_STATE_RUNNING,
                ManualCampaign.CAMPAIGN_STATE_ACTIVE,
                ManualCampaign.CAMPAIGN_STATE_INACTIVE,
            },
        },
    }.get(campaign_kind)

    if not campaign_config:
        logger.error(f"[❌] Неизвестный тип кампаний '{campaign_kind}' для запроса отчётов")
        return {"created": 0, "errors": 1}

    for store in stores_qs:
        # # Пропускаем магазины, где система рекламы выключена
        # try:
        #     from .models import StoreAdControl
        #     control = StoreAdControl.objects.filter(store=store).first()
        #     if control and not control.is_system_enabled:
        #         logger.info(f"[⛔] StoreAdControl выключен для {store}. Пропускаем запрос отчётов за {date_str}.")
        #         continue
        # except Exception as ctrl_err:
        #     logger.warning(f"[⚠️] Не удалось проверить StoreAdControl для {store}: {ctrl_err}")

        # Собираем campaign_id подходящих кампаний
        campaign_model = campaign_config["model"]
        state_filter = campaign_config["states"]
        all_ids = list(
            campaign_model.objects.filter(store=store, state__in=state_filter)
            .exclude(ozon_campaign_id__isnull=True)
            .exclude(ozon_campaign_id='')
            .values_list('ozon_campaign_id', flat=True)
            .distinct()
        )
        if not all_ids:
            continue

        # Авторизация
        try:
            token_info = get_store_performance_token(store)
            access_token = token_info.get('access_token')
            if not access_token:
                raise Exception("Нет access_token")
        except Exception as e:
            logger.error(f"[❌] Токен Performance не получен для {store}: {e}")
            errors += 1
            continue

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        url = "https://api-performance.ozon.ru:443/api/client/statistics/json"

        # Батчами по batch_size
        for i in range(0, len(all_ids), batch_size):
            batch = [str(x) for x in all_ids[i:i + batch_size]]
            payload = {
                "campaigns": batch,
                "dateFrom": date_str,
                "dateTo": date_str,
                "groupBy": "NO_GROUP_BY",
            }

            # Ретраим текущий батч, пока не получим UUID
            refresh_attempts = 0
            while True:
                try:
                    logger.info(f"[➡️ POST] /statistics/json {store} batch={len(batch)} for {date_str}")
                    resp = requests.post(url, headers=headers, json=payload, timeout=30)
                except Exception as e:
                    logger.error(f"[❌] Ошибка сети/запроса: {e}. Retry {retry_interval_sec}s…")
                    time.sleep(retry_interval_sec)
                    continue

                if resp.status_code in (200, 201, 202):
                    data = resp.json() if resp.text else {}
                    uuid_val = data.get('UUID') or data.get('uuid')
                    if not uuid_val:
                        logger.warning(f"[⚠️] Нет UUID в ответе: {data}. Retry {retry_interval_sec}s…")
                        time.sleep(retry_interval_sec)
                        continue

                    # Сохраняем PENDING отчёт; используем озон_campaign_id как 'MULTI:<UUID>'
                    try:
                        obj, _ = CampaignPerformanceReport.objects.update_or_create(
                            store=store,
                            ozon_campaign_id=f"MULTI:{uuid_val}",
                            date_from=day_start,
                            date_to=day_end,
                            defaults={
                                'report_uuid': uuid_val,
                                'status': CampaignPerformanceReport.STATUS_PENDING,
                                'request_payload': payload,
                            }
                        )
                        created += 1
                        uuids.append(uuid_val)
                        logger.info(f"[📨] UUID={uuid_val} сохранён (store={store}, batch={len(batch)}, {date_str})")
                    except Exception as db_err:
                        logger.error(f"[💾❌] Ошибка записи отчёта в БД: {db_err}. Retry {retry_interval_sec}s…")
                        time.sleep(retry_interval_sec)
                        continue
                    break  # идём к следующему батчу

                if resp.status_code == 403:
                    # Токен мог протухнуть — обновим и повторим
                    try:
                        refresh_attempts += 1
                        if refresh_attempts > 2:
                            logger.error(f"[🔐] 403 для {store}, превышен лимит обновлений токена. Пропускаем батч.")
                            errors += 1
                            break
                        from .utils import get_store_performance_token
                        token_info = get_store_performance_token(store)
                        access_token = token_info.get('access_token')
                        headers["Authorization"] = f"Bearer {access_token}"
                        logger.info(f"[🔐] Обновили токен для {store}, повторяем запрос…")
                        time.sleep(retry_interval_sec)
                        continue
                    except Exception as t_err:
                        logger.error(f"[❌] Не удалось обновить токен после 403: {t_err}")
                        errors += 1
                        break

                if resp.status_code == 429:
                    logger.info(f"[⏳] 429 лимит активных отчётов для {store}. Ждём {retry_interval_sec}s и пробуем снова…")
                    time.sleep(retry_interval_sec)
                    continue

                logger.error(f"[❌] statistics/json {store}: {resp.status_code} {resp.text}. Retry {retry_interval_sec}s…")
                time.sleep(retry_interval_sec)
                continue

    return {"created": created, "errors": errors, "uuids": uuids}
#-------------------------------------
#--------Performance: прод — запросить отчёт по всем авто-кампаниям на указанную дату---------------
# @shared_task(name="Performance: прод — запросить дневной отчёт по всем авто-кампаниям")
# def submit_auto_reports_for_day(date_str: str, store_id: int | None = None, batch_size: int = 10, retry_interval_sec: int = 10):
#     """Обёртка для автоматических кампаний (AdPlanItem)."""
#     return submit_campaign_reports_for_day(
#         date_str,
#         store_id=store_id,
#         batch_size=batch_size,
#         retry_interval_sec=retry_interval_sec,
#         campaign_kind="auto",
#     )
    


# @shared_task(name="Performance: прод — запросить дневной отчёт по ручным кампаниям")
# def submit_manual_reports_for_day(date_str: str, store_id: int | None = None, batch_size: int = 10, retry_interval_sec: int = 10):
#     """Обёртка для ручных кампаний (ManualCampaign)."""
#     return submit_campaign_reports_for_day(
#         date_str,
#         store_id=store_id,
#         batch_size=batch_size,
#         retry_interval_sec=retry_interval_sec,
#         campaign_kind="manual",
#     )
#-------------------------------------
#--------Performance: — дневной отчёт за вчера по всем компаниям каждый день запуск в 01:00
@shared_task(name="Performance: — дневной отчёт за вчера по всем компаниям каждый день запуск в 01:00")
def submit_all_reports_for_yesterday(store_id: int | None = None, batch_size: int = 10, retry_interval_sec: int = 10):    
    """
    Запрашивает отчёт за вчерашний день по всем  кампаниям .
    """
    date_str = (timezone.localdate() - timedelta(days=1)).strftime("%Y-%m-%d")
    fetch_daily_campaign_statistics(date_str)

#-------------------------------------
#--------Performance: прод — обёртка на вчерашний день (ручные кампании)---------------
# @shared_task(name="Performance: прод — дневной отчёт за вчера (ручные кампании)")
# def submit_manual_reports_for_yesterday(store_id: int | None = None, batch_size: int = 10, retry_interval_sec: int = 10):
#     """
#     Запрашивает отчёт за вчерашний день по всем ручным кампаниям (через submit_manual_reports_for_day).
#     """
#     date_str = (timezone.localdate() - timedelta(days=1)).strftime("%Y-%m-%d")
#     return submit_manual_reports_for_day(date_str, store_id=store_id, batch_size=batch_size, retry_interval_sec=retry_interval_sec)
#-------------------------------------

#--------Performance: обёртка на сегодня (все кампании) запускаем раз в час---------------
@shared_task(name="Performance: отчёт за сегодня по всем компаниям запускаем раз в час")
def submit_all_reports_for_today():

    date_str = timezone.localdate().strftime("%Y-%m-%d")
    # submit_auto_reports_for_day(date_str, store_id=store_id, batch_size=batch_size, retry_interval_sec=retry_interval_sec)
    # fetch_performance_reports()
    fetch_daily_campaign_statistics(date_str)
    update_auto_campaign_kpis_in_sheets()
    update_manual_campaign_kpis_in_sheets()
#-------------------------------------
#--------Performance: прод — обёртка на сегодня (ручные кампании)---------------
# @shared_task(name="Performance: прод — дневной отчёт за сегодня (ручные кампании)")
# def submit_manual_reports_for_today(store_id: int | None = None, batch_size: int = 10, retry_interval_sec: int = 10):
#     """
#     Запрашивает отчёт за текущий день по ручным кампаниям (через submit_manual_reports_for_day).
#     """
#     date_str = timezone.localdate().strftime("%Y-%m-%d")
#     submit_manual_reports_for_day(date_str, store_id=store_id, batch_size=batch_size, retry_interval_sec=retry_interval_sec)
#     fetch_performance_reports()
#-------------------------------------


#--------Performance: заполнение KPI авто-кампаний из отчётов и обновление Google Sheets (M..P)---------------
# Запускается раз в час, данные формируются из наших внутренних моделей
@shared_task(name="Performance:  — KPI авто-кампаний в Sheets (M..S) раз в час")
def update_auto_campaign_kpis_in_sheets(
    spreadsheet_url: str = None,
    sa_json_path: str = None,
    worksheet_name: str = "Main_ADV",
    start_row: int = 13,
    block_size: int = 100,
):
    """
    1) Считывает данные листа блоками.
    2) Для строк с campaign_id (колонка A) берёт только автоматические кампании (AdPlanItem) текущего магазина.
    3) Считает KPI и записывает в AdPlanItem.
    4) Обновляет таблицу в диапазоне M..S.
    Если URL не передан, перебирает все магазины с заполненным `google_sheet_url`.
    """

    logger.info("[🔄] Начало обновления KPI авто-кампаний")

    sa_json_path = sa_json_path or os.getenv(
        "GOOGLE_SA_JSON_PATH",
        "/workspace/ozon-469708-c5f1eca77c02.json",
    )

    store_hint = None
    if not spreadsheet_url:
        stores = list(
            OzonStore.objects.exclude(google_sheet_url__isnull=True).exclude(google_sheet_url='')
        )
        if not stores:
            logger.info("[ℹ️] Нет магазинов с заполненным google_sheet_url — обновление пропущено")
            return {"processed": 0, "results": [], "skipped": [{"reason": "no stores"}]}

        if len(stores) > 1:
            results: list[dict] = []
            errors: list[dict] = []
            skipped: list[dict] = []
            for store in stores:
                sheet_url = (store.google_sheet_url or '').strip()
                if not sheet_url:
                    skipped.append({"store_id": store.id, "reason": "no google_sheet_url"})
                    continue
                res = update_auto_campaign_kpis_in_sheets(
                    spreadsheet_url=sheet_url,
                    sa_json_path=sa_json_path,
                    worksheet_name=worksheet_name,
                    start_row=start_row,
                    block_size=block_size,
                )
                entry = {"store_id": store.id}
                if isinstance(res, dict):
                    entry.update(res)
                if entry.get("error"):
                    errors.append(entry)
                elif entry.get("skipped"):
                    skipped.append(entry)
                else:
                    results.append(entry)

            summary = {"processed": len(results), "results": results}
            if skipped:
                summary["skipped"] = skipped
            if errors:
                summary["errors"] = errors
            return summary

        store_hint = stores[0]
        spreadsheet_url = (store_hint.google_sheet_url or '').strip()
        if not spreadsheet_url:
            logger.warning(f"[⚠️] У магазина {store_hint} отсутствует google_sheet_url")
            return {"skipped": True, "reason": "no google_sheet_url", "store_id": store_hint.id}
    else:
        store_hint = OzonStore.objects.filter(google_sheet_url=spreadsheet_url).first()

    if not spreadsheet_url:
        logger.error("[❌] Не указан spreadsheet_url для обновления KPI")
        return {"error": "spreadsheet_url not provided"}

    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        gc = gspread.authorize(creds)

        t0 = time.perf_counter()
        sh = gc.open_by_url(spreadsheet_url)
        ws = sh.worksheet(worksheet_name)
        logger.info(f"[⏱] Открытие таблицы: {time.perf_counter() - t0:.3f}s")

        store_name = (ws.acell('V23').value or '').strip()
        if not store_name:
            logger.error("[❌] V23 (store) пусто — прерывание")
            return {"error": "store not set in V23"}

        store = store_hint or (
            OzonStore.objects.filter(google_sheet_url=spreadsheet_url).first()
        )
        if not store:
            store = (
                OzonStore.objects.filter(name__iexact=store_name).first()
                or OzonStore.objects.filter(client_id__iexact=store_name).first()
            )
        if not store:
            logger.error(f"[❌] Магазин '{store_name}' не найден")
            return {"error": f"store '{store_name}' not found"}

        auto_campaign_ids = list(
            AdPlanItem.objects.filter(store=store)
            .exclude(ozon_campaign_id__isnull=True)
            .exclude(ozon_campaign_id='')
            .values_list('ozon_campaign_id', flat=True)
        )
        auto_campaign_ids = [str(cid) for cid in auto_campaign_ids]

        # Период для расчёта s_amount/s_units/s_spend (ячейка V28)
        def _parse_period_days(value: str | None) -> int | None:
            if not value:
                return None
            digits = ''.join(ch for ch in str(value) if ch.isdigit())
            if not digits:
                return None
            try:
                days_val = int(digits)
                return days_val if days_val > 0 else None
            except Exception:
                return None

        try:
            raw_period = ws.acell('V28').value
        except Exception as e:
            logger.warning(f"[⚠️] Не удалось прочитать период V28: {e}")
            raw_period = None
        kpi_period_days = _parse_period_days(raw_period)
        if kpi_period_days:
            logger.info(f"[ℹ️] KPI период из V28: {kpi_period_days} дн.")
        else:
            logger.info("[ℹ️] KPI период V28 не задан — используем период с даты запуска кампании")

        # Определяем границу, где начинаются ручные кампании (колонка E = 'Ручная'),
        # чтобы не перезаписывать их показатели
        manual_start_row = None
        try:
            col_e = ws.col_values(5)
            for idx in range(start_row, len(col_e) + 1):
                if str(col_e[idx - 1]).strip().lower().startswith('руч'):
                    manual_start_row = idx
                    break
        except Exception as e:
            logger.warning(f"[⚠️] Не удалось определить границу ручных кампаний: {e}")

        if manual_start_row and manual_start_row <= start_row:
            logger.info("[ℹ️] Авто-кампаний нет выше ручных — выходим")
            return {"processed": 0, "updated": 0}

        tz = timezone.get_current_timezone()

        def _to_decimal(x) -> Decimal:
            if x is None:
                return Decimal('0')
            s = str(x)
            # удаляем неразрывные пробелы и заменяем запятую на точку
            s = s.replace('\u00A0', '').replace('\u202F', '').replace(' ', '').replace(',', '.')
            try:
                return Decimal(s)
            except Exception:
                return Decimal('0')

        def _to_local_date(value) -> dt_date:
            if isinstance(value, datetime):
                val = value
                if timezone.is_naive(val):
                    val = timezone.make_aware(val, tz)
                val = timezone.localtime(val, tz)
                return val.date()
            if isinstance(value, dt_date):
                return value
            return timezone.localdate()

        def _day_start(value) -> datetime:
            base_date = _to_local_date(value)
            start_naive = datetime.combine(base_date, datetime.min.time())
            return timezone.make_aware(start_naive, tz)

        def _day_end(value) -> datetime:
            start = _day_start(value)
            return start + timedelta(days=1) - timedelta(microseconds=1)

        def _sum_from_creation(ad: AdPlanItem):
            end_date = timezone.localdate()
            if kpi_period_days:
                start_date = end_date - timedelta(days=kpi_period_days - 1)
                # logger.info(f"if kpi_period_days start_date = {start_date}")
            else:
                start_dt = ad.ozon_created_at or ad.created_at
                # защитимся: если None, берём неделю назад
                if not start_dt:
                    start_dt = timezone.now() - timedelta(days=7)
                start_date = _to_local_date(start_dt)
            if ad.sku == 1928762293:
                logger.info(f"start_date  = {start_date}")
            qs = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                ozon_campaign_id=str(ad.ozon_campaign_id),
                report_date__gte=start_date,
            )
            if kpi_period_days:
                qs = qs.filter(report_date__lte=end_date)
            sales_amount = Decimal('0')
            sales_units = Decimal('0')
            spend = Decimal('0')
            for e in qs.iterator():
                t = e.totals or {}
                sales_amount += _to_decimal(t.get('ordersMoney'))
                sales_units += _to_decimal(t.get('orders'))
                spend += _to_decimal(t.get('moneySpent'))
            if ad.sku == 1914100274:
                logger.info(f"sales_units  = {sales_units}")
            return sales_amount, sales_units, spend

        def _drr_last_7_days(ad: AdPlanItem):
            end_date = timezone.localdate()
            start_date = end_date - timedelta(days=6)
            qs = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                ozon_campaign_id=str(ad.ozon_campaign_id),
                report_date__gte=start_date,
                report_date__lte=end_date,
            )
            sales_amount = Decimal('0')
            spend = Decimal('0')
            for e in qs.iterator():
                t = e.totals or {}
                sales_amount += _to_decimal(t.get('ordersMoney'))
                spend += _to_decimal(t.get('moneySpent'))
            if sales_amount > 0:
                drr = (spend / sales_amount * Decimal('100')).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            else:
                drr = Decimal('0.0')
            return drr, spend

        def _total_sales_since_creation(ad: AdPlanItem):
            end_date = timezone.localdate()
            if kpi_period_days:
                start_date = end_date - timedelta(days=kpi_period_days - 1)
            else:
                start_dt = ad.ozon_created_at or ad.created_at
                if not start_dt:
                    start_dt = timezone.now() - timedelta(days=7)
                start_date = _to_local_date(start_dt)

            start_dt = _day_start(start_date)
            end_dt = _day_end(end_date)
            qs = Sale.objects.filter(
                store=store,
                sku=ad.sku,
                date__gte=start_dt,
                date__lte=end_dt,
            ).only('quantity', 'price')
            amount = Decimal('0')
            units = 0
            for s in qs.iterator():
                try:
                    amount += Decimal(s.quantity) * Decimal(s.price)
                    units += int(s.quantity)
                except Exception:
                    continue

            return amount, units
        
        def _total_sales_last_7_days(ad: AdPlanItem):
            end_date = timezone.localdate()
            start_date = end_date - timedelta(days=6)
            start_dt = _day_start(start_date)
            end_dt = _day_end(end_date)
            qs = Sale.objects.filter(
                store=store,
                sku=ad.sku,
                date__gte=start_dt,
                date__lte=end_dt,
            ).only('quantity', 'price')
            amount = Decimal('0')
            for s in qs.iterator():
                try:
                    amount += Decimal(s.quantity) * Decimal(s.price)
                except Exception:
                    continue
            return amount

        current_row = start_row
        max_empty_rows = 5
        empty_rows = 0
        processed = 0
        updated = 0
        processed_campaign_ids: set[str] = set()

        while empty_rows < max_empty_rows:
            end_row = current_row + block_size - 1
            if manual_start_row and current_row > manual_start_row - 1:
                break
            if manual_start_row:
                end_row = min(end_row, manual_start_row - 1)
                if end_row < current_row:
                    break
            height = end_row - current_row + 1
            if height <= 0:
                break
            try:
                colA = ws.get(f'A{current_row}:A{end_row}') or []
            except Exception as e:
                logger.error(f"[❌] Ошибка чтения блока A{current_row}:A{end_row}: {e}")
                break

            if not colA:
                empty_rows += height
                current_row = end_row + 1
                continue

            # заготовим выходной массив M..S пустыми (7 столбцов)
            out_MS = [['', '', '', '', '', '', ''] for _ in range(height)]

            block_has_any = False
            for i, row_vals in enumerate(colA):
                row_number = current_row + i
                cellA = str(row_vals[0]).strip() if row_vals else ''
                if not cellA:
                    empty_rows += 1
                    continue
                else:
                    block_has_any = True
                    empty_rows = 0

                campaign_id = cellA
                # Ищем автоматическую кампанию
                ad = AdPlanItem.objects.filter(store=store, ozon_campaign_id=campaign_id).first()
                if not ad:
                    continue

                processed_campaign_ids.add(str(ad.ozon_campaign_id))

                # Рассчитываем KPI
                s_amount, s_units, s_spend = _sum_from_creation(ad)
                drr7, spend7 = _drr_last_7_days(ad)

                # Общие продажи по SKU
                total_amount, total_units = _total_sales_since_creation(ad)
                total_amount_7 = _total_sales_last_7_days(ad)
                # TACOS = adv_spend7 / total_sales_amount7 * 100
                if total_amount_7 > 0:
                    tacos = (spend7 / total_amount_7 * Decimal('100')).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
                else:
                    tacos = Decimal('0.0')

                # Обновляем модель
                ad.adv_sales_amount = s_amount
                ad.adv_sales_units = int(s_units) if s_units is not None else 0
                ad.adv_spend = s_spend
                ad.adv_drr_percent = drr7
                ad.total_sales_amount = total_amount
                ad.total_sales_units = int(total_units)
                ad.tacos_percent = tacos
                try:
                    ad.save(update_fields=['adv_sales_amount', 'adv_sales_units', 'adv_spend', 'adv_drr_percent', 'total_sales_amount', 'total_sales_units', 'tacos_percent'])
                except Exception as e:
                    logger.error(f"[💾❌] Не удалось сохранить KPI для кампании {campaign_id}: {e}")

                # Пишем в массив для M..S
                out_MS[i] = [
                    float(s_amount),
                    int(s_units),
                    float(s_spend),
                    float(drr7),
                    float(total_amount),
                    int(total_units),
                    float(tacos),
                ]
                updated += 1
                processed += 1

            # Обновляем блок в таблице
            try:
                ws.update(f'M{current_row}:S{end_row}', out_MS, value_input_option='USER_ENTERED')
            except Exception as e:
                logger.error(f"[❌] Ошибка записи блока M{current_row}:S{end_row}: {e}")

            current_row = end_row + 1

        # Суммируем месячный расход авто-кампаний и пишем в B10
        total_month_spend = Decimal('0')
        if auto_campaign_ids:
            try:
                month_start = timezone.localdate().replace(day=1)
                month_entries = CampaignPerformanceReportEntry.objects.filter(
                    store=store,
                    ozon_campaign_id__in=auto_campaign_ids,
                    report_date__gte=month_start,
                    report_date__lte=timezone.localdate(),
                )
                for entry in month_entries.iterator():
                    totals = entry.totals or {}
                    total_month_spend += _to_decimal(totals.get('moneySpent'))
            except Exception as spend_err:
                logger.warning(f"[⚠️] Не удалось посчитать месячный расход авто-кампаний: {spend_err}")
        try:
            ws.update('B10', [[float(total_month_spend)]], value_input_option='USER_ENTERED')
        except Exception as b10_err:
            logger.warning(f"[⚠️] Не удалось обновить B10 расходом авто-кампаний: {b10_err}")

        # Обновляем остаток месячного бюджета (B9 = B5 - расход за месяц по всем кампаниям)
        try:
            month_budget_raw = ws.acell('B5').value
            month_budget = _to_decimal(month_budget_raw)
        except Exception as b5_err:
            logger.warning(f"[⚠️] Не удалось прочитать B5 (месячный бюджет): {b5_err}")
            month_budget = Decimal('0')

        total_spent_month = Decimal('0')
        try:
            month_start = timezone.localdate().replace(day=1)
            today = timezone.localdate()
            spend_entries = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                report_date__gte=month_start,
                report_date__lte=today,
            )
            for entry in spend_entries.iterator():
                totals = entry.totals or {}
                total_spent_month += _to_decimal(totals.get('moneySpent'))
        except Exception as spend_err:
            logger.warning(f"[⚠️] Не удалось посчитать трату с начала месяца: {spend_err}")

        remaining_month_budget = month_budget - total_spent_month
        if remaining_month_budget < 0:
            remaining_month_budget = Decimal('0')

        try:
            ws.update('B9', [[float(remaining_month_budget)]], value_input_option='USER_ENTERED')
        except Exception as b9_err:
            logger.warning(f"[⚠️] Не удалось обновить B9 остатком бюджета: {b9_err}")

        logger.info(f"[📊] Обновление KPI завершено: обработано {processed}, записано {updated}")
        return {"processed": processed, "updated": updated, "store_id": getattr(store, 'id', None)}

    except Exception as e:
        logger.error(f"[❌] Ошибка update_auto_campaign_kpis_in_sheets: {e}")
        return {"error": str(e)}


def _update_campaign_statuses_in_sheets(store):
    """Обновляет статусы кампаний в Google Sheets для указанного магазина"""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        spreadsheet_url = (store.google_sheet_url or '').strip()
        if not spreadsheet_url:
            logger.warning(f"[⚠️] Пропускаем обновление статусов: у магазина {store} не задан google_sheet_url")
            return

        sa_json_path = os.getenv(
            "GOOGLE_SA_JSON_PATH",
            "/workspace/ozon-469708-c5f1eca77c02.json",
        )

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_url(spreadsheet_url)
        ws = spreadsheet.worksheet("Main_ADV")

        def _translate_auto_campaign_status(status: str) -> str:
            status_translations = {
                'PREVIEW': 'Предпросмотр',
                'ACTIVATED': 'Активирована',
                'UNKNOWN': 'Неизвестно',
                AdPlanItem.CAMPAIGN_STATE_RUNNING: 'Запущена',
                AdPlanItem.CAMPAIGN_STATE_ACTIVE: 'Активна',
                AdPlanItem.CAMPAIGN_STATE_INACTIVE: 'Неактивна',
                AdPlanItem.CAMPAIGN_STATE_PLANNED: 'Запланирована',
                AdPlanItem.CAMPAIGN_STATE_STOPPED: 'Остановлена (превышен бюджет)',
                AdPlanItem.CAMPAIGN_STATE_ARCHIVED: 'Архивная',
                AdPlanItem.CAMPAIGN_STATE_FINISHED: 'Завершена',
                AdPlanItem.CAMPAIGN_STATE_PAUSED: 'Приостановлена',
                AdPlanItem.CAMPAIGN_STATE_ENDED: 'Завершена',
                AdPlanItem.CAMPAIGN_STATE_MODERATION_DRAFT: 'Черновик модерации',
                AdPlanItem.CAMPAIGN_STATE_MODERATION_IN_PROGRESS: 'На модерации',
                AdPlanItem.CAMPAIGN_STATE_MODERATION_FAILED: 'Не прошла модерацию',
                AdPlanItem.CAMPAIGN_STATE_UNKNOWN: 'Неизвестно',
            }
            return status_translations.get(status, 'Неизвестно')

        ad_plan_items = AdPlanItem.objects.filter(store=store).exclude(ozon_campaign_id__isnull=True).exclude(ozon_campaign_id='')
        if not ad_plan_items.exists():
            logger.info(f"[ℹ️] У магазина {store} нет автоматических кампаний для обновления статусов")
            return

        try:
            a_values = ws.col_values(1)
        except Exception as col_err:
            logger.error(f"[❌] Не удалось прочитать колонку A для {store}: {col_err}")
            return

        index_map = {
            str(value).strip(): idx
            for idx, value in enumerate(a_values, start=1)
            if str(value).strip()
        }

        status_updates = []
        for ad_plan_item in ad_plan_items:
            cid = str(ad_plan_item.ozon_campaign_id).strip()
            row_idx = index_map.get(cid)
            if not row_idx:
                continue

            if ad_plan_item.paused_due_to_low_stock:
                actual_status = "Неактивна (низкие остатки)"
            else:
                actual_status = _translate_auto_campaign_status(ad_plan_item.state)

            status_updates.append({
                'range': f'C{row_idx}',
                'values': [[actual_status]]
            })

        if status_updates:
            ws.batch_update(status_updates)
            logger.info(f"[✅] Обновлено {len(status_updates)} статусов в колонке C для магазина {store}")
        else:
            logger.info(f"[ℹ️] Нет статусов для обновления в колонке C для магазина {store}")
            
    except Exception as e:
        logger.error(f"[❌] Ошибка при обновлении статусов в Google Sheets для магазина {store}: {e}")

# === Мониторинг рекламных кампаний по бюджету ===
@shared_task(name="Ежедневный мониторинг авто-кампаний: дневной лимит после обучения")
def monitor_auto_campaigns_weekly(reenable_hour: int = 9):
    """
    Логика мониторинга расхода авто-кампаний (AdPlanItem):
    1) В период обучения (train_days от даты создания кампании) — ничего не делаем.
    2) В неделю, где заканчивается обучение: дневной лимит = (week_budget - spend_за_дни_обучения_в_этой_неделе) / дни_до_конца_недели.
    3) В последующие недели: дневной лимит = week_budget / 7 (либо day_budget, если задан отдельно).
    Если текущий дневной расход > лимита — деактивируем кампанию до следующего дня и планируем повторную активацию на reenable_hour.
    """
    now = timezone.localtime()
    today = now.date()
    week_start = today - timedelta(days=today.weekday())  # Пн
    week_end = week_start + timedelta(days=6)             # Вс
    checked = stopped = resumed = skipped_training = 0

    from .models import StoreAdControl

    def _dec(x) -> Decimal:
        try:
            return Decimal(str(x))
        except Exception:
            return Decimal('0')

    def _parse_money_spent(val) -> Decimal:
        s = str(val or '').replace('\u00A0','').replace('\u202F','').replace('\xa0','').replace(' ', '').replace(',', '.')
        try:
            return Decimal(s)
        except Exception:
            return Decimal('0')

    def _sum_spend_for_period(ad: AdPlanItem, d_from: dt_date, d_to: dt_date) -> Decimal:
        total = Decimal('0')
        qs = CampaignPerformanceReportEntry.objects.filter(
            store=ad.store,
            ozon_campaign_id=str(ad.ozon_campaign_id),
            report_date__gte=d_from,
            report_date__lte=d_to,
        ).only('totals')
        for e in qs.iterator():
            tot = e.totals or {}
            total += _parse_money_spent(tot.get('moneySpent'))
        return total

    def _today_spend(ad: AdPlanItem) -> Decimal:
        return _sum_spend_for_period(ad, today, today)

    for ad in AdPlanItem.objects.filter(ozon_campaign_id__isnull=False).exclude(ozon_campaign_id=''):
        try:
            checked += 1

            # Пропускаем магазин, если система выключена
            try:
                ctrl = StoreAdControl.objects.filter(store=ad.store).first()
                if ctrl and not ctrl.is_system_enabled:
                    logger.info(f"[⛔] Пропуск кампании {ad.ozon_campaign_id} (SKU {ad.sku}): система магазина выключена")
                    continue
            except Exception:
                pass

            started_at = ad.ozon_created_at or ad.created_at
            if not started_at:
                continue
            t_days = int(ad.train_days or 0)
            age_days = (today - started_at.date()).days
            if age_days < t_days:
                skipped_training += 1
                logger.info(f"[🎓] Обучение: кампания {ad.ozon_campaign_id} (SKU {ad.sku}) age_days={age_days} < train_days={t_days}. Наблюдаем без действий.")
                continue

            week_budget = _dec(ad.week_budget or 0)
            manual_budget = _dec(ad.manual_budget or 0)
            effective_week_budget = manual_budget if manual_budget > Decimal('0') else week_budget
            # Базовый лимит после обучения — равномерно (используем ручной бюджет, если он задан)
            base_day_limit = (effective_week_budget / Decimal('7')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # Определяем, закончился ли период обучения в текущей неделе
            train_end_date = started_at.date() + timedelta(days=max(t_days - 1, 0))
            if week_start <= train_end_date <= week_end:
                # Считаем расход только за обучающие дни этой недели
                train_win_start = max(week_start, started_at.date())
                train_win_end = min(train_end_date, today)
                if train_win_end >= train_win_start:
                    spent_train = _sum_spend_for_period(ad, train_win_start, train_win_end)
                else:
                    spent_train = Decimal('0')
                days_left = (week_end - today).days + 1
                if days_left <= 0:
                    days_left = 1
                rem = max(Decimal('0'), week_budget - spent_train)
                day_limit = (rem / Decimal(days_left)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                logger.info(
                    f"[⚖️] Кампания {ad.ozon_campaign_id} (SKU {ad.sku}) — неделя завершения обучения: "
                    f"week_budget={float(week_budget)}, spent_train={float(spent_train)}, rem={float(rem)}, days_left={days_left}, day_limit={float(day_limit)}"
                )
            else:
                # Обучение завершилось ранее — используем базовый лимит. 
                # Дневной бюджет уменьшаем на 10% чтобы не было перерасхода. Т.к. существуют задержки в обновлении данных
                day_limit = ((effective_week_budget / Decimal('7')) * Decimal('0.9')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)




            today_spend = _today_spend(ad)
            logger.info(
                f"[💸] Кампания {ad.ozon_campaign_id} (SKU {ad.sku}) — today_spend={float(today_spend)} vs day_limit={float(day_limit)}"
            )

            # Автовключение в дневное время, если ранее остановили из-за перерасхода
            can_resume_now = now.hour > reenable_hour or (now.hour == reenable_hour and now.minute >= 0)
            if (
                ad.state == AdPlanItem.CAMPAIGN_STATE_STOPPED
                and can_resume_now
                and today_spend <= day_limit + Decimal('0.01')
            ):
                try:
                    from .utils import activate_campaign_for_store

                    activate_campaign_for_store(ad.store, ad.ozon_campaign_id)
                    AdPlanItem.objects.filter(id=ad.id).update(state=AdPlanItem.CAMPAIGN_STATE_ACTIVE)
                    ad.state = AdPlanItem.CAMPAIGN_STATE_ACTIVE
                    resumed += 1
                    logger.info(
                        f"[✅] Возобновили кампанию {ad.ozon_campaign_id} (SKU {ad.sku}) — условия для перезапуска выполнены"
                    )
                except Exception as e:
                    logger.error(f"[❌] Ошибка при повторном запуске кампании {ad.ozon_campaign_id}: {e}")

            if today_spend > day_limit + Decimal('0.01'):
                # Превышение — останавливаем до завтра
                try:
                    from .utils import deactivate_campaign_for_store
                    deactivate_campaign_for_store(ad.store, ad.ozon_campaign_id)
                    AdPlanItem.objects.filter(id=ad.id).update(state=AdPlanItem.CAMPAIGN_STATE_STOPPED)
                    ad.state = AdPlanItem.CAMPAIGN_STATE_STOPPED
                    stopped += 1
                    logger.info(f"[🛑] Превышен лимит. Остановили кампанию {ad.ozon_campaign_id} до завтра")
                except Exception as e:
                    logger.error(f"[❌] Ошибка деактивации {ad.ozon_campaign_id}: {e}")
        except Exception as e:
            logger.error(f"[❌] Ошибка мониторинга кампании {getattr(ad,'ozon_campaign_id','?')}: {e}")

    logger.info(f"[📊] Мониторинг: проверено={checked}, обучение={skipped_training}, остановлено={stopped}, возобновлено={resumed}")
    
    # Обновляем статусы в Google Sheets для измененных кампаний
    if stopped > 0 or resumed > 0:
        try:
            # Получаем уникальные магазины из измененных кампаний
            changed_stores = set()
            for ad in AdPlanItem.objects.filter(ozon_campaign_id__isnull=False).exclude(ozon_campaign_id=''):
                changed_stores.add(ad.store)
            
            for store in changed_stores:
                try:
                    _update_campaign_statuses_in_sheets(store)
                except Exception as store_err:
                    logger.warning(f"[⚠️] Ошибка обновления статусов в Sheets для магазина {store}: {store_err}")
        except Exception as sheets_err:
            logger.warning(f"[⚠️] Ошибка при обновлении статусов в Google Sheets: {sheets_err}")



@shared_task(name="Performance:  — KPI ручных кампаний в Sheets (M..S) раз в час")
def update_manual_campaign_kpis_in_sheets(
    spreadsheet_url: str = None,
    sa_json_path: str = None,
    worksheet_name: str = "Main_ADV",
    start_row: int = 13,
):
    """
    Обновляет блок ручных кампаний в Google Sheets (Main_ADV).
    Если URL не передан, перебирает все магазины с заполненным `google_sheet_url`.
    """

    sa_json_path = sa_json_path or os.getenv(
        "GOOGLE_SA_JSON_PATH",
        "/workspace/ozon-469708-c5f1eca77c02.json",
    )

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
    except Exception as cred_err:
        logger.error(f"[❌] Ошибка чтения сервисного аккаунта для Sheets: {cred_err}")
        return {"error": f"service account error: {cred_err}"}

    gc = gspread.authorize(creds)

    def _process_store(store_obj: OzonStore | None, sheet_url: str) -> dict:
        if not sheet_url:
            logger.warning("[⚠️] Пропускаем обновление: не указан URL Google Sheets")
            return {"error": "spreadsheet url not set"}

        try:
            t0 = time.perf_counter()
            sh = gc.open_by_url(sheet_url)
            ws = sh.worksheet(worksheet_name)
            logger.info(
                f"[⏱] Открытие таблицы (ручные кампании) {sheet_url}: {time.perf_counter() - t0:.3f}s"
            )

            # sheet_store_name = (ws.acell('V23').value or '').strip()
            store = store_obj

            # if store is None:
            #     if not sheet_store_name:
            #         logger.error("[❌] V23 (store) пусто — прекращаем обновление ручных кампаний")
            #         return {"error": "store not set in V23"}

            #     store = (
            #         OzonStore.objects.filter(name__iexact=sheet_store_name).first()
            #         or OzonStore.objects.filter(client_id__iexact=sheet_store_name).first()
            #         or OzonStore.objects.filter(name__icontains=sheet_store_name).first()
            #         or OzonStore.objects.filter(client_id__icontains=sheet_store_name).first()
            #     )
            #     if not store:
            #         logger.error(
            #             f"[❌] Магазин '{sheet_store_name}' не найден для обновления ручных кампаний"
            #         )
            #         return {"error": f"store '{sheet_store_name}' not found"}
            # else:
            #     if sheet_store_name and sheet_store_name.lower() not in {
            #         (store.name or '').lower(),
            #         (store.client_id or '').lower(),
            #     }:
            #         logger.warning(
            #             f"[⚠️] V23='{sheet_store_name}' не совпадает с магазином {store}. Продолжаем."
            #         )

            def _parse_period_days(value: str | None) -> int | None:
                if not value:
                    return None
                digits = ''.join(ch for ch in str(value) if ch.isdigit())
                if not digits:
                    return None
                try:
                    days_val = int(digits)
                    return days_val if days_val > 0 else None
                except Exception:
                    return None

            try:
                raw_period = ws.acell('V28').value
            except Exception as e:
                logger.warning(f"[⚠️] Не удалось прочитать период V28 для ручных кампаний: {e}")
                raw_period = None
            manual_kpi_period_days = _parse_period_days(raw_period)
            if manual_kpi_period_days:
                logger.info(
                    f"[ℹ️] KPI период (ручные кампании) из V28: {manual_kpi_period_days} дн."
                )
            else:
                logger.info("[ℹ️] KPI период V28 не задан — используем месячный период по умолчанию")

            def _parse_int_cell(value) -> int:
                if not value:
                    return 0
                s = ''.join(ch for ch in str(value) if ch.isdigit())
                return int(s) if s else 0

            min_fbs_stock = _parse_int_cell(ws.acell('V26').value)
            min_fbo_stock = _parse_int_cell(ws.acell('V27').value)
            logger.info(f"[⚙️] Пороги остатков: FBS={min_fbs_stock}, FBO={min_fbo_stock}")

            col_e = ws.col_values(5)
            col_a = ws.col_values(1)
            last_used_row = len(col_a)
            manual_start = None
            max_rows = max(len(col_e), len(col_a))
            for idx in range(start_row, max_rows + 1):
                type_val = col_e[idx - 1] if idx - 1 < len(col_e) else ''
                campaign_val = col_a[idx - 1] if idx - 1 < len(col_a) else ''
                if not str(campaign_val).strip():
                    continue
                if str(type_val).strip().lower().startswith('руч'):
                    manual_start = idx
                    break
            if manual_start is None:
                manual_start = max(start_row, last_used_row + 1)
            logger.info(f"[ℹ️] Ручные кампании будут записаны начиная с строки {manual_start}")

            clear_end = max(last_used_row, manual_start)
            ws.batch_clear([
                f'A{manual_start}:S{clear_end}',
                f'AX{manual_start}:AX{clear_end}',
            ])

            manual_active_states = {
                ManualCampaign.CAMPAIGN_STATE_RUNNING,
                ManualCampaign.CAMPAIGN_STATE_ACTIVE,
                ManualCampaign.CAMPAIGN_STATE_PLANNED,
            }

            campaigns_qs = (
                ManualCampaign.objects.filter(store=store, state__in=manual_active_states)
                .exclude(ozon_campaign_id__isnull=True)
                .exclude(ozon_campaign_id='')
            )
            campaigns = list(campaigns_qs)
            if not campaigns:
                logger.info(
                    f"[ℹ️] У магазина {store} нет ручных кампаний — блок очищен"
                )
                return {"written": 0}

            campaign_map: dict[str, ManualCampaign] = {str(c.ozon_campaign_id): c for c in campaigns}
            campaign_ids = list(campaign_map.keys())
            all_skus = set()

            def _campaign_sku_pairs(camp: ManualCampaign):
                pairs: list[tuple[int, str]] = []
                seen = set()

                def _add_pair(raw_sku, offer):
                    try:
                        sku_int = int(raw_sku)
                    except (TypeError, ValueError):
                        return
                    if sku_int in seen:
                        return
                    seen.add(sku_int)
                    pairs.append((sku_int, offer or ''))

                if camp.sku:
                    _add_pair(camp.sku, camp.offer_id)

                if isinstance(camp.sku_list, list):
                    offer_list = camp.offer_id_list if isinstance(camp.offer_id_list, list) else []
                    for idx, sku_item in enumerate(camp.sku_list):
                        offer_val = offer_list[idx] if idx < len(offer_list) else ''
                        _add_pair(sku_item, offer_val)

                return pairs

            campaign_data = {}
            for camp_id, camp in campaign_map.items():
                sku_pairs = _campaign_sku_pairs(camp)
                if not sku_pairs:
                    continue
                campaign_data[camp_id] = {
                    'campaign': camp,
                    'sku_pairs': sku_pairs,
                    'adv_sales_amount': Decimal('0'),
                    'adv_sales_units': Decimal('0'),
                    'adv_spend': Decimal('0'),
                    'orders_money_7': Decimal('0'),
                    'spend_7': Decimal('0'),
                    'total_sales_amount': Decimal('0'),
                    'total_sales_units': 0,
                    'total_sales_amount_7': Decimal('0'),
                }
                for sku_val, _ in sku_pairs:
                    all_skus.add(sku_val)

            if not campaign_data:
                logger.info(
                    f"[ℹ️] Ручные кампании без SKU у магазина {store} — блок очищен"
                )
                return {"written": 0}

            fbs_by_sku = {
                row['sku']: row['total'] or 0
                for row in FbsStock.objects.filter(store=store, sku__in=all_skus)
                .values('sku').annotate(total=Sum('present'))
            }
            fbo_by_sku = {
                row['sku']: row['total'] or 0
                for row in WarehouseStock.objects.filter(store=store, sku__in=all_skus)
                .values('sku').annotate(total=Sum('available_stock_count'))
            }

            products_map = {
                row['sku']: row
                for row in Product.objects.filter(store=store, sku__in=all_skus)
                .values('sku', 'offer_id', 'name')
            }

            today = timezone.localdate()
            month_start = today.replace(day=1)
            week_start = today - timedelta(days=6)
            if manual_kpi_period_days:
                adv_period_start = today - timedelta(days=manual_kpi_period_days - 1)
            else:
                adv_period_start = month_start


            adv_entries = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                ozon_campaign_id__in=campaign_ids,
                report_date__gte=adv_period_start,
                report_date__lte=today,
            )

            def _to_decimal(val) -> Decimal:
                if isinstance(val, Decimal):
                    return val

                if val is None:
                    return Decimal('0')

                if isinstance(val, (int, float)):
                    return Decimal(str(val)).quantize(Decimal('0.01'))

                if isinstance(val, str):
                    cleaned = (
                        val.replace('\u00A0', '')
                        .replace('\u202F', '')
                        .replace(' ', '')
                        .replace(',', '.')
                    )
                    if cleaned == '':
                        return Decimal('0')
                    try:
                        return Decimal(cleaned).quantize(Decimal('0.01'))
                    except Exception:
                        return Decimal('0')

                try:
                    return Decimal(str(val)).quantize(Decimal('0.01'))
                except Exception:
                    return Decimal('0')

            for entry in adv_entries.iterator():
                data = campaign_data.get(entry.ozon_campaign_id)
                if not data:
                    continue
                totals = entry.totals or {}
                data['adv_sales_amount'] += _to_decimal(totals.get('ordersMoney'))
                data['adv_sales_units'] += _to_decimal(totals.get('orders'))
                data['adv_spend'] += _to_decimal(totals.get('moneySpent'))

            week_entries = CampaignPerformanceReportEntry.objects.filter(
                store=store,
                ozon_campaign_id__in=campaign_ids,
                report_date__gte=week_start,
                report_date__lte=today,
            )
            for entry in week_entries.iterator():
                data = campaign_data.get(entry.ozon_campaign_id)
                if not data:
                    continue
                totals = entry.totals or {}
                data['orders_money_7'] += _to_decimal(totals.get('ordersMoney'))
                data['spend_7'] += _to_decimal(totals.get('moneySpent'))

            manual_month_spend = Decimal('0')
            try:
                month_spend_entries = CampaignPerformanceReportEntry.objects.filter(
                    store=store,
                    ozon_campaign_id__in=campaign_ids,
                    report_date__gte=month_start,
                    report_date__lte=today,
                )
                for entry in month_spend_entries.iterator():
                    totals = entry.totals or {}
                    manual_month_spend += _to_decimal(totals.get('moneySpent'))
            except Exception as spend_err:
                logger.warning(
                    f"[⚠️] Не удалось посчитать месячный расход ручных кампаний: {spend_err}"
                )

            amount_expr = ExpressionWrapper(
                F('quantity') * F('price'),
                output_field=DecimalField(max_digits=18, decimal_places=2)
            )
            sales_period_start = adv_period_start
            sales_month = {
                row['sku']: {
                    'amount': row['amount'] or Decimal('0'),
                    'units': row['units'] or 0,
                }
                for row in Sale.objects.filter(
                    store=store,
                    sku__in=all_skus,
                    date__gte=sales_period_start,
                )
                .values('sku')
                .annotate(amount=Sum(amount_expr), units=Sum('quantity'))
            }
            sales_week = {
                row['sku']: row['amount'] or Decimal('0')
                for row in Sale.objects.filter(
                    store=store,
                    sku__in=all_skus,
                    date__gte=week_start,
                    date__lte=today
                )
                .values('sku')
                .annotate(amount=Sum(amount_expr))
            }

            eligible_campaigns = []
            for camp_id, data in campaign_data.items():
                total_amount = Decimal('0')
                total_units = 0
                total_amount_7 = Decimal('0')
                for sku_val, _ in data['sku_pairs']:
                    if sku_val in sales_month:
                        total_amount += sales_month[sku_val]['amount']
                        total_units += int(sales_month[sku_val]['units'] or 0)
                    if sku_val in sales_week:
                        total_amount_7 += sales_week[sku_val]

                data['total_sales_amount'] = total_amount
                data['total_sales_units'] = total_units
                data['total_sales_amount_7'] = total_amount_7
                eligible_campaigns.append(data)

            if not eligible_campaigns:
                logger.info(
                    f"[ℹ️] За текущий месяц нет заказов по ручным кампаниям магазина {store} — блок очищен"
                )
                return {"written": 0}

            eligible_campaigns.sort(
                key=lambda x: (x['adv_sales_amount'], x['total_sales_amount']),
                reverse=True
            )

            def _campaign_status_ru(camp: ManualCampaign) -> str:
                status_map = {
                    ManualCampaign.CAMPAIGN_STATE_RUNNING: 'Запущена',
                    ManualCampaign.CAMPAIGN_STATE_ACTIVE: 'Активна',
                    ManualCampaign.CAMPAIGN_STATE_INACTIVE: 'Неактивна',
                    ManualCampaign.CAMPAIGN_STATE_PAUSED: 'Приостановлена',
                    ManualCampaign.CAMPAIGN_STATE_STOPPED: 'Остановлена',
                    ManualCampaign.CAMPAIGN_STATE_PLANNED: 'Запланирована',
                    ManualCampaign.CAMPAIGN_STATE_ENDED: 'Завершена',
                    ManualCampaign.CAMPAIGN_STATE_ARCHIVED: 'Архивная',
                    ManualCampaign.CAMPAIGN_STATE_FINISHED: 'Завершена',
                    ManualCampaign.CAMPAIGN_STATE_MODERATION_DRAFT: 'Черновик модерации',
                    ManualCampaign.CAMPAIGN_STATE_MODERATION_IN_PROGRESS: 'На модерации',
                    ManualCampaign.CAMPAIGN_STATE_MODERATION_FAILED: 'Не прошла модерацию',
                }
                return status_map.get(camp.state, 'Неизвестно')

            rows_al: list[list] = []
            rows_ms: list[list] = []
            highlight_cells: list[str] = []

            for data in eligible_campaigns:
                camp: ManualCampaign = data['campaign']
                status_ru = _campaign_status_ru(camp)
                adv_amount = data['adv_sales_amount']
                adv_units_val = data['adv_sales_units']
                if isinstance(adv_units_val, Decimal):
                    adv_units = int(adv_units_val.to_integral_value(rounding=ROUND_HALF_UP))
                else:
                    adv_units = int(adv_units_val)
                adv_spend = data['adv_spend']

                orders_money_7 = data['orders_money_7']
                spend_7 = data['spend_7']
                if orders_money_7 > 0:
                    drr_percent = (
                        spend_7 / orders_money_7 * Decimal('100')
                    ).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
                else:
                    drr_percent = Decimal('0.0')

                for sku_val, offer in data['sku_pairs']:
                    month_stats = sales_month.get(sku_val)
                    total_amount = month_stats['amount'] if month_stats else Decimal('0')
                    total_units = int(month_stats['units'] or 0) if month_stats else 0
                    week_amount = sales_week.get(sku_val, Decimal('0'))
                    if week_amount > 0:
                        tacos_percent = (
                            spend_7 / week_amount * Decimal('100')
                        ).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
                    else:
                        tacos_percent = Decimal('0.0')

                    product_info = products_map.get(sku_val, {})
                    product_name = (
                        offer
                        or product_info.get('offer_id')
                        or product_info.get('name')
                        or f"SKU_{sku_val}"
                    )
                    fbs_stock = int(fbs_by_sku.get(sku_val, 0))
                    fbo_stock = int(fbo_by_sku.get(sku_val, 0))

                    row_values = [
                        str(camp.ozon_campaign_id),
                        '',
                        status_ru,
                        camp.name or '',
                        'Ручная',
                        product_name,
                        int(sku_val),
                        fbs_stock,
                        fbo_stock,
                        0.0,
                        float(camp.week_budget or 0),
                        float(camp.daily_budget or 0),
                    ]
                    rows_al.append(row_values)
                    rows_ms.append([
                        float(adv_amount),
                        int(adv_units),
                        float(adv_spend),
                        float(drr_percent),
                        float(total_amount),
                        int(total_units),
                        float(tacos_percent),
                    ])

                    current_index = manual_start + len(rows_al) - 1
                    if min_fbs_stock and fbs_stock < min_fbs_stock:
                        highlight_cells.append(rowcol_to_a1(current_index, 8))
                    if min_fbo_stock and fbo_stock < min_fbo_stock:
                        highlight_cells.append(rowcol_to_a1(current_index, 9))

            if not rows_al:
                logger.info(
                    f"[ℹ️] После фильтрации не осталось строк для записи по магазину {store}"
                )
                return {"written": 0}

            manual_end = manual_start + len(rows_al) - 1
            ws.update(f'A{manual_start}:L{manual_end}', rows_al, value_input_option='USER_ENTERED')
            ws.update(f'M{manual_start}:S{manual_end}', rows_ms, value_input_option='USER_ENTERED')
            ws.update(f'AX{manual_start}:AX{manual_end}', [[''] for _ in rows_al])

            try:
                ws.update('C10', [[float(manual_month_spend)]], value_input_option='USER_ENTERED')
            except Exception as c10_err:
                logger.warning(
                    f"[⚠️] Не удалось обновить C10 месячным расходом ручных кампаний: {c10_err}"
                )

            format_cell_ranges(ws, [(
                f'H{manual_start}:I{manual_end}',
                CellFormat(backgroundColor=Color(1, 1, 1))
            )])

            if highlight_cells:
                gray_fill = CellFormat(backgroundColor=Color(0.9, 0.9, 0.9))
                format_cell_ranges(ws, [(cell, gray_fill) for cell in highlight_cells])

            logger.info(
                f"[✅] Обновлены ручные кампании для {store}: {len(rows_al)} строк (строки {manual_start}-{manual_end})"
            )
            return {"written": len(rows_al), "start_row": manual_start}

        except Exception as e:
            logger.error(
                f"[❌] Ошибка обновления ручных кампаний в таблице {sheet_url}: {e}"
            )
            return {"error": str(e)}

    if spreadsheet_url:
        store = OzonStore.objects.filter(google_sheet_url=spreadsheet_url).first()
        return _process_store(store, spreadsheet_url)

    stores = OzonStore.objects.exclude(google_sheet_url__isnull=True).exclude(google_sheet_url='')
    if not stores:
        logger.info("[ℹ️] Нет магазинов с заполненным google_sheet_url — обновление пропущено")
        return {"processed": 0, "results": [], "errors": []}

    results: list[dict] = []
    errors: list[dict] = []
    for store in stores:
        sheet_url = store.google_sheet_url
        result = _process_store(store, sheet_url)
        if result.get('error'):
            errors.append({"store_id": store.id, "error": result['error']})
            continue
        success_result = dict(result)
        success_result["store_id"] = store.id
        results.append(success_result)

    summary = {"processed": len(results), "results": results}
    if errors:
        summary["errors"] = errors
    return summary
