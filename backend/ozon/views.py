from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, permissions, status
from django.shortcuts import get_object_or_404
from users.models import User, OzonStore, StoreFilterSettings, StoreAccess
from .models import (
    Product,
    Category,
    ProductType,
    WarehouseStock,
    OzonWarehouseDirectory,
    OzonSupplyBatch,
    OzonSupplyDraft,
    Sale,
    FbsStock,
    DeliveryCluster,
    DeliveryClusterItemAnalytics,
    DeliveryAnalyticsSummary,
)
from .utils import (
    fetch_all_products_from_ozon,
    fetch_detailed_products_from_ozon,
    fetch_and_save_category_tree,
    fetch_warehouse_stock,
    fetch_fbs_sales,
    fetch_fbo_sales,
    fetch_fbs_stocks,
)
from .serializers import (
    DraftCreateSerializer,
    SupplyBatchStatusSerializer,
    SupplyBatchConfirmedSerializer,
)
import time
from django.db.models import Sum, F, Count, Q
from datetime import datetime, timedelta
from django.utils import timezone
from collections import defaultdict
from .tasks import (
    update_abc_sheet,
    create_or_update_AD,
    sync_campaign_activity_with_sheets,
    toggle_store_ads_status,
    rebalance_auto_weekly_budgets,
    sync_warehouse_stock_for_store,
)

import logging
import requests


def user_store_queryset(user):
    """
    Магазины, доступные пользователю: свои и те, куда его пригласили.
    """
    return (
        OzonStore.objects.filter(
            Q(user=user) | Q(accesses__user=user, accesses__status=StoreAccess.STATUS_ACCEPTED)
        )
        .distinct()
    )
# Наполянем модель товароми
class SyncOzonProductView(APIView):
    def post(self, request):
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        if not api_key or not client_id:
            return Response({"error": "Missing Api-Key header"}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)

        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=status.HTTP_403_FORBIDDEN)

        try:
            basic_items = fetch_all_products_from_ozon(ozon_store.client_id, ozon_store.api_key)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        product_ids = [item["product_id"] for item in basic_items]
        detailed_items = fetch_detailed_products_from_ozon(ozon_store.client_id, ozon_store.api_key, product_ids)

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
                store = ozon_store,
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

        return Response({"status": "ok", "products_saved": total_saved})

# Синхронизация категорий и типов товаров
class SyncOzonCategoryTreeView(APIView):
    def post(self, request):
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        if not api_key:
            return Response({"error": "Missing Api-Key"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=status.HTTP_403_FORBIDDEN)

        try:
            fetch_and_save_category_tree(ozon_store.client_id, ozon_store.api_key)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        return Response({"status": "ok", "message": "Категории и типы успешно сохранены"})

# Синхронизация остатков на складах    
class SyncOzonWarehouseStockView(APIView):
    def post(self, request):
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        if not api_key:
            return Response({"error": "Missing Api-Key"}, status=400)

        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=403)

        # Собираем SKU по пользователю
        skus = list(
            Product.objects.filter(store=ozon_store)
            .exclude(sku__isnull=True)
            .values_list("sku", flat=True)
        )

        if not skus:
            return Response({"status": "ok", "message": "Нет SKU для проверки."})

        try:
            stock_items = fetch_warehouse_stock(ozon_store.client_id, ozon_store.api_key, skus)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        # Удаляем старые записи, которых больше нет
        WarehouseStock.objects.filter(store=ozon_store).delete()


        updated_count = 0

        for item in stock_items:
            sku = item["sku"]
            product = Product.objects.filter(store=ozon_store, sku=sku).first()

            WarehouseStock.objects.update_or_create(
                store=ozon_store,
                sku=sku,
                cluster_id=item.get("cluster_id"),
                warehouse_id=item.get("warehouse_id"),
                defaults={
                    "product": product,
                    "warehouse_name": item.get("warehouse_name", ""),
                    "available_stock_count": item.get("available_stock_count", 0),
                    "return_from_customer_stock_count": item.get("return_from_customer_stock_count", 0),
                    "transit_stock_count": item.get("transit_stock_count", 0),
                    "stock_defect_stock_count": item.get("stock_defect_stock_count", 0),
                    "cluster_name": item.get("cluster_name", ""),
                }
            )
            updated_count += 1

        return Response({"status": "ok", "stocks_updated": updated_count})

# Синхронизация продаж    
class SyncOzonSalesView(APIView):
    def post(self, request):
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        days = int(request.data.get("days", 7))

        if not api_key:
            return Response({"error": "Missing Api-Key"}, status=400)

        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=403)

        try:
            fbo_sales = fetch_fbo_sales(ozon_store.client_id, ozon_store.api_key, days)
            fbs_sales = fetch_fbs_sales(ozon_store.client_id, ozon_store.api_key, days)
            
            # fbo_sales = []
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        total_created = 0
        total_updated = 0

        for sale_data in fbo_sales + fbs_sales:
            if sale_data["posting_number"] == "24112774-0215-1":
                        logging.info(f"------------------------------------------------------------------")
            obj, created = Sale.objects.update_or_create(
                posting_number=sale_data["posting_number"],
                sku=sale_data["sku"],
                sale_type=sale_data["sale_type"],
                defaults={
                    "store": ozon_store,
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

        return Response({
            "status": "ok",
            "created": total_created,
            "status_updated": total_updated
        })


class OzonFboWarehouseSearchView(APIView):
    """
    Поиск точек отгрузки FBO (https://api-seller.ozon.ru/v1/warehouse/fbo/list).
    Принимает:
      - store_id (если у пользователя несколько магазинов; если один — берём его)
      - filter_by_supply_type (список или строка) опционально
      - search (строка) опционально
    Возвращает поле "search" из ответа Ozon.
    """

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        store_id = request.data.get("store_id")
        filter_by_supply_type = request.data.get("filter_by_supply_type") or []
        search_query = request.data.get("search", "")

        stores_qs = user_store_queryset(request.user)
        if store_id:
            store = stores_qs.filter(id=store_id).first()
            if not store:
                return Response({"error": "Store not found or not accessible"}, status=404)
        else:
            count = stores_qs.count()
            if count == 0:
                return Response({"error": "No stores available"}, status=404)
            if count > 1:
                return Response({"error": "Specify store_id"}, status=400)
            store = stores_qs.first()

        if isinstance(filter_by_supply_type, str):
            filter_by_supply_type = [filter_by_supply_type]

        payload = {
            "filter_by_supply_type": filter_by_supply_type,
            "search": search_query,
        }

        headers = {
            "Client-Id": store.client_id,
            "Api-Key": store.api_key,
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(
                "https://api-seller.ozon.ru/v1/warehouse/fbo/list",
                json=payload,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            return Response({"error": f"Ozon API error: {e}"}, status=500)

        data = resp.json() or {}
        return Response({"search": data.get("search", [])})

#Получение остатков FBS
# Синхронизация остатков FBS        
class SyncFbsStockView(APIView):
    def post(self, request):
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        if not api_key:
            return Response({"error": "Missing Api-Key"}, status=400)

        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=403)

        skus = list(
            Product.objects.filter(store=ozon_store)
            .exclude(sku__isnull=True)
            .values_list("sku", flat=True)
        )

        if not skus:
            return Response({"status": "ok", "message": "Нет SKU для синхронизации."})

        try:
            stock_items = fetch_fbs_stocks(ozon_store.client_id, ozon_store.api_key, skus)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

        # Удаляем старые остатки
        FbsStock.objects.filter(store=ozon_store).delete()

        created = 0
        stock_objects = []

        for item in stock_items:
            stock_objects.append(FbsStock(
                store=ozon_store,
                product_id=item.get("product_id"),
                sku=item.get("sku"),
                fbs_sku=item.get("fbs_sku"),
                present=item.get("present", 0),
                reserved=item.get("reserved", 0),
                warehouse_id=item.get("warehouse_id"),
                warehouse_name=item.get("warehouse_name", "")
            ))
            created += 1

        FbsStock.objects.bulk_create(stock_objects)

        return Response({"status": "ok", "stocks_saved": created})

# Создание черновиков поставок (по одному на кластер)
class CreateSupplyDraftView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        serializer = DraftCreateSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.validated_data
        store_id = data["store_id"]
        store = get_object_or_404(user_store_queryset(request.user), id=store_id)

        destination = data["destinationWarehouse"]
        supply_type = data["supplyType"]

        last_seq = OzonSupplyBatch.objects.filter(store=store).order_by("-batch_seq").first()
        next_seq = (last_seq.batch_seq + 1) if last_seq else 1
        batch = OzonSupplyBatch.objects.create(
            store=store,
            batch_seq=next_seq,
            supply_type=supply_type,
            drop_off_point_warehouse_id=destination["warehouse_id"],
            drop_off_point_name=destination.get("name", ""),
            status="queued",
        )

        for shipment in data["shipments"]:
            cluster_name = shipment["warehouse"]
            cluster = OzonWarehouseDirectory.objects.filter(
                store=store,
                logistic_cluster_name__iexact=cluster_name,
            ).first()
            if not cluster:
                continue

            items = [
                {"sku": item["sku"], "quantity": item["quantity"]}
                for item in shipment["items"]
                if item["quantity"] > 0
            ]
            if not items:
                continue

            payload = {
                "cluster_ids": [str(cluster.logistic_cluster_id)],
                "drop_off_point_warehouse_id": destination["warehouse_id"],
                "items": items,
                "type": supply_type,
            }

            OzonSupplyDraft.objects.create(
                batch=batch,
                store=store,
                supply_type=supply_type,
                logistic_cluster_id=cluster.logistic_cluster_id,
                logistic_cluster_name=cluster.logistic_cluster_name,
                drop_off_point_warehouse_id=destination["warehouse_id"],
                drop_off_point_name=destination.get("name", ""),
                request_payload=payload,
                status="queued",
            )

        try:
            from .tasks import process_supply_batch
            process_supply_batch.delay(str(batch.batch_id))
            batch.status = "processing"
            batch.save(update_fields=["status", "updated_at"])
        except Exception as exc:
            logging.error(f"Не удалось запустить задачу создания черновиков: {exc}")

        drafts_data = [
            {
                "draft_id": d.id,
                "warehouse": d.logistic_cluster_name,
                "cluster_id": d.logistic_cluster_id,
                "status": d.status,
            }
            for d in batch.drafts.all()
        ]

        return Response(
            {
                "batch_id": str(batch.batch_id),
                "batch_seq": batch.batch_seq,
                "store_id": store.id,
                "drop_off_point_warehouse_id": batch.drop_off_point_warehouse_id,
                "drafts": drafts_data,
            },
            status=status.HTTP_202_ACCEPTED,
        )


class SupplyDraftBatchStatusView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, batch_id: str):
        store_qs = user_store_queryset(request.user)
        batch = get_object_or_404(OzonSupplyBatch, batch_id=batch_id, store__in=store_qs)
        if not batch.drafts.exclude(status="created").exists():
            return Response({"error": "Batch not found"}, status=status.HTTP_404_NOT_FOUND)

        serializer = SupplyBatchStatusSerializer(batch)
        return Response(serializer.data, status=status.HTTP_200_OK)


class SupplyDraftBatchListView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = SupplyBatchStatusSerializer

    def get_queryset(self):
        store_qs = user_store_queryset(self.request.user)
        qs = (
            OzonSupplyBatch.objects
            .filter(store__in=store_qs)
            .prefetch_related("drafts")
            .annotate(
                non_created_count=Count("drafts", filter=~Q(drafts__status="created"))
            )
            .filter(non_created_count__gt=0)
            .order_by("-created_at")
        )
        store_id = self.request.query_params.get("store_id")
        if store_id:
            qs = qs.filter(store_id=store_id)
        return qs


class SupplyDraftBatchConfirmedListView(generics.ListAPIView):
    """
    Батчи, по которым все черновики уже подтверждены (status=created).
    """

    permission_classes = [permissions.IsAuthenticated]
    serializer_class = SupplyBatchConfirmedSerializer

    def get_queryset(self):
        store_qs = user_store_queryset(self.request.user)
        qs = (
            OzonSupplyBatch.objects
            .filter(store__in=store_qs)
            .prefetch_related("drafts")
            .annotate(
                created_count=Count("drafts", filter=Q(drafts__status="created")),
                total_count=Count("drafts"),
            )
            .filter(created_count__gt=0, total_count=F("created_count"))
            .order_by("-created_at")
        )
        store_id = self.request.query_params.get("store_id")
        if store_id:
            qs = qs.filter(store_id=store_id)
        return qs


class SupplyDraftSelectWarehouseView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def _normalize(self, draft):
        data = draft.supply_warehouse or []
        flat = []
        for entry in data:
            if isinstance(entry, dict) and entry.get("warehouse_id"):
                flat.append(entry)
            elif isinstance(entry, dict) and "warehouses" in entry:
                for w in entry.get("warehouses", []):
                    sw = w.get("supply_warehouse") or {}
                    if sw.get("warehouse_id"):
                        flat.append({
                            **sw,
                            "status": w.get("status"),
                            "bundle_ids": w.get("bundle_ids"),
                            "travel_time_days": w.get("travel_time_days"),
                        })
        if flat:
            draft.supply_warehouse = flat
            if not draft.selected_supply_warehouse:
                draft.selected_supply_warehouse = flat[0]
            draft.save(update_fields=["supply_warehouse", "selected_supply_warehouse", "updated_at"])
        return flat

    def post(self, request, draft_id: int):
        warehouse_id = request.data.get("warehouse_id")
        if not warehouse_id:
            return Response({"error": "warehouse_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        draft = get_object_or_404(
            OzonSupplyDraft,
            id=draft_id,
            store__in=user_store_queryset(request.user),
        )

        warehouses = self._normalize(draft)
        selected = None
        for w in warehouses:
            wid = w.get("warehouse_id") or w.get("id")
            if wid and str(wid) == str(warehouse_id):
                selected = w
                break

        if not selected:
            return Response({"error": "warehouse_id not found in supply_warehouse"}, status=status.HTTP_400_BAD_REQUEST)

        draft.selected_supply_warehouse = selected
        draft.save(update_fields=["selected_supply_warehouse", "updated_at"])

        return Response({
            "draft_id": draft.id,
            "selected_supply_warehouse": draft.selected_supply_warehouse,
        }, status=status.HTTP_200_OK)


class SupplyDraftDeleteView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def delete(self, request, draft_id: int):
        draft = get_object_or_404(
            OzonSupplyDraft,
            id=draft_id,
            store__in=user_store_queryset(request.user),
        )

        batch = draft.batch
        draft.delete()

        # если в батче не осталось черновиков — удаляем батч
        if not batch.drafts.exists():
            batch.delete()
            return Response({"deleted": True, "batch_deleted": True}, status=status.HTTP_200_OK)

        return Response({"deleted": True, "batch_deleted": False}, status=status.HTTP_200_OK)


class SupplyDraftTimeslotFetchView(APIView):
    permission_classes = [permissions.IsAuthenticated]
    OZON_TIMESLOT_URL = "https://api-seller.ozon.ru/v1/draft/timeslot/info"

    @staticmethod
    def _normalize(draft: OzonSupplyDraft):
        data = draft.supply_warehouse or []
        flat = []
        for entry in data:
            if isinstance(entry, dict) and entry.get("warehouse_id"):
                flat.append(entry)
            elif isinstance(entry, dict) and "warehouses" in entry:
                for w in entry.get("warehouses", []):
                    sw = w.get("supply_warehouse") or {}
                    if sw.get("warehouse_id"):
                        flat.append({
                            **sw,
                            "status": w.get("status"),
                            "bundle_ids": w.get("bundle_ids"),
                            "travel_time_days": w.get("travel_time_days"),
                        })
        if flat:
            draft.supply_warehouse = flat
            if not draft.selected_supply_warehouse:
                draft.selected_supply_warehouse = flat[0]
            draft.save(update_fields=["supply_warehouse", "selected_supply_warehouse", "updated_at"])
        return flat

    def post(self, request):
        batch_id = request.data.get("batch_id")
        date_from = request.data.get("date_from")
        days = request.data.get("days")
        if not batch_id or not date_from or days is None:
            return Response({"error": "batch_id, date_from, days are required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            days = int(days)
        except (TypeError, ValueError):
            return Response({"error": "days must be integer"}, status=status.HTTP_400_BAD_REQUEST)

        batch = get_object_or_404(
            OzonSupplyBatch,
            batch_id=batch_id,
            store__in=user_store_queryset(request.user),
        )

        try:
            dt_from = datetime.fromisoformat(str(date_from).replace("Z", "+00:00"))
        except Exception:
            return Response({"error": "Invalid date_from format"}, status=status.HTTP_400_BAD_REQUEST)

        dt_to = dt_from + timedelta(days=days)
        date_from_iso = dt_from.isoformat().replace("+00:00", "Z")
        date_to_iso = dt_to.isoformat().replace("+00:00", "Z")

        results = []
        errors = []

        for draft in batch.drafts.all():
            if not draft.draft_id:
                errors.append({"draft_id": draft.id, "error": "draft_id missing (info not loaded)"})
                continue

            warehouses = self._normalize(draft)
            warehouse = draft.selected_supply_warehouse or (warehouses[0] if warehouses else None)
            if not warehouse:
                errors.append({"draft_id": draft.id, "error": "No warehouse selected"})
                continue
            warehouse_id = warehouse.get("warehouse_id") or warehouse.get("id")
            if not warehouse_id:
                errors.append({"draft_id": draft.id, "error": "warehouse_id missing"})
                continue

            payload = {
                "date_from": date_from_iso,
                "date_to": date_to_iso,
                "draft_id": draft.draft_id,
                "warehouse_ids": [str(warehouse_id)],
            }
            headers = {
                "Client-Id": draft.store.client_id,
                "Api-Key": draft.store.api_key,
                "Content-Type": "application/json",
            }
            try:
                resp = requests.post(self.OZON_TIMESLOT_URL, headers=headers, json=payload, timeout=30)
            except requests.RequestException as exc:
                errors.append({"draft_id": draft.id, "error": f"Request error: {exc}"})
                continue

            try:
                resp_data = resp.json()
            except ValueError:
                resp_data = {"raw": resp.text}

            if resp.status_code >= 400:
                errors.append({"draft_id": draft.id, "status_code": resp.status_code, "error": resp.text})
                continue

            draft.timeslot_response = resp_data
            draft.timeslot_updated_at = timezone.now()
            draft.save(update_fields=["timeslot_response", "timeslot_updated_at", "updated_at"])
            results.append({"draft_id": draft.id, "timeslot_response": resp_data})

        status_code = status.HTTP_207_MULTI_STATUS if errors and results else status.HTTP_200_OK
        if errors and not results:
            status_code = status.HTTP_400_BAD_REQUEST
        return Response({"results": results, "errors": errors}, status=status_code)


class SupplyDraftTimeslotListView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, batch_id: str):
        batch = get_object_or_404(
            OzonSupplyBatch,
            batch_id=batch_id,
            store__in=user_store_queryset(request.user),
        )

        drafts = batch.drafts.all()
        common_dates = None
        common_timeslots = None

        def extract_dates(ts):
            dates = set()
            for entry in ts.get("drop_off_warehouse_timeslots", []):
                for day in entry.get("days", []):
                    date_str = day.get("date_in_timezone")
                    if date_str:
                        dates.add(date_str[:10])
            return dates

        def get_wh_meta(wid, draft):
            # пробуем найти метаданные склада из выбранного/списка складов черновика
            sources = []
            if draft.selected_supply_warehouse:
                sources.append(draft.selected_supply_warehouse)
            if draft.supply_warehouse:
                sources.extend(draft.supply_warehouse)
            for w in sources:
                if str(w.get("warehouse_id") or w.get("id")) == str(wid):
                    return {
                        "warehouse_id": w.get("warehouse_id") or w.get("id"),
                        "warehouse_name": w.get("name"),
                        "warehouse_address": w.get("address"),
                    }
            return {"warehouse_id": wid, "warehouse_name": None, "warehouse_address": None}

        def build_timeslots_by_wh(ts, draft):
            slots_by_warehouse = {}
            for entry in ts.get("drop_off_warehouse_timeslots", []):
                wid = entry.get("drop_off_warehouse_id")
                tz = entry.get("warehouse_timezone")
                wh_meta = slots_by_warehouse.get(wid) or {
                    **get_wh_meta(wid, draft),
                    "warehouse_timezone": tz,
                    "dates": {},
                }
                for day in entry.get("days", []):
                    date_str = (day.get("date_in_timezone") or "")[:10]
                    date_bucket = wh_meta["dates"].get(date_str) or []
                    for slot in day.get("timeslots", []):
                        date_bucket.append({
                            "from": slot.get("from_in_timezone"),
                            "to": slot.get("to_in_timezone"),
                        })
                    wh_meta["dates"][date_str] = date_bucket
                slots_by_warehouse[wid] = wh_meta
            grouped = []
            for wid, meta in slots_by_warehouse.items():
                grouped.append({
                    "warehouse_id": meta.get("warehouse_id"),
                    "warehouse_name": meta.get("warehouse_name"),
                    "warehouse_address": meta.get("warehouse_address"),
                    "warehouse_timezone": meta.get("warehouse_timezone"),
                    "dates": [
                        {"date": date, "timeslots": meta["dates"][date]}
                        for date in sorted(meta["dates"])
                    ],
                })
            return grouped

        drafts_data = []
        for d in drafts:
            grouped_slots = []
            dates = set()
            if d.timeslot_response:
                dates = extract_dates(d.timeslot_response)
                grouped_slots = build_timeslots_by_wh(d.timeslot_response, d)
                if common_dates is None:
                    common_dates = dates
                else:
                    common_dates = common_dates & dates
                # пересечение временных интервалов по датам
                per_draft_slots = {}
                for wh in grouped_slots:
                    for day in wh.get("dates", []):
                        key = day["date"]
                        per_draft_slots.setdefault(key, set())
                        for slot in day.get("timeslots", []):
                            per_draft_slots[key].add((slot.get("from"), slot.get("to")))
                if common_timeslots is None:
                    # преобразуем в dict date -> set of tuples
                    common_timeslots = {k: set(v) for k, v in per_draft_slots.items()}
                else:
                    for date_key in list(common_timeslots.keys()):
                        if date_key in per_draft_slots:
                            common_timeslots[date_key] = common_timeslots[date_key] & per_draft_slots[date_key]
                        else:
                            common_timeslots[date_key] = set()

            drafts_data.append({
                "draft_id": d.id,
                "timeslot_response": d.timeslot_response,
                "selected_supply_warehouse": d.selected_supply_warehouse,
                "selected_timeslot": d.selected_timeslot,
                "timeslot_updated_at": d.timeslot_updated_at,
                "timeslots_by_warehouse": grouped_slots,
            })

        common_slots_serialized = []
        if common_timeslots:
            for date_key, slots in common_timeslots.items():
                if slots:
                    common_slots_serialized.append({
                        "date": date_key,
                        "timeslots": [{"from": f, "to": t} for f, t in sorted(slots)],
                    })

        return Response({
            "batch_id": batch_id,
            "drafts": drafts_data,
            "common_dates": sorted(list(common_dates or [])),
            "common_timeslots": common_slots_serialized,
        }, status=status.HTTP_200_OK)


class SupplyDraftSupplyStatusView(APIView):
    """
    Загружает данные по созданным заявкам (status=created) и возвращает товары поставки.
    """

    permission_classes = [permissions.IsAuthenticated]
    OZON_SUPPLY_STATUS_URL = "https://api-seller.ozon.ru/v1/draft/supply/create/status"
    OZON_SUPPLY_GET_URL = "https://api-seller.ozon.ru/v3/supply-order/get"
    OZON_SUPPLY_BUNDLE_URL = "https://api-seller.ozon.ru/v1/supply-order/bundle"

    def _call(self, url, headers, payload):
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        try:
            data = resp.json()
        except ValueError:
            data = {"raw": resp.text}
        return resp, data

    def get(self, request, batch_id: str):
        batch = get_object_or_404(
            OzonSupplyBatch,
            batch_id=batch_id,
            store__in=user_store_queryset(request.user),
        )

        results = []
        errors = []

        for draft in batch.drafts.all():
            if draft.status != "created" or not draft.operation_id_supply:
                continue

            headers = {
                "Client-Id": draft.store.client_id,
                "Api-Key": draft.store.api_key,
                "Content-Type": "application/json",
            }

            # 1) статус создания заявки
            status_payload = {"operation_id": draft.operation_id_supply}
            resp, status_data = self._call(self.OZON_SUPPLY_STATUS_URL, headers, status_payload)
            if resp.status_code >= 400:
                errors.append({"draft_id": draft.id, "error": status_data, "status_code": resp.status_code})
                continue
            order_ids = (status_data.get("result") or {}).get("order_ids") or []
            if not order_ids:
                errors.append({"draft_id": draft.id, "error": "order_ids empty"})
                continue

            # 2) детали заказов
            get_payload = {"order_ids": order_ids}
            resp, orders_data = self._call(self.OZON_SUPPLY_GET_URL, headers, get_payload)
            if resp.status_code >= 400:
                errors.append({"draft_id": draft.id, "error": orders_data, "status_code": resp.status_code})
                continue
            orders = orders_data.get("orders") or []

            # Собираем bundle_ids и склады
            bundle_ids = []
            storage_ids = set()
            dropoff_id = None
            for order in orders:
                drop = order.get("drop_off_warehouse") or {}
                if drop.get("warehouse_id"):
                    dropoff_id = drop.get("warehouse_id")
                for supply in order.get("supplies") or []:
                    if supply.get("bundle_id"):
                        bundle_ids.append(supply["bundle_id"])
                    storage = (supply.get("storage_warehouse") or {}).get("warehouse_id")
                    if storage:
                        storage_ids.add(str(storage))

            bundle_items = []
            if bundle_ids:
                bundle_payload = {
                    "bundle_ids": bundle_ids,
                    "is_asc": True,
                    "item_tags_calculation": {
                        "dropoff_warehouse_id": dropoff_id or 0,
                        "storage_warehouse_ids": list(storage_ids) if storage_ids else [],
                    },
                    "limit": 100,
                    "sort_field": "UNSPECIFIED",
                }
                resp, bundle_data = self._call(self.OZON_SUPPLY_BUNDLE_URL, headers, bundle_payload)
                if resp.status_code >= 400:
                    errors.append({"draft_id": draft.id, "error": bundle_data, "status_code": resp.status_code})
                else:
                    for item in bundle_data.get("items") or []:
                        bundle_items.append(
                            {
                                "sku": item.get("sku"),
                                "quantity": item.get("quantity"),
                                "offer_id": item.get("offer_id"),
                                "icon_path": item.get("icon_path"),
                                "name": item.get("name"),
                                "barcode": item.get("barcode"),
                                "product_id": item.get("product_id"),
                            }
                        )

            draft.supply_order_ids = order_ids
            draft.supply_order_response = orders_data
            draft.supply_bundle_items = bundle_items
            draft.save(update_fields=["supply_order_ids", "supply_order_response", "supply_bundle_items", "updated_at"])

            results.append(
                {
                    "draft_id": draft.id,
                    "order_ids": order_ids,
                    "orders": orders,
                    "order_states": [o.get("state") for o in orders if isinstance(o, dict)],
                    "bundle_items": bundle_items,
                }
            )

        status_code = status.HTTP_207_MULTI_STATUS if errors and results else status.HTTP_200_OK
        if errors and not results:
            status_code = status.HTTP_400_BAD_REQUEST
        return Response({"results": results, "errors": errors}, status=status_code)


class SupplyDraftCreateSupplyView(APIView):
    """
    Создает финальную заявку на поставку по всем черновикам батча с указанным тайм-слотом.
    """

    permission_classes = [permissions.IsAuthenticated]
    OZON_SUPPLY_CREATE_URL = "https://api-seller.ozon.ru/v1/draft/supply/create"

    @staticmethod
    def _normalize_warehouses(draft: OzonSupplyDraft):
        data = draft.supply_warehouse or []
        flat = []
        for entry in data:
            if isinstance(entry, dict) and entry.get("warehouse_id"):
                flat.append(entry)
            elif isinstance(entry, dict) and "warehouses" in entry:
                for w in entry.get("warehouses", []):
                    sw = w.get("supply_warehouse") or {}
                    if sw.get("warehouse_id"):
                        flat.append({
                            **sw,
                            "status": w.get("status"),
                            "bundle_ids": w.get("bundle_ids"),
                            "travel_time_days": w.get("travel_time_days"),
                        })
        if flat:
            draft.supply_warehouse = flat
            if not draft.selected_supply_warehouse:
                draft.selected_supply_warehouse = flat[0]
            draft.save(update_fields=["supply_warehouse", "selected_supply_warehouse", "updated_at"])
        return flat

    def post(self, request, batch_id: str):
        timeslot = request.data.get("timeslot") or {}
        from_ts = timeslot.get("from_in_timezone")
        to_ts = timeslot.get("to_in_timezone")
        if not (from_ts and to_ts):
            return Response({"error": "timeslot.from_in_timezone and timeslot.to_in_timezone required"}, status=status.HTTP_400_BAD_REQUEST)

        batch = get_object_or_404(
            OzonSupplyBatch,
            batch_id=batch_id,
            store__in=user_store_queryset(request.user),
        )

        results = []
        errors = []

        for draft in batch.drafts.all():
            if not draft.draft_id:
                errors.append({"draft_id": draft.id, "error": "draft_id missing (info not loaded)"})
                continue

            warehouses = self._normalize_warehouses(draft)
            selected = draft.selected_supply_warehouse or (warehouses[0] if warehouses else None)
            if not selected:
                errors.append({"draft_id": draft.id, "error": "No warehouse selected"})
                continue
            warehouse_id = selected.get("warehouse_id") or selected.get("id")
            if not warehouse_id:
                errors.append({"draft_id": draft.id, "error": "warehouse_id missing"})
                continue

            payload = {
                "draft_id": draft.draft_id,
                "timeslot": timeslot,
                "warehouse_id": int(warehouse_id),
            }
            headers = {
                "Client-Id": draft.store.client_id,
                "Api-Key": draft.store.api_key,
                "Content-Type": "application/json",
            }
            try:
                resp = requests.post(self.OZON_SUPPLY_CREATE_URL, headers=headers, json=payload, timeout=30)
            except requests.RequestException as exc:
                errors.append({"draft_id": draft.id, "error": f"Request error: {exc}"})
                continue

            try:
                resp_data = resp.json()
            except ValueError:
                resp_data = {"raw": resp.text}

            if resp.status_code >= 400:
                errors.append({"draft_id": draft.id, "status_code": resp.status_code, "error": resp.text})
                continue

            operation_id_supply = resp_data.get("operation_id")
            draft.operation_id_supply = operation_id_supply or ""
            draft.selected_timeslot = timeslot
            draft.status = "created"
            draft.save(update_fields=["operation_id_supply", "selected_timeslot", "status", "updated_at"])

            results.append({
                "draft_id": draft.id,
                "operation_id_supply": operation_id_supply,
                "warehouse_id": warehouse_id,
                "timeslot": timeslot,
            })

        if results and not errors:
            batch.status = "completed"
        elif results and errors:
            batch.status = "partial"
        batch.save(update_fields=["status", "updated_at"])

        status_code = status.HTTP_207_MULTI_STATUS if errors and results else status.HTTP_200_OK
        if errors and not results:
            status_code = status.HTTP_400_BAD_REQUEST
        return Response({"results": results, "errors": errors}, status=status_code)

# Получение конечной аналитики по продуктам


# Получение конечной аналитики по продуктам версия 2
class ProductAnalytics_V2_View(APIView):
    def post(self, request):
        logging.info("Headers: %s", request.headers)
        logging.info("Query: %s", request.query_params)
        logging.info("Body: %s", request.data)        
        start_time = time.time()
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        days = int(request.data.get("days", 30))
        sort_by_qty = request.data.get("sort_by_qty", 1)
        b7 = request.data.get("b7", 0)
        f9 = request.data.get("f9", 0)
        period_analiz = request.data.get("period_analiz", 1)
        price_min = request.data.get("price_min", 0)
        price_max = request.data.get("price_max", 1000000)
        f6 = float(request.data.get("f6")) if request.data.get("f6") not in [None, ""] else None
        g6 = float(request.data.get("g6")) if request.data.get("g6") not in [None, ""] else None

        f7 = request.data.get("f7", 0)
        f10 = float(request.data.get("f10")) if request.data.get("f10") not in [None, ""] else None
        exclude_offer_ids = request.data.get("exclude_offer_ids", [])
        mandatory_products = request.data.get("mandatory_products", [])

        logging.info(f"days == {days} and sort_by_qty == {sort_by_qty} and b7 == {b7} and f9 == {f9} and period_analiz == {period_analiz} and price_min == {price_min} and price_max == {price_max} and f6 == {f6} and g6 == {g6} and f10 == {f10} and f7 = {f7} and exclude_offer_ids = {exclude_offer_ids} and mandatory_products = {mandatory_products}")
        if days > 60 or days < 0:
            return Response({"error": "Период анализа должен быть от 0 до 60 дней"}, status=400)
        if price_max < price_min:
            return Response({"error": "Минимальная цена не может быть больше максимальной"}, status=400)
        try:
            ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=403)

        since_date = timezone.now() - timedelta(days=days-1)
        since_date = since_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # Все товары
        products = Product.objects.filter(
            store=ozon_store,
            price__gte=price_min,
            price__lte=price_max
        )
        
        # Исключаем товары по артикулам, если указаны
        if exclude_offer_ids:
            products = products.exclude(offer_id__in=exclude_offer_ids)
        products_by_sku = {p.sku: p for p in products}
        # logging.info(f" Target SKU ={products_by_sku.get(2909660721)}")
        # Создаем словарь для получения barcode по offer_id
        offer_id_to_barcode = {p.offer_id: p.barcodes[0] if p.barcodes else None for p in products}
        
        

        # Продажи
        logging.info(f"Дата до которой смотрим {since_date}")
        sales = Sale.objects.filter(store=ozon_store, date__gte=since_date, sale_type__in=[Sale.FBO, Sale.FBS])
        sales_by_cluster = {}
        logging.info(f"Кол-во продаж {len(sales)}")
        for s in sales:
            cluster = s.cluster_to or "Без кластера"
            sales_by_cluster.setdefault(cluster, {})
            sales_by_cluster[cluster].setdefault(s.sku, {"qty": 0, "price": 0})
            sales_by_cluster[cluster][s.sku]["qty"] += s.quantity
            sales_by_cluster[cluster][s.sku]["price"] += float(s.price)*s.quantity

        logging.info(f"Кол-во кластеров  {len(sales_by_cluster)}")
        
        # Посчитаем количество продаж по каждой позиции SKU и получим по каждому товару количество продаж
        product_revenue_map_qty = {}  
        for cluster in sales_by_cluster:
            for sku, data in sales_by_cluster[cluster].items():
                product_revenue_map_qty.setdefault(sku, 0)
                product_revenue_map_qty[sku] += data["qty"]
                
        logging.info(f"Количество уникальных SKU product_revenue_map_qty =  {len(product_revenue_map_qty)}")  
              
        # Остатки товаров по складам
        stocks = WarehouseStock.objects.filter(store=ozon_store)
        stocks_by_cluster = {}
        total_stock_all_clusters = {}
        requested_stock_by_sku = {}  # Отдельный расчет товаров в заявках на поставку
        logging.info(f"Остатки товаров по складам  {len(stocks)}")
        
        for stock in stocks:
            cluster = stock.cluster_name or "Без кластера"
            stock_sum = (
                stock.available_stock_count +
                stock.valid_stock_count +
                stock.waiting_docs_stock_count +
                stock.expiring_stock_count +
                stock.transit_defect_stock_count +
                stock.stock_defect_stock_count +
                stock.excess_stock_count +
                stock.other_stock_count +
                stock.requested_stock_count +
                stock.transit_stock_count +
                stock.return_from_customer_stock_count
            )
            
            # По кластеру
            if cluster not in stocks_by_cluster:
                stocks_by_cluster[cluster] = {}
            if stock.sku not in stocks_by_cluster[cluster]:
                stocks_by_cluster[cluster][stock.sku] = 0
            stocks_by_cluster[cluster][stock.sku] += stock_sum

            # По всем кластерам
            if stock.sku not in total_stock_all_clusters:
                total_stock_all_clusters[stock.sku] = 0
            total_stock_all_clusters[stock.sku] += stock_sum
            
            # Отдельный расчет товаров в заявках на поставку по SKU
            if stock.sku not in requested_stock_by_sku:
                requested_stock_by_sku[stock.sku] = 0
            requested_stock_by_sku[stock.sku] += stock.requested_stock_count
            
        # logging.info(f"Заявки на поставку по SKU 1928741963 {requested_stock_by_sku[1928741963]}")
        # FBS остатки
        fbs_stocks = FbsStock.objects.filter(store=ozon_store)
        fbs_by_sku = {}
        for f in fbs_stocks:
            fbs_by_sku.setdefault(f.sku, 0)
            fbs_by_sku[f.sku] += f.present



        # 1. Подсчёт выручки по всем кластерам и всем товарам
        total_revenue = 0
        revenue_by_cluster = {}
        product_revenue_map = {}

        for cluster in sales_by_cluster:
            for sku, data in sales_by_cluster[cluster].items():
                revenue_by_cluster.setdefault(cluster, 0)
                revenue_by_cluster[cluster] += data["price"]

                product_revenue_map.setdefault(sku, 0)
                product_revenue_map[sku] += data["price"]

        total_revenue = sum(revenue_by_cluster.values()) or 1  # защита от деления на 0

        
        # Получаем данные по кластерам доставки average_delivery_time impact_share
        delivery_cluster_data = {
            dc.name: {
                "average_delivery_time": dc.average_delivery_time,
                "impact_share": dc.impact_share
            }
            for dc in DeliveryCluster.objects.filter(store=ozon_store)
        }

        
        # Получаем аналитику по товарам в кластерах доставки
        # ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ
        item_analytics_map = {
            (a.cluster_name, a.sku): a
            for a in DeliveryClusterItemAnalytics.objects.filter(store=ozon_store)
        }
        
        # 2. Финальная сборка по кластерам
        all_clusters = set(sales_by_cluster) | set(stocks_by_cluster)

        cluster_list = []
        # len(all_clusters)
        logging.info(f"Total len clusters = {len(all_clusters)}")
        offer_delivery_totals = {}
        for cluster in all_clusters:
            cluster_data = {
                "cluster_name": cluster,
                "cluster_revenue": round(revenue_by_cluster.get(cluster, 0), 2),
                "cluster_share_percent": round((revenue_by_cluster.get(cluster, 0) / total_revenue) * 100, 2),
                "products": []
            }
            all_skus = set()
            if cluster in sales_by_cluster:
                all_skus |= set(sales_by_cluster[cluster])
            if cluster in stocks_by_cluster:
                all_skus |= set(stocks_by_cluster[cluster])

            for sku in all_skus:
                
                product = products_by_sku.get(sku)
                
                if not product:
                    # logging.info(f"434 строчка если нет product продолжаем")
                    continue
                
                # ОБЩАЯ АНАЛИТИКА ПО КЛАСТЕРУ (/v1/analytics/average-delivery-time) - это значит, что
                # на все артикулы в данном кластере цифры должны быть одинаковые
                # После столбца M(кластер) необходимо добавить следующие столбцы:
                # 1. N - Ср. время доставки до покупателя - туда вставить данные общие из параметра
                # average_delivery_time (там 2 разных приходит, надо с вами потестить)
                # 2. O - Доля влияния, %, туда вставить параметр impact_share
                delivery_info = delivery_cluster_data.get(cluster, {"average_delivery_time": 0, "impact_share": 0})

                # ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ (/v1/analytics/average-delivery-time/details) - это
                # значит, что получаем цифры по каждому товару уникальные
                # 1. P - Ср. время доставки до покупателя ТОВАР, ч - average_delivery_time (тоже там два
                # разных надо потестить)
                # 2. Q - Доля влияния на ТОВАР, % - туда вставить параметр impact_share (
                # 3. R - Рекомендации к поставке, шт - туда параметр recommended_supply
                item_analytics = item_analytics_map.get((cluster, sku))


                mandatory_quantity = get_mandatory_quantity_for_product(product.offer_id, mandatory_products) if mandatory_products else None
                
                sales_qty = sales_by_cluster.get(cluster, {}).get(sku, {}).get("qty", 0)
                sales_price = sales_by_cluster.get(cluster, {}).get(sku, {}).get("price", 0)
                stock_qty_cluster = stocks_by_cluster.get(cluster, {}).get(sku, 0)
                total_stock_qty = total_stock_all_clusters.get(sku, 0)

                #    stock_total_cluster/
                avg_daily_sales_total = round(product_revenue_map_qty.get(sku, 0) / days, 2) if days else 0
                fbo_total_stock = total_stock_all_clusters.get(sku, 0)
                oborachivaemost = round((fbo_total_stock / avg_daily_sales_total),2) if avg_daily_sales_total else 0
                
                
                if g6 is not None and oborachivaemost > g6:
                    # logging.info(f"g6 is not None and oborachivaemost > g6")
                    continue
                if f6 is not None and oborachivaemost < f6:
                    # logging.info(f"g6 is not None and oborachivaemost < g6")
                    continue
                
                # if sku == 653610923:
                #     logging.info(f"SKU == {sku}")
                #     logging.info(f"Оборачиваемость == {oborachivaemost}")
                #     logging.info(f"Name == {product.offer_id}")
                    
                if f10 is not None and f10 > float(total_stock_qty) :
                    oborachivaemost = 0
                    # logging.info(f"479 | if f10 is not None and f10 > float(total_stock_qty)")
                
                total_sum_sku_all_claster = product_revenue_map.get(sku, 0) #Сумма выручки по всем кластерам для данного SKU
                share_of_total_daily_average = sales_price / total_sum_sku_all_claster if total_sum_sku_all_claster else 0
                
                if b7 == 1 and share_of_total_daily_average >= f9:
                    # K15*B5*R15-S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * share_of_total_daily_average - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * share_of_total_daily_average
                elif b7 == 1 and share_of_total_daily_average < f9:
                    # K15*B5*F9-S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * f9 - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * f9
                
                elif b7 == 0 or b7 == None:
                    # K15*B5/на все кластера - S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz / len(all_clusters)  - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz / len(all_clusters)
                    # if round(for_delivery) > 0:
                    #     logging.info(f"{sku} | for delyvery = {for_delivery}")
                
                elif b7 == 2:
                    for_delivery = (item_analytics.recommended_supply if item_analytics else 0) - stock_qty_cluster
                    need_goods = (item_analytics.recommended_supply if item_analytics else 0)
                    
                    # if sku == 353866151:
                    #     logging.info(f"b7 == {b7} item_analytics.recommended_supply == {item_analytics.recommended_supply if item_analytics else 0} and stock_qty_cluster == {stock_qty_cluster}")
                    
                else:
                    
                    for_delivery = 0
                    need_goods = 0
                    # logging.info(f"b7 == {b7} and share_of_total_daily_average == {share_of_total_daily_average} and f9 == {f9
                    # logging.info(f"b7 == {b7} and share_of_total_daily_average == {share_of_total_daily_average} and f9 == {f9}")
                    # logging.info(product_revenue_map)   
                

                
                #если в ячейке F7 стоит 1, то показываются ВСЕ
                #товары, если пусто (по дефолту), то только те, которые имеют значение >0 в столбце T15.
                if f7 == 0 and round(for_delivery) <= 0 and mandatory_quantity is None:
                    continue
                
                
                offer_delivery_totals.setdefault(product.offer_id, 0)
                offer_delivery_totals[product.offer_id] += round(for_delivery)
                # if sku == 662496696:
                #     logging.info(f"Claster name == {cluster}")
                #     logging.info(f"Product == {product}")
                #     logging.info(f"share_of_total_daily_average == {share_of_total_daily_average}  == {sales_price} / {total_sum_sku_all_claster}")
                cluster_data["products"].append({
                    "sku": sku,
                    "name": product.name,
                    "offer_id": product.offer_id,
                    "photo": product.primary_image,
                    "category": product.category,
                    "type_name": product.type_name,
                    "price": float(product.price or 0),
                    "barcodes": product.barcodes,
                    "ozon_link": f"https://www.ozon.ru/product/{product.sku}/",
                    "sales_total_fbo_fbs":product_revenue_map_qty.get(sku, 0),
                    "payout_total": round(sales_price, 2),
                    "avg_daily_sales_fbo_fbs": round(product_revenue_map_qty.get(sku, 0) / days, 2) if days else 0,
                    "stock_total_cluster": stock_qty_cluster,
                    "fbs_stock_total_qty": fbs_by_sku.get(sku, 0),
                    "product_total_revenue_fbo_fbs": round(product_revenue_map.get(sku, 0), 2),
                    "avg_daily_sales_cluster_qty" : round(sales_qty / days, 2) if days else 0,
                    "avg_daily_sales_cluster_rub" : round(sales_price / days, 2) if days else 0,
                    "oborachivaemost": oborachivaemost,
                    "share_of_total_daily_average": share_of_total_daily_average,                    
                    "sales_qty_cluster": sales_qty,
                    "for_delivery" : round(for_delivery),
                    "need_goods" : need_goods,
                    "average_delivery_time": delivery_info["average_delivery_time"],
                    "impact_share": delivery_info["impact_share"],
                    "average_delivery_time_item": item_analytics.average_delivery_time if item_analytics else "",
                    "impact_share_item": item_analytics.impact_share if item_analytics else "",
                    "recommended_supply_item": item_analytics.recommended_supply if item_analytics else "",

                    
                    
                })
                
            
            if sort_by_qty == 1:
                # сортировка товаров по количеству продаж FBO+FBS
                cluster_data["products"].sort(key=lambda x: x["sales_total_fbo_fbs"], reverse=True)
                

            # сортировка товаров по выручке FBO+FBS
            if sort_by_qty == 2:
                cluster_data["products"].sort(key=lambda x: x["product_total_revenue_fbo_fbs"], reverse=True)

            if sort_by_qty == 3:
                cluster_data["products"].sort(key=lambda x: x.get("recommended_supply_item") or 0,reverse=True
)
                
            cluster_list.append(cluster_data)
            summary = [
                    {
                        "offer_id": offer_id,
                        "barcode": offer_id_to_barcode.get(offer_id),
                        "total_for_delivery": qty
                    } for offer_id, qty in offer_delivery_totals.items()
                ]

        # Пересчет for_delivery для обязательных товаров после всех основных расчетов
        if mandatory_products:
            # Сначала собираем информацию о том, в каких кластерах есть обязательные товары
            mandatory_clusters = {}
            for cluster_data in cluster_list:
                cluster_name = cluster_data["cluster_name"]
                for product_data in cluster_data["products"]:
                    offer_id = product_data["offer_id"]
                    mandatory_quantity = get_mandatory_quantity_for_product(offer_id, mandatory_products)
                    if mandatory_quantity is not None:
                        if offer_id not in mandatory_clusters:
                            mandatory_clusters[offer_id] = []
                        mandatory_clusters[offer_id].append(cluster_name)
            
            # Рассчитываем веса кластеров
            cluster_weights = {}
            for cluster, revenue in revenue_by_cluster.items():
                if total_revenue > 0:
                    cluster_weights[cluster] = revenue / total_revenue
                else:
                    cluster_weights[cluster] = 0
            
            # Проходим по всем кластерам и товарам
            for cluster_data in cluster_list:
                cluster_name = cluster_data["cluster_name"]
                
                for product_data in cluster_data["products"]:
                    offer_id = product_data["offer_id"]
                    sku = product_data["sku"]
                    
                    # Проверяем, является ли товар обязательным
                    mandatory_quantity = get_mandatory_quantity_for_product(offer_id, mandatory_products)
                    if mandatory_quantity is not None:
                        # Получаем общий остаток FBO для товара по всем кластерам
                        total_fbo_stock = total_stock_all_clusters.get(sku, 0)
                        
                        
                        # Если сумма остатков меньше обязательного количества
                        if total_fbo_stock < mandatory_quantity:
                            # Рассчитываем разницу - сколько нужно распределить
                            needed_quantity = mandatory_quantity - total_fbo_stock
                            
                            # Получаем кластеры, где есть этот товар
                            clusters_for_product = mandatory_clusters.get(offer_id, [])
                            
                            if clusters_for_product:
                                # Рассчитываем общий вес кластеров для этого товара
                                total_weight_for_product = sum(cluster_weights.get(cluster, 0) for cluster in clusters_for_product)
                                
                                if total_weight_for_product > 0:
                                    # Используем пропорциональное распределение
                                    cluster_weight = cluster_weights.get(cluster_name, 0)
                                    cluster_quantity = round(needed_quantity * (cluster_weight / total_weight_for_product))
                                else:
                                    # Если нет выручки, распределяем равномерно
                                    cluster_quantity = round(needed_quantity / len(clusters_for_product))
                            else:
                                cluster_quantity = 0
                            
                            
                            # Устанавливаем for_delivery равным cluster_quantity
                            product_data["for_delivery"] = cluster_quantity
                            
                            # Обновляем общую сумму для summary
                            offer_delivery_totals[offer_id] = sum(
                                p["for_delivery"] for cluster in cluster_list 
                                for p in cluster["products"] 
                                if p["offer_id"] == offer_id
                            )
            
            # Обновляем summary с новыми значениями
            summary = [
                {
                    "offer_id": offer_id,
                    "barcode": offer_id_to_barcode.get(offer_id),
                    "total_for_delivery": qty
                } for offer_id, qty in offer_delivery_totals.items()
            ]

        # сортировка кластеров по выручке
        cluster_list.sort(key=lambda c: c["cluster_revenue"], reverse=True)
        summary.sort(key=lambda c: c["total_for_delivery"], reverse=True)

        # Сводная аналитика доставки
        try:
            average_time = DeliveryAnalyticsSummary.objects.get(store=ozon_store).average_delivery_time
        except DeliveryAnalyticsSummary.DoesNotExist:
            average_time = None        
        execution_time = round(time.time() - start_time, 3)
        logging.info(f"[⏱] Время выполнения запроса: {execution_time}s")
        resp = Response({
            "clusters": cluster_list,
            "summary": summary,
            "execution_time_seconds": execution_time,
            "average_delivery_time": average_time,
        })
        resp["X-Execution-Time-s"] = f"{execution_time:.3f}"
        return resp





def get_mandatory_quantity_for_product(offer_id, mandatory_products):
    """
    Получает обязательное количество для товара по артикулу
    """
    for product in mandatory_products:
        if product["offer_id"] == offer_id:
            return product["quantity"]
    return None





# Энд поинт для фронта        
class ProductAnalyticsByItemView(APIView):
    def post(self, request):
        start_time = time.time()
        api_key = request.data.get("Api-Key")
        client_id = request.data.get("client_id")
        days = int(request.data.get("days", 30))

        if days > 60 or days < 0:
            return Response({"error": "Период анализа должен быть от 0 до 60 дней"}, status=400)

        try:
            store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
        except OzonStore.DoesNotExist:
            return Response({"error": "Invalid Api-Key"}, status=403)

        since_date = timezone.now() - timedelta(days=days-1)
        since_date = since_date.replace(hour=0, minute=0, second=0, microsecond=0)

        products = Product.objects.filter(store=store)
        products_by_sku = {p.sku: p for p in products}

        sales = Sale.objects.filter(store=store, date__gte=since_date, sale_type__in=[Sale.FBO, Sale.FBS])
        sales_by_sku_cluster = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "price": 0}))
        revenue_by_cluster = defaultdict(float)

        for s in sales:
            cluster = s.cluster_to or "Без кластера"
            sales_by_sku_cluster[s.sku][cluster]["qty"] += s.quantity
            sales_by_sku_cluster[s.sku][cluster]["price"] += float(s.price) * s.quantity
            revenue_by_cluster[cluster] += float(s.price) * s.quantity

        stocks = WarehouseStock.objects.filter(store=store)
        stocks_by_sku_cluster = defaultdict(lambda: defaultdict(int))
        total_stock_by_sku = defaultdict(int)

        for stock in stocks:
            cluster = stock.cluster_name or "Без кластера"
            stock_sum = stock.available_stock_count + stock.transit_stock_count + stock.return_from_customer_stock_count
            stocks_by_sku_cluster[stock.sku][cluster] += stock_sum
            total_stock_by_sku[stock.sku] += stock_sum

        fbs_stocks = FbsStock.objects.filter(store=store)
        fbs_by_sku = defaultdict(int)
        for fbs in fbs_stocks:
            fbs_by_sku[fbs.sku] += fbs.present

        delivery_cluster_data = {
            dc.name: {
                "average_delivery_time": dc.average_delivery_time,
                "impact_share": dc.impact_share
            }
            for dc in DeliveryCluster.objects.filter(store=store)
        }

        item_analytics_map = {
            (a.cluster_name, a.sku): a
            for a in DeliveryClusterItemAnalytics.objects.filter(store=store)
        }

        total_revenue = sum(revenue_by_cluster.values()) or 1

        result_products = []

        for sku, product in products_by_sku.items():
            item = {
                "sku": sku,
                "name": product.name,
                "offer_id": product.offer_id,
                "photo": product.primary_image,
                "category": product.category,
                "type_name": product.type_name,
                "price": float(product.price or 0),
                "barcodes": product.barcodes,
                "ozon_link": f"https://www.ozon.ru/product/{product.sku}/",
                "fbs_stock_total_qty": fbs_by_sku.get(sku, 0),
                "stock_total": total_stock_by_sku.get(sku, 0),
                "sales_total_fbo_fbs": 0,
                "product_total_revenue_fbo_fbs": 0,
                "clusters": []
            }

            product_total_qty = 0
            product_total_revenue = 0

            clusters = set(sales_by_sku_cluster[sku].keys()) | set(stocks_by_sku_cluster[sku].keys())
            if not clusters:
                continue  # пропустить товар, у которого нет кластеров с данными

            for cluster in clusters:
                sales_data = sales_by_sku_cluster[sku].get(cluster, {"qty": 0, "price": 0})
                stock_qty = stocks_by_sku_cluster[sku].get(cluster, 0)
                delivery_info = delivery_cluster_data.get(cluster, {"average_delivery_time": 0, "impact_share": 0})
                item_analytics = item_analytics_map.get((cluster, sku))

                product_total_qty += sales_data["qty"]
                product_total_revenue += sales_data["price"]

                item["clusters"].append({
                    "cluster_name": cluster,
                    "sales_qty_cluster": sales_data["qty"],
                    "payout_total": round(sales_data["price"], 2),
                    "stock_total_cluster": stock_qty,
                    "avg_daily_sales_cluster_qty": round(sales_data["qty"] / days, 2) if days else 0,
                    "avg_daily_sales_cluster_rub": round(sales_data["price"] / days, 2) if days else 0,
                    "average_delivery_time": delivery_info["average_delivery_time"],
                    "impact_share": delivery_info["impact_share"],
                    "average_delivery_time_item": item_analytics.average_delivery_time if item_analytics else "",
                    "impact_share_item": item_analytics.impact_share if item_analytics else "",
                    "recommended_supply_item": item_analytics.recommended_supply if item_analytics else "",
                })

            item["sales_total_fbo_fbs"] = product_total_qty
            item["product_total_revenue_fbo_fbs"] = round(product_total_revenue, 2)
            item["avg_daily_sales_fbo_fbs"] = round(product_total_qty / days, 2) if days else 0
            result_products.append(item)

        cluster_summary = [
            {
                "cluster_name": name,
                "cluster_revenue": round(revenue, 2),
                "cluster_share_percent": round((revenue / total_revenue) * 100, 2)
            } for name, revenue in revenue_by_cluster.items()
        ]
        cluster_summary.sort(key=lambda c: c["cluster_revenue"], reverse=True)

        try:
            average_time = DeliveryAnalyticsSummary.objects.get(store=store).average_delivery_time
        except DeliveryAnalyticsSummary.DoesNotExist:
            average_time = None

        execution_time = round(time.time() - start_time, 3)
        return Response({
            "products": result_products,
            "clusters_summary": cluster_summary,
            "execution_time_seconds": execution_time,
            "average_delivery_time": average_time
        })


class TriggerUpdateABCSheetView(APIView):
    """Эндпоинт для ручного запуска таска обновления листа ABC в Google Sheets."""
    def post(self, request):
        spreadsheet_url = request.data.get("spreadsheet_url")
        sa_json_path = request.data.get("sa_json_path")
        sync_mode = request.data.get("sync", True)  # Синхронный режим по умолчанию

        if sync_mode:
            # Синхронное выполнение - дожидаемся завершения
            try:
                result = update_abc_sheet(spreadsheet_url=spreadsheet_url, sa_json_path=sa_json_path)
                return Response({
                    "status": "completed",
                    "message": "Обновление ABC листа завершено успешно"
                })
            except Exception as e:
                return Response({
                    "status": "error",
                    "error": str(e)
                }, status=500)
        else:
            # Асинхронное выполнение - запускаем в фоне
            async_result = update_abc_sheet.delay(spreadsheet_url=spreadsheet_url, sa_json_path=sa_json_path)
            return Response({
                "status": "accepted",
                "task_id": async_result.id,
                "message": "Задача обновления ABC листа запущена"
            })


class TriggerSyncCampaignActivityOverrideView(APIView):
    """Запускает синхронизацию активности кампаний с override_training=1 (игнор периода обучения)."""
    def post(self, request):
        spreadsheet_url = request.data.get("spreadsheet_url")
        sa_json_path = request.data.get("sa_json_path")
        worksheet_name = request.data.get("worksheet_name", "Main_ADV")
        start_row = int(request.data.get("start_row", 13))
        block_size = int(request.data.get("block_size", 100))
        async_mode = bool(request.data.get("async", False))

        kwargs = dict(
            spreadsheet_url=spreadsheet_url,
            sa_json_path=sa_json_path,
            worksheet_name=worksheet_name,
            start_row=start_row,
            block_size=block_size,
            override_training=1,
        )

        if async_mode:
            res = sync_campaign_activity_with_sheets.delay(**kwargs)
            return Response({
                "status": "accepted",
                "task_id": res.id,
                "message": "Синхронизация активности запущена (override_training=1)"
            })
        # sync mode
        try:
            result = sync_campaign_activity_with_sheets(**kwargs)
            return Response({
                "status": "completed",
                "result": result,
                "message": "Синхронизация активности выполнена (override_training=1)"
            })
        except Exception as e:
            return Response({
                "status": "error",
                "error": str(e)
            }, status=500)

class TriggerRebalanceAutoBudgetsView(APIView):
    """Ручной запуск перерасчёта недельных бюджетов авто-кампаний для конкретного магазина."""

    def post(self, request):
        store_name = (request.data.get("store_name") or "").strip()
        sa_json_path = request.data.get("sa_json_path")
        worksheet_name = request.data.get("worksheet_name", "Main_ADV")
        start_row_raw = request.data.get("start_row", 13)

        try:
            start_row = int(start_row_raw)
        except (TypeError, ValueError):
            return Response(
                {"error": "start_row must be an integer"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not store_name:
            return Response(
                {"error": "store_name is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        store = (
            OzonStore.objects.filter(name__iexact=store_name).first()
            or OzonStore.objects.filter(client_id__iexact=store_name).first()
            or OzonStore.objects.filter(name__icontains=store_name).first()
            or OzonStore.objects.filter(client_id__icontains=store_name).first()
        )
        if not store:
            return Response(
                {"error": f"store '{store_name}' not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        spreadsheet_url = (store.google_sheet_url or "").strip()
        if not spreadsheet_url:
            return Response(
                {"error": f"store '{store.name}' has no google_sheet_url configured"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            result = rebalance_auto_weekly_budgets(
                spreadsheet_url=spreadsheet_url,
                sa_json_path=sa_json_path,
                worksheet_name=worksheet_name,
                start_row=start_row,
            )
        except Exception as err:
            logging.exception(
                "[❌] Ошибка ручного перерасчёта недельных бюджетов",
                exc_info=err,
            )
            return Response(
                {"status": "error", "error": str(err)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        payload = result if isinstance(result, dict) else {"result": result}
        payload.update(
            {
                "status": payload.get("status", "completed"),
                "store_id": store.id,
                "store_name": store.name,
            }
        )
        return Response(payload, status=status.HTTP_200_OK)


class CreateOrUpdateAdPlanView(APIView):
    """Эндпоинт для обновления рекламного плана."""    
    def get(self, request):
        """
        Синхронно запускает задачу чтения данных из Google Sheets и создания/обновления рекламных кампаний.
        Ждет завершения выполнения и возвращает результат.
        """
        try:
            logging.info(f"[🚀] Запуск синхронного чтения данных из Google Sheets")
            
            # Запускаем функцию синхронно (не асинхронно)
            result = create_or_update_AD()
            
            if isinstance(result, list) and len(result) > 0:
                return Response({
                    "status": "success",
                    "message": "Данные из Google Sheets успешно обработаны",
                    "rows_processed": len(result),
                    "result": result
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    "status": "success",
                    "message": "Данные из Google Sheets обработаны (пустой результат)",
                    "rows_processed": 0,
                    "result": []
                }, status=status.HTTP_200_OK)
                
        except Exception as e:
            logging.error(f"[❌] Ошибка при чтении данных из Google Sheets: {e}")
            return Response({
                "status": "error",
                "error": f"Ошибка при чтении данных: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ToggleStoreAdsStatusView(APIView):
    """Синхронно переключает статус рекламной системы для магазина и отражает его в S3 таблицы."""
    def post(self, request):
        store_name = request.data.get("store_name") or request.data.get("name")
        spreadsheet_url = request.data.get("spreadsheet_url")
        sa_json_path = request.data.get("sa_json_path")
        worksheet_name = request.data.get("worksheet_name", "Main_ADV")

        if not store_name:
            return Response({"error": "store_name is required"}, status=status.HTTP_400_BAD_REQUEST)

        store = (OzonStore.objects.filter(name__iexact=store_name).first() or
                 OzonStore.objects.filter(client_id__iexact=store_name).first())
        if not store:
            return Response({"error": f"Store '{store_name}' not found"}, status=status.HTTP_404_NOT_FOUND)

        try:
            result = toggle_store_ads_status(
                store_id=store.id,
                spreadsheet_url=spreadsheet_url,
                sa_json_path=sa_json_path,
                worksheet_name=worksheet_name,
            )
            return Response({"status": "ok", "result": result}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UpdateWarehouseStockView(APIView):
    """
    Эндпоинт для синхронного обновления остатков склада
    """
    def post(self, request):
        try:
            # Получаем данные из запроса
            api_key = request.data.get("Api-Key")
            client_id = request.data.get("client_id")
            
            # Проверяем наличие обязательных параметров
            if not api_key or not client_id:
                return Response({
                    "error": "Отсутствуют обязательные параметры: Api-Key и client_id"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Получаем магазин
            try:
                ozon_store = OzonStore.objects.get(api_key=api_key, client_id=client_id)
            except OzonStore.DoesNotExist:
                return Response({
                    "error": "Магазин не найден"
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Синхронно обновляем остатки
            sync_warehouse_stock_for_store(ozon_store)
            
            return Response({
                "status": "success",
                "message": f"Остатки склада успешно обновлены для магазина {ozon_store}",
                "store_id": ozon_store.id
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении остатков склада: {str(e)}")
            return Response({
                "error": f"Внутренняя ошибка сервера: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





class Planer_View(APIView):
    permission_classes = [permissions.IsAuthenticated]

    SORT_MAPPING = {
        'orders': 1,
        'revenue': 2,
        'ozon-rec': 3,
    }

    def _get_filters_from_settings(self, store: OzonStore):
        settings, _ = StoreFilterSettings.objects.get_or_create(store=store)
        return {
            "days": int(settings.planning_days),
            "sort_by_qty": self.SORT_MAPPING.get(settings.sort_by, 1),
            "b7": 1 if float(settings.warehouse_weight or 0) > 0 else 0,
            "f9": float(settings.specific_weight_threshold),
            "period_analiz": int(settings.analysis_period),
            "price_min": float(settings.price_min),
            "price_max": float(settings.price_max),
            "f6": float(settings.turnover_min) if settings.turnover_min is not None else None,
            "g6": float(settings.turnover_max) if settings.turnover_max is not None else None,
            "f7": 1 if settings.show_no_need else 0,
            "f10": float(settings.turnover_from_stock or 0),
            "exclude_offer_ids": list(settings.excluded_products.values_list("article", flat=True)),
            "mandatory_products": [
                {"offer_id": product.article, "quantity": product.quantity}
                for product in settings.required_products.all()
            ],
        }

    def post(self, request):
        start_time = time.time()
        store_id = request.data.get("store_id") or request.query_params.get("store_id")
        if not store_id:
            return Response({"error": "store_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        try:
            store_id = int(store_id)
        except (TypeError, ValueError):
            return Response({"error": "store_id must be an integer"}, status=status.HTTP_400_BAD_REQUEST)

        ozon_store = get_object_or_404(user_store_queryset(request.user), id=store_id)

        filters = self._get_filters_from_settings(ozon_store)
        days = filters["days"]
        sort_by_qty = filters["sort_by_qty"]
        b7 = filters["b7"]
        f9 = filters["f9"]
        period_analiz = filters["period_analiz"]
        price_min = filters["price_min"]
        price_max = filters["price_max"]
        f6 = filters["f6"]
        g6 = filters["g6"]
        f7 = filters["f7"]
        f10 = filters["f10"]
        exclude_offer_ids = filters["exclude_offer_ids"]
        mandatory_products = filters["mandatory_products"]

        logging.info(
            "Planner request store=%s days=%s sort_by=%s period=%s price_range=(%s,%s) "
            "turnover=(%s,%s) show_no_need=%s",
            ozon_store.id,
            days,
            sort_by_qty,
            period_analiz,
            price_min,
            price_max,
            f6,
            g6,
            f7,
        )

        if days > 60 or days < 0:
            return Response({"error": "Период анализа должен быть от 0 до 60 дней"}, status=400)
        if price_max < price_min:
            return Response({"error": "Минимальная цена не может быть больше максимальной"}, status=400)

        since_date = timezone.now() - timedelta(days=days-1)
        since_date = since_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # Все товары
        products = Product.objects.filter(
            store=ozon_store,
            price__gte=price_min,
            price__lte=price_max
        )
        
        # Исключаем товары по артикулам, если указаны
        if exclude_offer_ids:
            products = products.exclude(offer_id__in=exclude_offer_ids)
        products_by_sku = {p.sku: p for p in products}
        # logging.info(f" Target SKU ={products_by_sku.get(2909660721)}")
        # Создаем словарь для получения barcode по offer_id
        offer_id_to_barcode = {p.offer_id: p.barcodes[0] if p.barcodes else None for p in products}
        
        

        # Продажи
        logging.info(f"Дата до которой смотрим {since_date}")
        sales = Sale.objects.filter(store=ozon_store, date__gte=since_date, sale_type__in=[Sale.FBO, Sale.FBS])
        sales_by_cluster = {}
        logging.info(f"Кол-во продаж {len(sales)}")
        for s in sales:
            cluster = s.cluster_to or "Без кластера"
            sales_by_cluster.setdefault(cluster, {})
            sales_by_cluster[cluster].setdefault(s.sku, {"qty": 0, "price": 0})
            sales_by_cluster[cluster][s.sku]["qty"] += s.quantity
            sales_by_cluster[cluster][s.sku]["price"] += float(s.price)*s.quantity

        logging.info(f"Кол-во кластеров  {len(sales_by_cluster)}")
        
        # Посчитаем количество продаж по каждой позиции SKU и получим по каждому товару количество продаж
        product_revenue_map_qty = {}  
        for cluster in sales_by_cluster:
            for sku, data in sales_by_cluster[cluster].items():
                product_revenue_map_qty.setdefault(sku, 0)
                product_revenue_map_qty[sku] += data["qty"]
                
        logging.info(f"Количество уникальных SKU product_revenue_map_qty =  {len(product_revenue_map_qty)}")  
              
        # Остатки товаров по складам
        stocks = WarehouseStock.objects.filter(store=ozon_store)
        stocks_by_cluster = {}
        total_stock_all_clusters = {}
        requested_stock_by_sku = {}  # Отдельный расчет товаров в заявках на поставку
        logging.info(f"Остатки товаров по складам  {len(stocks)}")
        
        for stock in stocks:
            cluster = stock.cluster_name or "Без кластера"
            stock_sum = (
                stock.available_stock_count +
                stock.valid_stock_count +
                stock.waiting_docs_stock_count +
                stock.expiring_stock_count +
                stock.transit_defect_stock_count +
                stock.stock_defect_stock_count +
                stock.excess_stock_count +
                stock.other_stock_count +
                stock.requested_stock_count +
                stock.transit_stock_count +
                stock.return_from_customer_stock_count
            )
            
            # По кластеру
            if cluster not in stocks_by_cluster:
                stocks_by_cluster[cluster] = {}
            if stock.sku not in stocks_by_cluster[cluster]:
                stocks_by_cluster[cluster][stock.sku] = 0
            stocks_by_cluster[cluster][stock.sku] += stock_sum

            # По всем кластерам
            if stock.sku not in total_stock_all_clusters:
                total_stock_all_clusters[stock.sku] = 0
            total_stock_all_clusters[stock.sku] += stock_sum
            
            # Отдельный расчет товаров в заявках на поставку по SKU
            if stock.sku not in requested_stock_by_sku:
                requested_stock_by_sku[stock.sku] = 0
            requested_stock_by_sku[stock.sku] += stock.requested_stock_count
            
        # logging.info(f"Заявки на поставку по SKU 1928741963 {requested_stock_by_sku[1928741963]}")
        # FBS остатки
        fbs_stocks = FbsStock.objects.filter(store=ozon_store)
        fbs_by_sku = {}
        for f in fbs_stocks:
            fbs_by_sku.setdefault(f.sku, 0)
            fbs_by_sku[f.sku] += f.present



        # 1. Подсчёт выручки по всем кластерам и всем товарам
        total_revenue = 0
        revenue_by_cluster = {}
        product_revenue_map = {}

        for cluster in sales_by_cluster:
            for sku, data in sales_by_cluster[cluster].items():
                revenue_by_cluster.setdefault(cluster, 0)
                revenue_by_cluster[cluster] += data["price"]

                product_revenue_map.setdefault(sku, 0)
                product_revenue_map[sku] += data["price"]

        total_revenue = sum(revenue_by_cluster.values()) or 1  # защита от деления на 0

        
        # Получаем данные по кластерам доставки average_delivery_time impact_share
        delivery_cluster_data = {
            dc.name: {
                "average_delivery_time": dc.average_delivery_time,
                "impact_share": dc.impact_share
            }
            for dc in DeliveryCluster.objects.filter(store=ozon_store)
        }

        
        # Получаем аналитику по товарам в кластерах доставки
        # ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ
        item_analytics_map = {
            (a.cluster_name, a.sku): a
            for a in DeliveryClusterItemAnalytics.objects.filter(store=ozon_store)
        }
        
        # 2. Финальная сборка по кластерам
        all_clusters = set(sales_by_cluster) | set(stocks_by_cluster)

        cluster_list = []
        # len(all_clusters)
        logging.info(f"Total len clusters = {len(all_clusters)}")
        offer_delivery_totals = {}
        for cluster in all_clusters:
            cluster_data = {
                "cluster_name": cluster,
                "cluster_revenue": round(revenue_by_cluster.get(cluster, 0), 2),
                "cluster_share_percent": round((revenue_by_cluster.get(cluster, 0) / total_revenue) * 100, 2),
                "products": []
            }
            all_skus = set()
            if cluster in sales_by_cluster:
                all_skus |= set(sales_by_cluster[cluster])
            if cluster in stocks_by_cluster:
                all_skus |= set(stocks_by_cluster[cluster])

            for sku in all_skus:
                
                product = products_by_sku.get(sku)
                
                if not product:
                    # logging.info(f"434 строчка если нет product продолжаем")
                    continue
                
                # ОБЩАЯ АНАЛИТИКА ПО КЛАСТЕРУ (/v1/analytics/average-delivery-time) - это значит, что
                # на все артикулы в данном кластере цифры должны быть одинаковые
                # После столбца M(кластер) необходимо добавить следующие столбцы:
                # 1. N - Ср. время доставки до покупателя - туда вставить данные общие из параметра
                # average_delivery_time (там 2 разных приходит, надо с вами потестить)
                # 2. O - Доля влияния, %, туда вставить параметр impact_share
                delivery_info = delivery_cluster_data.get(cluster, {"average_delivery_time": 0, "impact_share": 0})

                # ЧАСТНАЯ АНАЛИТИКА ПО КЛАСТЕРУ (/v1/analytics/average-delivery-time/details) - это
                # значит, что получаем цифры по каждому товару уникальные
                # 1. P - Ср. время доставки до покупателя ТОВАР, ч - average_delivery_time (тоже там два
                # разных надо потестить)
                # 2. Q - Доля влияния на ТОВАР, % - туда вставить параметр impact_share (
                # 3. R - Рекомендации к поставке, шт - туда параметр recommended_supply
                item_analytics = item_analytics_map.get((cluster, sku))


                mandatory_quantity = get_mandatory_quantity_for_product(product.offer_id, mandatory_products) if mandatory_products else None
                
                sales_qty = sales_by_cluster.get(cluster, {}).get(sku, {}).get("qty", 0)
                sales_price = sales_by_cluster.get(cluster, {}).get(sku, {}).get("price", 0)
                stock_qty_cluster = stocks_by_cluster.get(cluster, {}).get(sku, 0)
                total_stock_qty = total_stock_all_clusters.get(sku, 0)

                #    stock_total_cluster/
                avg_daily_sales_total = round(product_revenue_map_qty.get(sku, 0) / days, 2) if days else 0
                fbo_total_stock = total_stock_all_clusters.get(sku, 0)
                oborachivaemost = round((fbo_total_stock / avg_daily_sales_total),2) if avg_daily_sales_total else 0
                
                
                if g6 is not None and oborachivaemost > g6:
                    # logging.info(f"g6 is not None and oborachivaemost > g6")
                    continue
                if f6 is not None and oborachivaemost < f6:
                    # logging.info(f"g6 is not None and oborachivaemost < g6")
                    continue
                
                # if sku == 653610923:
                #     logging.info(f"SKU == {sku}")
                #     logging.info(f"Оборачиваемость == {oborachivaemost}")
                #     logging.info(f"Name == {product.offer_id}")
                    
                if f10 is not None and f10 > float(total_stock_qty) :
                    oborachivaemost = 0
                    # logging.info(f"479 | if f10 is not None and f10 > float(total_stock_qty)")
                
                total_sum_sku_all_claster = product_revenue_map.get(sku, 0) #Сумма выручки по всем кластерам для данного SKU
                share_of_total_daily_average = sales_price / total_sum_sku_all_claster if total_sum_sku_all_claster else 0
                
                if b7 == 1 and share_of_total_daily_average >= f9:
                    # K15*B5*R15-S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * share_of_total_daily_average - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * share_of_total_daily_average
                elif b7 == 1 and share_of_total_daily_average < f9:
                    # K15*B5*F9-S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * f9 - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz * f9
                
                elif b7 == 0 or b7 == None:
                    # K15*B5/на все кластера - S15
                    for_delivery = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz / len(all_clusters)  - stock_qty_cluster
                    need_goods = round(product_revenue_map_qty.get(sku, 0) / days, 2) * period_analiz / len(all_clusters)
                    # if round(for_delivery) > 0:
                    #     logging.info(f"{sku} | for delyvery = {for_delivery}")
                
                elif b7 == 2:
                    for_delivery = (item_analytics.recommended_supply if item_analytics else 0) - stock_qty_cluster
                    need_goods = (item_analytics.recommended_supply if item_analytics else 0)
                    
                    # if sku == 353866151:
                    #     logging.info(f"b7 == {b7} item_analytics.recommended_supply == {item_analytics.recommended_supply if item_analytics else 0} and stock_qty_cluster == {stock_qty_cluster}")
                    
                else:
                    
                    for_delivery = 0
                    need_goods = 0
                    # logging.info(f"b7 == {b7} and share_of_total_daily_average == {share_of_total_daily_average} and f9 == {f9
                    # logging.info(f"b7 == {b7} and share_of_total_daily_average == {share_of_total_daily_average} and f9 == {f9}")
                    # logging.info(product_revenue_map)   
                

                
                #если в ячейке F7 стоит 1, то показываются ВСЕ
                #товары, если пусто (по дефолту), то только те, которые имеют значение >0 в столбце T15.
                if f7 == 0 and round(for_delivery) <= 0 and mandatory_quantity is None:
                    continue
                
                
                offer_delivery_totals.setdefault(product.offer_id, 0)
                offer_delivery_totals[product.offer_id] += round(for_delivery)
                # if sku == 662496696:
                #     logging.info(f"Claster name == {cluster}")
                #     logging.info(f"Product == {product}")
                #     logging.info(f"share_of_total_daily_average == {share_of_total_daily_average}  == {sales_price} / {total_sum_sku_all_claster}")
                cluster_data["products"].append({
                    "sku": sku,
                    "name": product.name,
                    "offer_id": product.offer_id,
                    "photo": product.primary_image,
                    "category": product.category,
                    "type_name": product.type_name,
                    "price": float(product.price or 0),
                    "barcodes": product.barcodes,
                    "ozon_link": f"https://www.ozon.ru/product/{product.sku}/",
                    "sales_total_fbo_fbs":product_revenue_map_qty.get(sku, 0),
                    "payout_total": round(sales_price, 2),
                    "avg_daily_sales_fbo_fbs": round(product_revenue_map_qty.get(sku, 0) / days, 2) if days else 0,
                    "stock_total_cluster": stock_qty_cluster,
                    "fbs_stock_total_qty": fbs_by_sku.get(sku, 0),
                    "product_total_revenue_fbo_fbs": round(product_revenue_map.get(sku, 0), 2),
                    "avg_daily_sales_cluster_qty" : round(sales_qty / days, 2) if days else 0,
                    "avg_daily_sales_cluster_rub" : round(sales_price / days, 2) if days else 0,
                    "oborachivaemost": oborachivaemost,
                    "share_of_total_daily_average": share_of_total_daily_average,                    
                    "sales_qty_cluster": sales_qty,
                    "for_delivery" : round(for_delivery),
                    "need_goods" : need_goods,
                    "average_delivery_time": delivery_info["average_delivery_time"],
                    "impact_share": delivery_info["impact_share"],
                    "average_delivery_time_item": item_analytics.average_delivery_time if item_analytics else "",
                    "impact_share_item": item_analytics.impact_share if item_analytics else "",
                    "recommended_supply_item": item_analytics.recommended_supply if item_analytics else "",

                    
                    
                })
                
            
            if sort_by_qty == 1:
                # сортировка товаров по количеству продаж FBO+FBS
                cluster_data["products"].sort(key=lambda x: x["sales_total_fbo_fbs"], reverse=True)
                

            # сортировка товаров по выручке FBO+FBS
            if sort_by_qty == 2:
                cluster_data["products"].sort(key=lambda x: x["product_total_revenue_fbo_fbs"], reverse=True)

            if sort_by_qty == 3:
                cluster_data["products"].sort(key=lambda x: x.get("recommended_supply_item") or 0,reverse=True
)
                
            cluster_list.append(cluster_data)
            summary = [
                    {
                        "offer_id": offer_id,
                        "barcode": offer_id_to_barcode.get(offer_id),
                        "total_for_delivery": qty
                    } for offer_id, qty in offer_delivery_totals.items()
                ]

        # Пересчет for_delivery для обязательных товаров после всех основных расчетов
        if mandatory_products:
            # Сначала собираем информацию о том, в каких кластерах есть обязательные товары
            mandatory_clusters = {}
            for cluster_data in cluster_list:
                cluster_name = cluster_data["cluster_name"]
                for product_data in cluster_data["products"]:
                    offer_id = product_data["offer_id"]
                    mandatory_quantity = get_mandatory_quantity_for_product(offer_id, mandatory_products)
                    if mandatory_quantity is not None:
                        if offer_id not in mandatory_clusters:
                            mandatory_clusters[offer_id] = []
                        mandatory_clusters[offer_id].append(cluster_name)
            
            # Рассчитываем веса кластеров
            cluster_weights = {}
            for cluster, revenue in revenue_by_cluster.items():
                if total_revenue > 0:
                    cluster_weights[cluster] = revenue / total_revenue
                else:
                    cluster_weights[cluster] = 0
            
            # Проходим по всем кластерам и товарам
            for cluster_data in cluster_list:
                cluster_name = cluster_data["cluster_name"]
                
                for product_data in cluster_data["products"]:
                    offer_id = product_data["offer_id"]
                    sku = product_data["sku"]
                    
                    # Проверяем, является ли товар обязательным
                    mandatory_quantity = get_mandatory_quantity_for_product(offer_id, mandatory_products)
                    if mandatory_quantity is not None:
                        # Получаем общий остаток FBO для товара по всем кластерам
                        total_fbo_stock = total_stock_all_clusters.get(sku, 0)
                        
                        
                        # Если сумма остатков меньше обязательного количества
                        if total_fbo_stock < mandatory_quantity:
                            # Рассчитываем разницу - сколько нужно распределить
                            needed_quantity = mandatory_quantity - total_fbo_stock
                            
                            # Получаем кластеры, где есть этот товар
                            clusters_for_product = mandatory_clusters.get(offer_id, [])
                            
                            if clusters_for_product:
                                # Рассчитываем общий вес кластеров для этого товара
                                total_weight_for_product = sum(cluster_weights.get(cluster, 0) for cluster in clusters_for_product)
                                
                                if total_weight_for_product > 0:
                                    # Используем пропорциональное распределение
                                    cluster_weight = cluster_weights.get(cluster_name, 0)
                                    cluster_quantity = round(needed_quantity * (cluster_weight / total_weight_for_product))
                                else:
                                    # Если нет выручки, распределяем равномерно
                                    cluster_quantity = round(needed_quantity / len(clusters_for_product))
                            else:
                                cluster_quantity = 0
                            
                            
                            # Устанавливаем for_delivery равным cluster_quantity
                            product_data["for_delivery"] = cluster_quantity
                            
                            # Обновляем общую сумму для summary
                            offer_delivery_totals[offer_id] = sum(
                                p["for_delivery"] for cluster in cluster_list 
                                for p in cluster["products"] 
                                if p["offer_id"] == offer_id
                            )
            
            # Обновляем summary с новыми значениями
            summary = [
                {
                    "offer_id": offer_id,
                    "barcode": offer_id_to_barcode.get(offer_id),
                    "total_for_delivery": qty
                } for offer_id, qty in offer_delivery_totals.items()
            ]

        # сортировка кластеров по выручке
        cluster_list.sort(key=lambda c: c["cluster_revenue"], reverse=True)
        summary.sort(key=lambda c: c["total_for_delivery"], reverse=True)

        # Сводная аналитика доставки
        try:
            average_time = DeliveryAnalyticsSummary.objects.get(store=ozon_store).average_delivery_time
        except DeliveryAnalyticsSummary.DoesNotExist:
            average_time = None        
        execution_time = round(time.time() - start_time, 3)
        logging.info(f"[⏱] Время выполнения запроса: {execution_time}s")
        resp = Response({
            "clusters": cluster_list,
            "summary": summary,
            "execution_time_seconds": execution_time,
            "average_delivery_time": average_time,
        })
        resp["X-Execution-Time-s"] = f"{execution_time:.3f}"
        return resp


class PlanerPivotView(Planer_View):
    """
    Альтернативный формат планнера: строки по товарам, столбцы по кластерам.
    """
    def post(self, request):
        base_response = super().post(request)
        if base_response.status_code != status.HTTP_200_OK:
            # Пробрасываем ошибки как есть
            return base_response

        data = base_response.data or {}
        clusters = data.get("clusters", [])
        summary = data.get("summary", [])

        cluster_order = [c.get("cluster_name") for c in clusters]

        # Собираем метаданные и распределение по кластерам
        product_meta = {}
        per_offer_cluster_qty = defaultdict(lambda: defaultdict(int))
        for cluster in clusters:
            cluster_name = cluster.get("cluster_name")
            for p in cluster.get("products", []):
                offer_id = p.get("offer_id")
                per_offer_cluster_qty[offer_id][cluster_name] = p.get("for_delivery", 0)
                if offer_id not in product_meta:
                    product_meta[offer_id] = {
                        "sku": p.get("sku"),
                        "name": p.get("name"),
                        "barcodes": p.get("barcodes"),
                        "photo": p.get("photo"),
                        "ozon_link": p.get("ozon_link"),
                    }

        # Формируем строки в порядке summary (отсортировано по total_for_delivery)
        rows = []
        for item in summary:
            offer_id = item.get("offer_id")
            meta = product_meta.get(offer_id, {})
            rows.append({
                "offer_id": offer_id,
                "sku": meta.get("sku"),
                "name": meta.get("name"),
                "barcode": item.get("barcode") or (meta.get("barcodes") or [None])[0],
                "photo": meta.get("photo"),
                "ozon_link": meta.get("ozon_link"),
                "total_for_delivery": item.get("total_for_delivery", 0),
                "clusters": {
                    name: per_offer_cluster_qty.get(offer_id, {}).get(name, 0)
                    for name in cluster_order
                },
            })

        resp = Response({
            "cluster_headers": cluster_order,
            "products": rows,
            "execution_time_seconds": data.get("execution_time_seconds"),
            "average_delivery_time": data.get("average_delivery_time"),
        })
        resp["X-Execution-Time-s"] = base_response.headers.get("X-Execution-Time-s")
        return resp
