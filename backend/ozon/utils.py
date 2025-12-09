import requests
from datetime import datetime, timedelta

from .models import Category, ProductType
from pprint import pprint
import logging
from time import sleep
import time
from django.utils import timezone
from users.models import OzonStore
logger = logging.getLogger(__name__)

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
        
        
def fetch_warehouse_stock(client_id, api_key, skus: list):
    """
    Делает запрос к Ozon API и возвращает данные по остаткам на складах по SKU.
    """
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





def fetch_fbo_sales(client_id, api_key, days: int = 7):
    logging.info(f"Enter FBO: {days} days")
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    result = []

    def fetch_range(since, to):
        offset = 0
        while True:
            payload = {
                "dir": "ASC",
                "filter": {
                    "since": since,
                    "to": to,
                    "status": ""
                },
                "limit": 1000,
                "offset": offset,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }

            # Retry with exponential backoff
            for attempt in range(5):
                resp = requests.post(url, headers=headers, json=payload)
                if resp.status_code == 429:
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code != 200:
                    raise Exception(f"FBO API error: {resp.status_code} {resp.text}")
                else:
                    break

            items = resp.json().get("result", [])
            if not items:
                break

            for item in items:
                product = item["products"][0]
                finance = item["financial_data"]["products"][0]

                result.append({
                    "sale_type": "FBO",
                    "posting_number": item["posting_number"],
                    "sku": product["sku"],
                    "price": float(product["price"]),
                    "quantity": product["quantity"],
                    "payout": float(finance["payout"]),
                    "commission_amount": float(finance["commission_amount"]),
                    "customer_price": None,
                    "tpl_provider": None,
                    "warehouse_id": item["analytics_data"].get("warehouse_id"),
                    "cluster_from": item["financial_data"].get("cluster_from", ""),
                    "cluster_to": item["financial_data"].get("cluster_to", ""),
                    "status": item["status"],
                    "date": item["created_at"]
                })

            offset += len(items)
            time.sleep(0.3)  # маленькая пауза между страницами

    now = timezone.now()
    if days <= 10:
        since = (now - timedelta(days=days)).isoformat()
        to = now.isoformat()
        fetch_range(since, to)
    else:
        step = 5
        for i in range(0, days, step):
            from_date = now - timedelta(days=i + step)
            to_date = now - timedelta(days=i)
            since = from_date.isoformat()
            to = to_date.isoformat()
            fetch_range(since, to)
            time.sleep(1.5)  # пауза между диапазонами

    logging.info(f"Fetched {len(result)} FBO sales")
    return result



def fetch_fbs_sales(client_id, api_key, days: int = 7):
    logging.info(f"Enter FBS: {days} days")
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    result = []

    def fetch_range(since, to):
        offset = 0
        while True:
            payload = {
                "dir": "ASC",
                "filter": {
                    "since": since,
                    "to": to,
                    "status": ""
                },
                "limit": 1000,
                "offset": offset,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }

            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                raise Exception(f"FBS API error: {resp.status_code} {resp.text}")

            items = resp.json().get("result", {}).get("postings", [])

            if not items:
                logging.info(f"No items found for range {since} — {to}")
                break

            for item in items:
                for finance in item["financial_data"]["products"]:
                    
                    # if item["posting_number"] == "24112774-0215-1":
                    #     logging.info(f"finance = {finance}")
                    # logging.info(f"Processing item {item['posting_number']}")
                    try:
                        result.append({
                            "sale_type": "FBS",
                            "posting_number": item["posting_number"],
                            "sku": finance["product_id"],
                            "price": float(finance.get("price", 0)),
                            "quantity": finance.get("quantity", 1),
                            "payout": float(finance.get("payout", 0)),
                            "commission_amount": float(finance.get("commission_amount", 0)),
                            "customer_price": float(finance.get("customer_price") or 0),
                            "tpl_provider": item.get("delivery_method", {}).get("tpl_provider", ""),
                            "warehouse_id": item.get("analytics_data", {}).get("warehouse_id"),
                            "cluster_from": item["financial_data"].get("cluster_from", ""),
                            "cluster_to": item["financial_data"].get("cluster_to", ""),
                            "status": item["status"],
                            "date": item.get("in_process_at") or item.get("shipment_date")
                        })
                        
                        if item["posting_number"] == "24112774-0215-1":
                            logging.info(f"------------------------------------------------------------------")
                    except Exception as e:
                        # logging.error(f"Error processing item {item['posting_number']}: {e}")
                        continue
            
            if items:
                offset += len(items)
                sleep(1)
            else:
                break
            # print(f"offset = {offset}")

    now = datetime.now()

    if days <= 10:
        since = (now - timedelta(days=days)).isoformat() + "Z"
        to = now.isoformat() + "Z"
        logging.info(f"Fetching single range {since} — {to}")
        fetch_range(since, to)
    else:
        step = 5
        for i in range(0, days, step):
            from_date = now - timedelta(days=i + step)
            to_date = now - timedelta(days=i)
            since = from_date.isoformat() + "Z"
            to = to_date.isoformat() + "Z"
            logging.info(f"Fetching range {since} — {to}")
            fetch_range(since, to)
            
    logging.info(f"Fetched {len(result)} FBS sales")
    return result


def fetch_fbs_stocks(client_id, api_key, sku_list):
    
    url = "https://api-seller.ozon.ru/v1/product/info/stocks-by-warehouse/fbs"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    all_results = []

    # Если вдруг полный список не пройдёт — будет fallback на чанки
    try:
        resp = requests.post(url, headers=headers, json={"sku": sku_list})
        if resp.status_code == 200:
            return resp.json().get("result", [])
    except Exception:
        pass

    # fallback по 100
    for i in range(0, len(sku_list), 100):
        chunk = sku_list[i:i + 100]
        payload = {"sku": chunk}
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            raise Exception(f"Ozon FBS stock API error: {resp.status_code} {resp.text}")

        all_results.extend(resp.json().get("result", []))

    return all_results




# =============================
# Performance API (Реклама Ozon)
# =============================
def request_performance_token(client_id: str, client_secret: str) -> dict:
    """
    Запрашивает токен у Performance API по client_id и client_secret.

    Возвращает словарь с полями: access_token, expires_in, token_type, expires_at.
    Бросает исключение при ошибке HTTP или при отсутствии токена в ответе.
    """
    if not client_id or not client_secret:
        raise ValueError("client_id и client_secret обязательны для получения токена Performance API")

    url = "https://api-performance.ozon.ru/api/client/token"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    if resp.status_code != 200:
        raise Exception(f"Performance API token error: {resp.status_code} {resp.text}")

    data = resp.json() or {}
    token = data.get("access_token")
    if not token:
        raise Exception(f"Performance API token response without access_token: {data}")

    expires_in = int(data.get("expires_in", 0) or 0)
    token_type = data.get("token_type", "Bearer")
    expires_at = timezone.now() + timedelta(seconds=expires_in) if expires_in else None

    return {
        "access_token": token,
        "expires_in": expires_in,
        "token_type": token_type,
        "expires_at": expires_at,
    }


def get_store_performance_token(store: OzonStore) -> dict:
    """Удобная обёртка: берёт client_id/secret из `OzonStore` и запрашивает токен."""
    return request_performance_token(
        client_id=store.performance_client_id,
        client_secret=store.performance_client_secret,
    )


# =============================
# Performance API: создание кампании CPC Product v2
# =============================

def _rub_to_micros(amount) -> str:
    """Перевод рублей в микрорубли (uint64 в строке), 1 рубль = 1_000_000.
    Принимает int/float/Decimal/str в рублях, возвращает строку целого.
    """
    if amount is None or amount == "":
        return None
    try:
        from decimal import Decimal, ROUND_HALF_UP
        micros = (Decimal(str(amount)) * Decimal('1000000')).to_integral_value(rounding=ROUND_HALF_UP)
        return str(micros)
    except Exception:
        return None


def create_cpc_product_campaign(
    access_token: str,
    sku: int,
    campaign_name: str,
    from_date: str = None,
    to_date: str = None,
    weekly_budget_rub: float | int | str | None = None,
    placement: str = "PLACEMENT_TOP_PROMOTION",
    product_autopilot_strategy: str = "TOP_MAX_CLICKS",
    auto_increase_percent: int = 0,
):
    """Создать кампанию и сразу добавить в неё SKU (правило: 1 кампания = 1 SKU)."""
    url = "https://api-performance.ozon.ru/api/client/campaign/cpc/v2/product"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Параметр autoIncreasePercent более не принимается API (отключён 29.10.2025)
    payload = {
        "title": campaign_name,
        "placement": placement,
        "productAutopilotStrategy": product_autopilot_strategy,
        "weeklyBudget": _rub_to_micros(weekly_budget_rub),
    }

    # fromDate: если не передан — ближайшая полночь по МСК


    logger.info(f"[📣] Создание кампании: {campaign_name} для SKU={sku}")
    resp = requests.post(url, headers=headers, json=payload, timeout=20)
    if resp.status_code not in (200, 201, 202):
        raise Exception(f"Create campaign error: {resp.status_code} {resp.text}")
    data = resp.json() if resp.text else {}
    # Извлекаем campaignId
    campaign_id = (
        data.get("campaignId")
        or data.get("id")
        or (data.get("result") or {}).get("campaignId")
        or (data.get("result") or {}).get("id")
    )
    if not campaign_id:
        logger.info(f"[ℹ️] Ответ создания кампании: {data}")
        raise Exception("Не удалось получить campaignId из ответа создания кампании")
    # Добавляем SKU в кампанию
    add_url = f"https://api-performance.ozon.ru/api/client/campaign/{campaign_id}/products"
    add_payload = {"bids": [{"sku": str(sku)}]}
    logger.info(f"[📦] Добавление товара SKU={sku} в кампанию {campaign_id}")
    add_resp = requests.post(add_url, headers=headers, json=add_payload, timeout=20)
    if add_resp.status_code not in (200, 201, 202):
        raise Exception(f"Add products error: {add_resp.status_code} {add_resp.text}")

    return {
        "campaign_id": str(campaign_id),
        "campaign_response": data,
        "add_products_response": add_resp.json() if add_resp.text else {"status": add_resp.status_code},
    }
    
    


def create_cpc_product_campaign_for_store(
    store: OzonStore,
    sku: int,
    campaign_name: str,
    from_date: str = None,
    to_date: str = None,
    weekly_budget_rub: float | int | str | None = None,
    placement: str = "PLACEMENT_TOP_PROMOTION",
    product_autopilot_strategy: str = "TOP_MAX_CLICKS",
    auto_increase_percent: int = 0,
):
    token_info = get_store_performance_token(store)
    access_token = token_info.get("access_token")
    if not access_token:
        raise Exception("Не удалось получить access_token для магазина")
    return create_cpc_product_campaign(
        access_token=access_token,
        sku=sku,
        campaign_name=campaign_name,
        from_date=from_date,
        to_date=to_date,
        weekly_budget_rub=weekly_budget_rub,
        placement=placement,
        product_autopilot_strategy=product_autopilot_strategy,
        auto_increase_percent=auto_increase_percent,
    )


# =============================
# Performance API: обновление кампании
# =============================

def update_campaign_budget(
    access_token: str,
    campaign_id: str,
    weekly_budget_rub: float | int | str | None = None,
    daily_budget_rub: float | int | str | None = None,
    total_budget_rub: float | int | str | None = None,
    from_date: str = None,
    to_date: str = None,
    auto_increase_percent: int = None,
):
    """
    Обновляет параметры кампании через Performance API.
    
    Args:
        access_token: Токен доступа к Performance API
        campaign_id: ID кампании для обновления
        weekly_budget_rub: Недельный бюджет в рублях
        daily_budget_rub: Дневной бюджет в рублях  
        total_budget_rub: Общий бюджет в рублях
        from_date: Дата начала кампании
        to_date: Дата окончания кампании
        auto_increase_percent: Процент автоподнятия бюджета (0-50)
    
    Returns:
        dict: Ответ от API Ozon
    """
    url = f"https://api-performance.ozon.ru/api/client/campaign/{campaign_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    # Формируем payload только с переданными параметрами
    payload = {}
    
    if weekly_budget_rub is not None:
        weekly_budget_micros = _rub_to_micros(weekly_budget_rub)
        if weekly_budget_micros is not None:
            payload["weeklyBudget"] = weekly_budget_micros
    
    if daily_budget_rub is not None:
        daily_budget_micros = _rub_to_micros(daily_budget_rub)
        if daily_budget_micros is not None:
            payload["dailyBudget"] = daily_budget_micros
    
    if total_budget_rub is not None:
        total_budget_micros = _rub_to_micros(total_budget_rub)
        if total_budget_micros is not None:
            payload["budget"] = total_budget_micros
    
    if from_date is not None:
        payload["fromDate"] = from_date
    
    if to_date is not None:
        payload["toDate"] = to_date
    
    # autoIncreasePercent удалён в API (Ozon, 29.10.2025), поэтому не отправляем
    
    if not payload:
        raise ValueError("Необходимо указать хотя бы один параметр для обновления")
    
    logger.info(f"[🔄] Обновление кампании {campaign_id}: {payload}")
    
    resp = requests.patch(url, headers=headers, json=payload, timeout=20)
    if resp.status_code not in (200, 201, 202, 204):
        raise Exception(f"Update campaign error: {resp.status_code} {resp.text}")
    
    # API может вернуть пустой ответ при успешном обновлении
    data = resp.json() if resp.text else {"status": "updated", "campaign_id": campaign_id}
    logger.info(f"[✅] Кампания {campaign_id} обновлена успешно")
    
    return data


def update_campaign_budget_for_store(
    store: OzonStore,
    campaign_id: str,
    weekly_budget_rub: float | int | str | None = None,
    daily_budget_rub: float | int | str | None = None,
    total_budget_rub: float | int | str | None = None,
    from_date: str = None,
    to_date: str = None,
    auto_increase_percent: int = None,
):
    """
    Удобная обёртка для обновления кампании: получает токен из OzonStore и вызывает update_campaign_budget.
    
    Args:
        store: Экземпляр OzonStore
        campaign_id: ID кампании для обновления
        weekly_budget_rub: Недельный бюджет в рублях
        daily_budget_rub: Дневной бюджет в рублях
        total_budget_rub: Общий бюджет в рублях
        from_date: Дата начала кампании
        to_date: Дата окончания кампании
        auto_increase_percent: Процент автоподнятия бюджета (0-50)
    
    Returns:
        dict: Ответ от API Ozon
    """
    token_info = get_store_performance_token(store)
    access_token = token_info.get("access_token")
    if not access_token:
        raise Exception("Не удалось получить access_token для магазина")
    
    return update_campaign_budget(
        access_token=access_token,
        campaign_id=campaign_id,
        weekly_budget_rub=weekly_budget_rub,
        daily_budget_rub=daily_budget_rub,
        total_budget_rub=total_budget_rub,
        from_date=from_date,
        to_date=to_date,
        auto_increase_percent=auto_increase_percent,
    )


# =============================
# Performance API: активация кампании
# =============================

def activate_campaign(access_token: str, campaign_id: str):
    """
    Активирует кампанию через Performance API.
    
    Args:
        access_token: Токен доступа к Performance API
        campaign_id: ID кампании для активации
    
    Returns:
        dict: Ответ от API Ozon
    """
    url = f"https://api-performance.ozon.ru/api/client/campaign/{campaign_id}/activate"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    # Пустой POST запрос
    payload = {}
    
    logger.info(f"[🔛] Активация кампании {campaign_id}")
    
    resp = requests.post(url, headers=headers, json=payload, timeout=20)
    if resp.status_code not in (200, 201, 202, 204):
        raise Exception(f"Activate campaign error: {resp.status_code} {resp.text}")
    
    # API может вернуть пустой ответ при успешной активации
    data = resp.json() if resp.text else {"status": "activated", "campaign_id": campaign_id}
    logger.info(f"[✅] Кампания {campaign_id} активирована успешно")
    
    return data


def activate_campaign_for_store(store: OzonStore, campaign_id: str):
    """
    Удобная обёртка для активации кампании: получает токен из OzonStore и вызывает activate_campaign.
    
    Args:
        store: Экземпляр OzonStore
        campaign_id: ID кампании для активации
    
    Returns:
        dict: Ответ от API Ozon
    """
    token_info = get_store_performance_token(store)
    access_token = token_info.get("access_token")
    if not access_token:
        raise Exception("Не удалось получить access_token для магазина")
    
    return activate_campaign(access_token=access_token, campaign_id=campaign_id)


# =============================
# Performance API: деактивация кампании
# =============================

def deactivate_campaign(access_token: str, campaign_id: str):
    """
    Деактивирует кампанию через Performance API.
    
    Args:
        access_token: Токен доступа к Performance API
        campaign_id: ID кампании для деактивации
    
    Returns:
        dict: Ответ от API Ozon
    """
    url = f"https://api-performance.ozon.ru/api/client/campaign/{campaign_id}/deactivate"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    # Пустой POST запрос
    payload = {}
    
    logger.info(f"[🔴] Деактивация кампании {campaign_id}")
    
    resp = requests.post(url, headers=headers, json=payload, timeout=20)
    if resp.status_code not in (200, 201, 202, 204):
        raise Exception(f"Deactivate campaign error: {resp.status_code} {resp.text}")
    
    # API может вернуть пустой ответ при успешной деактивации
    data = resp.json() if resp.text else {"status": "deactivated", "campaign_id": campaign_id}
    logger.info(f"[✅] Кампания {campaign_id} деактивирована успешно")
    
    return data


def deactivate_campaign_for_store(store: OzonStore, campaign_id: str):
    """
    Удобная обёртка для деактивации кампании: получает токен из OzonStore и вызывает deactivate_campaign.
    
    Args:
        store: Экземпляр OzonStore
        campaign_id: ID кампании для деактивации
    
    Returns:
        dict: Ответ от API Ozon
    """
    token_info = get_store_performance_token(store)
    access_token = token_info.get("access_token")
    if not access_token:
        raise Exception("Не удалось получить access_token для магазина")
    
    return deactivate_campaign(access_token=access_token, campaign_id=campaign_id)
