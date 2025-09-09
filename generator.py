import requests
import json
import time
from pprint import pprint   
from datetime import datetime, timedelta
import random
import os, glob

HEADERS = None

def get_categorys():
    url = "https://content-api.wildberries.ru/content/v2/object/all"
    limit = 1000
    offset = 0
    all_items = []

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"❌ Ошибка {response.status_code}: {response.text}")
            break

        data = response.json()
        items = data.get("data", [])
        if not items:
            break

        all_items.extend(items)
        offset += limit

    # Строим subjectID → parentName
    subject_map = {item["subjectID"]: item["parentName"] for item in all_items}
    print(f"✅ Загружено категорий: {len(subject_map)}")
    return subject_map

def get_warehouses():

    url = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print("❌ Ошибка запроса:", response.status_code, response.text)           

    try:
        data = response.json()
        # pprint(data)
    except Exception as e:
        print("❌ Ошибка при разборе JSON:", e)

#1 Получаем все товары    

def get_all_cards():
    def fetch_cards(url, status):
        limit = 100
        all_cards = []
        cursor = {"limit": limit}
        while True:
            body = {
                "settings": {
                    "cursor": cursor,
                    "filter": {
                        "withPhoto": -1
                    }
                }
            }
            response = requests.post(url, headers=HEADERS, json=body)
            if response.status_code != 200:
                print(f"❌ Ошибка запроса {status}: {response.status_code} {response.text}")
                break
            try:
                data = response.json()
                cards = data.get("cards", [])
                if not cards:
                    break
                
                for card in cards:
                    card["status"] = status  # ✅ добавляем статус

                all_cards.extend(cards)

                if len(cards) < limit:
                    break

                last_card = cards[-1]
                cursor = {
                    "limit": limit,
                    "updatedAt": last_card.get("updatedAt"),
                    "nmID": last_card.get("nmID")
                }

                time.sleep(0.3)

            except Exception as e:
                print(f"❌ Ошибка при разборе JSON ({status}):", e)
                break

        return all_cards

    # Активные карточки
    active_url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    active_cards = fetch_cards(active_url, "active")
    print(f"Активных карточек {len(active_cards)} ")

    # Архивные карточки
    archived_url = "https://content-api.wildberries.ru/content/v2/get/cards/trash"
    archived_cards = fetch_cards(archived_url, "archived")
    print(f"Архивных карточек {len(archived_cards)} ")

    # 🧩 Объединяем всё
    all_cards = active_cards + archived_cards

    with open("json/cards_all.json", "w", encoding="utf-8") as f:
        json.dump(all_cards, f, ensure_ascii=False, indent=2)

    print(f"Сохранено {len(all_cards)} карточек (в том числе архивных)")

#2 Приводим к общему виду
def process_cards(input_file="json/cards_all.json", output_file="json/cards_grouped.json"):
    with open(input_file, "r", encoding="utf-8") as f:
        cards = json.load(f)

    grouped = {}
    category_data = get_categorys()
    # category_map = {item["subjectID"]: item["parentName"] for item in category_data.get("data", [])}
    for card in cards:
        vendor_code = card.get("vendorCode")
        nm_id = card.get("nmID")
        brand = card.get("brand")
        photos = card.get("photos", [])
        sizes = card.get("sizes", [])
        subject_name = card.get("subjectName") 
        subjectID = card.get("subjectID")
        if subjectID in category_data:
            category_name = category_data[subjectID]
        else:
            category_name = ""
        # Фото
        photo = photos[0]["tm"] if photos else None
        status = card.get("status")
        # Штрихкод
        barcodes = sizes[0].get("skus", []) if sizes else []
        barcode = None
        for code in barcodes:
            if len(code) == 13 and code.isdigit():
                barcode = code
                break
        if not barcode and len(barcodes) > 1:
            barcode = barcodes[1]
        elif not barcode and barcodes:
            barcode = barcodes[0]

        link = f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"

        available = bool(photo and barcode)

        item = {
            "photo": photo,
            "vendorCode": vendor_code,
            "nmID": nm_id,
            "barcode": barcode,
            "category": category_name,
            "subject": subject_name,
            "brand": brand,
            "link": link,
            "available": available,
            "status": status,
        }

        if vendor_code not in grouped:
            grouped[vendor_code] = []

        grouped[vendor_code].append(item)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    print(f"Сформирован файл {output_file}")

# Получить все коды товаров чтобы искать их на складе
def get_all_barcodes_from_grouped(file_path="json/cards_grouped.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        cards_grouped_json = json.load(f)

    barcodes = set()
    for group in cards_grouped_json.values():
        for item in group:
            barcode = item.get("barcode")
            if barcode:
                barcodes.add(barcode)
    return list(barcodes), cards_grouped_json

#------------------------------------------------------------------------------------------------------------

#Получаем количестов товара на складе по штрихкодам АИЫ
#Функция возвращает словарь, где ключ — штрихкод, а значение — список складов с остатками
def get_stocks_by_barcode(barcodes):
    stocks_by_barcode = {}
    chunk_size = 1000

    # Получаем список складов
    warehouses_url = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
    try:
        warehouses = requests.get(warehouses_url, headers=HEADERS).json()
    except Exception as e:
        print("❌ Не удалось получить список складов:", e)
        return {}

    print(f"🧱 Складов получено: {len(warehouses)}")

    for wh in warehouses:
        wh_id = wh.get("id")
        wh_name = wh.get("name")
        if not wh_id:
            continue

        print(f"📦 Обработка склада: {wh_name} (ID: {wh_id})")

        # Разбиваем штрихкоды на чанки по 1000
        for i in range(0, len(barcodes), chunk_size):
            chunk = barcodes[i:i+chunk_size]
            url = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{wh_id}"
            try:
                response = requests.post(url, headers=HEADERS, json={"skus": chunk})
                if response.status_code != 200:
                    print(f"❌ Ошибка {response.status_code} на складе {wh_name}: {response.text}")
                    continue
                data = response.json()
            except Exception as e:
                print(f"❌ Ошибка при запросе к складу {wh_name}:", e)
                continue

            for stock in data.get("stocks", []):
                sku = stock.get("sku")
                amount = stock.get("amount", 0)
                if not sku:
                    continue

                if sku not in stocks_by_barcode:
                    stocks_by_barcode[sku] = []

                stocks_by_barcode[sku].append({
                    "warehouseId": wh_id,
                    "warehouseName": wh_name,
                    "amount": amount
                })

        break

    print(f"✅ Остатки собраны по {len(stocks_by_barcode)} штрихкодам")
    return stocks_by_barcode
def update_grouped_with_stocks():
    barcodes, cards_grouped_json = get_all_barcodes_from_grouped()
    print(f"Всего баркодов: {len(barcodes)}")

   # Получаем остатки по всем штрихкодам
    stocks = get_stocks_by_barcode(barcodes)
    

    for group in cards_grouped_json.values():
        for item in group:
            barcode = item.get("barcode")
            stock_list = stocks.get(barcode, [])
            item["stocks"] = stock_list
            item["totalStock"] = sum(s["amount"] for s in stock_list)

    with open("json/cards_grouped_with_stocks.json", "w", encoding="utf-8") as f:
        json.dump(cards_grouped_json, f, ensure_ascii=False, indent=2)


def get_prices():
    url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    prices_by_nmid = {}
    offset = 0
    limit = 1000

    while True:
        params = {
            "offset": offset,
            "limit": limit
        }
        # response = requests.get(url, headers=HEADERS_PRICE, params=params)            
        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code != 200:
            print("❌ Ошибка получения цен:", response.status_code, response.text)
            break

        data = response.json()
        items = data.get("data", {}).get("listGoods", [])
        if not items:
            break

        for item in items:
            nmid = item.get("nmID")
            prices_by_nmid[nmid] = {
                "prices": [
                    {
                        "size": s.get("techSizeName"),
                        "price": s.get("price"),
                        "discountedPrice": int(s.get("discountedPrice")),
                        "clubDiscountedPrice": int(s.get("clubDiscountedPrice"))
                    }
                    for s in item.get("sizes", [])
                ],
                "currency": item.get("currencyIsoCode4217"),
                # "discount": item.get("discount"),
                "clubDiscount": item.get("clubDiscount")
            }

        offset += limit

    return prices_by_nmid

def update_grouped_with_prices():
    file_path="json/cards_grouped_with_stocks.json"
    with open(file_path, "r", encoding="utf-8") as f:
        grouped = json.load(f)
#
    prices_by_nmid = get_prices()

    updated = 0
    for group in grouped.values():
        for item in group:
            nmid = item.get("nmID")
            price_info = prices_by_nmid.get(nmid)
            if price_info:
                item.update(price_info)
                updated += 1

    with open("json/cards_grouped_full.json", "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    print(f"✅ Обновлено товаров с ценами: {updated}")


# ------------------------------------------------------------------------------------------------------------------------------------------------------
def get_orders(days):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"GET ORDERS FROM data {date_from}")
    all_orders = []
    while True:
        params = {
            "dateFrom": date_from
        }

        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print("❌ Ошибка при получении заказов:", response.status_code, response.text)
            break

        data = response.json()
        if not data:
            print("Заказы полностью загружены.")
            break

        all_orders.extend(data)
        print(f"Получено {len(data)} заказов. Всего: {len(all_orders)}")

        # Обновляем дату для следующего запроса
        date_from = data[-1]["lastChangeDate"]

    return all_orders

def group_sales_by_warehouse(orders, days):
    grouped = {}

    for order in orders:
        warehouse = order.get("warehouseName")
        finished_price = order.get("finishedPrice")
        nmId = order.get("nmId")

        if not nmId or not warehouse or finished_price is None:
            continue

        if nmId not in grouped:
            grouped[nmId] = {
                "salesByWarehouse": {},
                "totalSales": 0,
                "totalRevenue": 0
            }

        # Увеличиваем количество продаж по складу
        grouped[nmId]["salesByWarehouse"][warehouse] = grouped[nmId]["salesByWarehouse"].get(warehouse, 0) + 1
        grouped[nmId]["totalSales"] += 1
        grouped[nmId]["totalRevenue"] += finished_price
        

    # Вычисляем среднюю цену
    for nmId, info in grouped.items():
        info["totalRevenue"] = int(info["totalRevenue"]) if info["totalSales"] else 0
        info["avgDailySales"] = round(info["totalSales"] / days, 2)
        
    return grouped


def process_orders_and_save(days):
    orders = get_orders(days)
    with open("json/orders_raw.json", "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=2)

    grouped_sales = group_sales_by_warehouse(orders, days)
    with open("json/sales_by_warehouse.json", "w", encoding="utf-8") as f:
        json.dump(grouped_sales, f, ensure_ascii=False, indent=2)



#------------------------------------------------------------------------------------------------------------
# Вычисляем вес каждого склада на основе выручки

def number_to_column_letter(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def generate_skipped_columns(start=14, step=2):  # Старт с N
    i = 0
    while True:
        yield start + step * i
        i += 1

def generate_distinct_color():
    def channel(): return random.randint(120, 200)
    return "#{:02X}{:02X}{:02X}".format(channel(), channel(), channel())

# Генерация веса для каждого склада
def calculate_warehouse_weights(sklad_max=0):
    orders_file="json/orders_raw.json"
    output_file="json/warehouse_weights.json"
    with open(orders_file, "r", encoding="utf-8") as f:
        orders = json.load(f)

    stats = {}
    total_revenue = 0
    count_sklad_prodavca = 0
    count_skald_wb = 0
    # Проходим по всем заказаным товарам и исключаем заказы со склада продавца
    # Считаем количество и объем товара с каждого склада
    for order in orders:
        warehouse = order.get("warehouseName")
        finished_price = order.get("finishedPrice", 0)
        warehouse_type = order.get("warehouseType", "Склад WB")
        
        if not warehouse or finished_price is None:
            continue

#если скалада еще нет в данных то создаем его
        if warehouse_type != "Склад продавца":
            if warehouse not in stats:
                stats[warehouse] = {
                    "ordersCount": 0,
                    "revenue": 0,
                    "warehouseType": warehouse_type
                }

        
        if warehouse_type != "Склад продавца":
            stats[warehouse]["ordersCount"] += 1
            stats[warehouse]["revenue"] += finished_price
            total_revenue += finished_price            
            count_skald_wb +=1
        else:
            count_sklad_prodavca +=1
            
    print(f"Заказов со склада продавца {count_sklad_prodavca}")
    print(f"Заказов со склада WB {count_skald_wb}")
    print(f"Общий объем заказов с складов WB {total_revenue}")

    
#определяем вес каждого скалада
    for warehouse in stats:
        revenue = stats[warehouse]["revenue"]
        if stats[warehouse]["warehouseType"] == "Склад продавца":
            stats[warehouse]["weight"] = 1
        else:
            stats[warehouse]["weight"] = round((revenue / total_revenue) * 100, 2) if total_revenue else 0


    sorted_items = sorted(stats.items(), key=lambda x: x[1]["revenue"], reverse=True)

#В этом блоке мы отключаем вывод данных по складам продавца

    if sklad_max > 0:
    # Делим склады на WB и продавца
        wb_items = [item for item in sorted_items if item[1]["warehouseType"] != "Склад продавца"]
        seller_items = [item for item in sorted_items if item[1]["warehouseType"] == "Склад продавца"]
        # Ограничиваем только WB-склады
        wb_items = wb_items[:sklad_max]
        # Объединяем обратно: сначала WB, потом продавца
        # sorted_items = seller_items + wb_items
        sorted_items = wb_items
    else:
        wb_items = [item for item in sorted_items if item[1]["warehouseType"] != "Склад продавца"]
        seller_items = [item for item in sorted_items if item[1]["warehouseType"] == "Склад продавца"]
        #  sorted_items = seller_items + wb_items        
        sorted_items = wb_items    
            
    col_generator = generate_skipped_columns()
    used_colors = set()
    sorted_stats = {}

    for warehouse, data in sorted_items:
        col_index = next(col_generator)
        col_letter = number_to_column_letter(col_index)

        # Генерация уникального цвета нужно для ячейки склада 
        if data["warehouseType"] == "Склад продавца":
            color = "#b6d7a8"  
        else:
            color = generate_distinct_color()
            while color in used_colors:
                color = generate_distinct_color()
        used_colors.add(color)

        data.update({
            "col_index": col_index,
            "col_letter": col_letter,
            "color": color
        })
        sorted_stats[warehouse] = data

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_stats, f, ensure_ascii=False, indent=2)

    print(f"✅ Сохранено в {output_file}")
    # pprint(sorted_stats)
    return sorted_stats




def get_all_nmId():
    cards_file="json/cards_grouped_full.json"
    with open(cards_file, "r", encoding="utf-8") as f:
        cards_grouped_full = json.load(f)
    nmId = []    
    for group in cards_grouped_full.values():
        for item in group:
            nmId.append(item.get("nmID"))
    
    return nmId


#----------Загружаем все остатки по складам и группируем их по товару--------------------------------------------------------------------------------------------------
# На выходе получаем массив
#   "2041917357625": {
#     "Санкт-Петербург Шушары": 6,
#     "Электросталь": 9,
#     "Коледино": 6,
#     "Краснодар": 3,
#     "Тула": 5,
#     "Казань": 17
#   }

def get_grouped_wb_stocks():
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    date_from = "2023-01-09T00:00:00"
    grouped = {}
    seen_dates = set()

    while True:
        print(f"Запрос с dateFrom = {date_from}")
        response = requests.get(url, headers=HEADERS, params={"dateFrom": date_from})

        if response.status_code != 200:
            print("❌ Ошибка запроса:", response.status_code, response.text)
            break

        data = response.json()
        if not data:
            print("Все данные получены.")
            break
        last_date = data[-1]["lastChangeDate"]
        if last_date in seen_dates:
            print(f"Повторный dateFrom ({last_date}) — завершаем цикл.")
            break
        seen_dates.add(last_date)

        for item in data:
            warehouse = item.get("warehouseName")
            quantity = item.get("quantity", 0)
            nmId = item.get("nmId")
            if not nmId or not warehouse:
                continue

            if nmId not in grouped:
                grouped[nmId] = {}

            grouped[nmId][warehouse] = quantity

        date_from = last_date
        print(f"Загружено строк: {len(data)} | Всего товаров: {len(grouped)}")
        time.sleep(1)
        all_nmId = get_all_nmId()
    # --- ДОБАВЛЯЕМ ОТСУТСТВУЮЩИЕ СКЛАДЫ СО ЗНАЧЕНИЕМ 0 ---
    all_warehouses = set()
    for wh_data in grouped.values():
        all_warehouses.update(wh_data.keys())

    for nmId, wh_data in grouped.items():
        for warehouse in all_warehouses:
            if warehouse not in wh_data:
                wh_data[warehouse] = 0
                
    for nm_id in all_nmId:
        if nm_id not in grouped:
            grouped[nm_id] = {warehouse: 0 for warehouse in all_warehouses}
    with open("json/wb_stocks_grouped.json", "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    print("Сохранено в json/wb_stocks_grouped.json")
    return grouped

#------------------------------------------------------------------------------------------------------------
def update_cards_with_sales_data(
    cards_file="json/cards_grouped_full.json",
    sales_file="json/sales_by_warehouse.json",
    orders_file="json/orders_raw.json",
    warehouse_weights_file="json/warehouse_weights.json",
    wb_stocks_file="json/wb_stocks_grouped.json",
    output_file="json/cards_final.json",
    days=60, period_analiz=15, B7=1, min_price=0, max_price=1000000, sklad_max=0, F7=0, F8=0
):
    with open(cards_file, "r", encoding="utf-8") as f:
        cards_grouped_full = json.load(f)

    with open(sales_file, "r", encoding="utf-8") as f:
        sales_by_warehouse = json.load(f)

    with open(orders_file, "r", encoding="utf-8") as f:
        orders = json.load(f)
    with open(warehouse_weights_file, "r", encoding="utf-8") as f:
        warehouse_weights = json.load(f)
    with open(wb_stocks_file, "r", encoding="utf-8") as f:
        wb_stocks = json.load(f)

    
   
    # Считаем выручку и среднюю цену
    revenue_data = {}
    update = 0
    for order in orders:
        nmId = order.get("nmId")
        finished_price = order.get("finishedPrice")
        if not nmId or finished_price is None:
            continue
        if nmId not in revenue_data:
            revenue_data[nmId] = {
                "totalRevenue_FBS" : 0
            }

        revenue_data[nmId]["totalRevenue_FBS"] += finished_price
        update +=1
        
    print(f"Добавили {update} записей товаров по FBS")

    updated = 0
    
    for group in cards_grouped_full.values():
        for item in group:
            barcode = item.get("barcode")
            nmId = item.get("nmID")
            if not nmId:
                continue

            stats = sales_by_warehouse.get(str(nmId))
            if stats:
                item["salesByWarehouse"] = stats.get("salesByWarehouse", {})
                item["totalSales"] = stats.get("totalSales", 0)
                item["totalRevenue"] = stats.get("totalRevenue", 0)
                item["avgDailySales"] = stats.get("avgDailySales", 0)
                updated += 1
            else:  
                item["salesByWarehouse"] = {}
                item["totalSales"] = 0
                item["totalRevenue"] = 0
                item["avgDailySales"] = 0
                
            item["totalRevenue_FBS"] = round(revenue_data.get(nmId, {}).get("totalRevenue_FBS", 0))

  
# Добавляем stocks_WB по складам WB
            item["stocks_WB"] = []
            total_delivery_analysis = 0
            delivery_analysis = 0
            wb_stock_entry = wb_stocks.get(str(nmId), {})

            for wh_short_name, stock_balance_amount in wb_stock_entry.items():
                full_name = next(
                    (long for long in warehouse_weights if wh_short_name in long),
                    None
                )
                if not full_name:
                    continue
                wh_info = warehouse_weights[full_name]
                if not wh_info:
                    continue
                
                
                # Считаем сколько товара нужно поставить
                # Для начала определяем проходит ли товар по ценовому фильтру
                if not stock_balance_amount:
                    stock_balance_amount = 0
                if item["prices"][0]["discountedPrice"] >= min_price and item["prices"][0]["discountedPrice"] <= max_price:
                        if B7 == 1:                    
                            # если b7=1, то = B5*m12*o9/100-n12
                            delivery_analysis = (period_analiz * float(item["avgDailySales"]) * (wh_info["weight"])) / 100 - stock_balance_amount
                        else:                            
                            #если b7=0 (или не заполнено), то идет равномерное распределение на все склады ((B5*m12)/на количество складов-n12
                            delivery_analysis = (period_analiz* float(item["avgDailySales"])) / len(warehouse_weights) - stock_balance_amount
                
                else:
                    delivery_analysis = 0
                

                
                if F8 == 0 and round(delivery_analysis) <= 0:
                    delivery_analysis = None 

                item["stocks_WB"].append({
                    "col_letter": wh_info["col_letter"],
                    "col_index": wh_info["col_index"],
                    "warehouseName_WB": full_name,
                    "stock_balance_amount": stock_balance_amount,
                    "delivery_analysis": round(delivery_analysis) if delivery_analysis is not None else None,
                })

                # if delivery_analysis is not None:
                #     total_delivery_analysis += round(delivery_analysis)

                # item["total_delivery_analysis"] = item["totalStock"] - round(total_delivery_analysis)

                if delivery_analysis is not None:
                    each_delivery = round(delivery_analysis)
                else:
                    each_delivery = None

                item["each_sclad_delivery"] = each_delivery

    items_list = list(cards_grouped_full.items())

# totalRevenue_FBS
    items_list.sort(key=lambda pair: pair[1][0].get("totalRevenue_FBS", 0), reverse=True)

    # Формируем отсортированный словарь обратно
    grouped_sorted = dict(items_list)
    
    product_list = []
    for vendor_code, items in grouped_sorted.items():
        for item in items:
            item["vendorCode"] = vendor_code
            product_list.append(item)

    # Считаем warehouse_total_delivery для каждого склада
    warehouse_delivery_totals = {}
    
    for item in product_list:
        stocks_wb = item.get("stocks_WB", [])
        for stock in stocks_wb:
            wh_name = stock["warehouseName_WB"]
            delivery = stock.get("delivery_analysis")
            if delivery is not None and delivery > 0:
                warehouse_delivery_totals[wh_name] = warehouse_delivery_totals.get(wh_name, 0) + delivery

    print(f"Найдено складов с delivery_analysis > 0: {len(warehouse_delivery_totals)}")

    # Добавляем это поле в warehouses
    for wh_name, total in warehouse_delivery_totals.items():
        if wh_name in warehouse_weights:
            warehouse_weights[wh_name]["warehouse_total_delivery"] = total
    
    for wh_name in warehouse_weights:
        if wh_name not in warehouse_delivery_totals:
            warehouse_weights[wh_name]["warehouse_total_delivery"] = 0


    # Проверяем условие F7 > warehouse_total_delivery и пересчитываем если нужно
    if F7 > 0:
        # print(f"\n=== ПЕРЕСЧЕТ ПО F7 = {F7} ===")
        
        # Находим склады, где F7 > warehouse_total_delivery
        warehouses_to_recalculate = []
        for wh_name, total in warehouse_delivery_totals.items():
            if F7 > total:
                warehouses_to_recalculate.append((wh_name, total))
                # print(f"Склад для пересчета: {wh_name} (было: {total})")
        
        # Пересчитываем каждый склад, где F7 > warehouse_total_delivery
        for wh_name, total in warehouses_to_recalculate:
            # print(f"\n--- Пересчет склада: {wh_name} ---")
            
            # Получаем коэффициент
            coefficient = F7 / total
            # print(f"Коэффициент: {coefficient:.4f}")
            
            # Пересчитываем delivery_analysis для всех товаров этого склада
            new_deliveries = []
            for item in product_list:
                stocks_wb = item.get("stocks_WB", [])
                for stock in stocks_wb:
                    if stock["warehouseName_WB"] == wh_name:
                        old_delivery = stock.get("delivery_analysis")
                        if old_delivery is not None and old_delivery > 0:
                            # Вычисляем новое значение
                            new_delivery_float = old_delivery * coefficient
                            new_delivery_int = int(new_delivery_float)  # Округляем вниз
                            fractional_part = new_delivery_float - new_delivery_int  # Дробная часть
                            
                            # print(f"  Товар {item.get('nmID')}: {old_delivery} → {new_delivery_int} (дробная часть: {fractional_part:.4f})")
                            
                            new_deliveries.append({
                                'item': item,
                                'stock': stock,
                                'new_delivery_int': new_delivery_int,
                                'fractional_part': fractional_part
                            })
            
            # print(f"Найдено товаров для пересчета: {len(new_deliveries)}")
            
            # Сортируем по дробной части (по убыванию)
            new_deliveries.sort(key=lambda x: x['fractional_part'], reverse=True)
            
            # Применяем новые значения
            new_sum = 0
            for delivery_info in new_deliveries:
                delivery_info['stock']['delivery_analysis'] = delivery_info['new_delivery_int']
                delivery_info['stock']['fractional_part'] = delivery_info['fractional_part']
                new_sum += delivery_info['new_delivery_int']
            
            # print(f"Сумма после округления: {new_sum}")
            
            # Распределяем дефицит
            deficit = F7 - new_sum
            # print(f"Дефицит: {deficit}")
            
            # Добавляем по 1 к товарам с наибольшей дробной частью
            added_count = 0
            for i in range(int(deficit)):
                if i < len(new_deliveries):
                    new_deliveries[i]['stock']['delivery_analysis'] += 1
                    added_count += 1
                    # print(f"  +1 к товару {new_deliveries[i]['item'].get('nmID')} (дробная часть была: {new_deliveries[i]['fractional_part']:.4f})")
            
            # print(f"Добавлено единиц: {added_count}")
            
            # Проверяем итоговую сумму
            final_sum = 0
            for delivery_info in new_deliveries:
                final_sum += delivery_info['stock']['delivery_analysis']
            
            # print(f"Итоговая сумма: {final_sum} (цель: {F7})")
            
            # Обновляем warehouse_total_delivery для этого склада
            warehouse_weights[wh_name]["warehouse_total_delivery"] = final_sum
            # print(f"Обновлен {wh_name}: {final_sum}")
        
        # print("=== ПЕРЕСЧЕТ ЗАВЕРШЕН ===\n")

    # Пересчитываем total_delivery_analysis для всех товаров после всех изменений
    for item in product_list:
        total_delivery_analysis = 0
        for stock in item.get("stocks_WB", []):
            delivery = stock.get("delivery_analysis")
            if delivery is not None:
                total_delivery_analysis += delivery
        
        item["total_delivery_analysis"] = item["totalStock"] - total_delivery_analysis

    result = {
        "products": product_list,
        "warehouses": warehouse_weights
    }

    
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Обновлено карточек с данными о продажах: {updated}")



def cleanup_json_files():
    files = glob.glob("json/*.json")
    for file in files:
        try:
            os.remove(file)
            print(f"Удалён файл: {file}")
        except Exception as e:
            print(f"❌ Ошибка при удалении {file}: {e}")
            
def generate_final_data(days=30, period_analiz=10, B7=1, min_price=0, max_price=1000000, sklad_max=0, API_KEY = None, F7=0, F8=0):
    global HEADERS
    HEADERS = {"Authorization": API_KEY}
    cleanup_json_files()
    get_all_cards()
    process_cards()
    update_grouped_with_stocks()
    update_grouped_with_prices()
    process_orders_and_save(days)
    calculate_warehouse_weights(sklad_max=sklad_max)
    get_grouped_wb_stocks()
    update_cards_with_sales_data(days=days, period_analiz=period_analiz, B7=B7, min_price=min_price, max_price=max_price, sklad_max=sklad_max, F7=F7, F8=F8)
    
    if not os.path.exists("json/cards_final.json"):
        return None

    with open("json/cards_final.json", "r", encoding="utf-8") as f:
        return json.load(f)

