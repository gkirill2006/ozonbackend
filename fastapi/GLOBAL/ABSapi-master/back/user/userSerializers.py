def userEntity(user) -> dict:
    return {
        "id": str(user["_id"]),
        "username": user["username"],
        "password": user["password"]
    }


def userResponseEntity(user) -> dict:
    return {
        "id": str(user["_id"]),
        "username": user["username"],
        "created_at": user["created_at"],
        "updated_at": user["updated_at"]
    }


def productResponseEntity(product) -> dict:
    return {
         "id": str(product["_id"]),
        "name": product['name'],
        "keys_id": product["keys_id"],
        "company": product["company"],
        "groups": product["groups"],
        "merge": product["merge"],
        "url": product['url'],
        "image": product['image'],
        "stock": product["stock"],
        "offer_id": product["offer_id"],
        "barcodes": product["barcodes"],
        "created_at": product["created_at"],
        "updated_at": product["updated_at"]
    }

def productsResponseEntity(products)  -> list:
    return [productResponseEntity(product) for product in products]


def settingsResponseEntity(settings) -> list:
    return {
        "id": str(settings["_id"]),
        "company": settings["company"],
        "ozon": settings["ozon"],
        "yandex": settings["yandex"],
        "wb": settings["wb"],
        "ali": settings["ali"],
        "sber": settings["sber"],
        "created_at": settings["created_at"],
        "updated_at": settings["updated_at"]
    }


def settingsListResponseEntity(settings) -> list:
    return [settingsResponseEntity(s) for s in (settings)]


def ozonResponseEntity(warehouse) -> list:
    return {
        "id": str(warehouse["_id"]),
        "ozon_id": warehouse["ozon_id"],
        "keys_id": warehouse["keys_id"],
        "company": warehouse["company"],
        "height": warehouse["height"],
        "depth": warehouse["depth"],
        "width": warehouse["width"],
        "dimension_unit": warehouse["dimension_unit"],
        "weight": warehouse["weight"],
        "weight_unit": warehouse["weight_unit"],
        "category": warehouse["category"],
        "country": warehouse["country"],
        "brend": warehouse["brend"],
        "description": warehouse["description"],
        "warehouse_id": warehouse["warehouse_id"],
        "offer_id": warehouse["offer_id"],
        "offer_id_ozon": warehouse["offer_id_ozon"],
        "name": warehouse["name"],
        "stock": warehouse["stock"],
        "ozon_url": warehouse["ozon_url"],
        "ozon_image": warehouse["ozon_image"],
        "sku": warehouse["sku"],
        "barcode": warehouse["barcode"],
        "barcodes": warehouse["barcodes"],
        "created_at": warehouse["created_at"],
        "updated_at": warehouse["updated_at"]
    }

def ozonListResponseEntity(warehouses) -> list:
    return [ozonResponseEntity(warehouse) for warehouse in (warehouses)]

def wbResponseEntity(warehouse) -> list:
    return {
        "id": str(warehouse["_id"]),
        "wb_id": warehouse["wb_id"],
        "keys_id": warehouse["keys_id"],
        "company": warehouse["company"],
        "height": warehouse["height"],
        "depth": warehouse["depth"],
        "width": warehouse["width"],
        "dimension_unit": warehouse["dimension_unit"],
        "weight": warehouse["weight"],
        "weight_unit": warehouse["weight_unit"],
        "category": warehouse["category"],
        "country": warehouse["country"],
        "brend": warehouse["brend"],
        "description": warehouse["description"],
        "warehouse_id": warehouse["warehouse_id"],
        "offer_id": warehouse["offer_id"],
        "offer_id_wb": warehouse["offer_id_wb"],
        "name": warehouse["name"],
        "stock": warehouse["stock"],
        "wb_url": warehouse["wb_url"],
        "wb_image": warehouse["wb_image"],
        "sku": warehouse["sku"],
        "barcode": warehouse["barcode"],
        "barcodes": warehouse["barcodes"],
        "created_at": warehouse["created_at"],
        "updated_at": warehouse["updated_at"]
    }

def wbListResponseEntity(warehouses) -> list:
    return [wbResponseEntity(warehouse) for warehouse in (warehouses)]

def yaResponseEntity(warehouse) -> list:
    return {
        "id": str(warehouse["_id"]),
        "modelid": warehouse["modelid"],
        "keys_id": warehouse["keys_id"],
        "company": warehouse["company"],
        "height": warehouse["height"],
        "depth": warehouse["depth"],
        "width": warehouse["width"],
        "dimension_unit": warehouse["dimension_unit"],
        "weight": warehouse["weight"],
        "weight_unit": warehouse["weight_unit"],
        "category": warehouse["category"],
        "country": warehouse["country"],
        "brend": warehouse["brend"],
        "description": warehouse["description"],
        "campaign_id": warehouse['campaign_id'],
        "business_id": warehouse['business_id'],
        "warehouse_id": warehouse["warehouse_id"],
        "offer_id": warehouse["offer_id"],
        "offer_id_ya": warehouse["offer_id_ya"],
        "name": warehouse["name"],
        "stock": warehouse["stock"],
        "ya_url": warehouse["ya_url"],
        "ya_image": warehouse["ya_image"],
        "sku": warehouse["sku"],
        "barcode": warehouse["barcode"],
        "barcodes": warehouse["barcodes"],
        "created_at": warehouse["created_at"],
        "updated_at": warehouse["updated_at"]
    }

def yaListResponseEntity(warehouses) -> list:
    return [yaResponseEntity(warehouse) for warehouse in (warehouses)]


def aliResponseEntity(warehouse) -> list:
    return {
        "id": str(warehouse["_id"]),
        "ali_id": warehouse["ali_id"],
        "keys_id": warehouse["keys_id"],
        "company": warehouse["company"],
        "offer_id": warehouse["offer_id"],
        "offer_id_ali": warehouse["offer_id_ali"],
        "name": warehouse["name"],
        "stock": warehouse["stock"],
        "ali_url": warehouse["ali_url"],
        "ali_image": warehouse["ali_image"],
        "sku": warehouse["sku"],
        "barcode": warehouse["barcode"],
        "barcodes": warehouse["barcodes"],
        "created_at": warehouse["created_at"],
        "updated_at": warehouse["updated_at"]
    }

def aliListResponseEntity(warehouses) -> list:
    return [aliResponseEntity(warehouse) for warehouse in (warehouses)]


def centralResponseEntity(warehouse) -> list:
    return {
        "id": str(warehouse["_id"]),
        "name": warehouse['name'],
        "keys_id": warehouse["keys_id"],
        "company": warehouse["company"],
        "dimension": warehouse["dimension"],
        "category": warehouse["category"],
        "country": warehouse["country"],
        "brend": warehouse["brend"],
        "weight": warehouse["weight"],
        "groups": warehouse["groups"],
        "merge": warehouse["merge"],
        "url": warehouse['url'],
        "image": warehouse['image'],
        "stock": warehouse["stock"],
        "offer_id": warehouse["offer_id"],
        "barcodes": warehouse["barcodes"],
        "created_at": warehouse["created_at"],
        "updated_at": warehouse["updated_at"]
    }

def centralListResponseEntity(warehouses) -> list:
    return [centralResponseEntity(warehouse) for warehouse in (warehouses)]

def queryResponseEntity(query) -> list:
    return {
        "id": str(query["_id"]),
        "last_query": query['last_query'],
        "created_at": query['created_at'],
    }

def queryListResponseEntity(queries) -> list:
    return [queryResponseEntity(query) for query in (queries)]