from tests.makers.base import (
    dt_now,
    generate_random_str_id,
    generate_random_string,
    generate_random_int_id,
)


def make_wb_product(settings: dict, offer_id: str, barcode: str) -> dict:
    return {
        'offer_id_wb': offer_id,
        'offer_id': [offer_id],
        'barcode': barcode,
        'barcodes': [barcode],
        'brend': generate_random_string(),
        'category': generate_random_string(),
        'company': settings['company'],
        'country': generate_random_string(),
        'created_at': dt_now(),
        'depth': generate_random_int_id(),
        'description': generate_random_string(),
        'dimension_unit': 'mm',
        'height': generate_random_int_id(),
        'keys_id': settings['_id'],
        'name': generate_random_string(),
        'sku': generate_random_str_id(),
        'stock': generate_random_int_id(),
        'updated_at': dt_now(),
        'warehouse_id': settings['wb']['warehouse_id'],
        'wb_id': generate_random_int_id(),
        'wb_image': generate_random_string(),
        'wb_token': settings['wb']['api_key'],
        'wb_url': generate_random_string(),
        'weight': generate_random_int_id(),
        'weight_unit': 'g',
        'width': generate_random_int_id()
    }


def make_ozon_product(settings: dict, offer_id: str, barcode: str) -> dict:
    return {
        'offer_id_ozon': offer_id,
        'offer_id': [offer_id],
        'api_key': settings['ozon']['api_key'],
        'barcode': barcode,
        'barcodes': [barcode],
        'brend': generate_random_string(),
        'category': generate_random_string(),
        'client_id': settings['ozon']['client_id'],
        'company': settings['company'],
        'country': generate_random_string(),
        'depth': generate_random_int_id(),
        'description': generate_random_string(),
        'dimension_unit': 'mm',
        'height': generate_random_int_id(),
        'keys_id': settings['_id'],
        'name': generate_random_string(),
        'ozon_id': generate_random_int_id(),
        'ozon_image': generate_random_string(),
        'ozon_url': generate_random_string(),
        'sku': generate_random_int_id(),
        'stock': generate_random_int_id(),
        'warehouse_id': generate_random_int_id(),
        'weight': generate_random_int_id(),
        'weight_unit': 'g',
        'width': generate_random_int_id(),
        'created_at': dt_now(),
        'updated_at': dt_now(),
    }


def make_yandex_product(settings: dict, offer_id: str, barcode: str) -> dict:
    return {
        'offer_id_ya': offer_id,
        'offer_id': [offer_id],
        'api_key': settings['yandex']['api_key'],
        'barcode': barcode,
        'barcodes': [barcode],
        'brend': generate_random_string(),
        'business_id': settings['yandex']['business_id'],
        'campaign_id': settings['yandex']['campaign_id'],
        'category': generate_random_string(),
        'company': settings['company'],
        'country': generate_random_string(),
        'depth': generate_random_int_id(),
        'description': generate_random_string(),
        'dimension_unit': 'mm',
        'height': generate_random_int_id(),
        'keys_id': settings['_id'],
        'modelid': generate_random_int_id(),
        'name': generate_random_string(),
        'sku': generate_random_int_id(),
        'stock': generate_random_int_id(),
        'warehouse_id': settings['yandex']['warehouse_id'],
        'weight': generate_random_int_id(),
        'weight_unit': 'g',
        'width': generate_random_int_id(),
        'ya_image': generate_random_string(),
        'ya_url': generate_random_string(),
        'created_at': dt_now(),
        'updated_at': dt_now(),
    }


def make_ali_product(settings: dict, offer_id: str, barcode: str) -> dict:
    return {
        'barcode': barcode,
        'barcodes': [barcode],
        'ali_id': generate_random_str_id(),
        'ali_image': generate_random_string(),
        'ali_url': generate_random_string(),
        'api_key': settings['ali']['token'],
        'company': settings['company'],
        'keys_id': settings['_id'],
        'name': generate_random_string(),
        'offer_id_ali': offer_id,
        'offer_id': [offer_id],
        'sku': generate_random_str_id(),
        'stock': generate_random_int_id(),
        'created_at': dt_now(),
        'updated_at': dt_now(),
    }


def make_sber_product(settings: dict, offer_id: str, barcode: str) -> dict:
    return {
        'offer_id_sber': offer_id,
        'offer_id': [offer_id],
        'barcode': barcode,
        'barcodes': [barcode],
        'api_key': settings['sber']['token'],
        'merchant_id': settings['sber']['token'],
        'name': generate_random_string(),
        'company': settings['company'],
        'keys_id': settings['_id'],
        'sber_url': generate_random_string(),
        'sber_image': generate_random_string(),
        'height': generate_random_int_id(),
        'depth': generate_random_int_id(),
        'width': generate_random_int_id(),
        'dimension_unit': 'mm',
        'weight': generate_random_int_id(),
        'weight_unit': 'g',
        'description': generate_random_string(),
        'stock': generate_random_int_id(),
        'updated_at': dt_now(),
        'created_at': dt_now(),
    }


def make_product(market: str, settings: dict, offer_id: str, barcode: str) -> dict:
    params = (settings, offer_id, barcode)
    match market:
        case 'yandex':
            return make_yandex_product(*params)
        case 'ozon':
            return make_ozon_product(*params)
        case 'ali':
            return make_ali_product(*params)
        case 'wb':
            return make_wb_product(*params)
        case 'sber':
            return make_sber_product(*params)
        case _:
            return {}
