import time

from bson import ObjectId
from fastapi.testclient import TestClient
from pymongo.database import Database

from tests.makers.base import (
    API_PREFIX,
    SYNC_URL,
    get_random_markets,
    generate_random_string,
)

CARD_FIELDS = [
    'merge',
    'url',
    'image',
    'stock',
    'dimension',
    'category',
    'country',
    'brend',
    'weight',
    'description',
    'options',
]

EXCLUDE_SLEEP_TIME = 5


def test_exclude_product(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    """Проверяет исключение товара из карточки"""

    markets = get_random_markets()
    settings = settings_factory(markets)
    offer_id = generate_random_string()
    inserted_products = {}
    for market in markets:
        inserted_product = product_factory(settings=settings, market=market, offer_id=offer_id)
        inserted_products[market] = inserted_product

    # объединяем созданные товары в карточку
    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(EXCLUDE_SLEEP_TIME)

    # получаем карточку, созданную при объединении
    merged_card = mongodb.central.find_one({'offer_id': offer_id})
    merged_card_id = merged_card['_id']

    # подготавливаем payload для исключения товара
    market_to_exclude, product_to_exclude = inserted_products.popitem()
    # везде, кроме получения коллекции с продуктами используем короткое название для яндекса
    short_market_to_exclude = 'ya' if market_to_exclude == 'yandex' else market_to_exclude
    exclude_payload = {
        'card_id': str(merged_card_id),
        'product_id': str(product_to_exclude['_id']),
        'keys_id': product_to_exclude['keys_id'],
        'market': short_market_to_exclude,
    }
    response = client.post(API_PREFIX + SYNC_URL + '/unmerge', json=exclude_payload)
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил исключение товара из карточки
    time.sleep(EXCLUDE_SLEEP_TIME)

    # получаем исключенный товар и новую карточку, в которую он был добавлен
    product_collection = mongodb[f'{product_to_exclude['keys_id']}_{market_to_exclude}']
    excluded_product = product_collection.find_one({'_id': product_to_exclude['_id']})
    separate_card_id = ObjectId(excluded_product['card_id'])

    # проверяем, что товар получил идентификатор новый карточки
    assert separate_card_id != merged_card_id

    # проверяем, что отдельная карточка для товара была создана
    separate_card = mongodb.central.find_one({'_id': separate_card_id})
    assert separate_card

    # проверяем, что в новой отдельной карточке содержится информация об исключенном товаре
    assert [product_to_exclude['barcode']] == separate_card['barcodes']
    assert [product_to_exclude[f'offer_id_{short_market_to_exclude}']] == separate_card['offer_id']
    excluded_product_data = {
        'product_id': str(excluded_product['_id']),
        'keys_id': excluded_product['keys_id'],
        'market': short_market_to_exclude,
    }
    assert [excluded_product_data] == separate_card['products']
    assert product_to_exclude['name'] == separate_card['name']
    assert product_to_exclude['keys_id'] == separate_card['keys_id']
    assert product_to_exclude['company'] == separate_card['company']
    assert separate_card['merge'][short_market_to_exclude]

    for field in CARD_FIELDS:
        merged_card_value = merged_card.get(field, {}).get(short_market_to_exclude)
        if merged_card_value:
            assert merged_card_value == separate_card[field][short_market_to_exclude]

    # проверяем, что информация об исключенном товаре удалена из старой карточки
    merged_card_after_exclude = mongodb.central.find_one({'_id': merged_card_id})
    assert excluded_product_data not in merged_card_after_exclude['products']
    for field in CARD_FIELDS:
        merged_card_value = merged_card.get(field, {}).get(short_market_to_exclude)
        if merged_card_value:
            assert short_market_to_exclude not in merged_card_after_exclude[field]
