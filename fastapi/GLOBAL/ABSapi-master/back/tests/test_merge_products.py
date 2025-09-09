import time

from bson import ObjectId
from fastapi.testclient import TestClient
from pymongo.database import Database

from tests.makers.base import (
    API_PREFIX,
    SYNC_URL,
    dt_now,
    get_random_markets,
    generate_random_string,
)

MERGE_SLEEP_TIME = 5


def test_merge_card_not_found(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
):
    """
    Проверяет объединение в карточку товара,
    для которого не нашлось карточки: нет пересечений по артикулу или штрихкоду.
    """

    markets = get_random_markets()
    settings = settings_factory(markets)

    market = markets.pop()
    product = product_factory(settings=settings, market=market)

    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    product_collection = mongodb[f'{product['keys_id']}_{market}']
    merged_product = product_collection.find_one({'_id': product['_id']})
    card_id = merged_product['card_id']

    # проверяем, что товар получил card_id
    assert card_id

    # проверяем, что была создана новая карточка
    card = mongodb.central.find_one({'_id': ObjectId(card_id)})
    assert card

    # проверяем, что карточка содержит поля товара
    short_market = 'ya' if market == 'yandex' else market
    assert merged_product['offer_id'] == card['offer_id']
    assert merged_product['barcodes'] == card['barcodes']
    merged_product_data = {
        'product_id': str(product['_id']),
        'market': short_market,
        'keys_id': product['keys_id'],
    }
    assert [merged_product_data] == card['products']
    assert merged_product['name'] == card['name']
    assert merged_product['keys_id'] == card['keys_id']
    assert merged_product['company'] == card['company']
    assert card['merge'][short_market]


def test_merge_card_found_by_offer_id(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    """
    Проверяет объединение в карточку товара,
    для которого нашлась карточка по пересечению артикула.
    """

    markets = get_random_markets()
    settings = settings_factory(markets)
    offer_id = generate_random_string()

    # создаем карточку уже содержащую товар с таким же артикулом
    market = markets.pop()
    product = product_factory(settings=settings, market=market, offer_id=offer_id)
    card_factory(products={market: product}, keys_id=settings['_id'])

    # Удаляем, созданный товар, чтобы не обрабатывать его при объединении.
    # Нужен только для наполнения карточки.
    product_collection = mongodb[f'{product['keys_id']}_{market}']
    product_collection.delete_one({'_id': product['_id']})

    # создаем новый товар с тем же артикулом
    new_market = markets.pop()
    new_product = product_factory(settings=settings, market=new_market, offer_id=offer_id)

    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    product_collection = mongodb[f'{new_product['keys_id']}_{new_market}']
    merged_product = product_collection.find_one({'_id': new_product['_id']})
    card_id = merged_product['card_id']

    # проверяем, что товар получил card_id
    assert card_id

    card = mongodb.central.find_one({'_id': ObjectId(card_id)})

    short_new_market = 'ya' if new_market == 'yandex' else new_market
    # т.к., артикул товаров одинаковый, только одно значение должно быть в карточке
    assert [offer_id] == card['offer_id']
    # проверяем, что в карточке только два товара
    assert len(card['barcodes']) == 2 and len(card['products']) == 2
    # проверяем, что карточка содержит поля товара
    assert new_product['barcode'] in card['barcodes']
    new_product_data = {
        'product_id': str(new_product['_id']),
        'market': short_new_market,
        'keys_id': new_product['keys_id'],
    }
    assert new_product_data in card['products']
    assert card['merge'][short_new_market]


def test_merge_some_cards_found_by_offer_id(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    """
    Проверяет объединение в карточку товара,
    для которого нашлись несколько карточек по пересечению артикула.
    """
    found_cards_count = 2
    all_cards_count = 3
    markets = get_random_markets(markets_min_count=all_cards_count)
    settings = settings_factory(markets)
    offer_id = generate_random_string()

    # создаем несколько отдельных карточек, содержащих товары с одинаковыми артикулами
    for _ in range(found_cards_count):
        # создаем карточку уже содержащую товар
        market = markets.pop()
        product = product_factory(settings=settings, market=market, offer_id=offer_id)
        card_factory(products={market: product}, keys_id=settings['_id'])

        # Удаляем, созданный товар, чтобы не обрабатывать его при объединении.
        # Нужен только для наполнения карточки.
        product_collection = mongodb[f'{product['keys_id']}_{market}']
        product_collection.delete_one({'_id': product['_id']})

    # создаем новый товар с тем же артикулом
    new_market = markets.pop()
    new_product = product_factory(settings=settings, market=new_market, offer_id=offer_id)

    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    product_collection = mongodb[f'{new_product['keys_id']}_{new_market}']
    merged_product = product_collection.find_one({'_id': new_product['_id']})
    card_id = ObjectId(merged_product['card_id'])

    # проверяем, что товар получил card_id
    assert card_id

    cards = list(mongodb.central.find())
    # проверяем, что для товара была создана новая карточка,
    # он не объединился ни с одной из существующих
    assert len(cards) == all_cards_count

    short_new_market = 'ya' if new_market == 'yandex' else new_market
    merged_product_data = {
        'product_id': str(merged_product['_id']),
        'market': short_new_market,
        'keys_id': merged_product['keys_id'],
    }
    for card in cards:
        # новая карточка, проверяем, что она содержит поля товара
        if card['_id'] == card_id:
            assert merged_product['offer_id'] == card['offer_id']
            assert merged_product['barcodes'] == card['barcodes']
            assert [merged_product_data] == card['products']
            assert merged_product['name'] == card['name']
            assert merged_product['keys_id'] == card['keys_id']
            assert merged_product['company'] == card['company']
            assert card['merge'][short_new_market]
        # ранее существующие карточки, проверяем, что товар не был к ним добавлен
        else:
            assert [merged_product_data] != card['products']


def test_merge_with_card_id(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    """
    Проверяет объединение в карточку товара,
    который уже содержит идентификатор карточки т.е., уже был объединен в карточку ранее.
    """

    markets = get_random_markets()
    settings = settings_factory(markets)

    market = markets.pop()
    product = product_factory(settings=settings, market=market)

    # выполняем объединение в первый раз
    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    product_collection = mongodb[f'{product['keys_id']}_{market}']
    merged_product = product_collection.find_one({'_id': product['_id']})
    card_id = merged_product['card_id']

    # проверяем, что товар получил card_id
    assert card_id

    # проверяем, что была создана новая карточка
    card = mongodb.central.find_one({'_id': ObjectId(card_id)})
    assert card

    # обновляем ранее созданный товар
    field_to_update = 'image'
    short_market = 'ya' if market == 'yandex' else market
    updated_value = f'new_{short_market}_{generate_random_string()}'
    product_collection.update_one(
        filter={'_id': merged_product['_id']},
        update={
            '$set':
                {
                    f'{short_market}_{field_to_update}': updated_value,
                    'updated_at': dt_now(),
                },
        },
    )

    # выполняем объединение еще раз (имитируем объединение товара содержащего идентификатор карточки)
    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    cards = list(mongodb.central.find())
    updated_card = cards[0]

    # проверяем, что новых карточек не было создано и есть только ранее существующая
    assert len(cards) == 1 and updated_card['_id'] == card['_id']

    # проверяем, что в карточке обновилось поле, обновленное в товаре
    assert updated_card[field_to_update][short_market] == updated_value


def test_merge_with_card_id_and_change_offer_id(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    """
    Проверяет объединение в карточку товара, который уже содержит идентификатор карточки
    т.е., уже был объединен в карточку ранее. При этом у товара изменился артикул.
    """

    markets = get_random_markets()
    settings = settings_factory(markets)

    market = markets.pop()
    product = product_factory(settings=settings, market=market)

    # выполняем объединение в первый раз
    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    product_collection = mongodb[f'{product['keys_id']}_{market}']
    merged_product = product_collection.find_one({'_id': product['_id']})
    card_id = merged_product['card_id']

    # проверяем, что товар получил card_id
    assert card_id

    # проверяем, что была создана новая карточка
    card = mongodb.central.find_one({'_id': ObjectId(card_id)})
    assert card

    # обновляем артикул ранее созданного товара
    short_market = 'ya' if market == 'yandex' else market
    offer_id_field = f'offer_id_{short_market}'
    updated_value = f'new_{short_market}_{generate_random_string()}'
    product_collection.update_one(
        filter={'_id': merged_product['_id']},
        update={
            '$set':
                {
                    offer_id_field: updated_value,
                    'updated_at': dt_now(),
                },
            '$addToSet': {
                'offer_id': updated_value,
            }
        },
    )

    # выполняем объединение еще раз (имитируем объединение товара содержащего идентификатор карточки)
    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    cards = list(mongodb.central.find())
    updated_card = cards[0]

    # проверяем, что новых карточек не было создано и есть только ранее существующая
    assert len(cards) == 1 and updated_card['_id'] == card['_id']

    # проверяем, что новый артикул был добавлен в карточку
    assert updated_value in updated_card['offer_id']
    # проверяем, что старый артикул не был удален из карточки
    assert merged_product[offer_id_field] in updated_card['offer_id']


def test_merge_with_empty_barcode(
    client: TestClient,
    mongodb: Database,
    settings_factory,
    product_factory,
    card_factory,
):
    markets = get_random_markets()
    settings = settings_factory(markets)

    # создаем карточку уже содержащую товар с пустым штрихкодом
    market = markets.pop()
    product = product_factory(settings=settings, market=market, barcode='')
    card_id = card_factory(products={market: product}, keys_id=settings['_id'])

    # Удаляем, созданный товар, чтобы не обрабатывать его при объединении.
    # Нужен только для наполнения карточки.
    product_collection = mongodb[f'{product['keys_id']}_{market}']
    product_collection.delete_one({'_id': product['_id']})

    # создаем новый товар также с пустым штрихкодом, но с другим артикулом
    new_market = markets.pop()
    product_factory(settings=settings, market=new_market, barcode='')

    response = client.post(API_PREFIX + SYNC_URL + '/merge', json={'keys_ids': [settings['_id']]})
    assert response.status_code == 200

    # ждем, чтобы подпроцесс выполнил объединение
    time.sleep(MERGE_SLEEP_TIME)

    cards = list(mongodb.central.find())
    # проверяем, что для товара была создана новая карточка,
    # он не объединился с существующей, также содержащей товар с пустым штрихкодом
    assert len(cards) == 2

    for card in cards:
        # проверяем, что для новой карточки в списке штрихкодов нет значений
        # т.е., что пустая строка не записалась
        if card['_id'] != card_id:
            assert card['barcodes'] == []

