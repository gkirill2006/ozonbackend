import logging

from bson.objectid import ObjectId
from pymongo.synchronous.database import Database

import models
from user.userSerializers import settingsResponseEntity
from user.utils import datetime_now_str, dbconsync, init_logging

SUPPORTED_MARKETS = [
    'ali',
    'ozon',
    'sber',
    'wb',
    'yandex',
]


def build_card_document(product: dict, market: str) -> dict:
    creation_date = datetime_now_str()

    weight = product.get('weight', 0)
    weight_data = {market: {'value': weight, 'weight_unit': product.get('weight_unit', '')}} if weight else {}

    description = product.get('description', '')
    description_data = {market: {'value': description}} if description else {}

    brend = product.get('brend', '')
    brend_data = {market: {'name': brend}} if brend else {}

    country = product.get('country', '')
    country_data = {market: {'name': country}} if country else {}

    category = product.get('category', '')
    category_data = {market: {'name': category}} if category else {}

    stock = product.get('stock', 0)
    stock_data = {market: {'count': stock}} if stock else {}

    image = product.get(f'{market}_image', '')
    image_data = {market: image} if image else {}

    url = product.get(f'{market}_url', '')
    url_data = {market: url} if url else {}

    options_data = {'offer_id': product[f'offer_id_{market}']}

    new_card_document = {
        'keys_id': product['keys_id'],
        'name': product['name'],
        # Не добавляем пустые строки в список артикулов/штрихкодов карточки.
        # Если не будет ни одного артикула или штрихкода за исключением пустой строки,
        # то в данные поля будет записан пустой массив.
        'offer_id': [offer_id for offer_id in product['offer_id'] if offer_id],
        'barcodes': [barcode for barcode in product['barcodes'] if barcode],
        'company': product['company'],
        'products': [{'market': market, 'product_id': str(product['_id']), 'keys_id': product['keys_id']}],
        'created_at': creation_date,
        'updated_at': creation_date,
        'merge': {market: True},
        'url': url_data,
        'image': image_data,
        'stock': stock_data,
        'dimension': models.Dimension(**{market: product}).model_dump(exclude_unset=True),
        'category': category_data,
        'country': country_data,
        'brend': brend_data,
        'weight': weight_data,
        'description': description_data,
        # В product offer_id список, перезатираем его строкой offer_id_{market} из options_data
        'options': models.Options(**{market: product | options_data}).model_dump(exclude_unset=True),
    }
    return new_card_document


def get_ozon_product_document(product: dict) -> dict:
    return {
        'options.ozon.ozon_id': product['ozon_id'],
        'options.ozon.warehouse_id': product['warehouse_id'],
        'options.ozon.sku': product['sku'],
        'options.ozon.client_id': product['client_id'],
        'options.ozon.api_key': product['api_key'],
        'options.ozon.offer_id': product['offer_id_ozon'],
        'options.ozon.barcode': product['barcode'],
        'merge.ozon': True,
        'url.ozon': product['ozon_url'],
        'image.ozon': product['ozon_image'],
        'dimension.ozon.height': product['height'],
        'dimension.ozon.depth': product['depth'],
        'dimension.ozon.width': product['width'],
        'dimension.ozon.dimension_unit': product['dimension_unit'],
        'weight.ozon.value': product['weight'],
        'weight.ozon.weight_unit': product['weight_unit'],
        'category.ozon.name': product['category'],
        'country.ozon.name': product['country'],
        'brend.ozon.name': product['brend'],
        'description.ozon.value': product['description'],
        'stock.ozon.count': product.get('stock', 0),
    }


def get_wb_product_document(product: dict) -> dict:
    return {
        'options.wb.wb_id': product['wb_id'],
        'options.wb.wb_token': product['wb_token'],
        'options.wb.warehouse_id': product['warehouse_id'],
        'options.wb.sku': product['sku'],
        'options.wb.offer_id': product['offer_id_wb'],
        'options.wb.barcode': product['barcode'],
        'merge.wb': True,
        'url.wb': product['wb_url'],
        'image.wb': product['wb_image'],
        'dimension.wb.height': product['height'],
        'dimension.wb.depth': product['depth'],
        'dimension.wb.width': product['width'],
        'dimension.wb.dimension_unit': product['dimension_unit'],
        'weight.wb.value': product['weight'],
        'weight.wb.weight_unit': product['weight_unit'],
        'category.wb.name': product['category'],
        'country.wb.name': product['country'],
        'brend.wb.name': product['brend'],
        'description.wb.value': product['description'],
        'stock.wb.count': product.get('stock', 0),
    }


def get_ali_product_document(product: dict) -> dict:
    return {
        'options.ali.ali_id': product['ali_id'],
        'options.ali.api_key': product['api_key'],
        'options.ali.sku': product['sku'],
        'options.ali.offer_id': product['offer_id_ali'],
        'options.ali.barcode': product['barcode'],
        'merge.ali': True,
        'url.ali': product['ali_url'],
        'image.ali': product['ali_image'],
        'stock.ali.count': product.get('stock', 0),
    }


def get_yandex_product_document(product: dict) -> dict:
    return {
        'options.ya.business_id': product['business_id'],
        'options.ya.campaign_id': product['campaign_id'],
        'options.ya.modelid': product['modelid'],
        'options.ya.api_key': product['api_key'],
        'options.ya.warehouse_id': product['warehouse_id'],
        'options.ya.sku': product['sku'],
        'options.ya.offer_id': product['offer_id_ya'],
        'options.ya.barcode': product['barcode'],
        'merge.ya': True,
        'url.ya': product['ya_url'],
        'image.ya': product['ya_image'],
        'dimension.ya.height': product['height'],
        'dimension.ya.depth': product['depth'],
        'dimension.ya.width': product['width'],
        'dimension.ya.dimension_unit': product['dimension_unit'],
        'weight.ya.value': product['weight'],
        'weight.ya.weight_unit': product['weight_unit'],
        'category.ya.name': product['category'],
        'country.ya.name': product['country'],
        'brend.ya.name': product['brend'],
        'description.ya.value': product['description'],
        'stock.ya.count': product.get('stock', 0),
    }


def get_sber_product_document(product: dict) -> dict:
    return {
        'options.sber.offer_id': product['offer_id_sber'],
        'options.sber.barcode': product['barcode'],
        'merge.sber': True,
        'url.sber': product['sber_url'],
        'image.sber': product['sber_image'],
        'dimension.sber.height': product['height'],
        'dimension.sber.depth': product['depth'],
        'dimension.sber.width': product['width'],
        'dimension.sber.dimension_unit': product['dimension_unit'],
        'weight.sber.value': product['weight'],
        'weight.sber.weight_unit': product['weight_unit'],
        'description.sber.value': product['description'],
        'stock.sber.count': product.get('stock', 0),
    }


def get_product_document_to_update_card_by_market(product: dict, market: str) -> dict:
    match market:
        case 'ozon':
            return get_ozon_product_document(product)
        case 'yandex':
            return get_yandex_product_document(product)
        case 'wb':
            return get_wb_product_document(product)
        case 'ali':
            return get_ali_product_document(product)
        case 'sber':
            return get_sber_product_document(product)
        case _:
            raise ValueError(f'Unsupported market {market}')


def create_new_card(db: Database, product: dict, market: str):
    # Костыль т.к., в названии коллекции с товарами полное название маркетплейса,
    # а в названиях полей карточки сокращенное.
    short_market = 'ya' if market == 'yandex' else market
    # Используется короткое название для яндекса.
    card_document = build_card_document(product=product, market=short_market)
    new_card = db.central.insert_one(card_document)
    new_card_id = str(new_card.inserted_id)
    product_collection = db[f"{product['keys_id']}_{market}"]
    product_collection.update_one(
        filter={'_id': product['_id']},
        update={'$set': {'card_id': new_card_id, 'updated_at': datetime_now_str()}},
    )


def add_product_to_exist_card(
    db: Database,
    product: dict,
    market: str,
    extra_data: dict,
):
    dt_now = datetime_now_str()
    product_document = get_product_document_to_update_card_by_market(product=product, market=market)
    update_query = {
        '$set': product_document | {'updated_at': dt_now},
        '$addToSet': {
            'offer_id': {'$each': extra_data['offer_id']},
            'barcodes': {'$each': extra_data['barcodes']},
            'products': {
                # Используется короткое название для яндекса.
                'market': extra_data['short_market'],
                'product_id': str(product['_id']),
                'keys_id': product['keys_id'],
            },
        },
    }
    db.central.update_one(filter={'_id': extra_data['card_id']}, update=update_query)
    product_collection = db[f"{product['keys_id']}_{market}"]
    product_collection.update_one(
        filter={'_id': product['_id']},
        update={'$set': {'card_id': str(extra_data['card_id']), 'updated_at': dt_now}},
    )


def merge_products_to_card(sellers_ids_with_merge_logs_ids: dict[str, str]):
    init_logging()
    logger = logging.getLogger('merge_cards')
    database2 = dbconsync()
    for seller_id, merge_log_id, in sellers_ids_with_merge_logs_ids.items():
        try:
            seller = settingsResponseEntity(database2.settings.find_one({'_id': ObjectId(seller_id)}))
        except TypeError as exc:
            database2.log.update_one(
                {'_id': ObjectId(merge_log_id)},
                {
                    '$set':
                        {
                            'status': 400,
                            'details': f'Bad request, no such keys. Error: {exc}',
                            'event': 'merge',
                            'keys_id': seller_id,
                            'updated_at': datetime_now_str(),
                        },
                },
            )
            break
        try:
            for seller_market in SUPPORTED_MARKETS:
                if seller[seller_market]:
                    collection = database2[f'{seller_id}_{seller_market}']
                else:
                    continue
                products = collection.find()
                for product in products:
                    # Исключаются пустые строки, если они есть.
                    offer_id = [offer_id for offer_id in product['offer_id'] if offer_id]
                    barcodes = [barcode for barcode in product['barcodes'] if barcode]

                    # Товар уже принадлежит какой-то карточке, его не нужно еще раз объединять.
                    # Выполняется только обновление данных в карточке.
                    if product.get('card_id'):
                        product_document = get_product_document_to_update_card_by_market(
                            product=product,
                            market=seller_market,
                        )
                        update_query = {
                            '$set': product_document | {'updated_at': datetime_now_str()},
                            # Обновляем данные поля т.к., у товара могли измениться артикул или штрихкод.
                            '$addToSet': {
                                'offer_id': {'$each': offer_id},
                                'barcodes': {'$each': barcodes},
                            },
                        }
                        database2.central.update_one(
                            filter={'_id': ObjectId(product['card_id'])},
                            update=update_query,
                        )
                    # Товар новый, еще не принадлежит ни одной из карточек.
                    # Выполняется поиск карточки для объединения по совпадению артикула или штрихкода.
                    else:
                        offer_id_or_barcode_filter = []
                        if offer_id:
                            offer_id_or_barcode_filter.append({'offer_id': {'$in': offer_id}})
                        if barcodes:
                            offer_id_or_barcode_filter.append({'barcodes': {'$in': barcodes}})

                        # Поиск карточки по совпадению артикула или штрихкода выполняется только,
                        # если у товара есть хотя бы одно из этих полей.
                        if offer_id_or_barcode_filter:
                            find_card_filter = {
                                '$and': [
                                    {'$or': offer_id_or_barcode_filter},
                                    {'keys_id': product['keys_id']},
                                ]
                            }
                            cards = list(database2.central.find(filter=find_card_filter))
                            cards_count = len(cards)
                            # Есть одна карточка, с которой товар может быть объединен.
                            # Выполняется обновление карточки: добавляется новый товар
                            # и обновление товара: добавляется идентификатор карточки.
                            if cards_count == 1:
                                card = cards[0]
                                card_products = card.get('products', [])
                                # Костыль т.к., в названии коллекции с товарами полное название маркетплейса,
                                # а в названиях полей карточки сокращенное.
                                short_market = 'ya' if seller_market == 'yandex' else seller_market
                                card_markets = {product['market'] for product in card_products}
                                # В карточке, с которой товар может объединиться, уже есть товар из того же маркетплейса.
                                # Объединение не выполняем, чтобы не затереть данные, а создаем новую карточку.
                                if short_market in card_markets:
                                    create_new_card(db=database2, product=product, market=seller_market)
                                else:
                                    add_product_to_exist_card(
                                        db=database2,
                                        product=product,
                                        market=seller_market,
                                        extra_data={
                                            'short_market': short_market,
                                            'card_id': card['_id'],
                                            'offer_id': offer_id,
                                            'barcodes': barcodes,
                                        }
                                    )
                            # Для товара нет ни одной карточки, с которой он мог бы объединиться (count = 0).
                            # Есть несколько карточек (count > 1), с которыми товар может быть объединен.
                            # Для обоих случаев создается новая карточка. При count > 1
                            # т.к., не ясно с какой из карточек выполнять объединение.
                            else:
                                create_new_card(db=database2, product=product, market=seller_market)
                        # У товара и артикул, и штрихкоды являются пустыми строками
                        # Нельзя найти карточку для объединения. Создается новая карточка.
                        else:
                            create_new_card(db=database2, product=product, market=seller_market)
        except Exception as exc:
            logger.exception(f'Error is occurred during merging products for {seller_id}.')
            database2.log.update_one(
                {'_id': ObjectId(merge_log_id)},
                {
                    '$set': {
                        'status': 400,
                        'details': f'Error is occurred: {exc}',
                        'event': 'merge',
                        'keys_id': seller_id,
                        'updated_at': datetime_now_str(),
                    },
                },
            )
            continue

        database2.log.update_one(
            {'_id': ObjectId(merge_log_id)},
            {
                '$set': {
                    'status': 200,
                    'details': f'Central for {seller_id} merged',
                    'event': 'merge',
                    'keys_id': seller_id,
                    'updated_at': datetime_now_str(),
                },
            },
        )


def move_product_to_separate_card(
    move_product_log_id: str,
    card_id: str,
    seller_id: str,
    product_id: str,
    market: str,
):
    """
    Удаляет товар из текущей карточки и создает новую отдельную карточку для этого товара
    """
    init_logging()
    logger = logging.getLogger('unmerge_product')
    database2 = dbconsync()
    dt_now = datetime_now_str()
    try:
        # Костыль т.к., в названии коллекции с товарами полное название маркетплейса,
        # а в названиях полей карточки сокращенное.
        # Предполагается, что от фронта будет прилетать сокращенное название.
        converted_market = 'yandex' if market == 'ya' else market
        product_collection = database2[f'{seller_id}_{converted_market}']
        product_id_filter = {'_id': ObjectId(product_id)}
        product = product_collection.find_one(product_id_filter)

        # Создается новая карточка для исключаемого товара.
        card_document = build_card_document(product=product, market=market)
        new_card = database2.central.insert_one(card_document)
        # Идентификатор созданной карточки добавляется существующему товару, который исключаем.
        new_card_id = str(new_card.inserted_id)
        product_collection.update_one(
            filter=product_id_filter,
            update={
                '$set': {
                    'card_id': new_card_id,
                    'updated_at': dt_now,
                },
            },
        )
        # Создается новая группа для новой только что созданной карточки.
        inserted_group = database2.central_groups.insert_one(
            {
                'card_ids': [new_card_id],
                'offer_id': card_document['offer_id'],
                'barcodes': card_document['barcodes'],
                'created_at': dt_now,
                'updated_at': dt_now,
            }
        )
        # Идентификатор созданной группы записывается в новую, только что созданную карточку,
        # в которую добавляется исключаемый товар.
        group_id = str(inserted_group.inserted_id)
        database2.central.update_one(
            filter={'_id': ObjectId(new_card_id)},
            update={'$set': {'group_id': group_id}},
        )

        # Обновляется старая карточка: удаляется информация об исключаемом товаре.
        unset_field_names = [
            'merge', 'url', 'image', 'stock', 'dimension', 'category',
            'country', 'brend', 'weight', 'description', 'options',
        ]
        fields_to_unset = {f'{field}.{market}': '' for field in unset_field_names}
        database2.central.update_one(
            filter={'_id': ObjectId(card_id)},
            update={
                '$pull': {
                    'products': {'product_id': product_id, 'keys_id': seller_id, 'market': market},
                },
                '$unset': fields_to_unset,
                '$set': {'updated_at': dt_now},
            }
        )
    except Exception as exc:
        logger.exception(
            f'Error is occurred during moving product {product_id} from card {card_id} '
            f'to separate card for {seller_id}.'
        )
        database2.log.update_one(
            {'_id': ObjectId(move_product_log_id)},
            {
                '$set': {
                    'status': 400,
                    'details': f'Error is occurred: {exc}',
                    'event': f'unmerge',
                    'updated_at': dt_now,
                },
            },
        )
        return
    database2.log.update_one(
        {'_id': ObjectId(move_product_log_id)},
        {
            '$set': {
                'status': 200,
                'details': f'Product {product_id} is removed from current card {card_id}'
                           f'and added to a separate card {new_card_id}',
                'event': f'unmerge',
                'updated_at': dt_now,
            },
        },
    )
