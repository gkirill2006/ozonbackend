import logging

from bson.objectid import ObjectId
from pymongo.synchronous.cursor import Cursor
from pymongo.synchronous.database import Database

from user.utils import datetime_now_str, dbconsync, init_logging


def build_product_cards_group_auto(cards: Cursor[dict]):
    database2 = dbconsync()
    for card in cards:
        dt_now = datetime_now_str()
        card_id = card['_id']
        group_id = card.get('group_id')

        # Исключаются пустые строки, если они есть.
        offer_id = [offer_id for offer_id in card['offer_id'] if offer_id]
        barcodes = [barcode for barcode in card['barcodes'] if barcode]

        # Карточка уже принадлежит какой-то группе, ее не нужно еще раз группировать.
        # Выполняется только обновление данных в группе.
        if group_id:
            update_query = {
                '$set': {'updated_at': dt_now},
                # Обновляем данные поля т.к., в карточку могли добавиться новые товары
                # или у существующих товаров могли измениться артикул или штрихкод.
                '$addToSet': {
                    'offer_id': {'$each': offer_id},
                    'barcodes': {'$each': barcodes},
                },
            }
            database2.central_groups.update_one(
                filter={'_id': ObjectId(group_id)},
                update=update_query,
            )
        # Карточка новая, еще не принадлежит ни одной из групп.
        # Выполняется поиск группы для объединения по совпадению артикула или штрихкода.
        else:
            find_group_filter = {
                '$or': [
                    {'offer_id': {'$in': offer_id}},
                    {'barcodes': {'$in': barcodes}},
                ]
            }
            groups = list(database2.central_groups.find(filter=find_group_filter))
            groups_count = len(groups)

            # Для карточки нет ни одной группы, к которой можно присоединиться.
            # Создается новая группа.
            if groups_count == 0:
                inserted_group = database2.central_groups.insert_one(
                    {
                        'card_ids': [str(card_id)],
                        'offer_id': offer_id,
                        'barcodes': barcodes,
                        'created_at': dt_now,
                        'updated_at': dt_now,
                    }
                )
                group_id = str(inserted_group.inserted_id)
                database2.central.update_one(
                    filter={'_id': card_id},
                    update={'$set': {'group_id': group_id, 'updated_at': dt_now}},
                )
            # Есть одна группа, в которую карточка может быть добавлена.
            # Выполняется обновление группы: добавляется новая карточка
            # и обновление карточки: добавляется идентификатор группы.
            elif groups_count == 1:
                group_id = groups[0]['_id']
                database2.central_groups.update_one(
                    filter={'_id': group_id},
                    update={
                        '$set': {'updated_at': dt_now},
                        '$addToSet': {
                            'card_ids': str(card_id),
                            'offer_id': {'$each': offer_id},
                            'barcodes': {'$each': barcodes},
                        }
                    }
                )
                database2.central.update_one(
                    filter={'_id': card_id},
                    update={'$set': {'group_id': str(group_id), 'updated_at': dt_now}},
                )
            # Есть несколько групп (count > 1), к которым карточка может быть добавлена.
            # Создается новая группа, содержащая все карточки из найденных и текущую карточку
            # Старые группы удаляются.
            else:
                group_ids = [group['_id'] for group in groups]
                merge_some_groups_to_new_group(
                    db=database2,
                    groups=groups,
                    group_ids=group_ids,
                    card=card,
                )


def merge_some_groups_to_new_group(
    db: Database,
    groups: Cursor[dict] | list[dict],
    group_ids: list[ObjectId],
    card: dict | None = None,
):
    dt_now = datetime_now_str()
    if card is None:
        group_card_ids = []
        groups_offer_id = set()
        groups_barcodes = set()
    else:
        group_card_ids = [str(card['_id'])]
        groups_offer_id = set(card['offer_id'])
        groups_barcodes = set(card['barcodes'])
    for group in groups:
        group_card_ids.extend(group['card_ids'])
        groups_offer_id.update(group['offer_id'])
        groups_barcodes.update(group['barcodes'])
    inserted_group = db.central_groups.insert_one(
        {
            'card_ids': group_card_ids,
            'offer_id': list(groups_offer_id),
            'barcodes': list(groups_barcodes),
            'created_at': dt_now,
            'updated_at': dt_now,
        }
    )
    group_id = str(inserted_group.inserted_id)
    group_card_object_ids = [ObjectId(card_id) for card_id in group_card_ids]
    db.central.update_many(
        filter={'_id': {'$in': group_card_object_ids}},
        update={'$set': {'group_id': group_id, 'updated_at': dt_now}},
    )
    db.central_groups.delete_many({'_id': {'$in': group_ids}})


def group_cards(group_log_id: str, seller_ids: list[str] | None = None, card_ids: list[str] | None = None):
    init_logging()
    logger = logging.getLogger('group_cards')
    database2 = dbconsync()
    try:
        if card_ids is None:
            event = 'autogroup'
            cards = database2.central.find(filter={'keys_id': {'$in': seller_ids}})
            build_product_cards_group_auto(cards)
        else:
            event = 'group'
            card_object_ids = [ObjectId(card_id) for card_id in card_ids]
            cards = database2.central.find(filter={'_id': {'$in': card_object_ids}}, projection=['group_id'])
            group_ids = [ObjectId(card['group_id']) for card in cards]
            groups = database2.central_groups.find(
                filter={'_id': {'$in': group_ids}},
                projection=['card_ids', 'offer_id', 'barcodes'],
            )
            merge_some_groups_to_new_group(db=database2, groups=groups, group_ids=group_ids)
    except Exception as exc:
        logger.exception('Error is occurred during grouping cards.')
        database2.log.update_one(
            {'_id': ObjectId(group_log_id)},
            {
                '$set': {
                    'status': 400,
                    'details': f'Error is occurred: {exc}',
                    'event': event,
                    'updated_at': datetime_now_str(),
                },
            },
        )
        return

    database2.log.update_one(
        {'_id': ObjectId(group_log_id)},
        {
            '$set': {
                'status': 200,
                'details': 'Product cards are grouped',
                'event': event,
                'updated_at': datetime_now_str(),
            },
        },
    )


def move_card_to_separate_group(move_card_log_id: str, group_id: str, card_id: str):
    """
    Удаляет карточку из текущей группы и создает новую отдельную группу для этой карточки
    """
    init_logging()
    logger = logging.getLogger('ungroup_card')
    database2 = dbconsync()
    dt_now = datetime_now_str()
    try:
        card_obj_id = ObjectId(card_id)

        # Создается новая группа для исключаемой карточки.
        current_card = database2.central.find_one({'_id': card_obj_id})
        inserted_group = database2.central_groups.insert_one(
            {
                'card_ids': [card_id],
                'offer_id': current_card['offer_id'],
                'barcodes': current_card['barcodes'],
                'created_at': dt_now,
                'updated_at': dt_now,
            }
        )
        # Идентификатор созданной группы записывается в существующую карточку, которую исключаем.
        separate_group_id = str(inserted_group.inserted_id)
        database2.central.update_one(
            filter={'_id': card_obj_id},
            update={'$set': {'group_id': separate_group_id, 'updated_at': dt_now}},
        )

        # Выполняется проверка содержатся ли артикулы и штрихкоды из исключаемой карточки
        # в других карточках из этой же группы.
        group_cards = database2.central.find({'group_id': group_id})
        group_cards_offer_ids = set()
        group_cards_barcodes = set()
        for card in group_cards:
            group_cards_offer_ids.update(card['offer_id'])
            group_cards_barcodes.update(card['barcodes'])
        offer_ids_to_remove_from_group = set(current_card['offer_id']).difference(group_cards_offer_ids)
        barcodes_to_remove_from_group = set(current_card['barcodes']).difference(group_cards_barcodes)
        # Если в исключаемой карточке есть артикул или штрихкод, которого нет в других карточках этой же группы,
        # то этот артикул/штрихкод будет удален из группы.
        offer_id_and_barcodes_to_remove = {}
        if offer_ids_to_remove_from_group:
            offer_id_and_barcodes_to_remove['offer_id'] = list(offer_ids_to_remove_from_group)
        if barcodes_to_remove_from_group:
            offer_id_and_barcodes_to_remove['barcodes'] = list(barcodes_to_remove_from_group)
        remove_offer_id_and_barcodes_query = (
            {'$pullAll': offer_id_and_barcodes_to_remove} if offer_id_and_barcodes_to_remove else {}
        )

        # Обновляется старая группа: удаляется идентификатор и артикулы/штрихкоды исключаемой карточки,
        # которых нет в других карточках этой же группы, если такие есть.
        database2.central_groups.update_one(
            filter={'_id': ObjectId(group_id)},
            update={
                '$pull': {'card_ids': card_id},
                **remove_offer_id_and_barcodes_query,
                '$set': {'updated_at': dt_now},
            }
        )
    except Exception as exc:
        logger.exception(
            f'Error is occurred during moving card {card_id} from group {group_id} '
            f'to separate group.'
        )
        database2.log.update_one(
            {'_id': ObjectId(move_card_log_id)},
            {
                '$set': {
                    'status': 400,
                    'details': f'Error is occurred: {exc}',
                    'event': 'ungroup',
                    'updated_at': dt_now,
                },
            },
        )
        return

    database2.log.update_one(
        {'_id': ObjectId(move_card_log_id)},
        {
            '$set': {
                'status': 200,
                'details': f'Card {card_id} is removed from current group {group_id}'
                           f'and added to a separate group {separate_group_id}',
                'event': 'ungroup',
                'updated_at': dt_now,
            },
        },
    )
