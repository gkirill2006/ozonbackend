from collections import defaultdict

import models
from tests.makers.base import dt_now, generate_random_string


def make_card(products: dict[str, dict], keys_id: str) -> dict:
    card = defaultdict(dict)
    offer_ids, barcodes, card_products = set(), set(), []
    for market, product in products.items():
        converted_market = 'ya' if market == 'yandex' else market
        # убираем пустые строки, если они есть, как и при реальном создании карточек при сшитии
        product_offer_id = [offer_id for offer_id in product['offer_id'] if offer_id]
        product_barcodes = [barcode for barcode in product['barcodes'] if barcode]
        offer_ids.update(product_offer_id)
        barcodes.update(product_barcodes)
        card_products.append(
            {
                'market': market,
                'product_id': str(product['_id']),
                'keys_id': product['keys_id'],
            }
        )

        weight = product.get('weight', 0)
        if weight:
            card['weight'].update(
                {
                    converted_market: {
                        'value': product['weight'],
                        'weight_unit': product['weight_unit'],
                    },
                },
            )

        description = product.get('description', '')
        if description:
            card['description'].update(
                {
                    converted_market: {'value': product['description']},
                }
            )

        brend = product.get('brend', '')
        if brend:
            card['brend'].update(
                {
                    converted_market: {'name': product['brend']},
                }
            )

        country = product.get('country', '')
        if country:
            card['country'].update(
                {
                    converted_market: {'name': product['country']},
                }
            )

        category = product.get('category', '')
        if category:
            card['category'].update(
                {
                    converted_market: {'name': product['category']},
                }
            )

        stock = product.get('stock', 0)
        if stock:
            card['stock'].update(
                {
                    converted_market: {'count': product['stock']},
                }
            )

        image = product.get(f'{converted_market}_image', '')
        if image:
            card['image'].update(
                {
                    converted_market: image,
                }
            )

        url = product.get(f'{converted_market}_url', '')
        if url:
            card['url'].update(
                {
                    converted_market: url,
                }
            )

        card['merge'].update({converted_market: True})

        card['dimension'].update(
            {
                converted_market: models.DimensionD(**product).model_dump(exclude_unset=True),
            }
        )

        product['offer_id'] = product[f'offer_id_{converted_market}']
        card['options'].update(
            models.Options(**{converted_market: product}).model_dump(exclude_unset=True),
        )

    card.update(
        {
            'keys_id': keys_id,
            'name': generate_random_string(),
            'company': generate_random_string(),
            'updated_at': dt_now(),
            'created_at': dt_now(),
            'offer_id': list(offer_ids),
            'barcodes': list(barcodes),
            'products': card_products,
        }
    )
    return dict(card)
