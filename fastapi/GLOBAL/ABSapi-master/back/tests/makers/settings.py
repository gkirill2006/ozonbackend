from datetime import datetime
from tests.makers.base import (
    SUPPORTED_MARKETS,
    dt_now,
    generate_random_int_id,
    generate_random_str_id,
    generate_random_string,
)


def make_ozon_settings() -> dict[str, str]:
    return {
        'client_id': generate_random_str_id(),
        'api_key': generate_random_string(),
    }


def make_yandex_settings() -> dict[str, str]:
    return {
        'campaign_id': generate_random_str_id(),
        'business_id': generate_random_str_id(),
        # чтобы не ломались тесты тут инт хотя в реальных данных тут строка
        # (в реальных данных в settings строка, а в товаре и карточке инт)
        'warehouse_id': generate_random_int_id(),
        'api_key': generate_random_string()
    }


def make_wb_settings() -> dict[str, str]:
    return {
        # чтобы не ломались тесты тут инт хотя в реальных данных тут строка
        # (в реальных данных в settings строка, а в товаре и карточке инт)
        'warehouse_id': generate_random_int_id(),
        'api_key': generate_random_string(),
    }


def make_sber_settings() -> dict[str, str]:
    return {
        'token': generate_random_string(),
        'merchant_id': generate_random_str_id(),
    }


def make_ali_settings() -> dict[str, str]:
    return {'token': generate_random_string()}


def make_market_settings(market: str) -> dict[str, str]:
    match market:
        case 'yandex':
            return {market: make_yandex_settings()}
        case 'ozon':
            return {market: make_ozon_settings()}
        case 'ali':
            return {market: make_ali_settings()}
        case 'wb':
            return {market: make_wb_settings()}
        case 'sber':
            return {market: make_sber_settings()}
        case _:
            return {}


def make_settings(markets: list[str]) -> dict[str, str | datetime]:
    seller_settings = {
        'company': generate_random_string(),
        'created_at': dt_now(),
        'updated_at': dt_now(),
    }
    for market in markets:
        seller_settings.update(make_market_settings(market))
    for market in set(SUPPORTED_MARKETS).difference(markets):
        seller_settings.update({market: None})
    return seller_settings
