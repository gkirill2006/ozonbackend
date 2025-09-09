from typing import Generator

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.database import Database

from config import settings
from main import app
from tests.makers.base import generate_random_string, generate_random_str_id
from tests.makers.card import make_card
from tests.makers.product import make_product
from tests.makers.settings import make_settings
from user.oauth2 import require_user


@pytest.fixture(scope='function', autouse=True)
def mock_env_is_test(monkeypatch):
    # переопределяем settings в текущем процессе
    settings.IS_TEST = True
    settings.DB_NAME = settings.TEST_DB_NAME
    # для переопределения settings в подпроцессах
    monkeypatch.setenv('IS_TEST', 'True')


@pytest.fixture(scope='function')
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as client:
        def override_require_user():
            yield ''

        app.dependency_overrides[require_user] = override_require_user
        yield client
        app.dependency_overrides.clear()


@pytest.fixture(scope='function')
def mongodb():
    client = MongoClient(settings.DB_URI2)
    yield client[settings.DB_NAME]
    client.drop_database(settings.DB_NAME)


@pytest.fixture(scope='function')
def settings_factory(mongodb: Database):
    def create(
        markets: list[str],
    ):
        seller_settings = make_settings(markets)
        new_settings = mongodb.settings.insert_one(seller_settings)
        new_settings_id = new_settings.inserted_id
        settings = mongodb.settings.find_one({'_id': new_settings_id})
        settings['_id'] = str(settings['_id'])
        return settings

    yield create


@pytest.fixture(scope='function')
def product_factory(mongodb: Database):
    def create(
        settings: dict,
        market: str,
        offer_id: str | None = None,
        barcode: str | None = None,
    ):
        new_offer_id = generate_random_string() if offer_id is None else offer_id
        new_barcode = generate_random_str_id() if barcode is None else barcode
        product = make_product(market=market, settings=settings, offer_id=new_offer_id, barcode=new_barcode)
        product_collection = mongodb[f'{settings['_id']}_{market}']
        inserted_product = product_collection.insert_one(product)
        inserted_product_id = inserted_product.inserted_id
        new_product = product_collection.find_one({'_id': inserted_product_id})
        return new_product

    yield create


@pytest.fixture(scope='function')
def card_factory(mongodb: Database):
    def create(
        products: dict,
        keys_id: str,
        only_card_id: bool = True,
    ) -> dict | ObjectId:
        card = make_card(products=products, keys_id=keys_id)
        inserted_card = mongodb.central.insert_one(card)
        inserted_card_id = inserted_card.inserted_id
        if only_card_id:
            return inserted_card_id
        else:
            new_card = mongodb.central.find_one({'_id': inserted_card_id})
            return new_card

    yield create
