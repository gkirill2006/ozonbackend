import random
from string import ascii_letters, digits

from user.utils import datetime_now_str

SUPPORTED_MARKETS = ['yandex', 'wb', 'ozon', 'ali', 'sber']

API_PREFIX = '/api/v2/'
SYNC_URL = 'sync'


def generate_random_string(string_type: str = 'string') -> str:
    string_len = random.randint(1, 12)
    if string_type == 'id':
        base = digits
    else:
        base = ascii_letters
    return ''.join(random.choices(base, k=string_len))


def generate_random_str_id() -> str:
    return generate_random_string('id')


def generate_random_int_id() -> int:
    return random.randint(100, 10_000)


def dt_now() -> str:
    return datetime_now_str()


def get_random_markets(markets_min_count: int = 2) -> list[str]:
    if markets_min_count < 2:
        raise ValueError('Markets min count cannot be less than 2')
    markets_count = random.randint(markets_min_count, 5)
    return random.sample(SUPPORTED_MARKETS, k=markets_count)
