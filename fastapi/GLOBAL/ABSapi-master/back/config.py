from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(".env"))


class Settings(BaseSettings):

    REFRESH_TOKEN_EXPIRES_IN: int
    ACCESS_TOKEN_EXPIRES_IN: int
    JWT_ALGORITHM: str
    JWT_PUBLIC_KEY: str
    JWT_PRIVATE_KEY: str

    MONGO_INITDB_ROOT_USERNAME: str
    MONGO_INITDB_ROOT_PASSWORD: str
    DB_URI: str
    DB_URI2: str
    DB_NAME: str
    TEST_DB_NAME: str = 'test_abs'
    IS_TEST: bool = False

    # SBERBANK
    LOGIN: str
    PASSWD: str

    # DOMAIN
    DOMAIN: str
    URL_DOMAIN: str

    ADMIN_LOGIN: str
    ADMIN_PASSWORD: str

    OZON_LIST_URL: str
    OZON_INFO_LIST_URL: str
    OZON_STOCK_FBS_URL: str
    OZON_STOCK_UPDATE: str
    OZON_ATRIBUTES: str
    OZON_CAT_TREE: str
    WB_STOCKS_URL: str
    WB_GOODS: str
    WB_CATS: str
    WB_SUBJECTS: str
    WB_COUNTRY: str
    YA: str
    YA_BIZ: str
    SBER: str
    ALI_GOODS: str
    ALI_UPDATE: str

    LOG_MODE: str
    TIMEZONE: str = 'Europe/Moscow'
    model_config = SettingsConfigDict(case_sensitive=True)

    @model_validator(mode='after')
    def set_db_name(self):
        if self.IS_TEST:
            self.DB_NAME = self.TEST_DB_NAME
        return self


settings = Settings()
