import itertools
from typing import Any
from pydantic import BaseModel, constr, Field, GetCoreSchemaHandler
from datetime import datetime
from bson import ObjectId
from pydantic_core import core_schema

class PyObjectId(str):
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(ObjectId),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda x: str(x)),
        )

    @classmethod
    def validate(cls, value) -> ObjectId:
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")

        return ObjectId(value)


class CommaSeparatedList(list):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_before_validator_function(cls.validate, handler(list[PyObjectId]))

    @classmethod
    def validate(cls, value: Any):
        if isinstance(value, str):
            value = map(str.strip, value.split(','))
        elif isinstance(value, list) and all(isinstance(x, str) for x in value):
            value = map(str.strip, itertools.chain.from_iterable(x.split(',') for x in value))
        return value


class Tokens(BaseModel):
    status: str


class TokenResponse(BaseModel):
    status: str
    access_token: str

    class Config:
        from_attributes = True


class DefStatus(BaseModel):
    status: str


class LoginUserSchema(BaseModel):
    username: str
    password: constr(min_length=8)


class YandexAPi(BaseModel):
    campaign_id: str | None = None
    business_id: str | None = None
    warehouse_id: str | None = None
    api_key: str | None = None


class OzonAPi(BaseModel):
    client_id: str | None = None
    api_key: str | None = None


class WBAPi(BaseModel):
    warehouse_id: str | None = None
    api_key: str | None = None


class SberAPi(BaseModel):
    token: str | None = None
    merchant_id: str | None = None

class AliAPi(BaseModel):
    token: str | None = None


class SetKeySchema(BaseModel):
    company: str
    ozon: OzonAPi | None = None
    yandex: YandexAPi | None = None
    wb: WBAPi | None = None
    sber: SberAPi | None = None
    ali: AliAPi | None = None

class SetKeyRSchema(SetKeySchema):
    id: str
    created_at: str
    updated_at: str
    pass


class SetKeyResponseSchema(BaseModel):
    status: str
    settings: SetKeyRSchema


class SetListKeyResponseSchema(BaseModel):
    status: str
    settings: list[SetKeyRSchema]


class LogResponseSchema(BaseModel):
    id: PyObjectId = Field(alias="_id")
    status: int
    keys_id: str | None = None
    group_companies: str | None = None
    group_items: str | None = None
    details: str | None = None
    event: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    progress: int | None = None

    class Config:
        arbitrary_types_allowed = True

class LogListResponseSchema(BaseModel):
    logs: list[LogResponseSchema]


class LogListRResponseSchema(BaseModel):
    logs: list[LogResponseSchema]
    pages: str


class LogListRequestSchema(BaseModel):
    ids: list[str]


class WbWarehouseResponseSchema(BaseModel):
    id: str
    name: str
    keys_id: str
    company: str
    height: int
    depth: int
    width: int
    dimension_unit: str
    weight: int
    weight_unit: str
    category: str
    country: str
    brend: str
    description: str | None = ""
    stock: int
    offer_id: list
    offer_id_wb: str
    wb_id: int
    wb_url: str
    wb_image: str
    sku: int
    barcode: str
    barcodes: list
    warehouse_id: int
    updated_at: str
    created_at: str


class WbListWarehouseResponseSchema(BaseModel):
    status: str
    warehouse: list[WbWarehouseResponseSchema]
    pages: str


class OzonWarehouseResponseSchema(BaseModel):
    id: str
    name: str
    keys_id: str
    company: str
    height: int
    depth: int
    width: int
    dimension_unit: str
    weight: int
    weight_unit: str
    category: str
    country: str
    brend: str
    description: str
    stock: int
    ozon_id: int
    ozon_url: str
    offer_id: list
    offer_id_ozon: str
    ozon_image: str
    sku: int
    barcode: str
    barcodes: list
    warehouse_id: int
    updated_at: str
    created_at: str


class OzonListWarehouseResponseSchema(BaseModel):
    status: str
    warehouse: list[OzonWarehouseResponseSchema]
    pages: str


class YaWarehouseResponseSchema(BaseModel):
    id: str
    name: str
    keys_id: str
    company: str
    height: int
    depth: int
    width: int
    dimension_unit: str
    weight: int
    weight_unit: str
    category: str
    country: str
    brend: str
    description: str
    business_id: str
    campaign_id: str
    stock: int
    offer_id: list
    offer_id_ya: str
    modelid: int
    ya_url: str
    ya_image: str
    sku: int
    barcode: str
    barcodes: list
    warehouse_id: int
    updated_at: str
    created_at: str


class YaListWarehouseResponseSchema(BaseModel):
    status: str
    warehouse: list[YaWarehouseResponseSchema]
    pages: str

class AliWarehouseResponseSchema(BaseModel):
    id: str
    name: str
    keys_id: str
    company: str
    stock: int
    offer_id: list
    offer_id_ali: str
    ali_url: str
    ali_image: str
    sku: int
    barcode: str
    barcodes: list
    updated_at: str
    created_at: str


class AliListWarehouseResponseSchema(BaseModel):
    status: str
    warehouse: list[AliWarehouseResponseSchema]
    pages: str


class Uni(BaseModel):
    ya: str | None = None
    ozon: str | None = None
    wb: str | None = None
    sber: str | None = None
    ali: str | None = None


class StateStockOptions(BaseModel):
    state: bool | None = False


class StockOptions(StateStockOptions):
    count: int | None = None
    pass


class StockUOptions(BaseModel):
    count: int | None = None


class Stock(BaseModel):
    ya: StockOptions | None = None
    ozon: StockOptions | None = None
    wb: StockOptions | None = None
    sber: StockOptions | None = None
    ali: StockOptions | None = None


class StockU(BaseModel):
    ya: StockUOptions | None = None
    ozon: StockUOptions | None = None
    wb: StockUOptions | None = None
    sber: StockUOptions | None = None
    ali: StockUOptions | None = None


class StateStock(BaseModel):
    ya: StateStockOptions | None = None
    ozon: StateStockOptions | None = None
    wb: StateStockOptions | None = None
    sber: StateStockOptions | None = None
    ali: StateStockOptions | None = None


class Merge(BaseModel):
    ya: bool | None = False
    ozon: bool | None = False
    wb: bool | None = False
    sber: bool | None = False
    ali: bool | None = False


class YaOptions(BaseModel):
    business_id: str | None = None
    campaign_id: str | None = None
    modelid: int | None = None
    warehouse_id: int | None = None
    api_key: str | None = None
    sku: int | None = None
    offer_id: str | None = None
    barcode: str | None = None


class OzonOptions(BaseModel):
    ozon_id: int | None = None
    warehouse_id: int | None = None
    sku: int | None = None
    offer_id: str | None = None
    barcode: str | None = None
    api_key: str | None = None
    client_id: str | None = None


class WbOptions(BaseModel):
    wb_id: int | None = None
    wb_token: str | None = None
    warehouse_id: int | None = None
    sku: int | None = None
    offer_id: str | None = None
    barcode: str | None = None


class AliOptions(BaseModel):
    ali_id: int | None = None
    api_key: str | None = None
    sku: str | None = None
    offer_id: str | None = None
    barcode: str | None = None

class SberOptions(BaseModel):
    offer_id: str | None = None
    barcode: str | None = None

class Options(BaseModel):
    ya: YaOptions | None = None
    ozon: OzonOptions | None = None
    wb: WbOptions | None = None
    ali: AliOptions | None = None
    sber: SberOptions | None = None

class GroupItems(BaseModel):
    keys_id: str
    product_id: str

class DimensionD(BaseModel):
    height: int | None = None
    depth: int | None = None
    width: int | None = None
    dimension_unit: str | None = None

class Dimension(BaseModel):
    ya: DimensionD | None = None
    ozon: DimensionD | None = None
    wb: DimensionD | None = None
    ali: DimensionD | None = None
    sber: DimensionD | None = None


class WeightD(BaseModel):
    value: int | None = None
    weight_unit: str | None = None

class Weight(BaseModel):
    ya: WeightD | None = None
    ozon: WeightD | None = None
    wb: WeightD | None = None
    ali: WeightD | None = None
    sber: WeightD | None = None

class NameExtra(BaseModel):
    name: str | None = None

class ExtraFields(BaseModel):
    ya: NameExtra | None = None
    ozon: NameExtra | None = None
    wb: NameExtra | None = None
    ali: NameExtra | None = None
    sber: NameExtra | None = None

class ValueExtra(BaseModel):
    value: Any | None = None


class Description(BaseModel):
    ya: ValueExtra | None = None
    ozon: ValueExtra | None = None
    wb: ValueExtra | None = None
    ali: ValueExtra | None = None
    sber: ValueExtra | None = None


class Tag(BaseModel):
    name: str
    color: str


class CentralwarehouseResponseSchema(BaseModel):
    id: str
    name: str
    keys_id: str
    company: str
    merge: Merge
    tag: Tag | None = None
    url: Uni
    image: Uni
    stock: Stock
    dimension: Dimension | None = None
    category: ExtraFields | None = None
    country: ExtraFields | None = None
    brend: ExtraFields | None = None
    weight: Weight | None = None
    description: Description | None = None
    groups: list[GroupItems] | None = None
    offer_id: list | None = None
    barcodes: list | None = None
    updated_at: str | None = None
    created_at: str | None = None


class CentralListWarehouseResponseSchema(BaseModel):
    status: str
    warehouse: list[CentralwarehouseResponseSchema]
    pages: str


class CardProductShortSchema(BaseModel):
    market: str
    keys_id: str
    product_id: str


class ProductBaseSchema(BaseModel):
    name: str
    keys_id: str
    company: str
    merge: Merge
    tag: Tag | None = None
    url: Uni
    image: Uni
    stock: Stock
    dimension: Dimension | None = None
    category: ExtraFields | None = None
    country: ExtraFields | None = None
    brend: ExtraFields | None = None
    weight: Weight | None = None
    description: Description | None = None
    manual_group_id: str | None = None
    manual_group: list[GroupItems] | None = None
    groups: list[GroupItems] | None = None
    offer_id: list | None = None
    barcodes: list | None = None
    group_id: str | None = None
    products: list[CardProductShortSchema] | None = None
    updated_at: str | None = None
    created_at: str | None = None


class ProductUpBaseSchema(BaseModel):
    id: PyObjectId = Field(alias="_id")
    name: str
    keys_id: str
    company: str
    options: Options
    merge: Merge
    tag: Tag | None = None
    url: Uni
    image: Uni
    stock: Stock
    dimension: Dimension | None = None
    category: ExtraFields | None = None
    country: ExtraFields | None = None
    brend: ExtraFields | None = None
    weight: Weight | None = None
    description: Description | None = None
    manual_group_id: str | None = None
    manual_group: list[GroupItems] | None = None
    groups: list[GroupItems] | None = None
    offer_id: list | None = None
    barcodes: list | None = None
    updated_at: str | None = None
    created_at: str | None = None


class ProductBaseUpdateSchema(BaseModel):
    name: str
    keys_id: str
    company: str
    options: Options
    merge: Merge
    tag: Tag | None = None
    url: Uni
    image: Uni
    stock: Stock
    groups: list[GroupItems] | None = None
    offer_id: list | None = None
    barcodes: list | None = None
    updated_at: str | None = None
    created_at: str | None = None


class ProductResponseSchema(ProductBaseSchema):
    id: PyObjectId = Field(alias="_id")


class ProductListResponseSchema(BaseModel):
    status: str
    products: list[ProductResponseSchema]
    pages: str


class Query(BaseModel):
    id: str
    last_query: str
    created_at: datetime


class QueryList(BaseModel):
    status: str
    queries: list[Query]
    pages: str

class MergeSchema(BaseModel):
    key_id: str


class MergeListSchema(BaseModel):
    merge: list[MergeSchema]


class UpdateListSchema(BaseModel):
    keys_ids: list[str]


class GroupListSchema(BaseModel):
    keys_ids: list[str] | None = None
    card_ids: list[str] | None = None

class UnmergeSchema(BaseModel):
    card_id: str
    product_id: str
    keys_id: str
    market: str

class UngroupSchema(BaseModel):
    group_id: str
    card_id: str


class ProductStateRequest(BaseModel):
    stock: StateStock | None = None
    product: GroupItems | None = None


class ProductsStateRequest(BaseModel):
    groups: list[ProductStateRequest]


class ProductUpdateRequest(BaseModel):
    stock: StockU | None = None
    product: GroupItems | None = None


class ProductsUpdateRequest(BaseModel):
    groups: list[ProductUpdateRequest]


class SberPush(BaseModel):
    data: dict
    meta: dict
    success: int

class SberDiscounts(BaseModel):
    discountType: str
    discountDescription: str
    discountAmount: float

class SberItems(BaseModel):
    itemIndex: str
    goodsId: str
    offerId: str
    itemName: str
    price: float
    finalPrice: float
    discounts: list[SberDiscounts]
    quantity: int
    taxRate: str
    reservationPerformed: bool
    isDigitalMarkRequired: bool

class SberLabel(BaseModel):
    deliveryId: str
    region: str
    city: str
    address: str
    fullName: str
    merchantName: str
    merchantId: int
    shipmentId: str
    shippingDate: str
    deliveryType: str
    labelText: str

class SberShipping(BaseModel):
    shippingDate: str
    shippingPoint: int

class SberShipment(BaseModel):
    shipmentId: str
    shipmentDate: str
    items: list[SberItems]
    label: SberLabel
    shipping: SberShipping
    fulfillmentMethod: str


class DataSberObject(BaseModel):
    merchantId: int
    shipments: list[SberShipment]


class MetaSberObject(BaseModel):
    source: str


class SberOrderSchema(BaseModel):
    data: DataSberObject
    meta: MetaSberObject


class SberCancelItems(BaseModel):
    itemIndex: str
    goodsId: str
    offerId: str



class SberCancelShipment(BaseModel):
    shipmentId: str
    items: list[SberCancelItems]
    fulfillmentMethod: str


class SberDataCancelObject(BaseModel):
    shipments: list[SberCancelShipment]
    merchantId: int


class SberCancelSchema(BaseModel):
    meta: dict
    data: SberDataCancelObject


class UpdatePayload(BaseModel):
    offer_id: str
    count: int
    ozon: bool
    yandex: bool
    sber: bool
    ali: bool
    wb: bool

class ListUpdatePayload(BaseModel):
    data: list[UpdatePayload]

class CheckFile(BaseModel):
    offer_id: Any | None = None
    image: str | None = None
    name: str | None = None
    count_add: Any | None = None
    count_yandex_now: Any | None = None
    count_yandex_was: Any | None = None
    count_ozon_now: Any | None = None
    count_ozon_was: Any | None = None
    count_ali_now: Any | None = None
    count_ali_was: Any | None = None
    count_sber_now: Any | None = None
    count_sber_was: Any | None = None
    count_wb_now: Any | None = None
    count_wb_was: Any | None = None
    ozon: Any | None = None
    ali: Any | None = None
    wb: Any | None = None
    sber: Any | None = None
    yandex: Any | None = None
    check_offer_id: bool | None = None
    check_count: bool | None = None
    check_yandex: bool | None = None
    check_ali: bool | None = None
    check_sber:bool | None = None
    check_wb: bool | None = None
    check_ozon: bool | None = None
    check_negative_count_yandex: bool | None = None
    check_negative_count_ali: bool | None = None
    check_negative_count_sber: bool | None = None
    check_negative_count_wb: bool | None = None
    check_negative_count_ozon: bool | None = None
    check_offer_id_miss: bool | None = None

class ResponseCheckFile(BaseModel):
    status: str
    data: list[CheckFile]
    error_count: int
    operation: str
    date: datetime
    user_id: str
    keys_id: str
    history_id: str | None = None
    send: bool | None = False
    send_date: datetime | None = None
    rollback: bool | None = False
    split: bool | None = False
    splited_history_id: str | None = None
    error_history_id: str | None = None

class UploadPayloadError(BaseModel):
    status: str
    data: list[CheckFile]
    error_count: int
    operation: str
    date: datetime
    user_id: str
    keys_id: str
    history_id: str



class ResponseDataCheckFile(ResponseCheckFile):
    id: PyObjectId = Field(alias="_id")
    pass

class ListResponseCheckFile(BaseModel):
    status: str
    check_history: list[ResponseDataCheckFile]


class ProductTag(BaseModel):
    name: str
    product_id: str
    color: str | None = None


class ProductTagList(BaseModel):
    taglist: list[ProductTag]


class LogIDs(BaseModel):
    log_ids: list[str]


class LogProgressResponseSchema(BaseModel):
    id: PyObjectId = Field(alias='_id')
    progress: int = 0


class LogProgressDetailResponseSchema(BaseModel):
    progress: int = 0
    status: int
    updated_at: datetime | None = None
    details: str | None = None
