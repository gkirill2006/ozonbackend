import pandas as pd
from PIL import Image
from typing import Annotated
from bson.objectid import ObjectId
from pymongo import TEXT
from datetime import UTC, datetime, timedelta, date
from pymongo.collection import ReturnDocument
from sse_starlette.sse import EventSourceResponse
from user.userSerializers import *
from user.oauth2 import AuthJWT, require_user
from fastapi import APIRouter, Response, Depends, HTTPException, Request, UploadFile, status, Query as FastAPIQuery
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.exceptions import ResponseValidationError
from bson.errors import InvalidId
from pathlib import Path
import os
import io
import math
import multiprocessing
import time
import asyncio
from models import *
from config import settings
from user import utils

from apps.groups.utils import group_cards, move_card_to_separate_group
from apps.cards.utils import merge_products_to_card, move_product_to_separate_card

user_router = APIRouter()
product_router = APIRouter()
settings_router = APIRouter()
sync_router = APIRouter()
warehouse_router = APIRouter()
logs_router = APIRouter()
sber = APIRouter()
# USER


@user_router.post('/login', response_model=TokenResponse)
async def login_for_admin(request: Request, payload: LoginUserSchema, response: Response,
                          authorize: AuthJWT = Depends()):
    # Check if the user exist
    db_user = await request.app.database.user.find_one({'username': payload.username.lower()})
    if not db_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail='Incorrect username or password')
    user = userEntity(db_user)
    user_id = user["id"]
    # Check if the password is valid
    if not utils.verify_password(payload.password, user['password']):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail='Incorrect username or password')
    # Create access token
    access_token = await authorize.create_access_token(
        subject=str(user["id"]), expires_time=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRES_IN))
    # Create refresh token
    refresh_token = await authorize.create_refresh_token(
        subject=str(user["id"]), expires_time=timedelta(minutes=settings.REFRESH_TOKEN_EXPIRES_IN))
    # Store refresh and access tokens in cookie
    response.set_cookie('access_token', access_token, settings.ACCESS_TOKEN_EXPIRES_IN * 60,
                        settings.ACCESS_TOKEN_EXPIRES_IN * 60, '/',
                        None, True, True, 'none')
    response.set_cookie('refresh_token', refresh_token,
                        settings.REFRESH_TOKEN_EXPIRES_IN * 60, settings.REFRESH_TOKEN_EXPIRES_IN * 60, '/',
                        None, True, True, 'none')
    response.set_cookie('logged_in', 'True', settings.ACCESS_TOKEN_EXPIRES_IN * 60,
                        settings.ACCESS_TOKEN_EXPIRES_IN * 60, '/',
                        None, True, True, 'none')
    date = utils.datetime_now_str()
    request.app.database.logs.insert_one({"user_id": user_id,
                                          "action": "вход",
                                          "details": f"вход осуществлен",
                                          "date": date})
    # Send both access
    return {'status': 'success', 'access_token': access_token}


@user_router.get('/refresh', response_model=TokenResponse)
async def refresh_token(request: Request, response: Response,  authorize: AuthJWT = Depends()):
    try:
        await authorize.jwt_refresh_token_required()
        user_id = await authorize.get_jwt_subject()
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail='Could not refresh access token')
        user = userEntity(await request.app.database.user.find_one({"_id": ObjectId(user_id)}))
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail='The user belonging to this token no logger exist')
        access_token = await authorize.create_access_token(
            subject=str(user["id"]), expires_time=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRES_IN))
    except Exception as e:
        error = e.__class__.__name__
        if error == 'MissingTokenError':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail='Please provide refresh token')
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error)

    response.set_cookie('access_token', access_token, settings.ACCESS_TOKEN_EXPIRES_IN * 60,
                        settings.ACCESS_TOKEN_EXPIRES_IN * 60, '/',
                        None, True, True, 'none')
    response.set_cookie('logged_in', 'True', settings.ACCESS_TOKEN_EXPIRES_IN * 60,
                        settings.ACCESS_TOKEN_EXPIRES_IN * 60, '/',
                        None, True, True, 'none')
    return {"status": "success", "access_token": access_token}


@user_router.get('/logout', response_model=DefStatus)
async def logout(response: Response, authorize: AuthJWT = Depends()):
    # uset cookies - logout
    await authorize.unset_jwt_cookies()
    response.set_cookie('logged_in', '', -1)
    return {'status': 'success'}


# PRODUCT

@product_router.get('', response_model=ProductListResponseSchema)
async def list_products_or_get_product_by_id(request: Request,  product_id: str | None = None,
                                             search: str | None = None, tag: str | None = None,
                                             limit: int = 40, page: int = 1,
                                             user_id: str = Depends(require_user)):
    if search and product_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="You can add only one target at same time. Product_id or tag or search")
    if search and tag:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="You can add only one target at same time. Product_id or tag or search")
    if product_id and tag:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="You can add only one target at same time. Product_id or tag or search")
    insert = None
    if tag:
        insert = {"tag.name": {"$regex": tag, "$options": 'i'}}
    if product_id:
        try:
            insert = {"_id": ObjectId(product_id)}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="No such id")
    if search:
        insert = {"$or":[
            {"barcodes": {"$regex": search, "$options" : 'i'}},
            {"name": {"$regex": search, "$options" : 'i'}},
            {"offer_id": {"$regex": search, "$options" : 'i'}},
            {"tag.name": {"$regex": search, "$options": 'i'}},
        ]}
    products = await (request.app.database.central.find(insert).skip(limit * (page - 1)).
                                     limit(limit)).to_list(None)
    if products:
        count = await (request.app.database.central.find(insert).to_list(None))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "products": products, "pages": str(pages)}
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Not found")

@product_router.get('/last_query', response_model=QueryList)
async def list_last_query(request: Request,  limit: int = 10, page: int = 1,
                                             user_id: str = Depends(require_user)):
    queries = queryListResponseEntity(await (request.app.database.last_query.find().skip(limit * (page - 1)).
                                     limit(limit)).sort({"created_at": -1}).to_list(None))
    if queries:
        count = queryListResponseEntity(await (request.app.database.last_query.find().to_list(None)))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "queries": queries, "pages": str(pages)}
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Not found")

@product_router.put('/last_query', response_model=DefStatus)
async def put_last_query(request: Request, last_query: str,  user_id: str = Depends(require_user)):
    await request.app.database.last_query.insert_one({"last_query": last_query, "created_at": utils.datetime_now_str()})
    return {"status": "success"}

@product_router.delete('/last_query', response_model=DefStatus)
async def delete_last_query(request: Request, user_id: str = Depends(require_user)):
    await request.app.database.last_query.drop()
    return {"status": "success"}


@product_router.put('/state')
async def product_state(request: Request, payload: ProductsStateRequest, user_id: str = Depends(require_user)):
    asyncio.sleep(3) #input lag
    query_items = payload.model_dump()['groups']
    product_ids = list()
    for i in query_items:
        query_filter = {"_id": ObjectId(i['product']['product_id'])}
        setitems = {}
        try:
            setitems["stock.ya.state"] = i['stock']['ya']['state']
        except TypeError:
            pass
        try:
            setitems["stock.ozon.state"] = i['stock']['ozon']['state']
        except TypeError:
            pass
        try:
            setitems["stock.sber.state"] = i['stock']['sber']['state']
        except TypeError:
            pass
        try:
            setitems["stock.wb.state"] = i['stock']['wb']['state']
        except TypeError:
            pass
        try:
            setitems["stock.ali.state"] = i['stock']['ali']['state']
        except TypeError:
            pass
        await request.app.database.central.update_one(query_filter, {"$set": setitems}, upsert=True)
        product_ids.append(i['product']['product_id'])

    return {"status": "success", "product_ids": product_ids}

@product_router.put('/tag', response_model=ProductListResponseSchema)
async def product_tag(request: Request, payload: ProductTagList, user_id: str = Depends(require_user)):
    taglist = payload.model_dump()['taglist']
    updated_list = []
    for d in taglist:
        try:
            product_id = ObjectId(d['product_id'])
        except InvalidId:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{d['product_id']} is InvalidId",
            )
        tag_name = d['name']
        tag_color = d.get('color')
        if tag_color is None:
            product_with_same_tag = await request.app.database.central.find_one(
                {"tag.name": {"$regex": d["name"], "$options": 'i'}}
            )
            if product_with_same_tag is None:
                tag_color = "black"
            else:
                tag_color =  product_with_same_tag['tag'].get('color', 'black')
        product = await request.app.database.central.find_one_and_update(
            {'_id': product_id},
            {"$set": {"tag": {"name": tag_name, "color": tag_color}}},
            return_document=ReturnDocument.AFTER,
        )
        updated_list.append(product)
    if updated_list:
        return {"status": "success", "products": updated_list, "pages": str(0)}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Not found",
        )


@product_router.delete('/tag')
async def delete_card_tag(
    request: Request,
    card_ids: Annotated[CommaSeparatedList[PyObjectId], FastAPIQuery()],
    user_id: str = Depends(require_user),
):
    result = await request.app.database.central.update_many(
        filter={'_id': {'$in': card_ids}},
        update={'$unset': {'tag': ''}},
    )
    return {"status": "success", "updated_count": result.modified_count}


@product_router.put('/update')
async def product_stock_update(request: Request, payload: ProductsUpdateRequest, user_id: str = Depends(require_user)):
    query_items = payload.model_dump()['groups']
    updatelist = list()
    for i in query_items:
        setitems = {}
        try:
            object_id = ObjectId(i['product']['product_id'])
            query_filter = {"_id": object_id}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="{i['product']['product_id']} is InvalidId")
        try:
            setitems["stock.ya.count"] = i['stock']['ya']['count']
        except TypeError:
            pass
        try:
            setitems["stock.ozon.count"] = i['stock']['ozon']['count']
        except TypeError:
            pass
        try:
            setitems["stock.sber.count"] = i['stock']['sber']['count']
        except TypeError:
            pass
        try:
            setitems["stock.wb.count"] = i['stock']['wb']['count']
        except TypeError:
            pass
        try:
            setitems["stock.ali.count"] = i['stock']['ali']['count']
        except TypeError:
            pass
        result = await request.app.database.central.find_one_and_update(query_filter, {"$set": setitems}, {"_id": 0},
                                                                            return_document=ReturnDocument.AFTER)

        result = ProductBaseUpdateSchema.model_validate(result)
        update_ya_pipeline = {"skus": []}
        update_ozon_pipeline = {"stocks": []}
        update_wb_pipeline = {"stocks": []}
        update_sber_pipeline = {"stocks": []}
        update_ali_pipeline = {"products": []}
        multiprocessing.set_start_method("spawn", force=True)
        processes = []
        try:
            if result.stock.ya.state == True and result.stock.ya is not None:
                update_ya_pipeline["api_key"] =  result.options.ya.api_key
                update_ya_pipeline["keys_id"] = result.keys_id
                update_ya_pipeline["campaignid"] = result.options.ya.campaign_id
                update_ya_pipeline["skus"].append({'items': [{"count": i['stock']['ya']['count']}],
                                                  'sku': str(result.options.ya.offer_id)})
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": result.keys_id})
                update_ya_pipeline["log_id"] = str(log.inserted_id)
                items = {"items": update_ya_pipeline}
                p = multiprocessing.Process(target=utils.stock_update_ya, kwargs=dict(items))
                p.start()
                processes.append(p)
                updatelist.append(str(log.inserted_id))
        except AttributeError:
            pass
        try:
            if result.stock.ozon.state == True and result.stock.ozon is not None:
                update_ozon_pipeline["client_id"] = result.options.ozon.client_id
                update_ozon_pipeline["keys_id"] =  result.keys_id
                update_ozon_pipeline["api_key"] =  result.options.ozon.api_key
                update_ozon_pipeline["warehouse_id"] = result.options.ozon.warehouse_id
                update_ozon_pipeline["stocks"].append({"offer_id": result.options.ozon.offer_id,
                                                      "product_id": result.options.ozon.ozon_id,
                                                      "stock": i['stock']['ozon']['count'],
                                                      "warehouse_id": update_ozon_pipeline["warehouse_id"]})
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": result.keys_id})
                update_ozon_pipeline["log_id"] = str(log.inserted_id)
                items = {"items": update_ozon_pipeline}
                p = multiprocessing.Process(target=utils.stock_update_ozon, kwargs=dict(items))
                p.start()
                processes.append(p)
                updatelist.append(str(log.inserted_id))
        except AttributeError:
            pass
        try:
            if result.stock.wb.state == True and result.stock.wb is not None:
                update_wb_pipeline["Authorization"] = result.options.wb.wb_token
                update_wb_pipeline["keys_id"] = result.keys_id
                update_wb_pipeline["warehouse_id"] = result.options.wb.warehouse_id
                update_wb_pipeline["stocks"].append({"sku": str(result.options.wb.sku),
                                                    "amount": i['stock']['wb']['count']})
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": result.keys_id})
                update_wb_pipeline["log_id"] = str(log.inserted_id)
                items = {"items": update_wb_pipeline}
                p = multiprocessing.Process(target=utils.stock_update_wb, kwargs=dict(items))
                p.start()
                processes.append(p)
                updatelist.append(str(log.inserted_id))
        except AttributeError:
            pass
        try:
            if result.stock.sber.state == True and result.stock.sber is not None:
                update_sber_pipeline["token"] = result.options.sber.token
                update_sber_pipeline["keys_id"] = result.keys_id
                update_sber_pipeline["stocks"].append({"offerId": result.options.sber.offer_id,
                                                    "stocks": i['stock']['sber']['count']})
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": result.keys_id})
                update_sber_pipeline["log_id"] = str(log.inserted_id)
                items = {"items": update_sber_pipeline}
                p = multiprocessing.Process(target=utils.stock_update_sber, kwargs=dict(items))
                p.start()
                processes.append(p)
                updatelist.append(str(log.inserted_id))
        except AttributeError:
            pass
        try:
            if result.stock.ali.state == True and result.stock.ali is not None :
                update_ali_pipeline["api_key"] = result.options.ali.api_key
                update_ali_pipeline["keys_id"] = result.keys_id
                update_ali_pipeline["products"].append({"product_id": str(result.options.ali.ali_id),
                                                    "skus": [{"sku_code": str(result.options.ali.offer_id),
                                                              "inventory": str(i['stock']['ali']['count'])}]})
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": result.keys_id})
                update_ali_pipeline["log_id"] = str(log.inserted_id)
                items = {"items": update_ali_pipeline}
                p = multiprocessing.Process(target=utils.stock_update_ali, kwargs=dict(items))
                p.start()
                processes.append(p)
                updatelist.append(str(log.inserted_id))
        except AttributeError:
            pass
    return {"status": "success", "ids": updatelist}


@product_router.put('/update/checkfile', response_model=ResponseCheckFile)
async def product_stock_update_from_filelist(request: Request, file: UploadFile, keys_id: str, plus: bool | None = False,
                                             minus: bool | None = False,
                                             user_id: str = Depends(require_user)):
    if plus and minus:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Can not use Plus and Minus at same time")
    if not plus and not minus:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Please choose plus or minus")
    if plus:
        operation = "plus"
    if minus:
        operation = "minus"
    readfile = await file.read()
    df = pd.read_excel(bytes(readfile))
    if len(df.columns.tolist()) != 7:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table size it's not 7 columns")
    if 'артикул продавца' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column aртикул продавца not in table")
    if 'количество' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column количество not in table")
    if 'ozon' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column ozon not in table")
    if 'ali' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column ali not in table")
    if 'sber' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column sber not in table")
    if 'wb' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column wb not in table")
    if 'yandex' not in df.columns:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table column yandex not in table")
    if df.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Table is empty")
    df['артикул продавца'] = df['артикул продавца'].apply(str)
    df["проверка артикул продавца"] = [True if type(x) == str else False for x in df["артикул продавца"]]
    df["проверка количество"] = [True if type(x) == int else False for x in df["количество"]]
    df["проверка yandex"] = [True if type(x) == bool else False for x in df["yandex"]]
    df["проверка ali"] = [True if type(x) == bool else False for x in df["ali"]]
    df["проверка sber"] = [True if type(x) == bool else False for x in df["sber"]]
    df["проверка wb"] = [True if type(x) == bool else False for x in df["wb"]]
    df["проверка ozon"] = [True if type(x) == bool else False for x in df["ozon"]]
    checkoffer_id = df["проверка артикул продавца"].eq(True).all()
    checkcount = df["проверка количество"].eq(True).all()
    checkyandex = df["проверка yandex"].eq(True).all()
    checkali = df["проверка ali"].eq(True).all()
    checksber = df["проверка sber"].eq(True).all()
    checkwb = df["проверка wb"].eq(True).all()
    checkozon = df["проверка ozon"].eq(True).all()
    df = df.rename(columns={'артикул продавца': 'offer_id', 'количество': 'count_add',
                            'проверка артикул продавца': 'check_offer_id',
                            'проверка количество': 'check_count', 'проверка yandex': 'check_yandex',
                            'проверка ali': 'check_ali', 'проверка sber': 'check_sber',
                            'проверка wb': 'check_wb', 'проверка ozon': 'check_ozon'})
    if (not checkoffer_id or not checkcount or not checkwb or not checksber or not checkyandex or not checkali
            or not checkozon):
        df.insert(loc=1, column='image', value=None)
        df.insert(loc=1, column='name', value=None)
        df.insert(loc=1, column='user_id', value=user_id)
        df.insert(loc=2, column='count_yandex_now', value=None)
        df.insert(loc=2, column='count_yandex_was', value=None)
        df.insert(loc=2, column='count_ozon_now', value=None)
        df.insert(loc=2, column='count_ozon_was', value=None)
        df.insert(loc=2, column='count_ali_now', value=None)
        df.insert(loc=2, column='count_ali_was', value=None)
        df.insert(loc=2, column='count_sber_now', value=None)
        df.insert(loc=2, column='count_sber_was', value=None)
        df.insert(loc=2, column='count_wb_now', value=None)
        df.insert(loc=2, column='count_wb_was', value=None)
        df['check_negative_count'] = None
        data = df.to_dict('records')
        try:
            offer_sum = (df['check_offer_id']).value_counts()[False]
            offer_sum = offer_sum.item()
        except KeyError:
            offer_sum = 0
        try:
            count_sum = (df['check_count']).value_counts()[False]
            print(count_sum)
            count_sum = count_sum.item()
        except KeyError:
            count_sum = 0
        try:
            yandex_sum = (df['check_yandex']).value_counts()[False]
            yandex_sum = yandex_sum.item()
        except KeyError:
            yandex_sum = 0
        try:
            ali_sum = (df['check_ali']).value_counts()[False]
            ali_sum = ali_sum.item()
        except KeyError:
            ali_sum = 0
        try:
            sber_sum = (df['check_sber']).value_counts()[False]
            sber_sum = sber_sum.item()
        except KeyError:
            sber_sum = 0
        try:
            wb_sum = (df['check_wb']).value_counts()[False]
            wb_sum = wb_sum.item()
        except KeyError:
            wb_sum = 0
        try:
            ozon_sum = (df['check_ozon']).value_counts()[False]
            ozon_sum = ozon_sum.item()
        except KeyError:
            ozon_sum = 0
        count = offer_sum + count_sum + yandex_sum + ali_sum + sber_sum + wb_sum + ozon_sum
        update_history = {"status": "error", "data": data, "error_count": count, "user_id": user_id,
                          "keys_id": keys_id,
                          "operation": operation,"date": utils.datetime_now_str()}
        history = await request.app.database.history.insert_one(update_history)
        update_history['history_id'] = str(history.inserted_id)
        return update_history
    dfdicts = df.to_dict('records')
    p_id_ya = []
    p_id_ozon = []
    p_id_sber = []
    p_id_wb = []
    p_id_ali = []
    for d in dfdicts:
        try:
            query_filter = {'offer_id': d['offer_id'], "keys_id": keys_id}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Invalid keys_id:{keys_id}")
        dfind = await request.app.database.central.find(query_filter).to_list(None)
        d['check_negative_count_yandex'] = True
        d['check_negative_count_ozon'] = True
        d['check_negative_count_ali'] = True
        d['check_negative_count_sber'] = True
        d['check_negative_count_wb'] = True
        if dfind:
            d['check_offer_id_miss'] = True
            for p in dfind:
                product = ProductUpBaseSchema.model_validate(p)
                d['name'] = product.name
                if product.stock.ya != None and product.merge.ya == True and d['yandex'] != False and str(product.id) not in p_id_ya:
                    # max 2000
                    if plus:
                        k = d['count_add'] + product.stock.ya.count
                    if minus:
                        k = product.stock.ya.count - d['count_add']
                        if k < 0:
                            d['check_negative_count_yandex'] = False
                    d['count_yandex_was'] = product.stock.ya.count
                    d['count_yandex_now'] = k
                    d['image'] = product.image.ya
                    p_id_ya.append(str(product.id))
                if product.stock.ozon != None and product.merge.ozon == True and d['ozon'] != False and str(product.id) not in p_id_ozon:
                    # max 100
                    if plus:
                        k = d['count_add'] + product.stock.ozon.count
                    if minus:
                        k = product.stock.ozon.count - d['count_add']
                        if k < 0:
                            d['check_negative_count_ozon'] = False
                    d['count_ozon_was'] = product.stock.ozon.count
                    d['count_ozon_now'] = k
                    d['image'] = product.image.ozon
                    p_id_ozon.append(str(product.id))
                if product.stock.ali != None and product.merge.ali == True and d['ali'] != False and str(product.id) not in p_id_ali:
                    # 1000
                    if plus:
                        k = d['count_add'] + product.stock.ali.count
                    if minus:
                        k = product.stock.ali.count - d['count_add']
                        if k < 0:
                            d['check_negative_count_ali'] = False
                    d['count_ali_was'] = product.stock.ali.count
                    d['count_ali_now'] = k
                    d['image'] = product.image.ali
                    p_id_ali.append(str(product.id))
                if product.stock.sber != None and product.merge.sber == True and d['sber'] != False and str(product.id) not in p_id_sber:
                    # 300
                    if plus:
                        k = d['count_add'] + product.stock.sber.count
                    if minus:
                        k = product.stock.sber.count - d['count_add']
                        if k < 0:
                            d['check_negative_count_sber'] = False
                    d['count_sber_was'] = product.stock.sber.count
                    d['count_sber_now'] = k
                    d['image'] = product.image.sber
                    p_id_sber.append(str(product.id))
                if product.stock.wb != None and product.merge.wb == True and d['wb'] != False and str(product.id) not in p_id_wb:
                    # 1000
                    if plus:
                        k = d['count_add'] + product.stock.wb.count
                    if minus:
                        k = product.stock.wb.count - d['count_add']
                        if k < 0:
                            d['check_negative_count_wb'] = False
                    d['count_wb_was'] = product.stock.wb.count
                    d['count_wb_now'] = k
                    d['image'] = product.image.wb
                    p_id_wb.append(str(product.id))
        else:
            d['check_offer_id_miss'] = False
    df = pd.DataFrame.from_records(dfdicts)
    df.to_csv('check.csv', sep='|')
    try:
        check_offer_id_miss = (df['check_offer_id_miss']).value_counts()[False]
        check_offer_id_miss = check_offer_id_miss.item()
    except KeyError:
        check_offer_id_miss = 0
    try:
        check_negative_count_wb = (df['check_negative_count_wb']).value_counts()[False]
        check_negative_count_wb = check_negative_count_wb.item()
    except KeyError:
        check_negative_count_wb = 0
    try:
        check_negative_count_sber = (df['check_negative_count_sber']).value_counts()[False]
        check_negative_count_sber = check_negative_count_sber.item()
    except KeyError:
        check_negative_count_sber = 0
    try:
        check_negative_count_ozon = (df['check_negative_count_ozon']).value_counts()[False]
        check_negative_count_ozon = check_negative_count_ozon.item()
    except KeyError:
        check_negative_count_ozon = 0
    try:
        check_negative_count_ali = (df['check_negative_count_ali']).value_counts()[False]
        check_negative_count_ali = check_negative_count_ali.item()
    except KeyError:
        check_negative_count_ali = 0
    try:
        check_negative_count_yandex = (df['check_negative_count_yandex']).value_counts()[False]
        check_negative_count_yandex = check_negative_count_yandex.item()
    except KeyError:
        check_negative_count_yandex = 0
    count = (check_negative_count_wb + check_negative_count_sber + check_negative_count_ozon + check_offer_id_miss +
             check_negative_count_ali + check_negative_count_yandex)
    if count != 0:
        update_history = {"status": "error", "data": dfdicts, "error_count": count, "user_id": user_id,
                          "keys_id": keys_id,
                          "operation": operation, "date": utils.datetime_now_str()}
        history = await request.app.database.history.insert_one(update_history)
        update_history['history_id'] = str(history.inserted_id)
        return update_history
    update_history = {"status": "success", "data": dfdicts, "error_count": 0, "user_id": user_id,
                      "keys_id": keys_id,
                      "operation": operation, "date": utils.datetime_now_str()}
    history = await request.app.database.history.insert_one(update_history)
    update_history['history_id'] = str(history.inserted_id)
    return update_history


@product_router.get('/update/history', response_model=ListResponseCheckFile)
async def get_update_history(request: Request, users_id: str | None = None, history_id: str | None = None,
                                             user_id: str = Depends(require_user)):
    if not users_id and not history_id:
        search = {}
    if users_id and history_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Choose only user_id or history_id")
    if users_id:
        search = {'user_id': users_id}
    if history_id:
        try:
            search = {'_id': ObjectId(history_id)}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Bad history_id")
    history = await (request.app.database.history.find(search)).to_list(None)
    if history:
        return {"status": "success", "check_history": history}
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Empty list")

@product_router.delete('/update/history', response_model=DefStatus)
async def delete_update_history(request: Request, users_id: str | None = None, history_id: str | None = None,
                                             user_id: str = Depends(require_user)):
    if not users_id and not history_id:
        delete = {}
    if users_id and history_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Choose only user_id or history_id")
    if users_id:
        delete = {'user_id': users_id}
    if history_id:
        try:
            delete = {'_id': ObjectId(history_id)}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Bad history_id")
    await request.app.database.history.delete_many(delete)
    return {"status": "success"}


@product_router.get('/update/history/download')
async def get_history_file(request: Request, history_id: str | None = None, user_id: str = Depends(require_user)):
    try:
        getfile = {'_id': ObjectId(history_id)}
    except InvalidId:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Invalid history_id")
    history = await (request.app.database.history.find(getfile)).to_list(None)
    if history:
        df = pd.DataFrame.from_records(history[0]['data'])
        sortcolumns = ["check_offer_id",
        "check_count",
        "check_yandex",
        "check_ali",
        "check_sber",
        "check_wb",
        "check_ozon",
        "check_negative_count_yandex",
        "check_negative_count_ali",
        "check_negative_count_sber",
        "check_negative_count_wb",
        "check_negative_count_ozon",
        "check_offer_id_miss"]
        df.sort_values(sortcolumns, ascending=True, inplace=True)
        df.style.apply(utils.coloredrows, axis=1)
        df.style.to_excel(f'history_{history_id}.xlsx', engine="openpyxl")
        cwd = os.getcwd()
        return FileResponse(f'{cwd}/history_{history_id}.xlsx', filename=f'history_{history_id}.xlsx')
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No such file")


@product_router.put('/update/payload_with_errors', response_model=ResponseCheckFile)
async def product_stock_payload_with_errors(request: Request, payload: UploadPayloadError,
                                             user_id: str = Depends(require_user)):
    readfile = payload.model_dump()
    data = readfile['data']
    errordata = []
    successdata = []
    for d in data:
        if (d['check_offer_id'] != True or d['check_count'] != True or d['check_yandex'] != True
                or d['check_ali'] != True or d['check_sber'] != True or d['check_wb'] != True
                or d['check_ozon'] != True or d['check_negative_count_yandex'] != True
                or d['check_negative_count_ali'] != True
                or d['check_negative_count_sber'] != True or d['check_negative_count_wb'] != True
                or d['check_negative_count_ozon'] != True or d['check_offer_id_miss'] != True):
            errordata.append(d)
        else:
            successdata.append(d)

    update_history_error = {"status": "error", "data": errordata, "error_count": readfile['error_count'],
                            "user_id": user_id, "keys_id": readfile['keys_id'], "split": True,
                            "splited_history_id": readfile['history_id'],
                            "operation": readfile['operation'], "date": utils.datetime_now_str()}
    history_error = await request.app.database.history.insert_one(update_history_error)
    update_history_success = {"status": "success", "data": successdata, "error_count": 0,
                            "user_id": user_id, "keys_id": readfile['keys_id'], "split": True,
                            "splited_history_id": readfile['history_id'],
                            "operation": readfile['operation'], "date": utils.datetime_now_str()}
    history = await request.app.database.history.insert_one(update_history_success)
    update_history_success['history_id'] = str(history.inserted_id)
    update_history_success['error_history_id'] = str(history_error.inserted_id)
    return update_history_success




@product_router.put('/update/payload')
async def product_stock_update_from_payload(request: Request, payload: ListUpdatePayload, keys_id: str,
                                            history_id: str | None = None, rollback: bool | None = None,
                                             user_id: str = Depends(require_user)):
    if rollback and not history_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"You must send history_id at same time")
    readfile = payload.model_dump()
    successupload = []
    update_ya_pipeline = {"skus": []}
    update_ozon_pipeline = {"stocks": []}
    update_wb_pipeline = {"stocks": []}
    update_sber_pipeline = {"stocks": []}
    update_ali_pipeline = {"products": []}
    processes = []
    p_id_ya = []
    p_id_ozon = []
    p_id_sber = []
    p_id_wb = []
    p_id_ali = []
    for d in readfile['data']:
        try:
            query_filter = {'offer_id': d['offer_id'], "keys_id": keys_id}
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Invalid keys_id:{keys_id}")
        itemstate = {"$set": {"stock.ya.state": d["yandex"],
                             "stock.ozon.state": d["ozon"],
                             "stock.ali.state": d["ali"],
                             "stock.sber.state": d["sber"],
                             "stock.wb.state": d["wb"]}}
        await request.app.database.central.update_many(query_filter, itemstate)
        dfind = await request.app.database.central.find(query_filter).to_list(None)
        if dfind:
            successupload.append({"offer_id": d['offer_id'], "количество": d['count'],
                                  "статус": "артикул обновлен"})
            setitems = {}
            for p in dfind:
                product = ProductUpBaseSchema.model_validate(p)
                if product.stock.ya != None and product.merge.ya == True and product.stock.ya.state != False and str(product.id) not in p_id_ya:
                    # max 2000
                    k = d['count']
                    setitems["stock.ya.count"] = k
                    update_ya_pipeline["api_key"] = product.options.ya.api_key
                    update_ya_pipeline["keys_id"] = product.keys_id
                    update_ya_pipeline["campaignid"] = product.options.ya.campaign_id
                    update_ya_pipeline["skus"].append({'items': [{"count": k}],
                                                       'sku': str(product.options.ya.offer_id)})
                    p_id_ya.append(str(product.id))
                    await request.app.database.central.find_one_and_update({"_id": product.id}, {"$set": setitems})
                if product.stock.ozon != None and product.merge.ozon == True and product.stock.ozon.state != False and str(product.id) not in p_id_ozon:
                    # max 100
                    k = d['count']
                    setitems["stock.ozon.count"] = k
                    update_ozon_pipeline["client_id"] = product.options.ozon.client_id
                    update_ozon_pipeline["keys_id"] =  product.keys_id
                    update_ozon_pipeline["api_key"] =  product.options.ozon.api_key
                    update_ozon_pipeline["warehouse_id"] = product.options.ozon.warehouse_id
                    update_ozon_pipeline["stocks"].append({"offer_id": product.options.ozon.offer_id,
                                                          "product_id": product.options.ozon.ozon_id,
                                                          "stock": k,
                                                          "warehouse_id": update_ozon_pipeline["warehouse_id"]})
                    p_id_ozon.append(str(product.id))
                    await request.app.database.central.find_one_and_update({"_id": product.id}, {"$set": setitems})
                if product.stock.ali != None and product.merge.ali == True and product.stock.ali.state != False and str(product.id) not in p_id_ali:
                    # 1000
                    k = d['count']
                    setitems["stock.ali.count"] = k
                    update_ali_pipeline["api_key"] = product.options.ali.api_key
                    update_ali_pipeline["keys_id"] = product.keys_id
                    update_ali_pipeline["products"].append({"product_id": str(product.options.ali.ali_id),
                                                        "skus": [{"sku_code": str(product.options.ali.offer_id),
                                                                  "inventory": str(k)}]})
                    p_id_ali.append(str(product.id))
                    await request.app.database.central.find_one_and_update({"_id": product.id}, {"$set": setitems})
                if product.stock.sber != None and product.merge.sber == True and product.stock.sber.state != False and str(product.id) not in p_id_sber:
                    # 300
                    k = d['count']
                    setitems["stock.sber.count"] = k
                    update_sber_pipeline["token"] = product.options.sber.token
                    update_sber_pipeline["keys_id"] = product.keys_id
                    update_sber_pipeline["stocks"].append({"offerId": product.options.sber.offer_id,
                                                           "stocks": k})
                    p_id_sber.append(str(product.id))
                    await request.app.database.central.find_one_and_update({"_id": product.id}, {"$set": setitems})
                if product.stock.wb != None and product.merge.wb == True and product.stock.wb.state != False and str(product.id) not in p_id_wb:
                    # 1000
                    k = d['count']
                    setitems["stock.wb.count"] = k
                    update_wb_pipeline["Authorization"] = product.options.wb.wb_token
                    update_wb_pipeline["keys_id"] = product.keys_id
                    update_wb_pipeline["warehouse_id"] = product.options.wb.warehouse_id
                    update_wb_pipeline["stocks"].append({"sku": str(product.options.wb.sku),
                                                        "amount": k})
                    p_id_wb.append(str(product.id))
                    await request.app.database.central.find_one_and_update({"_id": product.id}, {"$set": setitems})
        else:
            successupload.append({"артикул продавца": d['offer_id'], "количество": d['count'],
                                  "статус": "артикул не найден"})
    # update
    multiprocessing.set_start_method("spawn", force=True)
    # max 2000
    if update_ya_pipeline["skus"]:
        chunks = [update_ya_pipeline["skus"][x:x + 2000] for x in range(0, len(update_ya_pipeline["skus"]), 2000)]
        for chunk in chunks:
            update_ya_pipeline["skus"] = chunk
            log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                        "keys_id": update_ya_pipeline["keys_id"]})
            update_ya_pipeline["log_id"] = str(log.inserted_id)
            items = {"items": update_ya_pipeline}
            p = multiprocessing.Process(target=utils.stock_update_ya, kwargs=dict(items))
            p.start()
            processes.append(p)
    # max 100
    if update_ozon_pipeline["stocks"]:
        chunks = [update_ozon_pipeline["stocks"][x:x + 100] for x in range(0, len(update_ozon_pipeline["stocks"]), 100)]
        for chunk in chunks:
            update_ozon_pipeline["stocks"] = chunk
            log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                        "keys_id": update_ozon_pipeline["keys_id"]})
            update_ozon_pipeline["log_id"] = str(log.inserted_id)
            items = {"items": update_ozon_pipeline}
            p = multiprocessing.Process(target=utils.stock_update_ozon, kwargs=dict(items))
            p.start()
            processes.append(p)
    # max 1000
    if update_wb_pipeline["stocks"]:
        chunks = [update_wb_pipeline["stocks"][x:x + 1000] for x in range(0, len(update_wb_pipeline["stocks"]), 1000)]
        for chunk in chunks:
            update_wb_pipeline["stocks"] = chunk
            log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                        "keys_id": update_wb_pipeline["keys_id"]})
            update_wb_pipeline["log_id"] = str(log.inserted_id)
            items = {"items": update_wb_pipeline}
            p = multiprocessing.Process(target=utils.stock_update_wb, kwargs=dict(items))
            p.start()
            processes.append(p)
    # max 300
    if update_sber_pipeline["stocks"]:
        chunks = [update_sber_pipeline["stocks"][x:x + 300] for x in range(0, len(update_sber_pipeline["stocks"]), 300)]
        for chunk in chunks:
            update_sber_pipeline["stocks"] = chunk
            log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                        "keys_id": update_sber_pipeline["keys_id"]})
            update_sber_pipeline["log_id"] = str(log.inserted_id)
            items = {"items": update_sber_pipeline}
            p = multiprocessing.Process(target=utils.stock_update_sber, kwargs=dict(items))
            p.start()
            processes.append(p)
    # max 1000
    if update_ali_pipeline["products"]:
        chunks = [update_ali_pipeline["products"][x:x + 1000] for x in range(0, len(update_ali_pipeline["products"]),
                                                                             1000)]
        for chunk in chunks:
            update_ali_pipeline["products"] = chunk
            log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                        "keys_id": update_ali_pipeline["keys_id"]})
            update_ali_pipeline["log_id"] = str(log.inserted_id)
            items = {"items": update_ali_pipeline}
            p = multiprocessing.Process(target=utils.stock_update_ali, kwargs=dict(items))
            p.start()
            processes.append(p)
    if history_id:
        await request.app.database.history.update_one({"_id": ObjectId(history_id)}, {"$set": {"send": True,
                                                                                     "send_date": utils.datetime_now_str()}},
                                         upsert=True)
    if rollback:
        await request.app.database.history.update_one({"_id": ObjectId(history_id)}, {"$set": {"rollback": True}},
                                                      upsert=True)
    return {"status": "success", "ids": successupload}


# warehouse routes

@warehouse_router.get('/wb', response_model=WbListWarehouseResponseSchema)
async def get_wb_stock(request: Request, keys_id: str, limit: int = 40, page: int = 1, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_wb"]
    try:
        warehouse = wbListResponseEntity(await (collection.find().skip(limit * (page - 1)).
                                     limit(limit)).to_list(None))
        count = wbListResponseEntity(await(collection.find()).to_list(None))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "warehouse": warehouse, "pages": str(pages)}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@warehouse_router.delete('/wb', response_model=DefStatus)
async def delete_wb_stock(request: Request, keys_id: str, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_wb"]
    collection.drop()
    return {"status": "success"}


@warehouse_router.get('/ozon', response_model=OzonListWarehouseResponseSchema)
async def get_ozon_stock(request: Request, keys_id: str, limit: int = 40,
                         page: int = 1, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_ozon"]
    try:
        warehouse = ozonListResponseEntity(await(collection.find().skip(limit * (page - 1)).
                                     limit(limit)).to_list(None))
        df = pd.json_normalize(warehouse)
        df.to_csv('ozon.csv', sep='|')
        count = ozonListResponseEntity(await(collection.find()).to_list(None))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "warehouse": warehouse, "pages": str(pages)}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@warehouse_router.delete('/ozon', response_model=DefStatus)
async def delete_ozon_stock(request: Request, keys_id: str, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_ozon"]
    collection.drop()
    return {"status": "success"}


@warehouse_router.get('/ya', response_model=YaListWarehouseResponseSchema)
async def get_ya_stock(request: Request, keys_id: str, limit: int = 40, page: int = 1, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_yandex"]
    try:
        warehouse = yaListResponseEntity(await (collection.find().skip(limit * (page - 1)).
                                     limit(limit)).to_list(None))
        count = yaListResponseEntity(await(collection.find()).to_list(None))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "warehouse": warehouse, "pages": str(pages)}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@warehouse_router.delete('/ya', response_model=DefStatus)
async def delete_ya_stock(request: Request, keys_id: str, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_yandex"]
    collection.drop()
    return {"status": "success"}


@warehouse_router.get('/ali', response_model=AliListWarehouseResponseSchema)
async def get_ali_stock(request: Request, keys_id: str, limit: int = 40, page: int = 1, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_ali"]
    try:
        warehouse = aliListResponseEntity(await (collection.find().skip(limit * (page - 1)).
                                     limit(limit)).to_list(None))
        count = aliListResponseEntity(await(collection.find()).to_list(None))
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "warehouse": warehouse, "pages": str(pages)}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@warehouse_router.delete('/ali', response_model=DefStatus)
async def delete_ali_stock(request: Request, keys_id: str, user_id: str = Depends(require_user)):
    collection = request.app.database[f"{keys_id}_ali"]
    collection.drop()
    return {"status": "success"}


@warehouse_router.get('/central', response_model=CentralListWarehouseResponseSchema)
async def get_central_stock_by_keys_id(request: Request, keys_id: str, limit: int = 40, page: int = 1,
                                       user_id: str = Depends(require_user)):
    try:
        warehouse = centralListResponseEntity(await (request.app.database.central.find({"keys_id": keys_id}).
                                              skip(limit * (page - 1)).
                                     limit(limit)).to_list(None))
        count = await(request.app.database.central.find({"keys_id": keys_id})).to_list(None)
        pages = math.ceil(len(count) / limit)
        return {"status": "success", "warehouse": warehouse, "pages": str(pages)}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@warehouse_router.delete('/central', response_model=DefStatus)
async def delete_central_stock_by_keys_id(request: Request, keys_id: str | None = None, drop_all: bool | None = False,
                                          user_id: str = Depends(require_user)):
    collection = request.app.database.central
    if drop_all:
        await collection.drop()
    else:
        await collection.delete_many({"keys_id": keys_id})
    return {"status": "success"}


# logs
@logs_router.get("", response_model=LogListRResponseSchema)
async def get_logs_by_id(request: Request, keys_id: str | None = None, limit: int = 40,
                                  page: int = 1, user_id: str = Depends(require_user)):
    if not keys_id:
        insert = None
    else:
        insert = {"keys_id": keys_id}
    logs = await (request.app.database.log.find(insert).skip(limit * (page - 1)).
                                     limit(limit)).sort({"updated_at": -1}).to_list(None)
    count = await (request.app.database.log.find(insert)).to_list(None)

    pages = math.ceil(len(count) / limit)
    return {"logs": logs, "pages": str(pages)}

@logs_router.delete("", response_model=LogListRResponseSchema)
async def delete_logs_by_date_before(request: Request, before_date: datetime | None = None,
                                     drop: bool = False, limit: int = 40,
                                  page: int = 1, user_id: str = Depends(require_user)):
    if drop:
        request.app.database.log.drop()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Empty list")
    else:
        request.app.database.log.delete_many({"created_at": {"$lt": before_date}})
        logs = await (request.app.database.log.find().skip(limit * (page - 1)).
                                     limit(limit)).to_list(None)
        count = await (request.app.database.log.find()).to_list(None)
        pages = math.ceil(len(count) / limit)
        return {"logs": logs, "pages": str(pages)}



#settings
@settings_router.put('', response_model=SetKeyResponseSchema)
async def set_keys(request: Request, payload: SetKeySchema, keys_id: str | None = None, user_id: str = Depends(require_user)):
    dicts = payload.model_dump()
    myDict = {key: val for key, val in dicts.items() if val != None}
    myDict["updated_at"] = utils.datetime_now_str()
    created_at = utils.datetime_now_str()
    if not keys_id:
        dicts["created_at"] = created_at
        dicts["updated_at"] = created_at
        result = await request.app.database.settings.insert_one(dicts)
        settings = settingsResponseEntity(await request.app.database.settings.find_one(
            {"_id": ObjectId(result.inserted_id)}
        ))

        return {"status": "success", "settings": settings}

    settings = settingsResponseEntity(await request.app.database.settings.find_one_and_update({"_id": ObjectId(keys_id)},
                                                                                              {"$set": myDict},
                                                                            return_document=ReturnDocument.AFTER))
    return {"status": "success",  "settings": settings}

@settings_router.get('', response_model=SetListKeyResponseSchema)
async def get_settings(request: Request, keys_id: str | None = None, user_id: str = Depends(require_user)):
    if keys_id:
        keys = {'_id': ObjectId(keys_id)}
    else:
        keys = None
    try:
        settings = settingsListResponseEntity(await (request.app.database.settings.find(keys)).to_list(None))
        return {"status": "success", "settings": settings}
    except IndexError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empty list")

@settings_router.delete('')
async def delete_settings(
    request: Request,
    keys_id: str,
    sber: bool | None = False,
    wb: bool | None = False,
    ali: bool | None = False,
    ozon: bool | None = False,
    yandex: bool | None = False,
    alldel: bool | None = False,
    drop: bool | None = False,
    user_id: str = Depends(require_user),
):
    current_datetime = utils.datetime_now_str()
    mydict = {"sber": sber, "wb": wb, "ali": ali, "ozon": ozon, "yandex": yandex}
    keys = {key: val for key, val in mydict.items() if val != False}
    if sber or wb or ali or ozon or yandex:
        for key in keys:
            insert = {"_id": ObjectId(keys_id)}, {"set": {f"{key}": None}}
            await request.app.database.settings.find_one_and_update(insert)
            collection = request.app.database[f"{keys_id}_{key}"]
            collection.drop()
        log = request.app.database2.log.insert_one(
            {
                'status': 0,
                'created_at': utils.datetime_now_str(),
                "keys_id": str(keys_id),
            }
        )
        log_id = str(log.inserted_id)
        request.app.database.central.delete_many({"keys_id": keys_id})
        user = request.app.database2.user.find_one({"_id": ObjectId(user_id)})
        if user['admin']:
            keys_ids = []
            settings = await (request.app.database.log.find()).to_list(None)
            for s in settings:
                keys_ids.append(s['keys_id'])
        else:
            keys_ids = user['keys_ids']
        keys_ids_with_merge_log_ids = {keys_id: log_id for keys_id in keys_ids}
        merge_products_to_card(keys_ids_with_merge_log_ids)
        time.sleep(1)
        group_cards(group_log_id=log_id, seller_ids=keys_ids, card_ids=None)
        return {"status": "deleted, re-merge and re-group in process", "log_id": log_id}
    if alldel:
        await request.app.database.settings.find_one_and_delete({"_id": ObjectId(keys_id)})
    if drop:
        await request.app.client.drop_database(settings.DB_NAME)
        user = {
            'username': settings.ADMIN_LOGIN,
            'password': utils.hash_password(settings.ADMIN_PASSWORD),
            "photo": "/storage/photo/example/0.png",
            'created_at': current_datetime,
            'updated_at': current_datetime,
        }
        await request.app.database.user.insert_one(user)
    return {"status": "success"}


# sync
@sync_router.get("/progress_sse/{log_id}")
async def sync_progress_sse(log_id: str, request: Request, user_id: str = Depends(require_user)):
    """
    SSE endpoint for real-time synchronization progress tracking.
    The client connects to /api/v2/sync/progress_sse/<log_id>,
    and the server checks log.progress every second.
    """
    db = request.app.database2  # Синхронный PyMongo клиент

    async def event_generator():
        while True:
            # Проверяем, не закрыл ли клиент соединение
            if await request.is_disconnected():
                break

            # Синхронный find_one
            log_entry = db.log.find_one({"_id": ObjectId(log_id)})
            if not log_entry:
                break

            progress = log_entry.get("progress", 0)
            yield {"event": "progress", "data": str(progress)}

            # Если дошли до 100% или -1 => завершаем
            if progress >= 100 or progress < 0:
                break

            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())


@sync_router.get('/progress/{log_id}')
async def get_sync_progress(
    request: Request,
    log_id: str,
    detail: bool = False,
    user_id: str = Depends(require_user),
):
    """
    REST endpoint for retrieving the current synchronization progress.
    The frontend will poll this endpoint (e.g. every 1-2 seconds) to update the progress bar.
    Returns a JSON object with the 'progress' field (from 0 to 100).
    """
    if not ObjectId.is_valid(log_id):
        raise HTTPException(status_code=400, detail=f'Invalid log_id {log_id}')

    log_entry = await request.app.database.log.find_one({'_id': ObjectId(log_id)})

    if not log_entry:
        raise HTTPException(status_code=404, detail='Not found')

    if detail:
        return LogProgressDetailResponseSchema.model_validate(log_entry)
    else:
        return {'progress': log_entry.get('progress', 0)}


@sync_router.post("/progress/many", response_model=list[LogProgressResponseSchema])
async def get_multiple_sync_progress(request: Request, payload: LogIDs, user_id: str = Depends(require_user)):
    """
    REST endpoint for retrieving the current synchronization progress for multiple log IDs.
    The frontend can send a POST request with a JSON body containing a list of `log_ids`.
    The endpoint returns an array of objects, each containing:
      - `log_id` (string): The original log identifier
      - `progress` (int): The current progress value from 0 to 100
    Example request body:
        {
          "log_ids": [
            "641e63d9db3a237192b69f3c",
            "641e63d9db3a237192b69f4d"
          ]
        }
        Response:
        [
          {
            "id": "641e63d9db3a237192b69f3c",
            "progress": 100
          },
          {
            "id": "641e63d9db3a237192b69f4d",
            "progress": 0
          }
        ]
    If an invalid `log_id` is provided (wrong format), the endpoint raises a 400 error.
    If no valid documents are found, raises a 404 error.
    """
    obj_ids = []
    for log_id in payload.log_ids:
        try:
            obj_ids.append(ObjectId(log_id))
        except:
            raise HTTPException(status_code=400, detail=f'Invalid log_id: {log_id}')

    log_entries = await (request.app.database.log.find({'_id': {'$in': obj_ids}})).to_list()

    if not log_entries:
        raise HTTPException(status_code=404, detail='Not found')

    return log_entries


@sync_router.post("/updatewarehouses")
def get_update_warehouse(request: Request, payload: UpdateListSchema, user_id: str = Depends(require_user)):
    try:
        updateded_keys = payload.model_dump()
        if not updateded_keys:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"List is empty. Request must contain list of keys_id.")
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Request must contain list of keys_id")
    try:
        listob = list()
        for keys_id in updateded_keys['keys_ids']:
            listob.append(ObjectId(keys_id))
        keysds = settingsListResponseEntity(request.app.database2.settings.find({"_id": {"$in": listob}}))
    except TypeError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"{keys_id} in list not found")
    except InvalidId:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"{keys_id} in list is wrong")
    updatelist = []
    for keysd in keysds:
        multiprocessing.set_start_method("spawn", force=True)
        processes = []
        keys_id = keysd['id']
        company = keysd['company']
        for key in keysd:
            if key == 'ozon' and keysd[key] is not None:
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": keys_id})
                ozon_log = str(log.inserted_id)
                p = multiprocessing.Process(target=utils.update_ozon_warehouse, args=(ozon_log,
                                                                                keys_id, company,
                                                                                keysd[key]))
                p.start()
                processes.append(p)
                updatelist.append(ozon_log)
            if key == 'wb' and keysd[key] is not None:
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": keys_id})
                wb_log = str(log.inserted_id)
                p = multiprocessing.Process(target=utils.update_wb_warehouse, args=(wb_log, keys_id, company, keysd[key]))
                p.start()
                processes.append(p)
                updatelist.append(wb_log)
            if key == 'yandex' and keysd[key] is not None:
                urls = [settings.YA, settings.YA_BIZ]
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": keys_id})
                ya_log = str(log.inserted_id)
                p = multiprocessing.Process(target=utils.update_ya_warehouse, args=(ya_log, keys_id, company,
                                                                              urls, keysd[key]))
                p.start()
                processes.append(p)
                updatelist.append(ya_log)
            if key == 'ali' and keysd.get(key, None) is not None and keysd.get(key, {}).get('token', '') != '':
                urls = [settings.ALI_GOODS]
                log = request.app.database2.log.insert_one({'status': 0, 'created_at': utils.datetime_now_str(),
                                                            "keys_id": keys_id})
                ali_log = str(log.inserted_id)
                p = multiprocessing.Process(target=utils.update_ali_warehouse, args=(ali_log, keys_id, company,
                                                                              urls, keysd[key]))
                p.start()
                processes.append(p)
                updatelist.append(ali_log)
    return {"status": "sync in process", "ids": updatelist}


@sync_router.post("/merge")
def merge_products(request: Request, payload: UpdateListSchema, user_id: str = Depends(require_user)):
    multiprocessing.set_start_method("spawn", force=True)
    sellers_ids_with_merge_logs_ids = {}
    processes = []
    for seller_id in payload.keys_ids:
        try:
            request.app.database2.settings.find_one({'_id': ObjectId(seller_id)})
        except TypeError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{seller_id} in list not found",
            )
        except InvalidId:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{seller_id} in list is wrong",
            )
        log = request.app.database2.log.insert_one(
            {
                'status': 0,
                'created_at': utils.datetime_now_str(),
                "keys_id": str(seller_id),
            }
        )
        sellers_ids_with_merge_logs_ids[seller_id] = str(log.inserted_id)
    p = multiprocessing.Process(target=merge_products_to_card, args=(sellers_ids_with_merge_logs_ids,))
    p.start()
    processes.append(p)
    return {"status": "merge in process", "ids": list(sellers_ids_with_merge_logs_ids.values())}


@sync_router.post('/unmerge')
def move_product(
    request: Request,
    payload: UnmergeSchema,
    user_id: str = Depends(require_user),
):
    multiprocessing.set_start_method('spawn', force=True)
    processes = []
    card_id = payload.card_id
    product_id = payload.product_id
    market = payload.market
    keys_id = payload.keys_id
    try:
        card = request.app.database2.central.find_one({'_id': ObjectId(card_id)})
        converted_market = 'yandex' if market == 'ya' else market
        product_collection = request.app.database2[f'{keys_id}_{converted_market}']
        product = product_collection.find_one({'_id': ObjectId(product_id)})
        if not card:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Card with card_id {card_id} not found',
            )
        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Product with product_id {product_id} and market {market} '
                       f'for seller with keys_id {keys_id} not found',
            )
        if len(card['products']) == 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Card with card_id {card_id} contains only one product. '
                       f'Removing product from card is only available for card with several products.',
            )
        if product_id not in {product['product_id'] for product in card['products']}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Product with product_id {product_id} is not in card with card_id {card_id}.'
            )
    except TypeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Card with card_id {card_id} or product with product_id {product_id} not actually created',
        )
    except InvalidId:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Card id {card_id} or product_id {product_id} is invalid',
        )
    log = request.app.database2.log.insert_one(
        {
            'status': 0,
            'created_at': utils.datetime_now_str(),
            'keys_id': keys_id,
            'group_items': f'{market}-{product_id}',
        }
    )
    unmerge_log_id = str(log.inserted_id)
    p = multiprocessing.Process(
        target=move_product_to_separate_card,
        args=(
            unmerge_log_id,
            card_id,
            keys_id,
            product_id,
            market,
        ),
    )
    p.start()
    processes.append(p)
    return {"status": "Moving product to separate card is in process", "id": unmerge_log_id}



@sync_router.post("/group")
def group_cards_endpoint(request: Request, payload: GroupListSchema, user_id: str = Depends(require_user)):
    multiprocessing.set_start_method("spawn", force=True)
    processes = []
    card_ids = payload.card_ids
    seller_ids = payload.keys_ids
    if card_ids is None and seller_ids is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"You must add keys_ids or card_ids",
        )
    elif card_ids is not None and seller_ids is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"You must add keys_ids or card_ids",
        )
    elif card_ids is None and seller_ids is not None:
        if not seller_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"List is empty. Request must contain list of keys_id.",
            )
        if len(seller_ids) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"It must be at least 1 keys_id for grouping",
            )
        for seller_id in seller_ids:
            try:
                request.app.database2.settings.find_one({'_id': ObjectId(seller_id)})
            except TypeError:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"{seller_id} in list not found",
                )
            except InvalidId:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{seller_id} in list is wrong",
                )
        log = request.app.database2.log.insert_one(
            {
                'status': 0,
                'created_at': utils.datetime_now_str(),
                "group_companies": str(seller_ids),
            }
        )
        group_log_id = str(log.inserted_id)
        p = multiprocessing.Process(target=group_cards, args=(group_log_id, seller_ids, None))
        p.start()
        processes.append(p)
        return {"status": "grouping in process", "id": group_log_id}
    else:
        if not card_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"List is empty. Request must contain list of card_ids to group",
            )
        if len(card_ids) < 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"It must be at least 2 card_id for grouping",
            )
        for card_id in card_ids:
            try:
                card = request.app.database2.central.find_one({"_id": ObjectId(card_id)})
                if not card:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Card with card_id {card_id} not found",
                    )
            except TypeError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Card with card_id {card_id} not actually created",
                )
            except InvalidId:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{card_id} in list is invalid",
                )
        log = request.app.database2.log.insert_one(
            {
                'status': 0,
                'created_at': utils.datetime_now_str(),
                "group_items": str(card_ids),
            }
        )
        group_log_id = str(log.inserted_id)
        p = multiprocessing.Process(target=group_cards, args=(group_log_id, None, card_ids))
        p.start()
        processes.append(p)
        return {"status": "grouping in process", "id": group_log_id}


@sync_router.post('/ungroup')
def move_card(
    request: Request,
    payload: UngroupSchema,
    user_id: str = Depends(require_user),
):
    multiprocessing.set_start_method('spawn', force=True)
    processes = []
    group_id = payload.group_id
    card_id = payload.card_id
    try:
        group = request.app.database2.central_groups.find_one({'_id': ObjectId(group_id)})
        card = request.app.database2.central.find_one({'_id': ObjectId(card_id)})
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Group with group_id {group_id} not found',
            )
        if not card:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Card with card_id {card_id} not found',
            )
        if len(group['card_ids']) == 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Group with group_id {group_id} contains only one card. '
                       f'Removing card from group is only available for groups with several cards.',
            )
        if card_id not in group['card_ids']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Card with card_id {card_id} is not in group with group_id {group_id}.'
            )
    except TypeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Group with group_id {group_id} or card with card_id {card_id} not actually created',
        )
    except InvalidId:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Group id {group_id} or card_id {card_id} is invalid',
        )
    log = request.app.database2.log.insert_one(
        {
            'status': 0,
            'created_at': utils.datetime_now_str(),
            'group_items': group_id,
        }
    )
    ungroup_log_id = str(log.inserted_id)
    p = multiprocessing.Process(
        target=move_card_to_separate_group,
        args=(
            ungroup_log_id,
            group_id,
            card_id,
        ),
    )
    p.start()
    processes.append(p)
    return {"status": "Moving card to separate group is in process", "id": ungroup_log_id}


@sync_router.post("/result", response_model=LogListResponseSchema)
async def get_result_by_id(request: Request, payload: LogListRequestSchema, user_id: str = Depends(require_user)):
    keyslist = payload.model_dump()
    if keyslist:
        rlist = list()
        try:
            for k in keyslist["ids"]:
                rlist.append(ObjectId(k))
        except InvalidId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Id is invalid")
    else:
        rlist = None
    logs = await (request.app.database.log.find({"_id": {"$in": rlist}}).to_list(None))
    return {"logs": logs}


@sync_router.get("/events")
async def stream_events(request: Request, user_id: str = Depends(require_user)):
    return EventSourceResponse(utils.watch_collection(request.app.database), media_type='text/event-stream')



@sber.post("/upload")
async def upload_file(file: UploadFile, user_id: str = Depends(require_user)):
    if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Wrong file type - only xlsx')

    print(file.content_type)
    return {"filename": file.filename}



@sber.post("/order/new", response_model=SberPush)
async def push_order (payload: SberOrderSchema, request: Request,
                      username: Annotated[str, Depends(utils.get_current_username)]):
    data = payload.model_dump()['data']
    merchant_id = data['merchantId']
    keys_id = await request.app.database.keys.find_one({"sber.merchant_id": merchant_id})
    keys_id = str(keys_id['_id'])
    collection = request.app.database[f'{keys_id}_sber']
    shipments = data['shipments']
    updatelist = list()
    for shipment in shipments:
        items = shipment['items']
        shipment_id = shipment['shipmentId']
        for item in items:
            updatelist.append({'offer_id': item['offerId'], 'quantity': item['quantity'], 'shipment_id': shipment_id})
    for update in updatelist:
        await request.app.database.sberorder.find_one_and_update({'merchant_id': merchant_id,
                                                                  'offer_id': update['offer_id'],
                                                                  'shipment_id': update['shipment_id']},
                                                             {"$set": {"stock", update['quantity']}}, True)
        await collection.find_one_and_update({'merchant_id': merchant_id,
                                                             'offer_id_sber': update['offer_id']},
                                                        {"$set": {'$subtract': ["$stock", update['quantity']]}})

        return {"data": {}, "meta": {}, "success": 1}


@sber.post("/order/cancel", response_model=SberPush)
async def push_cancel (request: Request, payload: SberCancelSchema,
                       username: Annotated[str, Depends(utils.get_current_username)]):
    cancelorder = payload.model_dump()
    merchant_id = cancelorder['merchantId']
    keys_id = await request.app.database.keys.find_one({"sber.merchant_id": merchant_id})
    keys_id = str(keys_id['_id'])
    collection = request.app.database[f'{keys_id}_sber']
    shipments = cancelorder['shipments']
    cancellist = list()
    for shipment in shipments:
        items = shipment['items']
        for item in items:
            cancellist.append({'offer_id': item['offerId'], 'shipment_id': shipment['shipmentId']})
    for cancel in cancellist:
        order = await request.app.database.sberorder.find_one_and_delete({'merchant_id': merchant_id,
                                                              'shipment_id': cancel['shipmentId'],
                                                              'offer_id': cancel['offer_id']})
        if order:
            quantity = order['stock']
            await collection.find_one_and_update({'merchant_id': merchant_id, 'offer_id_sber': cancel['offer_id']},
                                                 {"$inc": {"stock": quantity}})
    return {"data": {}, "meta": {}, "success": 1}


