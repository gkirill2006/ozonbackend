import time
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from passlib.context import CryptContext
from user.userSerializers import *
import httpx
from ast import literal_eval
from pymongo.collection import ReturnDocument
from httpx._exceptions import ReadTimeout
import pandas as pd
import math
import pymongo
import logging
import os
from pathlib import Path
import json
import numpy as np
from bson.objectid import ObjectId
import secrets
from typing import Annotated
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings
import re
import pytz
import random

security = HTTPBasic()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

TZ = pytz.timezone(settings.TIMEZONE)

def custom_tz_time(self, secs):
    return datetime.fromtimestamp(secs, TZ).timetuple()


class CustomTZFormatter(logging.Formatter):
    converter = custom_tz_time


def init_logging():
    handler = logging.FileHandler('app.log')
    formatter = CustomTZFormatter(
        fmt='%(levelname)s [%(asctime)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handler.setFormatter(formatter)
    logging.basicConfig(
        level=settings.LOG_MODE,
        handlers=[handler],
    )


def datetime_now_str() -> str:
    return str(datetime.now(TZ).replace(tzinfo=None))

def hash_password(password: str):
    return pwd_context.hash(password)


def verify_password(password: str, hashed_password: str):
    return pwd_context.verify(password, hashed_password)


async def dbcon():
    mongodb_client = AsyncIOMotorClient(settings.DB_URI2)
    database = mongodb_client[settings.DB_NAME]
    return database


def dbconsync():
    client = pymongo.MongoClient(settings.DB_URI2)
    db = client[settings.DB_NAME]
    return db


def update_ozon_warehouse(log_id, keys_id, company, keys):
    cwd = os.getcwd()
    path = cwd + "/logs/sync/"
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.getLogger().setLevel(level=settings.LOG_MODE)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"{path}ozon.log", filemode="w",
    )
    db = dbconsync()
    log_id_obj = ObjectId(log_id)
    collection = db[f"{keys_id}_ozon"]
    client_id = keys["client_id"]
    api_key = keys["api_key"]
    data = {"limit": 500, "filter": { "visibility": "ALL" }}
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key
    }
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 10}})
    try:
        while True:
            try:
                r = httpx.post(settings.OZON_LIST_URL, headers=headers, json=data)
                break
            except httpx.ConnectError:
                time.sleep(1)
    except ReadTimeout:
        db.log.update_one(
            filter={"_id": log_id_obj},
            update={
                "$set": {
                    "status": 408,
                    "updated_at": datetime_now_str(),
                    "event": "update", "keys_id": keys_id,
                    'details': f"Timeout when getting offer_ids list from {settings.OZON_LIST_URL}",
                    "progress": -1,
                },
            },
        )
        return None

    if r.status_code != 200:
        db.log.update_one(
            filter={"_id": log_id_obj},
            update={
                "$set": {
                    "status": r.status_code,
                    "updated_at": datetime_now_str(),
                    "event": "update", "keys_id": keys_id,
                    'details': f"Error when getting offer_ids list from {settings.OZON_LIST_URL}",
                    "progress": -1,
                },
            },
        )
        return None

    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 20}})
    total = math.ceil(r.json()["result"]["total"] / 500)
    current_progress = 20
    progress_per_request = 70 // total  # 70% - from 20% to 90%
    totallist = list()
    while total != 0:
        try:
            while True:
                try:
                    r = httpx.post(settings.OZON_LIST_URL, headers=headers, json=data)
                    break
                except httpx.ConnectError:
                    time.sleep(1)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj}, 
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update", "keys_id": keys_id,
                        'details': f"Timeout when reading offer_ids list from {settings.OZON_LIST_URL}",
                        "progress": -1,
                    },
                },
            )
            return None

        if r.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": r.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Error when getting offer_ids list from {settings.OZON_LIST_URL}",
                        "progress": -1,
                    },
                },
            )
            return None

        productslists = r.json()["result"]["items"]
        productslist = list()
        for prod in productslists:
            if not prod['archived']:
                productslist.append(prod)
        last_id = r.json()["result"]["last_id"]
        offer_ids = list(map(lambda v: v["offer_id"], productslist))
        data = {"offer_id": offer_ids}
        try:
            while True:
                try:
                    rs = httpx.post(settings.OZON_INFO_LIST_URL, headers=headers, json=data)
                    break
                except httpx.ConnectError:
                    time.sleep(1)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Timeout when reading product info list from {settings.OZON_INFO_LIST_URL}",
                        "progress": -1,
                    },
                },
            )
            return None

        if rs.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": rs.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Error when getting product info list from {settings.OZON_INFO_LIST_URL}",
                        "progress": -1,
                    },
                },
            )
            return None

        data = {"limit": 500, "filter": { "visibility": "ALL" }}
        data["last_id"] = last_id
        productslist = rs.json()["items"]
        # add_list = ["id", "name", "offer_id", "barcode", "barcodes", "sources", "stocks", "created_at", "updated_at",
        #             "primary_image"]
        # cleandata = [{key: i[key] for key in i if key in add_list} for i in productslist]
        # skus = [src['sku'] for item in cleandata for src in item['sources'] if src['source'] == 'sds']
        # skus = list(map(lambda v: str(v["sku"]), cleandata))
        add_list = ["id", "name", "offer_id", "barcodes", "stocks", "created_at", "updated_at",
                     "primary_image"]
        cleandata = [{key: i[key] for key in i if key in add_list} for i in productslist]
        
        # Find sku by sds source and bind with offer_id
        offer_ids_to_sds_sku = {
            item["offer_id"]: src["sku"]
            for item in productslist for src in item.get("sources", [])
            if src["source"] == "sds"
        }

        # Open primary image list and take first image and bind with offer id
        offer_ids_to_primary_image = {
            item["offer_id"]: item["primary_image"][0]
            for item in productslist
            if isinstance(item.get("primary_image"), list) and item["primary_image"]
        }

        for item in cleandata:
            item["sku"] = offer_ids_to_sds_sku.get(item["offer_id"], "")
            item["primary_image"] = offer_ids_to_primary_image.get(item["offer_id"], "")
            item["barcode"] = ""

        skus = [str(v["sku"]) for v in cleandata if v["sku"] != ""]
        idata = {"sku": skus}
        try:
            while True:
                try:
                    rrs = httpx.post(settings.OZON_STOCK_FBS_URL, headers=headers, json=idata)
                    break
                except httpx.ConnectError:
                    time.sleep(1)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj}, 
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Timeout when reading FBS from {settings.OZON_STOCK_FBS_URL}",
                        "progress": -1,
                    },
                },
            )
            return None

        if rrs.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj}, 
                update={
                    "$set": {
                        "status": rrs.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Error when getting FBS from {settings.OZON_STOCK_FBS_URL}",
                        "progress": -1,
                    },
                },
            )
            return None
        stocks = rrs.json()["result"]
        df2 = pd.DataFrame(stocks)
        adata = {
            "filter": {
                "sku": skus
            },
            "limit": 500
        }
        try:
            while True:
                try:
                    rra = httpx.post(settings.OZON_ATRIBUTES, headers=headers, json=adata)
                    break
                except httpx.ConnectError:
                    time.sleep(1)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Timeout when reading attributes from {settings.OZON_ATRIBUTES}",
                        "progress": -1,
                    },
                },
            )
            return None

        if rrs.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": rra.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Error when getting attributes from {settings.OZON_ATRIBUTES}",
                        "progress": -1,
                    },
                },
            )
            return None
        attributes = rra.json()["result"]
        try:
            while True:
                try:
                    rrt = httpx.post(settings.OZON_CAT_TREE, headers=headers)
                    break
                except httpx.ConnectError:
                    time.sleep(1)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj}, 
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Timeout when reading catalog tree from {settings.OZON_CAT_TREE}",
                        "progress": -1,
                    },
                },
            )
            return None

        if rrs.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": rrt.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update", 
                        "keys_id": keys_id,
                        'details': f"Error when getting catalog tree from {settings.OZON_CAT_TREE}",
                        "progress": -1,
                    },
                },
            )
            return None
        df5 = pd.json_normalize(rrt.json()['result'])
        listofcats = list()
        childrenlist = df5['children']
        for children in childrenlist:
            for child in children:
                for ch in child['children']:
                    listofcats.append({'description_category_id': child['description_category_id'],
                                       'category_name': child['category_name'], 'type_id': ch['type_id'],
                                       'type_name': ch['type_name']})
        for attribute in attributes:
            for cats in listofcats:
                if attribute['type_id'] == cats['type_id']:
                    attribute['category'] = cats['category_name']
            atts = attribute['attributes']
            for att in atts:
                if att['id'] == 4389:
                    attribute['country'] = att['values'][0]['value']
                if att['id'] == 85:
                    attribute['brend'] = att['values'][0]['value']
                if att['id'] == 4191:
                    attribute['description'] = att['values'][0]['value']
        df1 = pd.json_normalize(cleandata)
        df3 = df1.rename(columns={"id": "product_id"})
        df3['present'] = 0
        df3['height'] = 0
        df3['depth'] = 0
        df3['width'] = 0
        df3['weight'] = 0
        df3['dimension_unit'] = ""
        df3['weight_unit'] = ""
        df3['description'] = ""
        df3['category'] = ""
        df3['brend'] = ""
        df3ldict = df3.to_dict('records')
        df2ldict = df2.to_dict('records')
        warehouse_id = df2ldict[0]['warehouse_id']
        for df3dict in df3ldict:
            for df2dict in df2ldict:
                if df3dict['product_id'] == df2dict['product_id']:
                    df3dict['present'] = df2dict['present']
            for attribute in attributes:
                if df3dict['product_id'] == attribute['id']:
                    try:
                        df3dict['height'] = attribute['height']
                    except KeyError:
                        df3dict['height'] = 0
                    try:
                        df3dict['depth'] = attribute['depth']
                    except KeyError:
                        df3dict['depth'] = 0
                    try:
                        df3dict['width'] = attribute['width']
                    except KeyError:
                        df3dict['width'] = 0
                    try:
                        df3dict['weight'] = attribute['weight']
                    except KeyError:
                        df3dict['weight'] = 0
                    try:
                        df3dict['dimension_unit'] = attribute['dimension_unit']
                    except KeyError:
                        df3dict['dimension_unit'] = ""
                    try:
                        df3dict['weight_unit'] = attribute['weight_unit']
                    except KeyError:
                        df3dict['weight_unit'] = ""
                    try:
                        df3dict['category'] = attribute['category']
                    except KeyError:
                        df3dict['category'] = ""
                    try:
                        df3dict['country'] = attribute['country']
                    except KeyError:
                        df3dict['country'] = ""
                    try:
                        df3dict['brend'] = attribute['brend']
                    except KeyError:
                        df3dict['brend'] = ""
                    try:
                        df3dict['description'] = attribute['description']
                    except KeyError:
                        df3dict['description'] = ""
        df = pd.json_normalize(df3ldict)
        df["keys_id"] = keys_id
        df["company"] = company
        df['warehouse_id'] = warehouse_id
        df["ozon_url"] = "https://ozon.ru/product/" + df["sku"].astype(str)
        # df = df.drop(['stocks.coming', 'stocks.present',
        #               'stocks.reserved'], axis=1)
        df = df.drop(['stocks.has_stock', 'stocks.stocks'], axis=1) 
        df = df.rename(columns={"product_id": "ozon_id",
                                "present": "stock", "primary_image": "ozon_image", "offer_id": "offer_id_ozon"})
        df['barcode'] = df['barcode'].replace(np.nan, "")
        df['country'] = df['country'].replace(np.nan, "")
        df['offer_id'] = df['offer_id_ozon'].str.replace(",", "yyyyyy")
        df['offer_id'] = df['offer_id'].str.split(",")
        pattern = re.compile(r'yyyyyy')
        df['offer_id'] = df['offer_id'].apply(lambda x: [pattern.sub(',', sub) for sub in x])
        df['client_id'] = client_id
        df['api_key'] = api_key
        icleandata = df.to_dict(orient="records")
        totallist.append(icleandata)
        total -= 1
        current_progress += progress_per_request
        db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": current_progress}})
    updatelist = [x for xs in totallist for x in xs]
    for update in updatelist:
        collection.update_one({"offer_id_ozon":  update['offer_id_ozon']},
                              {"$set": {"name": update['name'],
                                        "keys_id": update['keys_id'],
                                        "client_id": update['client_id'],
                                        "api_key": update['api_key'],
                                        "company": update['company'],
                                        "height": update['height'],
                                        "depth": update['depth'],
                                        "width": update['width'],
                                        "dimension_unit": update['dimension_unit'],
                                        "weight": update['weight'],
                                        "weight_unit": update['weight_unit'],
                                        "category": update['category'],
                                        "country": update['country'],
                                        "brend": update['brend'],
                                        "description": update['description'],
                                        "stock": update['stock'],
                                        "ozon_id": update['ozon_id'],
                                        "ozon_url": update['ozon_url'],
                                        "offer_id": update['offer_id'],
                                        "ozon_image": update['ozon_image'],
                                        "sku": update['sku'],
                                        "barcode": update['barcode'],
                                        "barcodes": update['barcodes'],
                                        "warehouse_id": update['warehouse_id'],
                                        "updated_at": update['updated_at'],
                                        "created_at": update['created_at'],
                                        }}, True)
    db.log.update_one(
        filter={"_id": log_id_obj},
        update={
            "$set": {
                "status": 200, 'details': "Ozon warehouse updated",
                "event": "update",
                "keys_id": keys_id,
                "updated_at": datetime_now_str(),
                "progress": 100,
            },
        },
    )



def update_wb_warehouse(log_id, keys_id, company, keys):
    cwd = os.getcwd()
    path = cwd + "/logs/sync/"
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.getLogger().setLevel(level=settings.LOG_MODE)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"{path}wb.log", filemode="w",
    )
    db = dbconsync()
    log_id_obj = ObjectId(log_id)
    collection = db[f"{keys_id}_wb"]
    wb_token = keys["api_key"]
    warehouse_id = keys["warehouse_id"]
    headers = {
        "Authorization": wb_token
    }
    total = 100
    totallist = list()
    i = 0
    # page limit size for wb. If total below 100, we at last page
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 20}})
    while total == 100:
        if i == 0:
            json = {
                "settings": {
                    "cursor": {
                        "limit": 100
                    },
                    "filter": {
                        "withPhoto": 1
                    }
                }
            }
        else:
            json = {
                "settings": {
                    "cursor": {
                        "limit": 100,
                        "updatedAt": updated_at,
                        "nmID": nmid
                    },
                    "filter": {
                        "withPhoto": 1
                    }
                }
            }
        progressive_limit = 0
        while True:
            try:
                rr = httpx.post(settings.WB_GOODS, headers=headers, json=json, timeout=20.0)
            except ReadTimeout:
                db.log.update_one({"_id": log_id_obj},
                                  {"$set": {"status": 408, "updated_at": datetime_now_str(),
                                            "event": "update", "keys_id": keys_id,
                                            "details": f"Timeout when reading ndID list from {settings.WB_GOODS}"}})
                rr = httpx.post(settings.WB_GOODS, headers=headers, json=json, timeout=60.0)
            if rr.status_code == 429:
                progressive_limit += 5
                db.log.update_one({"_id": log_id_obj}, {"$set": {"status": rr.status_code,
                                               "updated_at": datetime_now_str(),
                                               "event": "wb_content_TO_update", "keys_id": keys_id,
                                               "details": f"Error when getting subjects from {settings.WB_GOODS}"}})
                time.sleep(progressive_limit)
            elif rr.status_code == 200:
                break
            else:
                db.log.update_one(
                    filter={"_id": log_id_obj},
                    update={
                        "$set": {
                            "status": rr.status_code,
                            "updated_at": datetime_now_str(),
                            "event": "update",
                            "keys_id": keys_id,
                            "details": f"Error when getting ndID list from {settings.WB_GOODS}",
                            "progress": -1,
                        },
                    },
                )
                return None
        cardslist = rr.json()["cards"]
        updated_at = rr.json()["cursor"]["updatedAt"]
        nmid = rr.json()["cursor"]["nmID"]
        json["settings"]["cursor"]["updatedAt"] = updated_at
        json["settings"]["cursor"]["nmID"] = nmid
        total = rr.json()["cursor"]["total"]
        sku = list(map(lambda v: v["sizes"][0]["skus"][0], cardslist))
        skus = list(map(lambda v: v["sizes"][0]["skus"], cardslist))
        wb_images = list(map(lambda v: v["photos"][0]["big"], cardslist))
        params = {"warehouseId": int(warehouse_id)}
        data = {"skus": sku}
        try:
            r = httpx.post(f"{settings.WB_STOCKS_URL}/{warehouse_id}",
                           params=params, headers=headers, json=data, timeout=10.0)
        except ReadTimeout:
            db.log.update_one({"_id": log_id_obj},
                              {"$set": {"status": 408, "updated_at": datetime_now_str(),
                            "event": "update", "keys_id": keys_id,
                            "details": f"Timeout when reading stock from {settings.WB_STOCKS_URL}/{warehouse_id}"}})
            r = httpx.post(f"{settings.WB_STOCKS_URL}/{warehouse_id}",
                           params=params, headers=headers, json=data, timeout=60.0)

        if r.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": r.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        "details": f"Error when getting stock from {settings.WB_STOCKS_URL}/{warehouse_id}",
                        "progress": -1,
                    },
                },
            )
            return None

        stock = r.json()["stocks"]
        add_list = ["vendorCode", "title", "createdAt", "updatedAt", "nmID", "dimensions",
                    "subjectID", "brand", "description", "characteristics"]
        cleandata = [{key: i[key] for key in i if key in add_list} for i in cardslist]
        df2 = pd.json_normalize(stock)
        df = pd.json_normalize(cleandata)
        df['sku'] = sku
        df['barcodes'] = skus
        df['wb_image'] = wb_images
        df['keys_id'] = keys_id
        df["company"] = company
        df['warehouse_id'] = warehouse_id
        df['dimension_unit'] = 'mm'
        df['length'] = df['dimensions.length'] * 10
        df['width'] = df['dimensions.width'] * 10
        df['height'] = df['dimensions.height'] * 10
        df['weight_unit'] = 'g'
        df['weight'] = 0
        df['country'] = ""
        df['warehouse_id'] = df['warehouse_id'].astype(int)
        df["wb_url"] = "https://wildberries.ru/catalog/" + df["nmID"].astype(str) + "/detail.aspx"
        df['amount'] = 0
        dfldict = df.to_dict('records')
        df2ldict = df2.to_dict('records')
        for dfdict in dfldict:
            for df2dict in df2ldict:
                if dfdict['sku'] == df2dict["sku"]:
                    dfdict['amount'] = df2dict["amount"]
            characteristics = dfdict['characteristics']
            try:
                for characteristic in characteristics:
                    if characteristic['id'] == 14177451:
                        dfdict['country'] = characteristic['value'][0]
                    if characteristic['id'] == 89008:
                        dfdict['weight'] = characteristic['value']
            except TypeError:
                pass
        df3 = pd.json_normalize(dfldict)
        df3 = df3.rename(columns={"title": "name", "vendorCode": "offer_id_wb", "createdAt": "created_at",
                           "updatedAt": "updated_at", "nmID": "wb_id", "amount": "stock"})
        df3['offer_id'] = df3['offer_id_wb'].str.replace(",", "yyyyyy")
        df3['offer_id'] = df3['offer_id'].str.split(",")
        pattern = re.compile(r'yyyyyy')
        df3['offer_id'] = df3['offer_id'].apply(lambda x: [pattern.sub(',', sub) for sub in x])
        df3['wb_token'] = wb_token
        cleandata = df3.to_dict(orient="records")
        totallist.append(cleandata)
        i += 1
    updatelist = [x for xs in totallist for x in xs]
    progressive_limit = 0
    while True:
        try:
            rrc = httpx.get(settings.WB_CATS, headers=headers, timeout=20.0)
        except httpx.TimeoutException:
            rrc = httpx.get(settings.WB_CATS, headers=headers, timeout=60.0)
        if rrc.status_code == 200:
            break
        elif rrc.status_code == 429:
            progressive_limit += 60
            db.log.update_one({"_id": log_id_obj}, {"$set": {"status": rrc.status_code,
                                                                   "updated_at": datetime_now_str(),
                                                                   "event": "wb_content_TO_update", "keys_id": keys_id,
                                                                   "details": f"Error when getting subjects from {settings.WB_CATS} {progressive_limit}"}})
            time.sleep(progressive_limit)
        else:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": rrc.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        "details": f"Error when getting categories from {settings.WB_CATS}",
                        "progress": -1,
                    },
                },
            )
            return None
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 80}})
    parents = rrc.json()['data']
    allsubjectlist = list()
    for parent in parents:
        progressive_limit = 0
        while True:
            params = {"parentID": parent['id'], "limit": 1000}
            try:
                rrsub = httpx.get(settings.WB_SUBJECTS, headers=headers, params=params, timeout=20.0)
            except httpx.TimeoutException:
                rrsub = httpx.get(settings.WB_SUBJECTS, headers=headers, params=params, timeout=60.0)
            if rrsub.status_code == 200:
                break
            elif rrsub.status_code == 429:
                progressive_limit += 1
                db.log.update_one({"_id": log_id_obj}, {"$set": {"status": rrsub.status_code,
                                                                       "updated_at": datetime_now_str(),
                                                                       "event": "wb_content_TO_update", "keys_id": keys_id,
                                                                       "details": f"Error when getting subjects from {settings.WB_SUBJECTS} TO:{progressive_limit}"}})
                time.sleep(progressive_limit)
            else:
                db.log.update_one(
                    filter={"_id": log_id_obj},
                    update={
                        "$set": {
                            "status": rrsub.status_code,
                            "updated_at": datetime_now_str(),
                            "event": "update",
                            "keys_id": keys_id,
                            "details": f"Error when getting subjects from {settings.WB_SUBJECTS}",
                            "progress": -1,
                        },
                    },
                )
                return None
        subjects = rrsub.json()['data']
        allsubjectlist.append(subjects)
    allsubjects = [x for xs in allsubjectlist for x in xs]
    for update in updatelist:
        for asubject in allsubjects:
            if update['subjectID'] == asubject['subjectID']:
                update['category'] = asubject['parentName']
    for update in updatelist:
        collection.update_one({"offer_id_wb": update['offer_id_wb']},
                              {"$set": {"name": update['name'],
                                        "keys_id": update['keys_id'],
                                        "wb_token": update['wb_token'],
                                        "company": update['company'],
                                        "height": update['height'],
                                        "depth": update['length'],
                                        "width": update['width'],
                                        "dimension_unit": update['dimension_unit'],
                                        "weight": update['weight'],
                                        "weight_unit": update['weight_unit'],
                                        "category": update.get('category', ''),
                                        "country": update['country'],
                                        "brend": update['brand'],
                                        "description": update['description'],
                                        "stock": update['stock'],
                                        "offer_id": update['offer_id'],
                                        "wb_id": update['wb_id'],
                                        "wb_url": update['wb_url'],
                                        "wb_image": update['wb_image'],
                                        "sku": update['sku'],
                                        "barcode": update['sku'],
                                        "barcodes": update['barcodes'],
                                        "warehouse_id": update['warehouse_id'],
                                        "updated_at": update['updated_at'],
                                        "created_at": update['created_at'],
                                        }}, True)
    db.log.update_one(
        filter={"_id": log_id_obj},
        update={
            "$set": {
                "status": 200,
                'details': "WB warehouse updated",
                "event": "update",
                "keys_id": keys_id,
                "updated_at": datetime_now_str(),
                'progress': 100,
            },
        },
    )

def update_ya_warehouse(log_id, keys_id, company, urls, keys):
    cwd = os.getcwd()
    path = cwd + "/logs/sync/"
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.getLogger().setLevel(level=settings.LOG_MODE)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"{path}yandex.log", filemode="w",
    )
    db = dbconsync()
    collection = db[f"{keys_id}_yandex"]
    campaign_id = keys["campaign_id"]
    api_key = keys["api_key"]
    warehouse_id = int(keys["warehouse_id"])
    business_id = keys["business_id"]
    headers = {
        "Api-Key": api_key
    }
    json = {
        "cardStatuses": [
        "HAS_CARD_CAN_NOT_UPDATE",
        "HAS_CARD_CAN_UPDATE",
        "HAS_CARD_CAN_UPDATE_PROCESSING",
    ]
    }
    log_id_obj = ObjectId(log_id)
    params = {"limit": 200}
    stock_params = {"limit": 200}
    total = 200
    totallist = list()
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 20}})
    # page limit size for yandex. If total below 200, we at last page
    while total == 200:
        list_url = urls[1] + business_id + "/offer-mappings"
        try:
            r = httpx.post(list_url, headers=headers, json=json, params=params, timeout=20.0)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Timeout when reading offer list from {list_url}",
                        "progress": -1,
                    },
                },
            )
            return None

        if r.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": r.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Error when getting offer list from {list_url}",
                        "progress": -1,
                    },
                },
            )
            return None

        cardslist = r.json()["result"]["offerMappings"]
        for card in cardslist:
            if not card["offer"]["barcodes"]:
                card["offer"]["barcodes"] = ['0000000000000']
        total = len(cardslist)
        try:
            nextPageToken = r.json()["result"]["paging"]["nextPageToken"]
        except KeyError:
            nextPageToken = None
        if nextPageToken:
            params.update({"page_token": nextPageToken})
        df = pd.json_normalize(cardslist)
        offer_ids = df["offer.offerId"].to_list()
        ya_images =  list(map(lambda v: v["offer"]["pictures"][0], cardslist))
        barcodes =  list(map(lambda v: v["offer"]["barcodes"], cardslist))
        barcode = list(map(lambda v: v["offer"]["barcodes"][0], cardslist))
        manufacturer = list(map(lambda v: v["offer"]["manufacturerCountries"][0] if v["offer"]["manufacturerCountries"] else "", cardslist))
        vendor = list(map(lambda v: v["offer"]["vendor"], cardslist))
        description = list(map(lambda v: v["offer"].get("description", ""), cardslist))
        length = list(map(lambda v: int(v["offer"]["weightDimensions"]["length"]), cardslist))
        width = list(map(lambda v: int(v["offer"]["weightDimensions"]["width"]), cardslist))
        height = list(map(lambda v: int(v["offer"]["weightDimensions"]["height"]), cardslist))
        weight = list(map(lambda v: int(v["offer"]["weightDimensions"]["weight"]), cardslist))
        df["barcodes"] = barcodes
        df["barcode"] = barcode
        df["ya_image"] = ya_images
        df["country"] = manufacturer
        df['country'] = df['country'].replace(np.nan, "-")
        df["brend"] = vendor
        df['brend'] = df['brend'].replace(np.nan, "-")
        df['description'] = description
        df['description'] = df['description'].replace(np.nan, "-")
        df["length"] = length
        df["width"] = width
        df['height'] = height
        df['weight'] = weight
        df["length"] = df["length"] * 10
        df["width"] = df["width"] * 10
        df['height'] = df['height'] * 10
        df['weight'] = df['weight'] * 1000
        df["dimension_unit"] = "mm"
        df["weight_unit"] = "g"
        df = df.rename(columns={"offer.offerId": "offer_id", "offer.name": "name", "mapping.marketModelId": "model_id",
                                "mapping.marketCategoryName": "category",
                           "mapping.marketSku": "sku", "offer.basicPrice.updatedAt": "updated_at"})
        df["ya_url"] = "https://market.yandex.ru/product/" + df["model_id"].astype(str) + "?sku=" + df["sku"].astype(str)
        df["campaign_id"] = campaign_id
        df["warehouse_id"] = warehouse_id
        df["business_id"] = business_id
        df["keys_id"] = keys_id
        df["company"] = company
        df["created_at"] = df["updated_at"]
        stock_url = urls[0] + campaign_id + "/offers/stocks"
        stock_json = {"offerIds": offer_ids}
        try:
            rr = httpx.post(stock_url, headers=headers, json=stock_json, params=stock_params, timeout=20.0)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Timeout when reading stock from {stock_url}",
                        "progress": -1,
                    },
                },
            )
            return None

        if rr.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": rr.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Error when getting stock from {stock_url}",
                        "progress": -1,
                    },
                },
            )
            return None

        try:
            nextPageToken_stock = rr.json()["result"]["paging"]["nextPageToken"]
        except KeyError:
            nextPageToken_stock = None
        if nextPageToken_stock:
            params.update({"page_token": nextPageToken_stock})
        stocklist = rr.json()['result']["warehouses"][0]["offers"]
        newstocklist = list()
        for stock in stocklist:
            if stock["stocks"]:
                for i in stock["stocks"]:
                    if i["type"] == 'AVAILABLE':
                        newstocklist.append(stock)
            else:
                newstocklist.append(stock)
        df2 = pd.json_normalize(newstocklist)
        stocklists = list()
        for v in newstocklist:
            if v["stocks"]:
                for i in v["stocks"]:
                    if i["type"] == 'AVAILABLE':
                        stocklists.append(i["count"])
            else:
                stocklists.append(0)
        df2["stock"] = stocklists
        df2 = df2.rename(columns={"offerId": "offer_id"})
        df3 = pd.merge(df, df2, on="offer_id")
        df3 = df3.rename(columns={"offer_id": "offer_id_ya", "model_id": "modelid"})
        df3 = df3[["offer_id_ya", "name", "modelid", "sku", "ya_url", "campaign_id", "business_id", "category",
                "warehouse_id", "keys_id", "company", "created_at", "updated_at", "ya_image","barcodes", "weight_unit",
                "barcode", "stock", "country", "brend", "description", "length", "width", "height", "weight",
                "dimension_unit"]]
        df3['offer_id'] = df3['offer_id_ya'].str.replace(",", "yyyyyy")
        df3['offer_id'] = df3['offer_id'].str.split(",")
        pattern = re.compile(r'yyyyyy')
        df3['offer_id'] = df3['offer_id'].apply(lambda x: [pattern.sub(',', sub) for sub in x])
        df3['api_key'] = api_key
        cleandata = df3.to_dict(orient="records")
        totallist.append(cleandata)
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 80}})
    updatelist = [x for xs in totallist for x in xs]
    for update in updatelist:
        collection.update_one({"offer_id_ya": update['offer_id_ya']},
                              {"$set": {"name": update['name'],
                                        "keys_id": update['keys_id'],
                                        "company": update['company'],
                                        "business_id": update['business_id'],
                                        "campaign_id": update['campaign_id'],
                                        "api_key": update['api_key'],
                                        "height": update['height'],
                                        "depth": update['length'],
                                        "width": update['width'],
                                        "dimension_unit": update['dimension_unit'],
                                        "weight": update['weight'],
                                        "weight_unit": update['weight_unit'],
                                        "category": update['category'],
                                        "country": update['country'],
                                        "brend": update['brend'],
                                        "description": update['description'],
                                        "stock": update['stock'],
                                        "offer_id": update['offer_id'],
                                        "modelid": update['modelid'],
                                        "ya_url": update['ya_url'],
                                        "ya_image": update['ya_image'],
                                        "sku": update['sku'],
                                        "barcode": update['barcode'],
                                        "barcodes": update['barcodes'],
                                        "warehouse_id": update['warehouse_id'],
                                        "updated_at": update['updated_at'],
                                        "created_at": update['created_at'],
                                        }}, True)
    db.log.update_one(
        filter={"_id": log_id_obj},
        update={
            "$set": {
                "status": 200,
                'details': "Yandex warehouse updated",
                "event": "update",
                "keys_id": keys_id,
                "updated_at": datetime_now_str(),
                "progress": 100,
            },
        },
    )


def update_ali_warehouse(log_id, keys_id, company, urls, keys):
    cwd = os.getcwd()
    path = cwd + "/logs/sync/"
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.getLogger().setLevel(level=settings.LOG_MODE)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"{path}ali.log", filemode="w",
    )
    db = dbconsync()
    collection = db[f"{keys_id}_ali"]
    token = keys["token"]
    
    headers = {
        "x-auth-token": token,
        "x-request-locale": "ru_RU"
    }
    last_product_id = None
    total = 50
    totallist = list()
    log_id_obj = ObjectId(log_id)
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 20}})
    while total == 50:
        if last_product_id is None:
            json = {
                "filter": {
                    "status": "ONLINE"
                },
                "limit": 50
            }
        else:
            json = {
                "filter": {
                    "status": "ONLINE"
                },
                "last_product_id": last_product_id,
                "limit": 50
            }
        try:
            r = httpx.post(urls[0], headers=headers, json=json, timeout=20.0)
        except ReadTimeout:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": 408,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Timeout when reading offer list from {urls[0]}",
                        "progress": -1,
                    },
                },
            )
            return None

        if r.status_code != 200:
            db.log.update_one(
                filter={"_id": log_id_obj},
                update={
                    "$set": {
                        "status": r.status_code,
                        "updated_at": datetime_now_str(),
                        "event": "update",
                        "keys_id": keys_id,
                        'details': f"Error when getting offer list from {urls[0]}",
                        "progress": -1,
                    },
                },
            )
            return None
        rs = r.json()["data"]
        last_product_id	 = rs[-1]["id"]
        total = len(rs)
        df2 = pd.json_normalize(rs)
        df2.sku = df2.sku.fillna('{}')
        df2 = df2.explode('sku').reset_index(drop=True)
        df2['api_key'] = token
        df2['keys_id'] = keys_id
        df2 = df2.rename(columns={"id": "ali_id"})
        df = pd.concat([df2, df2['sku'].apply(pd.Series)], axis = 1).drop('sku', axis = 1)
        df = df.rename(columns={"main_image_url":"ali_image", "ali_created_at":"created_at",
                                "ali_updated_at":"updated_at", "sku_id": "sku", "Subject": "name",
                                "code":"offer_id_ali", "ipm_sku_stock":"stock"})
        df['sku'] = df['sku'].astype(str)
        df['barcode'] = df['sku']
        df['barcode'] = df['barcode'].astype(str)
        df['ali_url'] = 'https://aliexpress.ru/item/' + df['ali_id'] + '.html'
        df['offer_id_ali'] = df['offer_id_ali'].astype(str)
        df['offer_id'] = df['offer_id_ali'].str.replace(",", "yyyyyy")
        df['offer_id'] = df['offer_id'].str.split(",")
        pattern = re.compile(r'yyyyyy')
        df['offer_id'] = df['offer_id'].apply(lambda x: [pattern.sub(',', sub) for sub in x])
        df['barcodes'] = df['sku'].str.split(",")
        df['company'] = company
        df = df[["offer_id_ali", "name", "ali_id", "sku", "ali_url", "offer_id", "api_key",
                   "keys_id", "company", "created_at", "updated_at", "ali_image", "barcodes",
                   "barcode", "stock"]]
        df = df.dropna()
        cleandata = df.to_dict(orient="records")
        totallist.append(cleandata)
    db.log.update_one(filter={"_id": log_id_obj}, update={"$set": {"progress": 80}})
    updatelist = [x for xs in totallist for x in xs]
    for update in updatelist:
        timeout = 1
        file = update['ali_image'].rsplit('/', 1)[1]
        file_url = settings.URL_DOMAIN + '/storage/ali/' + file
        path = os.getcwd()
        exsdt = os.getcwd() + '/storage/ali/'
        path = path + '/storage/ali/' + file
        Path(exsdt).mkdir(parents=True, exist_ok=True)
        try:
            Path(path).resolve(strict=True)
        except FileNotFoundError:
            while True:
                response = httpx.get(update['ali_image'], timeout=60)
                with open(path, 'wb') as f:
                    f.write(response.content)
                if response.status_code == 200:
                    break
                else:
                    timeout += 2
                    time.sleep(timeout)
        collection.update_one({"barcode": update['barcode']},
                              {"$set": {"name": update['name'],
                                        "keys_id": update['keys_id'],
                                        "company": update['company'],
                                        "api_key": update['api_key'],
                                        "stock": update['stock'],
                                        "offer_id": update['offer_id'],
                                        "offer_id_ali": update['offer_id_ali'],
                                        "ali_id": update['ali_id'],
                                        "ali_url": update['ali_url'],
                                        "ali_image": file_url,
                                        "sku": update['sku'],
                                        "barcode": update['barcode'],
                                        "barcodes": update['barcodes'],
                                        "updated_at": update['updated_at'],
                                        "created_at": update['created_at'],
                                        }}, True)
    db.log.update_one(
        filter={"_id": log_id_obj},
        update={
            "$set": {
                "status": 200,
                'details': "Ali warehouse updated",
                "event": "update",
                "keys_id": keys_id,
                "updated_at": datetime_now_str(),
                "progress": 100,
            },
        },
    )


def stock_update_ya(items):
    database2 = dbconsync()
    auth = items['api_key']
    headers = {
        "Api-Key": auth
    }
    json = {
        "skus": items['skus'] # items: list [count: int], sku(offer_id): str --> []
    }
    list_url = settings.YA + f"/{items['campaignid']}/offers/stocks"
    r = httpx.put(list_url, headers=headers, json=json, timeout=20.0)
    if r.status_code == 200:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": 200,
                                                                        "details": f"Stock for Ya updated",
                                                                        "event": "stock", "keys_id": items['keys_id'],
                                                                        "updated_at": datetime_now_str()}})
    else:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                  "details": f"Error during updating Ya {list_url}",
                                                                  "event": "stock", "keys_id": items['keys_id'],
                                                                  "updated_at": datetime_now_str()}})

def stock_update_ali(items):
    database2 = dbconsync()
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        "x-auth-token": items['api_key']
    }
    json = {
        "products": items['products'] # product_id: str, skus: list [{sku_code(offer_id): str, inventory: str}] --> []
    }
    list_url = settings.ALI_UPDATE
    r = httpx.post(list_url, headers=headers, json=json, timeout=20.0)
    if r.status_code == 200:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": 200,
                                                                        "details": f"Stock for Ali updated",
                                                                        "event": "stock", "keys_id": items['keys_id'],
                                                                        "updated_at": datetime_now_str()}})
    else:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                  "details": f"Error during updating Ali {list_url}",
                                                                  "event": "stock", "keys_id": items['keys_id'],
                                                                  "updated_at": datetime_now_str()}})


def stock_update_ozon(items):
    database2 = dbconsync()
    headers = {
        "Client-Id": str(items['client_id']),
        "Api-Key": str(items['api_key'])
    }
    json = {
        "stocks": items['stocks'] # offer_id: str, product_id: int, stock: int, warehouse_id: int -- > []
    }
    list_url = settings.OZON_STOCK_UPDATE
    r = httpx.post(list_url, headers=headers, json=json, timeout=20.0)
    if r.status_code == 200:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": 200,
                                                                        'details': f"Ozon stock updated",
                                                                        "event": "stock", "keys_id": items['keys_id'],
                                                                        "updated_at": datetime_now_str()}})
    else:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                  "details": f"Error during updating Ozon {list_url}",
                                                                  "event": "stock", "keys_id": items['keys_id'],
                                                                  "updated_at": datetime_now_str()}})

def stock_update_wb(items):
    database2 = dbconsync()
    headers = {
        "Authorization": items['Authorization']
    }
    json = {
        "stocks": items['stocks'] # sku: str, amount: int -- > []
    }
    list_url = settings.WB_STOCKS_URL + f"/{items['warehouse_id']}"
    r = httpx.put(list_url, headers=headers, json=json, timeout=20.0)
    if r.status_code == 204:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": 200,
                                                                        "details": f"WB stock updated",
                                                                        "event": "stock", "keys_id": items['keys_id'],
                                                                        "updated_at": datetime_now_str()}})
    else:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                      "details": f"Error during updating WB {list_url}",
                                                                      "event": "stock", "keys_id": items['keys_id'],
                                                                      "updated_at": datetime_now_str()}})

def stock_update_sber(items):
    database2 = dbconsync()
    headers = {
        'Content-Type': 'application/json',
    }

    json = {
        'meta': {},
        'data': {
            'token': items['token'],
            'stocks': items['stocks'], # offerId: str, quantity: int -- > []
        },
    }
    list_url = settings.SBER + f"/stock/update"
    r = httpx.post(list_url, headers=headers, json=json, timeout=20.0)
    if r.status_code == 200 and r.json()["success"] == 1:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                     "details": f"Sber stock updated",
                                                                     "event": "stock", "keys_id": items['keys_id'],
                                                                     "updated_at": datetime_now_str()}})
    else:
        database2.log.update_one({"_id": ObjectId(items['log_id'])}, {"$set": {"status": r.status_code,
                                                                  'details': f"Error during updating Sber {list_url}",
                                                                  "event": "stock", "keys_id": items['keys_id'],
                                                                  "updated_at": datetime_now_str()}})



async def watch_collection(db):
    resume_token = None
    pipeline = [{'$match': {'operationType': 'update'}}]
    while True:
        try:
            async with db.log.watch(pipeline) as stream:
                async for insert_change in stream:
                    resume_token = stream.resume_token
                    insert_change = json.dumps(insert_change, default=str)
                    yield insert_change
        except pymongo.errors.PyMongoError:
            if resume_token is None:
                pass
            else:
                async with db.log.watch(pipeline,
                         resume_after=resume_token) as stream:
                    async for insert_change in stream:
                        insert_change = json.dumps(insert_change, default=str)
                        yield insert_change


def get_current_username(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = bytes(settings.LOGIN, encoding='utf-8')
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = bytes(settings.PASSWD, encoding='utf-8')
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def coloredrows(column):
    print(column)
    if (column.check_offer_id == True or column.check_count == True or column.check_yandex == True or
        column.check_ali == True or column.check_sber == True or
        column.check_wb == True or column.check_ozon == True or column.check_negative_count_yandex == True or
        column.check_negative_count_ali == True or column.check_negative_count_sber == True or
        column.check_negative_count_wb == True or column.check_negative_count_ozon == True or
        column.check_offer_id_miss == True):
        color = 'background-color: red'
        return [color]*3
