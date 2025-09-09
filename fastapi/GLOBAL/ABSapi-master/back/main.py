from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings
import logging
from fastapi.middleware.cors import CORSMiddleware
import subprocess
from pathlib import Path
import os
from addduser import adduser
from pymongo import MongoClient, read_preferences
import sys
import os
sys.path.append(f'{os.getcwd()}/user')
sys.path.append(f'{os.getcwd()}')
from user.utils import *
from user.routes import *
from apps.groups.routes import group_router

origins = [
    "https://localhost",
    "https://localhost:8000",
    "http://localhost",
    "http://localhost:8000",
    "https://panel.checkyourstore.ru",
    "https://panel.checkyourstore.ru:80",
    "https://panel.checkyourstore.ru:443",

]


async def lifespan(app: FastAPI):
    init_logging()
    print(f'Is online {datetime_now_str()}')
    #multiprocessing.set_start_method("spawn", force=True)
    processes = []
    for i in range(3):
        pport = 27018 + i
        ppath = os.getcwd() + "/data/db" + str(i)
        Path(ppath).mkdir(parents=True, exist_ok=True)
        p = multiprocessing.Process(target=subprocess.call(
            f"screen -dmS db0 mongod --port {pport} --bind_ip localhost --dbpath {ppath} --replSet foo", shell=True))
        p.start()
        processes.append(p)
    c = AsyncIOMotorClient('localhost', 27018, directConnection=True)
    config = {'_id': 'foo', 'members': [{'_id': 0, 'host': 'localhost:27018'}, {'_id': 1, 'host': 'localhost:27019'},
                                        {'_id': 2, 'host': 'localhost:27020', 'arbiterOnly': True}]}
    c.admin.command("replSetInitiate", config)
    app.client = AsyncIOMotorClient(settings.DB_URI2)
    app.database = app.client[settings.DB_NAME]
    app.client2 = MongoClient(settings.DB_URI2)
    app.database2 = app.client2[settings.DB_NAME]
    #app.client.drop_database(settings.DB_NAME)
    user = await app.database.user.find_one({'username': 'admin'})
    if not user:
        await adduser(app.database, settings)
    print("Connected to the MongoDB database!")
    yield
    print("Disconnected from the MongoDB database!")
    app.client.close()
    app.client2.close()
    c.close()
    for p in processes:
        p.close()

app = FastAPI(lifespan=lifespan, openapi_url="/d/openapi.json",
              docs_url="/d/docs", redoc_url="/d/redoc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "GET", "PUT", "DELETE"],
    allow_headers=["*"],
)


app.include_router(user_router, tags=["user"], prefix="/api/v2/user")
app.include_router(product_router, tags=["product"], prefix="/api/v2/product")
app.include_router(warehouse_router, tags=["warehouse"], prefix="/api/v2/warehouse")
app.include_router(logs_router, tags=["logs"], prefix="/api/v2/log")
app.include_router(settings_router, tags=["settings"], prefix="/api/v2/settings")
app.include_router(sync_router, tags=["sync"], prefix="/api/v2/sync")
app.include_router(sber, tags=["sber"], prefix="/api/v2/sber")
app.include_router(group_router, tags=['group'], prefix='/api/v2/group')
