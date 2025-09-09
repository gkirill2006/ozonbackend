import sys
import os
sys.path.append(f'{os.getcwd()}/user')
from user import utils
from config import settings
from user.utils import datetime_now_str


async def adduser(db, settings):
        dt_now = datetime_now_str()
        user = {'username': settings.ADMIN_LOGIN, 'password': utils.hash_password(settings.ADMIN_PASSWORD),
                "photo": "/storage/photo/example/0.png",
                'created_at': dt_now, 'updated_at': dt_now}
        db.user.insert_one(user)
