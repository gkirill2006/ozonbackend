import datetime
import json
import telebot
import requests
import ssl
from pprint import pprint
import os
import time
from telebot import apihelper
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

print("START TG BOT")
api_key_bot = os.environ.get("TELEGRAM_BOT_TOKEN")
print(f"API key = {api_key_bot}")
bot = telebot.TeleBot(api_key_bot)

url = os.environ.get("API_URL")
access_api = "792049e29b622a24a4fa86958d487d3d43306eec796d1b56739db393e221e1f1"
url_BotInit = f"{url}/auth/4bBFJCoiYnhFjbz3awRJ5LorPYLVtUNy/"
print(url_BotInit)

tg_proxy = os.environ.get("TELEGRAM_PROXY")
if tg_proxy:
    apihelper.proxy = {"https": tg_proxy, "http": tg_proxy}

tg_api_url = os.environ.get("TELEGRAM_API_URL")
if tg_api_url:
    apihelper.API_URL = tg_api_url.rstrip("/") + "/bot{0}/{1}"

apihelper.RETRY_ON_ERROR = True
apihelper.CONNECT_TIMEOUT = int(os.environ.get("TG_CONNECT_TIMEOUT", "10"))
apihelper.READ_TIMEOUT = int(os.environ.get("TG_READ_TIMEOUT", "20"))

SESSION = requests.Session()
RETRY = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST"),
)
ADAPTER = HTTPAdapter(max_retries=RETRY)
SESSION.mount("http://", ADAPTER)
SESSION.mount("https://", ADAPTER)


def api_request(method, target_url, **kwargs):
    timeout = kwargs.pop("timeout", 20)
    return SESSION.request(method, target_url, timeout=timeout, **kwargs)
@bot.message_handler(commands=['start'])
def send_welcome(message):
    
    print("Enter to start")
    if message.from_user.is_bot == False:
        print(message.text)
        text = message.text.split()
        print(text)
        message_from_user = text[-1]
        message_from_user = message_from_user.split('refcode')
        print(message_from_user)
        session_id = message_from_user[0]
        ref_code = None
        try:
            if message_from_user[1]:
                ref_code = message_from_user[1]
                print(f"ref_code = {ref_code}")        
        except:
            print("No refcode")
        
        if session_id != '/start':
            
            
            print(session_id)


            payload = json.dumps({
                "session_id": session_id,
                "telegram_id": message.from_user.id,
                "language_code": message.from_user.language_code,
                "is_bot": message.from_user.is_bot,
                "username": message.from_user.username,
                "referred_by" : ref_code
            })
            headers = {
                'Content-Type': 'application/json',
                'Api-Key': access_api
            }
            try:
                print("Попадаем в чекпоинт 1")
                response = api_request("POST", url_BotInit, headers=headers, data=payload)
                print(f"Response status: {response.status_code}")
                print(f"Response content: {response.text}")
                
                if response.status_code == 200:
                    bot.reply_to(message, "✅ Авторизация успешна! Теперь можете вернуться в приложение.")
                else:
                    bot.reply_to(message, "❌ Ошибка при авторизации. Попробуйте еще раз.")
            except Exception as e:
                print(f"An error: {e}")
                bot.reply_to(message, "❌ Произошла ошибка. Попробуйте еще раз.")
        elif message.text == '/start':
            print("Попадаем в чекпоинт 2")
            
            payload = json.dumps({
                "telegram_id": message.from_user.id,
                "language_code": message.from_user.language_code,
                "is_bot": message.from_user.is_bot,
                "username": message.from_user.username,
            })
            headers = {
                'Content-Type': 'application/json',
                'Api-Key': access_api
            }
            try:
                response = api_request("POST", url_BotInit, headers=headers, data=payload)
                print(response)
                # mess = "Вы успешно авторизовались, теперь можно вернуться обратно на сайт"
            except Exception as e:
                print(f"An error: {e}")

@bot.message_handler(commands=['admin'])
def admin_auth(message):
    """Обработка команды /admin для авторизации в админ-панели"""
    if message.from_user.is_bot:
        return
    
    # Получаем session_id из сообщения
    text = message.text.split()
    if len(text) < 2:
        bot.reply_to(message, "❌ Неверный формат команды. Используйте: /admin <session_id>")
        return
    
    session_id = text[1]
    
    # Создаем или получаем пользователя
    user_payload = json.dumps({
        "telegram_id": message.from_user.id,
        "language_code": message.from_user.language_code,
        "is_bot": message.from_user.is_bot,
        "username": message.from_user.username,
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Api-Key': access_api
    }
    
    try:
        # Создаем пользователя
        user_response = api_request("POST", url_BotInit, headers=headers, data=user_payload)
        if user_response.status_code == 200:
            user_data = user_response.json()
            user_id = user_data.get('user_id') or user_data.get('id')
            
            # Сохраняем session_id с привязкой к пользователю
            cache_payload = json.dumps({
                "session_id": session_id,
                "user_id": user_id
            })
            
            cache_response = api_request(
                "POST",
                f"{url}/auth/cache-session/",
                headers=headers,
                data=cache_payload,
            )
            
            if cache_response.status_code == 200:
                bot.reply_to(message, "✅ Авторизация успешна! Теперь можете вернуться в админ-панель.")
            else:
                bot.reply_to(message, "❌ Ошибка при сохранении сессии. Попробуйте еще раз.")
        else:
            bot.reply_to(message, "❌ Ошибка при создании пользователя. Попробуйте еще раз.")
            
    except Exception as e:
        print(f"Error in admin auth: {e}")
        bot.reply_to(message, "❌ Произошла ошибка. Попробуйте еще раз.")
        
if __name__ == '__main__':
    while True:
        try:
            bot.infinity_polling(
                timeout=apihelper.READ_TIMEOUT,
                long_polling_timeout=apihelper.READ_TIMEOUT,
                skip_pending=True,
            )
        except Exception as e:  # noqa: BLE001
            print(f"Polling error: {e}")
            time.sleep(5)
