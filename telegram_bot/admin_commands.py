import requests
import json
from typing import Dict, Any

class AdminCommands:
    def __init__(self, bot_token: str, backend_url: str):
        self.bot_token = bot_token
        self.backend_url = backend_url
        
    def handle_admin_command(self, chat_id: int, user_id: int, username: str = None) -> str:
        """
        Обрабатывает команду /admin в боте
        """
        try:
            # Генерируем session_id на backend
            response = requests.get(f"{self.backend_url}/auth/generate-session-id/")
            
            if response.status_code == 200:
                data = response.json()
                session_id = data['session_id']
                
                # Сохраняем session_id в кэше с привязкой к пользователю
                cache_response = requests.post(f"{self.backend_url}/auth/cache-session/", json={
                    'session_id': session_id,
                    'user_id': user_id
                })
                
                if cache_response.status_code == 200:
                    return f"""
🔐 **Доступ к админке OPanel**

Ваш код для входа: `{session_id}`

📋 **Инструкция:**
1. Перейдите в админку: https://your-domain.com/login
2. Введите код выше
3. Подтвердите вход

⏰ Код действителен 10 минут
"""
                else:
                    return "❌ Ошибка создания сессии"
            else:
                return "❌ Ошибка генерации кода"
                
        except Exception as e:
            return f"❌ Ошибка: {str(e)}"
    
    def handle_session_code(self, chat_id: int, session_code: str) -> str:
        """
        Обрабатывает ввод кода сессии пользователем
        """
        try:
            # Проверяем код сессии
            response = requests.post(f"{self.backend_url}/auth/verify-session/", json={
                'session_id': session_code,
                'user_id': chat_id
            })
            
            if response.status_code == 200:
                return "✅ Код подтвержден! Теперь вы можете войти в админку."
            else:
                return "❌ Неверный или устаревший код"
                
        except Exception as e:
            return f"❌ Ошибка проверки кода: {str(e)}"

# Пример использования в основном файле бота:
"""
from admin_commands import AdminCommands

# В основном классе бота
admin_commands = AdminCommands(BOT_TOKEN, BACKEND_URL)

@bot.message_handler(commands=['admin'])
def handle_admin_command(message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    response = admin_commands.handle_admin_command(
        chat_id=message.chat.id,
        user_id=user_id,
        username=username
    )
    
    bot.reply_to(message, response, parse_mode='Markdown')

@bot.message_handler(func=lambda message: len(message.text) == 32 and message.text.isalnum())
def handle_session_code(message):
    session_code = message.text
    
    response = admin_commands.handle_session_code(
        chat_id=message.chat.id,
        session_code=session_code
    )
    
    bot.reply_to(message, response)
""" 
