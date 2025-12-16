import os, json
from pathlib import Path
from datetime import timedelta

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
STATICFILES_DIRS = [
    BASE_DIR / 'static',
]


AUTH_USER_MODEL = 'users.User'
# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-_w0m5u@_1g5x$pb+akrhxk(#7h3$0a!2_tle(i!x_ay_rv!mzn'
# GOOGLE_CREDENTIALS_DICT = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "{}"))

API_KEY = '792049e29b622a24a4fa86958d487d3d43306eec796d1b56739db393e221e1f1'

#Local
TELEGRAM_BOT_TOKEN="7789819880:AAEiangD6q1z6B16VdqMM_ADWRWNyXsDsUQ"

# TELEGRAM_BOT_TOKEN = '7619741744:AAGGn9WicVRcDrvaPT0LN5uIr9Cw4vyWOSg'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
# DEBUG = False

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

ALLOWED_HOSTS = [
    "markets-backend",
    "markets-backend:8000",
    "localhost",
    "127.0.0.1",
    "94.141.122.224",
    "188.68.222.242",
    "ozon.codemark.me"
]
CSRF_TRUSTED_ORIGINS = [
    'http://markets-backend:8000',
    'https://t.me',
    'https://web.telegram.org',
    'http://localhost:9000',
    'http://127.0.0.1:9000',
    'https://ozon.codemark.me'
]

# CSRF_TRUSTED_ORIGINS = ['']

WSGI_APPLICATION = 'backend.wsgi.application'
# Application definition

INSTALLED_APPS = [
    'jazzmin',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'ozon',
    'users',
    'corsheaders',
    'django_celery_beat'
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ),
}

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(days=7),  # время жизни access-токена
    'REFRESH_TOKEN_LIFETIME': timedelta(days=7),     # время жизни refresh-токена
    'ROTATE_REFRESH_TOKENS': False,
    'BLACKLIST_AFTER_ROTATION': False,
    'ALGORITHM': 'HS256',
    'SIGNING_KEY': SECRET_KEY,
}
ROOT_URLCONF = 'backend.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]




# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('POSTGRES_DB'),
        'USER': os.getenv('POSTGRES_USER'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD'),
        'HOST': os.getenv('POSTGRES_HOST', 'db'),  # Default to 'db' for Docker
        'PORT': os.getenv('POSTGRES_PORT', '5432'),
    }
}


# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = 'ru'

TIME_ZONE = 'Europe/Moscow'

USE_I18N = True
USE_L10N = True
USE_TZ = True


DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOW_CREDENTIALS = True

JAZZMIN_SETTINGS = {
    "site_title": "Admin",
    "site_header": "Панель управления",
    "site_brand": "",
    "welcome_sign": "Добро пожаловать в панель управления",
    "copyright": " © 2025",
    "show_sidebar": True,
    "navigation_expanded": True,
    "hide_apps": [],
    "hide_models": [],
    "icons": {
        "auth.User": "fas fa-user",
        "auth.Group": "fas fa-users",
        "yourapp.TelegramUser": "fas fa-robot",
    },
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',  # поток в консоль
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',  # минимальный уровень логов
    },
}


# Celery settings
CELERY_BROKER_URL = 'redis://redis:6379/0'
CELERY_RESULT_BACKEND = 'redis://redis:6379/0'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Europe/Moscow'

CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

# Настройки для предотвращения утечек памяти и стабильности
CELERY_WORKER_MAX_MEMORY_PER_CHILD = 500000  # 500MB
CELERY_WORKER_CONCURRENCY = 2
CELERY_TASK_TIME_LIMIT = 3600  # 1 час максимальное время выполнения задачи
CELERY_TASK_SOFT_TIME_LIMIT = 3000  # 50 минут мягкий лимит
CELERY_WORKER_DISABLE_RATE_LIMITS = True
CELERY_TASK_IGNORE_RESULT = True
CELERY_RESULT_EXPIRES = 3600  # Результаты удаляются через час

# Настройки для стабильности брокера
CELERY_BROKER_CONNECTION_RETRY = True
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BROKER_CONNECTION_MAX_RETRIES = 10

# CELERY_BROKER_URL = "redis://redis:6379"
# CELERY_RESULT_BACKEND = "redis://redis:6379"
DATA_UPLOAD_MAX_NUMBER_FIELDS = 100000
