from django.apps import AppConfig


# ozon/apps.py

class OzonConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'ozon'

    def ready(self):
        import ozon.tasks  # <- обязательно, чтобы celery подхватил

