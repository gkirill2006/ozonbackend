import os
from celery import Celery
from django.conf import settings
from celery.schedules import crontab


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')

app = Celery('backend')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
    
# app.conf.beat_schedule = {
#     'update_delivery_clusters-every-4-hour': {
#         'task': 'ozon.tasks.update_delivery_clusters',
#         'schedule': crontab(minute=0, hour='*/4'),  # каждые 4 часа
#     },
#     'update_cluster_item_analytics-every-4-hour': {
#         'task': 'ozon.tasks.update_cluster_item_analytics',
#         'schedule': crontab(minute=0, hour='*/4'),   # каждые 4 часа
#     },
    
# }