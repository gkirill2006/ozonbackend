from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ozon', '0030_adplanitem_paused_due_to_low_stock'),
    ]

    operations = [
        migrations.AddField(
            model_name='adplanitem',
            name='is_mandatory',
            field=models.BooleanField(default=False, verbose_name='Обязательная кампания'),
        ),
    ]
