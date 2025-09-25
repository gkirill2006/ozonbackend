from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ozon', '0029_campaignperformancereportentry_report_date'),
    ]

    operations = [
        migrations.AddField(
            model_name='adplanitem',
            name='paused_due_to_low_stock',
            field=models.BooleanField(default=False, verbose_name='Отключена из-за низких остатков'),
        ),
    ]

