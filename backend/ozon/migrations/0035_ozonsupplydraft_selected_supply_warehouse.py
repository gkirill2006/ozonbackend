from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ozon', '0034_ozonsupplybatch_ozonsupplydraft'),
    ]

    operations = [
        migrations.AddField(
            model_name='ozonsupplydraft',
            name='selected_supply_warehouse',
            field=models.JSONField(blank=True, null=True),
        ),
    ]
