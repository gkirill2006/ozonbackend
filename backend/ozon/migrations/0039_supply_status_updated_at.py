from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ozon", "0038_supply_order_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="ozonsupplydraft",
            name="supply_status_updated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
