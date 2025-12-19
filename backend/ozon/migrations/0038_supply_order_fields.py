from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ozon", "0037_operation_id_supply_created_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="ozonsupplydraft",
            name="supply_order_ids",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="ozonsupplydraft",
            name="supply_order_response",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="ozonsupplydraft",
            name="supply_bundle_items",
            field=models.JSONField(blank=True, null=True),
        ),
    ]
