from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ozon", "0036_timeslot_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="ozonsupplydraft",
            name="operation_id_supply",
            field=models.CharField(blank=True, max_length=64),
        ),
    ]
