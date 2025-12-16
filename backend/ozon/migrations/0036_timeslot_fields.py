from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ozon', '0035_ozonsupplydraft_selected_supply_warehouse'),
    ]

    operations = [
        migrations.AddField(
            model_name='ozonsupplydraft',
            name='selected_timeslot',
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='ozonsupplydraft',
            name='timeslot_response',
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='ozonsupplydraft',
            name='timeslot_updated_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
