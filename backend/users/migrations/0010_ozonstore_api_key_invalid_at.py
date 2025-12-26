from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0009_storeaccess"),
    ]

    operations = [
        migrations.AddField(
            model_name="ozonstore",
            name="api_key_invalid_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
