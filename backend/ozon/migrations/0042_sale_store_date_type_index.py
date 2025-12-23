from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ozon", "0041_sale_product_indexes"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="sale",
            index=models.Index(fields=["store", "date", "sale_type"], name="ozon_sale_store_date_type_idx"),
        ),
    ]
