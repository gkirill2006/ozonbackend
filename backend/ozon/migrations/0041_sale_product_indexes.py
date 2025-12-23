from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ozon", "0040_merge_20251223_1228"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="product",
            index=models.Index(fields=["store", "price"], name="ozon_prod_store_price_idx"),
        ),
        migrations.AddIndex(
            model_name="sale",
            index=models.Index(fields=["store", "date"], name="ozon_sale_store_date_idx"),
        ),
    ]
