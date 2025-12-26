from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("ozon", "0044_fbs_posting_labels"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(
                fields=["store", "status", "archived_at"],
                name="ozon_fbs_store_status_arch_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(
                fields=["store", "status", "last_seen_at"],
                name="ozon_fbs_store_status_seen_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(
                fields=["store", "needs_label"],
                name="ozon_fbs_store_label_idx",
            ),
        ),
    ]
