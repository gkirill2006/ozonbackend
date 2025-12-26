from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0009_storeaccess"),
        ("ozon", "0042_sale_store_date_type_index"),
    ]

    operations = [
        migrations.CreateModel(
            name="OzonBotSettings",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("pdf_sort_mode", models.CharField(choices=[("offer_id", "Offer ID"), ("weight", "Weight"), ("created_at", "Created at")], default="created_at", max_length=32)),
                ("pdf_sort_ascending", models.BooleanField(default=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("store", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="bot_settings", to="users.ozonstore")),
            ],
            options={
                "verbose_name": "Bot settings",
                "verbose_name_plural": "Bot settings",
            },
        ),
        migrations.CreateModel(
            name="OzonFbsPosting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("posting_number", models.CharField(max_length=64)),
                ("order_id", models.BigIntegerField(blank=True, null=True)),
                ("order_number", models.CharField(blank=True, max_length=64)),
                ("status", models.CharField(choices=[("awaiting_packaging", "Awaiting packaging"), ("awaiting_deliver", "Awaiting deliver"), ("acceptance_in_progress", "Acceptance in progress"), ("delivering", "Delivering"), ("delivered", "Delivered"), ("cancelled", "Cancelled"), ("unknown", "Unknown")], default="unknown", max_length=32)),
                ("substatus", models.CharField(blank=True, max_length=64)),
                ("delivery_method_id", models.BigIntegerField(blank=True, null=True)),
                ("delivery_method_name", models.CharField(blank=True, max_length=255)),
                ("delivery_method_warehouse_id", models.BigIntegerField(blank=True, null=True)),
                ("delivery_method_warehouse", models.CharField(blank=True, max_length=255)),
                ("tpl_provider_id", models.BigIntegerField(blank=True, null=True)),
                ("tpl_provider", models.CharField(blank=True, max_length=255)),
                ("tpl_integration_type", models.CharField(blank=True, max_length=64)),
                ("tracking_number", models.CharField(blank=True, max_length=128)),
                ("in_process_at", models.DateTimeField(blank=True, null=True)),
                ("shipment_date", models.DateTimeField(blank=True, null=True)),
                ("delivering_date", models.DateTimeField(blank=True, null=True)),
                ("cancellation", models.JSONField(blank=True, null=True)),
                ("available_actions", models.JSONField(blank=True, null=True)),
                ("products", models.JSONField(blank=True, null=True)),
                ("raw_payload", models.JSONField(blank=True, null=True)),
                ("status_changed_at", models.DateTimeField(blank=True, null=True)),
                ("awaiting_packaging_at", models.DateTimeField(blank=True, null=True)),
                ("awaiting_deliver_at", models.DateTimeField(blank=True, null=True)),
                ("acceptance_in_progress_at", models.DateTimeField(blank=True, null=True)),
                ("delivering_at", models.DateTimeField(blank=True, null=True)),
                ("delivered_at", models.DateTimeField(blank=True, null=True)),
                ("cancelled_at", models.DateTimeField(blank=True, null=True)),
                ("archived_at", models.DateTimeField(blank=True, null=True)),
                ("needs_label", models.BooleanField(default=False)),
                ("labels_printed_at", models.DateTimeField(blank=True, null=True)),
                ("print_count", models.PositiveIntegerField(default=0)),
                ("last_seen_at", models.DateTimeField(blank=True, null=True)),
                ("last_synced_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("store", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="fbs_postings", to="users.ozonstore")),
            ],
            options={
                "verbose_name": "FBS posting",
                "verbose_name_plural": "FBS postings",
                "unique_together": {("store", "posting_number")},
            },
        ),
        migrations.CreateModel(
            name="OzonFbsPostingPrintLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("forced", models.BooleanField(default=False)),
                ("printed_at", models.DateTimeField(auto_now_add=True)),
                ("meta", models.JSONField(blank=True, null=True)),
                ("posting", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="print_logs", to="ozon.ozonfbsposting")),
                ("user", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "verbose_name": "FBS posting print log",
                "verbose_name_plural": "FBS posting print logs",
            },
        ),
        migrations.CreateModel(
            name="OzonFbsPostingStatusHistory",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(max_length=32)),
                ("changed_at", models.DateTimeField()),
                ("source", models.CharField(choices=[("ozon", "Ozon"), ("bot", "Bot"), ("system", "System")], default="ozon", max_length=16)),
                ("payload", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("posting", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="status_history", to="ozon.ozonfbsposting")),
            ],
            options={
                "verbose_name": "FBS posting status history",
                "verbose_name_plural": "FBS posting status history",
            },
        ),
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(fields=["store", "status"], name="ozon_fbs_store_status_idx"),
        ),
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(fields=["store", "posting_number"], name="ozon_fbs_store_posting_idx"),
        ),
        migrations.AddIndex(
            model_name="ozonfbsposting",
            index=models.Index(fields=["status", "archived_at"], name="ozon_fbs_status_archived_idx"),
        ),
        migrations.AddIndex(
            model_name="ozonfbspostingstatushistory",
            index=models.Index(fields=["posting", "status"], name="ozon_fbs_hist_posting_status_idx"),
        ),
        migrations.AddIndex(
            model_name="ozonfbspostingstatushistory",
            index=models.Index(fields=["changed_at"], name="ozon_fbs_hist_changed_at_idx"),
        ),
    ]
