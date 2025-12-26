from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ozon", "0043_fbs_postings"),
    ]

    operations = [
        migrations.CreateModel(
            name="OzonFbsPostingLabel",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("task_id", models.BigIntegerField()),
                ("task_type", models.CharField(choices=[("big_label", "Big label"), ("small_label", "Small label")], max_length=32)),
                ("status", models.CharField(blank=True, max_length=32)),
                ("file_url", models.URLField(blank=True)),
                ("file_path", models.CharField(blank=True, max_length=512)),
                ("response_payload", models.JSONField(blank=True, null=True)),
                ("error_message", models.TextField(blank=True)),
                ("last_checked_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("posting", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="labels", to="ozon.ozonfbsposting")),
            ],
            options={
                "verbose_name": "FBS posting label",
                "verbose_name_plural": "FBS posting labels",
                "unique_together": {("posting", "task_type")},
            },
        ),
        migrations.AddIndex(
            model_name="ozonfbspostinglabel",
            index=models.Index(fields=["posting", "task_type"], name="ozon_fbs_label_posting_type_idx"),
        ),
        migrations.AddIndex(
            model_name="ozonfbspostinglabel",
            index=models.Index(fields=["status"], name="ozon_fbs_label_status_idx"),
        ),
    ]
