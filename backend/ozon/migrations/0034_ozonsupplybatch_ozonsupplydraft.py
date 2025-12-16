from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0008_storeexcludedproduct_storerequiredproduct'),
        ('ozon', '0033_ozonwarehousedirectory'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            DROP TABLE IF EXISTS ozon_ozonsupplydraft CASCADE;
            DROP TABLE IF EXISTS ozon_ozonsupplybatch CASCADE;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.CreateModel(
            name='OzonSupplyBatch',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('batch_id', models.UUIDField(default=uuid.uuid4, editable=False, unique=True)),
                ('batch_seq', models.PositiveIntegerField(default=0)),
                ('supply_type', models.CharField(max_length=64)),
                ('drop_off_point_warehouse_id', models.BigIntegerField()),
                ('drop_off_point_name', models.CharField(blank=True, max_length=255)),
                ('status', models.CharField(default='queued', max_length=32)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('store', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='supply_batches', to='users.ozonstore')),
            ],
            options={
                'ordering': ('-created_at',),
            },
        ),
        migrations.CreateModel(
            name='OzonSupplyDraft',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('supply_type', models.CharField(max_length=64)),
                ('logistic_cluster_id', models.BigIntegerField()),
                ('logistic_cluster_name', models.CharField(max_length=255)),
                ('drop_off_point_warehouse_id', models.BigIntegerField()),
                ('drop_off_point_name', models.CharField(blank=True, max_length=255)),
                ('request_payload', models.JSONField()),
                ('response_payload', models.JSONField(blank=True, null=True)),
                ('operation_id', models.CharField(blank=True, max_length=64)),
                ('draft_id', models.BigIntegerField(blank=True, null=True)),
                ('supply_warehouse', models.JSONField(blank=True, null=True)),
                ('status', models.CharField(choices=[('queued', 'Queued'), ('in_progress', 'In progress'), ('draft_created', 'Draft created'), ('info_loaded', 'Info loaded'), ('failed', 'Failed')], default='queued', max_length=32)),
                ('attempts', models.PositiveIntegerField(default=0)),
                ('next_attempt_at', models.DateTimeField(blank=True, null=True)),
                ('error_message', models.TextField(blank=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('batch', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='drafts', to='ozon.ozonsupplybatch')),
                ('store', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='supply_drafts', to='users.ozonstore')),
            ],
            options={
                'verbose_name': 'Черновик поставки OZON',
                'verbose_name_plural': 'Черновики поставок OZON',
            },
        ),
        migrations.AddIndex(
            model_name='ozonsupplydraft',
            index=models.Index(fields=['store', 'logistic_cluster_id'], name='ozon_ozons_store_id_6006c7_idx'),
        ),
        migrations.AddIndex(
            model_name='ozonsupplydraft',
            index=models.Index(fields=['operation_id'], name='ozon_ozons_operati_44b308_idx'),
        ),
        migrations.AddIndex(
            model_name='ozonsupplydraft',
            index=models.Index(fields=['batch'], name='ozon_ozons_batch_i_b46082_idx'),
        ),
    ]
