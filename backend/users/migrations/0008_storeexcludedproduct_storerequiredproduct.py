from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0007_storefiltersettings'),
    ]

    operations = [
        migrations.CreateModel(
            name='StoreRequiredProduct',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('article', models.CharField(max_length=255)),
                ('quantity', models.PositiveIntegerField(default=1)),
                ('filter_settings', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='required_products', to='users.storefiltersettings')),
            ],
            options={
                'verbose_name': 'Store required product',
                'verbose_name_plural': 'Store required products',
            },
        ),
        migrations.CreateModel(
            name='StoreExcludedProduct',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('article', models.CharField(max_length=255)),
                ('filter_settings', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='excluded_products', to='users.storefiltersettings')),
            ],
            options={
                'verbose_name': 'Store excluded product',
                'verbose_name_plural': 'Store excluded products',
            },
        ),
        migrations.AlterUniqueTogether(
            name='storerequiredproduct',
            unique_together={('filter_settings', 'article')},
        ),
        migrations.AlterUniqueTogether(
            name='storeexcludedproduct',
            unique_together={('filter_settings', 'article')},
        ),
    ]
