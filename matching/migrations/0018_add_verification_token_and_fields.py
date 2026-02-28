"""
Add verification_token, status, original_data, updated_data, and
fields_confirmed columns to ClientVerification for the client-facing
profile confirmation flow.
"""

import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0017_add_verification_and_cost_models'),
    ]

    operations = [
        migrations.AddField(
            model_name='clientverification',
            name='verification_token',
            field=models.UUIDField(
                default=uuid.uuid4,
                help_text='Token embedded in the verification email link',
                unique=True,
                db_index=True,
            ),
            # For existing rows we need a one-off default; uuid4 generates
            # a unique value per row because Django calls the callable.
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='clientverification',
            name='status',
            field=models.CharField(
                choices=[
                    ('pending', 'Pending'),
                    ('confirmed', 'Confirmed'),
                    ('expired', 'Expired'),
                ],
                default='pending',
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name='clientverification',
            name='original_data',
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text='Snapshot of profile data at the time the verification was created',
            ),
        ),
        migrations.AddField(
            model_name='clientverification',
            name='updated_data',
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text='Data submitted by the client via the confirmation form',
            ),
        ),
        migrations.AddField(
            model_name='clientverification',
            name='fields_confirmed',
            field=models.JSONField(
                blank=True,
                default=list,
                help_text='List of field names the client explicitly confirmed or edited',
            ),
        ),
    ]
