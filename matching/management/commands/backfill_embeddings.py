"""
Backfill embedding vectors for all existing SupabaseProfile records.

Generates bge-large-en-v1.5 (1024-dim) embeddings for seeking, offering,
who_you_serve, and what_you_do fields using local sentence-transformers.

Usage:
    python manage.py backfill_embeddings
    python manage.py backfill_embeddings --batch-size 100
    python manage.py backfill_embeddings --dry-run
    python manage.py backfill_embeddings --force  # re-embed even if already done
"""

import time

from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone

from matching.models import SupabaseProfile


# Fields to embed and their corresponding DB columns
EMBEDDING_FIELDS = {
    'seeking': 'embedding_seeking',
    'offering': 'embedding_offering',
    'who_you_serve': 'embedding_who_you_serve',
    'what_you_do': 'embedding_what_you_do',
}


class Command(BaseCommand):
    help = 'Backfill embedding vectors for all profiles using bge-large-en-v1.5'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size', type=int, default=50,
            help='Number of profiles to process per batch (default: 50)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Count profiles and preview without writing embeddings',
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Re-embed profiles that already have embeddings',
        )
        parser.add_argument(
            '--profile-ids', nargs='+',
            help='Specific profile UUIDs to backfill (overrides default query)',
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        dry_run = options['dry_run']
        force = options['force']
        target_ids = options.get('profile_ids')

        start_time = time.time()

        # Initialize embedding service
        from lib.enrichment.hf_client import HFClient
        from lib.enrichment.embeddings import ProfileEmbeddingService

        hf_client = HFClient()
        emb_service = ProfileEmbeddingService(hf_client)

        # Load profile IDs upfront to avoid holding a long-lived cursor
        # (Supabase PgBouncer kills idle cursors after ~20 min)
        if target_ids:
            profile_ids = target_ids
            self.stdout.write(f'Targeting {len(profile_ids)} specific profile(s)')
        elif force:
            profile_ids = list(SupabaseProfile.objects.values_list('id', flat=True))
        else:
            profile_ids = list(
                SupabaseProfile.objects.filter(embeddings_updated_at__isnull=True)
                .values_list('id', flat=True)
            )

        total = len(profile_ids)
        self.stdout.write(f'Found {total} profiles to embed (force={force}, dry_run={dry_run})')

        if dry_run:
            qs = SupabaseProfile.objects.filter(id__in=profile_ids)
            for field in EMBEDDING_FIELDS:
                has_text = qs.exclude(**{field: None}).exclude(**{field: ''}).count()
                self.stdout.write(f'  {field}: {has_text}/{total} have text')
            return

        succeeded = 0
        failed = 0
        skipped = 0
        fields_embedded = 0

        for i, pid in enumerate(profile_ids):
            profile = SupabaseProfile.objects.get(id=pid)
            profile_dict = {
                'id': str(profile.id),
                'name': profile.name,
                'seeking': profile.seeking or '',
                'offering': profile.offering or '',
                'who_you_serve': profile.who_you_serve or '',
                'what_you_do': profile.what_you_do or '',
            }

            # Check if there's any text to embed
            has_text = any(
                profile_dict.get(f, '').strip() and len(profile_dict[f].strip()) >= 5
                for f in EMBEDDING_FIELDS
            )
            if not has_text:
                skipped += 1
                continue

            try:
                embeddings = emb_service.embed_profile(profile_dict)
                if not embeddings:
                    skipped += 1
                    continue

                # Write embeddings via raw SQL (Django can't natively write vector columns)
                set_clauses = []
                params = []
                for field_name, vector in embeddings.items():
                    pgvector_str = f'[{",".join(str(v) for v in vector)}]'
                    set_clauses.append(f'{field_name} = %s::vector')
                    params.append(pgvector_str)

                set_clauses.append('embeddings_model = %s')
                params.append('BAAI/bge-large-en-v1.5')
                set_clauses.append('embeddings_updated_at = %s')
                params.append(timezone.now())
                params.append(str(profile.id))

                sql = f"UPDATE profiles SET {', '.join(set_clauses)} WHERE id = %s"
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)

                succeeded += 1
                fields_embedded += len(embeddings)

            except Exception as e:
                failed += 1
                self.stderr.write(f'  Failed: {profile.name} ({profile.id}): {e}')

            # Progress logging
            if (i + 1) % batch_size == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                self.stdout.write(
                    f'  Progress: {i + 1}/{total} '
                    f'({succeeded} ok, {failed} failed, {skipped} skipped) '
                    f'[{rate:.1f} profiles/sec]'
                )

        elapsed = time.time() - start_time

        self.stdout.write(self.style.SUCCESS(
            f'\nBackfill complete:\n'
            f'  Total profiles:    {total}\n'
            f'  Succeeded:         {succeeded}\n'
            f'  Failed:            {failed}\n'
            f'  Skipped (no text): {skipped}\n'
            f'  Fields embedded:   {fields_embedded}\n'
            f'  Time elapsed:      {elapsed:.1f}s'
        ))
