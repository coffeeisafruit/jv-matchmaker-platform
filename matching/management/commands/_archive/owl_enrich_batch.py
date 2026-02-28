"""
Django management command for OWL batch profile enrichment.

Usage (CSV input):
    python manage.py owl_enrich_batch --input contacts.csv --max 10
    python manage.py owl_enrich_batch --input contacts.csv --resume

Usage (Supabase input - recommended):
    python manage.py owl_enrich_batch --from-supabase --max 100
    python manage.py owl_enrich_batch --from-supabase --filter-sparse --save-to-supabase

Parallel processing (faster for large batches):
    python manage.py owl_enrich_batch --from-supabase --workers 3 --max 50
    python manage.py owl_enrich_batch --from-supabase --workers 5 --save-to-supabase
"""

import csv
from pathlib import Path
from django.core.management.base import BaseCommand

from matching.enrichment.owl_research.processors.owl_batch_processor import (
    run_owl_batch_enrichment_sync,
    load_profiles_from_csv,
    load_profiles_from_supabase,
)


class Command(BaseCommand):
    help = 'Run OWL batch enrichment on profiles from CSV or Supabase'

    def add_arguments(self, parser):
        # Input source (CSV or Supabase)
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            '--input',
            type=str,
            help='Path to input CSV file with profiles'
        )
        input_group.add_argument(
            '--from-supabase',
            action='store_true',
            help='Load profiles directly from Supabase database'
        )

        # Output options
        parser.add_argument(
            '--output',
            type=str,
            default='owl_enrichment_output',
            help='Output directory for results'
        )

        # Processing options
        parser.add_argument(
            '--max',
            type=int,
            default=None,
            help='Maximum profiles to process (for testing)'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=2.0,
            help='Delay in seconds between profiles (rate limiting)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=1,
            help='Number of concurrent workers for parallel processing (default: 1)'
        )
        parser.add_argument(
            '--no-resume',
            action='store_true',
            help='Start fresh instead of resuming from checkpoint'
        )

        # Supabase-specific options
        parser.add_argument(
            '--filter-sparse',
            action='store_true',
            default=True,
            help='Only process profiles missing key JV fields (default: True)'
        )
        parser.add_argument(
            '--no-filter',
            action='store_true',
            help='Process all profiles, even those with complete data'
        )
        parser.add_argument(
            '--require-website',
            action='store_true',
            help='Only include profiles that have a website (improves research quality)'
        )
        parser.add_argument(
            '--save-to-supabase',
            action='store_true',
            help='Write enriched data back to Supabase (recommended)'
        )

    def handle(self, *args, **options):
        input_csv = options.get('input')
        from_supabase = options.get('from_supabase', False)
        output_dir = options['output']
        max_profiles = options.get('max')
        delay = options.get('delay', 2.0)
        workers = options.get('workers', 1)
        resume = not options.get('no_resume', False)

        # Supabase options
        filter_sparse = not options.get('no_filter', False)
        require_website = options.get('require_website', False)
        save_to_supabase = options.get('save_to_supabase', False)

        self.stdout.write(self.style.SUCCESS('\n' + '='*60))
        self.stdout.write(self.style.SUCCESS('OWL BATCH ENRICHMENT'))
        self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

        # Load and preview profiles
        if from_supabase:
            self.stdout.write('Source: Supabase database')
            profiles = load_profiles_from_supabase(
                filter_sparse=filter_sparse,
                require_website=require_website,
            )
            self.stdout.write(f'Loaded {len(profiles)} profiles from Supabase')

            if filter_sparse:
                self.stdout.write(self.style.WARNING('  (filtered to profiles missing key JV fields)'))
            if require_website:
                self.stdout.write(self.style.WARNING('  (filtered to profiles with websites)'))
        else:
            # Validate CSV input file
            if not Path(input_csv).exists():
                self.stderr.write(self.style.ERROR(f'Input file not found: {input_csv}'))
                return

            self.stdout.write(f'Source: {input_csv}')
            profiles = load_profiles_from_csv(input_csv)
            self.stdout.write(f'Loaded {len(profiles)} profiles')

        if not profiles:
            self.stdout.write(self.style.WARNING('No profiles to process!'))
            return

        # Show preview
        self.stdout.write('\nFirst 5 profiles:')
        for i, p in enumerate(profiles[:5]):
            name = p.get('name', p.get('Name', 'Unknown'))
            company = p.get('company', p.get('Company', ''))
            missing = p.get('_missing_fields', [])
            missing_str = f" [missing: {', '.join(missing[:2])}...]" if missing else ""
            self.stdout.write(f'  {i+1}. {name} ({company}){missing_str}')

        # Show configuration
        self.stdout.write('\nConfiguration:')
        if max_profiles:
            self.stdout.write(f'  Will process: {max_profiles} profiles')
        else:
            self.stdout.write(f'  Will process: ALL {len(profiles)} profiles')

        self.stdout.write(f'  Output directory: {output_dir}')
        self.stdout.write(f'  Delay between profiles: {delay}s')
        self.stdout.write(f'  Workers (parallel): {workers}')
        self.stdout.write(f'  Resume from checkpoint: {resume}')

        if save_to_supabase:
            self.stdout.write(self.style.SUCCESS('  💾 Will save enriched data back to Supabase'))

        self.stdout.write(self.style.WARNING('\nStarting OWL enrichment...\n'))

        # Run enrichment
        try:
            progress = run_owl_batch_enrichment_sync(
                input_csv=input_csv,
                output_dir=output_dir,
                resume=resume,
                max_profiles=max_profiles,
                delay=delay,
                workers=workers,
                # Supabase options
                from_supabase=from_supabase,
                filter_sparse=filter_sparse,
                save_to_supabase=save_to_supabase,
                require_website=require_website,
            )

            # Summary
            self.stdout.write(self.style.SUCCESS('\n' + '='*60))
            self.stdout.write(self.style.SUCCESS('OWL ENRICHMENT COMPLETE'))
            self.stdout.write(self.style.SUCCESS('='*60))
            self.stdout.write(f'Completed: {progress.completed}')
            self.stdout.write(f'Failed: {progress.failed}')
            self.stdout.write(f'Skipped: {progress.skipped}')
            self.stdout.write(f'Avg Verified Fields: {progress.avg_verified_fields:.1f}/12')
            self.stdout.write(f'\nResults saved to: {output_dir}/')

            if save_to_supabase:
                self.stdout.write(self.style.SUCCESS('💾 Enriched data has been saved to Supabase'))

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('\n\nInterrupted! Progress has been saved.'))
            self.stdout.write('Run with --resume (default) to continue from checkpoint.')
        except Exception as e:
            self.stderr.write(self.style.ERROR(f'\nError: {e}'))
            raise
