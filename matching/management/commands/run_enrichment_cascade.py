"""
Django management command: run the enrichment cascade.

Usage:
    # Full cascade on Tier C+D
    python3 manage.py run_enrichment_cascade --tiers C,D

    # Layer 1 only (free scraping)
    python3 manage.py run_enrichment_cascade --layers 1

    # Layer 1+2 dry run (see how many qualify)
    python3 manage.py run_enrichment_cascade --layers 1,2 --dry-run

    # Layer 3 with Vast.ai GPU
    LLM_BASE_URL=http://gpu:8000/v1 LLM_MODEL=qwen/qwen3-30b-a3b \\
      python3 manage.py run_enrichment_cascade --layers 3

    # New profiles only (ongoing growth)
    python3 manage.py run_enrichment_cascade --new-only

    # Test with 50 profiles
    python3 manage.py run_enrichment_cascade --limit 50 --dry-run

    # Resume from checkpoint
    python3 manage.py run_enrichment_cascade --layers 1 --limit 1000 --resume
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Run the 6-layer enrichment cascade pipeline'

    def add_arguments(self, parser):
        parser.add_argument(
            '--layers',
            type=str,
            default='1,2,3,4,5,6',
            help='Comma-separated layer numbers to run (default: 1,2,3,4,5,6)',
        )
        parser.add_argument(
            '--tiers',
            type=str,
            default=None,
            help='Comma-separated JV tiers to process (e.g., C,D). Default: all.',
        )
        parser.add_argument(
            '--score-threshold',
            type=float,
            default=50.0,
            help='Minimum jv_readiness_score for Layer 2 qualification (default: 50)',
        )
        parser.add_argument(
            '--match-threshold',
            type=int,
            default=64,
            help='Minimum harmonic_mean for quality match in L5/L6 (default: 64)',
        )
        parser.add_argument(
            '--buffer-target',
            type=int,
            default=30,
            help='Target number of quality matches per client (default: 30)',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Batch size for Layer 3 AI enrichment (default: 1000)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Maximum number of profiles to process',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview mode — no DB writes or API calls',
        )
        parser.add_argument(
            '--resume',
            action='store_true',
            help='Resume from last checkpoint (skip already-processed profiles)',
        )
        parser.add_argument(
            '--new-only',
            action='store_true',
            help='Only process profiles imported since last cascade run',
        )

    def handle(self, *args, **options):
        from matching.enrichment.flows.cascade_flow import enrichment_cascade_flow

        # Parse layers
        layers = [int(x.strip()) for x in options['layers'].split(',')]

        # Parse tiers
        tier_filter = None
        if options['tiers']:
            tier_filter = {t.strip().upper() for t in options['tiers'].split(',')}

        # Checkpoint ID for resume
        checkpoint_id = None
        if options['resume']:
            # Use a fixed ID so checkpoint files accumulate across runs
            checkpoint_id = "resume"

        self.stdout.write(self.style.SUCCESS(
            f'\n{"="*60}\n'
            f'ENRICHMENT CASCADE\n'
            f'{"="*60}\n'
        ))
        self.stdout.write(f'  Layers:          {layers}')
        self.stdout.write(f'  Tiers:           {tier_filter or "all"}')
        self.stdout.write(f'  Score threshold: {options["score_threshold"]}')
        self.stdout.write(f'  Match threshold: {options["match_threshold"]}')
        self.stdout.write(f'  Buffer target:   {options["buffer_target"]}')
        self.stdout.write(f'  Batch size:      {options["batch_size"]}')
        self.stdout.write(f'  Limit:           {options["limit"] or "none"}')
        self.stdout.write(f'  Dry run:         {options["dry_run"]}')
        self.stdout.write(f'  Resume:          {options["resume"]}')
        self.stdout.write('')

        result = enrichment_cascade_flow(
            layers=layers,
            tier_filter=tier_filter,
            score_threshold=options['score_threshold'],
            match_threshold=options['match_threshold'],
            buffer_target=options['buffer_target'],
            batch_size=options['batch_size'],
            limit=options['limit'],
            dry_run=options['dry_run'],
            checkpoint_id=checkpoint_id,
        )

        # Print summary
        self.stdout.write(self.style.SUCCESS(
            f'\n{"="*60}\n'
            f'CASCADE COMPLETE\n'
            f'{"="*60}\n'
        ))

        if result.l1:
            self.stdout.write(f'  L1 Free Extraction:')
            self.stdout.write(f'    Found data:   {result.l1.get("profiles_found_data", 0)}')
            self.stdout.write(f'    Empty:        {result.l1.get("profiles_no_data", 0)}')
            self.stdout.write(f'    Errors:       {result.l1.get("profiles_error", 0)}')

        if result.l2:
            self.stdout.write(f'  L2 Rescore:')
            self.stdout.write(f'    Rescored:     {result.l2.get("profiles_rescored", 0)}')
            self.stdout.write(f'    Qualified:    {result.l2.get("qualified_count", 0)}')
            promotions = result.l2.get("tier_promotions", {})
            if promotions:
                self.stdout.write(f'    Promotions:   {promotions}')

        if result.l3:
            self.stdout.write(f'  L3 AI Enrichment:')
            self.stdout.write(f'    Enriched:     {result.l3.get("profiles_enriched", 0)}')
            self.stdout.write(f'    Errors:       {result.l3.get("profiles_error", 0)}')
            self.stdout.write(f'    Cost:         ${result.l3.get("total_cost", 0):.2f}')

        if result.l4:
            self.stdout.write(f'  L4 Claude Judge:')
            self.stdout.write(f'    Conflicts:    {result.l4.get("conflicts_found", 0)}')
            self.stdout.write(f'    Resolved:     {result.l4.get("conflicts_resolved", 0)}')

        if result.l5:
            self.stdout.write(f'  L5 Cross-Client:')
            self.stdout.write(f'    New matches:  {result.l5.get("new_matches", 0)}')
            self.stdout.write(f'    Reports flagged: {result.l5.get("reports_flagged", 0)}')

        if result.l6:
            self.stdout.write(f'  L6 Gap Acq:')
            self.stdout.write(f'    Clients checked:  {result.l6.get("clients_checked", 0)}')
            self.stdout.write(f'    With gaps:        {result.l6.get("clients_with_gaps", 0)}')
            self.stdout.write(f'    Acquisitions:     {result.l6.get("acquisitions_triggered", 0)}')

        self.stdout.write(f'\n  Total cost:    ${result.total_cost:.2f}')
        self.stdout.write(f'  Total runtime: {result.total_runtime:.1f}s')
        self.stdout.write(f'  Dry run:       {result.dry_run}')
