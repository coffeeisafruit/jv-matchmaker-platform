"""
OWL Enrichment for Janet's Becoming International JV Matches

Uses OWL's multi-agent research to:
1. Enrich Janet's profile from becominginternational.com
2. Enrich top matches with verified contact data
3. Output enriched CSV with confidence scores
"""
import asyncio
import csv
import os
from datetime import datetime
from django.core.management.base import BaseCommand

from matching.enrichment.owl_research.agents.owl_enrichment_service import (
    OWLEnrichmentService,
)


# Janet's profile for enrichment
JANET_PROFILE = {
    'name': 'Janet Bray Attwood',
    'company': 'Becoming International',
    'website': 'https://www.becominginternational.com',
    'linkedin': '',
}


class Command(BaseCommand):
    help = 'Enrich Janet and her top JV matches using OWL research'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            default='Chelsea_clients/janet_matches.csv',
            help='Input CSV with matches to enrich'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='Chelsea_clients/janet_matches_enriched.csv',
            help='Output CSV with enriched data'
        )
        parser.add_argument(
            '--top',
            type=int,
            default=20,
            help='Number of top matches to enrich (default: 20)'
        )
        parser.add_argument(
            '--enrich-janet',
            action='store_true',
            help='Also enrich Janet\'s profile from becominginternational.com'
        )
        parser.add_argument(
            '--skip-matches',
            action='store_true',
            help='Skip match enrichment (only enrich Janet if --enrich-janet is set)'
        )

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']
        top_n = options['top']
        enrich_janet = options['enrich_janet']
        skip_matches = options['skip_matches']

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('OWL ENRICHMENT: Janet Becoming International'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}\n'))

        # Run async enrichment
        asyncio.run(self._run_enrichment(
            input_file, output_file, top_n, enrich_janet, skip_matches
        ))

    async def _run_enrichment(
        self,
        input_file: str,
        output_file: str,
        top_n: int,
        enrich_janet: bool,
        skip_matches: bool
    ):
        service = OWLEnrichmentService()

        # Step 1: Enrich Janet's profile if requested
        if enrich_janet:
            self.stdout.write('\n--- Enriching Janet\'s Profile ---')
            self.stdout.write(f'Website: {JANET_PROFILE["website"]}')

            janet_result = await service.enrich_profile(
                name=JANET_PROFILE['name'],
                company=JANET_PROFILE['company'],
                website=JANET_PROFILE['website'],
            )

            if janet_result.enriched:
                self.stdout.write(self.style.SUCCESS(
                    f'Janet enriched: {janet_result.enriched.get_verified_field_count()}/12 verified fields'
                ))
                self.stdout.write(janet_result.enriched.get_verification_report())

                # Save Janet's enriched profile
                janet_output = 'Chelsea_clients/janet_profile_enriched.json'
                import json
                with open(janet_output, 'w') as f:
                    json.dump(janet_result.to_jv_matcher_format(), f, indent=2)
                self.stdout.write(f'Saved Janet profile to: {janet_output}')
            else:
                self.stdout.write(self.style.WARNING(
                    f'Janet enrichment failed: {janet_result.error}'
                ))

        # Step 2: Enrich top matches
        if not skip_matches:
            if not os.path.exists(input_file):
                self.stdout.write(self.style.ERROR(f'Input file not found: {input_file}'))
                self.stdout.write('Run `python manage.py match_janet` first to generate matches.')
                return

            self.stdout.write(f'\n--- Enriching Top {top_n} Matches ---')
            self.stdout.write(f'Input: {input_file}')

            # Read matches
            matches = []
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    matches.append(row)

            self.stdout.write(f'Total matches in file: {len(matches)}')
            matches_to_enrich = matches[:top_n]
            self.stdout.write(f'Enriching top {len(matches_to_enrich)} matches...\n')

            # Enrich each match
            enriched_matches = []
            for i, match in enumerate(matches_to_enrich, 1):
                name = match.get('name', '')
                company = match.get('company', '')
                website = match.get('website', '')
                linkedin = match.get('linkedin', '')

                self.stdout.write(f'[{i}/{len(matches_to_enrich)}] Enriching: {name}')

                try:
                    result = await service.enrich_profile(
                        name=name,
                        company=company,
                        website=f'https://{website}' if website and not website.startswith('http') else website,
                        linkedin=linkedin,
                        existing_data=match,
                    )

                    if result.enriched:
                        verified_count = result.enriched.get_verified_field_count()
                        confidence = result.enriched.overall_confidence
                        self.stdout.write(self.style.SUCCESS(
                            f'  ✓ Enriched: {verified_count}/12 fields, {confidence:.0%} confidence'
                        ))

                        # Merge enriched data with original match
                        enriched_data = result.to_jv_matcher_format()
                        enriched_match = {**match, **enriched_data}

                        # Add enrichment metadata
                        enriched_match['_enriched'] = True
                        enriched_match['_verified_fields'] = verified_count
                        enriched_match['_confidence'] = f'{confidence:.0%}'

                        # Highlight key findings
                        if enriched_data.get('email'):
                            self.stdout.write(f'    📧 Email: {enriched_data["email"]}')
                        if enriched_data.get('booking_link'):
                            self.stdout.write(f'    📅 Booking: {enriched_data["booking_link"]}')
                        if enriched_data.get('signature_programs'):
                            self.stdout.write(f'    🎯 Programs: {enriched_data["signature_programs"][:100]}...')

                        enriched_matches.append(enriched_match)
                    else:
                        self.stdout.write(self.style.WARNING(
                            f'  ✗ Failed: {result.error}'
                        ))
                        match['_enriched'] = False
                        match['_error'] = result.error
                        enriched_matches.append(match)

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'  ✗ Error: {e}'))
                    match['_enriched'] = False
                    match['_error'] = str(e)
                    enriched_matches.append(match)

            # Write enriched output
            if enriched_matches:
                # Determine all fieldnames
                all_fields = set()
                for m in enriched_matches:
                    all_fields.update(m.keys())

                # Order fields sensibly
                priority_fields = [
                    'rank', 'match_score', 'name', 'company',
                    'email', 'website', 'linkedin', 'booking_link', 'phone',
                    'niche', 'who_they_serve', 'offering', 'signature_programs',
                    'list_size', 'match_reason',
                    '_enriched', '_verified_fields', '_confidence',
                ]
                fieldnames = [f for f in priority_fields if f in all_fields]
                fieldnames.extend(sorted(f for f in all_fields if f not in fieldnames))

                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(enriched_matches)

                self.stdout.write(self.style.SUCCESS(f'\n✓ Wrote {len(enriched_matches)} enriched matches to: {output_file}'))

        # Summary
        stats = service.get_stats()
        self.stdout.write(f'\n--- Summary ---')
        self.stdout.write(f'Profiles processed: {stats["profiles_processed"]}')
        self.stdout.write(f'OWL stats: {stats["owl_stats"]}')
