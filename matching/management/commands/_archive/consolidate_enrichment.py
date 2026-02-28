"""
Consolidate enrichment data with confidence tracking.
Extends existing SmartEnrichmentService patterns.

Usage:
    python manage.py consolidate_enrichment --source owl --dry-run
    python manage.py consolidate_enrichment --source owl --limit 100
    python manage.py consolidate_enrichment --source owl  # All profiles
"""

import os
import csv
import json
from datetime import datetime
from typing import Dict, List, Optional
from django.core.management.base import BaseCommand
from django.db import connection
from matching.enrichment.confidence import ConfidenceScorer
from matching.enrichment.consolidation import ProfileMerger


class Command(BaseCommand):
    help = 'Consolidate enrichment data with confidence scoring'

    def add_arguments(self, parser):
        parser.add_argument(
            '--source',
            type=str,
            default='owl',
            choices=['owl', 'apollo', 'all'],
            help='Enrichment source to consolidate'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview changes without applying them'
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of profiles to process'
        )
        parser.add_argument(
            '--output-sql',
            type=str,
            help='Output SQL file path (instead of executing)'
        )

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        self.output_sql = options['output_sql']
        self.scorer = ConfidenceScorer()
        self.merger = ProfileMerger()

        self.stdout.write("=" * 70)
        self.stdout.write("ENRICHMENT CONSOLIDATION")
        self.stdout.write("=" * 70)
        self.stdout.write(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        self.stdout.write(f"Source: {options['source']}")
        if options['limit']:
            self.stdout.write(f"Limit: {options['limit']} profiles")
        self.stdout.write("")

        # Load enrichment data based on source
        if options['source'] == 'owl':
            self.consolidate_owl(options.get('limit'))
        elif options['source'] == 'apollo':
            self.consolidate_apollo(options.get('limit'))
        elif options['source'] == 'all':
            self.consolidate_owl(options.get('limit'))
            self.consolidate_apollo(options.get('limit'))

    def consolidate_owl(self, limit: Optional[int] = None):
        """Consolidate OWL enrichment data"""
        self.stdout.write("")
        self.stdout.write("=" * 70)
        self.stdout.write("CONSOLIDATING OWL ENRICHMENT DATA")
        self.stdout.write("=" * 70)
        self.stdout.write("")

        # Load OWL enriched profiles
        owl_file = 'owl_enrichment_output/owl_enriched_profiles.csv'
        if not os.path.exists(owl_file):
            self.stdout.write(self.style.ERROR(f"❌ OWL file not found: {owl_file}"))
            return

        owl_profiles = self._load_owl_profiles(owl_file, limit)
        self.stdout.write(f"Loaded {len(owl_profiles)} OWL profiles")
        self.stdout.write("")

        # Get existing profiles from Supabase
        existing_profiles = self._fetch_existing_profiles([p['id'] for p in owl_profiles])
        self.stdout.write(f"Found {len(existing_profiles)} existing profiles in Supabase")
        self.stdout.write("")

        # Consolidate each profile
        stats = {
            'processed': 0,
            'updated': 0,
            'new_fields_added': 0,
            'conflicts_resolved': 0,
            'skipped': 0
        }

        sql_statements = []

        for owl_profile in owl_profiles:
            profile_id = owl_profile['id']
            existing = existing_profiles.get(profile_id)

            if not existing:
                stats['skipped'] += 1
                self.stdout.write(f"⚠️  Profile not found in Supabase: {owl_profile['name']} ({profile_id})")
                continue

            # Merge profile data with confidence tracking
            merged_data, merged_metadata = self._merge_profile_with_owl(
                existing, owl_profile
            )

            # Calculate overall profile confidence
            profile_confidence = self.scorer.calculate_profile_confidence(merged_metadata)

            # Generate SQL update
            if self._has_changes(existing, merged_data, merged_metadata):
                sql = self._generate_update_sql(
                    profile_id,
                    merged_data,
                    merged_metadata,
                    profile_confidence
                )
                sql_statements.append(sql)

                stats['updated'] += 1
                stats['new_fields_added'] += self._count_new_fields(existing, merged_data)

                if self._has_conflicts(existing, owl_profile):
                    stats['conflicts_resolved'] += 1

                # Show progress every 100 profiles
                if stats['updated'] % 100 == 0:
                    self.stdout.write(f"   Processed {stats['updated']} profiles...")

            stats['processed'] += 1

        # Output or execute SQL
        if sql_statements:
            if self.output_sql:
                self._write_sql_file(sql_statements, self.output_sql)
            elif not self.dry_run:
                self._execute_sql_batch(sql_statements)

        # Print summary
        self.stdout.write("")
        self.stdout.write("=" * 70)
        self.stdout.write("CONSOLIDATION SUMMARY")
        self.stdout.write("=" * 70)
        self.stdout.write(f"Processed: {stats['processed']}")
        self.stdout.write(f"Updated: {stats['updated']}")
        self.stdout.write(f"New fields added: {stats['new_fields_added']}")
        self.stdout.write(f"Conflicts resolved: {stats['conflicts_resolved']}")
        self.stdout.write(f"Skipped: {stats['skipped']}")
        self.stdout.write("")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("✅ Dry run complete! No changes made."))
        elif self.output_sql:
            self.stdout.write(self.style.SUCCESS(f"✅ SQL written to: {self.output_sql}"))
        else:
            self.stdout.write(self.style.SUCCESS("✅ Consolidation complete!"))

    def _load_owl_profiles(self, file_path: str, limit: Optional[int] = None) -> List[Dict]:
        """Load OWL enriched profiles from CSV"""
        profiles = []

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                profiles.append(row)

        return profiles

    def _fetch_existing_profiles(self, profile_ids: List[str]) -> Dict[str, Dict]:
        """Fetch existing profiles from Supabase"""
        with connection.cursor() as cursor:
            # Build query with parameter placeholders
            placeholders = ','.join(['%s'] * len(profile_ids))
            cursor.execute(f"""
                SELECT id, name, email, company, website, linkedin, phone,
                       niche, list_size, seeking, who_you_serve, what_you_do,
                       offering, enrichment_metadata, profile_confidence,
                       last_enriched_at
                FROM profiles
                WHERE id IN ({placeholders})
            """, profile_ids)

            columns = [col[0] for col in cursor.description]
            profiles = {}

            for row in cursor.fetchall():
                profile = dict(zip(columns, row))

                # Parse JSONB enrichment_metadata if it's a string
                if isinstance(profile.get('enrichment_metadata'), str):
                    try:
                        profile['enrichment_metadata'] = json.loads(profile['enrichment_metadata'])
                    except (json.JSONDecodeError, TypeError):
                        profile['enrichment_metadata'] = {}

                profiles[str(profile['id'])] = profile

            return profiles

    def _merge_profile_with_owl(
        self,
        existing: Dict,
        owl_profile: Dict
    ) -> tuple[Dict, Dict]:
        """
        Merge existing profile with OWL data using confidence scoring.

        Returns:
            Tuple of (merged_data, merged_metadata)
        """
        merged_data = {}
        merged_metadata = existing.get('enrichment_metadata') or {}

        # OWL timestamp (use current time if not available)
        owl_timestamp = datetime.now()

        # Fields to merge from OWL
        owl_fields = ['seeking', 'who_you_serve', 'what_you_do', 'offering', 'niche']

        for field in owl_fields:
            owl_value = owl_profile.get(field)
            if not owl_value or not owl_value.strip():
                continue  # Skip empty values

            # Create OWL metadata
            owl_metadata = {
                'source': 'owl',
                'enriched_at': owl_timestamp.isoformat(),
                'source_date': owl_timestamp.date().isoformat(),
                'confidence': self.scorer.calculate_confidence(
                    field_name=field,
                    source='owl',
                    enriched_at=owl_timestamp
                ),
                'confidence_expires_at': self.scorer.calculate_expires_at(
                    field, owl_timestamp
                ).isoformat(),
                'verification_count': 0,
                'cross_validated': False
            }

            # Get existing metadata for this field
            existing_field_metadata = merged_metadata.get(field, {})
            existing_value = existing.get(field)

            # Merge using ProfileMerger
            merged_value, field_metadata = self.merger.merge_field(
                field,
                existing_value,
                existing_field_metadata,
                owl_value,
                owl_metadata
            )

            # Always update metadata for tracking (even if value doesn't change)
            merged_metadata[field] = field_metadata

            # Update field value if changed
            if merged_value != existing_value:
                merged_data[field] = merged_value

        return merged_data, merged_metadata

    def _has_changes(self, existing: Dict, merged_data: Dict, merged_metadata: Dict) -> bool:
        """Check if there are any changes to apply"""
        # Always return True if we have any metadata to add
        # (We want to track sources even if values don't change)
        if merged_metadata:
            return True

        # Check if any field values changed
        if merged_data:
            return True

        return False

    def _has_conflicts(self, existing: Dict, owl_profile: Dict) -> bool:
        """Check if there were conflicts between existing and OWL data"""
        for field in ['seeking', 'who_you_serve', 'what_you_do', 'offering']:
            existing_value = existing.get(field)
            owl_value = owl_profile.get(field)

            if existing_value and owl_value and existing_value != owl_value:
                return True

        return False

    def _count_new_fields(self, existing: Dict, merged_data: Dict) -> int:
        """Count how many previously empty fields were filled"""
        count = 0
        for field, value in merged_data.items():
            if not existing.get(field) and value:
                count += 1
        return count

    def _generate_update_sql(
        self,
        profile_id: str,
        merged_data: Dict,
        merged_metadata: Dict,
        profile_confidence: float
    ) -> str:
        """Generate SQL UPDATE statement for profile"""
        set_clauses = []
        values = []

        # Add field updates
        for field, value in merged_data.items():
            set_clauses.append(f"{field} = %s")
            values.append(value)

        # Add metadata update
        set_clauses.append("enrichment_metadata = %s")
        values.append(json.dumps(merged_metadata))

        # Add profile confidence
        set_clauses.append("profile_confidence = %s")
        values.append(profile_confidence)

        # Add last_enriched_at
        set_clauses.append("last_enriched_at = %s")
        values.append(datetime.now())

        # Add updated_at
        set_clauses.append("updated_at = %s")
        values.append(datetime.now())

        # Build SQL
        sql = f"""
UPDATE profiles
SET {', '.join(set_clauses)}
WHERE id = %s;
"""
        values.append(profile_id)

        # Use psycopg2's mogrify-like behavior to create executable SQL
        # For now, return parameterized SQL
        return (sql, values)

    def _write_sql_file(self, sql_statements: List[tuple], output_path: str):
        """Write SQL statements to file"""
        with open(output_path, 'w') as f:
            f.write("-- OWL Enrichment Consolidation SQL\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write(f"-- Statements: {len(sql_statements)}\n")
            f.write("\n")
            f.write("BEGIN;\n\n")

            for i, (sql, values) in enumerate(sql_statements):
                # Convert to executable SQL with escaped values
                f.write(f"-- Statement {i+1}\n")
                f.write(sql % tuple(self._escape_sql_value(v) for v in values))
                f.write("\n")

            f.write("\nCOMMIT;\n")

        self.stdout.write(f"Wrote {len(sql_statements)} SQL statements to: {output_path}")

    def _execute_sql_batch(self, sql_statements: List[tuple]):
        """Execute SQL statements in batch"""
        self.stdout.write(f"Executing {len(sql_statements)} SQL updates...")

        with connection.cursor() as cursor:
            for sql, values in sql_statements:
                cursor.execute(sql, values)

        self.stdout.write(self.style.SUCCESS(f"✅ Executed {len(sql_statements)} updates"))

    def _escape_sql_value(self, value):
        """Escape SQL value for string interpolation"""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            # Escape single quotes
            return "'" + value.replace("'", "''") + "'"
        elif isinstance(value, datetime):
            return "'" + value.isoformat() + "'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # JSON or other types
            return "'" + str(value).replace("'", "''") + "'"

    def consolidate_apollo(self, limit: Optional[int] = None):
        """Consolidate Apollo enrichment data"""
        self.stdout.write(self.style.WARNING("Apollo consolidation not yet implemented"))
        # TODO: Implement Apollo consolidation similar to OWL
