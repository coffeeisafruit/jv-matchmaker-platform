"""
Scan all profiles for data quality issues and optionally fix them.

Uses TextSanitizer validation to detect template bios, generic companies,
malformed list fields, and other AI-enrichment artifacts. Reports findings
and can apply fixes in --fix mode.

Usage:
    python manage.py audit_profiles                    # Report only
    python manage.py audit_profiles --fix              # Report + fix issues
    python manage.py audit_profiles --fix --dry-run    # Show what would be fixed
    python manage.py audit_profiles --profile-id UUID  # Single profile
"""

from django.core.management.base import BaseCommand
from django.utils import timezone

from matching.models import SupabaseProfile
from matching.enrichment.text_sanitizer import TextSanitizer


class Command(BaseCommand):
    help = 'Audit all profiles for data quality issues (template bios, generic companies, etc.)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--fix', action='store_true',
            help='Apply TextSanitizer fixes to flagged records',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Show what --fix would change without writing to DB',
        )
        parser.add_argument(
            '--profile-id', type=str, default='',
            help='Audit a single profile by UUID',
        )
        parser.add_argument(
            '--batch-size', type=int, default=500,
            help='Batch size for DB updates (default: 500)',
        )

    def handle(self, **options):
        fix_mode = options['fix']
        dry_run = options['dry_run']
        profile_id = options.get('profile_id', '').strip()
        batch_size = options['batch_size']

        if profile_id:
            profiles = SupabaseProfile.objects.filter(id=profile_id)
        else:
            profiles = SupabaseProfile.objects.all()

        total = profiles.count()
        self.stdout.write(f'Auditing {total} profiles...\n')

        issues = {
            'template_bio': [],
            'generic_company': [],
            'offering_syntax': [],
            'seeking_syntax': [],
            'dirty_match_reason': [],
        }
        to_update = []

        for profile in profiles.iterator(chunk_size=batch_size):
            name = profile.name or 'Unknown'
            changed = False

            # --- Bio validation ---
            bio = profile.bio or ''
            if bio:
                cleaned_bio = TextSanitizer.validate_bio(bio, name)
                if cleaned_bio != bio:
                    issues['template_bio'].append({
                        'id': str(profile.id),
                        'name': name,
                        'field': 'bio',
                        'before': bio[:100],
                        'after': cleaned_bio[:100] if cleaned_bio else '(cleared)',
                    })
                    if fix_mode:
                        profile.bio = cleaned_bio
                        changed = True

            # --- Company validation ---
            company = profile.company or ''
            if company:
                cleaned_company = TextSanitizer.validate_company(company, name)
                if cleaned_company != company:
                    issues['generic_company'].append({
                        'id': str(profile.id),
                        'name': name,
                        'field': 'company',
                        'before': company,
                        'after': cleaned_company if cleaned_company else '(cleared)',
                    })
                    if fix_mode:
                        profile.company = cleaned_company
                        changed = True

            # --- Offering list syntax ---
            offering = profile.offering or ''
            if offering:
                cleaned_offering = TextSanitizer.clean_list_field(offering)
                if cleaned_offering != offering:
                    issues['offering_syntax'].append({
                        'id': str(profile.id),
                        'name': name,
                        'field': 'offering',
                        'before': offering[:80],
                        'after': cleaned_offering[:80],
                    })
                    if fix_mode:
                        profile.offering = cleaned_offering
                        changed = True

            # --- Seeking list syntax ---
            seeking = profile.seeking or ''
            if seeking:
                cleaned_seeking = TextSanitizer.clean_list_field(seeking)
                if cleaned_seeking != seeking:
                    issues['seeking_syntax'].append({
                        'id': str(profile.id),
                        'name': name,
                        'field': 'seeking',
                        'before': seeking[:80],
                        'after': cleaned_seeking[:80],
                    })
                    if fix_mode:
                        profile.seeking = cleaned_seeking
                        changed = True

            if changed:
                to_update.append(profile)

        # --- Report ---
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('PROFILE QUALITY AUDIT REPORT')
        self.stdout.write('=' * 60 + '\n')

        total_issues = 0
        for category, items in issues.items():
            if items:
                label = category.replace('_', ' ').title()
                self.stdout.write(f'\n--- {label} ({len(items)} issues) ---')
                for item in items[:20]:  # Show first 20
                    self.stdout.write(
                        f"  {item['name']} ({item['id'][:8]}...): "
                        f"{item['before'][:60]}"
                    )
                    if fix_mode:
                        self.stdout.write(f"    → {item['after'][:60]}")
                if len(items) > 20:
                    self.stdout.write(f'  ... and {len(items) - 20} more')
                total_issues += len(items)

        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(f'Total issues found: {total_issues}')
        self.stdout.write(f'Profiles scanned: {total}')

        # --- Apply fixes ---
        if fix_mode and to_update:
            if dry_run:
                self.stdout.write(
                    f'\nDRY RUN: Would update {len(to_update)} profiles'
                )
            else:
                fields_to_update = ['bio', 'company', 'offering', 'seeking']
                for i in range(0, len(to_update), batch_size):
                    batch = to_update[i:i + batch_size]
                    SupabaseProfile.objects.bulk_update(batch, fields_to_update)
                self.stdout.write(
                    f'\nFixed {len(to_update)} profiles in DB'
                )
        elif fix_mode:
            self.stdout.write('\nNo fixes needed — all profiles pass validation.')
        else:
            if total_issues:
                self.stdout.write(
                    f'\nRun with --fix to apply TextSanitizer corrections'
                )
            else:
                self.stdout.write('\nAll profiles pass validation.')
