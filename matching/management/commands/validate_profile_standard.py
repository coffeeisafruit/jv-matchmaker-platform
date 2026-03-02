"""
Validate MemberReport profiles against the production standard.

Runs deterministic checks on client_profile, outreach_templates, and
partner cards.  Optionally calls ProfileGapFiller to auto-generate
missing sections, then re-validates and updates production_status.
"""

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from matching.models import MemberReport, SupabaseProfile
from matching.enrichment.profile_standard import (
    validate_profile,
    FieldRequirement,
)


class Command(BaseCommand):
    help = 'Validate MemberReport profiles against the production standard'

    def add_arguments(self, parser):
        parser.add_argument('--client-name', type=str, help='Validate report for a specific client')
        parser.add_argument('--all', action='store_true', help='Validate all active reports')
        parser.add_argument('--fix', action='store_true', help='Auto-fill gaps with AI before validation')
        parser.add_argument('--dry-run', action='store_true', help='Show what would change without saving')

    def handle(self, *args, **options):
        reports = self._get_reports(options)

        if not reports:
            raise CommandError('No reports found. Use --client-name or --all.')

        self.stdout.write(f'\nValidating {len(reports)} report(s)...\n')

        gap_filler = None
        if options['fix']:
            from matching.enrichment.profile_gap_filler import ProfileGapFiller
            gap_filler = ProfileGapFiller()
            if not gap_filler.is_available():
                self.stdout.write(self.style.WARNING(
                    'WARNING: No API key configured. --fix will have limited effect.\n'
                ))

        passed_count = 0
        failed_count = 0

        for report in reports:
            result = self._validate_report(report, gap_filler, options)
            if result:
                passed_count += 1
            else:
                failed_count += 1

        # Summary
        self.stdout.write(f'\n{"="*60}')
        self.stdout.write(f'SUMMARY: {passed_count} passed, {failed_count} failed out of {len(reports)}')
        self.stdout.write(f'{"="*60}\n')

    def _get_reports(self, options):
        if options.get('client_name'):
            reports = list(MemberReport.objects.filter(
                member_name__icontains=options['client_name'],
                is_active=True,
            ).order_by('-created_at')[:1])
            if not reports:
                # Try company name
                reports = list(MemberReport.objects.filter(
                    company_name__icontains=options['client_name'],
                    is_active=True,
                ).order_by('-created_at')[:1])
            return reports
        elif options.get('all'):
            return list(MemberReport.objects.filter(is_active=True).order_by('-created_at'))
        return []

    def _validate_report(self, report, gap_filler, options):
        """Validate a single report. Returns True if passed."""
        partner_cards = list(
            report.partners.order_by('rank').values(
                'name', 'company', 'tagline', 'audience', 'why_fit',
                'detail_note', 'email', 'phone', 'linkedin', 'website',
                'schedule', 'list_size', 'tags', 'match_score',
            )
        )
        partner_count = len(partner_cards)

        # Initial validation
        result = validate_profile(
            report.client_profile or {},
            report.outreach_templates or {},
            partner_count,
            partner_cards,
        )

        self.stdout.write(f'\nPROFILE VALIDATION: {report.member_name} ({report.company_name})')
        self.stdout.write('=' * 60)
        self.stdout.write(f'Score: {result.score:.0f}/100  {"PRODUCTION" if result.passed else "DRAFT -- not production ready"}')

        # Group issues by requirement
        required_issues = [i for i in result.issues if i.requirement == FieldRequirement.REQUIRED]
        recommended_issues = [i for i in result.issues if i.requirement == FieldRequirement.RECOMMENDED]

        # Show required issues
        if required_issues:
            self.stdout.write(f'\nREQUIRED (blocking):')
            for issue in required_issues:
                self.stdout.write(f'  [FAIL] {issue.field} -- {issue.message}')

        # Show partner card summary
        partner_req = [i for i in required_issues if i.field.startswith('partner_card[')]
        partner_rec = [i for i in recommended_issues if i.field.startswith('partner_card[')]

        if partner_count > 0:
            self.stdout.write(f'\nPARTNER CARDS ({partner_count} total):')
            cards_with_issues = set()
            for i in partner_req + partner_rec:
                # Extract partner name from field like "partner_card[Name].field"
                name = i.field.split('[')[1].split(']')[0] if '[' in i.field else ''
                cards_with_issues.add(name)
            good_cards = partner_count - len(cards_with_issues)
            self.stdout.write(f'  {good_cards}/{partner_count} cards fully compliant')
            if partner_req:
                self.stdout.write(f'  {len(partner_req)} required issues across cards')

        # Show recommended issues
        non_partner_rec = [i for i in recommended_issues if not i.field.startswith('partner_card[')]
        if non_partner_rec:
            self.stdout.write(f'\nRECOMMENDED:')
            for issue in non_partner_rec:
                self.stdout.write(f'  [WARN] {issue.field} -- {issue.message}')

        # Missing fields
        if result.missing_fields:
            self.stdout.write(f'\nMISSING FIELDS: {", ".join(result.missing_fields)}')

        # --fix mode: attempt to fill gaps
        if gap_filler and not result.passed and result.missing_fields:
            self.stdout.write(f'\n  -> Running AI gap filler for {len(result.missing_fields)} missing fields...')

            # Build profile_data dict from SupabaseProfile
            sp = report.supabase_profile
            if sp:
                profile_data = {
                    'name': sp.name or '',
                    'company': sp.company or '',
                    'bio': sp.bio or '',
                    'what_you_do': sp.what_you_do or '',
                    'who_you_serve': sp.who_you_serve or '',
                    'offering': sp.offering or '',
                    'seeking': sp.seeking or '',
                    'niche': sp.niche or '',
                    'signature_programs': sp.signature_programs or '',
                    'audience_type': sp.audience_type or '',
                    'list_size': sp.list_size,
                    'social_reach': sp.social_reach,
                    'booking_link': sp.booking_link or '',
                    'email': sp.email or '',
                    'website': sp.website or '',
                }

                filled = gap_filler.fill_gaps(
                    profile_data=profile_data,
                    current_profile=report.client_profile or {},
                    missing_fields=result.missing_fields,
                )

                if filled:
                    if not options.get('dry_run'):
                        # Merge filled data into client_profile
                        updated_profile = dict(report.client_profile or {})
                        updated_profile.update(filled)

                        # Handle outreach templates separately
                        if 'outreach_templates' in filled:
                            report.outreach_templates = filled.pop('outreach_templates')

                        report.client_profile = updated_profile
                        report.save()

                        self.stdout.write(f'  -> Filled {len(filled)} fields: {", ".join(filled.keys())}')

                        # Re-validate after fix
                        result = validate_profile(
                            report.client_profile,
                            report.outreach_templates or {},
                            partner_count,
                            partner_cards,
                        )
                        self.stdout.write(f'  -> Re-validated: {result.score:.0f}/100 {"PASSED" if result.passed else "still failing"}')
                    else:
                        self.stdout.write(f'  -> [DRY RUN] Would fill: {", ".join(filled.keys())}')
            else:
                self.stdout.write(f'  -> No linked SupabaseProfile, cannot run gap filler')

        # Update production status
        if not options.get('dry_run'):
            report.production_score = result.score
            report.production_issues = [
                {'field': i.field, 'requirement': i.requirement.value, 'message': i.message}
                for i in result.issues
            ]
            report.production_validated_at = timezone.now()
            report.production_status = 'production' if result.passed else 'draft'
            report.save()

        if not result.passed and not gap_filler:
            self.stdout.write(f'\n  Run with --fix to auto-generate missing sections.')

        return result.passed
