"""
Generate a monthly member report with access code.

Creates a MemberReport + ReportPartner records from the Supabase database,
reusing the scoring pipeline from match_janet.py and client resolution from
generate_match_pdf.py.

Usage:
    python manage.py generate_member_report --client-name "Penelope Jane Smith" --month 2026-02
    python manage.py generate_member_report --client-profile-id UUID --month 2026-02 --top 10
    python manage.py generate_member_report --client-name "Janet Bray Attwood" --month 2026-02 --enrich
"""

import re
import secrets
from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from django.utils import timezone

from matching.models import MemberReport, ReportPartner, SupabaseProfile
from matching.services import SupabaseMatchScoringService


def _looks_like_person_name(name: str) -> bool:
    """Filter out entries that are categories, not people."""
    if not name:
        return False
    # Category-style names: "Business Skills, Fitness, Life"
    if name.count(',') >= 2:
        return False
    # Names starting with generic categories
    category_prefixes = [
        'business skills', 'more info', 'health', 'fitness',
        'lifestyle', 'mental health', 'self improvement',
    ]
    lower = name.lower().strip()
    for prefix in category_prefixes:
        if lower.startswith(prefix):
            return False
    # Real names have at least 2 words, each starting with uppercase
    words = name.strip().split()
    if len(words) < 2:
        return False
    return True


def _clean_url_field(value: str) -> str:
    """Strip trailing commas and whitespace from URLs."""
    if not value:
        return ''
    return value.strip().rstrip(',')


def _extract_linkedin(sp) -> str:
    """Get LinkedIn URL from the profile, checking both linkedin and website fields."""
    if sp.linkedin and 'linkedin.com' in sp.linkedin.lower():
        return _clean_url_field(sp.linkedin)
    # Many Supabase profiles store LinkedIn URL in the website field
    if sp.website and 'linkedin.com' in sp.website.lower():
        return _clean_url_field(sp.website)
    return ''


def _extract_website(sp) -> str:
    """Get the actual website URL, excluding LinkedIn/Facebook/Calendly links."""
    if not sp.website:
        return ''
    url = _clean_url_field(sp.website)
    social_domains = ['linkedin.com', 'facebook.com', 'calendly.com', 'cal.com', 'tinyurl.com']
    for domain in social_domains:
        if domain in url.lower():
            return ''  # Not a real website
    return url


def _extract_schedule(sp) -> str:
    """Get scheduling link from website or booking_link."""
    if sp.booking_link:
        return _clean_url_field(sp.booking_link)
    if sp.website and ('calendly.com' in sp.website.lower() or 'cal.com' in sp.website.lower()):
        return _clean_url_field(sp.website)
    return ''


class Command(BaseCommand):
    help = 'Generate a monthly member report with unique access code'

    def add_arguments(self, parser):
        parser.add_argument(
            '--client-name', type=str, default=None,
            help='Client name to look up in Supabase',
        )
        parser.add_argument(
            '--client-profile-id', type=str, default=None,
            help='Client Supabase profile UUID',
        )
        parser.add_argument(
            '--month', type=str, default=None,
            help='Report month (YYYY-MM). Defaults to current month.',
        )
        parser.add_argument(
            '--top', type=int, default=10,
            help='Number of top matches to include (default: 10)',
        )
        parser.add_argument(
            '--company', type=str, default=None,
            help='Override company name',
        )
        parser.add_argument(
            '--all', action='store_true',
            help='Generate reports for ALL active members',
        )
        parser.add_argument(
            '--enrich', action='store_true',
            help='Use MatchEnrichmentService for AI-generated why_fit text',
        )
        parser.add_argument(
            '--expires-days', type=int, default=45,
            help='Days until access code expires (default: 45)',
        )
        parser.add_argument(
            '--launch-date', type=str, default=None,
            help='Launch date for countdown timer (YYYY-MM-DD)',
        )

    def handle(self, *args, **options):
        # --all mode: generate reports for every active member
        if options.get('all'):
            month_date = self._parse_month(options.get('month'))
            members = SupabaseProfile.objects.filter(status='Member')
            self.stdout.write(self.style.SUCCESS(
                f'\nBATCH MODE: Generating reports for {members.count()} members'
            ))
            for member_sp in members:
                if not _looks_like_person_name(member_sp.name):
                    continue
                try:
                    self._generate_report_for(member_sp, options, month_date)
                except Exception as e:
                    self.stdout.write(self.style.ERROR(
                        f'  FAILED: {member_sp.name} — {e}'
                    ))
            return

        # Single-member mode
        client_sp = self._resolve_client(options)
        month_date = self._parse_month(options.get('month'))
        self._generate_report_for(client_sp, options, month_date)

    def _generate_report_for(self, client_sp, options, month_date):
        """Generate a single member report."""
        client_dict = self._supabase_to_client_dict(client_sp)

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS(f'GENERATING MEMBER REPORT: {client_sp.name}'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}\n'))

        self.stdout.write(f'Report month: {month_date.strftime("%B %Y")}')

        top_n = options['top']

        # Score partners using ISMC scorer
        top_matches = self._score_with_ismc(client_sp, top_n)

        # Optional AI enrichment
        if options['enrich']:
            self._enrich_matches(top_matches, client_dict)

        # Generate access code
        access_code = secrets.token_hex(4).upper()

        # Build client_profile JSON (matches DEMO_PROFILE structure)
        client_profile_json = self._build_client_profile(client_sp)

        # Build outreach templates
        outreach_templates = self._build_outreach_templates(client_sp)

        # Determine company name (--company override or clean up Supabase data)
        company_name = options.get('company') or self._clean_company_name(client_sp)

        # Create MemberReport
        expires_at = timezone.now() + timedelta(days=options['expires_days'])
        launch_date = None
        if options.get('launch_date'):
            launch_date = timezone.make_aware(
                datetime.strptime(options['launch_date'], '%Y-%m-%d')
            )

        report = MemberReport.objects.create(
            member_name=client_sp.name,
            member_email=client_sp.email or '',
            company_name=company_name,
            access_code=access_code,
            month=month_date,
            expires_at=expires_at,
            is_active=True,
            client_profile=client_profile_json,
            supabase_profile=client_sp,
            outreach_templates=outreach_templates,
            launch_date=launch_date,
            footer_text=f'Report generated for {client_sp.name}.',
        )

        # Create ReportPartner records
        self.stdout.write('\nCreating partner records...')
        section_counts = {'priority': 0, 'this_week': 0, 'low_priority': 0, 'jv_programs': 0}

        for rank, match in enumerate(top_matches, 1):
            partner_sp = match['partner']
            section, section_label, section_note = self._assign_section(match, rank, top_n)
            section_counts[section] += 1

            # Clean field data — resolve LinkedIn vs website confusion
            linkedin = _extract_linkedin(partner_sp)
            website = _extract_website(partner_sp)
            schedule = _extract_schedule(partner_sp)

            ReportPartner.objects.create(
                report=report,
                rank=rank,
                section=section,
                section_label=section_label,
                section_note=section_note,
                name=partner_sp.name,
                company=self._clean_company_name(partner_sp),
                tagline=self._build_tagline(partner_sp),
                email=partner_sp.email or '',
                website=website,
                phone=partner_sp.phone or '',
                linkedin=linkedin,
                apply_url=partner_sp.booking_link or '',
                schedule=schedule,
                badge=self._assign_badge(match, partner_sp),
                badge_style=self._assign_badge_style(match),
                list_size=self._format_list_size(partner_sp.list_size),
                audience=self._build_audience_desc(partner_sp, client_dict),
                why_fit=match.get('why_fit') or match.get('reason', ''),
                detail_note=match.get('detail_note') or self._build_detail_note(partner_sp, client_dict),
                tags=self._build_tags(match, partner_sp),
                match_score=match['score'],
                source_profile=partner_sp,
            )
            self.stdout.write(
                f'  {rank}. {partner_sp.name} [{section}] '
                f'(score: {match["score"]:.1f}, '
                f'email: {"yes" if partner_sp.email else "no"}, '
                f'linkedin: {"yes" if linkedin else "no"})'
            )

        # Summary
        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('REPORT GENERATED SUCCESSFULLY'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}'))
        self.stdout.write(f'\n  Member:      {report.member_name}')
        self.stdout.write(f'  Company:     {report.company_name}')
        self.stdout.write(f'  Month:       {month_date.strftime("%B %Y")}')
        self.stdout.write(f'  Partners:    {len(top_matches)}')
        self.stdout.write(f'  Sections:    priority={section_counts["priority"]}, '
                          f'this_week={section_counts["this_week"]}, '
                          f'low_priority={section_counts["low_priority"]}, '
                          f'jv_programs={section_counts["jv_programs"]}')
        self.stdout.write(f'  Expires:     {expires_at.strftime("%B %d, %Y")}')
        self.stdout.write(self.style.SUCCESS(f'\n  ACCESS CODE: {access_code}'))
        self.stdout.write(f'  Report URL:  /matching/report/{report.id}/\n')

    # =========================================================================
    # CLIENT RESOLUTION
    # =========================================================================

    def _resolve_client(self, options) -> SupabaseProfile:
        profile_id = options.get('client_profile_id')
        client_name = options.get('client_name')

        if not profile_id and not client_name:
            raise CommandError('Provide --client-name or --client-profile-id')

        if profile_id:
            profile = SupabaseProfile.objects.filter(id=profile_id).first()
            if not profile:
                raise CommandError(f'No profile found with ID: {profile_id}')
            return profile

        profile = SupabaseProfile.objects.filter(name__iexact=client_name).first()
        if not profile:
            profile = SupabaseProfile.objects.filter(name__icontains=client_name).first()
        if not profile:
            raise CommandError(f'No profile found matching name: {client_name}')
        return profile

    @staticmethod
    def _supabase_to_client_dict(sp: SupabaseProfile) -> dict:
        return {
            'name': sp.name,
            'company': sp.company or '',
            'what_you_do': sp.what_you_do or '',
            'who_you_serve': sp.who_you_serve or '',
            'seeking': sp.seeking or '',
            'offering': sp.offering or '',
            'bio': sp.bio or '',
            'niche': sp.niche or '',
            'list_size': sp.list_size or 0,
            'signature_programs': sp.signature_programs or '',
        }

    # =========================================================================
    # ISMC SCORING
    # =========================================================================

    def _score_with_ismc(self, client_sp: SupabaseProfile, top_n: int) -> list:
        """Score all candidates against client using the ISMC harmonic scorer."""
        scorer = SupabaseMatchScoringService()

        self.stdout.write('\nLoading partners from Supabase...')
        partners = list(
            SupabaseProfile.objects.filter(status='Member')
            .exclude(id=client_sp.id)
        )

        # Filter non-person names
        filtered = []
        skipped_names = 0
        for p in partners:
            if not _looks_like_person_name(p.name):
                skipped_names += 1
                continue
            filtered.append(p)

        self.stdout.write(f'  Total in database: {len(partners)}')
        self.stdout.write(f'  Skipped (non-person names): {skipped_names}')
        self.stdout.write(f'  Candidates to score: {len(filtered)}')

        self.stdout.write('Scoring partners with ISMC...')
        scored = []
        for p in filtered:
            result = scorer.score_pair(client_sp, p)
            score_ab = result['score_ab']
            breakdown_ab = result['breakdown_ab']

            why_fit = self._build_why_fit_from_ismc(p, breakdown_ab)

            # Build reason summary from top ISMC factors
            reasons = []
            if p.who_you_serve:
                reasons.append(f'Serves: {p.who_you_serve[:80]}')
            if (p.list_size or 0) >= 10000:
                reasons.append(f'{p.list_size:,} list')
            if p.email:
                reasons.append('email available')
            elif _extract_linkedin(p):
                reasons.append('LinkedIn available')

            scored.append({
                'partner': p,
                'score': score_ab,
                'breakdown': breakdown_ab,
                'reason': '; '.join(reasons) if reasons else 'business alignment',
                'why_fit': why_fit,
                'detail_note': '',
            })

        scored.sort(key=lambda x: x['score'], reverse=True)
        top_matches = scored[:top_n]
        self.stdout.write(f'  Top {len(top_matches)} selected')
        return top_matches

    def _build_why_fit_from_ismc(self, partner: SupabaseProfile, breakdown: dict) -> str:
        """Build a narrative why-fit paragraph from ISMC breakdown factors."""
        parts = []

        # Intent signals
        intent_factors = breakdown.get('intent', {}).get('factors', [])
        for f in intent_factors:
            if f['score'] >= 7.0 and f['name'] == 'JV History':
                parts.append(f'{partner.name} {f["detail"]}.')
            if f['score'] >= 7.0 and f['name'] == 'Seeking Stated':
                if partner.seeking:
                    parts.append(f'Actively seeking: {partner.seeking}.')

        # Synergy signals
        synergy_factors = breakdown.get('synergy', {}).get('factors', [])
        for f in synergy_factors:
            if f['score'] >= 6.0 and f['name'] == 'Offering↔Seeking':
                if partner.what_you_do:
                    parts.append(partner.what_you_do)
                elif partner.offering:
                    parts.append(partner.offering)
            if f['score'] >= 6.0 and f['name'] == 'Audience Alignment':
                if partner.who_you_serve:
                    parts.append(f'Audience: {partner.who_you_serve}.')
            if f['score'] >= 6.0 and f['name'] == 'Platform Overlap':
                parts.append(f'{f["detail"]}.')

        # Momentum signals
        momentum_factors = breakdown.get('momentum', {}).get('factors', [])
        for f in momentum_factors:
            if f['score'] >= 7.0 and f['name'] == 'List Size':
                ls = partner.list_size or 0
                if ls >= 50000:
                    parts.append(f'Large audience of {ls:,}+ subscribers — ideal for cross-promotion.')
                elif ls >= 10000:
                    parts.append(f'{ls:,} subscribers — solid reach for partnership.')
            if f['score'] >= 7.0 and f['name'] == 'Social Reach':
                reach = partner.social_reach or 0
                if reach >= 10000:
                    parts.append(f'{reach:,} social reach.')

        # Fallback if nothing scored high enough
        if not parts:
            if partner.who_you_serve:
                parts.append(f'{partner.who_you_serve}.')
            elif partner.niche:
                parts.append(f'{partner.niche} specialist.')
            if partner.what_you_do:
                parts.append(partner.what_you_do)

        return ' '.join(parts) if parts else ''

    def _build_detail_note(self, partner: SupabaseProfile, client: dict) -> str:
        """Generate the italic detail note for the expanded card view."""
        parts = []
        if partner.signature_programs:
            parts.append(f'Programs: {partner.signature_programs}')
        if partner.business_focus and partner.business_focus != partner.niche:
            parts.append(f'Focus: {partner.business_focus}')
        if partner.notes:
            # Include relevant notes (booking links, contact preferences)
            for line in partner.notes.split('\n')[:2]:
                line = line.strip()
                if line and len(line) < 200:
                    parts.append(line)
        return ' · '.join(parts) if parts else ''

    # =========================================================================
    # AI ENRICHMENT (optional)
    # =========================================================================

    def _enrich_matches(self, matches: list, client: dict):
        self.stdout.write('\nEnriching matches with AI-generated content...')
        try:
            from matching.enrichment.match_enrichment import MatchEnrichmentService
            service = MatchEnrichmentService(client)
        except Exception as e:
            self.stdout.write(self.style.WARNING(f'  Enrichment unavailable: {e}'))
            return

        for match in matches:
            partner = match['partner']
            partner_dict = {
                'name': partner.name,
                'company': partner.company or '',
                'email': partner.email or '',
                'website': partner.website or '',
                'niche': partner.niche or '',
                'list_size': partner.list_size or 0,
                'score': match['score'],
            }
            full_profile = {
                'name': partner.name,
                'who_you_serve': partner.who_you_serve or '',
                'what_you_do': partner.what_you_do or '',
                'seeking': partner.seeking or '',
                'offering': partner.offering or '',
            }
            try:
                enriched = service.enrich_match(partner_dict, full_profile)
                match['why_fit'] = enriched.why_fit or match.get('why_fit', '')
                match['detail_note'] = enriched.mutual_benefit or ''
                self.stdout.write(f'  Enriched: {partner.name}')
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'  Failed: {partner.name} - {e}'))

    # =========================================================================
    # SECTION ASSIGNMENT
    # =========================================================================

    def _assign_section(self, match: dict, rank: int, total: int) -> tuple:
        """Assign section based on ISMC score, contact availability, and signals."""
        partner = match['partner']
        score = match['score']
        has_email = bool(partner.email)
        has_linkedin = bool(_extract_linkedin(partner))
        has_schedule = bool(_extract_schedule(partner))

        # JV Programs: has booking/apply link and actively seeking JVs
        if partner.booking_link and partner.seeking and 'jv' in (partner.seeking or '').lower():
            return 'jv_programs', 'JV Programs', 'Apply directly via their partner page'

        # Priority: high ISMC score + has email
        if score >= 70 and has_email:
            return 'priority', 'Priority Contacts', 'High match score — reach out this week'

        # This Week: moderate score + has email
        if score >= 50 and has_email:
            return 'this_week', 'This Week', 'Email available — schedule outreach'

        # LinkedIn outreach
        if has_linkedin:
            return 'low_priority', 'LinkedIn Outreach', 'Connect on LinkedIn first'

        # Schedule link available
        if has_schedule:
            return 'this_week', 'This Week', 'Schedule a call directly'

        return 'low_priority', 'Research Needed', 'Find contact info before outreach'

    # =========================================================================
    # DISPLAY HELPERS
    # =========================================================================

    def _clean_company_name(self, sp: SupabaseProfile) -> str:
        """Extract a clean company name, filtering out niche/category text."""
        company = (sp.company or '').strip()
        # If company starts with comma, it's garbage from Supabase
        if company.startswith(',') or company.startswith('.'):
            company = company.lstrip(',.').strip()
        # Detect category-style text (generic niche words, comma-separated)
        category_words = {
            'business skills', 'self improvement', 'success', 'fitness',
            'lifestyle', 'mental health', 'health', 'personal finances',
            'relationships', 'spirituality', 'natural health', 'service provider',
        }
        lower = company.lower()
        # If any segment is a generic category, it's not a company name
        segments = [s.strip().lower() for s in company.split(',')]
        if any(seg in category_words for seg in segments):
            # Try to find a non-category segment
            real = [s.strip() for s in company.split(',') if s.strip().lower() not in category_words]
            company = real[0] if real else ''
        # If it's still comma-separated categories, take the first segment
        if company.count(',') >= 1:
            first = company.split(',')[0].strip()
            if first and len(first) > 3:
                company = first
            else:
                company = ''
        # If empty or placeholder, fall back to name
        if not company or company.lower() in ('more info', 'n/a', 'none', 'tbd'):
            return sp.name
        return company

    def _assign_badge(self, match: dict, partner: SupabaseProfile) -> str:
        score = match['score']
        if score >= 85:
            return 'Top Match'
        if partner.seeking and 'jv' in (partner.seeking or '').lower():
            return 'Active JV'
        if score >= 75:
            return 'Strong Match'
        if (partner.list_size or 0) >= 100000:
            return f'{self._format_list_size(partner.list_size)} Reach'
        return ''

    def _assign_badge_style(self, match: dict) -> str:
        if match['score'] >= 85:
            return 'priority'
        if match['score'] >= 70:
            return 'fit'
        return 'fit'

    def _build_tagline(self, sp: SupabaseProfile) -> str:
        """Build a full tagline — NO truncation."""
        if sp.what_you_do:
            return sp.what_you_do
        if sp.offering:
            return sp.offering
        if sp.niche:
            return sp.niche
        if sp.business_focus:
            return sp.business_focus
        return ''

    def _format_list_size(self, size: int | None) -> str:
        if not size:
            return ''
        if size >= 1_000_000:
            return f'{size / 1_000_000:.0f}M+'
        if size >= 1000:
            return f'{size // 1000}K'
        return str(size)

    def _build_audience_desc(self, sp: SupabaseProfile, client: dict) -> str:
        """Build a rich audience description for the expanded card view."""
        parts = []
        if sp.list_size:
            parts.append(f'{sp.list_size:,} subscribers')
        if sp.who_you_serve:
            parts.append(sp.who_you_serve)
        elif sp.niche:
            parts.append(f'{sp.niche} audience')
        if sp.offering and sp.offering not in ' '.join(parts):
            parts.append(f'Offering: {sp.offering}')
        return '. '.join(parts) if parts else ''

    def _build_tags(self, match: dict, partner: SupabaseProfile) -> list:
        tags = []
        all_text = ' '.join(filter(None, [
            partner.niche or '', partner.who_you_serve or '',
            partner.what_you_do or '', partner.offering or '',
        ])).lower()

        # Audience-based tags
        if 'women' in all_text or 'female' in all_text:
            tags.append({'label': 'Women', 'style': 'fit'})
        if 'coach' in all_text:
            tags.append({'label': 'Coaches', 'style': 'fit'})
        if 'speaker' in all_text or 'speaking' in all_text:
            tags.append({'label': 'Speakers', 'style': 'fit'})
        if 'entrepreneur' in all_text:
            tags.append({'label': 'Entrepreneurs', 'style': 'fit'})
        if 'author' in all_text or 'book' in all_text:
            tags.append({'label': 'Author', 'style': 'fit'})
        if 'event' in all_text or 'summit' in all_text:
            tags.append({'label': 'Events', 'style': 'fit'})
        if 'podcast' in all_text:
            tags.append({'label': 'Podcast', 'style': 'fit'})

        # Status tags
        if partner.seeking and 'jv' in (partner.seeking or '').lower():
            tags.append({'label': 'Active JV', 'style': 'priority'})
        if match['score'] >= 85:
            tags.append({'label': 'Top Match', 'style': 'priority'})

        # Networking tag for large lists
        if (partner.list_size or 0) >= 50000:
            tags.append({'label': 'Large List', 'style': 'fit'})

        return tags[:4]

    # =========================================================================
    # CLIENT PROFILE JSON (matches DEMO_PROFILE structure)
    # =========================================================================

    def _build_client_profile(self, sp: SupabaseProfile) -> dict:
        name = sp.name
        company = self._clean_company_name(sp)
        first_name = name.split()[0] if name else ''

        return {
            'client': {
                'name': company,
                'tagline': sp.what_you_do or sp.niche or '',
            },
            'contact_name': name,
            'avatar_initials': ''.join(w[0].upper() for w in name.split()[:2]) if name else '??',
            'title': f'{name} · {company}',
            'program_name': company,
            'program_sub': sp.offering or '',
            'program_focus': sp.niche or 'Business Growth',
            'target_audience': 'Target Audience',
            'target_audience_sub': sp.who_you_serve or '',
            'network_reach': self._format_list_size(sp.list_size) or 'Growing',
            'network_reach_sub': f'{first_name}\'s subscriber network',
            'tiers': [],
            'main_website': _extract_website(sp) or '',
            'offers_partners': self._build_offers_partners(sp),
            'key_message': sp.offering or '',
            'about_story': sp.bio or f'{name} is the founder of {company}.',
            'shared_stage': [],
            'credentials': self._build_credentials(sp),
            'ideal_partner_intro': f'Partners serving <strong>{sp.who_you_serve or "entrepreneurs and business owners"}</strong>.',
            'ideal_partner_sub': sp.seeking or '',
            'perfect_for': [],
            'seeking_goals': self._build_seeking_goals(sp),
            'seeking_focus': sp.seeking or '',
            'faqs': [],
            'contact_email': 'help@jvmatches.com',
        }

    def _build_offers_partners(self, sp: SupabaseProfile) -> list:
        offers = []
        if sp.list_size:
            offers.append({
                'value': self._format_list_size(sp.list_size),
                'label': 'subscribers',
                'highlight': False,
            })
        if sp.social_reach:
            offers.append({
                'value': self._format_list_size(sp.social_reach),
                'label': 'social reach',
                'highlight': False,
            })
        return offers

    def _build_credentials(self, sp: SupabaseProfile) -> list:
        creds = []
        company = self._clean_company_name(sp)
        if company and company != sp.name:
            creds.append(f'Founder of {company}')
        if sp.signature_programs:
            creds.append(sp.signature_programs)
        if sp.bio:
            first_sentence = sp.bio.split('.')[0].strip()
            if first_sentence and len(first_sentence) < 200:
                creds.append(first_sentence)
        return creds

    def _build_seeking_goals(self, sp: SupabaseProfile) -> list:
        goals = []
        if sp.seeking:
            for line in sp.seeking.split(','):
                line = line.strip()
                if line:
                    goals.append(line)
        return goals[:5]

    # =========================================================================
    # OUTREACH TEMPLATES
    # =========================================================================

    def _build_outreach_templates(self, sp: SupabaseProfile) -> dict:
        name = sp.name
        company = self._clean_company_name(sp)
        return {
            'initial': {
                'title': 'Initial Outreach',
                'text': (
                    f"Hi [Partner Name],\n\n"
                    f"I came across your work and love what you're doing for [their audience]. "
                    f"I'm {name} with {company}, and I think our audiences could really "
                    f"benefit from knowing about each other.\n\n"
                    f"Would you be open to a quick call to explore some partnership ideas?\n\n"
                    f"Best,\n{name}"
                ),
            },
            'followup': {
                'title': 'Follow-Up',
                'text': (
                    f"Hi [Partner Name],\n\n"
                    f"Just following up on my earlier message. I'd love to connect and "
                    f"explore how we might support each other's communities.\n\n"
                    f"No pressure at all — just thought there might be a great fit.\n\n"
                    f"Warmly,\n{name}"
                ),
            },
        }

    # =========================================================================
    # UTILITY
    # =========================================================================

    @staticmethod
    def _parse_month(month_str: str | None) -> date:
        if not month_str:
            today = date.today()
            return today.replace(day=1)
        try:
            parts = month_str.split('-')
            return date(int(parts[0]), int(parts[1]), 1)
        except (ValueError, IndexError):
            raise CommandError(f'Invalid month format: {month_str}. Use YYYY-MM.')
