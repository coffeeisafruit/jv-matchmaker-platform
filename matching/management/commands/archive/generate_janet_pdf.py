"""
Generate Janet Bray Attwood's PDF report with ENRICHED match data.
Uses full profile data (seeking, offering, who_they_serve) for compelling reasoning.
TOP 10 matches only with verification.
"""

import csv
import logging
import re
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from pathlib import Path

logger = logging.getLogger(__name__)

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from matching.pdf_services.pdf_generator import PDFGenerator
from matching.models import SupabaseProfile
from matching.enrichment.match_enrichment import (
    MatchEnrichmentService,
    MatchVerificationAgent,
    TextSanitizer,
    enrich_and_verify_matches,
)
from matching.enrichment.ai_research import (
    ProfileResearchService,
    research_and_enrich_profile,
)
from matching.enrichment.deep_research import (
    deep_research_profile,
    SimpleDeepResearch,
)
from matching.enrichment.owl_research.agents.owl_enrichment_service import (
    enrich_profile_with_owl_sync,
)


# Janet's full profile for the enrichment service
JANET_PROFILE = {
    'name': 'Janet Bray Attwood',
    'company': 'The Passion Test',
    # What she DOES (her main business)
    'what_you_do': 'NY Times bestselling author and co-creator of The Passion Test, helping people discover their passions and purpose',
    'who_you_serve': 'Coaches, entrepreneurs, and transformation seekers ready to expand their impact globally',
    # What she's SEEKING connections for (the current program she's promoting)
    'seeking': 'Partners for Becoming International - a 3-tier program teaching wannabe, 1st level, and 2nd level coaches/entrepreneurs how to expand internationally',
    'offering': 'Global network of 5,000+ certified facilitators in 65+ countries, bestseller credibility, engaged audience seeking transformation',
    # CURRENT FOCUS
    'current_program': 'Becoming International',
    'program_description': 'A 3-tier program teaching wannabe, 1st level, and 2nd level entrepreneurs/coaches how to expand internationally',
}


class Command(BaseCommand):
    help = 'Generate Janet Bray Attwood PDF report with ENRICHED match data (top 10)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--skip-verification',
            action='store_true',
            help='Skip the verification step (not recommended)'
        )
        parser.add_argument(
            '--ai-verification',
            action='store_true',
            help='Use AI-powered verification (Claude) for intelligent content evaluation'
        )
        parser.add_argument(
            '--research-sparse',
            action='store_true',
            help='Research sparse profiles by fetching their websites to get REAL data'
        )
        parser.add_argument(
            '--no-cache',
            action='store_true',
            help='Skip research cache and always fetch fresh data'
        )
        parser.add_argument(
            '--deep-research',
            action='store_true',
            help='Use multi-source deep research (web search) for profiles that website scraping missed'
        )
        parser.add_argument(
            '--owl-research',
            action='store_true',
            help='Use OWL multi-agent research with source verification (requires owl_framework venv)'
        )

    def handle(self, *args, **options):
        skip_verification = options.get('skip_verification', False)
        use_ai_verification = options.get('ai_verification', False)
        research_sparse = options.get('research_sparse', False)
        use_cache = not options.get('no_cache', False)
        deep_research = options.get('deep_research', False)
        owl_research = options.get('owl_research', False)

        self.stdout.write(self.style.SUCCESS('\n' + '='*60))
        self.stdout.write(self.style.SUCCESS('JANET BRAY ATTWOOD - ENRICHED JV REPORT'))
        self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

        # Step 1: Load scraped contact info (for emails, phones)
        scraped_contacts = self._load_scraped_contacts()
        self.stdout.write(f'Loaded {len(scraped_contacts)} scraped contacts')

        # Step 2: Load basic match data from CSV
        raw_matches = self._load_match_csv()
        self.stdout.write(f'Loaded {len(raw_matches)} matches from CSV')

        # Step 3: Pull FULL profile data from Supabase (the gold!)
        self.stdout.write('\nPulling full profile data from Supabase...')
        supabase_profiles = self._load_supabase_profiles(raw_matches)
        self.stdout.write(f'Found {len(supabase_profiles)} full profiles in Supabase')

        # Step 3.5: Backfill from research cache for any match not in Supabase (so all 10 get profile data)
        from matching.enrichment.ai_research import ProfileResearchCache
        cache = ProfileResearchCache()
        for match in raw_matches:
            name = match.get('name', '')
            if not name:
                continue
            name_lower = name.lower()
            if name_lower in supabase_profiles:
                continue
            cached = cache.get(name)
            if cached:
                supabase_profiles[name_lower] = {
                    'name': cached.get('name', name),
                    'email': cached.get('email', ''),
                    'company': cached.get('company', ''),
                    'website': cached.get('website', '') or match.get('website', ''),
                    'linkedin': cached.get('linkedin', '') or match.get('linkedin', ''),
                    'niche': cached.get('niche', ''),
                    'list_size': cached.get('list_size') or match.get('list_size', 0) or 0,
                    'social_reach': 0,
                    'who_you_serve': cached.get('who_you_serve', ''),
                    'what_you_do': cached.get('what_you_do', ''),
                    'seeking': cached.get('seeking', ''),
                    'offering': cached.get('offering', ''),
                    'business_focus': cached.get('business_focus', ''),
                    'bio': cached.get('bio', ''),
                    'notes': cached.get('notes', ''),
                }
                self.stdout.write(f'  ✓ From research cache: {name}')
        if len(supabase_profiles) < len(raw_matches):
            self.stdout.write(self.style.WARNING(
                f'  {len(raw_matches) - len(supabase_profiles)} match(es) have no Supabase or cache data'
            ))

        # Step 4: Merge scraped contacts with Supabase data
        for name_lower, scraped in scraped_contacts.items():
            if name_lower in supabase_profiles:
                # Prefer scraped email if available
                if scraped.get('email'):
                    supabase_profiles[name_lower]['email'] = scraped['email']
                # Merge best_way_to_contact into notes (for PR teams, assistants, etc.)
                if scraped.get('best_way_to_contact'):
                    existing_notes = supabase_profiles[name_lower].get('notes', '') or ''
                    contact_info = scraped['best_way_to_contact']
                    if contact_info not in existing_notes:
                        supabase_profiles[name_lower]['notes'] = f"{existing_notes}\nPreferred Contact: {contact_info}".strip()

        # Step 4.5: Research sparse profiles to fill data gaps
        if research_sparse:
            self.stdout.write('\n' + '='*60)
            self.stdout.write(self.style.SUCCESS('RESEARCHING SPARSE PROFILES'))
            self.stdout.write('='*60)
            self.stdout.write('Fetching REAL data from partner websites...\n')

            # Build lookup of CSV websites
            csv_websites = {m.get('name', '').lower(): m.get('website', '') for m in raw_matches}

            research_count = 0
            for name_lower, profile in supabase_profiles.items():
                # Check if profile needs research - missing the KEY fields for compelling content
                # The gold fields are: seeking, who_you_serve (these make content compelling)
                needs_seeking = not profile.get('seeking')
                needs_audience = not profile.get('who_you_serve')
                needs_research = needs_seeking or needs_audience

                # Get website from Supabase OR CSV
                website = profile.get('website', '') or csv_websites.get(name_lower, '')

                if needs_research and website:
                    name = profile.get('name', name_lower)
                    self.stdout.write(f'  Researching: {name} (website: {website[:30]}...)' if len(website) > 30 else f'  Researching: {name} ({website})')

                    try:
                        enriched_data, was_researched = research_and_enrich_profile(
                            name=name,
                            website=website,
                            existing_data=profile,
                            use_cache=use_cache,
                            force_research=True  # Caller determined this profile needs research
                        )

                        if was_researched:
                            # Update profile with researched data
                            supabase_profiles[name_lower].update(enriched_data)
                            research_count += 1

                            # Show what we found
                            if enriched_data.get('seeking'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND seeking: {enriched_data["seeking"][:60]}...'))
                            if enriched_data.get('who_you_serve'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND audience: {enriched_data["who_you_serve"][:60]}...'))
                            if enriched_data.get('what_you_do'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND what_you_do: {enriched_data["what_you_do"][:60]}...'))
                            if not any([enriched_data.get('seeking'), enriched_data.get('who_you_serve'), enriched_data.get('what_you_do')]):
                                self.stdout.write(self.style.WARNING(f'    (no extractable data on website)'))
                        else:
                            self.stdout.write(self.style.WARNING(f'    (using cached data)'))

                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'    Error: {e}'))

            self.stdout.write(f'\nResearched {research_count} profiles')

        # Step 4.6: Deep research for profiles that still need enrichment
        if deep_research:
            self.stdout.write('\n' + '='*60)
            self.stdout.write(self.style.SUCCESS('DEEP RESEARCH (Multi-source web search)'))
            self.stdout.write('='*60)
            self.stdout.write('Searching web for additional profile data...\n')

            deep_count = 0
            for name_lower, profile in supabase_profiles.items():
                # Deep research can fill: seeking, who_you_serve, what_you_do, offering, bio, email
                # Check which fields are missing and could benefit from research
                missing_fields = []
                if not profile.get('seeking'):
                    missing_fields.append('seeking')
                if not profile.get('who_you_serve'):
                    missing_fields.append('who_you_serve')
                if not profile.get('what_you_do'):
                    missing_fields.append('what_you_do')
                if not profile.get('offering'):
                    missing_fields.append('offering')
                if not profile.get('bio'):
                    missing_fields.append('bio')
                if not profile.get('email'):
                    missing_fields.append('email')

                # Research if ANY key fields are missing
                if missing_fields:
                    name = profile.get('name', name_lower)
                    company = profile.get('company', '')

                    self.stdout.write(f'  Deep researching: {name} (missing: {", ".join(missing_fields[:3])}{"..." if len(missing_fields) > 3 else ""})')

                    try:
                        enriched_data, was_researched = deep_research_profile(
                            name=name,
                            company=company,
                            existing_data=profile,
                            use_gpt_researcher=False  # Use simple web search
                        )

                        if was_researched:
                            supabase_profiles[name_lower].update(enriched_data)
                            deep_count += 1

                            # Show all fields that were found
                            found_any = False
                            if enriched_data.get('seeking'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND seeking: {enriched_data["seeking"][:60]}...'))
                                found_any = True
                            if enriched_data.get('who_you_serve'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND audience: {enriched_data["who_you_serve"][:60]}...'))
                                found_any = True
                            if enriched_data.get('what_you_do'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND what_you_do: {enriched_data["what_you_do"][:60]}...'))
                                found_any = True
                            if enriched_data.get('offering'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND offering: {enriched_data["offering"][:60]}...'))
                                found_any = True
                            if enriched_data.get('bio'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND bio/credentials: {enriched_data["bio"][:60]}...'))
                                found_any = True
                            if enriched_data.get('email'):
                                self.stdout.write(self.style.SUCCESS(f'    FOUND email: {enriched_data["email"]}'))
                                found_any = True
                            if not found_any:
                                self.stdout.write(self.style.WARNING(f'    (no verified data found)'))
                        else:
                            self.stdout.write(self.style.WARNING(f'    (no results)'))

                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'    Error: {e}'))

            self.stdout.write(f'\nDeep researched {deep_count} profiles')

        # Step 4.7: OWL research - multi-agent research with source verification
        if owl_research:
            self.stdout.write('\n' + '='*60)
            self.stdout.write(self.style.SUCCESS('OWL RESEARCH (Multi-agent verified research)'))
            self.stdout.write('='*60)
            self.stdout.write('Using OWL toolkits + Claude Agent SDK for deep research...\n')

            csv_websites = {m.get('name', '').lower(): m.get('website', '') for m in raw_matches}
            owl_count = 0

            for name_lower, profile in supabase_profiles.items():
                # OWL research is best for profiles missing key JV data
                missing_fields = []
                if not profile.get('seeking'):
                    missing_fields.append('seeking')
                if not profile.get('who_you_serve'):
                    missing_fields.append('who_you_serve')
                if not profile.get('what_you_do'):
                    missing_fields.append('what_you_do')
                if not profile.get('offering'):
                    missing_fields.append('offering')

                # Only run OWL research if key fields are missing
                if missing_fields:
                    name = profile.get('name', name_lower)
                    company = profile.get('company', '')
                    website = profile.get('website', '') or csv_websites.get(name_lower, '')
                    linkedin = profile.get('linkedin', '')

                    self.stdout.write(f'  OWL researching: {name} (missing: {", ".join(missing_fields[:3])}{"..." if len(missing_fields) > 3 else ""})')

                    try:
                        owl_data, success = enrich_profile_with_owl_sync(
                            name=name,
                            company=company,
                            website=website,
                            linkedin=linkedin,
                            existing_data=profile,
                        )

                        if success:
                            # Update profile with OWL-verified data
                            # Only update fields that OWL found and verified
                            if owl_data.get('seeking'):
                                supabase_profiles[name_lower]['seeking'] = owl_data['seeking']
                            if owl_data.get('who_you_serve'):
                                supabase_profiles[name_lower]['who_you_serve'] = owl_data['who_you_serve']
                            if owl_data.get('what_you_do'):
                                supabase_profiles[name_lower]['what_you_do'] = owl_data['what_you_do']
                            if owl_data.get('offering'):
                                supabase_profiles[name_lower]['offering'] = owl_data['offering']
                            if owl_data.get('signature_programs'):
                                supabase_profiles[name_lower]['signature_programs'] = owl_data['signature_programs']
                            # CONTACT INFO - Critical for outreach
                            if owl_data.get('email'):
                                supabase_profiles[name_lower]['email'] = owl_data['email']
                            if owl_data.get('phone'):
                                supabase_profiles[name_lower]['phone'] = owl_data['phone']
                            if owl_data.get('booking_link'):
                                supabase_profiles[name_lower]['booking_link'] = owl_data['booking_link']

                            owl_count += 1
                            verified = owl_data.get('_verified_fields', 0)
                            confidence = owl_data.get('_confidence', 0)

                            self.stdout.write(self.style.SUCCESS(f'    ✓ OWL SUCCESS - {verified}/12 verified fields ({confidence:.0%} confidence)'))

                            # Show contact info first (most important)
                            if owl_data.get('email'):
                                self.stdout.write(self.style.SUCCESS(f'      EMAIL: {owl_data["email"]}'))
                            if owl_data.get('booking_link'):
                                self.stdout.write(self.style.SUCCESS(f'      BOOKING: {owl_data["booking_link"][:50]}...'))
                            # Show key findings with sources
                            if owl_data.get('seeking'):
                                self.stdout.write(self.style.SUCCESS(f'      SEEKING: {owl_data["seeking"][:60]}...'))
                            if owl_data.get('signature_programs'):
                                self.stdout.write(self.style.SUCCESS(f'      PROGRAMS: {owl_data["signature_programs"][:60]}...'))
                            if owl_data.get('who_you_serve'):
                                self.stdout.write(self.style.SUCCESS(f'      SERVES: {owl_data["who_you_serve"][:60]}...'))
                        else:
                            self.stdout.write(self.style.WARNING(f'    ⚠ No verified data found'))

                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'    ✗ Error: {str(e)[:60]}...'))

            self.stdout.write(f'\nOWL researched {owl_count} profiles with verified sources')

        # Step 5: Enrich matches with full data, verify, and auto-fix issues
        self.stdout.write('\nEnriching matches with full profile data...')

        if use_ai_verification:
            self.stdout.write(self.style.SUCCESS('Using AI-POWERED verification (Claude):'))
            self.stdout.write('  - Formatting Agent (complete sentences, structure, length)')
            self.stdout.write('  - Content Quality Agent (personalization, specificity)')
            self.stdout.write('  - Data Quality Agent (field correctness, boilerplate detection)')
            self.stdout.write('  - Outreach Agent (email effectiveness, personalization)')
            self.stdout.write('  + Auto-fix capability using Claude')
        else:
            self.stdout.write('Using RULE-BASED verification system:')
            self.stdout.write('  - EncodingVerificationAgent (Unicode, special chars)')
            self.stdout.write('  - FormattingVerificationAgent (structure, bullets)')
            self.stdout.write('  - ContentVerificationAgent (empty sections, specificity)')
            self.stdout.write('  - CapitalizationVerificationAgent (bullet caps)')
            self.stdout.write('  - TruncationVerificationAgent (cut-off words)')
            self.stdout.write('  - DataQualityVerificationAgent (boilerplate, misplaced data)')
            self.stdout.write(self.style.WARNING('  (Use --ai-verification for intelligent AI-powered checks)'))
        self.stdout.write('')

        enrichment_service = MatchEnrichmentService(JANET_PROFILE)

        # Choose verification agent based on flag
        if use_ai_verification:
            from matching.enrichment.ai_verification import AIMatchVerificationAgent
            verification_agent = AIMatchVerificationAgent()
        else:
            verification_agent = MatchVerificationAgent()

        enriched_matches = []
        for match_data in raw_matches:
            name = match_data.get('name', '')
            name_lower = name.lower()

            # Get full profile from Supabase
            full_profile = supabase_profiles.get(name_lower, {})

            # Enrich
            enriched = enrichment_service.enrich_match(match_data, full_profile)

            # Verify AND auto-fix (unless skipped)
            if not skip_verification:
                if use_ai_verification:
                    # AI agent returns (match, score, issues)
                    fixed_match, score, issues = verification_agent.verify_and_fix(enriched, full_profile)
                    status_icon = '✓' if fixed_match.verification_passed else '⚠'
                    self.stdout.write(f'  {status_icon} {name}: Score {score:.0f}/100')

                    if issues:
                        for issue in issues[:3]:
                            if '[content]' in issue or '[formatting]' in issue:
                                self.stdout.write(self.style.ERROR(f'      {issue}'))
                            else:
                                self.stdout.write(self.style.WARNING(f'      {issue}'))

                    enriched_matches.append(fixed_match)
                else:
                    # Rule-based agent returns VerificationResult object
                    fixed_match, result = verification_agent.verify_and_fix(enriched)
                    fixed_match.verification_score = result.score
                    fixed_match.verification_passed = result.status.value == 'passed'

                    status_icon = '✓' if fixed_match.verification_passed else '⚠'
                    data_badge = {'rich': '★', 'partial': '◐', 'sparse': '○'}.get(enriched.data_quality, '?')
                    self.stdout.write(f'  {status_icon} {name}: Score {result.score}/100 [{data_badge} {enriched.data_quality}]')

                    if result.issues:
                        # Show issues by severity
                        critical = [i for i in result.issues if '[encoding]' in i or '[content]' in i]
                        warnings = [i for i in result.issues if i not in critical]

                        for issue in critical[:3]:
                            self.stdout.write(self.style.ERROR(f'      CRITICAL: {issue}'))
                        for issue in warnings[:2]:
                            self.stdout.write(self.style.WARNING(f'      Warning: {issue}'))

                    enriched_matches.append(fixed_match)
            else:
                logger.warning(
                    "skip_verification=True: bypassing verification for %s — "
                    "verification_passed=True and score=100 are synthetic, not verified",
                    name,
                )
                enriched.verification_passed = True
                enriched.verification_score = 100
                enriched_matches.append(enriched)

        # Step 6: Convert enriched matches to PDF format (with final sanitization)
        self.stdout.write('\nPreparing matches for PDF (final sanitization pass)...')
        pdf_matches = []

        for em in enriched_matches:
            # Build contact string
            contact_lines = []
            if em.email:
                contact_lines.append(em.email)
            if em.linkedin and 'linkedin.com' in em.linkedin:
                contact_lines.append(em.linkedin)
            contact_str = ' | '.join(contact_lines) if contact_lines else 'Contact via JV Directory'

            # Determine match type based on contact availability
            if em.email:
                match_type = 'Email Ready'
            elif em.linkedin:
                match_type = 'LinkedIn'
            else:
                match_type = 'Verify'

            # Clean company name
            company = TextSanitizer.sanitize(em.company)
            if company and company.isupper():
                company = company.title()

            # Prepare benefits string with safe truncation
            # Include social reach if available
            reach_parts = [f"List: {em.list_size:,}"]
            if em.social_reach and em.social_reach > 0:
                reach_parts.append(f"Social: {em.social_reach:,}")
            if em.seeking:
                seeking_preview = TextSanitizer.truncate_safe(em.seeking, 50)
                reach_parts.append(f"Seeking: {seeking_preview}")
            benefits = ' | '.join(reach_parts)

            # Extract calendar link and contact preference from notes
            calendar_link = ''
            best_contact = ''
            if em.notes:
                lines = em.notes.split('\n')
                contact_methods = []

                for line in lines:
                    line_stripped = line.strip()

                    # Calendar link
                    if 'Calendar:' in line:
                        calendar_link = line.replace('Calendar:', '').strip()

                    # Extract ACTUAL contact methods, not boilerplate intro text
                    # Look for specific patterns that indicate real contact info
                    elif line_stripped.startswith('Email:'):
                        contact_methods.append(line_stripped)
                    elif line_stripped.startswith('Phone:'):
                        contact_methods.append(line_stripped)
                    elif line_stripped.startswith('Text:'):
                        contact_methods.append(line_stripped)
                    elif line_stripped.startswith('PR Team:'):
                        contact_methods.append(line_stripped)
                    elif line_stripped.startswith('Preferred Contact:'):
                        # Extract just the contact info, not the label
                        contact_info = line_stripped.replace('Preferred Contact:', '').strip()
                        contact_methods.append(contact_info)
                    elif 'calendly.com' in line_stripped.lower() or 'cal.com' in line_stripped.lower():
                        if not calendar_link:  # Don't override explicit Calendar: field
                            calendar_link = line_stripped

                # Join actual contact methods (not intro text)
                if contact_methods:
                    best_contact = ' | '.join(contact_methods[:2])  # Max 2 for space

            # FINAL sanitization of all text fields for PDF
            pdf_match = {
                'name': TextSanitizer.sanitize(em.name),
                'company': company,
                'score': int(em.score),
                'type': match_type,
                'contact': contact_str,
                'website': TextSanitizer.sanitize(em.website) if em.website else '',
                'social_reach': em.social_reach,
                'calendar_link': calendar_link,
                'best_contact': best_contact,
                'fit': TextSanitizer.sanitize(em.why_fit),  # Sanitized!
                'opportunity': TextSanitizer.sanitize(em.mutual_benefit),  # Sanitized!
                'benefits': benefits,
                'timing': 'Ready' if em.email else 'This week',
                'message': TextSanitizer.sanitize(em.outreach_message),  # Sanitized!
            }
            pdf_matches.append(pdf_match)

        # Step 7: Build member data for PDF
        member_data = {
            'participant': 'Janet Bray Attwood',
            'date': datetime.now().strftime("%B %d, %Y"),
            'profile': {
                'what_you_do': JANET_PROFILE['what_you_do'],
                'who_you_serve': JANET_PROFILE['who_you_serve'],
                'seeking': JANET_PROFILE['seeking'],
                'offering': JANET_PROFILE['offering'],
            },
            'matches': pdf_matches,
        }

        # Step 8: Generate PDF
        output_dir = Path('/Users/josephtepe/Projects/jv-matchmaker-platform/Chelsea_clients')
        generator = PDFGenerator(output_dir=str(output_dir))
        pdf_path = generator.generate(member_data)

        self.stdout.write(self.style.SUCCESS(f'\n✓ PDF generated: {pdf_path}'))

        # Print summary
        self.stdout.write('\n' + '='*60)
        self.stdout.write('ENRICHED MATCHES SUMMARY')
        self.stdout.write('='*60)

        for i, em in enumerate(enriched_matches, 1):
            status = '✓' if em.verification_passed else '⚠'
            self.stdout.write(f"\n{i}. {status} {em.name} ({em.company})")
            self.stdout.write(f"   Score: {em.score}/100 | Verification: {em.verification_score}/100")
            if em.seeking:
                self.stdout.write(f"   SEEKING: {em.seeking[:80]}...")
            if em.who_they_serve:
                self.stdout.write(f"   SERVES: {em.who_they_serve[:80]}...")

    def _load_scraped_contacts(self) -> dict:
        """Load scraped contact info from CSV."""
        scraped_contacts = {}
        scraped_file = Path('/Users/josephtepe/Projects/jv-matchmaker-platform/Chelsea_clients/janet_scraped_contacts.csv')

        if not scraped_file.exists():
            return scraped_contacts

        with open(scraped_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('name', '').strip()
                if name:
                    scraped_contacts[name.lower()] = {
                        'email': row.get('email', '').strip(),
                        'phone': row.get('phone', '').strip(),
                        'website': row.get('website', '').strip(),
                        'calendar_link': row.get('calendar_link', '').strip(),
                        'best_way_to_contact': row.get('best_way_to_contact', '').strip(),
                    }

        return scraped_contacts

    def _load_match_csv(self) -> list:
        """Load basic match data from CSV - TOP 10 ONLY."""
        matches = []
        matches_file = Path('/Users/josephtepe/Projects/jv-matchmaker-platform/Chelsea_clients/janet_actionable_matches.csv')

        with open(matches_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if len(matches) >= 10:
                    break

                # Convert score to 0-100 scale
                raw_score = float(row.get('match_score', 0))
                score = int(raw_score * 10)  # 8.5 -> 85

                # Parse list size
                try:
                    list_size = int(row.get('list_size', 0))
                except:
                    list_size = 0

                match = {
                    'name': row.get('name', '').strip(),
                    'company': row.get('company', '').strip(),
                    'email': row.get('email', '').strip(),
                    'website': row.get('website', '').strip(),
                    'linkedin': row.get('linkedin', '').strip(),
                    'niche': row.get('niche', '').strip(),
                    'list_size': list_size,
                    'score': score,
                    'outreach_status': row.get('outreach_status', 'pending'),
                }
                matches.append(match)

        return matches

    def _load_supabase_profiles(self, matches: list) -> dict:
        """
        Pull FULL profile data from Supabase for each match.
        This is where we get the GOLD: seeking, offering, who_they_serve, what_they_do
        """
        supabase_profiles = {}

        for match in matches:
            name = match.get('name', '')
            if not name:
                continue

            # Try to find in Supabase by name - EXACT MATCH FIRST
            try:
                # 1. Try exact match first (case-insensitive)
                profile = SupabaseProfile.objects.filter(name__iexact=name).first()

                # 2. If no exact match, try full name contains (not just first name!)
                if not profile:
                    profile = SupabaseProfile.objects.filter(name__icontains=name).first()

                # 3. Last resort: match by first AND last name parts
                if not profile and len(name.split()) >= 2:
                    first_name, last_name = name.split()[0], name.split()[-1]
                    profile = SupabaseProfile.objects.filter(
                        Q(name__icontains=first_name) & Q(name__icontains=last_name)
                    ).first()

                if profile:
                    supabase_profiles[name.lower()] = {
                        'name': profile.name,
                        'email': profile.email or '',
                        'company': profile.company or '',
                        'website': profile.website or '',
                        'linkedin': profile.linkedin or '',
                        'niche': profile.niche or '',
                        'list_size': profile.list_size or 0,
                        'social_reach': profile.social_reach or 0,
                        # THE GOLD - full profile fields!
                        'who_you_serve': profile.who_you_serve or '',
                        'what_you_do': profile.what_you_do or '',
                        'seeking': profile.seeking or '',
                        'offering': profile.offering or '',
                        'business_focus': profile.business_focus or '',
                        'bio': profile.bio or '',
                        'notes': profile.notes or '',  # Calendar links, contact preferences
                    }
                    self.stdout.write(f'  ✓ Found: {name}')
                    if profile.seeking:
                        self.stdout.write(f'    SEEKING: {profile.seeking[:60]}...')
                else:
                    self.stdout.write(f'  ⚠ Not found in Supabase: {name}')
            except Exception as e:
                self.stdout.write(f'  ✗ Error looking up {name}: {e}')

        return supabase_profiles
