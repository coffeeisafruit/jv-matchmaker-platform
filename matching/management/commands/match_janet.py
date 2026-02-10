"""
One-off management command to match JV partners for Janet Bray Attwood.
Based on match_penelope.py but customized for Janet's specific needs.

This is a standalone command that does NOT modify any existing services.
Can be safely deleted after use.
"""
import csv
import re
import os
import requests
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from matching.models import SupabaseProfile
from matching.pdf_services.pdf_generator import PDFGenerator


# =============================================================================
# JANET'S PROFILE (Becoming International positioning)
# =============================================================================
JANET_PROFILE = {
    'name': 'Janet Bray Attwood',
    'company': 'Becoming International',
    'website': 'becominginternational.com',
    'niche': 'International expansion for coaches & speakers',
    'offering': 'Becoming International Mastermind - land international speaking engagements, build elite global networks',
    'offering_details': 'Mastermind program for coaches/speakers ready to expand globally',
    'seeking': 'JV Launch Partners, Affiliates, Speaking Platforms',
    'partnership_types': 'Cross-promotion, Affiliate, Speaking/Events',
    'who_she_serves': 'Coaches and speakers ready to expand their reach internationally',
    'credentials': 'NY Times bestselling author (The Passion Test), 5,000+ facilitators in 65+ countries, TLC founding member',
    'booking_link': 'https://www.becominginternational.com',
}

# =============================================================================
# SCORING CONFIGURATION
# =============================================================================

# Scoring weights (adjusted for Janet's needs)
SCORING_WEIGHTS = {
    'audience_overlap': 0.35,    # Partner serves coaches, entrepreneurs, seekers
    'niche_alignment': 0.30,     # Complementary business focus
    'scale_match': 0.20,         # List size compatibility
    'profile_quality': 0.15,     # Has email, website, linkedin
}

# Keywords indicating target audience (coaches/speakers wanting international expansion)
AUDIENCE_KEYWORDS = [
    'coach', 'coaching', 'speaker', 'speaking', 'consultant',
    'international', 'global', 'expand', 'scale', 'worldwide',
    'entrepreneur', 'business owner', 'expert', 'thought leader',
    'author', 'trainer', 'facilitator', 'mentor',
    'event', 'summit', 'conference', 'mastermind',
    'network', 'connections', 'high-level', 'elite',
    'visibility', 'platform', 'stage', 'keynote',
]

# List size thresholds
LARGE_LIST_THRESHOLD = 500000  # Flag for verification if > 500K
MINIMUM_LIST_SIZE = 10000       # Minimum list size for Janet's matches

# Complementary niches (BONUS - audiences of coaches/speakers wanting to go global)
COMPLEMENTARY_NICHES = {
    # High bonus (+3) - Direct international/speaking alignment
    'speaker training': 3, 'public speaking': 3, 'speaking coach': 3,
    'international business': 3, 'global expansion': 3, 'international': 3,
    'event producer': 3, 'summit host': 3, 'conference': 3, 'virtual summit': 3,
    'mastermind': 3, 'high-level networking': 3, 'elite network': 3,
    'keynote': 3, 'stage': 3, 'ted': 3, 'tedx': 3,
    # Medium bonus (+2) - Coach/consultant audiences
    'business coach': 2, 'business coaching': 2,
    'entrepreneur coach': 2, 'entrepreneurship': 2,
    'leadership': 2, 'leadership coach': 2, 'executive coach': 2,
    'visibility': 2, 'personal branding': 2, 'thought leadership': 2,
    'podcast host': 2, 'media': 2, 'pr': 2, 'publicity': 2,
    'author': 2, 'publishing': 2, 'book coach': 2,
    'coach training': 2, 'certification': 2, 'facilitator': 2,
    'women entrepreneur': 2, 'female entrepreneur': 2,
}

# Competing niches (EXCLUDE or PENALTY - direct competitors)
COMPETING_NICHES = {
    # EXCLUDE - Direct competitors in international coaching/speaker space
    'international speaker certification': 'exclude',
    'global speaker training': 'exclude',
    # Keep passion-related exclusions (still relevant to avoid confusion)
    'passion coach': 'exclude',
    'passion test': 'exclude',
    # EXCLUDE - Unrelated niches (poor fit for Janet's audience)
    'safety compliance': 'exclude',
    'workplace safety': 'exclude',
    'osha': 'exclude',
    'self-love': 'exclude',
    'self love': 'exclude',
    'dating': 'exclude',
    'relationship coach': 'exclude',
    'weight loss': 'exclude',
    'fitness': 'exclude',
    'real estate': 'exclude',
    'mortgage': 'exclude',
    'financial advisor': 'exclude',
    'insurance': 'exclude',
    'mlm': 'exclude',
    'network marketing': 'exclude',
    # Penalty (-2) - Overlapping but not direct competitors
    'speaker bureau': -2,
}

# Emails to exclude from matches (internal team, etc.)
EXCLUDE_EMAILS = [
    'joetepe@gmail.com',
]


class Command(BaseCommand):
    help = 'Find top 50 JV partner matches for Janet Bray Attwood'

    def add_arguments(self, parser):
        parser.add_argument(
            '--top',
            type=int,
            default=50,
            help='Number of top matches to return (default: 50)'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='Chelsea_clients/janet_matches.csv',
            help='Output CSV file path'
        )
        parser.add_argument(
            '--skip-ai',
            action='store_true',
            help='Skip AI insight generation (faster, but no personalized insights)'
        )
        parser.add_argument(
            '--skip-pdf',
            action='store_true',
            help='Skip PDF generation (CSV only)'
        )
        parser.add_argument(
            '--require-email',
            action='store_true',
            help='Only include matches that have an email address'
        )

    def handle(self, *args, **options):
        top_n = options['top']
        output_file = options['output']
        skip_ai = options['skip_ai']
        skip_pdf = options['skip_pdf']
        require_email = options['require_email']

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('JV MATCHMAKER: Finding Partners for Janet Bray Attwood'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}\n'))

        # Show Janet's profile
        self.stdout.write(f"Client: {JANET_PROFILE['name']}")
        self.stdout.write(f"Company: {JANET_PROFILE['company']}")
        self.stdout.write(f"Niche: {JANET_PROFILE['niche']}")
        self.stdout.write(f"Seeking: {JANET_PROFILE['seeking']}")
        self.stdout.write(f"\nFinding top {top_n} matches...\n")

        # Load and filter partners
        self.stdout.write('Loading Supabase partners...')
        all_partners = list(SupabaseProfile.objects.filter(status='Member'))
        self.stdout.write(f'  Total partners in database: {len(all_partners)}')

        # Filter out competitors and excluded emails
        partners = []
        excluded_competitors = 0
        excluded_emails = 0
        excluded_no_email = 0

        for p in all_partners:
            # Check for excluded emails (internal team, etc.)
            if p.email and p.email.lower() in [e.lower() for e in EXCLUDE_EMAILS]:
                excluded_emails += 1
                continue

            # Check for competing niche (exclude)
            if self._is_competitor(p):
                excluded_competitors += 1
                continue

            # Check for email requirement
            if require_email and not p.email:
                excluded_no_email += 1
                continue

            partners.append(p)

        self.stdout.write(f'  Filtered out: {excluded_competitors} competitors')
        if require_email:
            self.stdout.write(f'  Filtered out: {excluded_no_email} without email')
        self.stdout.write(f'  Candidates to score: {len(partners)}')

        # Pre-process partners for scoring
        self.stdout.write('\nPre-processing partners...')
        partner_data = self._preprocess_partners(partners)

        # Score all partners
        self.stdout.write('Scoring partners...')
        scored_partners = []

        for i, pd in enumerate(partner_data):
            score_result = self._score_partner(pd)
            if score_result:
                scored_partners.append(score_result)

            if (i + 1) % 500 == 0:
                self.stdout.write(f'  Scored {i + 1}/{len(partner_data)}...')

        self.stdout.write(f'  Total scored: {len(scored_partners)}')

        # Sort by score and take top N
        scored_partners.sort(key=lambda x: x['score'], reverse=True)
        top_matches = scored_partners[:top_n]

        self.stdout.write(f'\nTop {len(top_matches)} matches selected')

        # Skip external AI API calls - use built-in personalized outreach instead
        # (AI insights are generated in _generate_personalized_outreach)
        if not skip_ai:
            self.stdout.write('\nUsing built-in personalized outreach generation...')

        # Create output directory if needed
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.stdout.write(f'Created directory: {output_dir}')

        # Write CSV output
        self._write_csv(top_matches, output_file)

        # Generate PDF if not skipped
        pdf_path = None
        if not skip_pdf and top_matches:
            self.stdout.write('\nGenerating PDF report...')
            pdf_path = self._generate_pdf(top_matches, output_dir or 'Chelsea_clients')

        # Print summary
        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('COMPLETE!'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}'))
        self.stdout.write(f'\nCSV Output: {output_file}')
        if pdf_path:
            self.stdout.write(f'PDF Output: {pdf_path}')
        self.stdout.write(f'Total matches: {len(top_matches)}')

        if top_matches:
            scores = [m['score'] for m in top_matches]
            self.stdout.write(f'\nScore Range:')
            self.stdout.write(f'  Highest: {max(scores):.1f}')
            self.stdout.write(f'  Lowest: {min(scores):.1f}')
            self.stdout.write(f'  Average: {sum(scores)/len(scores):.2f}')

            self.stdout.write(f'\nTop 5 Matches:')
            for i, m in enumerate(top_matches[:5], 1):
                self.stdout.write(f'  {i}. {m["name"]} ({m["score"]:.1f}) - {m["niche"][:50] if m["niche"] else "N/A"}...')

    def _is_competitor(self, partner):
        """Check if partner is in a competing niche (should be excluded)."""
        text_to_check = ' '.join(filter(None, [
            partner.niche or '',
            partner.what_you_do or '',
            partner.offering or '',
            partner.business_focus or '',
        ])).lower()

        for keyword, action in COMPETING_NICHES.items():
            if keyword in text_to_check:
                if action == 'exclude':
                    return True

        return False

    def _preprocess_partners(self, partners):
        """Pre-process partners for faster scoring."""
        partner_data = []

        for p in partners:
            # Combine text fields
            all_text = ' '.join(filter(None, [
                p.niche or '',
                p.what_you_do or '',
                p.who_you_serve or '',
                p.offering or '',
                p.seeking or '',
                p.business_focus or '',
                p.bio or '',
            ])).lower()

            # Extract keywords
            keywords = set(re.findall(r'\b\w+\b', all_text))

            partner_data.append({
                'partner': p,
                'name': p.name or '',
                'email': p.email or '',
                'company': p.company or '',
                'website': p.website or '',
                'linkedin': p.linkedin or '',
                'niche': p.niche or '',
                'who_you_serve': p.who_you_serve or '',
                'offering': p.offering or p.what_you_do or '',
                'list_size': p.list_size or 0,
                'all_text': all_text,
                'keywords': keywords,
            })

        return partner_data

    def _score_partner(self, pd):
        """Score a partner based on Janet's criteria."""
        scores = {}
        reasons = []
        flags = []

        # 1. Audience Overlap (35%) - Do they serve coaches, entrepreneurs, seekers?
        audience_score = self._score_audience_overlap(pd)
        scores['audience_overlap'] = max(audience_score, 2.0)

        # 2. Niche Alignment (30%) - Complementary business focus
        niche_score, niche_bonus = self._score_niche_alignment(pd)
        scores['niche_alignment'] = max(niche_score, 2.0)

        # 3. Scale Match (20%) - List size compatibility
        scale_score = self._score_scale_match(pd)
        scores['scale_match'] = max(scale_score, 2.0)

        # 4. Profile Quality (15%) - Has contact info
        quality_score = self._score_profile_quality(pd)
        scores['profile_quality'] = max(quality_score, 2.0)

        # Calculate base harmonic mean
        base_score = self._calculate_harmonic_mean(scores)

        # Apply bonuses
        total_bonus = niche_bonus
        final_score = base_score + total_bonus

        # Cap at 10
        final_score = min(final_score, 10.0)

        # Generate SPECIFIC match reasons (not generic)
        reasons = self._generate_specific_reasons(pd, scores, niche_bonus)

        # Add verification flags for large lists
        list_size = pd['list_size']
        if list_size > LARGE_LIST_THRESHOLD:
            flags.append(f'Verify list ({list_size:,})')

        return {
            'partner': pd['partner'],
            'name': pd['name'],
            'email': pd['email'],
            'company': pd['company'],
            'website': pd['website'],
            'linkedin': pd['linkedin'],
            'niche': pd['niche'],
            'who_they_serve': pd['who_you_serve'],
            'offering': pd['offering'],
            'list_size': pd['list_size'],
            'score': round(final_score, 1),
            'component_scores': scores,
            'niche_bonus': niche_bonus,
            'reason': '; '.join(reasons) if reasons else 'general business alignment',
            'flags': '; '.join(flags) if flags else '',
            # Placeholders for AI insights
            'why_good_fit': '',
            'suggested_approach': '',
            'conversation_starter': '',
        }

    def _generate_specific_reasons(self, pd, scores, niche_bonus):
        """Generate specific, dynamic match reasons based on actual data (Becoming International focus)."""
        reasons = []
        all_text = pd['all_text']
        niche = pd['niche'] or ''
        who_serves = pd['who_you_serve'] or ''

        # Complementary niche reason (Becoming International focus)
        if niche_bonus >= 3:
            if 'speaker' in all_text or 'speaking' in all_text or 'keynote' in all_text:
                reasons.append('speaker training -> international stages')
            elif 'event' in all_text or 'summit' in all_text:
                reasons.append('event producer -> global events')
            elif 'international' in all_text or 'global' in all_text:
                reasons.append('international focus alignment')
            elif 'mastermind' in all_text or 'network' in all_text:
                reasons.append('elite network builder')
            else:
                reasons.append('high-value complementary niche')
        elif niche_bonus >= 2:
            if 'business coach' in all_text:
                reasons.append('coaches entrepreneurs -> global expansion')
            elif 'leadership' in all_text:
                reasons.append('leadership -> international influence')
            elif 'podcast' in all_text or 'media' in all_text:
                reasons.append('media platform -> global reach')
            elif 'author' in all_text or 'publishing' in all_text:
                reasons.append('author -> international platform')
            elif 'visibility' in all_text or 'branding' in all_text:
                reasons.append('visibility expert -> global presence')
            else:
                reasons.append('complementary service offering')

        # Audience-based reason
        if 'speaker' in who_serves.lower():
            reasons.append('serves speakers wanting bigger stages')
        elif 'coach' in who_serves.lower():
            reasons.append('serves coaches ready to scale')
        elif 'entrepreneur' in who_serves.lower():
            reasons.append('serves entrepreneurs seeking growth')

        # Scale reason (be specific about size)
        list_size = pd['list_size']
        if list_size >= 50000:
            reasons.append(f'{list_size:,} list (major reach)')
        elif list_size >= 10000:
            reasons.append(f'{list_size:,} list (solid reach)')
        elif list_size >= 5000:
            reasons.append(f'{list_size:,} list')

        # Contact availability
        if pd['email'] and pd['linkedin']:
            reasons.append('direct contact available')
        elif pd['email']:
            reasons.append('email available')

        # If no specific reasons, extract from niche field
        if not reasons and niche:
            if len(niche) < 50:
                reasons.append(f'niche: {niche}')
            else:
                reasons.append('aligned business focus')

        return reasons[:4]  # Limit to 4 reasons max

    def _score_audience_overlap(self, pd):
        """Score how well partner's audience matches Janet's target."""
        score = 3.0  # Base score

        all_text = pd['all_text']
        who_serves = pd['who_you_serve'].lower() if pd['who_you_serve'] else ''

        # Check for audience keywords
        keyword_matches = 0
        for kw in AUDIENCE_KEYWORDS:
            if kw in all_text or kw in who_serves:
                keyword_matches += 1

        # More matches = higher score
        if keyword_matches >= 5:
            score = 10
        elif keyword_matches >= 3:
            score = 8
        elif keyword_matches >= 2:
            score = 6
        elif keyword_matches >= 1:
            score = 4

        return score

    def _score_niche_alignment(self, pd):
        """Score niche alignment with complementary/competing logic."""
        base_score = 5.0  # Neutral
        niche_bonus = 0

        all_text = pd['all_text']

        # Check for complementary niches (bonuses)
        for niche, bonus in COMPLEMENTARY_NICHES.items():
            if niche in all_text:
                niche_bonus = max(niche_bonus, bonus)
                base_score = max(base_score, 7.0)

        # Check for competing niches (penalties - but not exclude, those are filtered earlier)
        for niche, penalty in COMPETING_NICHES.items():
            if isinstance(penalty, int) and niche in all_text:
                niche_bonus = min(niche_bonus, penalty)  # Apply penalty

        return base_score, niche_bonus

    def _score_scale_match(self, pd):
        """Score based on list size (scale compatibility)."""
        list_size = pd['list_size']

        # Janet wants partners with meaningful list sizes
        if list_size >= 50000:
            return 10  # Large list - great
        elif list_size >= 20000:
            return 9
        elif list_size >= 10000:
            return 8
        elif list_size >= 5000:
            return 7
        elif list_size >= 1000:
            return 5
        elif list_size >= 500:
            return 4
        else:
            return 3  # Small or unknown

    def _score_profile_quality(self, pd):
        """Score based on profile completeness."""
        score = 0

        # Each field adds points
        if pd['email']:
            score += 3
        if pd['website']:
            score += 2
        if pd['linkedin']:
            score += 2
        if pd['niche']:
            score += 1
        if pd['who_you_serve']:
            score += 1
        if pd['offering']:
            score += 1

        return min(score, 10)

    def _calculate_harmonic_mean(self, scores):
        """Calculate weighted harmonic mean of scores."""
        min_score = 1.0  # Floor to prevent extreme penalization

        total_weight = sum(SCORING_WEIGHTS.values())
        weighted_reciprocal_sum = sum(
            SCORING_WEIGHTS[key] / max(scores.get(key, min_score), min_score)
            for key in SCORING_WEIGHTS
        )

        if weighted_reciprocal_sum == 0:
            return 0.0

        return total_weight / weighted_reciprocal_sum

    def _generate_ai_insights(self, matches):
        """Generate AI-enhanced partnership insights for each match."""
        # Try Anthropic first, then fall back to OpenRouter
        anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', None)
        openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', None)

        if anthropic_key:
            self.stdout.write('  Using Anthropic Claude API...')
            api_provider = 'anthropic'
            api_key = anthropic_key
        elif openrouter_key:
            self.stdout.write('  Using OpenRouter API...')
            api_provider = 'openrouter'
            api_key = openrouter_key
        else:
            self.stdout.write(self.style.WARNING(
                '  No API key found (ANTHROPIC_API_KEY or OPENROUTER_API_KEY). Skipping AI insights.'
            ))
            return

        for i, match in enumerate(matches):
            try:
                prompt = self._build_insight_prompt(match)

                if api_provider == 'anthropic':
                    response = self._call_anthropic(api_key, prompt)
                else:
                    ai_config = getattr(settings, 'AI_CONFIG', {})
                    model = ai_config.get('default_model', 'meta-llama/llama-3.2-3b-instruct:free')
                    response = self._call_openrouter(api_key, model, prompt)

                if response:
                    # Parse response into fields
                    self._parse_ai_response(match, response)

                if (i + 1) % 10 == 0:
                    self.stdout.write(f'  Generated insights for {i + 1}/{len(matches)}...')

            except Exception as e:
                self.stdout.write(self.style.WARNING(f'  Error generating insight for {match["name"]}: {e}'))
                continue

        self.stdout.write(f'  AI insights complete')

    def _build_insight_prompt(self, match):
        """Build the prompt for AI insight generation."""
        return f"""You are a JV (joint venture) partnership expert. Generate partnership insights for this match.

CLIENT: Janet Bray Attwood
- Company: Becoming International
- Program: Becoming International Mastermind
- Helps: Coaches and speakers land international speaking engagements and build elite global networks
- Audience: Coaches, speakers, consultants ready to expand internationally
- Credentials: NY Times bestselling author (The Passion Test), 5,000+ facilitators in 65+ countries
- Website: becominginternational.com

POTENTIAL JV PARTNER:
- Name: {match['name']}
- Company: {match['company'] or 'N/A'}
- Niche: {match['niche'] or 'N/A'}
- Who they serve: {match['who_they_serve'] or 'N/A'}
- What they offer: {match['offering'][:200] if match['offering'] else 'N/A'}
- List size: {match['list_size'] or 'Unknown'}

The key insight: Janet helps coaches and speakers GO GLOBAL - land international speaking gigs, build elite networks, and expand their reach worldwide. IDEAL JV PARTNERS have audiences of coaches/speakers who want international expansion.

Generate exactly 3 outputs (keep each to 1-2 sentences):

1. WHY_GOOD_FIT: Why this partner is a good fit for Janet (focus on how their audience of coaches/speakers would benefit from going international)

2. SUGGESTED_APPROACH: How to pitch this JV partnership (cross-promotion, affiliate on mastermind, speaking at their events, joint international summits)

3. CONVERSATION_STARTER: A specific opening line to start the conversation (reference their work if possible)

Format your response exactly like this:
WHY_GOOD_FIT: [your response]
SUGGESTED_APPROACH: [your response]
CONVERSATION_STARTER: [your response]"""

    def _call_openrouter(self, api_key, model, prompt):
        """Call OpenRouter API."""
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://jv-matchmaker.com',
        }

        data = {
            'model': model,
            'messages': [{'role': 'user', 'content': prompt}],
            'max_tokens': 500,
            'temperature': 0.7,
        }

        response = requests.post(
            'https://openrouter.ai/api/v1/chat/completions',
            headers=headers,
            json=data,
            timeout=30,
        )

        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content']
        else:
            return None

    def _parse_ai_response(self, match, response):
        """Parse AI response into individual fields."""
        # Clean up response - remove markdown formatting
        response = response.replace('**', '').replace('*', '')

        # Try to extract each field using flexible patterns
        patterns = {
            'why_good_fit': r'(?:WHY[_\s]?GOOD[_\s]?FIT|1\.)[:\s]+(.+?)(?=(?:SUGGESTED|2\.|\n\n|$))',
            'suggested_approach': r'(?:SUGGESTED[_\s]?APPROACH|2\.)[:\s]+(.+?)(?=(?:CONVERSATION|3\.|\n\n|$))',
            'conversation_starter': r'(?:CONVERSATION[_\s]?STARTER|3\.)[:\s]+(.+?)(?=$)',
        }

        for field, pattern in patterns.items():
            match_result = re.search(pattern, response, re.IGNORECASE | re.DOTALL)
            if match_result:
                value = match_result.group(1).strip()
                # Clean up the value - remove newlines, extra spaces
                value = ' '.join(value.split())
                # Truncate if too long
                if len(value) > 500:
                    value = value[:497] + '...'
                match[field] = value

    def _write_csv(self, matches, output_file):
        """Write matches to CSV file."""
        if not matches:
            self.stdout.write(self.style.WARNING('No matches to write'))
            return

        fieldnames = [
            'rank',
            'match_score',
            'name',
            'company',
            'email',
            'website',
            'linkedin',
            'niche',
            'who_they_serve',
            'offering',
            'list_size',
            'flags',
            'match_reason',
            'why_good_fit',
            'suggested_approach',
            'conversation_starter',
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, m in enumerate(matches, 1):
                # Generate personalized outreach for any empty fields
                outreach = self._generate_personalized_outreach(m)

                writer.writerow({
                    'rank': i,
                    'match_score': m['score'],
                    'name': m['name'],
                    'company': m['company'],
                    'email': m['email'],
                    'website': m['website'],
                    'linkedin': m['linkedin'],
                    'niche': m['niche'],
                    'who_they_serve': m['who_they_serve'],
                    'offering': m['offering'][:500] if m['offering'] else '',
                    'list_size': m['list_size'],
                    'flags': m.get('flags', ''),
                    'match_reason': m['reason'],
                    'why_good_fit': outreach['why_good_fit'],
                    'suggested_approach': outreach['suggested_approach'],
                    'conversation_starter': outreach['conversation_starter'],
                })

        self.stdout.write(f'Wrote {len(matches)} matches to {output_file}')

    def _generate_personalized_outreach(self, match):
        """Generate comprehensive personalized outreach messages based on match data."""
        name = match['name']
        first_name = name.split()[0] if name else 'there'
        company = match['company'] or ''
        niche = match['niche'] or ''
        who_serves = match['who_they_serve'] or ''
        offering = match['offering'] or ''
        list_size = match['list_size'] or 0
        all_text = (niche + ' ' + offering + ' ' + who_serves).lower()

        # Determine partner type for personalization (Becoming International focus)
        partner_type = 'coach'
        audience_descriptor = 'coaches and consultants'
        global_opportunity = 'expand their reach internationally'

        if 'speaker' in all_text or 'speaking' in all_text or 'keynote' in all_text:
            partner_type = 'speaker trainer'
            audience_descriptor = 'speakers and presenters'
            global_opportunity = 'land international speaking engagements'
        elif 'event' in all_text or 'summit' in all_text or 'conference' in all_text:
            partner_type = 'event producer'
            audience_descriptor = 'event attendees and speakers'
            global_opportunity = 'access international stages and audiences'
        elif 'business coach' in all_text or 'entrepreneur' in all_text:
            partner_type = 'business coach'
            audience_descriptor = 'entrepreneurs and coaches'
            global_opportunity = 'scale their business globally'
        elif 'leadership' in all_text or 'executive' in all_text:
            partner_type = 'leadership expert'
            audience_descriptor = 'leaders and executives'
            global_opportunity = 'build international influence'
        elif 'podcast' in all_text or 'media' in all_text:
            partner_type = 'podcaster/media host'
            audience_descriptor = 'content creators and thought leaders'
            global_opportunity = 'reach international audiences'
        elif 'author' in all_text or 'book' in all_text or 'publish' in all_text:
            partner_type = 'author/publisher'
            audience_descriptor = 'authors and thought leaders'
            global_opportunity = 'build a global platform'
        elif 'visibility' in all_text or 'branding' in all_text or 'pr' in all_text:
            partner_type = 'visibility expert'
            audience_descriptor = 'experts seeking visibility'
            global_opportunity = 'get featured on international stages'
        elif 'mastermind' in all_text or 'network' in all_text:
            partner_type = 'community builder'
            audience_descriptor = 'high-level professionals'
            global_opportunity = 'connect with elite international networks'
        elif 'coach' in all_text:
            partner_type = 'coach'
            audience_descriptor = 'coaches and consultants'
            global_opportunity = 'take their coaching practice international'

        # Extract specific work details for personalization
        their_work = company if company and company != name else niche[:60] if niche else 'your work'

        # =========================================================
        # WHY GOOD FIT - Detailed strategic rationale (Becoming International)
        # =========================================================
        if list_size >= 50000:
            why_fit = (
                f"STRATEGIC ALIGNMENT: {name} has built a substantial audience of {list_size:,} subscribers "
                f"in the {partner_type} space. Their {audience_descriptor} are ambitious professionals ready to "
                f"expand their impact—exactly the profile for Becoming International. "
                f"OPPORTUNITY: Many in their audience have mastered their craft domestically but haven't yet tapped "
                f"international markets. Janet's mastermind helps them {global_opportunity}."
            )
        elif list_size >= 10000:
            why_fit = (
                f"STRATEGIC ALIGNMENT: {name}'s {list_size:,} subscribers are {audience_descriptor} actively "
                f"investing in their growth. These are exactly the driven professionals who want to expand globally. "
                f"OPPORTUNITY: Becoming International shows them HOW to {global_opportunity}—"
                f"a perfect next step after working with {first_name}."
            )
        elif 'speaker' in all_text or 'event' in all_text or 'summit' in all_text:
            why_fit = (
                f"STRATEGIC ALIGNMENT: As a {partner_type}, {name} works with {audience_descriptor} who want bigger stages. "
                f"International speaking is the ultimate expansion opportunity for this audience. "
                f"OPPORTUNITY: Becoming International provides the roadmap to {global_opportunity}—"
                f"turning {first_name}'s community into global thought leaders."
            )
        elif 'coach' in all_text:
            why_fit = (
                f"STRATEGIC ALIGNMENT: {name}'s coaching clients are successful professionals ready for the next level. "
                f"Many coaches plateau domestically but never explore international expansion. "
                f"OPPORTUNITY: Becoming International shows {first_name}'s audience how to {global_opportunity}—"
                f"opening entirely new revenue streams and impact opportunities."
            )
        else:
            why_fit = (
                f"STRATEGIC ALIGNMENT: {name}'s focus on {niche[:50] if niche else 'professional development'} "
                f"attracts ambitious professionals seeking growth. "
                f"OPPORTUNITY: Becoming International helps {first_name}'s audience {global_opportunity}—"
                f"a natural next step for high-achievers ready to go global."
            )

        # =========================================================
        # SUGGESTED APPROACH - Specific partnership strategy (Becoming International)
        # =========================================================
        if 'podcast' in all_text or 'media' in all_text:
            approach = (
                f"PARTNERSHIP STRATEGY: Guest Interview & Cross-Promotion\n"
                f"• Janet as podcast guest: 'How Coaches Can Go Global'\n"
                f"• Offer reciprocal promotion to Janet's 65-country network\n"
                f"• Provide {first_name}'s audience with exclusive international expansion resources\n"
                f"• Affiliate partnership on Becoming International Mastermind"
            )
        elif 'summit' in all_text or 'event' in all_text or 'conference' in all_text:
            approach = (
                f"PARTNERSHIP STRATEGY: Speaking & Event Collaboration\n"
                f"• Janet as keynote: 'The Roadmap to International Speaking Success'\n"
                f"• Co-host an international virtual summit\n"
                f"• Offer event promotion to Janet's global facilitator network\n"
                f"• Affiliate partnership on Becoming International Mastermind"
            )
        elif 'speaker' in all_text or 'speaking' in all_text:
            approach = (
                f"PARTNERSHIP STRATEGY: Speaker Development Partnership\n"
                f"• Offer Becoming International as the 'next level' for {first_name}'s speakers\n"
                f"• Joint workshop: 'From Local Expert to International Speaker'\n"
                f"• Affiliate commissions on Mastermind referrals\n"
                f"• Cross-promote to both speaker communities"
            )
        elif list_size >= 20000:
            approach = (
                f"PARTNERSHIP STRATEGY: Cross-Promotion & Affiliate\n"
                f"• Janet promotes {first_name}'s work to her 65-country network\n"
                f"• {first_name} introduces Becoming International to their {list_size:,} subscribers\n"
                f"• Generous affiliate commissions on Mastermind enrollments\n"
                f"• Co-create content: 'Going Global' webinar or challenge"
            )
        elif 'coach' in all_text:
            approach = (
                f"PARTNERSHIP STRATEGY: Coach Expansion Partnership\n"
                f"• Position Becoming International as the growth path for {first_name}'s coaches\n"
                f"• Affiliate partnership with strong commissions\n"
                f"• Joint webinar: 'How to Take Your Coaching Practice International'\n"
                f"• Cross-refer clients ready for global expansion"
            )
        else:
            approach = (
                f"PARTNERSHIP STRATEGY: Collaboration Discovery\n"
                f"• Schedule intro call to explore mutual synergies\n"
                f"• Discuss affiliate partnership on Becoming International Mastermind\n"
                f"• Explore joint content creation opportunities\n"
                f"• Cross-promote to respective audiences"
            )

        # =========================================================
        # FULL OUTREACH EMAIL - Ready to personalize and send (Becoming International)
        # =========================================================

        # Subject line variations based on partner type
        if 'speaker' in all_text or 'speaking' in all_text:
            subject = f"Help your speakers go global?"
        elif 'event' in all_text or 'summit' in all_text:
            subject = "International collaboration idea"
        elif 'business' in all_text or 'entrepreneur' in all_text:
            subject = f"Your {audience_descriptor} ready to go global?"
        elif 'podcast' in all_text:
            subject = "Guest pitch: Going Global as a Coach/Speaker"
        elif 'coach' in all_text:
            subject = "Help your coaches expand internationally?"
        else:
            subject = f"International expansion opportunity for your {audience_descriptor}"

        # Build the email with clean formatting (Becoming International positioning)
        full_email = (
            f"SUBJECT: {subject}\n"
            f"\n"
            f"Hi {first_name},\n"
            f"\n"
            f"I've been following {their_work} and love how you help {audience_descriptor}.\n"
            f"\n"
            f"Here's what I've noticed: Many of your {audience_descriptor} have mastered their craft—"
            f"but they haven't yet tapped into international markets. They're missing out on global speaking "
            f"engagements, elite international networks, and the credibility that comes from a worldwide presence.\n"
            f"\n"
            f"That's exactly what I help with. I'm Janet Attwood, founder of Becoming International—"
            f"a mastermind that helps coaches and speakers land international speaking engagements "
            f"and build elite global networks.\n"
            f"\n"
            f"With facilitators in 65+ countries (from my work with The Passion Test), I've built the connections "
            f"and systems to help ambitious professionals {global_opportunity}.\n"
            f"\n"
            f"I'd love to explore how we might support each other's communities. A few ideas:\n"
            f"\n"
            f"  • I could be a guest on your podcast/webinar: 'How to Take Your Expertise International'\n"
            f"  • We could co-host an international expansion workshop for your audience\n"
            f"  • Affiliate partnership on the Becoming International Mastermind\n"
            f"  • Cross-promote to our respective communities\n"
            f"\n"
            f"Would you be open to a quick call to explore possibilities?\n"
            f"\n"
            f"Book a time here: https://www.becominginternational.com\n"
            f"\n"
            f"To going global together,\n"
            f"Janet Bray Attwood\n"
            f"Founder, Becoming International\n"
            f"NY Times Bestselling Author\n"
            f"Global Network in 65+ Countries"
        )

        return {
            'why_good_fit': match['why_good_fit'] or why_fit,
            'suggested_approach': match['suggested_approach'] or approach,
            'conversation_starter': match['conversation_starter'] or full_email,
        }

    def _generate_pdf(self, matches, output_dir):
        """Generate PDF report using the PDF generator."""
        try:
            # Prepare data in the format expected by PDFGenerator (Becoming International)
            member_data = {
                'participant': JANET_PROFILE['name'],
                'date': datetime.now().strftime("%B %d, %Y"),
                'profile': {
                    'what_you_do': f"Founder of {JANET_PROFILE['company']} - {JANET_PROFILE['offering']}",
                    'who_you_serve': JANET_PROFILE['who_she_serves'],
                    'seeking': JANET_PROFILE['seeking'],
                    'offering': f"Global network in 65+ countries, {JANET_PROFILE['credentials']}",
                    'current_projects': JANET_PROFILE['offering_details'],
                },
                'matches': []
            }

            # Convert matches to PDF format
            for m in matches:
                # Determine urgency based on score
                if m['score'] >= 8:
                    timing = 'This week'
                elif m['score'] >= 6:
                    timing = 'This quarter'
                else:
                    timing = 'Ongoing'

                # Build contact string
                contact_parts = []
                if m['email']:
                    contact_parts.append(m['email'])
                if m['linkedin']:
                    contact_parts.append(m['linkedin'])
                if m['website']:
                    contact_parts.append(m['website'])
                contact = ' | '.join(contact_parts) if contact_parts else '[Not provided]'

                # Generate personalized outreach
                outreach = self._generate_personalized_outreach(m)

                pdf_match = {
                    'name': m['name'],
                    'score': int(m['score'] * 10),  # Convert 0-10 to 0-100
                    'type': 'JV Partnership',
                    'timing': timing,
                    'contact': contact,
                    'fit': outreach['why_good_fit'],
                    'opportunity': outreach['suggested_approach'],
                    'benefits': f"List size: {m['list_size']:,}" if m['list_size'] else 'Mutual audience expansion',
                    'message': outreach['conversation_starter'],
                }
                member_data['matches'].append(pdf_match)

            # Generate PDF
            generator = PDFGenerator(output_dir=output_dir)
            pdf_path = generator.generate(member_data)

            self.stdout.write(self.style.SUCCESS(f'  PDF generated: {pdf_path}'))
            return pdf_path

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  PDF generation failed: {e}'))
            return None
