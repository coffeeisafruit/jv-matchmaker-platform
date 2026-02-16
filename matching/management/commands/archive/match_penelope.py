"""
One-off management command to match JV partners for Penelope Jane Smith.
Based on match_linkedin_contacts_v2.py but customized for Penelope's specific needs.

This is a standalone command that does NOT modify any existing services.
Can be safely deleted after use.
"""
import csv
import re
import os
import requests
from django.core.management.base import BaseCommand
from django.conf import settings
from matching.models import SupabaseProfile


# =============================================================================
# PENELOPE'S PROFILE (Hard-coded for this one-off task)
# =============================================================================
PENELOPE_PROFILE = {
    'name': 'Penelope Jane Smith',
    'company': 'Real Prosperity Inc',
    'niche': 'Financial freedom for women entrepreneurs',
    'offering': 'Financial Freedom 101 (3-day event), Financial Freedom Accelerator ($20K-$65K)',
    'seeking': 'JV partners, affiliates, podcast guests, summit speakers',
    'who_she_serves': 'Women entrepreneurs, coaches, consultants, 6-7 figure business owners',
    'website': 'realprosperityinc.com',
    'launch_date': 'Feb 10, 2026',
}

# =============================================================================
# SUPPRESS LIST (70+ names - already promoted for Penelope)
# =============================================================================
SUPPRESS_LIST = {
    "lyca moran", "rob goyette", "rachel harrison sund", "chris cade",
    "jake davey", "estie star", "michael whitehouse", "angela hryniuk",
    "karen strang allen", "jeanna gabellini", "laura posey", "stephanie kwong",
    "deborah hurwitz", "brooke mackie", "christina hills", "marcelle siegel",
    "elizabeth purvis", "jeanne sullivan", "sally good", "ana-la-rai sagle",
    "jennifer yagos", "pamela pedrick", "jean border", "annie harmon",
    "atlantis wolf", "martha wilson", "andrew darlow", "tim drown",
    "marianne torrence", "elizabeth watson", "susan shloss", "leah skurdal",
    "michele kasl", "kimberly tara", "anne doherty stephan", "julieta santafe",
    "sonya nagy", "beth rausch", "shariann tom", "jessica freeman",
    "andrea jenson", "robert evans", "rosie stonehill", "ihoby rakotomalala",
    "lenore foster", "leah moore", "lori mcdowell", "maree piplovick",
    "rita losee", "cindy greenway", "grier cooper", "shannon rose",
    "anita anderson", "shiraz baboo", "darlene de la plata", "havalah a collins",
    "kate winch", "kristiina laaksonen", "kristina heagh-avritt", "robin lee",
    "carol liege", "dilyana mileva", "kari alajoki", "michael neely",
    "monifa harris", "victoria buckmann", "rennie gabriel", "travis cody",
    "vrinda normand", "sharla jacobs", "shannon grainger",
    # Additional variations to catch
    "estie starr", "estee star", "estee starr",
}

# =============================================================================
# SCORING CONFIGURATION
# =============================================================================

# Scoring weights (adjusted for Penelope's needs)
SCORING_WEIGHTS = {
    'audience_overlap': 0.35,    # Partner serves women entrepreneurs, coaches
    'niche_alignment': 0.30,     # Complementary business focus
    'scale_match': 0.20,         # List size compatibility
    'profile_quality': 0.15,     # Has email, website, linkedin
}

# Keywords indicating target audience
AUDIENCE_KEYWORDS = [
    'women', 'woman', 'female', 'entrepreneur', 'coach', 'coaching',
    'consultant', 'consulting', 'business owner', 'business women',
    'high-ticket', 'high ticket', 'premium', '6 figure', '7 figure',
    'six figure', 'seven figure', 'heart-centered', 'conscious',
    'service provider', 'service-based', 'transformation',
]

# Women-specific keywords (CRITICAL for Penelope's target audience)
WOMEN_KEYWORDS = [
    'women', 'woman', 'female', 'her', 'she', 'ladies', 'feminine',
    'goddess', 'sisterhood', 'mom', 'mother', 'wife', 'girl boss',
    'womenpreneurs', 'fempreneurs',
]

# List size thresholds
LARGE_LIST_THRESHOLD = 500000  # Flag for verification if > 500K

# Complementary niches (BONUS - they teach making money, Penelope teaches growing it)
COMPLEMENTARY_NICHES = {
    # High bonus (+3)
    'business coach': 3, 'business coaching': 3,
    'marketing': 3, 'marketing coach': 3, 'marketing strategist': 3,
    'sales': 3, 'sales coach': 3, 'sales training': 3,
    'client acquisition': 3, 'lead generation': 3,
    # Medium bonus (+2)
    'speaking': 2, 'speaker': 2, 'public speaking': 2, 'speaker training': 2,
    'visibility': 2, 'podcast': 2, 'podcaster': 2,
    'mindset': 2, 'mindset coach': 2, 'success mindset': 2,
    'productivity': 2, 'systems': 2, 'operations': 2,
    'women leadership': 2, 'women empowerment': 2, 'female leadership': 2,
    'manifestation': 2, 'abundance': 2, 'law of attraction': 2,
    'personal development': 2, 'transformation': 2,
    'copywriting': 2, 'content creation': 2,
    'branding': 2, 'brand strategist': 2,
}

# Competing niches (EXCLUDE or PENALTY - same space as Penelope)
COMPETING_NICHES = {
    # EXCLUDE (return None)
    'financial freedom': 'exclude',
    'wealth coach': 'exclude',
    'wealth coaching': 'exclude',
    'money coach': 'exclude',
    'money coaching': 'exclude',
    'financial coach': 'exclude',
    'financial coaching': 'exclude',
    'wealth management': 'exclude',
    'prosperity coach': 'exclude',  # Same as Penelope
    # Penalty (-3)
    'investing': -3, 'investment': -3,
    'trading': -3, 'trader': -3,
    'crypto': -3, 'cryptocurrency': -3,
    'real estate investing': -3,
    'stock market': -3,
}


class Command(BaseCommand):
    help = 'Find top 50 JV partner matches for Penelope Jane Smith'

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
            default='Chelsea_clients/penelope_matches.csv',
            help='Output CSV file path'
        )
        parser.add_argument(
            '--skip-ai',
            action='store_true',
            help='Skip AI insight generation (faster, but no personalized insights)'
        )

    def handle(self, *args, **options):
        top_n = options['top']
        output_file = options['output']
        skip_ai = options['skip_ai']

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('JV MATCHMAKER: Finding Partners for Penelope Jane Smith'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}\n'))

        # Show Penelope's profile
        self.stdout.write(f"Client: {PENELOPE_PROFILE['name']}")
        self.stdout.write(f"Company: {PENELOPE_PROFILE['company']}")
        self.stdout.write(f"Niche: {PENELOPE_PROFILE['niche']}")
        self.stdout.write(f"Seeking: {PENELOPE_PROFILE['seeking']}")
        self.stdout.write(f"Launch Date: {PENELOPE_PROFILE['launch_date']}")
        self.stdout.write(f"\nSuppressed names: {len(SUPPRESS_LIST)}")
        self.stdout.write(f"Finding top {top_n} matches...\n")

        # Load and filter partners
        self.stdout.write('Loading Supabase partners...')
        all_partners = list(SupabaseProfile.objects.filter(status='Member'))
        self.stdout.write(f'  Total partners in database: {len(all_partners)}')

        # Filter out suppressed names and competitors
        partners = []
        suppressed_count = 0
        excluded_competitors = 0

        for p in all_partners:
            name_lower = (p.name or '').lower().strip()

            # Check suppress list
            if self._is_suppressed(name_lower):
                suppressed_count += 1
                continue

            # Check for competing niche (exclude)
            if self._is_competitor(p):
                excluded_competitors += 1
                continue

            partners.append(p)

        self.stdout.write(f'  Filtered out: {suppressed_count} suppressed, {excluded_competitors} competitors')
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

        # Generate AI insights if not skipped
        if not skip_ai and top_matches:
            self.stdout.write('\nGenerating AI-enhanced partnership insights...')
            self._generate_ai_insights(top_matches)

        # Create output directory if needed
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.stdout.write(f'Created directory: {output_dir}')

        # Write CSV output
        self._write_csv(top_matches, output_file)

        # Print summary
        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('COMPLETE!'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}'))
        self.stdout.write(f'\nOutput: {output_file}')
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

    def _is_suppressed(self, name_lower):
        """Check if name is in suppress list."""
        # Direct match
        if name_lower in SUPPRESS_LIST:
            return True

        # Check for partial matches (first + last name)
        for suppressed in SUPPRESS_LIST:
            # Check if all parts of suppressed name appear in the name
            parts = suppressed.split()
            if all(part in name_lower for part in parts):
                return True

        return False

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
        """Score a partner based on Penelope's criteria."""
        scores = {}
        reasons = []
        flags = []

        # 1. Audience Overlap (35%) - Do they serve women entrepreneurs, coaches?
        audience_score, women_bonus = self._score_audience_overlap(pd)
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
        total_bonus = niche_bonus + women_bonus
        final_score = base_score + total_bonus

        # Cap at 10
        final_score = min(final_score, 10.0)

        # Generate SPECIFIC match reasons (not generic)
        reasons = self._generate_specific_reasons(pd, scores, niche_bonus, women_bonus)

        # Add verification flags for large lists
        list_size = pd['list_size']
        if list_size > LARGE_LIST_THRESHOLD:
            flags.append(f'⚠️ Verify list ({list_size:,})')

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
            'women_bonus': women_bonus,
            'niche_bonus': niche_bonus,
            'reason': '; '.join(reasons) if reasons else 'general business alignment',
            'flags': '; '.join(flags) if flags else '',
            # Placeholders for AI insights
            'why_good_fit': '',
            'suggested_approach': '',
            'conversation_starter': '',
        }

    def _generate_specific_reasons(self, pd, scores, niche_bonus, women_bonus):
        """Generate specific, dynamic match reasons based on actual data."""
        reasons = []
        all_text = pd['all_text']
        niche = pd['niche'] or ''
        who_serves = pd['who_you_serve'] or ''

        # Women-focused reason (PRIORITY)
        if women_bonus >= 2:
            if 'women entrepreneur' in all_text:
                reasons.append('serves women entrepreneurs')
            elif 'women' in all_text and 'coach' in all_text:
                reasons.append('coaches women')
            elif 'female' in all_text:
                reasons.append('female-focused audience')
            else:
                reasons.append('women-centric business')

        # Complementary niche reason (be specific about WHAT niche)
        if niche_bonus >= 3:
            if 'business coach' in all_text:
                reasons.append('business coaching → wealth building')
            elif 'marketing' in all_text:
                reasons.append('marketing expertise → revenue growth')
            elif 'sales' in all_text:
                reasons.append('sales training → income generation')
            elif 'lead generation' in all_text:
                reasons.append('lead gen → client acquisition')
            else:
                reasons.append('complementary expertise')
        elif niche_bonus >= 2:
            if 'speaker' in all_text or 'speaking' in all_text:
                reasons.append('visibility/speaking platform')
            elif 'mindset' in all_text:
                reasons.append('mindset work → abundance focus')
            elif 'podcast' in all_text:
                reasons.append('podcast platform for exposure')
            elif 'branding' in all_text:
                reasons.append('brand strategy expertise')
            else:
                reasons.append('related service offering')

        # Scale reason (be specific about size)
        list_size = pd['list_size']
        if list_size >= 50000:
            reasons.append(f'{list_size:,} list (large reach)')
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
            # Extract key term from niche
            niche_lower = niche.lower()
            if len(niche) < 50:
                reasons.append(f'niche: {niche}')
            else:
                reasons.append('aligned business focus')

        return reasons[:4]  # Limit to 4 reasons max

    def _score_audience_overlap(self, pd):
        """Score how well partner's audience matches Penelope's target."""
        score = 3.0  # Base score
        women_bonus = 0

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

        # WOMEN-SPECIFIC BONUS (Critical for Penelope)
        women_keyword_count = 0
        for kw in WOMEN_KEYWORDS:
            if kw in all_text or kw in who_serves:
                women_keyword_count += 1

        # Apply women-specific bonus
        if women_keyword_count >= 3:
            women_bonus = 3  # Strong women focus
        elif women_keyword_count >= 2:
            women_bonus = 2  # Moderate women focus
        elif women_keyword_count >= 1:
            women_bonus = 1  # Some women focus

        # Bonus for explicitly serving women entrepreneurs
        if 'women entrepreneur' in who_serves or 'female entrepreneur' in who_serves:
            women_bonus = max(women_bonus, 2)  # At least +2

        return score, women_bonus

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

        # Penelope wants partners with meaningful list sizes
        # Ideal: 5K - 100K (similar or slightly larger than typical JV partner)
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
        api_key = settings.OPENROUTER_API_KEY

        if not api_key:
            self.stdout.write(self.style.WARNING(
                '  No OPENROUTER_API_KEY found. Skipping AI insights.'
            ))
            return

        model = settings.AI_CONFIG.get('default_model', 'meta-llama/llama-3.2-3b-instruct:free')

        for i, match in enumerate(matches):
            try:
                prompt = self._build_insight_prompt(match)
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

CLIENT: Penelope Jane Smith
- Company: Real Prosperity Inc
- Teaches: Financial freedom and wealth building for women entrepreneurs
- Frontend Offer: Financial Freedom 101 (3-day virtual event)
- Backend: Financial Freedom Accelerator ($20K-$65K)
- Commission: $50/registration, $1K-$3.5K/sale

POTENTIAL JV PARTNER:
- Name: {match['name']}
- Company: {match['company'] or 'N/A'}
- Niche: {match['niche'] or 'N/A'}
- Who they serve: {match['who_they_serve'] or 'N/A'}
- What they offer: {match['offering'][:200] if match['offering'] else 'N/A'}
- List size: {match['list_size'] or 'Unknown'}

The key insight: Penelope teaches women entrepreneurs how to BUILD WEALTH after they make money. This partner likely teaches something that comes BEFORE (making money through {match['niche'] or 'their expertise'}), making them complementary.

Generate exactly 3 outputs (keep each to 1-2 sentences):

1. WHY_GOOD_FIT: Why this partner is a good fit for Penelope (focus on complementary value - their audience needs what Penelope teaches)

2. SUGGESTED_APPROACH: How to pitch this JV partnership (what's in it for them - $50/reg, $1K-3.5K/sale, lifetime commissions)

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
        import re

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
            'women_focus',
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
                # Women focus indicator
                women_bonus = m.get('women_bonus', 0)
                if women_bonus >= 3:
                    women_focus = '★★★'
                elif women_bonus >= 2:
                    women_focus = '★★'
                elif women_bonus >= 1:
                    women_focus = '★'
                else:
                    women_focus = ''

                writer.writerow({
                    'rank': i,
                    'match_score': m['score'],
                    'women_focus': women_focus,
                    'name': m['name'],
                    'company': m['company'],
                    'email': m['email'],
                    'website': m['website'],
                    'linkedin': m['linkedin'],
                    'niche': m['niche'],
                    'who_they_serve': m['who_they_serve'],
                    'offering': m['offering'][:500] if m['offering'] else '',  # Truncate long offerings
                    'list_size': m['list_size'],
                    'flags': m.get('flags', ''),
                    'match_reason': m['reason'],
                    'why_good_fit': m['why_good_fit'],
                    'suggested_approach': m['suggested_approach'],
                    'conversation_starter': m['conversation_starter'],
                })

        self.stdout.write(f'Wrote {len(matches)} matches to {output_file}')
