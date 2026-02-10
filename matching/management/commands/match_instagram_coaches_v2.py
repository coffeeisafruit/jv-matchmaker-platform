"""
Management command to match Instagram coaches to Supabase JV partners
and generate personalized outreach emails.
"""
import csv
import re
from django.core.management.base import BaseCommand
from matching.models import SupabaseProfile


# Common coaching/business keywords for BIO analysis
COACHING_KEYWORDS = [
    'coach', 'coaching', 'mentor', 'mentoring', 'consultant', 'consulting',
    'trainer', 'training', 'speaker', 'author', 'writer', 'podcast',
    'entrepreneur', 'business', 'marketing', 'sales', 'leadership',
    'mindset', 'motivation', 'wellness', 'health', 'fitness', 'nutrition',
    'life', 'career', 'executive', 'relationship', 'spiritual', 'money',
    'wealth', 'finance', 'real estate', 'online', 'digital', 'social media',
    'transformation', 'empowerment', 'success', 'growth', 'personal development',
    'self-help', 'self-improvement', 'mindfulness', 'meditation', 'yoga',
]

# Category to niche mapping for better matching
CATEGORY_NICHE_MAP = {
    'coach': ['coaching', 'coach', 'mentor', 'consultant'],
    'health/beauty': ['health', 'wellness', 'fitness', 'beauty', 'nutrition', 'weight'],
    'motivational speaker': ['motivation', 'speaker', 'inspirational', 'mindset'],
    'entrepreneur': ['business', 'entrepreneur', 'startup', 'founder'],
    'product/service': ['product', 'service', 'business', 'marketing'],
    'personal blog': ['blog', 'content', 'writer', 'author'],
    'fitness': ['fitness', 'health', 'workout', 'training', 'gym'],
    'education': ['education', 'teaching', 'learning', 'training', 'course'],
}

# Patterns for parsing BIO to extract what they do/offer
OFFERING_PATTERNS = [
    r'i help (\w+(?:\s+\w+){0,5})',
    r'i teach (\w+(?:\s+\w+){0,5})',
    r'helping (\w+(?:\s+\w+){0,5})',
    r'teaching (\w+(?:\s+\w+){0,5})',
    r'coach(?:ing)? (?:for )?(\w+(?:\s+\w+){0,5})',
    r'trainer (?:for )?(\w+(?:\s+\w+){0,5})',
    r'specialist in (\w+(?:\s+\w+){0,5})',
    r'expert in (\w+(?:\s+\w+){0,5})',
]

# Patterns for parsing BIO to extract who they serve
AUDIENCE_PATTERNS = [
    r'for (\w+(?:\s+\w+){0,3})',
    r'helping (\w+(?:\s+\w+){0,3})',
    r'empowering (\w+(?:\s+\w+){0,3})',
    r'serving (\w+(?:\s+\w+){0,3})',
    r'work(?:ing)? with (\w+(?:\s+\w+){0,3})',
]

# Audience keywords to look for
AUDIENCE_KEYWORDS = [
    'women', 'men', 'moms', 'mothers', 'dads', 'fathers', 'parents',
    'entrepreneurs', 'business owners', 'coaches', 'leaders', 'executives',
    'professionals', 'athletes', 'students', 'teens', 'adults', 'seniors',
    'couples', 'families', 'teams', 'companies', 'startups',
]

# Scoring weights for harmonic mean calculation
SCORING_WEIGHTS = {
    'niche_alignment': 0.35,      # Category/niche match
    'seeking_offering': 0.35,     # What coach needs vs what partner offers
    'scale_match': 0.20,          # Audience size compatibility
    'keyword_overlap': 0.10,      # BIO keyword overlap
}

EMAIL_TEMPLATE = """Hey {first_name},

Growing your coaching business alone is exhausting. You post content, hope someone notices, and wonder why your list isn't moving.

What if you could partner with established coaches, authors, and podcasters who'd promote you to their audience - while you do the same for them?

For example, we have {partner_name} in our community{partner_description} - someone who could be a great fit for a {coach_category} like you.

That's how the top names in self-improvement grow fast. And on February 11th, David Riklan (founder of SelfGrowth.com with 1M+ subscribers) is showing exactly how to find these partners.

[REGISTER HERE: {{LINK}}]

One member grew their email list from 2K to 5K in 6 months. Another made $64K in a single day from one partnership.

Worth 30 minutes?

- Joe"""


class Command(BaseCommand):
    help = 'Match Instagram coaches to Supabase partners and generate personalized emails'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the Instagram coaches CSV file')
        parser.add_argument(
            '--output',
            type=str,
            default='matched_coaches_output.csv',
            help='Output CSV file path (default: matched_coaches_output.csv)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Limit number of coaches to process (for testing)'
        )
        parser.add_argument(
            '--min-followers',
            type=int,
            default=0,
            help='Minimum follower count to process'
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        output_file = options['output']
        limit = options['limit']
        min_followers = options['min_followers']

        self.stdout.write(f'Loading Supabase partners...')
        partners = list(SupabaseProfile.objects.filter(status='Member'))
        self.stdout.write(f'  Loaded {len(partners)} partners')

        # Pre-process partners for faster matching
        self.partner_data = self._preprocess_partners(partners)

        self.stdout.write(f'Processing coaches from: {csv_file}')

        processed = 0
        matched = 0
        results = []

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                if limit and processed >= limit:
                    break

                # Get follower count
                try:
                    follower_count = int(row.get('FOLLOWER COUNT', 0) or 0)
                except (ValueError, TypeError):
                    follower_count = 0

                if follower_count < min_followers:
                    continue

                # Get coach data
                coach = self._extract_coach_data(row)
                if not coach['email']:
                    continue

                # Find best match
                best_match = self._find_best_match(coach)

                if best_match:
                    # Generate personalized email
                    email_text = self._generate_email(coach, best_match)
                    partner = best_match['partner']

                    results.append({
                        'coach_name': coach['name'],
                        'coach_email': coach['email'],
                        'coach_category': coach['category'],
                        'coach_follower_count': coach['follower_count'],
                        'matched_partner_name': partner.name,
                        'matched_partner_email': partner.email or '',
                        'matched_partner_offering': partner.offering or partner.what_you_do or '',
                        'matched_partner_serves': partner.who_you_serve or '',
                        'matched_partner_niche': partner.niche or '',
                        'match_score': best_match['score'],
                        'match_reason': best_match['reason'],
                        'personalized_email': email_text,
                    })
                    matched += 1

                processed += 1
                if processed % 1000 == 0:
                    self.stdout.write(f'  Processed {processed} coaches, {matched} matched...')

        # Write output CSV
        self._write_output(results, output_file)

        self.stdout.write(self.style.SUCCESS(
            f'\nComplete!\n'
            f'  Processed: {processed}\n'
            f'  Matched: {matched}\n'
            f'  Output: {output_file}'
        ))

    def _preprocess_partners(self, partners):
        """Pre-process partners for faster matching."""
        partner_data = []

        # First pass: collect all keywords to calculate frequency (for IDF)
        all_partner_keywords = []

        for p in partners:
            # Combine all text fields for keyword matching
            all_text = ' '.join(filter(None, [
                p.niche or '',
                p.what_you_do or '',
                p.who_you_serve or '',
                p.offering or '',
                p.seeking or '',
                p.business_focus or '',
            ])).lower()

            # Extract keywords
            keywords = set(re.findall(r'\b\w+\b', all_text))
            all_partner_keywords.append(keywords)

            # Count niches (comma-separated in niche field)
            niche_text = p.niche or ''
            niche_count = len([n.strip() for n in niche_text.split(',') if n.strip()])

            # Calculate breadth penalty: partners with 8+ niches get penalized
            # 1-3 niches = 1.0 (specialist bonus), 4-7 = 0.9, 8-12 = 0.7, 13+ = 0.5
            if niche_count <= 3:
                breadth_multiplier = 1.1  # Specialist bonus
            elif niche_count <= 7:
                breadth_multiplier = 1.0  # Normal
            elif niche_count <= 12:
                breadth_multiplier = 0.75  # Generalist penalty
            else:
                breadth_multiplier = 0.6  # Heavy generalist penalty

            partner_data.append({
                'partner': p,
                'niche': (p.niche or '').lower(),
                'niche_count': niche_count,
                'breadth_multiplier': breadth_multiplier,
                'offering': p.offering or p.what_you_do or '',
                'serves': p.who_you_serve or '',
                'all_text': all_text,
                'keywords': keywords,
                'list_size': p.list_size or 0,
            })

        # Second pass: calculate keyword frequency across all partners (for IDF-like scoring)
        from collections import Counter
        keyword_doc_freq = Counter()
        for kw_set in all_partner_keywords:
            for kw in kw_set:
                keyword_doc_freq[kw] += 1

        # Store for use in matching - keywords appearing in 50%+ of partners are "common"
        self.keyword_rarity = {}
        total_partners = len(partners)
        for kw, freq in keyword_doc_freq.items():
            doc_freq_ratio = freq / total_partners
            if doc_freq_ratio > 0.5:
                self.keyword_rarity[kw] = 0.3  # Very common, low value
            elif doc_freq_ratio > 0.25:
                self.keyword_rarity[kw] = 0.6  # Common
            elif doc_freq_ratio > 0.1:
                self.keyword_rarity[kw] = 1.0  # Normal
            else:
                self.keyword_rarity[kw] = 1.5  # Rare, high value

        return partner_data

    def _extract_coach_data(self, row):
        """Extract and normalize coach data from CSV row."""
        # Get email (prefer extracted from bio)
        email = (
            row.get('EMAIL EXTRACTED FROM BIO', '').strip() or
            row.get('EMAIL', '').strip()
        )

        # Get name
        name = row.get('FULL NAME', '').strip()
        if not name:
            name = row.get('USERNAME', '').strip()

        # Get first name
        first_name = name.split()[0] if name else ''

        # Get category
        category = row.get('CATEGORY', '').strip().lower()

        # Get bio
        bio = row.get('BIO', '').strip().lower()

        # Get follower count
        try:
            follower_count = int(row.get('FOLLOWER COUNT', 0) or 0)
        except (ValueError, TypeError):
            follower_count = 0

        # Extract keywords from bio
        bio_keywords = set(re.findall(r'\b\w+\b', bio)) if bio else set()

        # Parse BIO to extract what they do/offer
        parsed_offering = self._parse_bio_offering(bio)

        # Parse BIO to extract who they serve
        parsed_audience = self._parse_bio_audience(bio)

        # Infer what they might be seeking (coaches usually seek growth, partnerships)
        parsed_seeking = self._infer_seeking(bio, category)

        return {
            'name': name,
            'first_name': first_name,
            'email': email,
            'category': category or 'coach',
            'bio': bio,
            'bio_keywords': bio_keywords,
            'follower_count': follower_count,
            'parsed_offering': parsed_offering,
            'parsed_audience': parsed_audience,
            'parsed_seeking': parsed_seeking,
        }

    def _parse_bio_offering(self, bio):
        """Extract what the coach does/offers from their bio."""
        if not bio:
            return set()

        offerings = set()

        # Try each pattern
        for pattern in OFFERING_PATTERNS:
            matches = re.findall(pattern, bio)
            for match in matches:
                # Clean and add meaningful words
                words = match.lower().split()
                offerings.update(w for w in words if len(w) > 3 and w in COACHING_KEYWORDS)

        # Also look for direct keyword mentions
        for keyword in COACHING_KEYWORDS:
            if keyword in bio:
                offerings.add(keyword)

        return offerings

    def _parse_bio_audience(self, bio):
        """Extract who the coach serves from their bio."""
        if not bio:
            return set()

        audiences = set()

        # Try each pattern
        for pattern in AUDIENCE_PATTERNS:
            matches = re.findall(pattern, bio)
            for match in matches:
                words = match.lower().split()
                audiences.update(w for w in words if w in AUDIENCE_KEYWORDS or len(w) > 4)

        # Also look for direct audience keyword mentions
        for keyword in AUDIENCE_KEYWORDS:
            if keyword in bio:
                audiences.add(keyword)

        return audiences

    def _infer_seeking(self, bio, category):
        """Infer what the coach might be seeking based on their bio and category."""
        seeking = set()

        # Common things coaches seek
        seeking_indicators = {
            'grow': ['growth', 'audience', 'list building'],
            'partner': ['partnerships', 'collaborations', 'jv'],
            'client': ['clients', 'sales', 'revenue'],
            'visibility': ['exposure', 'reach', 'audience'],
            'collab': ['collaborations', 'partnerships'],
            'book': ['speaking', 'visibility'],
            'podcast': ['visibility', 'audience'],
            'dm': ['clients', 'sales'],
            'link in bio': ['sales', 'clients'],
        }

        for indicator, inferred in seeking_indicators.items():
            if indicator in bio:
                seeking.update(inferred)

        # Default seeking for coaches
        if not seeking:
            seeking = {'growth', 'partnerships', 'visibility'}

        return seeking

    def _find_best_match(self, coach):
        """Find the best matching partner for a coach using harmonic mean scoring."""
        best_score = 0
        best_match = None
        best_reason = ''

        coach_category = coach['category']
        coach_bio_keywords = coach['bio_keywords']
        coach_followers = coach['follower_count']
        coach_seeking = coach['parsed_seeking']
        coach_audience = coach['parsed_audience']
        coach_offering = coach['parsed_offering']

        for pd in self.partner_data:
            scores = {}
            reasons = []

            # 1. Category/Niche alignment (0-10 scale)
            niche_score = self._score_category_match(coach_category, pd['niche']) / 5  # Convert 0-50 to 0-10
            scores['niche_alignment'] = max(niche_score, 2.0)  # Floor of 2 - assume some baseline relevance
            if niche_score >= 6:
                reasons.append(f"niche: {pd['partner'].niche or 'general'}")

            # 2. Seeking→Offering match (0-10 scale)
            partner_offering_keywords = pd['keywords']
            seeking_match = len(coach_seeking & partner_offering_keywords)
            # Also check if partner serves similar audience
            partner_serves = (pd['serves'] or '').lower()
            audience_match = sum(1 for a in coach_audience if a in partner_serves)
            # Check if coach's offering overlaps with partner's niche (complementary)
            offering_niche_match = len(coach_offering & pd['keywords'])

            seeking_offering_score = min((seeking_match * 2) + (audience_match * 2) + (offering_niche_match), 10)
            # Base score of 3 if they're both in coaching space
            if len(coach_bio_keywords & pd['keywords']) > 0:
                seeking_offering_score = max(seeking_offering_score, 3)
            scores['seeking_offering'] = max(seeking_offering_score, 2.0)
            if seeking_match > 0:
                matched_terms = list(coach_seeking & partner_offering_keywords)[:2]
                if matched_terms:
                    reasons.append(f"offers: {', '.join(matched_terms)}")

            # 3. Scale compatibility (0-10 scale)
            # Default to neutral score (5) when data is missing - don't penalize unknown
            scale_score = 5.0  # Neutral default
            if pd['list_size'] > 0 and coach_followers > 0:
                ratio = max(pd['list_size'], coach_followers) / max(min(pd['list_size'], coach_followers), 1)
                if ratio <= 2:
                    scale_score = 10  # Very similar
                    reasons.append("similar scale")
                elif ratio <= 5:
                    scale_score = 8  # Compatible
                    reasons.append("compatible scale")
                elif ratio <= 10:
                    scale_score = 6  # Moderate difference
                else:
                    scale_score = 4  # Large gap but still possible
            scores['scale_match'] = scale_score

            # 4. Keyword overlap (0-10 scale) - weighted by keyword rarity
            keyword_overlap = coach_bio_keywords & pd['keywords']
            relevant_overlap = keyword_overlap & set(COACHING_KEYWORDS)

            # Weight keywords by rarity (rare keywords = higher value)
            weighted_keyword_score = 0
            for kw in relevant_overlap:
                rarity = self.keyword_rarity.get(kw, 1.0)
                weighted_keyword_score += 1.5 * rarity

            keyword_score = min(weighted_keyword_score, 10)
            # Base score for being in same general space
            keyword_score = max(keyword_score, 3.0)
            scores['keyword_overlap'] = keyword_score
            if len(relevant_overlap) >= 2:
                reasons.append(f"keywords: {', '.join(list(relevant_overlap)[:2])}")

            # Calculate weighted harmonic mean
            raw_score = self._calculate_harmonic_mean(scores)

            # Apply breadth penalty/bonus - specialists get boosted, generalists penalized
            final_score = raw_score * pd['breadth_multiplier']

            # Update best match
            if final_score > best_score:
                best_score = final_score
                best_reason = '; '.join(reasons) if reasons else 'general coaching fit'
                best_match = {
                    'partner': pd['partner'],
                    'score': round(final_score, 1),
                    'reason': best_reason,
                    'offering_text': self._clean_text(pd['offering']),
                    'serves_text': self._clean_text(pd['serves']),
                    'component_scores': scores,
                }

        return best_match

    def _calculate_harmonic_mean(self, scores):
        """
        Calculate weighted harmonic mean of scores.
        Formula: H = sum(weights) / sum(weight_i / score_i)
        Penalizes low scores more heavily than arithmetic mean.
        """
        min_score = 1.0  # Floor to prevent extreme penalization

        total_weight = sum(SCORING_WEIGHTS.values())
        weighted_reciprocal_sum = sum(
            SCORING_WEIGHTS[key] / max(scores.get(key, min_score), min_score)
            for key in SCORING_WEIGHTS
        )

        if weighted_reciprocal_sum == 0:
            return 0.0

        return total_weight / weighted_reciprocal_sum

    def _score_category_match(self, coach_category, partner_niche):
        """Score the category/niche alignment."""
        if not partner_niche:
            return 0

        # Direct match
        if coach_category in partner_niche or partner_niche in coach_category:
            return 50

        # Check category mapping
        for cat, keywords in CATEGORY_NICHE_MAP.items():
            if cat in coach_category or coach_category in cat:
                for kw in keywords:
                    if kw in partner_niche:
                        return 40
                break

        # Partial keyword match
        coach_words = set(coach_category.split())
        niche_words = set(partner_niche.split())
        if coach_words & niche_words:
            return 25

        return 0

    def _clean_text(self, text):
        """Clean and truncate text for email insertion."""
        if not text:
            return 'offers valuable resources'

        # Remove extra whitespace
        text = ' '.join(text.split())

        # Truncate if too long
        if len(text) > 100:
            text = text[:97] + '...'

        # Make it lowercase for natural reading
        return text.lower()

    def _generate_email(self, coach, match):
        """Generate personalized email for a coach."""
        first_name = coach['first_name'] or 'there'
        category = coach['category'] or 'coaching'

        # Clean up category for display
        category_display = category.replace('/', ' & ').title()
        # Add context word for cleaner reading
        if category_display.lower() == 'coach':
            category_display = 'coach'
        elif 'coach' not in category_display.lower():
            category_display = f'{category_display} professional'

        # Get partner info
        partner = match['partner']
        partner_name = partner.name or 'a fellow coach'

        # Build partner description dynamically based on available data
        partner_description = self._build_partner_description(partner)

        return EMAIL_TEMPLATE.format(
            first_name=first_name,
            partner_name=partner_name,
            partner_description=partner_description,
            coach_category=category_display,
        )

    def _build_partner_description(self, partner):
        """Build a natural description of the partner based on available data."""
        parts = []

        # What they do/offer
        offering = partner.offering or partner.what_you_do
        if offering and len(offering.strip()) > 5:
            offering_clean = offering.strip()
            if len(offering_clean) > 80:
                offering_clean = offering_clean[:77] + '...'
            parts.append(f"who specializes in {offering_clean.lower()}")

        # Who they serve
        serves = partner.who_you_serve
        if serves and len(serves.strip()) > 5:
            serves_clean = serves.strip()
            if len(serves_clean) > 60:
                serves_clean = serves_clean[:57] + '...'
            parts.append(f"works with {serves_clean.lower()}")

        # Niche fallback
        if not parts and partner.niche:
            niche = partner.niche.strip()
            if len(niche) > 60:
                niche = niche[:57] + '...'
            parts.append(f"who focuses on {niche.lower()}")

        # Company fallback
        if not parts and partner.company:
            parts.append(f"from {partner.company}")

        # Build final description
        if not parts:
            return ""  # No good description available

        if len(parts) == 1:
            return f" {parts[0]}"
        else:
            return f" {parts[0]} and {parts[1]}"

    def _write_output(self, results, output_file):
        """Write results to CSV file."""
        if not results:
            self.stdout.write(self.style.WARNING('No results to write'))
            return

        fieldnames = [
            'coach_name',
            'coach_email',
            'coach_category',
            'coach_follower_count',
            'matched_partner_name',
            'matched_partner_email',
            'matched_partner_offering',
            'matched_partner_serves',
            'matched_partner_niche',
            'match_score',
            'match_reason',
            'personalized_email',
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        self.stdout.write(f'  Wrote {len(results)} rows to {output_file}')
