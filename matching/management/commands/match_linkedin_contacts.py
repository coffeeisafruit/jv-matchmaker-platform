"""
Management command to match LinkedIn/Apollo contacts to Supabase JV partners.
Matching only - no email generation.
"""
import csv
import re
from django.core.management.base import BaseCommand
from matching.models import SupabaseProfile


# Common coaching/business keywords for matching
COACHING_KEYWORDS = [
    'coach', 'coaching', 'mentor', 'mentoring', 'consultant', 'consulting',
    'trainer', 'training', 'speaker', 'author', 'writer', 'podcast',
    'entrepreneur', 'business', 'marketing', 'sales', 'leadership',
    'mindset', 'motivation', 'wellness', 'health', 'fitness', 'nutrition',
    'life', 'career', 'executive', 'relationship', 'spiritual', 'money',
    'wealth', 'finance', 'real estate', 'online', 'digital', 'social media',
    'transformation', 'empowerment', 'success', 'growth', 'personal development',
    'self-help', 'self-improvement', 'mindfulness', 'meditation', 'yoga',
    'therapist', 'counselor', 'psychologist', 'founder', 'ceo', 'director',
    'strategist', 'advisor', 'expert', 'specialist', 'professional',
]

# Industry to niche mapping
INDUSTRY_NICHE_MAP = {
    'health, wellness & fitness': ['health', 'wellness', 'fitness', 'nutrition', 'weight'],
    'higher education': ['education', 'teaching', 'learning', 'training', 'course'],
    'professional training & coaching': ['coaching', 'training', 'mentor', 'consultant'],
    'mental health care': ['mental health', 'therapy', 'counseling', 'wellness', 'mindset'],
    'e-learning': ['education', 'online', 'course', 'training', 'learning'],
    'marketing & advertising': ['marketing', 'business', 'sales', 'growth'],
    'management consulting': ['consulting', 'business', 'strategy', 'leadership'],
    'financial services': ['finance', 'money', 'wealth', 'investment'],
    'real estate': ['real estate', 'property', 'investment'],
    'media production': ['media', 'podcast', 'content', 'video'],
    'writing & editing': ['author', 'writer', 'content', 'publishing'],
    'non-profit organization management': ['leadership', 'community', 'service'],
    'religious institutions': ['spiritual', 'faith', 'ministry'],
    'alternative medicine': ['wellness', 'health', 'holistic', 'natural'],
    'individual & family services': ['family', 'relationship', 'parenting'],
}

# Title keywords that indicate coaching/consulting roles
TITLE_KEYWORDS = {
    'coach': ['coach', 'coaching'],
    'consultant': ['consultant', 'consulting', 'advisor'],
    'trainer': ['trainer', 'training', 'facilitator'],
    'speaker': ['speaker', 'keynote', 'presenter'],
    'author': ['author', 'writer', 'published'],
    'therapist': ['therapist', 'counselor', 'psychologist', 'lcsw', 'lpc'],
    'founder': ['founder', 'ceo', 'owner', 'principal'],
    'executive': ['executive', 'director', 'vp', 'president'],
    'educator': ['professor', 'teacher', 'instructor', 'educator'],
}

# Scoring weights for harmonic mean calculation
SCORING_WEIGHTS = {
    'niche_alignment': 0.35,      # Industry/niche match
    'title_match': 0.35,          # Title/position relevance
    'scale_match': 0.20,          # Company size compatibility
    'keyword_overlap': 0.10,      # Keyword overlap
}


class Command(BaseCommand):
    help = 'Match LinkedIn/Apollo contacts to Supabase partners (matching only, no emails)'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the LinkedIn/Apollo CSV file')
        parser.add_argument(
            '--output',
            type=str,
            default='matched_linkedin_contacts.csv',
            help='Output CSV file path (default: matched_linkedin_contacts.csv)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Limit number of contacts to process (for testing)'
        )
        parser.add_argument(
            '--require-email',
            action='store_true',
            help='Only process contacts with email addresses'
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        output_file = options['output']
        limit = options['limit']
        require_email = options['require_email']

        self.stdout.write(f'Loading Supabase partners...')
        partners = list(SupabaseProfile.objects.filter(status='Member'))
        self.stdout.write(f'  Loaded {len(partners)} partners')

        # Pre-process partners for faster matching
        self.partner_data = self._preprocess_partners(partners)

        self.stdout.write(f'Processing contacts from: {csv_file}')

        processed = 0
        matched = 0
        skipped_no_email = 0
        results = []

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                if limit and processed >= limit:
                    break

                # Get contact data
                contact = self._extract_contact_data(row)

                # Skip if no email and require_email is set
                if require_email and not contact['email']:
                    skipped_no_email += 1
                    continue

                # Find best match
                best_match = self._find_best_match(contact)

                if best_match:
                    partner = best_match['partner']

                    results.append({
                        'contact_first_name': contact['first_name'],
                        'contact_last_name': contact['last_name'],
                        'contact_full_name': contact['full_name'],
                        'contact_email': contact['email'],
                        'contact_title': contact['title'],
                        'contact_position': contact['position'],
                        'contact_company': contact['company'],
                        'contact_industry': contact['industry'],
                        'contact_employees': contact['employees'],
                        'contact_linkedin_url': contact['linkedin_url'],
                        'matched_partner_name': partner.name,
                        'matched_partner_email': partner.email or '',
                        'matched_partner_niche': partner.niche or '',
                        'matched_partner_offering': partner.offering or partner.what_you_do or '',
                        'matched_partner_serves': partner.who_you_serve or '',
                        'matched_partner_list_size': partner.list_size or 0,
                        'match_score': best_match['score'],
                        'match_reason': best_match['reason'],
                    })
                    matched += 1

                processed += 1
                if processed % 1000 == 0:
                    self.stdout.write(f'  Processed {processed} contacts, {matched} matched...')

        # Write output CSV
        self._write_output(results, output_file)

        self.stdout.write(self.style.SUCCESS(
            f'\nComplete!\n'
            f'  Processed: {processed}\n'
            f'  Matched: {matched}\n'
            f'  Skipped (no email): {skipped_no_email}\n'
            f'  Output: {output_file}'
        ))

        # Show score distribution
        if results:
            scores = [r['match_score'] for r in results]
            self.stdout.write(f'\nScore Distribution:')
            self.stdout.write(f'  Min: {min(scores):.1f}')
            self.stdout.write(f'  Max: {max(scores):.1f}')
            self.stdout.write(f'  Average: {sum(scores)/len(scores):.2f}')

    def _preprocess_partners(self, partners):
        """Pre-process partners for faster matching."""
        partner_data = []
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

            partner_data.append({
                'partner': p,
                'niche': (p.niche or '').lower(),
                'offering': p.offering or p.what_you_do or '',
                'serves': p.who_you_serve or '',
                'all_text': all_text,
                'keywords': keywords,
                'list_size': p.list_size or 0,
            })

        return partner_data

    def _extract_contact_data(self, row):
        """Extract and normalize contact data from CSV row."""
        # Get names
        first_name = row.get('First Name', '').strip()
        last_name = row.get('Last Name', '').strip()
        full_name = f"{first_name} {last_name}".strip()

        # Get email (prefer Email_Merged, then Email, then Email Address)
        email = (
            row.get('Email_Merged', '').strip() or
            row.get('Email', '').strip() or
            row.get('Email Address', '').strip()
        )

        # Get title and position
        title = row.get('Title', '').strip()
        position = row.get('Position', '').strip()
        # Use position as fallback if title is empty
        if not title:
            title = position

        # Get company and industry
        company = row.get('Company', '').strip() or row.get('Company Name', '').strip()
        industry = row.get('Industry', '').strip().lower()

        # Get employee count as scale proxy
        try:
            employees = int(row.get('# Employees', 0) or 0)
        except (ValueError, TypeError):
            employees = 0

        # Get LinkedIn URL
        linkedin_url = row.get('Person Linkedin Url', '').strip() or row.get('URL', '').strip()

        # Extract keywords from title
        title_lower = title.lower()
        title_keywords = set(re.findall(r'\b\w+\b', title_lower))

        # Infer role category from title
        inferred_category = self._infer_category_from_title(title_lower)

        return {
            'first_name': first_name,
            'last_name': last_name,
            'full_name': full_name,
            'email': email,
            'title': title,
            'position': position,
            'company': company,
            'industry': industry,
            'employees': employees,
            'linkedin_url': linkedin_url,
            'title_keywords': title_keywords,
            'inferred_category': inferred_category,
        }

    def _infer_category_from_title(self, title_lower):
        """Infer a category from the contact's title."""
        for category, keywords in TITLE_KEYWORDS.items():
            for kw in keywords:
                if kw in title_lower:
                    return category
        return 'professional'

    def _find_best_match(self, contact):
        """Find the best matching partner for a contact using harmonic mean scoring."""
        best_score = 0
        best_match = None
        best_reason = ''

        contact_industry = contact['industry']
        contact_title_keywords = contact['title_keywords']
        contact_employees = contact['employees']
        contact_category = contact['inferred_category']

        for pd in self.partner_data:
            scores = {}
            reasons = []

            # 1. Industry/Niche alignment (0-10 scale)
            niche_score = self._score_industry_match(contact_industry, pd['niche'])
            scores['niche_alignment'] = max(niche_score, 2.0)  # Floor of 2
            if niche_score >= 6:
                reasons.append(f"industry: {contact_industry or 'general'}")

            # 2. Title/Position match (0-10 scale)
            title_score = self._score_title_match(contact_title_keywords, contact_category, pd)
            scores['title_match'] = max(title_score, 2.0)
            if title_score >= 6:
                reasons.append(f"role: {contact_category}")

            # 3. Scale compatibility (0-10 scale)
            # Use employee count as proxy for contact's "scale"
            # Compare to partner's list_size (rough approximation)
            scale_score = 5.0  # Neutral default
            if pd['list_size'] > 0 and contact_employees > 0:
                # Approximate: 1 employee ≈ 100 list members (rough heuristic)
                contact_scale_proxy = contact_employees * 100
                ratio = max(pd['list_size'], contact_scale_proxy) / max(min(pd['list_size'], contact_scale_proxy), 1)
                if ratio <= 3:
                    scale_score = 10
                    reasons.append("similar scale")
                elif ratio <= 10:
                    scale_score = 7
                elif ratio <= 50:
                    scale_score = 5
                else:
                    scale_score = 3
            scores['scale_match'] = scale_score

            # 4. Keyword overlap (0-10 scale)
            keyword_overlap = contact_title_keywords & pd['keywords']
            relevant_overlap = keyword_overlap & set(COACHING_KEYWORDS)
            keyword_score = min(len(relevant_overlap) * 2, 10)
            keyword_score = max(keyword_score, 3.0)  # Base score
            scores['keyword_overlap'] = keyword_score
            if len(relevant_overlap) >= 2:
                reasons.append(f"keywords: {', '.join(list(relevant_overlap)[:2])}")

            # Calculate weighted harmonic mean
            final_score = self._calculate_harmonic_mean(scores)

            # Update best match
            if final_score > best_score:
                best_score = final_score
                best_reason = '; '.join(reasons) if reasons else 'general professional fit'
                best_match = {
                    'partner': pd['partner'],
                    'score': round(final_score, 1),
                    'reason': best_reason,
                    'component_scores': scores,
                }

        return best_match

    def _calculate_harmonic_mean(self, scores):
        """
        Calculate weighted harmonic mean of scores.
        Formula: H = sum(weights) / sum(weight_i / score_i)
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

    def _score_industry_match(self, contact_industry, partner_niche):
        """Score the industry/niche alignment."""
        if not partner_niche or not contact_industry:
            return 3.0  # Neutral when data missing

        # Direct match
        if contact_industry in partner_niche or partner_niche in contact_industry:
            return 10

        # Check industry mapping
        for industry, keywords in INDUSTRY_NICHE_MAP.items():
            if industry in contact_industry or contact_industry in industry:
                for kw in keywords:
                    if kw in partner_niche:
                        return 8
                break

        # Partial keyword match
        industry_words = set(contact_industry.split())
        niche_words = set(partner_niche.split())
        if industry_words & niche_words:
            return 5

        return 2

    def _score_title_match(self, title_keywords, category, partner_data):
        """Score how well the contact's title matches the partner."""
        score = 3.0  # Base score

        # Check if contact's category aligns with partner's focus
        partner_keywords = partner_data['keywords']

        # Category-based boost
        category_keywords = TITLE_KEYWORDS.get(category, [])
        for kw in category_keywords:
            if kw in partner_data['all_text']:
                score += 3
                break

        # Direct keyword overlap
        overlap = title_keywords & partner_keywords
        coaching_overlap = overlap & set(COACHING_KEYWORDS)
        score += len(coaching_overlap) * 1.5

        return min(score, 10)

    def _write_output(self, results, output_file):
        """Write results to CSV file."""
        if not results:
            self.stdout.write(self.style.WARNING('No results to write'))
            return

        fieldnames = [
            'contact_first_name',
            'contact_last_name',
            'contact_full_name',
            'contact_email',
            'contact_title',
            'contact_position',
            'contact_company',
            'contact_industry',
            'contact_employees',
            'contact_linkedin_url',
            'matched_partner_name',
            'matched_partner_email',
            'matched_partner_niche',
            'matched_partner_offering',
            'matched_partner_serves',
            'matched_partner_list_size',
            'match_score',
            'match_reason',
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        self.stdout.write(f'  Wrote {len(results)} rows to {output_file}')
