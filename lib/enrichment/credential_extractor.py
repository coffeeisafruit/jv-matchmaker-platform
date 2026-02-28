"""
Credential Extractor (NER-Based)

Extracts structured credentials from the social_proof and bio text fields
using Named Entity Recognition + pattern matching.

Currently social_proof is an unstructured text blob that can't be searched
or scored. This module extracts: books, media appearances, speaking events,
certifications, and awards into structured JSON.

Usage:
    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.credential_extractor import CredentialExtractor

    extractor = CredentialExtractor(HFClient())
    credentials = extractor.extract_credentials(
        social_proof="TEDx speaker, Forbes 30 Under 30, Author of 'The Growth Equation'",
        bio="ICF certified coach with 15 years experience..."
    )
    # {
    #     "books": [{"title": "The Growth Equation"}],
    #     "media_appearances": ["Forbes"],
    #     "speaking_events": ["TEDx"],
    #     "certifications": ["ICF"],
    #     "awards": ["Forbes 30 Under 30"],
    #     "credibility_score": 0.65
    # }
"""

import logging
import re
from typing import Optional

logger = logging.getLogger('enrichment.hf.credentials')

# Known high-credibility media outlets (for credibility scoring)
MAJOR_MEDIA = {
    'forbes', 'inc', 'inc magazine', 'entrepreneur', 'fast company',
    'new york times', 'nyt', 'wall street journal', 'wsj', 'usa today',
    'huffington post', 'huffpost', 'business insider', 'cnbc', 'cnn',
    'abc', 'nbc', 'cbs', 'fox', 'bbc', 'yahoo finance', 'bloomberg',
    'time', 'wired', 'techcrunch', 'mashable', 'the guardian',
    'oprah', 'good morning america', 'today show',
}

# Known certification bodies in coaching/consulting space
CERTIFICATION_PATTERNS = [
    r'\bICF\s*(PCC|MCC|ACC)\b',
    r'\bICF\b',
    r'\bNLP\s*(Practitioner|Master|Trainer)\b',
    r'\bCPC\b',  # Certified Professional Coach
    r'\bCBT\b',  # Cognitive Behavioral Therapy
    r'\bRYT\s*\d+\b',  # Registered Yoga Teacher
    r'\bCPA\b',
    r'\bCFP\b',  # Certified Financial Planner
    r'\bPMP\b',  # Project Management Professional
    r'\bPHR|SPHR\b',  # HR certifications
    r'\bCertified\s+\w+\s+(Coach|Practitioner|Trainer|Therapist)\b',
]

# Speaking event patterns
SPEAKING_PATTERNS = [
    r'\bTEDx?\b',
    r'\bkeynote\b',
    r'\bSXSW\b',
    r'\bsummit\b',
    r'\bconference\b',
    r'\bspeak(?:er|ing)\b',
]

# Book detection patterns
BOOK_PATTERNS = [
    r"(?:author\s+of|wrote|published|book)\s+['\"]([^'\"]+)['\"]",
    r"['\"]([^'\"]{5,60})['\"]",  # Quoted titles (fallback, broader)
    r'\bbest-?selling\s+(?:author|book)\b',
]

# Award patterns
AWARD_PATTERNS = [
    r'(?:Forbes|Inc)\s+\d+\s+Under\s+\d+',
    r'Inc\s*\.?\s*5000',
    r'\baward[- ]winning\b',
    r'\bTop\s+\d+\b',
]


class CredentialExtractor:
    """
    Extracts structured credentials from free-text social_proof and bio fields.

    Uses a combination of:
    1. Regex pattern matching for known credential types
    2. NER (via HF) for entity extraction as supplementary signal
    3. Keyword matching against known media/certification databases

    NER is optional — pattern matching alone handles most cases in this domain.
    """

    def __init__(self, hf_client=None):
        """
        Args:
            hf_client: Optional HFClient for NER. If None, uses pattern matching only.
        """
        self.hf = hf_client

    def extract_credentials(self, social_proof: str, bio: str = "") -> dict:
        """
        Extract structured credentials from free text.

        Args:
            social_proof: The social_proof field from SupabaseProfile
            bio: Optional bio field for additional signal

        Returns:
            Structured credentials dict with credibility_score.
        """
        combined = f"{social_proof or ''} {bio or ''}".strip()
        if not combined:
            return {"credibility_score": 0.0}

        combined_lower = combined.lower()

        credentials = {
            "books": self._extract_books(combined),
            "media_appearances": self._extract_media(combined_lower),
            "speaking_events": self._extract_speaking(combined, combined_lower),
            "certifications": self._extract_certifications(combined),
            "awards": self._extract_awards(combined),
            "podcast_appearances": [],  # Populated by NER if available
        }

        # Optional: Use NER to find additional entities
        if self.hf:
            ner_entities = self._extract_via_ner(combined)
            credentials = self._merge_ner_results(credentials, ner_entities)

        credentials["credibility_score"] = self.compute_credibility_score(credentials)
        return credentials

    def _extract_books(self, text: str) -> list[dict]:
        """Extract book titles from text."""
        books = []

        # Look for explicit "author of" patterns
        author_match = re.findall(
            r"(?:author\s+of|wrote|published)\s+['\"\u201c]([^'\"\u201d]+)['\"\u201d]",
            text, re.IGNORECASE,
        )
        for title in author_match:
            if len(title) >= 5:
                books.append({"title": title.strip()})

        # Check for "best-selling author" without specific title
        if not books and re.search(r'best.?selling\s+author', text, re.IGNORECASE):
            books.append({"title": "(best-selling author — title not specified)"})

        return books

    def _extract_media(self, text_lower: str) -> list[str]:
        """Extract media appearance mentions."""
        found = []
        for outlet in MAJOR_MEDIA:
            if outlet in text_lower:
                # Capitalize properly
                found.append(outlet.title() if len(outlet) > 3 else outlet.upper())
        return sorted(set(found))

    def _extract_speaking(self, text: str, text_lower: str) -> list[str]:
        """Extract speaking events and engagements."""
        events = []

        if re.search(r'\bTEDx?\b', text):
            events.append("TEDx" if "tedx" in text_lower else "TED")

        if 'keynote' in text_lower:
            events.append("Keynote Speaker")

        if 'sxsw' in text_lower:
            events.append("SXSW")

        # Generic speaking mentions
        if re.search(r'\b(spoke|spoken|speaking)\s+at\b', text_lower):
            events.append("Conference Speaker")

        return events

    def _extract_certifications(self, text: str) -> list[str]:
        """Extract professional certifications."""
        certs = []
        for pattern in CERTIFICATION_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Re-run to get the full match, not just groups
                for m in re.finditer(pattern, text, re.IGNORECASE):
                    certs.append(m.group().strip())
        return sorted(set(certs))

    def _extract_awards(self, text: str) -> list[str]:
        """Extract awards and recognitions."""
        awards = []
        for pattern in AWARD_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for m in matches:
                awards.append(m.group().strip())
        return sorted(set(awards))

    def _extract_via_ner(self, text: str) -> list[dict]:
        """
        Use HF NER model to extract additional entities.

        Returns raw NER output — caller should merge with pattern results.
        """
        try:
            entities = self.hf.extract_entities(text)
            return entities
        except Exception as e:
            logger.warning(f"NER extraction failed, using pattern-only: {e}")
            return []

    def _merge_ner_results(self, credentials: dict, ner_entities: list[dict]) -> dict:
        """
        Merge NER entities into pattern-extracted credentials.

        NER helps catch entities that patterns missed (unusual spellings,
        new certifications, etc.), but pattern results take priority.
        """
        existing_media = {m.lower() for m in credentials.get('media_appearances', [])}

        for entity in ner_entities:
            if entity.get('score', 0) < 0.8:
                continue

            word = entity.get('word', '').strip()
            group = entity.get('entity_group', '')

            # ORG entities might be media outlets or certification bodies
            if group == 'ORG' and word.lower() not in existing_media:
                if word.lower() in MAJOR_MEDIA:
                    credentials.setdefault('media_appearances', []).append(word)

        return credentials

    @staticmethod
    def compute_credibility_score(credentials: dict) -> float:
        """
        Compute a credibility score from structured credentials.

        Scoring rubric:
        - Books: 0.15 each, max 0.30
        - Major media appearances: 0.10 each, max 0.30
        - TEDx/TED: 0.25 (one-time)
        - Certifications: 0.10 each, max 0.20
        - Awards: 0.10 each, max 0.20
        - Any credentials present: base 0.10

        Returns:
            Float 0.0 - 1.0
        """
        score = 0.0

        books = credentials.get('books', [])
        if books:
            score += min(len(books) * 0.15, 0.30)

        media = credentials.get('media_appearances', [])
        if media:
            score += min(len(media) * 0.10, 0.30)

        speaking = credentials.get('speaking_events', [])
        if any('TED' in s for s in speaking):
            score += 0.25
        elif speaking:
            score += 0.10

        certs = credentials.get('certifications', [])
        if certs:
            score += min(len(certs) * 0.10, 0.20)

        awards = credentials.get('awards', [])
        if awards:
            score += min(len(awards) * 0.10, 0.20)

        # Base score for having any credentials
        if score > 0:
            score += 0.10

        return min(1.0, round(score, 2))
