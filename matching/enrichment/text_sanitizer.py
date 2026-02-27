"""
Text sanitization for PDF rendering.

Fixes Unicode characters, word-safe truncation, capitalization, and formatting.
Extracted from match_enrichment.py for single-responsibility.
"""

import re
from typing import List


class TextSanitizer:
    """
    Sanitizes text for PDF rendering.
    Fixes: Unicode chars, word truncation, capitalization, formatting.
    """

    # Unicode characters that don't render well in PDFs
    UNICODE_REPLACEMENTS = {
        '\u2014': '-',   # em-dash
        '\u2013': '-',   # en-dash
        '\u2212': '-',   # minus sign
        '\u2010': '-',   # hyphen
        '\u2011': '-',   # non-breaking hyphen
        '\u00ad': '-',   # soft hyphen
        '\u2018': "'",   # left single quote
        '\u2019': "'",   # right single quote
        '\u201c': '"',   # left double quote
        '\u201d': '"',   # right double quote
        '\u2026': '...',  # ellipsis
        '\u00a0': ' ',   # non-breaking space
        '\u200b': '',    # zero-width space
        '\u200c': '',    # zero-width non-joiner
        '\u200d': '',    # zero-width joiner
        '\ufeff': '',    # byte order mark
        '\u25a0': '-',   # black square (fallback for bad dashes)
        '\u25aa': '-',   # small black square
        '\u25cf': '*',   # black circle -> bullet
        '\u2022': '*',   # bullet point
        '\u00b7': '*',   # middle dot
    }

    @classmethod
    def sanitize(cls, text: str) -> str:
        """Full sanitization pipeline for any text."""
        if not text:
            return ''

        # Step 1: Replace known problematic Unicode
        result = cls._replace_unicode(text)

        # Step 2: Remove any remaining non-ASCII that could cause issues
        result = cls._clean_non_ascii(result)

        # Step 3: Normalize whitespace
        result = cls._normalize_whitespace(result)

        return result.strip()

    @classmethod
    def _replace_unicode(cls, text: str) -> str:
        """Replace known problematic Unicode characters."""
        for unicode_char, replacement in cls.UNICODE_REPLACEMENTS.items():
            text = text.replace(unicode_char, replacement)
        return text

    @classmethod
    def _clean_non_ascii(cls, text: str) -> str:
        """Remove non-ASCII chars that might cause rendering issues."""
        # Keep standard printable ASCII, newlines, and tabs
        cleaned = []
        for char in text:
            if ord(char) < 128 or char in '\n\t':
                cleaned.append(char)
            elif ord(char) >= 128:
                # Try to transliterate common extended chars
                cleaned.append(cls._transliterate(char))
        return ''.join(cleaned)

    @classmethod
    def _transliterate(cls, char: str) -> str:
        """Transliterate common extended Latin characters."""
        transliterations = {
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'ã': 'a',
            'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'õ': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
            'ñ': 'n', 'ç': 'c', 'ß': 'ss',
        }
        return transliterations.get(char, '')

    @classmethod
    def _normalize_whitespace(cls, text: str) -> str:
        """Normalize whitespace without breaking intentional line breaks."""
        # Replace multiple spaces with single space
        text = re.sub(r'[ \t]+', ' ', text)
        # Replace more than 2 consecutive newlines with 2
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text

    @classmethod
    def truncate_safe(cls, text: str, max_length: int, suffix: str = '...') -> str:
        """
        Truncate text at word boundary - NEVER cut words mid-way.
        """
        if not text or len(text) <= max_length:
            return text

        # Find the last space before max_length
        truncate_at = max_length - len(suffix)

        # Look for word boundary
        last_space = text.rfind(' ', 0, truncate_at)

        if last_space == -1:
            # No space found, truncate at max_length (rare edge case)
            return text[:truncate_at] + suffix

        return text[:last_space] + suffix

    @classmethod
    def capitalize_bullet(cls, text: str) -> str:
        """Capitalize the first letter after a bullet point."""
        if not text:
            return text

        # Handle bullet at start of string
        if text.startswith('* ') or text.startswith('- '):
            if len(text) > 2:
                return text[:2] + text[2].upper() + text[3:]

        # Capitalize first character if it's lowercase
        if text[0].islower():
            return text[0].upper() + text[1:]

        return text

    @classmethod
    def format_bullet_list(cls, items: List[str], bullet: str = '*') -> str:
        """Format a list of items as properly capitalized bullets."""
        formatted = []
        for item in items:
            item = cls.sanitize(item)
            item = cls.capitalize_bullet(item)
            formatted.append(f"{bullet} {item}")
        return '\n'.join(formatted)
