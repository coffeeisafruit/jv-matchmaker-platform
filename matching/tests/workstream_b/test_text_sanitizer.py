"""Tests for TextSanitizer class."""

import pytest
from matching.enrichment.text_sanitizer import TextSanitizer


# --- sanitize() tests ---

class TestSanitize:
    def test_empty_string_returns_empty(self):
        assert TextSanitizer.sanitize('') == ''

    def test_em_dashes_replaced(self):
        result = TextSanitizer.sanitize('word\u2014word')
        assert result == 'word-word'

    def test_smart_quotes_replaced(self):
        result = TextSanitizer.sanitize('\u201cHello\u201d \u2018world\u2019')
        assert result == '"Hello" \'world\''

    def test_zero_width_chars_removed(self):
        result = TextSanitizer.sanitize('hel\u200blo\ufeffle')
        assert result == 'hellole'
        assert '\u200b' not in result
        assert '\ufeff' not in result

    def test_full_pipeline(self):
        """Sanitize should replace unicode, clean non-ascii, normalize whitespace, and strip."""
        raw = '  \u201cCaf\u00e9\u201d \u2014  extra   spaces\n\n\n\nnewlines  '
        result = TextSanitizer.sanitize(raw)
        assert '\u201c' not in result
        assert '\u201d' not in result
        assert '\u2014' not in result
        # Multiple spaces should be collapsed
        assert '  ' not in result
        # 3+ newlines should be collapsed to 2
        assert '\n\n\n' not in result
        # Should be stripped
        assert result == result.strip()


# --- truncate_safe() tests ---

class TestTruncateSafe:
    def test_text_shorter_than_limit_unchanged(self):
        text = 'Short text'
        assert TextSanitizer.truncate_safe(text, max_length=50) == text

    def test_truncates_at_word_boundary(self):
        text = 'The quick brown fox jumps over the lazy dog'
        result = TextSanitizer.truncate_safe(text, max_length=20)
        assert len(result) <= 20
        assert result.endswith('...')
        # Should not cut in the middle of a word
        without_suffix = result[:-3]
        assert not without_suffix.endswith(' ')  # trailing space would be trimmed

    def test_no_space_edge_case(self):
        text = 'Superlongwordwithoutanyspaces'
        result = TextSanitizer.truncate_safe(text, max_length=10)
        assert len(result) <= 10

    def test_empty_text(self):
        assert TextSanitizer.truncate_safe('', max_length=10) == ''


# --- capitalize_bullet() tests ---

class TestCapitalizeBullet:
    def test_star_bullet_capitalized(self):
        assert TextSanitizer.capitalize_bullet('* access') == '* Access'

    def test_dash_bullet_capitalized(self):
        assert TextSanitizer.capitalize_bullet('- item') == '- Item'

    def test_plain_lowercase_capitalized(self):
        assert TextSanitizer.capitalize_bullet('lowercase') == 'Lowercase'

    def test_empty_returns_empty(self):
        assert TextSanitizer.capitalize_bullet('') == ''


# --- format_bullet_list() tests ---

class TestFormatBulletList:
    def test_formats_multiple_items(self):
        items = ['first item', 'second item', 'third item']
        result = TextSanitizer.format_bullet_list(items)
        lines = result.strip().split('\n')
        assert len(lines) == 3
        assert lines[0] == '* First item'
        assert lines[1] == '* Second item'
        assert lines[2] == '* Third item'

    def test_handles_unicode_in_items(self):
        items = ['caf\u00e9 partner\u2014great', '\u201csmart quote\u201d entry']
        result = TextSanitizer.format_bullet_list(items)
        # Unicode should be sanitized
        assert '\u2014' not in result
        assert '\u201c' not in result
        assert '\u201d' not in result


# --- _transliterate() tests ---

class TestTransliterate:
    def test_e_acute(self):
        assert TextSanitizer._transliterate('\u00e9') == 'e'

    def test_n_tilde(self):
        assert TextSanitizer._transliterate('\u00f1') == 'n'

    def test_eszett(self):
        assert TextSanitizer._transliterate('\u00df') == 'ss'


# --- _normalize_whitespace() tests ---

class TestNormalizeWhitespace:
    def test_multiple_spaces_collapsed(self):
        result = TextSanitizer._normalize_whitespace('hello    world')
        assert result == 'hello world'

    def test_excess_newlines_collapsed(self):
        result = TextSanitizer._normalize_whitespace('a\n\n\n\n\nb')
        assert result == 'a\n\nb'


# --- clean_list_field() tests ---

class TestCleanListField:
    def test_leading_comma_stripped(self):
        assert TextSanitizer.clean_list_field(', Audio Books, Content') == 'Audio Books, Content'

    def test_leading_semicolon_stripped(self):
        assert TextSanitizer.clean_list_field('; First, Second') == 'First, Second'

    def test_double_comma_fixed(self):
        assert TextSanitizer.clean_list_field('A,, B, C') == 'A, B, C'

    def test_spacing_normalized(self):
        assert TextSanitizer.clean_list_field('A ,B,  C') == 'A, B, C'

    def test_empty_returns_empty(self):
        assert TextSanitizer.clean_list_field('') == ''
        assert TextSanitizer.clean_list_field('  ,  ') == ''


# --- validate_company() tests ---

class TestValidateCompany:
    def test_real_company_passes(self):
        assert TextSanitizer.validate_company('SelfGrowth.com') == 'SelfGrowth.com'

    def test_generic_blocked(self):
        assert TextSanitizer.validate_company('App Development') == ''
        assert TextSanitizer.validate_company('coaching') == ''
        assert TextSanitizer.validate_company('Personal Development') == ''

    def test_name_match_blocked(self):
        assert TextSanitizer.validate_company('John Smith', name='John Smith') == ''

    def test_empty_returns_empty(self):
        assert TextSanitizer.validate_company('') == ''

    def test_legitimate_companies_with_generic_words(self):
        # Companies that contain generic words but are real names
        assert TextSanitizer.validate_company('The Effective Way™') == 'The Effective Way™'
        assert TextSanitizer.validate_company('Beyond the Dawn Digital') == 'Beyond the Dawn Digital'


# --- validate_bio() tests ---

class TestValidateBio:
    def test_good_bio_passes(self):
        bio = 'David Riklan is the founder of SelfGrowth.com, specializing in personal growth.'
        assert TextSanitizer.validate_bio(bio, 'David Riklan') == bio

    def test_offering_as_role_blocked(self):
        assert TextSanitizer.validate_bio('David is a podcast at SelfGrowth.com', 'David') == ''
        assert TextSanitizer.validate_bio('Jane is a webinar platform', 'Jane') == ''

    def test_serves_as_offering_blocked(self):
        assert TextSanitizer.validate_bio('Shannon serves as Public Speaking at SNS', 'Shannon') == ''

    def test_legitimate_serves_as_passes(self):
        assert TextSanitizer.validate_bio('Jane serves as CEO at Acme Corp', 'Jane') != ''

    def test_empty_returns_empty(self):
        assert TextSanitizer.validate_bio('') == ''

    def test_they_specialize_blocked(self):
        assert TextSanitizer.validate_bio('They specialize in management consulting and leadership training.', 'Test Corp') == ''

    def test_they_provide_blocked(self):
        assert TextSanitizer.validate_bio('They provide coaching services for entrepreneurs.', 'Test Corp') == ''

    def test_the_company_blocked(self):
        assert TextSanitizer.validate_bio('The company specializes in digital marketing solutions.', 'Test Corp') == ''

    def test_their_services_blocked(self):
        assert TextSanitizer.validate_bio('Their services include web design and branding.', 'Test Corp') == ''

    def test_they_are_dedicated_blocked(self):
        assert TextSanitizer.validate_bio('They are dedicated to delivering exceptional results for their clients.', 'Test Corp') == ''

    def test_they_are_committed_blocked(self):
        assert TextSanitizer.validate_bio('They are committed to providing world-class coaching.', 'Test Corp') == ''


# --- validate_match_reason() tests ---

class TestValidateMatchReason:
    def test_keyword_arrays_stripped(self):
        reason = "Keyword match: ['Speaking'] ↔ ['Article Submission Sites']"
        assert TextSanitizer.validate_match_reason(reason) == ''

    def test_warning_emoji_stripped(self):
        reason = 'Good match. ⚠️ Based on profile data'
        result = TextSanitizer.validate_match_reason(reason)
        assert '⚠️' not in result
        assert 'Good match' in result

    def test_clean_reason_unchanged(self):
        reason = 'Shared focus on wellness and personal development'
        assert TextSanitizer.validate_match_reason(reason) == reason

    def test_score_references_stripped(self):
        reason = 'Good match (score: 73.2) for collaboration'
        result = TextSanitizer.validate_match_reason(reason)
        assert 'score' not in result
        assert 'Good match' in result

    def test_empty_returns_empty(self):
        assert TextSanitizer.validate_match_reason('') == ''
