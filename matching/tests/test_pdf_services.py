"""
Tests for matching/pdf_services/ package

Covers:
- DataValidator: validate_member_data(), safe_get()
- pdf_components: detect_urgency(), detect_collaboration_type(), get_score_color(),
  parse_score(), safe_get()
- PDFGenerator: generate(), generate_to_bytes()
- pdf_styles: create_pdf_styles(), COLORS

All tests are pure Python — no database access required.
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.test_settings')

import pytest

from matching.pdf_services.data_validator import DataValidator, ValidationError
from matching.pdf_services.pdf_components import (
    detect_urgency,
    detect_collaboration_type,
    get_score_color,
    parse_score,
    safe_get,
)
from matching.pdf_services.pdf_generator import PDFGenerator, PDFGenerationError
from matching.pdf_services.pdf_styles import create_pdf_styles, COLORS


# =============================================================================
# HELPER: Valid member data factory
# =============================================================================

def _make_valid_member_data():
    return {
        'participant': 'Jane Smith',
        'date': 'February 14, 2026',
        'profile': {
            'what_you_do': 'Business coaching and consulting',
            'who_you_serve': 'Entrepreneurs',
            'seeking': 'JV partners for launches',
            'offering': 'Email list access',
        },
        'matches': [
            {
                'name': 'Bob Johnson',
                'score': '85/100',
                'type': 'Joint Venture',
                'message': 'Hi Bob, I would love to connect...',
                'contact': 'bob@example.com',
                'fit': 'Great audience alignment',
                'opportunity': 'Cross-promotion',
                'benefits': 'Mutual list growth',
                'timing': 'This quarter',
            },
        ],
    }


# =============================================================================
# DataValidator.validate_member_data
# =============================================================================

class TestValidateMemberData:
    """Tests for DataValidator.validate_member_data()"""

    def test_valid_data_passes(self):
        """Complete valid data returns True."""
        data = _make_valid_member_data()
        assert DataValidator.validate_member_data(data) is True

    def test_missing_participant_raises(self):
        """Missing 'participant' key raises ValidationError."""
        data = _make_valid_member_data()
        del data['participant']
        with pytest.raises(ValidationError, match="Missing participant name"):
            DataValidator.validate_member_data(data)

    def test_empty_participant_raises(self):
        """Empty string participant raises ValidationError."""
        data = _make_valid_member_data()
        data['participant'] = ''
        with pytest.raises(ValidationError, match="Missing participant name"):
            DataValidator.validate_member_data(data)

    def test_missing_profile_field_raises(self):
        """Missing required profile field raises ValidationError with field name."""
        data = _make_valid_member_data()
        del data['profile']['seeking']
        with pytest.raises(ValidationError, match="Missing profile field: seeking"):
            DataValidator.validate_member_data(data)

    def test_empty_matches_raises(self):
        """Empty matches list raises ValidationError."""
        data = _make_valid_member_data()
        data['matches'] = []
        with pytest.raises(ValidationError, match="No matches provided"):
            DataValidator.validate_member_data(data)

    def test_match_missing_field_raises(self):
        """Match without required 'contact' field raises ValidationError."""
        data = _make_valid_member_data()
        del data['matches'][0]['contact']
        with pytest.raises(ValidationError, match="Match #1 missing field: contact"):
            DataValidator.validate_member_data(data)

    def test_score_valid_fraction(self):
        """Score in '95/100' format passes validation."""
        data = _make_valid_member_data()
        data['matches'][0]['score'] = '95/100'
        assert DataValidator.validate_member_data(data) is True

    def test_score_valid_plain(self):
        """Plain numeric score string '85' passes validation."""
        data = _make_valid_member_data()
        data['matches'][0]['score'] = '85'
        assert DataValidator.validate_member_data(data) is True

    def test_score_valid_float(self):
        """Float score 63.3 passes validation."""
        data = _make_valid_member_data()
        data['matches'][0]['score'] = 63.3
        assert DataValidator.validate_member_data(data) is True

    def test_score_out_of_range(self):
        """Score of 150 (out of 0-100) raises ValidationError."""
        data = _make_valid_member_data()
        data['matches'][0]['score'] = 150
        with pytest.raises(ValidationError, match="invalid score"):
            DataValidator.validate_member_data(data)

    def test_score_invalid_string(self):
        """Non-numeric score string 'abc' raises ValidationError."""
        data = _make_valid_member_data()
        data['matches'][0]['score'] = 'abc'
        with pytest.raises(ValidationError, match="invalid score"):
            DataValidator.validate_member_data(data)


# =============================================================================
# DataValidator.safe_get
# =============================================================================

class TestDataValidatorSafeGet:
    """Tests for DataValidator.safe_get()"""

    def test_safe_get_existing_key(self):
        """Returns value for existing key with truthy value."""
        obj = {'name': 'Alice'}
        assert DataValidator.safe_get(obj, 'name') == 'Alice'

    def test_safe_get_missing_key(self):
        """Returns '[Not provided]' for missing key."""
        obj = {'name': 'Alice'}
        assert DataValidator.safe_get(obj, 'email') == '[Not provided]'

    def test_safe_get_empty_string(self):
        """Returns default when value is empty string."""
        obj = {'name': ''}
        assert DataValidator.safe_get(obj, 'name') == '[Not provided]'

    def test_safe_get_custom_default(self):
        """Returns custom default when key is missing."""
        obj = {'name': 'Alice'}
        assert DataValidator.safe_get(obj, 'email', 'N/A') == 'N/A'

    def test_safe_get_whitespace_only(self):
        """Returns default when value is whitespace only."""
        obj = {'name': '   '}
        assert DataValidator.safe_get(obj, 'name') == '[Not provided]'


# =============================================================================
# detect_urgency
# =============================================================================

class TestDetectUrgency:
    """Tests for detect_urgency()"""

    def test_urgency_high_keywords(self):
        """Text containing 'urgent' returns 'High'."""
        assert detect_urgency('This is urgent') == 'High'

    def test_urgency_high_immediate(self):
        """Text containing 'immediate' returns 'High'."""
        assert detect_urgency('Immediate action needed') == 'High'

    def test_urgency_high_asap(self):
        """Text containing 'asap' returns 'High'."""
        assert detect_urgency('Need this asap') == 'High'

    def test_urgency_low_keywords(self):
        """Text containing 'no rush' returns 'Low'."""
        assert detect_urgency('No rush at all') == 'Low'

    def test_urgency_low_ongoing(self):
        """Text containing 'ongoing' returns 'Low'."""
        assert detect_urgency('This is an ongoing opportunity') == 'Low'

    def test_urgency_low_long_term(self):
        """Text containing 'long-term' returns 'Low'."""
        assert detect_urgency('A long-term partnership') == 'Low'

    def test_urgency_medium_default(self):
        """Text without urgency keywords returns 'Medium'."""
        assert detect_urgency('Next quarter') == 'Medium'

    def test_urgency_none_input(self):
        """None input returns 'Medium'."""
        assert detect_urgency(None) == 'Medium'

    def test_urgency_empty_string(self):
        """Empty string returns 'Medium'."""
        assert detect_urgency('') == 'Medium'


# =============================================================================
# detect_collaboration_type
# =============================================================================

class TestDetectCollaborationType:
    """Tests for detect_collaboration_type()"""

    def test_type_joint_venture(self):
        """Text with 'joint venture' returns 'Joint Venture'."""
        assert detect_collaboration_type('Joint venture opportunity') == 'Joint Venture'

    def test_type_jv_abbreviation(self):
        """Text with 'jv' returns 'Joint Venture'."""
        assert detect_collaboration_type('A JV deal') == 'Joint Venture'

    def test_type_referral(self):
        """Text with 'referral' returns 'Cross-Referral'."""
        assert detect_collaboration_type('Cross-referral program') == 'Cross-Referral'

    def test_type_publishing(self):
        """Text with 'publishing' returns 'Publishing'."""
        assert detect_collaboration_type('Book publishing deal') == 'Publishing'

    def test_type_publishing_book_keyword(self):
        """Text with 'book' returns 'Publishing'."""
        assert detect_collaboration_type('Co-author a book') == 'Publishing'

    def test_type_speaking(self):
        """Text with 'speaking' returns 'Speaking'."""
        assert detect_collaboration_type('Speaking event') == 'Speaking'

    def test_type_speaking_event_keyword(self):
        """Text with 'event' returns 'Speaking'."""
        assert detect_collaboration_type('Host an event together') == 'Speaking'

    def test_type_coaching(self):
        """Text with 'coaching' returns 'Coaching'."""
        assert detect_collaboration_type('Coaching program') == 'Coaching'

    def test_type_coaching_mentoring_keyword(self):
        """Text with 'mentoring' returns 'Coaching'."""
        assert detect_collaboration_type('Group mentoring opportunity') == 'Coaching'

    def test_type_default(self):
        """Text without recognized keywords returns 'Partnership'."""
        assert detect_collaboration_type('Something else entirely') == 'Partnership'

    def test_type_none(self):
        """None input returns 'Partnership'."""
        assert detect_collaboration_type(None) == 'Partnership'


# =============================================================================
# parse_score
# =============================================================================

class TestParseScore:
    """Tests for parse_score()"""

    def test_parse_fraction(self):
        """'95/100' parses to 95."""
        assert parse_score('95/100') == 95

    def test_parse_plain(self):
        """'85' parses to 85."""
        assert parse_score('85') == 85

    def test_parse_float(self):
        """63.3 parses to 63 (truncated to int)."""
        assert parse_score(63.3) == 63

    def test_parse_float_string(self):
        """'72.8' parses to 72."""
        assert parse_score('72.8') == 72

    def test_parse_invalid(self):
        """'abc' returns 0."""
        assert parse_score('abc') == 0

    def test_parse_none(self):
        """None returns 0."""
        assert parse_score(None) == 0

    def test_parse_zero(self):
        """'0' parses to 0."""
        assert parse_score('0') == 0

    def test_parse_hundred(self):
        """'100/100' parses to 100."""
        assert parse_score('100/100') == 100


# =============================================================================
# get_score_color
# =============================================================================

class TestGetScoreColor:
    """Tests for get_score_color()"""

    def test_score_excellent(self):
        """Score >= 90 returns COLORS['score_excellent']."""
        assert get_score_color(95) == COLORS['score_excellent']

    def test_score_excellent_boundary(self):
        """Score of exactly 90 returns COLORS['score_excellent']."""
        assert get_score_color(90) == COLORS['score_excellent']

    def test_score_good(self):
        """Score >= 75 (but < 90) returns COLORS['score_good']."""
        assert get_score_color(80) == COLORS['score_good']

    def test_score_good_boundary(self):
        """Score of exactly 75 returns COLORS['score_good']."""
        assert get_score_color(75) == COLORS['score_good']

    def test_score_fair(self):
        """Score < 75 returns COLORS['score_fair']."""
        assert get_score_color(60) == COLORS['score_fair']

    def test_score_fair_low(self):
        """Very low score returns COLORS['score_fair']."""
        assert get_score_color(10) == COLORS['score_fair']


# =============================================================================
# pdf_components.safe_get (module-level function)
# =============================================================================

class TestComponentsSafeGet:
    """Tests for the module-level safe_get() in pdf_components."""

    def test_safe_get_existing_key(self):
        """Returns value for existing key."""
        assert safe_get({'name': 'Alice'}, 'name') == 'Alice'

    def test_safe_get_missing_key(self):
        """Returns '[Not provided]' for missing key."""
        assert safe_get({'name': 'Alice'}, 'email') == '[Not provided]'

    def test_safe_get_empty_string(self):
        """Returns default for empty string value."""
        assert safe_get({'name': ''}, 'name') == '[Not provided]'

    def test_safe_get_custom_default(self):
        """Returns custom default when key missing."""
        assert safe_get({'a': 1}, 'b', 'N/A') == 'N/A'

    def test_safe_get_none_obj(self):
        """Returns default when obj is None."""
        assert safe_get(None, 'key') == '[Not provided]'


# =============================================================================
# PDFGenerator
# =============================================================================

class TestPDFGenerator:
    """Tests for PDFGenerator class"""

    def test_init_creates_output_dir(self, tmp_path):
        """Constructor creates the output directory if it does not exist."""
        output_dir = tmp_path / 'new_outputs'
        assert not output_dir.exists()
        generator = PDFGenerator(output_dir=str(output_dir))
        assert output_dir.exists()
        assert output_dir.is_dir()

    def test_generate_valid_data(self, tmp_path):
        """Generating with valid data returns a path to an existing PDF file."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        result = generator.generate(data)
        assert isinstance(result, str)
        assert result.endswith('.pdf')
        assert os.path.exists(result)
        assert os.path.getsize(result) > 0

    def test_generate_with_member_id(self, tmp_path):
        """When member_id is provided it appears in the filename."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        result = generator.generate(data, member_id='MBR001')
        filename = os.path.basename(result)
        assert 'MBR001' in filename

    def test_generate_validation_failure(self, tmp_path):
        """Missing participant raises PDFGenerationError (wraps ValidationError)."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        del data['participant']
        with pytest.raises(PDFGenerationError, match="Invalid data"):
            generator.generate(data)

    def test_generate_to_bytes_returns_bytes(self, tmp_path):
        """generate_to_bytes() returns a non-empty bytes object."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        result = generator.generate_to_bytes(data)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_generate_adds_date_if_missing(self, tmp_path):
        """When 'date' is not in data, generate() adds it automatically."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        del data['date']
        assert 'date' not in data
        result = generator.generate(data)
        # After generation, date should have been injected
        assert 'date' in data
        assert os.path.exists(result)

    def test_generate_filename_without_member_id(self, tmp_path):
        """Without member_id, filename uses date format."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        result = generator.generate(data)
        filename = os.path.basename(result)
        assert 'Jane_Smith' in filename
        assert '_JV_Report.pdf' in filename

    def test_generate_multiple_matches(self, tmp_path):
        """PDF generation succeeds with multiple matches."""
        generator = PDFGenerator(output_dir=str(tmp_path))
        data = _make_valid_member_data()
        data['matches'].append({
            'name': 'Alice Williams',
            'score': '92/100',
            'type': 'Cross-Referral',
            'message': 'Hi Alice, great to meet you...',
            'contact': 'alice@example.com',
            'fit': 'Complementary audiences',
            'opportunity': 'Joint webinar series',
            'benefits': 'Shared lead generation',
            'timing': 'Immediate',
        })
        result = generator.generate(data)
        assert os.path.exists(result)
        assert os.path.getsize(result) > 0


# =============================================================================
# pdf_styles
# =============================================================================

class TestPdfStyles:
    """Tests for pdf_styles module"""

    def test_create_pdf_styles_returns_styles(self):
        """create_pdf_styles() returns a stylesheet with expected custom styles."""
        styles = create_pdf_styles()
        expected_names = [
            'Hero', 'HeroSubtitle', 'SectionHead', 'Subhead',
            'Body', 'BodySmall', 'Small', 'SmallBold',
            'MatchTitle', 'BigScore', 'ProfileLabel', 'ProfileValue',
            'TableHeader', 'TableCell', 'MessageBox',
        ]
        for name in expected_names:
            assert styles[name] is not None, f"Missing style: {name}"

    def test_colors_dict_has_expected_keys(self):
        """COLORS dict includes primary, secondary, and score color keys."""
        expected_keys = [
            'primary', 'secondary', 'dark', 'light_bg', 'border', 'white',
            'urgency_high', 'urgency_medium', 'urgency_low',
            'score_excellent', 'score_good', 'score_fair',
        ]
        for key in expected_keys:
            assert key in COLORS, f"Missing COLORS key: {key}"

    def test_colors_values_are_color_objects(self):
        """All COLORS values are reportlab color objects (have .hexval() or are Color)."""
        from reportlab.lib.colors import Color
        for key, value in COLORS.items():
            assert isinstance(value, Color), (
                f"COLORS['{key}'] should be a reportlab Color, got {type(value)}"
            )
