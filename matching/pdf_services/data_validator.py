"""
Data validation for PDF generation
"""


class ValidationError(Exception):
    """Raised when data validation fails"""
    pass


class DataValidator:
    """Validates member data before PDF generation"""

    REQUIRED_PROFILE_FIELDS = ['what_you_do', 'seeking', 'offering']
    REQUIRED_MATCH_FIELDS = ['name', 'score', 'type', 'message', 'contact']

    @staticmethod
    def validate_member_data(data: dict) -> bool:
        """
        Validate member data structure

        Args:
            data: Dict with 'participant', 'profile', 'matches'

        Raises:
            ValidationError: If required data missing

        Returns:
            True if valid
        """
        # Validate participant
        if not data.get('participant'):
            raise ValidationError("Missing participant name")

        # Validate profile
        profile = data.get('profile', {})
        for field in DataValidator.REQUIRED_PROFILE_FIELDS:
            if not profile.get(field):
                raise ValidationError(f"Missing profile field: {field}")

        # Validate matches
        matches = data.get('matches', [])
        if not matches:
            raise ValidationError("No matches provided")

        for i, match in enumerate(matches):
            # Check required fields
            for field in DataValidator.REQUIRED_MATCH_FIELDS:
                if not match.get(field):
                    raise ValidationError(
                        f"Match #{i+1} missing field: {field}"
                    )

            # Validate score
            try:
                score_str = str(match['score'])
                if '/' in score_str:
                    score = float(score_str.split('/')[0])
                else:
                    score = float(score_str)

                if not 0 <= score <= 100:
                    raise ValueError("Score must be 0-100")

            except (ValueError, IndexError) as e:
                raise ValidationError(
                    f"Match #{i+1} has invalid score: {match.get('score')}"
                )

        return True

    @staticmethod
    def safe_get(obj: dict, key: str, default: str = "[Not provided]") -> str:
        """Safely get value with user-friendly default"""
        value = obj.get(key, default)
        return value if value and str(value).strip() else default
