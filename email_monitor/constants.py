"""Shared constants for email_monitor app."""

# Error codes stored in signup_url field when discovery/subscription fails.
DISCOVERY_ERROR_CODES = frozenset({
    'http_403', 'http_404', 'http_other', 'timeout',
    'js_required', 'no_form', 'captcha', 'error', 'bad_discovery',
})
