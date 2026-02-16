"""
Test settings — uses local PostgreSQL (same engine as production/Supabase).

Django's test framework will auto-create a test_<dbname> database from
the DATABASE_URL in .env, matching the production schema exactly.
"""

from config.settings import *  # noqa: F401, F403
