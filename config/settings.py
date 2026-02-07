"""
Django settings for GTM Engine Platform.

HAT Stack: HTMX + Alpine.js + Tailwind CSS
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
load_dotenv(BASE_DIR / ".env")

# Environment variables (with defaults for development)
SECRET_KEY = os.environ.get(
    "SECRET_KEY",
    "django-insecure-dev-key-change-in-production"
)

DEBUG = os.environ.get("DEBUG", "True").lower() == "true"

ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")

# API Keys (loaded from environment)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
CLAY_WEBHOOK_SECRET = os.environ.get("CLAY_WEBHOOK_SECRET", "")

# Application definition
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
    # Third party
    "django_htmx",
    # Local apps
    "core.apps.CoreConfig",
    "positioning.apps.PositioningConfig",
    "matching.apps.MatchingConfig",
    "outreach.apps.OutreachConfig",
    "playbook.apps.PlaybookConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_htmx.middleware.HtmxMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

# Database
# Use PostgreSQL (Supabase) if DATABASE_URL is set, otherwise SQLite
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import dj_database_url
    DATABASES = {
        "default": dj_database_url.parse(DATABASE_URL, conn_max_age=600)
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# Custom User Model
AUTH_USER_MODEL = "core.User"

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

# Media files
MEDIA_URL = "media/"
MEDIA_ROOT = BASE_DIR / "media"

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Login/Logout URLs
LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/dashboard/"
LOGOUT_REDIRECT_URL = "/"

# AI Model Configuration (OpenRouter)
# Free models for testing - switch to paid for production
AI_CONFIG = {
    "default_model": "meta-llama/llama-3.2-3b-instruct:free",
    "fast_model": "meta-llama/llama-3.2-3b-instruct:free",
    "quality_model": "meta-llama/llama-3.2-3b-instruct:free",
    "max_tokens": 4096,
    "temperature": 0.7,
    # Paid models (for production):
    # "default_model": "openai/gpt-4o-mini",
    # "quality_model": "anthropic/claude-sonnet-4",
}

# GTM Engine Configuration
GTM_CONFIG = {
    "max_matches_per_user": 20,
    "max_pvps_per_month": 50,
    "scoring_weights": {
        "intent": 0.45,
        "synergy": 0.25,
        "momentum": 0.20,
        "context": 0.10,
    },
    "quality_threshold": 8.0,  # AutoClaygent 7-criterion minimum
}

# Email OAuth Configuration (Gmail & Outlook)
# Get credentials from Google Cloud Console and Azure AD
GOOGLE_OAUTH_CLIENT_ID = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
GOOGLE_OAUTH_REDIRECT_URI = os.environ.get(
    "GOOGLE_OAUTH_REDIRECT_URI",
    "http://localhost:8001/outreach/email/oauth/google/callback/"
)

MICROSOFT_OAUTH_CLIENT_ID = os.environ.get("MICROSOFT_OAUTH_CLIENT_ID", "")
MICROSOFT_OAUTH_CLIENT_SECRET = os.environ.get("MICROSOFT_OAUTH_CLIENT_SECRET", "")
MICROSOFT_OAUTH_REDIRECT_URI = os.environ.get(
    "MICROSOFT_OAUTH_REDIRECT_URI",
    "http://localhost:8001/outreach/email/oauth/microsoft/callback/"
)

# OAuth Scopes
GOOGLE_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
]

MICROSOFT_OAUTH_SCOPES = [
    "Mail.Send",
    "Mail.Read",
    "User.Read",
    "offline_access",
]

# Static files with WhiteNoise
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

# Production Security Settings
if not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True

# CSRF Trusted Origins (for DigitalOcean)
CSRF_TRUSTED_ORIGINS = os.environ.get(
    "CSRF_TRUSTED_ORIGINS",
    "https://*.ondigitalocean.app"
).split(",")
