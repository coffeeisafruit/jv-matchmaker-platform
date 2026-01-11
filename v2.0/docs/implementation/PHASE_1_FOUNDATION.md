# Phase 1: Foundation - Implementation Guide

**Estimated Time**: Week 1-2  
**Goal**: Add multi-tenancy infrastructure without breaking existing features

## Prerequisites

- Current codebase is working
- Database migrations are up to date
- You've reviewed `/docs/planning/INTEGRATION_PLAN.md`

## Step 1: Create Tenant Model

**File**: `core/models.py`

Add to existing file (don't replace):

```python
from django.db import models
from django.utils import timezone

class TimeStampedModel(models.Model):
    """Abstract base model with created/updated timestamps."""
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        abstract = True

class Tenant(TimeStampedModel):
    """Multi-tenant workspace model."""
    name = models.CharField(max_length=200)
    slug = models.SlugField(max_length=80, unique=True)
    domain = models.CharField(max_length=255, blank=True, default="")
    plan = models.CharField(
        max_length=20,
        choices=[
            ('free', 'Free'),
            ('starter', 'Starter'),
            ('growth', 'Growth'),
            ('pro', 'Pro'),
            ('enterprise', 'Enterprise'),
        ],
        default='free'
    )
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ['name']
        verbose_name = 'Tenant'
        verbose_name_plural = 'Tenants'

    def __str__(self):
        return self.name

class Membership(TimeStampedModel):
    """User membership in a tenant workspace."""
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE, related_name='memberships')
    user = models.ForeignKey('core.User', on_delete=models.CASCADE, related_name='memberships')
    role = models.CharField(
        max_length=20,
        choices=[
            ('owner', 'Owner'),
            ('admin', 'Admin'),
            ('member', 'Member'),
        ],
        default='member'
    )
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = [('tenant', 'user')]
        indexes = [
            models.Index(fields=['tenant', 'user']),
            models.Index(fields=['tenant', 'role']),
        ]

    def __str__(self):
        return f"{self.user.username} @ {self.tenant.name} ({self.role})"
```

## Step 2: Create Migration

```bash
python manage.py makemigrations core
```

## Step 3: Create Data Migration

**File**: `core/migrations/XXXX_create_default_tenants.py`

```python
from django.db import migrations
from django.utils.text import slugify

def create_default_tenants(apps, schema_editor):
    User = apps.get_model('core', 'User')
    Tenant = apps.get_model('core', 'Tenant')
    Membership = apps.get_model('core', 'Membership')
    
    for user in User.objects.all():
        # Create tenant from user's business_name
        tenant_name = user.business_name or f"{user.username}'s Workspace"
        slug = slugify(tenant_name)
        
        # Ensure unique slug
        base_slug = slug
        counter = 1
        while Tenant.objects.filter(slug=slug).exists():
            slug = f"{base_slug}-{counter}"
            counter += 1
        
        tenant = Tenant.objects.create(
            name=tenant_name,
            slug=slug,
            domain=user.business_domain or '',
            plan=user.tier or 'free',
            is_active=True
        )
        
        # Create membership
        Membership.objects.create(
            tenant=tenant,
            user=user,
            role='owner',
            is_active=True
        )

def reverse_create_default_tenants(apps, schema_editor):
    Tenant = apps.get_model('core', 'Tenant')
    Tenant.objects.all().delete()

class Migration(migrations.Migration):
    dependencies = [
        ('core', 'XXXX_previous_migration'),  # Replace with actual migration number
    ]

    operations = [
        migrations.RunPython(create_default_tenants, reverse_create_default_tenants),
    ]
```

## Step 4: Create Tenant Middleware

**File**: `core/middleware.py` (new file)

```python
from django.http import HttpRequest
from typing import Optional
from .models import Tenant

TENANT_ATTR = "tenant"

def _tenant_from_request(request: HttpRequest) -> Optional[Tenant]:
    """
    Get tenant from request.
    - Prefer session["tenant_slug"] if set
    - Else, if user has only one membership, use it
    - Else, return None
    """
    if not request.user.is_authenticated:
        return None
    
    # Check session
    slug = request.session.get("tenant_slug")
    if slug:
        return Tenant.objects.filter(slug=slug, is_active=True).first()
    
    # Check user's memberships
    memberships = request.user.memberships.filter(is_active=True)
    if memberships.count() == 1:
        return memberships.first().tenant
    
    return None

class TenantMiddleware:
    """Middleware to set request.tenant for multi-tenancy."""
    
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.tenant = _tenant_from_request(request)  # type: ignore
        return self.get_response(request)
```

## Step 5: Add Middleware to Settings

**File**: `config/settings.py`

Add to `MIDDLEWARE` list (after AuthenticationMiddleware):

```python
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "core.middleware.TenantMiddleware",  # ADD THIS
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_htmx.middleware.HtmxMiddleware",
]
```

## Step 6: Add Context Processor (Optional)

**File**: `core/context_processors.py` (new file)

```python
def current_tenant(request):
    """Add current tenant to template context."""
    return {"current_tenant": getattr(request, "tenant", None)}
```

**File**: `config/settings.py`

Add to `TEMPLATES` context_processors:

```python
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
                "core.context_processors.current_tenant",  # ADD THIS
            ],
        },
    },
]
```

## Step 7: Run Migrations

```bash
python manage.py migrate
```

## Step 8: Verify

1. Check that all existing users have a Tenant:
   ```python
   from core.models import User, Tenant, Membership
   
   # Should be True
   assert User.objects.count() == Tenant.objects.count()
   assert User.objects.count() == Membership.objects.count()
   ```

2. Check that middleware works:
   - Log in as a user
   - In a view, check `request.tenant` is set
   - Should see your tenant name

## Next Steps

Once Phase 1 is complete:
- Proceed to Phase 2: Add tenant FK to existing models (nullable)
- Or proceed to Phase 3: Create new co-sell apps (overlaps, intros, partners, audit)

See `/docs/planning/INTEGRATION_PLAN.md` for full roadmap.

## Troubleshooting

**Issue**: Migration fails with "no such table: core_tenant"
- **Solution**: Make sure you ran `makemigrations` before creating the data migration

**Issue**: `request.tenant` is None
- **Solution**: Check that user has a Membership, and middleware is in settings

**Issue**: Existing views break
- **Solution**: This shouldn't happen - tenant is optional. Check that you're not requiring `request.tenant` in existing views yet.
