# Current Codebase Analysis: How Research Fits

## Current Architecture Overview

### ✅ What Already Works (Keep As-Is)

1. **Tech Stack Alignment**
   - ✅ Django 5.x (matches all research)
   - ✅ HTMX middleware already installed (`django_htmx`)
   - ✅ PostgreSQL via Supabase (can add RLS later)
   - ✅ HAT stack ready (just need to ensure Alpine.js + Tailwind are in templates)

2. **Existing User Model** (`core/models.py`)
   ```python
   class User(AbstractUser):
       business_name = models.CharField(max_length=255)  # → Maps to Tenant.name
       business_domain = models.URLField()               # → Maps to Tenant.domain
       tier = models.CharField(...)                      # → Maps to Tenant.plan
   ```
   - **Migration Path**: Create `Tenant` from `User.business_name`, keep `User` for auth

3. **JV Matching System** (`matching/`)
   - ✅ `Profile` model for JV partners (keep separate from co-sell `Overlap`)
   - ✅ `Match` model with AI scoring (Intent, Synergy, Momentum, Context)
   - ✅ `MatchScoringService` - sophisticated scoring algorithm
   - ✅ `PartnershipAnalyzer` - dynamic insights
   - ✅ Supabase integration (read-only profiles)
   - **Decision**: Keep this as-is, add co-sell features alongside

4. **Service Layer Pattern** (`matching/services.py`)
   - ✅ Already using service classes (`MatchScoringService`, `PartnershipAnalyzer`)
   - ✅ Can extend this pattern to new apps
   - **Action**: Refactor to `services/` directory structure (Original Research pattern)

5. **Existing Apps Structure**
   ```
   core/          → Add Tenant model here
   matching/      → Keep for JV partner matching
   positioning/   → Keep for ICP/Transformation
   outreach/      → Keep for PVP/Campaigns
   ```

### 🔄 What Needs Adaptation

1. **Single-User → Multi-Tenant Migration**
   - **Current**: `Profile.user` (FK to User)
   - **Research**: `Overlap.tenant` (FK to Tenant)
   - **Strategy**: 
     - Add `Tenant` model to `core/`
     - Create `Membership` model (User ↔ Tenant relationship)
     - Add `tenant` FK to existing models (nullable initially)
     - Migrate: Create default Tenant for each User

2. **Data Model Separation**
   - **Current**: `Profile` = JV partner directory
   - **Research**: `Overlap` = Co-sell account overlaps
   - **Decision**: Keep separate! They serve different purposes:
     - `Profile` (JV): AI-powered matching, Supabase read-only
     - `Overlap` (Co-sell): Deterministic domain matching, CSV/CRM sources

3. **Matching Algorithms**
   - **Current**: AI scoring (Intent, Synergy, Momentum, Context) for JV partners
   - **Research**: Exact domain matching for co-sell overlaps
   - **Decision**: Two separate systems:
     - `MatchScoringService` (existing) → JV partner discovery
     - `EntityResolutionService` (new) → Co-sell overlap detection

### ➕ What to Add (New Apps)

1. **`overlaps/` App** (ChatGPT structure)
   - `Overlap` model (co-sell account overlaps)
   - `OverlapImportRun` model (CSV import tracking)
   - CSV import service
   - **No conflict**: Completely new functionality

2. **`intros/` App** (ChatGPT structure)
   - `IntroRequest` model (workflow object)
   - `Outcome` model (logged results)
   - **No conflict**: New feature set

3. **`partners/` App** (ChatGPT structure)
   - `Partner` model (external orgs for co-sell)
   - `PartnerConnection` model (tenant ↔ partner relationship)
   - **Note**: Different from `Profile` (JV partners) - these are co-sell partners

4. **`audit/` App** (ChatGPT structure)
   - `AuditEvent` model (immutable event log)
   - **No conflict**: New feature

5. **`integrations/` App** (New)
   - `SlackInstallation` model
   - `SlackMessageDelivery` model
   - Salesforce/HubSpot OAuth (future)
   - **No conflict**: New feature

### ⚠️ Potential Conflicts & Resolutions

1. **User Model Fields**
   - **Conflict**: `User.business_name` vs `Tenant.name`
   - **Resolution**: 
     - Create `Tenant` from `User.business_name` during migration
     - Keep `User.business_name` for backward compatibility (deprecate later)
     - New signups: Create Tenant during onboarding

2. **Profile Model Naming**
   - **Conflict**: `matching.Profile` (JV partners) vs potential confusion with co-sell
   - **Resolution**: 
     - Keep `matching.Profile` as-is (it's for JV partners)
     - Use `overlaps.Overlap` for co-sell (different concept)
     - Clear naming: "JV Partners" vs "Co-Sell Overlaps" in UI

3. **Tier/Plan Field**
   - **Conflict**: `User.tier` vs `Tenant.plan`
   - **Resolution**: 
     - Add `plan` field to `Tenant` (from research)
     - Migrate `User.tier` → `Tenant.plan`
     - Keep `User.tier` for backward compatibility initially

4. **Service Layer Structure**
   - **Current**: `matching/services.py` (single file)
   - **Research**: `services/` directory with multiple files
   - **Resolution**: 
     - Keep existing `services.py` files working
     - Create `services/` directories in new apps
     - Gradually refactor existing apps if needed

### 📋 Migration Strategy

#### Phase 1: Add Tenant Model (Non-Breaking)

```python
# core/models.py - ADD (don't replace)
class Tenant(TimeStampedModel):
    name = models.CharField(max_length=200)
    slug = models.SlugField(max_length=80, unique=True)
    domain = models.CharField(max_length=255, blank=True)
    plan = models.CharField(max_length=20, choices=PlanChoices, default='free')
    is_active = models.BooleanField(default=True)

class Membership(TimeStampedModel):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    role = models.CharField(...)
    # unique_together: (tenant, user)
```

**Migration Steps**:
1. Create `Tenant` model (no FK changes yet)
2. Data migration: Create default Tenant for each User
   ```python
   for user in User.objects.all():
       tenant = Tenant.objects.create(
           name=user.business_name,
           slug=slugify(user.business_name),
           domain=user.business_domain or '',
           plan=user.tier,
       )
       Membership.objects.create(tenant=tenant, user=user, role='owner')
   ```
3. Add `tenant` FK to existing models (nullable initially)
4. Update views to use `request.tenant` (via middleware)

#### Phase 2: Add Tenant FK to Existing Models (Backward Compatible)

```python
# matching/models.py - ADD field (nullable)
class Profile(models.Model):
    user = models.ForeignKey(User, ...)  # Keep existing
    tenant = models.ForeignKey(Tenant, null=True, blank=True)  # Add new
    
    def save(self, *args, **kwargs):
        if not self.tenant and self.user:
            # Auto-set from user's primary membership
            membership = self.user.memberships.filter(is_active=True).first()
            if membership:
                self.tenant = membership.tenant
        super().save(*args, **kwargs)
```

**Migration Steps**:
1. Add nullable `tenant` FK
2. Data migration: Populate `tenant` from `user.memberships`
3. Make `tenant` required in new records (keep `user` for backward compat)

#### Phase 3: Add New Co-Sell Apps (No Conflicts)

```python
# overlaps/models.py - NEW APP
class Overlap(models.Model):
    tenant = models.ForeignKey(Tenant, ...)  # Required from day 1
    # ... rest of ChatGPT model
```

**No migration needed**: New models, no FK to existing models initially.

### 🎯 Integration Points

1. **Dashboard** (`core/views.py` - `DashboardView`)
   - **Current**: Shows JV matches, stats
   - **Add**: Tabs for "JV Partners" | "Co-Sell Overlaps" | "Internal Hygiene"
   - **HTMX**: Tab switching, separate views

2. **User Authentication** (`core/views.py`)
   - **Current**: Standard Django auth
   - **Add**: `TenantMiddleware` to set `request.tenant`
   - **No changes**: Auth flow stays the same

3. **Service Layer** (`matching/services.py`)
   - **Current**: `MatchScoringService` for JV partners
   - **Add**: `overlaps/services/csv_import.py` for co-sell
   - **No conflict**: Different services, different purposes

4. **Templates** (`templates/`)
   - **Current**: JV matching templates
   - **Add**: Co-sell templates in `templates/overlaps/`, `templates/intros/`
   - **Update**: Dashboard to show both sections

### 🔧 Code Changes Required

#### Minimal Changes (Phase 1)

1. **Add Tenant Model** (`core/models.py`)
   ```python
   # ADD to existing file
   class Tenant(TimeStampedModel):
       # ... ChatGPT model
   ```

2. **Add TenantMiddleware** (`core/middleware.py` - NEW FILE)
   ```python
   # Copy ChatGPT's TenantMiddleware
   ```

3. **Update Settings** (`config/settings.py`)
   ```python
   MIDDLEWARE = [
       # ... existing
       "core.middleware.TenantMiddleware",  # ADD
   ]
   ```

4. **Create New Apps**
   ```bash
   python manage.py startapp overlaps
   python manage.py startapp intros
   python manage.py startapp partners
   python manage.py startapp audit
   ```

#### No Breaking Changes

- ✅ Existing `User` model stays the same
- ✅ Existing `Profile` and `Match` models stay the same
- ✅ Existing views continue to work
- ✅ Existing templates continue to work
- ✅ Supabase integration continues to work

### 📊 Feature Comparison Matrix

| Feature | Current (JV Matchmaker) | Research (Co-Sell) | Integration Strategy |
|---------|------------------------|-------------------|---------------------|
| **Partner Discovery** | AI scoring (Intent/Synergy/Momentum/Context) | Exact domain matching | Keep both, separate UI sections |
| **Data Source** | Supabase profiles (read-only) | CSV import, Salesforce, HubSpot | Support both sources |
| **Matching Model** | `Profile` + `Match` | `Overlap` | Separate models, different purposes |
| **Workflow** | PVP generation, outreach campaigns | Intro requests, Slack approvals | Both workflows available |
| **User Model** | Single-user (`User.business_name`) | Multi-tenant (`Tenant`) | Migrate gradually, backward compatible |

### ✅ Summary: How It Fits

**Perfect Alignment**:
- ✅ Tech stack (Django + HTMX + Alpine + Tailwind)
- ✅ Service layer pattern (already using services.py)
- ✅ PostgreSQL database
- ✅ HTMX already installed

**Easy Integration**:
- ✅ Add Tenant model alongside User (non-breaking)
- ✅ Add new apps (overlaps, intros, partners, audit) - no conflicts
- ✅ Keep existing JV matching system as-is
- ✅ Add co-sell features as parallel feature set

**Migration Path**:
- ✅ Phase 1: Add Tenant, create default Tenants for existing Users
- ✅ Phase 2: Add tenant FK to existing models (nullable, backward compatible)
- ✅ Phase 3: Add new co-sell apps (no FK to existing models initially)
- ✅ Phase 4: Gradually migrate views to use tenant context

**No Breaking Changes**:
- ✅ All existing functionality continues to work
- ✅ Existing data remains accessible
- ✅ Existing users see no disruption
- ✅ Can roll out co-sell features incrementally
