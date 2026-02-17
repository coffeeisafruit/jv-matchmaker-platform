# Execute Plan: One Report System for All Members

## What This Is

This is a self-contained execution prompt. Paste this entire file content into a new Claude Code context window to implement the plan.

## Goal

Consolidate the JV matchmaker report generation into ONE system (`generate_member_report`) that works for any member using the existing `SupabaseMatchScoringService` (ISMC harmonic scorer). Clean up tech debt by archiving old per-client commands and dead scripts.

## Execute in Order

### Phase 1: Archive tech debt (use a Bash subagent)

Move these files to `archive/` directories. Create the archive dirs first.

**Commands to archive → `matching/management/commands/archive/`:**
- `matching/management/commands/match_janet.py`
- `matching/management/commands/match_penelope.py`
- `matching/management/commands/match_instagram_coaches.py`
- `matching/management/commands/match_linkedin_contacts.py`
- `matching/management/commands/generate_janet_pdf.py`
- `matching/management/commands/generate_match_pdf.py`
- `matching/management/commands/enrich_janet_matches.py`
- `matching/management/commands/run_janet_research.py`

**Dead scripts to archive → `archive/scripts/`:**
- Root level: `absolute_final_update.py`, `merge_duplicates.py`, `find_missing_contacts.py`, `update_missing_contacts.py`, `merge_all_contacts.py`, `final_contact_update.py`, `fix_specific_contacts.py`, `add_new_contacts.py`, `analyze_enrichment_quality.py`, `verification_analysis.py`, `test_owl_simple.py`
- From `scripts/`: `automated_enrichment_pipeline.py`, `automated_enrichment_pipeline_optimized.py`, `automated_enrichment_pipeline_verified.py`

**Orphaned templates to archive → `archive/templates/`:**
- `templates/matching/candidate_matches_report.html`
- `templates/matching/jv_email_list.html`
- `templates/matching/saved_candidate_list.html`
- `templates/matching/saved_candidate_detail.html`
- `templates/matching/guest_candidate_form.html`

**IMPORTANT:** Before archiving `match_janet.py`, copy the `JANET_PROFILE` dict (lines 22-34) — it has the correct data needed to fix Janet's Supabase profile.

### Phase 2: Fix Janet's Supabase profile (one-time, Django shell)

Janet's Supabase profile is corrupted with David Lynch Foundation data. Fix it using the correct data from `JANET_PROFILE` in `match_janet.py`:

```python
# In Django shell:
from matching.models import SupabaseProfile
janet = SupabaseProfile.objects.get(name__icontains='Janet Bray Attwood')
janet.company = 'Becoming International'
janet.niche = 'International expansion for coaches & speakers'
janet.offering = 'Becoming International Mastermind - land international speaking engagements, build elite global networks'
janet.website = 'becominginternational.com'
janet.who_you_serve = 'Coaches and speakers ready to expand their reach internationally'
janet.what_you_do = 'Becoming International Mastermind - land international speaking engagements, build elite global networks'
janet.seeking = 'JV Launch Partners, Affiliates, Speaking Platforms'
janet.save()
print(f"Fixed: {janet.name} → {janet.company}")
```

### Phase 3: Rewrite `generate_member_report.py` (main work)

**File:** `matching/management/commands/generate_member_report.py`

**What to change:**

1. **Replace inline scoring with ISMC scorer.** Remove these module-level constants: `SCORING_WEIGHTS`, `AUDIENCE_KEYWORDS`, `EXCLUDE_EMAILS`. Remove the `_score_partner()` method (lines 335-441) and `_generate_why_fit()` method (lines 443-472). Replace with:

```python
from matching.services import SupabaseMatchScoringService
```

New method `_score_with_ismc(self, client_sp, top_n)`:
- Load candidates: `SupabaseProfile.objects.filter(status='Member').exclude(id=client_sp.id)`
- Filter non-person names using existing `_looks_like_person_name()`
- For each candidate: `scorer.score_pair(client_sp, partner_sp)` → use `result['score_ab']` (directional: how valuable partner is for THIS client) and `result['breakdown_ab']` for the ISMC component details
- Sort by `score_ab` descending, take top N
- Return list of dicts: `{'partner': sp, 'score': score_ab, 'breakdown': breakdown_ab, 'reason': ..., 'why_fit': ..., 'detail_note': ...}`

2. **Add `--company` argument** to `add_arguments()`:
```python
parser.add_argument('--company', type=str, default=None, help='Override company name')
```

3. **Add `--all` argument** to `add_arguments()`:
```python
parser.add_argument('--all', action='store_true', help='Generate reports for ALL active members')
```

4. **Modify `handle()` method:**
- If `--all` is set: loop over all `SupabaseProfile.objects.filter(status='Member')`, generate a report for each
- If `--company` is set: use it instead of `_clean_company_name()`
- Replace the current inline scoring block (lines 157-189) with `self._score_with_ismc(client_sp, top_n)`

5. **New method `_build_why_fit_from_ismc(self, partner, breakdown)`:**
Build narrative from ISMC breakdown. Read the Intent/Synergy/Momentum/Context factor details from `breakdown`:
- `breakdown['intent']['factors']` — list of dicts with `name`, `score`, `detail`
- `breakdown['synergy']['factors']` — same structure
- `breakdown['momentum']['factors']` — same structure
- `breakdown['context']['factors']` — same structure

Combine high-scoring factors with profile data into a 2-3 sentence paragraph. Example:
*"David actively seeks JV partners and has 3 past partnerships. His business coaching audience aligns well with your offering. 150K subscribers with strong platform overlap."*

6. **Update section assignment** in `_assign_section()` to use ISMC 0-100 scale:
- `score >= 70` + has email → `priority`
- `score >= 50` + has email → `this_week`
- Has LinkedIn only → `low_priority`
- Has booking link + seeking JVs → `jv_programs`

**Keep these existing helpers unchanged:** `_looks_like_person_name()`, `_clean_url_field()`, `_extract_linkedin()`, `_extract_website()`, `_extract_schedule()`, `_clean_company_name()`, `_resolve_client()`, `_supabase_to_client_dict()`, `_assign_badge()`, `_assign_badge_style()`, `_build_tagline()`, `_format_list_size()`, `_build_audience_desc()`, `_build_tags()`, `_build_detail_note()`, `_build_client_profile()`, `_build_outreach_templates()`, `_parse_month()`, `_enrich_matches()`

### Phase 4: Add auto-refresh tasks to `matching/tasks.py`

**File:** `matching/tasks.py`

Add 3 task functions after the existing `bulk_recalculate_matches` function (line 147). Follow the same pattern as `recalculate_matches_for_profile`:

```python
@shared_task
def regenerate_member_report(report_id: int):
    """Regenerate a single member report with fresh ISMC scoring.
    Preserves the same access code and report URL."""
    from matching.models import MemberReport, ReportPartner, SupabaseProfile
    from matching.services import SupabaseMatchScoringService
    # 1. Get the report, verify it exists and is active
    # 2. Delete existing ReportPartner records
    # 3. Re-score using SupabaseMatchScoringService
    # 4. Create new ReportPartner records
    # 5. Update report.created_at to now (marks as fresh)

@shared_task
def regenerate_all_monthly_reports(month: str = None):
    """Monthly batch: regenerate all active reports."""
    from matching.models import MemberReport
    # Loop all active MemberReport objects
    # Call regenerate_member_report for each

@shared_task
def refresh_reports_for_profile(profile_id: str):
    """Incremental: when a profile changes, update reports containing that partner."""
    from matching.models import ReportPartner
    # Find all ReportPartner records with source_profile_id == profile_id
    # For each affected report, call regenerate_member_report
```

### Phase 5: Add `is_stale` property to `MemberReport` in `matching/models.py`

Add this property to the `MemberReport` class:

```python
@property
def is_stale(self):
    """Report is stale if older than 30 days."""
    from django.utils import timezone
    return (timezone.now() - self.created_at).days >= 30
```

### Phase 6: Verify

1. Fix Janet's profile (Phase 2)
2. Run: `python manage.py generate_member_report --client-name "Janet Bray Attwood" --month 2026-02`
3. Check output: access code printed, partners listed with ISMC scores
4. Start server: `python manage.py runserver`
5. Visit `/matching/report/`, enter access code
6. Verify: hub shows "Becoming International", outreach page has partners with why_fit narratives, badges, tags
7. Run for another member: `python manage.py generate_member_report --client-name "Penelope Jane Smith" --month 2026-02` — should work with zero config
8. Run batch: `python manage.py generate_member_report --all --month 2026-02`

## Key Reference Files (read these first)

- `matching/services.py` lines 1216-1643 — `SupabaseMatchScoringService` (the ISMC scorer to use)
- `matching/management/commands/generate_member_report.py` — THE file to rewrite
- `matching/tasks.py` — add refresh tasks here
- `matching/models.py` — add `is_stale` property to `MemberReport`
- `matching/management/commands/match_janet.py` lines 22-34 — `JANET_PROFILE` dict (correct data for one-time fix)

## Parallel Execution Strategy

Use multiple subagents for efficiency:
- **Subagent 1 (Bash):** Phase 1 — archive all files (mkdir + git mv)
- **Subagent 2 (Bash):** Phase 2 — fix Janet's profile in Django shell
- **Subagent 3:** Phase 3 — rewrite generate_member_report.py (main work, do this after reading services.py)
- After Phase 3 completes: Phase 4 (tasks.py) and Phase 5 (models.py) can run in parallel
- Phase 6: verify everything works end-to-end
