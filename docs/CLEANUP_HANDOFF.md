# Cleanup & Technical Debt Handoff

Items identified during the enrichment pipeline enhancement (Phases 1-5).
None of these are blocking — everything works as-is. These are improvements
for when time permits.

---

## 1. Root-Level One-Off Scripts (Safe to Delete)

These are one-time data migration/fix scripts that have already been run.
All are in git history if ever needed again.

| File | Purpose |
|------|---------|
| `absolute_final_update.py` | Hard-coded contact updates (Sheri Rosenthal, Michelle Hummel, etc.) |
| `merge_duplicates.py` | Merged duplicate contact records |
| `find_missing_contacts.py` | Searched Supabase for contacts missing email/phone |
| `update_missing_contacts.py` | Updated missing contact info |
| `merge_all_contacts.py` | Generic contact merging |
| `final_contact_update.py` | One-time contact data fixes |
| `fix_specific_contacts.py` | Targeted contact corrections |
| `add_new_contacts.py` | Added Bobby Cardwell and other new contacts |
| `analyze_enrichment_quality.py` | Enrichment coverage analysis |
| `verification_analysis.py` | Verification method analysis |
| `test_owl_simple.py` | Manual OWL testing script |

**Risk**: Zero. No other code imports these.

---

## 2. One-Off Management Commands (Safe to Delete)

These were built for specific user matching runs and have hard-coded
user-specific criteria, suppress lists, and output paths.

| Command | Purpose |
|---------|---------|
| `matching/management/commands/match_penelope.py` | Custom matching for Penelope Jane Smith |
| `matching/management/commands/match_janet.py` | Custom matching for Janet / Becoming International |
| `matching/management/commands/enrich_janet_matches.py` | OWL enrichment for Janet's match results |
| `matching/management/commands/generate_janet_pdf.py` | PDF report for Janet's matches |

**Risk**: Low. These are ad-hoc run-once commands. If a new matching run is
needed for these users, the generic `match_linkedin_contacts_v2.py` or
`match_instagram_coaches_v2.py` commands can be parameterized instead.

---

## 3. Duplicate v1 Commands (Delete After Confirming v2 Works)

| v1 (delete) | v2 (keep) | v2 improvements |
|---|---|---|
| `match_linkedin_contacts.py` (435 lines) | `match_linkedin_contacts_v2.py` (488 lines) | Adds breadth multiplier, IDF-like keyword scoring |
| `match_instagram_coaches.py` | `match_instagram_coaches_v2.py` | Same improvements |

**Risk**: Low. Confirm no cron jobs or scripts reference the v1 filenames.

---

## 4. Duplicate Harmonic Mean Implementations (Refactor)

Six independent implementations of `_calculate_harmonic_mean()` exist across:

- `matching/services.py` — `MatchScoringService.calculate_harmonic_mean()`
- `matching/services.py` — `SupabaseMatchScoringService._score_directional()` (inline)
- `matching/management/commands/match_janet.py`
- `matching/management/commands/match_penelope.py`
- `matching/management/commands/match_linkedin_contacts.py`
- `matching/management/commands/match_linkedin_contacts_v2.py`
- `matching/management/commands/match_instagram_coaches.py`
- `matching/management/commands/match_instagram_coaches_v2.py`

**Suggested fix**: Extract to a shared utility:
```python
# matching/utils.py
def calculate_harmonic_mean(scores: dict[str, float], min_score: float = 1.0) -> float:
    """Weighted harmonic mean. scores = {name: value}, all values 0-10."""
    ...
```
Then update all files to import from it.

**Risk**: Low but requires testing each command after refactor.

---

## 5. Two Scoring Systems (Understand, Don't Delete)

### `MatchScoringService` (Profile model → Match table)
- Used by Django views (`matching/views.py` lines 681, 735, 757)
- Scores `Profile` objects against a user's ICP
- Writes to `Match` table (Django-managed)
- **Status**: KEEP — actively used by the UI

### `SupabaseMatchScoringService` (SupabaseProfile → SupabaseMatch table)
- Used by `matching/tasks.py` for match recalculation
- Scores `SupabaseProfile` pairs with enrichment-aware ISMC
- Updates `match_suggestions` table (Supabase-managed)
- **Status**: KEEP — this is the new enrichment-aware scorer

### Long-term consideration
These two systems score differently (one is user-vs-profile, the other is
profile-vs-profile). If the `Profile` model is eventually retired in favor of
`SupabaseProfile` everywhere, `MatchScoringService` can be replaced by
`SupabaseMatchScoringService` and the views updated accordingly.

---

## 6. Enrichment Service Overlap (Low Priority)

Three enrichment entry points exist:

| Service | File | Used By |
|---------|------|---------|
| `ExaResearchService` | `matching/enrichment/exa_research.py` | Primary path — Exa API |
| `AIResearchService` | `matching/enrichment/ai_research.py` | Fallback — crawl4ai + Claude |
| `SmartEnrichmentService` | `matching/enrichment/smart_enrichment_service.py` | `smart_enrich` command, `progressive_enrich.py` |

`SmartEnrichmentService` orchestrates free-first-then-expensive logic. It's
still valid but could be simplified now that `research_and_enrich_profile()`
in `ai_research.py` already implements Exa-first → crawl4ai fallback.

**No action needed now** — both paths work. If `SmartEnrichmentService` is
unused in practice (check script run history), it can be removed later.

---

## Priority Order

1. **Delete root scripts** — 5 minutes, zero risk
2. **Delete one-off commands** — 5 minutes, zero risk
3. **Delete v1 commands** — 5 minutes, confirm no cron references
4. **Refactor harmonic mean** — 30 minutes, needs testing
5. **Unify scoring systems** — Future project, non-trivial
