"""
Final Production Judge — AI-powered profile quality evaluation.

Runs after deterministic validation (profile_standard.py). Evaluates whether
content is compelling, specific, and partnership-ready using Claude as a
senior JV partnership strategist.

Scoring tiers:
  90-100: APPROVED — ship immediately
  80-89:  APPROVED_WITH_NOTES — ship with advisory
  70-79:  Self-remediate inline, then re-score
  50-69:  Route to targeted pipeline stage
  <50:    Full pipeline restart

Every evaluation is logged to ProfileQualityLog with append-only score history.

Usage:
    from matching.enrichment.flows.final_judge import final_judge_task
    result = final_judge_task(report_id=42)

    # Or via management command:
    python manage.py validate_profile_standard --client-name "..." --judge
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from prefect import task, get_run_logger

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class JudgeResult:
    """Result from a Final Judge evaluation."""
    report_id: int
    score: int = 0
    verdict: str = 'blocked'
    remediation_action: str = 'none'
    passed: bool = False
    judge_notes: str = ''
    error: str = ''


# ---------------------------------------------------------------------------
# The evaluation prompt
# ---------------------------------------------------------------------------

JUDGE_SYSTEM_PROMPT = """You are the Final Production Judge for JV MatchMaker profiles. You evaluate AI-generated client profiles before they are delivered to real clients seeking Joint Venture partnerships.

You are a senior partnership strategist who has personally brokered hundreds of JV deals. You know what makes a busy affiliate manager open an email, what makes a potential partner say "I want to work with them," and what makes a client feel seen and understood. You are also a ruthless editor — you will not let vague, generic, or placeholder content reach a client.

Ask yourself constantly: "If I were a potential JV partner reading this, would I be compelled to reach out?"

## SCORING RUBRIC

### REQUIRED FIELDS (each must pass — failing ANY blocks production):

- contact_name: Real human name (2+ words). Reject: "Business Owner," initials only, company name, empty.
- program_name: Distinct program/offer name. Reject: person's name repeated, category labels, generic placeholders.
- program_focus: What makes this program specifically valuable. Reject: "Business Growth," "Helping Entrepreneurs," vague phrases.
- target_audience: Specific enough that a partner could check their list. Reject: "Entrepreneurs & Business Owners," broad catch-alls.
- about_story: Min 2 complete sentences, feels like a real human wrote it. Reject: auto-generated filler, "As a [title]..." patterns.
- credentials: Min 2 distinct items, complete text. At least one must build JV credibility (audience size, revenue, media, case study).
- seeking_goals: Min 1 clearly stated goal for JV partnerships.
- contact info: Email OR booking link must be present and valid-looking.
- partner_count: Must be >= 10.

### OUTREACH EMAIL STANDARD:

Initial Outreach (ALL required):
1. {{partner_first_name}} token in greeting
2. Opening specific to THIS client's offer (not generic)
3. Clear value proposition for the partner
4. Social proof (specific credential, metric, or result)
5. Soft CTA with real booking link or specific ask
6. Signature: name + company
7. Length: 120-250 words

Follow-Up (ALL required):
1. Value-add opening (not "Just following up")
2. Reference to initial message
3. Value restated in different language
4. Soft close
5. Length: 50-100 words

### PARTNER CARD STANDARD (per card):

Required: name, company/tagline, audience (20+ chars), why_fit (3-part: shared audience → alignment → next step), at least 1 contact, detail_note (non-empty).
Recommended: tags (2-4), list_size (numeric), website (URL), schedule (booking link).

### RECOMMENDED FIELDS (scored, not blocking):

offers_partners (2+ items), tiers (2-3), key_message_headline (+ 3 points), partner_deliverables (3+), why_converts (2+), launch_stats (1+ case study), faqs (3+ Q&A), resource_links (2+), perfect_for (3+ archetypes), shared_stage (named collaborators).

## SCORING:

Score = (required fields passed × base) + (recommended × bonus) + (quality assessment)
- 90-100: APPROVED
- 80-89: APPROVED_WITH_NOTES
- 70-79: REVISION_REQUIRED (self-remediable)
- 50-69: REVISION_REQUIRED (needs pipeline routing)
- Below 50: BLOCKED

## OUTPUT FORMAT

Return a JSON object with exactly these fields:
{
    "score": <integer 0-100>,
    "verdict": "approved" | "approved_with_notes" | "revision_required" | "blocked",
    "required_fields_passed": ["field1", "field2", ...],
    "required_fields_failed": [{"field": "...", "reason": "...", "blocking": true}],
    "recommended_fields_present": ["field1", "field2", ...],
    "recommended_fields_missing": ["field1", "field2", ...],
    "partner_cards_flagged": [{"name": "...", "issues": ["issue1", "issue2"]}],
    "outreach_issues": {"initial": ["issue1"], "followup": ["issue1"]},
    "remediation_notes": "What should be fixed and how",
    "judge_notes": "Institutional memory — pattern analysis, pipeline feedback, what to improve"
}

CRITICAL: Return ONLY valid JSON. No markdown, no explanation outside the JSON."""


def _build_evaluation_prompt(
    client_profile: dict,
    outreach_templates: dict,
    partner_cards: list[dict],
    partner_count: int,
) -> str:
    """Build the user-message portion of the evaluation prompt."""
    return f"""Evaluate this JV MatchMaker client profile for production readiness.

## CLIENT PROFILE
```json
{json.dumps(client_profile, indent=2, default=str)[:8000]}
```

## OUTREACH TEMPLATES
```json
{json.dumps(outreach_templates, indent=2, default=str)[:3000]}
```

## PARTNER CARDS ({partner_count} total, showing first 5)
```json
{json.dumps(partner_cards[:5], indent=2, default=str)[:5000]}
```

## PARTNER COUNT: {partner_count}

Evaluate against the rubric and return your JSON verdict."""


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------

@task(name="final-production-judge", retries=1, retry_delay_seconds=10)
def final_judge_task(
    report_id: int,
    dry_run: bool = False,
) -> dict:
    """
    Run the Final Production Judge on a single MemberReport.

    Steps:
      1. Load report data
      2. Get previous evaluation (if any)
      3. Call Claude with the full evaluation prompt
      4. Parse structured JSON response
      5. Log to ProfileQualityLog (append-only)
      6. Take remediation action based on score tier
      7. Update report production_status

    Returns dict with: report_id, score, verdict, remediation_action, passed, error.
    """
    try:
        log = get_run_logger()
    except Exception:
        log = logger

    result = JudgeResult(report_id=report_id)

    try:
        # --- Step 1: Load report data ---
        import django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'jv_matchmaker.settings')
        try:
            django.setup()
        except RuntimeError:
            pass  # Already configured

        from django.utils import timezone
        from matching.models import MemberReport, ProfileQualityLog

        try:
            report = MemberReport.objects.get(id=report_id, is_active=True)
        except MemberReport.DoesNotExist:
            result.error = f'Report {report_id} not found or inactive'
            log.error(result.error)
            return _result_to_dict(result)

        client_profile = report.client_profile or {}
        outreach_templates = report.outreach_templates or {}
        partner_cards = list(
            report.partners.order_by('rank').values(
                'name', 'company', 'tagline', 'audience', 'why_fit',
                'detail_note', 'email', 'phone', 'linkedin', 'website',
                'schedule', 'list_size', 'tags', 'match_score',
            )
        )
        partner_count = len(partner_cards)

        log.info(
            "Final Judge evaluating: %s (%s) — %d partners",
            report.member_name, report.company_name, partner_count,
        )

        # --- Step 2: Get previous evaluation ---
        previous_log = (
            ProfileQualityLog.objects
            .filter(report=report)
            .order_by('-evaluated_at')
            .first()
        )
        previous_run_id = previous_log.run_id if previous_log else None
        previous_score = previous_log.score if previous_log else None
        score_history = list(previous_log.score_history) if previous_log else []

        # --- Step 3: Call Claude ---
        from matching.enrichment.claude_client import ClaudeClient

        client = ClaudeClient(
            max_tokens=4096,
            openrouter_key=os.environ.get('OPENROUTER_API_KEY', ''),
            anthropic_key=os.environ.get('ANTHROPIC_API_KEY', ''),
        )

        if not client.api_key:
            result.error = 'No API key configured (OPENROUTER_API_KEY or ANTHROPIC_API_KEY)'
            log.warning(result.error)
            return _result_to_dict(result)

        eval_prompt = _build_evaluation_prompt(
            client_profile, outreach_templates, partner_cards, partner_count,
        )

        full_prompt = f"{JUDGE_SYSTEM_PROMPT}\n\n---\n\n{eval_prompt}"

        raw_response = client.call(full_prompt)
        if not raw_response:
            result.error = 'Claude returned empty response'
            log.error(result.error)
            return _result_to_dict(result)

        # --- Step 4: Parse response ---
        parsed = client.parse_json(raw_response)
        if not parsed:
            # Try extracting JSON from the response
            parsed = _extract_json(raw_response)
        if not parsed:
            result.error = f'Failed to parse judge response as JSON'
            log.error("%s: %s", result.error, raw_response[:500])
            return _result_to_dict(result)

        score = int(parsed.get('score', 0))
        verdict = parsed.get('verdict', 'blocked')
        result.score = score
        result.verdict = verdict
        result.judge_notes = parsed.get('judge_notes', '')

        # --- Step 5: Log to ProfileQualityLog ---
        run_id = str(uuid.uuid4())

        # Append current evaluation to score history
        score_history.append({
            'run_id': run_id,
            'score': score,
            'evaluated_at': datetime.now().isoformat(),
            'verdict': verdict,
        })

        if not dry_run:
            ProfileQualityLog.objects.create(
                report=report,
                run_id=run_id,
                score=score,
                verdict=verdict,
                previous_run_id=previous_run_id,
                previous_score=previous_score,
                score_delta=(score - previous_score) if previous_score is not None else None,
                score_history=score_history,
                required_fields_passed=parsed.get('required_fields_passed', []),
                required_fields_failed=parsed.get('required_fields_failed', []),
                recommended_fields_present=parsed.get('recommended_fields_present', []),
                recommended_fields_missing=parsed.get('recommended_fields_missing', []),
                partner_cards_flagged=parsed.get('partner_cards_flagged', []),
                outreach_issues=parsed.get('outreach_issues', {}),
                remediation_action='none',  # Updated below if remediation happens
                remediation_notes=parsed.get('remediation_notes', ''),
                judge_notes=parsed.get('judge_notes', ''),
            )

        log.info(
            "Judge evaluation: %s — score %d, verdict %s",
            report.member_name, score, verdict,
        )

        # --- Step 6: Remediation based on score tier ---
        if score >= 90:
            # APPROVED — ship immediately
            result.passed = True
            result.remediation_action = 'none'
            if not dry_run:
                report.production_status = 'production'

        elif score >= 80:
            # APPROVED_WITH_NOTES — ship with advisory
            result.passed = True
            result.remediation_action = 'none'
            if not dry_run:
                report.production_status = 'production'

        elif score >= 70:
            # Self-remediate, then re-score
            result.remediation_action = 'self_remediated'
            log.info("Score 70-79: attempting self-remediation for %s", report.member_name)

            remediated = _self_remediate(
                report, parsed, client_profile, outreach_templates, log, dry_run,
            )

            if remediated and not dry_run:
                # Re-run deterministic validation after remediation
                from matching.enrichment.profile_standard import validate_profile
                new_validation = validate_profile(
                    report.client_profile,
                    report.outreach_templates,
                    partner_count,
                    partner_cards,
                )
                report.production_score = new_validation.score
                report.production_issues = [
                    {'field': i.field, 'requirement': i.requirement.value, 'message': i.message}
                    for i in new_validation.issues
                ]

                # Log remediation attempt
                remediation_run_id = str(uuid.uuid4())
                new_score = int(new_validation.score)
                score_history.append({
                    'run_id': remediation_run_id,
                    'score': new_score,
                    'evaluated_at': datetime.now().isoformat(),
                    'verdict': 'approved' if new_score >= 80 else 'revision_required',
                })
                ProfileQualityLog.objects.create(
                    report=report,
                    run_id=remediation_run_id,
                    score=new_score,
                    verdict='approved' if new_score >= 80 else 'revision_required',
                    previous_run_id=run_id,
                    previous_score=score,
                    score_delta=new_score - score,
                    score_history=score_history,
                    required_fields_passed=parsed.get('required_fields_passed', []),
                    required_fields_failed=parsed.get('required_fields_failed', []),
                    recommended_fields_present=parsed.get('recommended_fields_present', []),
                    recommended_fields_missing=parsed.get('recommended_fields_missing', []),
                    partner_cards_flagged=parsed.get('partner_cards_flagged', []),
                    outreach_issues=parsed.get('outreach_issues', {}),
                    remediation_action='self_remediated',
                    remediation_notes=f'Self-remediated from {score} to {new_score}',
                    judge_notes=parsed.get('judge_notes', ''),
                )

                if new_score >= 80:
                    result.passed = True
                    result.score = new_score
                    report.production_status = 'production'
                    log.info("Self-remediation succeeded: %d → %d", score, new_score)
                else:
                    report.production_status = 'in_remediation'
                    result.score = new_score
                    _route_to_pipeline(report, parsed, log, dry_run)
                    log.info("Self-remediation insufficient: %d → %d, routing to pipeline", score, new_score)

        elif score >= 50:
            # Route to targeted pipeline stage
            result.remediation_action = 'routed_to_verification'
            if not dry_run:
                report.production_status = 'in_remediation'
                _route_to_pipeline(report, parsed, log, dry_run)

                # Update the log entry
                ProfileQualityLog.objects.filter(run_id=run_id).update(
                    remediation_action=result.remediation_action,
                )

        else:
            # Full pipeline restart
            result.remediation_action = 'full_restart'
            if not dry_run:
                report.production_status = 'pipeline_restart'
                _full_restart(report, parsed, log, dry_run)

                # Update the log entry
                ProfileQualityLog.objects.filter(run_id=run_id).update(
                    remediation_action='full_restart',
                )

        # --- Step 7: Save report ---
        if not dry_run:
            report.production_score = result.score if result.score else score
            report.production_validated_at = timezone.now()
            report.save()

        return _result_to_dict(result)

    except Exception as exc:
        result.error = str(exc)
        log.exception("Final Judge failed for report %d: %s", report_id, exc)
        return _result_to_dict(result)


# ---------------------------------------------------------------------------
# Remediation helpers
# ---------------------------------------------------------------------------

def _self_remediate(
    report,
    parsed: dict,
    client_profile: dict,
    outreach_templates: dict,
    log,
    dry_run: bool,
) -> bool:
    """Attempt to fix identified issues using ProfileGapFiller.

    Returns True if any fields were updated.
    """
    try:
        from matching.enrichment.profile_gap_filler import ProfileGapFiller
        filler = ProfileGapFiller()
        if not filler.is_available():
            log.warning("ProfileGapFiller not available — skipping self-remediation")
            return False

        sp = report.supabase_profile
        if not sp:
            log.warning("No linked SupabaseProfile — cannot self-remediate")
            return False

        # Determine which fields need fixing from the judge's assessment
        missing_fields = []
        failed = parsed.get('required_fields_failed', [])
        missing_rec = parsed.get('recommended_fields_missing', [])

        for item in failed:
            field_name = item.get('field', '') if isinstance(item, dict) else str(item)
            if field_name and field_name not in ('partner_count', 'contact_method'):
                missing_fields.append(field_name)

        for field_name in missing_rec:
            if field_name not in missing_fields:
                missing_fields.append(field_name)

        if not missing_fields:
            log.info("No fixable fields identified by judge")
            return False

        # Build profile_data from SupabaseProfile
        profile_data = {
            'name': sp.name or '',
            'company': sp.company or '',
            'bio': sp.bio or '',
            'what_you_do': sp.what_you_do or '',
            'who_you_serve': sp.who_you_serve or '',
            'offering': sp.offering or '',
            'seeking': sp.seeking or '',
            'niche': sp.niche or '',
            'signature_programs': sp.signature_programs or '',
            'audience_type': sp.audience_type or '',
            'list_size': sp.list_size,
            'social_reach': sp.social_reach,
            'booking_link': sp.booking_link or '',
            'email': sp.email or '',
            'website': sp.website or '',
        }

        filled = filler.fill_gaps(profile_data, client_profile, missing_fields)

        if filled and not dry_run:
            updated_profile = dict(report.client_profile or {})

            # Handle outreach templates separately
            if 'outreach_templates' in filled:
                report.outreach_templates = filled.pop('outreach_templates')

            updated_profile.update(filled)
            report.client_profile = updated_profile
            report.save()
            log.info("Self-remediated %d fields: %s", len(filled), ', '.join(filled.keys()))
            return True

        return bool(filled)

    except Exception as exc:
        log.exception("Self-remediation failed: %s", exc)
        return False


def _route_to_pipeline(report, parsed: dict, log, dry_run: bool) -> None:
    """Route a failing profile to the appropriate pipeline stage."""
    from matching.enrichment.retry_queue import enqueue
    from config.alerting import send_alert

    failed_fields = parsed.get('required_fields_failed', [])
    missing_rec = parsed.get('recommended_fields_missing', [])
    remediation_notes = parsed.get('remediation_notes', '')

    # Determine routing
    needs_verification = any(
        isinstance(f, dict) and f.get('field', '') in (
            'contact_method', 'credentials', 'partner_count',
        )
        for f in failed_fields
    )
    needs_research = any(
        isinstance(f, dict) and f.get('field', '') in (
            'program_focus', 'target_audience', 'about_story', 'seeking_goals',
        )
        for f in failed_fields
    )

    routing = 'research' if needs_research else 'verification'

    if not dry_run:
        sp = report.supabase_profile
        profile_id = str(sp.id) if sp else str(report.id)

        enqueue(
            profile_id=profile_id,
            operation='profile_validation_failed',
            reason=f'Score {parsed.get("score", 0)}: {remediation_notes[:200]}',
            context={
                'report_id': report.id,
                'routing': routing,
                'failed_fields': [
                    f.get('field', str(f)) if isinstance(f, dict) else str(f)
                    for f in failed_fields
                ],
                'missing_recommended': missing_rec,
            },
        )

        send_alert(
            'warning',
            f'Profile needs {routing}: {report.member_name} ({report.company_name})',
            f'Score: {parsed.get("score", 0)}/100\n'
            f'Failed fields: {", ".join(f.get("field", str(f)) if isinstance(f, dict) else str(f) for f in failed_fields[:5])}\n'
            f'Notes: {remediation_notes[:300]}',
        )

    log.info("Routed %s to %s pipeline stage", report.member_name, routing)


def _full_restart(report, parsed: dict, log, dry_run: bool) -> None:
    """Queue a full pipeline restart for critically failing profiles."""
    from matching.enrichment.retry_queue import enqueue
    from config.alerting import send_alert

    remediation_notes = parsed.get('remediation_notes', '')

    if not dry_run:
        sp = report.supabase_profile
        profile_id = str(sp.id) if sp else str(report.id)

        enqueue(
            profile_id=profile_id,
            operation='profile_pipeline_restart',
            reason=f'Score {parsed.get("score", 0)}: fundamental quality issues',
            context={
                'report_id': report.id,
                'failed_fields': [
                    f.get('field', str(f)) if isinstance(f, dict) else str(f)
                    for f in parsed.get('required_fields_failed', [])
                ],
                'judge_notes': parsed.get('judge_notes', ''),
            },
        )

        send_alert(
            'critical',
            f'Profile needs FULL RESTART: {report.member_name} ({report.company_name})',
            f'Score: {parsed.get("score", 0)}/100\n'
            f'The profile has fundamental data quality issues that cannot be fixed '
            f'without re-collecting data or a full pipeline re-run.\n'
            f'Notes: {remediation_notes[:300]}',
        )

    log.info("Full restart queued for %s (score: %s)", report.member_name, parsed.get('score'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_json(text: str) -> Optional[dict]:
    """Try to extract JSON from a response that may include markdown."""
    # Try raw parse first
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        pass

    # Try extracting from ```json ... ``` blocks
    import re
    match = re.search(r'```(?:json)?\s*\n?(.*?)```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Try finding the outermost { ... }
    start = text.find('{')
    end = text.rfind('}')
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass

    return None


def _result_to_dict(result: JudgeResult) -> dict:
    """Convert JudgeResult to a plain dict for serialization."""
    return {
        'report_id': result.report_id,
        'score': result.score,
        'verdict': result.verdict,
        'remediation_action': result.remediation_action,
        'passed': result.passed,
        'judge_notes': result.judge_notes,
        'error': result.error,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import argparse
    import django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'jv_matchmaker.settings')
    django.setup()

    parser = argparse.ArgumentParser(description='Run Final Production Judge')
    parser.add_argument('report_id', type=int, help='MemberReport ID to evaluate')
    parser.add_argument('--dry-run', action='store_true', help='Do not save changes')
    args = parser.parse_args()

    result = final_judge_task(args.report_id, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))
