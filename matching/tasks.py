"""
Background tasks for the Matching module.

These tasks can be run with Celery for async processing,
or called directly (synchronously) if Celery is not configured.
"""

import logging
from django.db.models import Q

logger = logging.getLogger(__name__)

# Try to import Celery, fall back to sync execution if not available
try:
    from celery import shared_task
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False

    # Create a no-op decorator for when Celery isn't installed
    def shared_task(func):
        """Fallback decorator when Celery is not available."""
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper.delay = wrapper  # .delay() just calls the function directly
        wrapper.apply_async = lambda *a, **kw: wrapper(*a, **kw)
        return wrapper


@shared_task
def recalculate_matches_for_profile(profile_id: str):
    """
    Recalculate all SupabaseMatch scores for a profile.

    Called when profile data changes via Clay enrichment webhook.
    This ensures match scores reflect the latest profile data.

    Args:
        profile_id: UUID string of the SupabaseProfile

    Returns:
        dict with recalculation results
    """
    from matching.models import SupabaseProfile, SupabaseMatch

    results = {
        'profile_id': str(profile_id),
        'matches_found': 0,
        'matches_updated': 0,
        'errors': []
    }

    try:
        # Verify profile exists
        profile = SupabaseProfile.objects.get(id=profile_id)
        logger.info(f"Recalculating matches for profile: {profile.name} ({profile_id})")

        # Get all matches where this profile is involved (either side)
        matches = SupabaseMatch.objects.filter(
            Q(profile_id=profile_id) | Q(suggested_profile_id=profile_id)
        )

        results['matches_found'] = matches.count()

        for match in matches:
            try:
                # Get both profiles for the match
                source_profile = SupabaseProfile.objects.filter(id=match.profile_id).first()
                target_profile = SupabaseProfile.objects.filter(id=match.suggested_profile_id).first()

                if not source_profile or not target_profile:
                    results['errors'].append(f"Missing profile for match {match.id}")
                    continue

                # Recalculate scores using ISMC scoring service
                from matching.services import SupabaseMatchScoringService
                scorer = SupabaseMatchScoringService()

                old_score = match.harmonic_mean
                new_scores = scorer.score_pair(source_profile, target_profile)

                match.score_ab = new_scores['score_ab']
                match.score_ba = new_scores['score_ba']
                match.harmonic_mean = new_scores['harmonic_mean']
                match.save(update_fields=['score_ab', 'score_ba', 'harmonic_mean'])

                logger.info(
                    f"Match {match.id} recalculated: "
                    f"{source_profile.name} <-> {target_profile.name} "
                    f"({old_score} -> {new_scores['harmonic_mean']})"
                )
                results['matches_updated'] += 1

            except Exception as e:
                results['errors'].append(f"Error processing match {match.id}: {str(e)}")

        logger.info(
            f"Match recalculation complete for {profile.name}: "
            f"{results['matches_found']} found, {results['matches_updated']} flagged"
        )

    except SupabaseProfile.DoesNotExist:
        error_msg = f"SupabaseProfile not found: {profile_id}"
        logger.error(error_msg)
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"Error in recalculation: {str(e)}"
        logger.error(error_msg)
        results['errors'].append(error_msg)

    return results


@shared_task
def bulk_recalculate_matches(profile_ids: list):
    """
    Recalculate matches for multiple profiles.

    Useful when batch enrichment updates multiple profiles at once.

    Args:
        profile_ids: List of UUID strings

    Returns:
        dict with bulk recalculation results
    """
    results = {
        'total_profiles': len(profile_ids),
        'processed': 0,
        'failed': 0,
        'details': []
    }

    for profile_id in profile_ids:
        try:
            result = recalculate_matches_for_profile(profile_id)
            results['processed'] += 1
            results['details'].append(result)
        except Exception as e:
            results['failed'] += 1
            results['details'].append({
                'profile_id': profile_id,
                'error': str(e)
            })

    return results


@shared_task
def regenerate_member_report(report_id: int):
    """
    Regenerate a single member report with fresh ISMC scoring.
    Preserves the same access code and report URL.
    """
    from django.utils import timezone
    from matching.models import MemberReport, ReportPartner, SupabaseProfile
    from matching.services import SupabaseMatchScoringService

    results = {'report_id': report_id, 'partners_created': 0, 'errors': []}

    try:
        report = MemberReport.objects.get(id=report_id, is_active=True)
    except MemberReport.DoesNotExist:
        results['errors'].append(f'Report {report_id} not found or inactive')
        return results

    client_sp = report.supabase_profile
    if not client_sp:
        results['errors'].append(f'Report {report_id} has no linked supabase_profile')
        return results

    logger.info(f"Regenerating report {report_id} for {report.member_name}")

    # Delete existing partner records
    old_count = report.partners.count()
    report.partners.all().delete()
    logger.info(f"Deleted {old_count} old ReportPartner records")

    # Re-score using ISMC
    scorer = SupabaseMatchScoringService()
    candidates = list(
        SupabaseProfile.objects.filter(status='Member')
        .exclude(id=client_sp.id)
    )

    scored = []
    for p in candidates:
        if not p.name or p.name.count(',') >= 2 or len(p.name.strip().split()) < 2:
            continue
        try:
            result = scorer.score_pair(client_sp, p)
            scored.append({
                'partner': p,
                'score': result['score_ab'],
                'breakdown': result['breakdown_ab'],
            })
        except Exception as e:
            results['errors'].append(f'Scoring error for {p.name}: {str(e)}')

    scored.sort(key=lambda x: x['score'], reverse=True)
    top_matches = scored[:10]

    for rank, match in enumerate(top_matches, 1):
        partner_sp = match['partner']
        try:
            ReportPartner.objects.create(
                report=report,
                rank=rank,
                section='priority' if match['score'] >= 70 else 'this_week',
                section_label='Priority Contacts' if match['score'] >= 70 else 'This Week',
                section_note='',
                name=partner_sp.name,
                company=partner_sp.company or partner_sp.name,
                tagline=partner_sp.what_you_do or partner_sp.offering or '',
                email=partner_sp.email or '',
                website=partner_sp.website or '',
                phone=partner_sp.phone or '',
                linkedin=partner_sp.linkedin or '',
                match_score=match['score'],
                source_profile=partner_sp,
            )
            results['partners_created'] += 1
        except Exception as e:
            results['errors'].append(f'Error creating partner {partner_sp.name}: {str(e)}')

    # Mark report as freshly updated
    report.created_at = timezone.now()
    report.save(update_fields=['created_at'])

    logger.info(
        f"Report {report_id} regenerated: {results['partners_created']} partners"
    )
    return results


@shared_task
def regenerate_all_monthly_reports(month: str = None):
    """Monthly batch: regenerate all active reports."""
    from matching.models import MemberReport

    reports = MemberReport.objects.filter(is_active=True)
    if month:
        try:
            parts = month.split('-')
            from datetime import date
            month_date = date(int(parts[0]), int(parts[1]), 1)
            reports = reports.filter(month=month_date)
        except (ValueError, IndexError):
            logger.error(f"Invalid month format: {month}")
            return {'error': f'Invalid month: {month}'}

    results = {'total': reports.count(), 'triggered': 0, 'errors': []}

    for report in reports:
        try:
            regenerate_member_report(report.id)
            results['triggered'] += 1
        except Exception as e:
            results['errors'].append(f'Report {report.id}: {str(e)}')

    logger.info(f"Batch regeneration: {results['triggered']}/{results['total']} reports")
    return results


@shared_task
def refresh_reports_for_profile(profile_id: str):
    """Incremental: when a profile changes, update reports containing that partner."""
    from matching.models import ReportPartner

    affected = ReportPartner.objects.filter(
        source_profile_id=profile_id
    ).values_list('report_id', flat=True).distinct()

    results = {'profile_id': str(profile_id), 'reports_affected': len(affected), 'triggered': 0}

    for report_id in affected:
        try:
            regenerate_member_report(report_id)
            results['triggered'] += 1
        except Exception as e:
            logger.error(f"Failed to refresh report {report_id}: {e}")

    logger.info(
        f"Profile {profile_id} changed: refreshed {results['triggered']}/{results['reports_affected']} reports"
    )
    return results
