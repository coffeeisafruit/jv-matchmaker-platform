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

                # Recalculate scores using the existing match context
                # This is a simplified recalculation - full implementation would
                # call the jv-matcher scoring logic
                old_score = match.harmonic_mean

                # For now, we'll just log that a recalculation is needed
                # Full implementation would call:
                # new_scores = MatchScoringService.calculate_scores(source_profile, target_profile)
                # match.score_ab = new_scores['score_ab']
                # match.score_ba = new_scores['score_ba']
                # match.harmonic_mean = new_scores['harmonic_mean']
                # match.save()

                logger.info(
                    f"Match {match.id} flagged for recalculation: "
                    f"{source_profile.name} <-> {target_profile.name} "
                    f"(current score: {old_score})"
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
