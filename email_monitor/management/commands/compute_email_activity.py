"""
Management command: compute_email_activity

Monthly aggregation of email monitor data → EmailActivitySummary → SupabaseProfile scores.

1. Queries InboundEmail records from past 30 days grouped by profile
2. Computes mailing_activity_score and promotion_willingness_score
3. Creates/updates EmailActivitySummary for current month
4. Writes scores to SupabaseProfile via raw SQL (managed=False)
5. Updates jv_readiness_score with email monitor signals
6. Regenerates promotion_graph_data.js

Usage:
    python3 manage.py compute_email_activity
    python3 manage.py compute_email_activity --month 2025-03-01
    python3 manage.py compute_email_activity --dry-run
"""

import logging
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Compute monthly email activity scores and write to SupabaseProfile'

    def add_arguments(self, parser):
        parser.add_argument('--month', type=str, default='',
                            help='Month to compute (YYYY-MM-DD, default: current month)')
        parser.add_argument('--dry-run', action='store_true',
                            help='Compute scores without writing to DB')
        parser.add_argument('--profile-id', type=str, default='',
                            help='Compute for a single profile only')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        profile_id_filter = options['profile_id']

        if options['month']:
            month = date.fromisoformat(options['month']).replace(day=1)
        else:
            today = date.today()
            month = today.replace(day=1)

        self.stdout.write(f'Computing email activity for {month.strftime("%B %Y")}...')
        if dry_run:
            self.stdout.write(self.style.WARNING('  DRY RUN — no DB writes'))

        from email_monitor.models import MonitoredSubscription, InboundEmail, EmailActivitySummary
        from email_monitor.services.activity_scorer import (
            compute_mailing_activity_score,
            compute_promotion_willingness_score,
            build_promotion_network,
            compute_jv_readiness_delta,
        )

        thirty_days_ago = date.today() - timedelta(days=30)
        sixty_days_ago = date.today() - timedelta(days=60)

        # Get all profiles with subscriptions
        profile_ids = list(
            MonitoredSubscription.objects.filter(status='active')
            .values_list('profile_id', flat=True)
            .distinct()
        )
        if profile_id_filter:
            profile_ids = [p for p in profile_ids if str(p) == profile_id_filter]

        self.stdout.write(f'  Processing {len(profile_ids)} monitored profiles')
        updated = 0

        for profile_id in profile_ids:
            subscriptions = MonitoredSubscription.objects.filter(profile_id=profile_id)

            # 30-day email stats
            emails_30d = InboundEmail.objects.filter(
                subscription__in=subscriptions,
                received_at__date__gte=thirty_days_ago,
            )
            total_30d = emails_30d.count()

            # Prior month activity (for dormant detection)
            prior_month_start = month - timedelta(days=60)
            prior_month_end = month - timedelta(days=31)
            was_active_prior = InboundEmail.objects.filter(
                subscription__in=subscriptions,
                received_at__date__gte=prior_month_start,
                received_at__date__lt=prior_month_end,
            ).exists()

            # Count email types from analysis field
            promotional = 0
            own_product = 0
            content_only = 0
            partner_promos_30d = 0
            partners_promoted_names: dict[str, dict] = {}
            promotion_types: dict[str, int] = {}
            has_own_products = False

            for email_record in emails_30d.filter(analyzed_at__isnull=False):
                analysis = email_record.analysis or {}
                etype = analysis.get('email_type', 'content')

                if etype == 'partner_promotion' or analysis.get('is_promoting_partner'):
                    promotional += 1
                    partner_promos_30d += 1
                    for partner in analysis.get('promoted_partners', []):
                        pname = (partner.get('name', '') or '').strip()
                        if pname:
                            if pname not in partners_promoted_names:
                                partners_promoted_names[pname] = {
                                    'name': pname,
                                    'url': partner.get('website_or_url', ''),
                                    'count': 0,
                                    'affiliate_detected': partner.get('affiliate_link_detected', False),
                                }
                            partners_promoted_names[pname]['count'] += 1
                            ptype = partner.get('product_type', '')
                            if ptype:
                                promotion_types[ptype] = promotion_types.get(ptype, 0) + 1
                elif etype == 'own_promotion':
                    own_product += 1
                    has_own_products = True
                else:
                    content_only += 1

                if analysis.get('own_products_mentioned'):
                    has_own_products = True

            # Compute scores
            weeks_in_30d = 30 / 7
            avg_per_week = total_30d / weeks_in_30d
            activity_score = compute_mailing_activity_score(avg_per_week)
            promo_score = compute_promotion_willingness_score(total_30d, promotional)
            unique_partners = len(partners_promoted_names)

            if not dry_run:
                # Upsert EmailActivitySummary
                summary, _ = EmailActivitySummary.objects.update_or_create(
                    profile=_get_profile(profile_id),
                    month=month,
                    defaults={
                        'emails_sent': total_30d,
                        'avg_emails_per_week': round(avg_per_week, 2),
                        'promotional_emails': promotional,
                        'own_product_emails': own_product,
                        'content_only_emails': content_only,
                        'promotion_ratio': promotional / total_30d if total_30d else 0.0,
                        'unique_partners_promoted': unique_partners,
                        'partners_promoted': list(partners_promoted_names.values()),
                        'promotion_types': promotion_types,
                        'mailing_activity_score': activity_score,
                        'promotion_willingness_score': promo_score or 0.0,
                    }
                )

                # Build promotion network for the profile
                network = build_promotion_network(str(profile_id), month)

                # Write scores to SupabaseProfile via raw SQL
                jv_delta = compute_jv_readiness_delta(
                    profile_id=str(profile_id),
                    was_active_prior_month=was_active_prior,
                    total_emails_30d=total_30d,
                    partner_promos_30d=partner_promos_30d,
                    unique_partners_90d=unique_partners,
                    has_own_products=has_own_products,
                )
                _update_profile_scores(
                    profile_id=str(profile_id),
                    activity_score=activity_score,
                    promo_score=promo_score,
                    network=network,
                    jv_delta=jv_delta,
                )

            updated += 1
            self.stdout.write(
                f'  {profile_id}: activity={activity_score:.2f}, '
                f'promo={promo_score:.2f if promo_score is not None else "None"}, '
                f'partners={unique_partners}'
            )

        self.stdout.write(self.style.SUCCESS(f'\n  Updated {updated} profiles'))

        if not dry_run:
            # Regenerate promotion graph data
            import subprocess
            subprocess.run(['python3', 'scripts/export_promotion_graph.py'], check=False)
            self.stdout.write('  Regenerated promotion_graph_data.js')


def _get_profile(profile_id):
    from matching.models import SupabaseProfile
    return SupabaseProfile.objects.get(pk=profile_id)


def _update_profile_scores(
    profile_id: str,
    activity_score: float,
    promo_score,
    network: dict,
    jv_delta: int,
) -> None:
    """Write email monitor scores to SupabaseProfile via raw SQL (managed=False)."""
    import json

    with connection.cursor() as cursor:
        if promo_score is not None:
            cursor.execute("""
                UPDATE profiles
                SET email_list_activity_score = %s,
                    promotion_willingness_score = %s,
                    last_email_list_check_at = NOW(),
                    promotion_network = %s,
                    jv_readiness_score = LEAST(100, GREATEST(0, COALESCE(jv_readiness_score, 0) + %s))
                WHERE id = %s::uuid
            """, [activity_score, promo_score, json.dumps(network), jv_delta, profile_id])
        else:
            cursor.execute("""
                UPDATE profiles
                SET email_list_activity_score = %s,
                    last_email_list_check_at = NOW(),
                    promotion_network = %s,
                    jv_readiness_score = LEAST(100, GREATEST(0, COALESCE(jv_readiness_score, 0) + %s))
                WHERE id = %s::uuid
            """, [activity_score, json.dumps(network), jv_delta, profile_id])
