"""
Send Match Follow-ups (B4 — Tier 2 Prompted Feedback)

Identifies PartnerRecommendations eligible for follow-up feedback
(was_contacted=True, contacted 7-14 days ago, no feedback yet)
and reports/queues them for in-app prompting.

Usage:
    python manage.py send_match_followups                    # Report eligible follow-ups
    python manage.py send_match_followups --dry-run          # Preview only
    python manage.py send_match_followups --days-min 7       # Custom window start
    python manage.py send_match_followups --days-max 14      # Custom window end
    python manage.py send_match_followups --user-email X     # Single user
"""

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Identify and report partner recommendations eligible for Tier 2 follow-up feedback'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview eligible follow-ups without marking them',
        )
        parser.add_argument(
            '--days-min',
            type=int,
            default=7,
            help='Minimum days since contact to trigger follow-up (default: 7)',
        )
        parser.add_argument(
            '--days-max',
            type=int,
            default=14,
            help='Maximum days since contact — older ones are excluded (default: 14)',
        )
        parser.add_argument(
            '--user-email',
            type=str,
            default=None,
            help='Only process follow-ups for a specific user (by email)',
        )
        parser.add_argument(
            '--include-uncontacted',
            action='store_true',
            help='Also include viewed-but-not-contacted recommendations (nudge to reach out)',
        )

    def handle(self, *args, **options):
        from matching.models import PartnerRecommendation

        dry_run = options['dry_run']
        days_min = options['days_min']
        days_max = options['days_max']
        user_email = options.get('user_email')
        include_uncontacted = options['include_uncontacted']

        now = timezone.now()
        window_start = now - timedelta(days=days_max)
        window_end = now - timedelta(days=days_min)

        # ── Tier 2: Contacted, waiting for outcome feedback ──────────
        contacted_qs = PartnerRecommendation.objects.filter(
            was_contacted=True,
            contacted_at__gte=window_start,
            contacted_at__lte=window_end,
            feedback_outcome__isnull=True,  # No Tier 2 feedback yet
        ).select_related('user', 'partner')

        if user_email:
            contacted_qs = contacted_qs.filter(user__email=user_email)

        contacted_list = list(contacted_qs)

        # ── Optional: Viewed but not contacted (nudge) ───────────────
        nudge_list = []
        if include_uncontacted:
            nudge_qs = PartnerRecommendation.objects.filter(
                was_viewed=True,
                was_contacted=False,
                recommended_at__gte=window_start,
                recommended_at__lte=window_end,
                feedback_outcome__isnull=True,
            ).select_related('user', 'partner')

            if user_email:
                nudge_qs = nudge_qs.filter(user__email=user_email)

            nudge_list = list(nudge_qs)

        # ── Report ───────────────────────────────────────────────────
        self.stdout.write('')
        self.stdout.write(self.style.HTTP_INFO('=' * 60))
        self.stdout.write(self.style.HTTP_INFO('MATCH FOLLOW-UP REPORT'))
        self.stdout.write(self.style.HTTP_INFO(f'Window: {days_min}-{days_max} days after contact'))
        self.stdout.write(self.style.HTTP_INFO('=' * 60))

        if contacted_list:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                f'Tier 2 Feedback Eligible: {len(contacted_list)} recommendations'
            ))
            self.stdout.write('')

            for rec in contacted_list:
                days_since = (now - rec.contacted_at).days if rec.contacted_at else '?'
                msg_used = 'used outreach msg' if rec.outreach_message_used else 'custom msg'
                views = f'{rec.view_count} views' if rec.view_count else 'no views'
                source = rec.explanation_source or 'unknown'

                self.stdout.write(
                    f'  {rec.partner.name:<30} '
                    f'contacted {days_since}d ago | '
                    f'{msg_used} | {views} | '
                    f'source={source}'
                )
        else:
            self.stdout.write('')
            self.stdout.write('  No recommendations eligible for Tier 2 feedback.')

        if nudge_list:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING(
                f'Viewed but not contacted (nudge candidates): {len(nudge_list)}'
            ))
            self.stdout.write('')

            for rec in nudge_list:
                days_since = (now - rec.recommended_at).days
                views = f'{rec.view_count} views' if rec.view_count else '1 view'

                self.stdout.write(
                    f'  {rec.partner.name:<30} '
                    f'recommended {days_since}d ago | '
                    f'{views}'
                )

        # ── Summary ──────────────────────────────────────────────────
        self.stdout.write('')
        self.stdout.write(self.style.HTTP_INFO('-' * 60))
        self.stdout.write(f'  Total Tier 2 eligible:     {len(contacted_list)}')
        self.stdout.write(f'  Total nudge candidates:    {len(nudge_list)}')

        if contacted_list:
            # Behavioral breakdown
            used_outreach = sum(1 for r in contacted_list if r.outreach_message_used)
            llm_explained = sum(
                1 for r in contacted_list
                if r.explanation_source in ('llm_verified', 'llm_partial')
            )
            self.stdout.write(f'  Used outreach message:     {used_outreach}/{len(contacted_list)}')
            self.stdout.write(f'  Had LLM explanation:       {llm_explained}/{len(contacted_list)}')

        if dry_run:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING('DRY RUN — no changes made'))
        else:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                'In-app feedback prompts will show for eligible recommendations.'
            ))

        self.stdout.write('')

        # ── Feedback options reminder ────────────────────────────────
        self.stdout.write('Tier 2 feedback options:')
        for choice in PartnerRecommendation.FeedbackOutcome.choices:
            self.stdout.write(f'  - {choice[1]}')
        self.stdout.write('')
