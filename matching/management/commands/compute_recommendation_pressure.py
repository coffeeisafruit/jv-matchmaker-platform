"""
Django management command to compute recommendation pressure for all partners.

Computes platform-wide recommendation pressure (how many times each partner
has been recommended in the last 30 days) and caches it on SupabaseProfile.

This helps with bipartite allocation - preventing partner fatigue by spreading
recommendations more evenly across the directory.

Usage:
    python manage.py compute_recommendation_pressure
    python manage.py compute_recommendation_pressure --dry-run
    python manage.py compute_recommendation_pressure --days=14
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Count
from datetime import timedelta
from matching.models import SupabaseProfile, PartnerRecommendation


class Command(BaseCommand):
    help = 'Compute recommendation pressure for all partners based on platform-wide tracking'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print pressure stats without saving to database',
        )
        parser.add_argument(
            '--days',
            type=int,
            default=30,
            help='Number of days to consider for pressure calculation (default: 30)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        days = options['days']
        cutoff = timezone.now() - timedelta(days=days)
        now = timezone.now()

        self.stdout.write(f'Computing recommendation pressure (last {days} days)...')

        # Get recommendation counts per partner
        pressure_data = PartnerRecommendation.objects.filter(
            recommended_at__gte=cutoff
        ).values('partner_id').annotate(
            pressure=Count('id')
        ).order_by('-pressure')

        if not pressure_data:
            self.stdout.write(
                self.style.WARNING('No recommendations found in the time window.')
            )
            return

        # Create lookup dict
        pressure_by_partner = {
            str(p['partner_id']): p['pressure'] for p in pressure_data
        }

        total_recommendations = sum(p['pressure'] for p in pressure_data)
        unique_partners = len(pressure_data)

        self.stdout.write(f'Found {total_recommendations} recommendations across {unique_partners} partners')

        # Distribution analysis
        distribution = {
            'fresh': 0,       # 0 recommendations
            'light': 0,       # 1-5
            'moderate': 0,    # 6-15
            'heavy': 0,       # 16-30
            'over': 0,        # 31+
        }

        for p in pressure_data:
            pressure = p['pressure']
            if pressure <= 5:
                distribution['light'] += 1
            elif pressure <= 15:
                distribution['moderate'] += 1
            elif pressure <= 30:
                distribution['heavy'] += 1
            else:
                distribution['over'] += 1

        # Count fresh partners (not in recommendations at all)
        total_partners = SupabaseProfile.objects.filter(status='Member').count()
        distribution['fresh'] = total_partners - unique_partners

        self.stdout.write('\nPressure Distribution:')
        self.stdout.write(f'  Fresh (0 recommendations): {distribution["fresh"]}')
        self.stdout.write(f'  Light (1-5): {distribution["light"]}')
        self.stdout.write(f'  Moderate (6-15): {distribution["moderate"]}')
        self.stdout.write(f'  Heavy (16-30): {distribution["heavy"]}')
        self.stdout.write(f'  Over-recommended (31+): {distribution["over"]}')

        # Show top 10 most recommended
        self.stdout.write('\nTop 10 Most Recommended Partners:')
        for i, p in enumerate(list(pressure_data)[:10], 1):
            partner = SupabaseProfile.objects.filter(id=p['partner_id']).first()
            name = partner.name if partner else 'Unknown'
            self.stdout.write(f'  {i}. {name}: {p["pressure"]} recommendations')

        if dry_run:
            self.stdout.write(
                self.style.WARNING('\nDry run complete. No data saved.')
            )
            return

        # Update SupabaseProfile records with pressure data
        self.stdout.write('\nUpdating profile pressure data...')
        updated_count = 0

        # Reset all partners to 0 first (those not recommended)
        SupabaseProfile.objects.filter(
            status='Member'
        ).update(
            recommendation_pressure_30d=0,
            pressure_updated_at=now
        )

        # Update partners with actual pressure
        for partner_id, pressure in pressure_by_partner.items():
            updated = SupabaseProfile.objects.filter(
                id=partner_id
            ).update(
                recommendation_pressure_30d=pressure,
                pressure_updated_at=now
            )
            if updated:
                updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f'\nSuccessfully updated pressure data for {updated_count} partners.'
            )
        )

        # Gini coefficient (measure of inequality - lower is more equal distribution)
        if pressure_data:
            pressures = sorted([p['pressure'] for p in pressure_data])
            n = len(pressures)
            cumsum = sum((i + 1) * p for i, p in enumerate(pressures))
            gini = (2 * cumsum) / (n * sum(pressures)) - (n + 1) / n if sum(pressures) > 0 else 0

            self.stdout.write(f'\nGini Coefficient: {gini:.3f}')
            if gini > 0.5:
                self.stdout.write(
                    self.style.WARNING(
                        'High inequality in recommendations. Consider surfacing fresh partners.'
                    )
                )
            elif gini < 0.3:
                self.stdout.write(
                    self.style.SUCCESS('Good distribution of recommendations across partners.')
                )
